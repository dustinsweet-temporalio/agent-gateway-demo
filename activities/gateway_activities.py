from __future__ import annotations

import dataclasses
import os
from typing import Any

import requests
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from common.models import (
    CorrelationContext,
    EvaluatePolicyInput,
    GatewayOperationResolution,
    InvokeToolInput,
    NestedToolCallRequest,
    PolicyDecision,
    SignalOperationCallbackInput,
    SubmitNestedToolCallInput,
    ToolCallResponse,
)

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
# This worker's own namespace. Both Activities at the bottom of this file
# connect here and nowhere else: there is no longer any code in the gateway that
# reaches into another team's cluster, because the Nexus Endpoint does that.
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
OPERATION_RESOLVED_SIGNAL = "operation_resolved"


def _protected_environments() -> set[str]:
    raw = os.getenv("PROTECTED_ENVIRONMENTS", "prod,production")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


@activity.defn
def evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    """Represents the Agent Gateway policy engine.

    Kept in an Activity, not in Workflow code, so policy rules can change without
    a Workflow code change and without introducing non-determinism. Policy is a
    function of the tool identity and the requested arguments: promoting a release
    to a protected environment (prod) requires approval; everything else, including
    reads and promotions to staging, runs immediately.
    """
    if input.tool_name == "promote_release":
        environment = str(input.arguments.get("environment", "")).lower()
        if environment in _protected_environments():
            return PolicyDecision(
                requires_approval=True,
                reason=f"Promotion to {environment} requires human approval.",
            )
    return PolicyDecision(requires_approval=False, reason=None)


@activity.defn
def invoke_tool(input: InvokeToolInput) -> dict[str, Any]:
    """Invoke the downstream tool.

    Activities have at-least-once semantics, so the idempotency key is passed
    downstream. A retry after a Worker crash or network blip must not promote a
    release twice. A longer-running tool would also call activity.heartbeat() here;
    this mock tool returns quickly, so it does not.
    """
    url = os.getenv("MOCK_TOOL_URL", "http://mock-tool:9000/invoke")
    try:
        resp = requests.post(
            url,
            json={"tool_name": input.tool_name, "arguments": input.arguments},
            headers={"Idempotency-Key": input.idempotency_key},
            timeout=45,
        )
    except requests.RequestException as err:
        # Transient transport failure: allow Temporal to retry per the retry policy.
        raise RuntimeError(f"tool transport error: {err}") from err

    if 400 <= resp.status_code < 500:
        # A client error will not succeed on retry, so mark it non-retryable.
        raise ApplicationError(
            f"tool rejected request: {resp.status_code} {resp.text}",
            non_retryable=True,
        )

    resp.raise_for_status()
    return resp.json()




# ------------------------------------------------- serving an external caller
#
# Both Activities below connect to this worker's OWN namespace. Nothing in the
# gateway reaches into another team's cluster any more: the cross-team hop is a
# Nexus Endpoint, and by the time either of these runs, the call has already
# crossed it. Workflow code still never holds a client, because it has to replay
# deterministically and a network call cannot.


@activity.defn
async def submit_nested_tool_call(
    input: SubmitNestedToolCallInput,
) -> dict[str, Any]:
    """Lodge an external tool's protected-action request with the chain.

    An Update, which workflow code cannot issue directly, so
    ProtectedActionWorkflow goes through here. The chain is addressed by the
    workflow_id the caller carried as correlation context, so the request lands
    on the operator's existing session rather than opening a second one.

    caller_principal is the chain's owner, threaded through from whoever asked
    for the release. The external tool is acting on their behalf, and the chain
    checks a nested call against its owner, so a tool that invented its own
    principal here would simply be refused. Its own identity travels alongside
    in caller_service and in the call path, so the ledger records both who
    authorized the release and which system actually asked.
    """
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = client.get_workflow_handle(input.gateway_workflow_id)
    request = NestedToolCallRequest(
        tool1_name=f"{input.caller_service.replace('-', '_')}_scan",
        tool1_arguments={
            **input.arguments,
            "caller_workflow_id": input.caller_workflow_id,
        },
        tool2_name=input.tool_name,
        tool2_arguments=dict(input.arguments),
        idempotency_key=input.idempotency_key,
        correlation=CorrelationContext(
            workflow_id=input.gateway_workflow_id,
            workflow_id_source="explicit_authorized",
            idempotency_key=input.idempotency_key,
            caller_principal=input.caller_principal,
            caller_service=input.caller_service,
            runtime="nexus",
            call_path=[
                input.caller_service,
                f"{input.caller_service.replace('-', '_')}_scan",
            ],
        ),
        requested_action=(
            f"Promote {input.arguments.get('service', '?')} "
            f"{input.arguments.get('version', '?')} to "
            f"{input.arguments.get('environment', '?')}"
        ),
        justification=input.justification or None,
        controlled_tool1=True,
        replay_safe=True,
        callback_workflow_id=input.callback_workflow_id,
        origin_operation_id=input.origin_operation_id,
    )
    response: ToolCallResponse = await handle.execute_update(
        "request_nested_tool_call", request, result_type=ToolCallResponse
    )
    activity.logger.info(
        "chain %s answered %s for %s",
        input.gateway_workflow_id,
        response.status,
        input.caller_service,
    )
    return dataclasses.asdict(response)


@activity.defn
async def signal_operation_callback(input: SignalOperationCallbackInput) -> None:
    """Tell the workflow holding a request open that its operation resolved.

    The other half of the pause. Something is suspended waiting for this --
    in CASE-2b a ProtectedActionWorkflow, which is in turn the handler for a
    Nexus operation another team's security scan is suspended on. Delivering it
    here completes that chain of waits without anyone polling.
    """
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = client.get_workflow_handle(input.callback_workflow_id)
    await handle.signal(
        OPERATION_RESOLVED_SIGNAL,
        GatewayOperationResolution(
            operation_id=input.operation_id,
            status=input.status,
            result=input.result,
            reason=input.reason,
        ),
    )
    activity.logger.info(
        "told %s that %s is %s",
        input.callback_workflow_id,
        input.operation_id,
        input.status,
    )
