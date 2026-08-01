from __future__ import annotations

import os
import uuid
from typing import Any

import requests
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from common.models import (
    CanaryAnalysisStarted,
    EvaluatePolicyInput,
    InvokeToolInput,
    PolicyDecision,
    SignalReleaseSafetyInput,
    StartCanaryAnalysisInput,
)

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
# Release Safety's namespace. The gateway team does not deploy anything into it
# and cannot see inside their code; it knows a namespace, a task queue, a
# workflow type name, and a signal name, all of which Release Safety publishes.
RELEASE_SAFETY_NAMESPACE = os.getenv("RELEASE_SAFETY_NAMESPACE", "release-safety")
RELEASE_SAFETY_TASK_QUEUE = os.getenv("RELEASE_SAFETY_TASK_QUEUE", "release-safety-tq")
RELEASE_SAFETY_WORKFLOW_TYPE = "CanaryAnalysisWorkflow"
RELEASE_SAFETY_RESOLUTION_SIGNAL = "gateway_operation_resolved"


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


# ---------------------------------------------------------- Release Safety edge
#
# Two Activities that reach into another team's Temporal namespace. Both create
# their own Client, scoped to that namespace, inside the Activity. Workflow code
# never holds a client: it has to replay deterministically, and a network call
# cannot.


@activity.defn
async def start_canary_analysis(
    input: StartCanaryAnalysisInput,
) -> CanaryAnalysisStarted:
    """Hand a qualified release to Release Safety's canary platform.

    Started by id and workflow type name, not by importing their workflow class,
    because their code is not on this worker's path and should not be. This is
    also deliberately NOT a child workflow: a child shares its parent's namespace
    and is torn down with it, which would make canary look separated without
    being separated. The two workflows are peers, in different namespaces,
    correlated by the chain workflow_id carried in the input.

    The workflow id is shaped to be unmistakable in the Temporal UI's workflow
    list, so that switching namespaces during the demo visibly lands you in
    another team's system rather than in more of the same.
    """
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=RELEASE_SAFETY_NAMESPACE)
    canary_workflow_id = (
        f"release-safety::canary::{input.service}::{input.version}::"
        f"{uuid.uuid4().hex[:8]}"
    )
    handle = await client.start_workflow(
        RELEASE_SAFETY_WORKFLOW_TYPE,
        {
            "workflow_id": input.gateway_workflow_id,
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
            "scripted_outcome": input.scripted_outcome,
            "tick_seconds": input.tick_seconds,
            "window_ticks": input.window_ticks,
            "idempotency_key": input.idempotency_key,
            "origin_operation_id": input.origin_operation_id,
            "requester": input.requester,
            "gateway_namespace": input.gateway_namespace,
        },
        id=canary_workflow_id,
        task_queue=RELEASE_SAFETY_TASK_QUEUE,
    )
    activity.logger.info(
        "opened canary window %s in namespace %s",
        canary_workflow_id,
        RELEASE_SAFETY_NAMESPACE,
    )
    return CanaryAnalysisStarted(
        canary_workflow_id=canary_workflow_id,
        canary_run_id=handle.result_run_id or "",
        namespace=RELEASE_SAFETY_NAMESPACE,
        task_queue=RELEASE_SAFETY_TASK_QUEUE,
    )


@activity.defn
async def signal_release_safety_workflow(input: SignalReleaseSafetyInput) -> None:
    """Deliver a terminal operation result back to the durable Tool1 that asked.

    The counterpart of the pause. Canary stopped its own execution waiting for
    this, and this is what wakes it, whether the answer is approved, rejected, or
    timed out. It runs on the gateway's own worker, in the gateway's namespace,
    and reaches out to Release Safety's -- the gateway is the one that knows the
    decision, so the gateway is the one that delivers it.
    """
    namespace = input.callback_namespace or RELEASE_SAFETY_NAMESPACE
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=namespace)
    handle = client.get_workflow_handle(input.callback_workflow_id)
    await handle.signal(
        RELEASE_SAFETY_RESOLUTION_SIGNAL,
        {
            "operation_id": input.operation_id,
            "status": input.status,
            "result": input.result,
            "reason": input.reason,
        },
    )
    activity.logger.info(
        "notified %s that %s is %s",
        input.callback_workflow_id,
        input.operation_id,
        input.status,
    )
