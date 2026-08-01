from __future__ import annotations

import os
from typing import Any

import requests
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from release_safety.models import (
    AgentGatewayPromoteInput,
    CANARY_THRESHOLD,
    CanaryTickInput,
    PublishCanaryStateInput,
    ReportCanaryVerdictInput,
)

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
# Where Agent Gateway's chain workflows live. A different namespace, run by a
# different team, which is the entire reason this file exists.
GATEWAY_NAMESPACE = os.getenv("AGENT_GATEWAY_NAMESPACE", "default")
# Shared observability backend. Release Safety reads live traffic metrics from
# the same platform the deployment tooling reports into.
METRICS_URL = os.getenv("RELEASE_SAFETY_METRICS_URL", "http://mock-tool:9000/invoke")

# The Update and Signal names Agent Gateway publishes. Strings, not imported
# symbols: this team does not have Agent Gateway's Python package on its path,
# and would not want it. These four names plus the payload shapes in models.py
# are the entire integration surface.
GATEWAY_NESTED_CALL_UPDATE = "request_nested_tool_call"
GATEWAY_CANARY_VERDICT_SIGNAL = "canary_verdict_reported"

# Scripted per-tick error rates, indexed by tick number. Fixed sequences rather
# than sampled noise, so the same demo run produces the same numbers every time
# and "watch the third tick" is a thing a presenter can actually say.
#
#   pass            every tick comfortably under threshold
#   fail            the first tick is already over, so the window dies at tick 1
#                   and Agent Gateway is never called at all
#   flaky_then_pass the first tick is over and the rest are clean. The window
#                   still FAILS, at tick 1, despite the name: one bad reading
#                   fails the window, it is not averaged out or retried past.
#                   Use it to show exactly that, not to show a recovery.
_TICK_SCHEDULE: dict[str, list[float]] = {
    "pass": [0.004, 0.006, 0.003, 0.002],
    "fail": [0.031, 0.0, 0.0, 0.0],
    "flaky_then_pass": [0.031, 0.006, 0.003, 0.002],
}


@activity.defn
def run_canary_tick(input: CanaryTickInput) -> dict[str, Any]:
    """Evaluate one canary tick against the error-rate threshold.

    Deterministic and scripted, because a demo that samples real noise cannot be
    rehearsed. In a real Release Safety platform this is where the query against
    the metrics store goes; the shape of the result is the same either way, and
    so is everything the workflow does with it.
    """
    rates = _TICK_SCHEDULE.get(input.scripted_outcome)
    if rates is None:
        raise ApplicationError(
            f"unknown scripted_outcome {input.scripted_outcome!r}; expected one "
            f"of {sorted(_TICK_SCHEDULE)}",
            non_retryable=True,
        )
    error_rate = rates[min(input.tick_number - 1, len(rates) - 1)]
    result = {
        "tick": input.tick_number,
        "error_rate": error_rate,
        "threshold": CANARY_THRESHOLD,
        "passed": error_rate <= CANARY_THRESHOLD,
    }
    activity.logger.info(
        "canary tick %s/%s error_rate=%s passed=%s",
        input.tick_number,
        input.window_ticks,
        error_rate,
        result["passed"],
    )
    return result


@activity.defn
async def call_agent_gateway_promote(
    input: AgentGatewayPromoteInput,
) -> dict[str, Any]:
    """The nested tool call: canary asks Agent Gateway to promote to production.

    This is the moment Tool1 calls Tool2 through the gateway, and both are real
    systems. Canary does not promote anything itself. It makes a governed request
    carrying the ORIGINAL chain's workflow_id, so the gateway resolves it onto the
    same user-visible task the operator started rather than opening a second one,
    which is the correlation requirement stated in the requirements document.

    The client is created here, inside the Activity, and points at the gateway's
    namespace. Workflow code never holds a client: it has to stay deterministic
    and replay-safe, and a network call from workflow code is neither.

    The requester is threaded through from whoever asked for the release. Canary
    is acting on their behalf, and the gateway checks a nested call against the
    chain's owner, so a canary that invented its own principal here would simply
    be refused. Its own identity travels alongside, in caller_service and in the
    call path, so the ledger records both who authorized the release and which
    system actually made the call.
    """
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=input.gateway_namespace)
    handle = client.get_workflow_handle(input.gateway_workflow_id)
    request = {
        "tool1_name": "release_safety_canary",
        "tool1_arguments": {
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
            "canary_workflow_id": input.canary_workflow_id,
        },
        "tool2_name": "promote_release",
        "tool2_arguments": {
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
        },
        "idempotency_key": input.idempotency_key,
        "correlation": {
            "workflow_id": input.gateway_workflow_id,
            "workflow_id_source": "explicit_authorized",
            "idempotency_key": input.idempotency_key,
            "caller_principal": input.requester,
            "caller_service": "release-safety",
            "runtime": "temporal",
            "call_path": ["release-safety", "release_safety_canary"],
        },
        "requested_action": (
            f"Promote {input.service} {input.version} to {input.environment}"
        ),
        "justification": input.justification,
        "controlled_tool1": True,
        "replay_safe": True,
        # Canary is durable and wants the answer delivered, not polled for.
        "callback_workflow_id": input.canary_workflow_id,
        "callback_namespace": os.getenv("TEMPORAL_NAMESPACE", "release-safety"),
        "origin_operation_id": input.origin_operation_id,
    }
    response = await handle.execute_update(GATEWAY_NESTED_CALL_UPDATE, request)
    activity.logger.info(
        "agent gateway answered %s for %s",
        response.get("status"),
        input.gateway_workflow_id,
    )
    return response


@activity.defn
async def report_canary_verdict(input: ReportCanaryVerdictInput) -> None:
    """Tell Agent Gateway that no promotion request is coming."""
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=input.gateway_namespace)
    handle = client.get_workflow_handle(input.gateway_workflow_id)
    await handle.signal(
        GATEWAY_CANARY_VERDICT_SIGNAL,
        {
            "origin_operation_id": input.origin_operation_id,
            "canary_workflow_id": input.canary_workflow_id,
            "verdict": input.verdict,
            "reason": input.reason,
            "detail": input.detail,
        },
    )


@activity.defn
def publish_canary_state(input: PublishCanaryStateInput) -> dict[str, Any]:
    """Report window progress to the shared observability backend."""
    resp = requests.post(
        METRICS_URL,
        json={
            "tool_name": "record_canary_state",
            "arguments": {
                "canary_workflow_id": input.canary_workflow_id,
                "service": input.service,
                "version": input.version,
                "environment": input.environment,
                "phase": input.phase,
                "ticks_completed": input.ticks_completed,
                "window_ticks": input.window_ticks,
                "threshold": input.threshold,
                "tick_results": input.tick_results,
                "verdict": input.verdict,
                "gateway_operation_id": input.gateway_operation_id,
                "error": input.error,
            },
        },
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()
