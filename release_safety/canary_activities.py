from __future__ import annotations

import os
from typing import Any

import requests
from temporalio import activity
from temporalio.exceptions import ApplicationError

from release_safety.models import (
    CANARY_THRESHOLD,
    CanaryTickInput,
    PublishCanaryStateInput,
)

# Shared observability backend. Release Safety reads live traffic metrics from
# the same platform the deployment tooling reports into.
METRICS_URL = os.getenv("RELEASE_SAFETY_METRICS_URL", "http://mock-tool:9000/invoke")

# Both Activities here are this team's own business: reading metrics, and
# reporting progress for a dashboard. Nothing in this file talks to Agent
# Gateway. The two Activities that used to -- one holding a Temporal client for
# their namespace, one sending them a Signal -- are gone, replaced by Nexus calls
# the workflow makes directly. There is no longer any code in this package that
# needs credentials for someone else's cluster.

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
