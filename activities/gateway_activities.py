from __future__ import annotations

import os
from typing import Any

import requests
from temporalio import activity
from temporalio.exceptions import ApplicationError

from common.models import EvaluatePolicyInput, InvokeToolInput, PolicyDecision


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
