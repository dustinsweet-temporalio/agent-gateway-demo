"""The company-wide security mandate toggle, and the one thing it changes.

The mandate is the in-story policy that lands between the two acts of the CASE-2
walkthrough: no engineering team promotes its own service to production on its
own say-so any more, so while it is in force, Security approves every production
release. Mechanically it is a single runtime flag on the gateway process that
changes *when* an Operation is created with required_approver_team="security".

It changes nothing about how that restriction is enforced. There is exactly one
authorization mechanism -- Operation.required_approver_team, checked by
gateway.server._authorize_approver at the HTTP boundary and again by
AgenticChainWorkflow._approver_authorized on the durable side -- and these tests
assert against that same mechanism rather than a second one.

Four claims:

  default off        a fresh gateway process restricts nothing, so CASE-1 as it
                     was demoed is unchanged
  scope              on, a production promotion is Security's whoever asked; a
                     staging one is nobody's in particular
  grandfathering     the value is snapshotted onto the request, so flipping the
                     toggle does not reach back into operations already queued
  end to end         the Act One shape: mandate on, uncontrolled caller gone,
                     Waypoint refused, Security approves, promotion lands
"""

from __future__ import annotations

import asyncio
import copy
import shutil
import uuid

from temporalio import activity
from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import activities.gateway_activities as gateway_activities
from activities.gateway_activities import await_quality_gate
from common.models import (
    ApprovalDecision,
    ChainInput,
    CorrelationContext,
    EvaluatePolicyInput,
    InvokeToolInput,
    NestedToolCallRequest,
    PolicyDecision,
    ResumeNestedRequest,
    ToolCallRequest,
    ToolCallResponse,
)
from gateway.server import (
    MANDATE_TOGGLE,
    _MandateToggle,
    _render_mandate_bar,
)
from mock_tool import server as backend
from tests import release_step_fakes
from tests.release_step_fakes import RELEASE_STEP_ACTIVITIES
from workflows.chain import AgenticChainWorkflow, _required_approver_team_for
from workflows.release_children import (
    CutReleaseChildWorkflow,
    PromoteReleaseChildWorkflow,
    QualityGateChildWorkflow,
)

TASK_QUEUE = "test-security-mandate"
SERVICE = "delivery-matching-service"
PRINCIPAL = "requester@example.com"
WAYPOINT_APPROVER = "Dustin Sweet <dustin.sweet@quickmeals.com>"
SECURITY_APPROVER = "Abe Roover <abe.roover@quickmeals.com>"

_PRISTINE_DEPLOYED = copy.deepcopy(backend._deployed)
_PRISTINE_RELEASES = copy.deepcopy(backend._releases)
_IDLE_GATES = copy.deepcopy(backend._quality_gates)


def _reset_backend() -> None:
    release_step_fakes.reset()
    backend._deployed.clear()
    backend._deployed.update(copy.deepcopy(_PRISTINE_DEPLOYED))
    backend._releases.clear()
    backend._releases.update(copy.deepcopy(_PRISTINE_RELEASES))
    backend._seen.clear()
    backend._quality_gates = copy.deepcopy(_IDLE_GATES)


# ------------------------------------------------------- the assignment rule


def test_the_mandate_is_off_until_somebody_turns_it_on() -> None:
    """A gateway process that has never been touched restricts nothing.

    This is what grandfathers CASE-1: it was demoed before the mandate existed,
    and a fresh gateway behaves exactly the way it behaved then.
    """
    assert MANDATE_TOGGLE.is_on() is False
    assert _required_approver_team_for(None, "prod", security_mandate=False) is None


def test_the_mandate_covers_production_promotions_whoever_asked() -> None:
    fresh = _MandateToggle()
    assert fresh.is_on() is False
    assert fresh.set(True) is True
    assert fresh.is_on() is True

    # On: production is Security's call, from any caller and from none.
    for caller in (None, "", "waypoint", "google-adk-agent"):
        assert (
            _required_approver_team_for(caller, "prod", security_mandate=True)
            == "security"
        )
    # Both spellings PROTECTED_ENVIRONMENTS recognises, and case-insensitively:
    # a mandate that missed one of them would be worse than no mandate.
    assert (
        _required_approver_team_for(None, "PRODUCTION", security_mandate=True)
        == "security"
    )

    # Off again, and the same call is unrestricted.
    assert fresh.set(False) is False
    assert _required_approver_team_for("waypoint", "prod", False) is None


def test_the_mandate_does_not_reach_anything_but_production() -> None:
    """Staging promotions and reads are untouched, mandate or no mandate.

    The mandate is about releasing to customers. A pipeline's own staging hop is
    not that, and gating it would put a Security approval in the middle of a flow
    that is supposed to run unattended.
    """
    for environment in ("staging", "", "dev"):
        assert (
            _required_approver_team_for(None, environment, security_mandate=True)
            is None
        )


def test_a_security_requested_promotion_is_restricted_with_or_without_it() -> None:
    """The pre-existing rule is untouched: the mandate was added beside it.

    A promotion the Security team's own scan asked for is theirs to stand behind
    because of who asked, not because of any company policy, so it carries the
    same restriction with the toggle off.
    """
    assert (
        _required_approver_team_for("security", "prod", security_mandate=False)
        == "security"
    )
    assert (
        _required_approver_team_for("security", "prod", security_mandate=True)
        == "security"
    )


def test_the_dashboard_states_the_mandate_rather_than_announcing_it() -> None:
    """Persistent label, not a toast.

    The walkthrough points at this several minutes after flipping it, so both
    states have to be legible from the page alone.
    """
    off = _render_mandate_bar(False)
    assert "Mandate: OFF" in off
    # Off shows the control that turns it on, and nothing else.
    assert 'value="on"' in off and 'value="off"' not in off

    on = _render_mandate_bar(True)
    assert "Mandate: ON" in on
    assert "Security-team approval" in on
    assert 'value="off"' in on and 'value="on"' not in on


# ------------------------------------------------------------ live behavior


@activity.defn(name="evaluate_policy")
async def fake_evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    requires_approval = (
        input.tool_name == "promote_release"
        and str(input.arguments.get("environment", "")).lower() == "prod"
    )
    return PolicyDecision(
        requires_approval=requires_approval,
        reason="Promotion to prod requires human approval."
        if requires_approval
        else None,
    )


@activity.defn(name="invoke_tool")
async def backend_invoke_tool(input: InvokeToolInput) -> dict:
    return release_step_fakes.backend_call(
        input.tool_name, dict(input.arguments), input.idempotency_key
    )


async def _environment() -> WorkflowEnvironment:
    temporal = shutil.which("temporal")
    assert temporal, "Temporal CLI is required for workflow tests"
    env = await WorkflowEnvironment.start_local(
        dev_server_existing_path=temporal,
        dev_server_log_level="error",
    )
    gateway_activities.TEMPORAL_ADDRESS = (
        env.client.service_client.config.target_host
    )
    gateway_activities.TEMPORAL_NAMESPACE = env.client.namespace
    return env


def _worker(env: WorkflowEnvironment) -> Worker:
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[
            AgenticChainWorkflow,
            CutReleaseChildWorkflow,
            PromoteReleaseChildWorkflow,
            QualityGateChildWorkflow,
        ],
        activities=[
            fake_evaluate_policy,
            backend_invoke_tool,
            await_quality_gate,
            *RELEASE_STEP_ACTIVITIES,
        ],
    )


async def _start_chain(env: WorkflowEnvironment, name: str) -> WorkflowHandle:
    workflow_id = f"wf-{name}-{uuid.uuid4().hex[:8]}"
    return await env.client.start_workflow(
        AgenticChainWorkflow.run,
        ChainInput(workflow_id=workflow_id, owner_principal=PRINCIPAL),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )


def _direct_request(
    workflow_id: str,
    operation_id: str,
    environment: str,
    *,
    security_mandate: bool,
) -> ToolCallRequest:
    """What gateway.server._submit_tool_call builds for a CASE-1 promotion."""
    arguments = {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": environment,
    }
    return ToolCallRequest(
        tool_name="promote_release",
        arguments=arguments,
        safe_arguments=dict(arguments),
        idempotency_key=f"idem-{operation_id}",
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal=PRINCIPAL,
            runtime="ClaudeCode",
            call_path=["ClaudeCode", "promote_release"],
        ),
        approval_timeout_seconds=60,
        requested_action=f"Promote {SERVICE} 2.3.0 to {environment}",
        operation_id=operation_id,
        security_mandate=security_mandate,
    )


def _legacy_scan_request(
    workflow_id: str, key: str, *, security_mandate: bool
) -> NestedToolCallRequest:
    """What the legacy scan script's run_nested_release call turns into.

    Uncontrolled and replay-safe, which is the script's honest self-description:
    it cannot hold the pause, but re-running a scan is safe -- it just costs all
    four stages again.
    """
    arguments = {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": "prod",
    }
    return NestedToolCallRequest(
        tool1_name="release_orchestrator",
        tool1_arguments=dict(arguments),
        tool2_name="promote_release",
        tool2_arguments=dict(arguments),
        safe_tool1_arguments=dict(arguments),
        safe_tool2_arguments=dict(arguments),
        idempotency_key=key,
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal=PRINCIPAL,
            runtime="ClaudeCode",
            call_path=["ClaudeCode", "release_orchestrator"],
        ),
        requested_action=f"Promote {SERVICE} 2.3.0 to prod",
        parent_operation_id=f"{key}-parent",
        nested_operation_id=f"{key}-child",
        controlled_tool1=False,
        replay_safe=True,
        security_mandate=security_mandate,
    )


async def _operation_view(handle: WorkflowHandle, operation_id: str):
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    for op in summary.operations:
        if op.operation_id == operation_id:
            return op
    raise AssertionError(f"{operation_id} not found in workflow status")


async def _ledger_events(handle: WorkflowHandle) -> set[str]:
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    return {entry.event for entry in ledger}


async def _wait_for_status(
    handle: WorkflowHandle,
    operation_id: str,
    expected: str,
    timeout: float = 15,
) -> ToolCallResponse:
    async def poll() -> ToolCallResponse:
        while True:
            response = await handle.query(
                "get_operation_status",
                operation_id,
                result_type=ToolCallResponse,
            )
            if response.status == expected:
                return response
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(poll(), timeout=timeout)


def test_the_mandate_end_to_end() -> None:
    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                await _mandate_off_leaves_production_unrestricted(env)
                await _mandate_on_makes_production_the_security_teams_call(env)
                await _mandate_on_does_not_reach_a_staging_promotion(env)
                await _act_one_shape_end_to_end(env)

    asyncio.run(run())


async def _mandate_off_leaves_production_unrestricted(
    env: WorkflowEnvironment,
) -> None:
    """CASE-1 exactly as it was demoed. The toggle is the only difference."""
    _reset_backend()
    handle = await _start_chain(env, "mandate-off")

    waiting = await handle.execute_update(
        AgenticChainWorkflow.request_tool_call,
        _direct_request(handle.id, "op-off", "prod", security_mandate=False),
    )
    assert waiting.status == "waiting_for_approval"
    assert (await _operation_view(handle, "op-off")).required_approver_team is None

    # An approver with no team at all still decides it.
    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="op-off", approver="approver@demo", approver_team=""
        ),
    )
    completed = await _wait_for_status(handle, "op-off", "completed")
    assert completed.result["status"] == "completed"
    assert backend._deployed["prod"]["version"] == "2.3.0"


async def _mandate_on_makes_production_the_security_teams_call(
    env: WorkflowEnvironment,
) -> None:
    """The whole behavioral change, on the plainest path there is.

    Nobody from Security is anywhere near this call. It is the Waypoint team
    promoting its own service, exactly as in CASE-1, and the mandate is the only
    reason it now needs Security.
    """
    _reset_backend()
    handle = await _start_chain(env, "mandate-on")

    waiting = await handle.execute_update(
        AgenticChainWorkflow.request_tool_call,
        _direct_request(handle.id, "op-on", "prod", security_mandate=True),
    )
    assert waiting.status == "waiting_for_approval"
    view = await _operation_view(handle, "op-on")
    assert view.required_approver_team == "security"
    # The query gateway.server._authorize_approver runs before signalling. Same
    # query, same answer, no second mechanism.
    assert (
        await handle.query(
            AgenticChainWorkflow.get_required_approver_team,
            "op-on",
            result_type=str,
        )
        == "security"
    )

    # Waypoint's own approver, who could have decided this yesterday, is refused
    # now. The operation stays waiting rather than being quietly dropped.
    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="op-on",
            approver=WAYPOINT_APPROVER,
            approver_team="waypoint",
        ),
    )
    await asyncio.sleep(0.5)
    still_waiting = await _operation_view(handle, "op-on")
    assert still_waiting.status == "waiting_for_approval"
    assert "approval_refused_wrong_team" in await _ledger_events(handle)
    assert backend._deployed["prod"]["version"] == "2.2.0"

    # Turning the mandate off now does not un-restrict what is already queued:
    # the value the operation was created with is in Event History, and this
    # operation was created under the mandate.
    was_on = MANDATE_TOGGLE.is_on()
    try:
        MANDATE_TOGGLE.set(False)
        assert (
            await _operation_view(handle, "op-on")
        ).required_approver_team == "security"
    finally:
        MANDATE_TOGGLE.set(was_on)

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="op-on",
            approver=SECURITY_APPROVER,
            approver_team="security",
        ),
    )
    completed = await _wait_for_status(handle, "op-on", "completed")
    assert completed.result["status"] == "completed"
    assert backend._deployed["prod"]["version"] == "2.3.0"


async def _mandate_on_does_not_reach_a_staging_promotion(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "mandate-staging")

    response = await handle.execute_update(
        AgenticChainWorkflow.request_tool_call,
        _direct_request(handle.id, "op-stg", "staging", security_mandate=True),
    )
    # Unprotected to begin with, so it runs without an approval at all, and it
    # carries no team restriction for anyone to have been refused by.
    assert response.status == "completed"
    assert (await _operation_view(handle, "op-stg")).required_approver_team is None


async def _act_one_shape_end_to_end(env: WorkflowEnvironment) -> None:
    """The walkthrough's Act One, minus the crash.

    A caller that cannot hold a pause asks for a production promotion while the
    mandate is on. The caller is gone by the time anyone looks at the queue; the
    request is not. Waypoint cannot clear it, Security can, and because the
    caller is uncontrolled a human still has to come back and finish it by hand.
    """
    _reset_backend()
    handle = await _start_chain(env, "mandate-legacy")

    blocked = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _legacy_scan_request(handle.id, "legacy-scan", security_mandate=True),
    )
    assert blocked.status == "blocked_nested_approval"
    child_id = blocked.operation_id
    assert (
        await _operation_view(handle, child_id)
    ).required_approver_team == "security"

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id=child_id,
            approver=WAYPOINT_APPROVER,
            approver_team="waypoint",
        ),
    )
    await asyncio.sleep(0.5)
    assert (
        await _operation_view(handle, child_id)
    ).status == "waiting_for_approval"
    assert backend._deployed["prod"]["version"] == "2.2.0"

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id=child_id,
            approver=SECURITY_APPROVER,
            approver_team="security",
        ),
    )
    # Approved, and still not executed: the caller that asked is gone, so the
    # gateway will not run the protected step on its own.
    await _wait_for_status(handle, child_id, "approved_retry_required")
    assert backend._deployed["prod"]["version"] == "2.2.0"

    # A human finishes it, which is the whole cost of an uncontrolled caller.
    resumed = await handle.execute_update(
        AgenticChainWorkflow.resume_nested_tool_call,
        ResumeNestedRequest(operation_id=child_id, caller_principal=PRINCIPAL),
    )
    assert resumed.status == "completed"
    assert backend._deployed["prod"]["version"] == "2.3.0"
