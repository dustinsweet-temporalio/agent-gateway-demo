from __future__ import annotations

import asyncio
import shutil
import uuid

from temporalio import activity, workflow
from temporalio.client import WorkflowHandle, WorkflowUpdateFailedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from common.models import (
    AGENT_GATEWAY_APPROVAL_SIGNAL,
    AdkTemporalSessionCallback,
    ApprovalDecision,
    ApprovalResolution,
    AutonomousAgentInput,
    CancelOperation,
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
from workflows.autonomous_agent import AutonomousAgentWorkflow
from workflows.chain import AgenticChainWorkflow


TASK_QUEUE = "test-agent-gateway"
ACTIVITY_CALLS: list[tuple[str, dict]] = []


@workflow.defn
class ApprovalCallbackReceiverWorkflow:
    @workflow.init
    def __init__(self, session_id: str) -> None:
        self._session_id = session_id
        self._resolution: ApprovalResolution | None = None

    @workflow.run
    async def run(self, session_id: str) -> ApprovalResolution:
        await workflow.wait_condition(lambda: self._resolution is not None)
        assert self._resolution is not None
        return self._resolution

    @workflow.signal(name=AGENT_GATEWAY_APPROVAL_SIGNAL)
    def approval_resolved(self, resolution: ApprovalResolution) -> None:
        if resolution.adk_session_id == self._session_id:
            self._resolution = resolution


@activity.defn(name="evaluate_policy")
async def fake_evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    requires_approval = (
        input.tool_name == "promote_release"
        and str(input.arguments.get("environment", "")).lower() == "prod"
    )
    return PolicyDecision(
        requires_approval=requires_approval,
        reason=(
            "Promotion to prod requires human approval."
            if requires_approval
            else None
        ),
    )


@activity.defn(name="evaluate_policy")
async def slow_fake_evaluate_policy(
    input: EvaluatePolicyInput,
) -> PolicyDecision:
    await asyncio.sleep(0.2)
    return await fake_evaluate_policy(input)


@activity.defn(name="invoke_tool")
async def fake_invoke_tool(input: InvokeToolInput) -> dict:
    ACTIVITY_CALLS.append((input.tool_name, dict(input.arguments)))
    return {
        "tool_name": input.tool_name,
        "arguments": input.arguments,
        "executed": True,
    }


async def _environment() -> WorkflowEnvironment:
    temporal = shutil.which("temporal")
    assert temporal, "Temporal CLI is required for workflow tests"
    return await WorkflowEnvironment.start_local(
        dev_server_existing_path=temporal,
        dev_server_log_level="error",
    )


async def _wait_for_status(
    handle: WorkflowHandle,
    operation_id: str,
    expected: str,
    timeout: float = 5,
):
    async def poll():
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


def _correlation(workflow_id: str) -> CorrelationContext:
    return CorrelationContext(
        workflow_id=workflow_id,
        workflow_id_source="explicit_authorized",
        caller_principal="requester@example.com",
        runtime="ClaudeCode",
        call_path=["ClaudeCode", "promote_release"],
    )


def _tool_request(
    workflow_id: str,
    operation_id: str,
    idempotency_key: str,
    environment: str = "prod",
    timeout: float = 30,
) -> ToolCallRequest:
    arguments = {
        "service": "delivery-matching-service",
        "version": "2.3.1",
        "environment": environment,
    }
    return ToolCallRequest(
        tool_name="promote_release",
        arguments=arguments,
        safe_arguments=arguments,
        idempotency_key=idempotency_key,
        correlation=_correlation(workflow_id),
        approval_timeout_seconds=timeout,
        requested_action=f"Promote release to {environment}",
        operation_id=operation_id,
    )


def test_case1_simple_approval_rejection_timeout_cancel_and_dedup() -> None:
    async def run() -> None:
        ACTIVITY_CALLS.clear()
        async with await _environment() as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[AgenticChainWorkflow],
                activities=[fake_evaluate_policy, fake_invoke_tool],
            ):
                workflow_id = f"wf-case1-{uuid.uuid4().hex[:8]}"
                handle = await env.client.start_workflow(
                    AgenticChainWorkflow.run,
                    ChainInput(
                        workflow_id=workflow_id,
                        owner_principal="requester@example.com",
                    ),
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )

                unauthorized = _tool_request(
                    workflow_id,
                    "op-unauthorized",
                    "idem-unauthorized",
                    environment="staging",
                )
                unauthorized.correlation.caller_principal = (
                    "attacker@example.com"
                )
                try:
                    await handle.execute_update(
                        AgenticChainWorkflow.request_tool_call,
                        unauthorized,
                    )
                    raise AssertionError("unauthorized workflow access succeeded")
                except WorkflowUpdateFailedError:
                    pass

                immediate = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _tool_request(
                        workflow_id,
                        "op-immediate",
                        "idem-immediate",
                        environment="staging",
                    ),
                )
                assert immediate.status == "completed"
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "promote_release"
                ]

                ACTIVITY_CALLS.clear()
                waiting_request = _tool_request(
                    workflow_id,
                    "op-approved",
                    "idem-approved",
                )
                waiting = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    waiting_request,
                )
                assert waiting.status == "waiting_for_approval"
                assert ACTIVITY_CALLS == []

                duplicate = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    waiting_request,
                )
                assert duplicate.operation_id == waiting.operation_id

                await handle.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="op-approved",
                        approver="approver@example.com",
                    ),
                )
                approved = await _wait_for_status(
                    handle, "op-approved", "completed"
                )
                assert approved.result["executed"] is True
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "promote_release"
                ]

                rejected = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _tool_request(
                        workflow_id,
                        "op-rejected",
                        "idem-rejected",
                    ),
                )
                assert rejected.status == "waiting_for_approval"
                await handle.signal(
                    AgenticChainWorkflow.reject_operation,
                    ApprovalDecision(
                        operation_id="op-rejected",
                        approver="approver@example.com",
                        reason="change window closed",
                    ),
                )
                rejected = await _wait_for_status(
                    handle, "op-rejected", "rejected"
                )
                assert rejected.reason == "change window closed"

                expiring = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _tool_request(
                        workflow_id,
                        "op-expired",
                        "idem-expired",
                        timeout=0.1,
                    ),
                )
                assert expiring.status == "waiting_for_approval"
                expired = await _wait_for_status(
                    handle, "op-expired", "expired"
                )
                assert expired.reason == "approval_timeout"

                canceling = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _tool_request(
                        workflow_id,
                        "op-canceled",
                        "idem-canceled",
                    ),
                )
                assert canceling.status == "waiting_for_approval"
                await handle.signal(
                    AgenticChainWorkflow.cancel_operation,
                    CancelOperation(
                        operation_id="op-canceled",
                        canceled_by="requester@example.com",
                        reason="request withdrawn",
                    ),
                )
                canceled = await _wait_for_status(
                    handle, "op-canceled", "canceled"
                )
                assert canceled.reason == "request withdrawn"

                ledger = await handle.query(
                    AgenticChainWorkflow.get_ledger
                )
                events = {entry.event for entry in ledger}
                assert {
                    "approved",
                    "rejected",
                    "expired",
                    "canceled",
                    "dedup_hit",
                }.issubset(events)

    asyncio.run(run())


def _nested_request(
    workflow_id: str,
    key: str,
    *,
    controlled: bool,
    replay_safe: bool,
) -> NestedToolCallRequest:
    arguments = {
        "service": "delivery-matching-service",
        "version": "2.3.1",
        "environment": "prod",
    }
    return NestedToolCallRequest(
        tool1_name="release_orchestrator",
        tool1_arguments=arguments,
        tool2_name="promote_release",
        tool2_arguments=arguments,
        safe_tool1_arguments=arguments,
        safe_tool2_arguments=arguments,
        idempotency_key=key,
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal="requester@example.com",
            runtime="ClaudeCode",
            call_path=["ClaudeCode", "release_orchestrator"],
        ),
        requested_action="Promote nested release to prod",
        parent_operation_id=f"{key}-parent",
        nested_operation_id=f"{key}-child",
        controlled_tool1=controlled,
        replay_safe=replay_safe,
    )


def test_case2_controlled_resume_and_uncontrolled_fail_closed() -> None:
    async def run() -> None:
        ACTIVITY_CALLS.clear()
        async with await _environment() as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[AgenticChainWorkflow],
                activities=[fake_evaluate_policy, fake_invoke_tool],
            ):
                workflow_id = f"wf-case2-{uuid.uuid4().hex[:8]}"
                handle = await env.client.start_workflow(
                    AgenticChainWorkflow.run,
                    ChainInput(
                        workflow_id=workflow_id,
                        owner_principal="requester@example.com",
                    ),
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )

                controlled = _nested_request(
                    workflow_id,
                    "controlled",
                    controlled=True,
                    replay_safe=False,
                )
                waiting = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    controlled,
                )
                assert waiting.status == "waiting_for_approval"
                assert waiting.parent_operation_id == "controlled-parent"
                assert waiting.call_path == [
                    "ClaudeCode",
                    "release_orchestrator",
                    "promote_release",
                ]
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "release_orchestrator_prepare"
                ]
                await handle.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="controlled-child",
                        approver="approver@example.com",
                    ),
                )
                parent = await _wait_for_status(
                    handle, "controlled-parent", "completed"
                )
                assert parent.result["resumed_from_checkpoint"] is True
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "release_orchestrator_prepare",
                    "promote_release",
                    "release_orchestrator_resume",
                ]

                ACTIVITY_CALLS.clear()
                unsafe = _nested_request(
                    workflow_id,
                    "unsafe",
                    controlled=False,
                    replay_safe=False,
                )
                blocked = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    unsafe,
                )
                assert blocked.status == "blocked_nested_approval"
                assert blocked.retry_required is True
                await handle.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="unsafe-child",
                        approver="approver@example.com",
                    ),
                )
                approved = await _wait_for_status(
                    handle,
                    "unsafe-child",
                    "approved_retry_required",
                )
                assert approved.retry_required is True
                refused = await handle.execute_update(
                    AgenticChainWorkflow.resume_nested_tool_call,
                    ResumeNestedRequest(
                        operation_id="unsafe-child",
                        caller_principal="requester@example.com",
                    ),
                )
                assert refused.reason == "uncontrolled_tool_not_replay_safe"
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "release_orchestrator_prepare"
                ]

                ACTIVITY_CALLS.clear()
                replayable = _nested_request(
                    workflow_id,
                    "replayable",
                    controlled=False,
                    replay_safe=True,
                )
                await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    replayable,
                )
                await handle.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="replayable-child",
                        approver="approver@example.com",
                    ),
                )
                await _wait_for_status(
                    handle,
                    "replayable-child",
                    "approved_retry_required",
                )
                resumed = await handle.execute_update(
                    AgenticChainWorkflow.resume_nested_tool_call,
                    ResumeNestedRequest(
                        operation_id="replayable-child",
                        caller_principal="requester@example.com",
                    ),
                )
                assert resumed.status == "completed"
                assert resumed.result["replayed"] is True
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "release_orchestrator_prepare",
                    "release_orchestrator_prepare",
                    "promote_release",
                    "release_orchestrator_resume",
                ]

    asyncio.run(run())


def _autonomous_input(
    workflow_id: str,
    operation_id: str,
    timeout: float = 30,
) -> AutonomousAgentInput:
    arguments = {
        "service": "delivery-matching-service",
        "version": "2.3.1",
        "environment": "prod",
    }
    return AutonomousAgentInput(
        workflow_id=workflow_id,
        operation_id=operation_id,
        idempotency_key=f"idem-{operation_id}",
        owner_principal="release-agent@google-adk",
        agent_run_id=f"run-{operation_id}",
        agent_identity="release-agent@google-adk",
        tool_name="promote_release",
        arguments=arguments,
        safe_arguments=arguments,
        requested_action="Autonomously promote release to prod",
        justification="validated rollout",
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="inferred_from_agent_run",
            agent_run_id=f"run-{operation_id}",
            caller_principal="release-agent@google-adk",
            runtime="GoogleADKAgent",
            call_path=[
                "GoogleADKAgent",
                "AgentGateway",
                "promote_release",
            ],
        ),
        approval_timeout_seconds=timeout,
    )


def test_case3_autonomous_checkpoint_resume_reject_and_expire() -> None:
    async def wait_until_waiting(handle: WorkflowHandle, operation_id: str):
        return await _wait_for_status(
            handle, operation_id, "waiting_for_approval"
        )

    async def run() -> None:
        ACTIVITY_CALLS.clear()
        async with await _environment() as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[
                    AutonomousAgentWorkflow,
                    ApprovalCallbackReceiverWorkflow,
                ],
                activities=[fake_evaluate_policy, fake_invoke_tool],
            ):
                callback_workflow_id = (
                    f"wf-adk-session-{uuid.uuid4().hex[:8]}"
                )
                callback_handle = await env.client.start_workflow(
                    ApprovalCallbackReceiverWorkflow.run,
                    "adk-session-approved",
                    id=callback_workflow_id,
                    task_queue=TASK_QUEUE,
                )
                workflow_id = f"wf-case3-{uuid.uuid4().hex[:8]}"
                approved_input = _autonomous_input(
                    workflow_id,
                    "op-agent-approved",
                )
                approved_input.callback = AdkTemporalSessionCallback(
                    workflow_id=callback_workflow_id,
                    run_id=callback_handle.run_id,
                    session_id="adk-session-approved",
                )
                handle = await env.client.start_workflow(
                    AutonomousAgentWorkflow.run,
                    approved_input,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                waiting = await wait_until_waiting(
                    handle, "op-agent-approved"
                )
                assert waiting.status == "waiting_for_approval"
                assert ACTIVITY_CALLS == []
                checkpoint = await handle.query(
                    AutonomousAgentWorkflow.get_workflow_status
                )
                assert checkpoint.checkpoint["step"] == "waiting_for_approval"
                assert (
                    checkpoint.checkpoint["dependent_action_executed"] is False
                )

                await handle.signal(
                    AutonomousAgentWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="op-agent-approved",
                        approver="approver@example.com",
                    ),
                )
                completed = await handle.result()
                assert completed.operations[0].status == "completed"
                assert completed.dependent_action_executed is True
                assert [name for name, _ in ACTIVITY_CALLS] == [
                    "promote_release",
                    "record_autonomous_followup",
                ]
                callback = await asyncio.wait_for(
                    callback_handle.result(),
                    timeout=5,
                )
                assert callback.gateway_workflow_id == workflow_id
                assert callback.operation_id == "op-agent-approved"
                assert callback.status == "completed"
                assert callback.adk_session_id == "adk-session-approved"
                assert callback.result == completed.operations[0].result

                ACTIVITY_CALLS.clear()
                rejected_id = f"wf-case3-reject-{uuid.uuid4().hex[:8]}"
                rejected_handle = await env.client.start_workflow(
                    AutonomousAgentWorkflow.run,
                    _autonomous_input(rejected_id, "op-agent-rejected"),
                    id=rejected_id,
                    task_queue=TASK_QUEUE,
                )
                await wait_until_waiting(
                    rejected_handle, "op-agent-rejected"
                )
                await rejected_handle.signal(
                    AutonomousAgentWorkflow.reject_operation,
                    ApprovalDecision(
                        operation_id="op-agent-rejected",
                        approver="approver@example.com",
                        reason="risk too high",
                    ),
                )
                rejected = await rejected_handle.result()
                assert rejected.operations[0].status == "rejected"
                assert rejected.dependent_action_executed is False
                assert ACTIVITY_CALLS == []

                expired_id = f"wf-case3-expire-{uuid.uuid4().hex[:8]}"
                expired_handle = await env.client.start_workflow(
                    AutonomousAgentWorkflow.run,
                    _autonomous_input(
                        expired_id,
                        "op-agent-expired",
                        timeout=0.1,
                    ),
                    id=expired_id,
                    task_queue=TASK_QUEUE,
                )
                expired = await expired_handle.result()
                assert expired.operations[0].status == "expired"
                assert expired.dependent_action_executed is False
                assert ACTIVITY_CALLS == []

    asyncio.run(run())


def test_case3_cancel_during_policy_evaluation_stops_the_run() -> None:
    async def run() -> None:
        ACTIVITY_CALLS.clear()
        async with await _environment() as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[AutonomousAgentWorkflow],
                activities=[slow_fake_evaluate_policy, fake_invoke_tool],
            ):
                workflow_id = f"wf-case3-cancel-{uuid.uuid4().hex[:8]}"
                operation_id = "op-agent-canceled"
                handle = await env.client.start_workflow(
                    AutonomousAgentWorkflow.run,
                    _autonomous_input(workflow_id, operation_id),
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                await handle.signal(
                    AutonomousAgentWorkflow.cancel_operation,
                    CancelOperation(
                        operation_id=operation_id,
                        canceled_by="release-agent@google-adk",
                        reason="request withdrawn",
                    ),
                )
                canceled = await asyncio.wait_for(
                    handle.result(),
                    timeout=5,
                )

                assert canceled.operations[0].status == "canceled"
                assert canceled.dependent_action_executed is False
                assert ACTIVITY_CALLS == []

    asyncio.run(run())
