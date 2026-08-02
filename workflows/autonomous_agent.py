from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta, timezone, datetime

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from activities.gateway_activities import evaluate_policy, invoke_tool
    from common.models import (
        AGENT_GATEWAY_APPROVAL_SIGNAL,
        AdkTemporalSessionCallback,
        AgentRunSummary,
        ApprovalDecision,
        ApprovalResolution,
        AutonomousAgentInput,
        CancelOperation,
        EvaluatePolicyInput,
        InvokeToolInput,
        LedgerEntry,
        Operation,
        OperationStatus,
        OperationView,
        PromoteReleaseInput,
        ToolCallResponse,
    )
    from workflows.release_children import PromoteReleaseChildWorkflow


POLICY_TIMEOUT = timedelta(seconds=10)
INVOKE_TIMEOUT = timedelta(seconds=60)
RELEASE_CHILD_TIMEOUT = timedelta(minutes=5)


@workflow.defn
class AutonomousAgentWorkflow:
    """Durable Google ADK-style autonomous run with a gateway-owned gate.

    The agent's local checkpoint is workflow state. No protected or dependent
    external mutation happens while approval is pending. Approval decisions are
    Signals from Agent Gateway; the agent workflow never decides for itself.
    """

    @workflow.init
    def __init__(self, input: AutonomousAgentInput) -> None:
        self._input = input
        self._operation = Operation(
            operation_id=input.operation_id,
            tool_name=input.tool_name,
            arguments=input.arguments,
            safe_arguments=input.safe_arguments,
            idempotency_key=input.idempotency_key,
            status=OperationStatus.EVALUATING,
            requester=input.owner_principal,
            requested_action=input.requested_action,
            justification=input.justification,
            approval_timeout_seconds=input.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            call_path=input.correlation.call_path
            or [input.agent_identity, "AgentGateway", input.tool_name],
            workflow_id_source=input.correlation.workflow_id_source,
        )
        self._ledger: list[LedgerEntry] = []
        self._checkpoint = {
            "step": "starting",
            "next_step": "evaluate_gateway_policy",
            "protected_action_executed": False,
            "dependent_action_executed": False,
        }
        self._dependent_action_executed = False
        self._protected_result = None
        self._callback = input.callback
        self._callback_notified = False

    @workflow.run
    async def run(self, input: AutonomousAgentInput) -> AgentRunSummary:
        self._log(
            "agent_run_started",
            {
                "agent_run_id": input.agent_run_id,
                "agent_identity": input.agent_identity,
                "workflow_id_source": input.correlation.workflow_id_source,
                "call_path": self._operation.call_path,
            },
        )
        try:
            decision = await workflow.execute_activity(
                evaluate_policy,
                EvaluatePolicyInput(
                    tool_name=input.tool_name,
                    arguments=input.arguments,
                    caller_principal=input.owner_principal,
                ),
                start_to_close_timeout=POLICY_TIMEOUT,
            )
        except ActivityError as err:
            self._fail(f"policy evaluation failed: {err}", "policy_failed")
            return await self._finish()

        self._operation.risk_reason = decision.reason
        self._operation.protected = decision.requires_approval
        self._log(
            "policy_evaluated",
            {"requires_approval": decision.requires_approval},
        )
        if self._operation.status == OperationStatus.CANCELED:
            return await self._finish()

        if decision.requires_approval:
            self._operation.status = OperationStatus.WAITING_FOR_APPROVAL
            self._operation.deadline_epoch = (
                workflow.now().timestamp() + input.approval_timeout_seconds
            )
            self._operation.deadline_iso = self._iso(
                self._operation.deadline_epoch
            )
            self._checkpoint = {
                "step": "waiting_for_approval",
                "next_step": "invoke_protected_action",
                "protected_action_executed": False,
                "dependent_action_executed": False,
            }
            self._log(
                "agent_checkpointed",
                {
                    "checkpoint": self._checkpoint,
                    "deadline_iso": self._operation.deadline_iso,
                },
            )
            try:
                await workflow.wait_condition(
                    lambda: self._operation.status
                    != OperationStatus.WAITING_FOR_APPROVAL,
                    timeout=timedelta(
                        seconds=input.approval_timeout_seconds
                    ),
                )
            except asyncio.TimeoutError:
                if (
                    self._operation.status
                    == OperationStatus.WAITING_FOR_APPROVAL
                ):
                    self._operation.status = OperationStatus.EXPIRED
                    self._operation.error = "approval_timeout"
                    self._operation.decided_iso = workflow.now().isoformat()
                    self._checkpoint = {
                        **self._checkpoint,
                        "step": "expired",
                        "next_step": None,
                    }
                    self._log("expired")

            if self._operation.status in {
                OperationStatus.REJECTED,
                OperationStatus.EXPIRED,
                OperationStatus.CANCELED,
            }:
                return await self._finish()

        await self._invoke_protected_action()
        if (
            self._operation.status == OperationStatus.FAILED
            or self._protected_result is None
        ):
            return await self._finish()
        await self._run_dependent_action()
        return await self._finish()

    @workflow.signal
    def register_callback(
        self,
        callback: AdkTemporalSessionCallback,
    ) -> None:
        """Attach a callback to an existing idempotent agent run."""

        if self._callback is None:
            self._callback = callback
            self._log(
                "adk_session_callback_registered",
                {
                    "workflow_id": callback.workflow_id,
                    "session_id": callback.session_id,
                },
            )
        elif self._callback != callback:
            self._log(
                "adk_session_callback_registration_ignored",
                {"reason": "callback_already_registered"},
            )

    @workflow.signal
    def approve_operation(self, decision: ApprovalDecision) -> None:
        if (
            decision.operation_id != self._operation.operation_id
            or self._operation.status
            != OperationStatus.WAITING_FOR_APPROVAL
        ):
            return
        if (
            self._operation.deadline_epoch is not None
            and workflow.now().timestamp()
            >= self._operation.deadline_epoch
        ):
            self._operation.status = OperationStatus.EXPIRED
            self._operation.error = "approval_timeout"
            self._operation.decided_iso = workflow.now().isoformat()
            self._checkpoint = {
                **self._checkpoint,
                "step": "expired",
                "next_step": None,
            }
            self._log(
                "late_approval_ignored",
                {"approver": decision.approver},
            )
            return
        self._operation.status = OperationStatus.APPROVED
        self._operation.approver = decision.approver
        self._operation.decided_iso = workflow.now().isoformat()
        self._checkpoint = {
            **self._checkpoint,
            "step": "approved",
            "next_step": "invoke_protected_action",
        }
        self._log("approved", {"approver": decision.approver})

    @workflow.signal
    def reject_operation(self, decision: ApprovalDecision) -> None:
        if (
            decision.operation_id != self._operation.operation_id
            or self._operation.status
            != OperationStatus.WAITING_FOR_APPROVAL
        ):
            return
        self._operation.status = OperationStatus.REJECTED
        self._operation.approver = decision.approver
        self._operation.error = decision.reason
        self._operation.decided_iso = workflow.now().isoformat()
        self._checkpoint = {
            **self._checkpoint,
            "step": "rejected",
            "next_step": None,
        }
        self._log(
            "rejected",
            {"approver": decision.approver, "reason": decision.reason},
        )

    @workflow.signal
    def cancel_operation(self, cancellation: CancelOperation) -> None:
        if (
            cancellation.operation_id != self._operation.operation_id
            or self._operation.status
            not in {
                OperationStatus.EVALUATING,
                OperationStatus.WAITING_FOR_APPROVAL,
                OperationStatus.APPROVED,
            }
        ):
            return
        self._operation.status = OperationStatus.CANCELED
        self._operation.approver = cancellation.canceled_by
        self._operation.error = cancellation.reason
        self._operation.decided_iso = workflow.now().isoformat()
        self._checkpoint = {
            **self._checkpoint,
            "step": "canceled",
            "next_step": None,
        }
        self._log(
            "canceled",
            {
                "canceled_by": cancellation.canceled_by,
                "reason": cancellation.reason,
            },
        )

    @workflow.query
    def get_operation_status(self, operation_id: str) -> ToolCallResponse:
        if operation_id != self._operation.operation_id:
            return ToolCallResponse(
                status="not_found",
                workflow_id=self._input.workflow_id,
                operation_id=operation_id,
            )
        return self._response()

    @workflow.query
    def get_operation_result(self, operation_id: str) -> ToolCallResponse:
        return self.get_operation_status(operation_id)

    @workflow.query
    def get_workflow_status(self) -> AgentRunSummary:
        return self._summary()

    @workflow.query
    def get_ledger(self) -> list[LedgerEntry]:
        return list(self._ledger)

    @workflow.query
    def get_workflow_owner(self) -> str:
        return self._input.owner_principal

    @workflow.query
    def get_required_approver_team(self, operation_id: str) -> str:
        """No team restriction ever applies to an autonomous agent run.

        Declared so the gateway can ask any workflow behind an approval queue
        entry the same question without first working out which type it is. A
        CASE-3 run promotes directly and is never gated by another QuickMeals
        team's check, so there is no team whose approval it specifically needs;
        the ordinary GATEWAY_APPROVERS membership check is the whole rule here.

        Note this is about who may APPROVE. The ADK release agent
        (release-agent@google-adk) is a requester and only ever a requester: it
        is not in GATEWAY_APPROVERS, so it cannot approve anything, its own runs
        included.
        """
        return ""

    async def _invoke_protected_action(self) -> None:
        if self._operation.status in {
            OperationStatus.REJECTED,
            OperationStatus.EXPIRED,
            OperationStatus.CANCELED,
        }:
            return
        self._operation.status = OperationStatus.INVOKING
        self._checkpoint = {
            **self._checkpoint,
            "step": "invoking_protected_action",
            "next_step": "run_dependent_action",
        }
        self._log("invoking")
        try:
            result = await self._execute_protected_step()
        except (ActivityError, ChildWorkflowError) as err:
            self._fail(str(err), "protected_action_failed")
            return
        # A promotion whose new instances failed their health check comes back
        # as a completed workflow reporting a failed status rather than as a
        # thrown error, because the promotion machinery worked: it declined to
        # route traffic. The run still has to fail, or the dependent action
        # would run off the back of a promotion that never went live.
        if isinstance(result, dict) and result.get("status") == "failed":
            self._fail(
                str(result.get("reason") or result.get("message") or "step failed"),
                "protected_action_failed",
            )
            return
        self._protected_result = result
        self._operation.result = {"protected_action": result}
        self._checkpoint = {
            **self._checkpoint,
            "step": "protected_action_completed",
            "next_step": "run_dependent_action",
            "protected_action_executed": True,
        }
        self._log("protected_action_completed")

    async def _execute_protected_step(self) -> dict:
        """Run the approved action, in whichever shape that action has.

        A promotion is a Child Workflow here for the same reason it is one in the
        chain: it is four real steps -- deploy, health check, route, note -- and
        an operator watching an autonomous run stall wants to see which one. The
        same workflow type as CASE-1 and CASE-2a use, so a promotion means the
        same thing however it was requested.

        Nothing about the gate machinery follows from this. A CASE-3 run promotes
        to production directly, never to staging, so it starts no quality gate
        run and never waits on one, and nothing in this workflow consults a gate
        verdict for any candidate.
        """
        if self._operation.tool_name == "promote_release":
            promotion = await workflow.execute_child_workflow(
                PromoteReleaseChildWorkflow.run,
                PromoteReleaseInput(
                    service=str(self._operation.arguments.get("service", "")),
                    version=str(self._operation.arguments.get("version", "")),
                    environment=str(
                        self._operation.arguments.get("environment", "")
                    ),
                    idempotency_key=self._operation.idempotency_key,
                ),
                id=(
                    f"promote-release::"
                    f"{self._operation.arguments.get('service', '')}::"
                    f"{self._operation.arguments.get('version', '')}::"
                    f"{self._operation.arguments.get('environment', '')}::"
                    f"{workflow.uuid4().hex[:8]}"
                ),
                execution_timeout=RELEASE_CHILD_TIMEOUT,
            )
            return dataclasses.asdict(promotion)
        return await workflow.execute_activity(
            invoke_tool,
            InvokeToolInput(
                tool_name=self._operation.tool_name,
                arguments=self._operation.arguments,
                idempotency_key=self._operation.idempotency_key,
            ),
            start_to_close_timeout=INVOKE_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

    async def _run_dependent_action(self) -> None:
        try:
            result = await workflow.execute_activity(
                invoke_tool,
                InvokeToolInput(
                    tool_name="record_autonomous_followup",
                    arguments={
                        "agent_run_id": self._input.agent_run_id,
                        "protected_operation_id": self._operation.operation_id,
                    },
                    idempotency_key=f"{self._input.idempotency_key}:followup",
                ),
                start_to_close_timeout=INVOKE_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
        except ActivityError as err:
            self._fail(str(err), "dependent_action_failed")
            return
        self._dependent_action_executed = True
        self._operation.status = OperationStatus.COMPLETED
        self._operation.decided_iso = workflow.now().isoformat()
        self._operation.result = {
            "protected_action": self._protected_result,
            "dependent_action": result,
        }
        self._checkpoint = {
            **self._checkpoint,
            "step": "completed",
            "next_step": None,
            "dependent_action_executed": True,
        }
        self._log("agent_run_completed")

    async def _finish(self) -> AgentRunSummary:
        await self._notify_callback()
        return self._summary()

    async def _notify_callback(self) -> None:
        if (
            not self._operation.protected
            or self._operation.status
            not in {
                OperationStatus.COMPLETED,
                OperationStatus.REJECTED,
                OperationStatus.EXPIRED,
                OperationStatus.CANCELED,
                OperationStatus.FAILED,
            }
        ):
            return
        response = self._response()
        callback = self._callback
        if callback is not None and not self._callback_notified:
            await workflow.get_external_workflow_handle(
                callback.workflow_id,
                run_id=callback.run_id,
            ).signal(
                AGENT_GATEWAY_APPROVAL_SIGNAL,
                ApprovalResolution(
                    gateway_workflow_id=self._input.workflow_id,
                    operation_id=self._operation.operation_id,
                    status=response.status,
                    adk_session_id=callback.session_id,
                    agent_run_id=self._input.agent_run_id,
                    result=response.result,
                    reason=response.reason,
                    message=response.message,
                ),
            )
            self._callback_notified = True
            self._checkpoint = {
                **self._checkpoint,
                "callback_notified": True,
                "callback_session_id": callback.session_id,
            }
            self._log(
                "adk_session_callback_notified",
                {
                    "workflow_id": callback.workflow_id,
                    "session_id": callback.session_id,
                    "status": response.status,
                },
            )

    def _fail(self, error: str, event: str) -> None:
        self._operation.status = OperationStatus.FAILED
        self._operation.error = error
        self._operation.decided_iso = workflow.now().isoformat()
        self._checkpoint = {
            **self._checkpoint,
            "step": "failed",
            "next_step": None,
        }
        self._log(event, {"error": error})

    def _response(self) -> ToolCallResponse:
        op = self._operation
        common = {
            "workflow_id": self._input.workflow_id,
            "operation_id": op.operation_id,
            "call_path": op.call_path,
        }
        if op.status == OperationStatus.WAITING_FOR_APPROVAL:
            return ToolCallResponse(
                status="waiting_for_approval",
                reason="approval_required",
                message="Approval is required before this tool call can continue.",
                poll_after_seconds=5,
                **common,
            )
        if op.status in {
            OperationStatus.EVALUATING,
            OperationStatus.APPROVED,
            OperationStatus.INVOKING,
        }:
            return ToolCallResponse(
                status="processing",
                message="The autonomous agent run is still processing.",
                poll_after_seconds=5,
                **common,
            )
        if op.status == OperationStatus.COMPLETED:
            return ToolCallResponse(
                status="completed",
                result=op.result,
                **common,
            )
        messages = {
            OperationStatus.REJECTED: "The approver rejected this operation.",
            OperationStatus.EXPIRED: (
                "Approval timed out; neither the protected nor dependent action "
                "was invoked."
            ),
            OperationStatus.CANCELED: "The autonomous agent run was canceled.",
            OperationStatus.FAILED: "The autonomous agent run failed.",
        }
        return ToolCallResponse(
            status=op.status.value,
            reason=op.error,
            message=messages.get(op.status, "The autonomous agent run stopped."),
            **common,
        )

    def _summary(self) -> AgentRunSummary:
        op = self._operation
        view = OperationView(
            operation_id=op.operation_id,
            tool_name=op.tool_name,
            status=op.status.value,
            requester=op.requester,
            requested_action=op.requested_action,
            arguments=op.safe_arguments,
            justification=op.justification,
            risk_reason=op.risk_reason,
            protected=op.protected,
            created_iso=op.created_iso,
            approval_timeout_seconds=op.approval_timeout_seconds,
            deadline_epoch=op.deadline_epoch,
            deadline_iso=op.deadline_iso,
            approver=op.approver,
            decided_iso=op.decided_iso,
            decision_reason=op.error,
            result=op.result,
            call_path=op.call_path,
            workflow_id_source=op.workflow_id_source,
            checkpoint=self._checkpoint,
        )
        return AgentRunSummary(
            workflow_id=self._input.workflow_id,
            run_id=workflow.info().run_id,
            total_operations=1,
            status_counts={op.status.value: 1},
            operations=[view],
            agent_run_id=self._input.agent_run_id,
            agent_identity=self._input.agent_identity,
            checkpoint=self._checkpoint,
            dependent_action_executed=self._dependent_action_executed,
        )

    def _log(self, event: str, detail: dict | None = None) -> None:
        entry = LedgerEntry(
            ts=workflow.now().isoformat(),
            event=event,
            operation_id=self._operation.operation_id,
            detail=detail or {},
        )
        self._ledger.append(entry)
        workflow.logger.info(
            "autonomous agent ledger event",
            extra={"event": event, "operation_id": entry.operation_id},
        )

    @staticmethod
    def _iso(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
