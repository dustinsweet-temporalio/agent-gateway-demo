from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from common.models import (
        ApprovalDecision,
        ChainInput,
        ChainState,
        ChainSummary,
        EvaluatePolicyInput,
        InvokeToolInput,
        LedgerEntry,
        Operation,
        OperationStatus,
        OperationView,
        ToolCallRequest,
        ToolCallResponse,
    )
    from activities.gateway_activities import evaluate_policy, invoke_tool


POLICY_TIMEOUT = timedelta(seconds=10)
INVOKE_TIMEOUT = timedelta(seconds=30)

# Idle timeout for an agentic chain. A chain that has no pending work and receives
# no new tool call or decision for this long completes on its own. This is a fixed
# constant rather than an environment read, because Workflow code must not read
# non-deterministic sources: every Worker replaying this Workflow must agree on the
# value. To demo the idle close live, lower this and rebuild the worker image. To
# make it per-chain configurable, carry it on ChainInput instead of here.
IDLE_TIMEOUT_SECONDS = 24 * 60 * 60

TERMINAL = {
    OperationStatus.COMPLETED,
    OperationStatus.REJECTED,
    OperationStatus.EXPIRED,
    OperationStatus.FAILED,
}
PENDING = {
    OperationStatus.EVALUATING,
    OperationStatus.WAITING_FOR_APPROVAL,
    OperationStatus.APPROVED,
    OperationStatus.INVOKING,
}

# Continue-As-New input is subject to the same 2 MB payload limit as any other
# Workflow input. Keep every pending operation plus the most recent terminal
# operations, and drop older terminal records. A production system offloads
# older records to external storage. See README, "Continue-As-New".
TERMINAL_RETENTION = 50
LEDGER_RETENTION = 500


@workflow.defn
class AgenticChainWorkflow:
    """One durable, long-lived workflow per agentic chain (workflow_id).

    The workflow holds the approval pause. A tool call arrives as an Update and
    returns fast with either a completed result or a waiting payload. Approve and
    reject decisions arrive as Signals that only mutate state; the main loop reacts
    to those state changes and performs the downstream invocation. This mirrors the
    Temporal best practice that message handlers should not drive Activities
    directly. The workflow rolls over with Continue-As-New to keep history bounded,
    and completes on its own once it has been idle with no pending work.
    """

    @workflow.init
    def __init__(self, input: ChainInput) -> None:
        # State is initialized before any handler runs, which matters because a
        # tool-call Update can arrive together with workflow start (update-with-start).
        self._state: ChainState = input.state or ChainState(workflow_id=input.workflow_id)
        self._lock = asyncio.Lock()
        self._closing = False
        # Transient wake flag so the main loop recomputes its timeout when a new
        # waiting operation with a fresh deadline is created. Not carried across CAN.
        self._reschedule = False

    @workflow.run
    async def run(self, input: ChainInput) -> ChainSummary:
        # Seed the idle clock on first run only. On Continue-As-New the carried value
        # is preserved so a rollover does not reset the idle window.
        if self._state.last_activity_epoch == 0.0:
            self._state.last_activity_epoch = workflow.now().timestamp()

        while True:
            try:
                await workflow.wait_condition(
                    self._loop_should_wake,
                    timeout=self._next_wait(),
                )
            except asyncio.TimeoutError:
                # Timeout means an approval deadline or the idle window may have elapsed.
                pass

            self._reschedule = False
            self._expire_overdue()
            await self._process_approved()

            if not self._closing and workflow.info().is_continue_as_new_suggested():
                await self._do_continue_as_new()  # does not return

            # Graceful completion: an explicit close signal, or the chain has been
            # idle with nothing pending. Drain handlers first, then re-check that no
            # work arrived during the drain before completing.
            if not self._has_pending_work() and (self._closing or self._is_idle()):
                await workflow.wait_condition(workflow.all_handlers_finished)
                if not self._has_pending_work():
                    if self._closing:
                        self._log("closed", None)
                    else:
                        self._log("idle_timeout", None, {"idle_seconds": IDLE_TIMEOUT_SECONDS})
                    return self._summary()

    # ----------------------------------------------------------------- handlers

    @workflow.update
    async def request_tool_call(self, req: ToolCallRequest) -> ToolCallResponse:
        """Idempotent, deterministic tool-call entry point.

        Returns quickly with a completed result (no approval needed) or a waiting
        payload (approval needed). The downstream tool is never invoked here on the
        approval path; the main loop performs it after approval.
        """
        async with self._lock:
            self._touch()
            existing_id = self._state.idempotency_index.get(req.idempotency_key)
            if existing_id is not None:
                op = self._state.operations[existing_id]
                self._log("dedup_hit", op)
                return self._response_for(op)

            op = self._new_operation(req)
            self._state.operations[op.operation_id] = op
            self._state.idempotency_index[req.idempotency_key] = op.operation_id
            self._log("created", op)

            decision = await workflow.execute_activity(
                evaluate_policy,
                EvaluatePolicyInput(
                    tool_name=req.tool_name,
                    arguments=req.arguments,
                    caller_principal=req.correlation.caller_principal,
                ),
                start_to_close_timeout=POLICY_TIMEOUT,
            )
            op.risk_reason = decision.reason
            op.protected = decision.requires_approval
            self._log(
                "policy_evaluated",
                op,
                {"requires_approval": decision.requires_approval},
            )

            if not decision.requires_approval:
                op.status = OperationStatus.INVOKING
                await self._invoke(op)
                return self._response_for(op)

            op.status = OperationStatus.WAITING_FOR_APPROVAL
            op.deadline_epoch = workflow.now().timestamp() + req.approval_timeout_seconds
            op.deadline_iso = self._iso(op.deadline_epoch)
            self._reschedule = True
            self._log("waiting_for_approval", op)
            return self._response_for(op)

    @request_tool_call.validator
    def _validate_request(self, req: ToolCallRequest) -> None:
        # Rejected Updates leave no trace in Event History.
        if not req.tool_name:
            raise ValueError("tool_name is required")
        if not req.idempotency_key:
            raise ValueError("idempotency_key is required")

    @workflow.signal
    def approve_operation(self, decision: ApprovalDecision) -> None:
        # Signal handlers only mutate state. They are synchronous, so they run to
        # completion without interleaving, and they are idempotent: a duplicate or
        # late decision on an operation that is no longer waiting is ignored.
        op = self._state.operations.get(decision.operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_APPROVAL:
            return
        self._touch()
        op.status = OperationStatus.APPROVED
        op.approver = decision.approver
        op.decided_iso = workflow.now().isoformat()
        self._log("approved", op, {"approver": decision.approver})

    @workflow.signal
    def reject_operation(self, decision: ApprovalDecision) -> None:
        op = self._state.operations.get(decision.operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_APPROVAL:
            return
        self._touch()
        op.status = OperationStatus.REJECTED
        op.approver = decision.approver
        op.error = decision.reason
        op.decided_iso = workflow.now().isoformat()
        self._log(
            "rejected",
            op,
            {"approver": decision.approver, "reason": decision.reason},
        )

    @workflow.signal
    def close_chain(self) -> None:
        # Lets the chain complete gracefully once no work is pending, so it can be
        # queried afterward as a completed (not terminated) workflow.
        self._closing = True

    @workflow.query
    def get_operation_status(self, operation_id: str) -> ToolCallResponse:
        op = self._state.operations.get(operation_id)
        if op is None:
            return ToolCallResponse(
                status="not_found",
                workflow_id=self._state.workflow_id,
                operation_id=operation_id,
            )
        return self._response_for(op)

    @workflow.query
    def get_operation_result(self, operation_id: str) -> ToolCallResponse:
        return self.get_operation_status(operation_id)

    @workflow.query
    def get_workflow_status(self) -> ChainSummary:
        return self._summary()

    @workflow.query
    def get_ledger(self) -> list[LedgerEntry]:
        return list(self._state.ledger)

    # ------------------------------------------------------------- loop helpers

    def _loop_should_wake(self) -> bool:
        if self._closing or self._reschedule:
            return True
        if workflow.info().is_continue_as_new_suggested():
            return True
        if not self._has_pending_work() and self._is_idle():
            return True
        now = workflow.now().timestamp()
        for op in self._state.operations.values():
            if op.status == OperationStatus.APPROVED:
                return True
            if (
                op.status == OperationStatus.WAITING_FOR_APPROVAL
                and op.deadline_epoch is not None
                and now >= op.deadline_epoch
            ):
                return True
        return False

    def _next_wait(self) -> timedelta:
        now = workflow.now().timestamp()
        candidates = [
            op.deadline_epoch
            for op in self._state.operations.values()
            if op.status == OperationStatus.WAITING_FOR_APPROVAL
            and op.deadline_epoch is not None
        ]
        # The idle deadline is always a candidate, so the loop always has a finite
        # wake time and will eventually evaluate the idle-close condition.
        candidates.append(self._state.last_activity_epoch + IDLE_TIMEOUT_SECONDS)
        return timedelta(seconds=max(0.0, min(candidates) - now))

    def _is_idle(self) -> bool:
        return (
            workflow.now().timestamp()
            >= self._state.last_activity_epoch + IDLE_TIMEOUT_SECONDS
        )

    def _touch(self) -> None:
        self._state.last_activity_epoch = workflow.now().timestamp()

    def _expire_overdue(self) -> None:
        now = workflow.now().timestamp()
        for op in self._state.operations.values():
            if (
                op.status == OperationStatus.WAITING_FOR_APPROVAL
                and op.deadline_epoch is not None
                and now >= op.deadline_epoch
            ):
                op.status = OperationStatus.EXPIRED
                op.decided_iso = workflow.now().isoformat()
                self._log("expired", op)

    async def _process_approved(self) -> None:
        approved_ids = [
            op.operation_id
            for op in self._state.operations.values()
            if op.status == OperationStatus.APPROVED
        ]
        for op_id in approved_ids:
            async with self._lock:
                op = self._state.operations[op_id]
                if op.status != OperationStatus.APPROVED:
                    continue
                op.status = OperationStatus.INVOKING
                self._log("invoking", op)
            await self._invoke(op)
            self._touch()

    async def _invoke(self, op: Operation) -> None:
        try:
            result = await workflow.execute_activity(
                invoke_tool,
                InvokeToolInput(
                    tool_name=op.tool_name,
                    arguments=op.arguments,
                    idempotency_key=op.idempotency_key,
                ),
                start_to_close_timeout=INVOKE_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            op.status = OperationStatus.COMPLETED
            op.result = result
            op.decided_iso = workflow.now().isoformat()
            self._log("completed", op)
        except ActivityError as err:
            op.status = OperationStatus.FAILED
            op.error = str(err)
            self._log("failed", op, {"error": str(err)})

    async def _do_continue_as_new(self) -> None:
        # Continue-As-New must be called from the main method, never a handler.
        # Drain in-flight Update and Signal handlers first so their work is not
        # interrupted by the transition.
        await workflow.wait_condition(workflow.all_handlers_finished)
        self._prune_terminal()
        self._log("continue_as_new", None, {"operations": len(self._state.operations)})
        workflow.continue_as_new(
            ChainInput(workflow_id=self._state.workflow_id, state=self._state)
        )

    def _prune_terminal(self) -> None:
        terminal_ids = [
            op_id
            for op_id, op in self._state.operations.items()
            if op.status in TERMINAL
        ]
        excess = len(terminal_ids) - TERMINAL_RETENTION
        if excess > 0:
            for op_id in terminal_ids[:excess]:
                op = self._state.operations.pop(op_id)
                self._state.idempotency_index.pop(op.idempotency_key, None)
        if len(self._state.ledger) > LEDGER_RETENTION:
            self._state.ledger = self._state.ledger[-LEDGER_RETENTION:]

    def _has_pending_work(self) -> bool:
        return any(op.status in PENDING for op in self._state.operations.values())

    # -------------------------------------------------------------- projections

    def _new_operation(self, req: ToolCallRequest) -> Operation:
        self._state.op_seq += 1
        op_id = req.operation_id or ("op-" + workflow.uuid4().hex[:6])
        return Operation(
            operation_id=op_id,
            tool_name=req.tool_name,
            arguments=req.arguments,
            idempotency_key=req.idempotency_key,
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=req.requested_action,
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
        )

    def _response_for(self, op: Operation) -> ToolCallResponse:
        wf_id = self._state.workflow_id
        if op.status == OperationStatus.WAITING_FOR_APPROVAL:
            return ToolCallResponse(
                status="waiting_for_approval",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="approval_required",
                message="Approval is required before this tool call can continue.",
                poll_after_seconds=self._state.poll_after_seconds,
            )
        if op.status in (
            OperationStatus.EVALUATING,
            OperationStatus.APPROVED,
            OperationStatus.INVOKING,
        ):
            return ToolCallResponse(
                status="processing",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                message="The tool call is still running.",
                poll_after_seconds=self._state.poll_after_seconds,
            )
        if op.status == OperationStatus.COMPLETED:
            return ToolCallResponse(
                status="completed",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                result=op.result,
            )
        if op.status == OperationStatus.REJECTED:
            return ToolCallResponse(
                status="rejected",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message="The approver rejected this operation.",
            )
        if op.status == OperationStatus.EXPIRED:
            return ToolCallResponse(
                status="expired",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="approval_timeout",
                message="Approval timed out; the downstream tool was not invoked.",
            )
        return ToolCallResponse(
            status="failed",
            workflow_id=wf_id,
            operation_id=op.operation_id,
            reason=op.error,
            message="The downstream tool invocation failed.",
        )

    def _summary(self) -> ChainSummary:
        counts: dict[str, int] = {}
        views: list[OperationView] = []
        for op in self._state.operations.values():
            counts[op.status.value] = counts.get(op.status.value, 0) + 1
            views.append(
                OperationView(
                    operation_id=op.operation_id,
                    tool_name=op.tool_name,
                    status=op.status.value,
                    requester=op.requester,
                    requested_action=op.requested_action,
                    arguments=op.arguments,
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
                )
            )
        return ChainSummary(
            workflow_id=self._state.workflow_id,
            run_id=workflow.info().run_id,
            total_operations=len(self._state.operations),
            status_counts=counts,
            operations=views,
            closing=self._closing,
        )

    def _log(self, event: str, op, detail: dict | None = None) -> None:
        entry = LedgerEntry(
            ts=workflow.now().isoformat(),
            event=event,
            operation_id=op.operation_id if op is not None else None,
            detail=detail or {},
        )
        self._state.ledger.append(entry)
        workflow.logger.info(
            "ledger event",
            extra={"event": event, "operation_id": entry.operation_id},
        )

    @staticmethod
    def _iso(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
