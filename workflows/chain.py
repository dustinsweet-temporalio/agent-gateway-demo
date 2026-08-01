from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from common.models import (
        ApprovalDecision,
        CancelOperation,
        ChainInput,
        ChainState,
        ChainSummary,
        EvaluatePolicyInput,
        InvokeToolInput,
        LedgerEntry,
        Operation,
        OperationStatus,
        OperationView,
        NestedToolCallRequest,
        ResumeNestedRequest,
        ToolCallRequest,
        ToolCallResponse,
    )
    from common.semver import InvalidVersionError, next_version, normalize_bump
    from activities.gateway_activities import evaluate_policy, invoke_tool


POLICY_TIMEOUT = timedelta(seconds=10)
INVOKE_TIMEOUT = timedelta(seconds=60)

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
    OperationStatus.CANCELED,
    OperationStatus.BLOCKED,
    OperationStatus.FAILED,
}
PENDING = {
    OperationStatus.EVALUATING,
    OperationStatus.WAITING_FOR_APPROVAL,
    OperationStatus.APPROVED,
    OperationStatus.APPROVED_AWAITING_RETRY,
    OperationStatus.WAITING_FOR_DEPENDENCY,
    OperationStatus.INVOKING,
}

# Continue-As-New input is subject to the same 2 MB payload limit as any other
# Workflow input. Keep every pending operation plus the most recent terminal
# operations, and drop older terminal records. A production system offloads
# older records to external storage. See README, "Continue-As-New".
TERMINAL_RETENTION = 50
LEDGER_RETENTION = 500

# The release pipeline the orchestrated CASE-2 path runs: a candidate reaches
# staging, is qualified by quality gates there, and only then goes to its target.
# These are constants rather than environment reads because the pipeline is a fixed
# flow, which is the entire reason it exists, and because every Worker replaying
# this Workflow must agree on the shape it took.
PIPELINE_STAGING_ENVIRONMENT = "staging"
PIPELINE_PRODUCTION_ENVIRONMENT = "prod"


class QualityGateFailure(Exception):
    """Quality gates did not pass, so the pipeline stops before its target.

    Raised inside the Workflow and caught by the calling handler, which fails the
    parent operation without ever creating the child. A candidate that failed its
    gates does not reach the approval queue: the system declines to ask.
    """


class PipelinePolicyConflict(Exception):
    """Policy protects an environment the pipeline promotes through internally.

    The pipeline carries one approval gate, at its target. If policy also protects
    an intermediate environment, the pipeline refuses rather than promoting to a
    protected environment without an approval.
    """


class PipelineVersionDrift(Exception):
    """A replay recomputed a different version than the one already approved.

    The approver approved one specific version. If production moved between the
    approval and the retry, recomputing the bump lands somewhere else, and
    promoting that would execute something nobody approved. Raised before the
    replay mutates anything.
    """


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
        self._state: ChainState = input.state or ChainState(
            workflow_id=input.workflow_id,
            owner_principal=input.owner_principal,
        )
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
            self._log(
                "created",
                op,
                {
                    "tool_name": op.tool_name,
                    "requester": op.requester,
                    "workflow_id_source": op.workflow_id_source,
                    "call_path": op.call_path,
                    "arguments": op.safe_arguments,
                },
            )

            try:
                decision = await workflow.execute_activity(
                    evaluate_policy,
                    EvaluatePolicyInput(
                        tool_name=req.tool_name,
                        arguments=req.arguments,
                        caller_principal=req.correlation.caller_principal,
                    ),
                    start_to_close_timeout=POLICY_TIMEOUT,
                )
            except ActivityError as err:
                op.status = OperationStatus.FAILED
                op.error = f"policy evaluation failed: {err}"
                op.decided_iso = workflow.now().isoformat()
                self._log("policy_failed", op, {"error": str(err)})
                return self._response_for(op)
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
        self._validate_owner(req.correlation.caller_principal)

    @workflow.update
    async def request_nested_tool_call(
        self, req: NestedToolCallRequest
    ) -> ToolCallResponse:
        """Run Tool1 until it reaches Tool2, then suspend safely at Tool2.

        Controlled Tool1 calls are checkpointed in workflow state and resume
        automatically after Tool2 approval. Uncontrolled Tool1 calls fail closed:
        approval is recorded, but Tool2 is not invoked until the caller explicitly
        retries and Tool1 advertised replay safety.
        """
        async with self._lock:
            self._touch()
            existing_id = self._state.idempotency_index.get(req.idempotency_key)
            if existing_id is not None:
                parent = self._state.operations[existing_id]
                child = (
                    self._state.operations.get(parent.child_operation_id)
                    if parent.child_operation_id
                    else None
                )
                self._log("dedup_hit", parent)
                return self._nested_response(parent, child)

            parent = self._new_nested_parent(req)
            self._state.operations[parent.operation_id] = parent
            self._state.idempotency_index[req.idempotency_key] = parent.operation_id
            self._log(
                "nested_tool1_started",
                parent,
                {
                    "controlled": req.controlled_tool1,
                    "replay_safe": req.replay_safe,
                    "call_path": parent.call_path,
                    "arguments": parent.safe_arguments,
                },
            )

            # Tool1 is safe, so it can run up to the protected Tool2 boundary.
            # A bump-driven Tool1 does real work here (read the deployed version,
            # compute the next one, cut it); a Tool1 given an explicit version
            # runs its single prepare step exactly as it always has.
            resolved_version = ""
            resolved_tool2_arguments: dict | None = None
            resolved_requested_action: str | None = None
            try:
                if req.bump:
                    prepare, resolved_version = await self._orchestrate_tool1(
                        parent, req.bump, req.idempotency_key
                    )
                    resolved_tool2_arguments = {
                        **req.tool2_arguments,
                        "version": resolved_version,
                    }
                    resolved_requested_action = self._promotion_sentence(
                        resolved_tool2_arguments
                    )
                else:
                    prepare = await self._execute_tool_activity(
                        tool_name=f"{req.tool1_name}_prepare",
                        arguments=req.tool1_arguments,
                        idempotency_key=f"{req.idempotency_key}:prepare",
                    )
            except ActivityError as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 preparation failed: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_prepare_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except InvalidVersionError as err:
                # A version the orchestrator cannot interpret stops the flow. It
                # never falls back to a guessed version, and Tool2 is never
                # created, so nothing reaches the approval surface.
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 could not resolve a version: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_version_resolution_failed",
                    parent,
                    {"error": str(err), "bump": req.bump},
                )
                return self._response_for(parent)
            except QualityGateFailure as err:
                # The candidate did not qualify. The pipeline stops here, so the
                # child is never created and nothing is ever put in front of an
                # approver. Refusing to ask is the safety property.
                parent.status = OperationStatus.FAILED
                parent.error = f"Quality gates did not pass: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_quality_gates_failed",
                    parent,
                    {"error": str(err)},
                )
                return self._response_for(parent)
            except PipelinePolicyConflict as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Release pipeline refused to run: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_pipeline_policy_conflict",
                    parent,
                    {"error": str(err)},
                )
                return self._response_for(parent)
            parent.checkpoint = {
                "stage": "waiting_for_tool2",
                "tool1_prepare_result": prepare,
                "nested_tool": req.tool2_name,
            }
            if resolved_version:
                # Recorded so a Worker or gateway restart resumes against the
                # version this run actually cut, and so an explicit replay can
                # detect that the fleet moved underneath it.
                parent.checkpoint["bump"] = req.bump
                parent.checkpoint["resolved_version"] = resolved_version
                parent.arguments = {
                    **parent.arguments,
                    "version": resolved_version,
                }
                parent.safe_arguments = {
                    **parent.safe_arguments,
                    "version": resolved_version,
                }
            self._log("tool1_checkpointed", parent, {"stage": "waiting_for_tool2"})

            child = self._new_nested_child(
                req,
                parent,
                arguments=resolved_tool2_arguments,
                requested_action=resolved_requested_action,
            )
            parent.child_operation_id = child.operation_id
            self._state.operations[child.operation_id] = child
            self._state.idempotency_index[
                f"{req.idempotency_key}:nested"
            ] = child.operation_id
            self._log(
                "nested_tool2_created",
                child,
                {
                    "parent_operation_id": parent.operation_id,
                    "call_path": child.call_path,
                    "arguments": child.safe_arguments,
                },
            )

            try:
                decision = await workflow.execute_activity(
                    evaluate_policy,
                    EvaluatePolicyInput(
                        tool_name=child.tool_name,
                        arguments=child.arguments,
                        caller_principal=child.requester,
                    ),
                    start_to_close_timeout=POLICY_TIMEOUT,
                )
            except ActivityError as err:
                child.status = OperationStatus.FAILED
                child.error = f"policy evaluation failed: {err}"
                child.decided_iso = workflow.now().isoformat()
                self._log("policy_failed", child, {"error": str(err)})
                self._propagate_child_terminal(child)
                return self._response_for(parent)
            child.risk_reason = decision.reason
            child.protected = decision.requires_approval
            self._log(
                "policy_evaluated",
                child,
                {"requires_approval": decision.requires_approval},
            )

            if not decision.requires_approval:
                child.status = OperationStatus.INVOKING
                await self._invoke(child)
                await self._resume_tool1(parent, child)
                return self._response_for(parent)

            child.status = OperationStatus.WAITING_FOR_APPROVAL
            child.deadline_epoch = (
                workflow.now().timestamp() + req.approval_timeout_seconds
            )
            child.deadline_iso = self._iso(child.deadline_epoch)
            parent.status = (
                OperationStatus.WAITING_FOR_DEPENDENCY
                if req.controlled_tool1
                else OperationStatus.BLOCKED
            )
            if not req.controlled_tool1:
                parent.error = (
                    "Tool1 is not suspension-aware. Approval can be recorded, but "
                    "the original call must be retried explicitly."
                )
            self._reschedule = True
            self._log(
                "waiting_for_approval",
                child,
                {
                    "controlled_parent": req.controlled_tool1,
                    "retry_required": not req.controlled_tool1,
                },
            )
            return self._nested_response(parent, child)

    @request_nested_tool_call.validator
    def _validate_nested_request(self, req: NestedToolCallRequest) -> None:
        if not req.tool1_name or not req.tool2_name:
            raise ValueError("tool1_name and tool2_name are required")
        if not req.idempotency_key:
            raise ValueError("idempotency_key is required")
        self._validate_owner(req.correlation.caller_principal)

    @workflow.update
    async def resume_nested_tool_call(
        self, req: ResumeNestedRequest
    ) -> ToolCallResponse:
        """Explicitly replay an approved uncontrolled nested call when safe."""
        async with self._lock:
            self._touch()
            self._validate_owner(req.caller_principal)
            child = self._state.operations.get(req.operation_id)
            if child is None or not child.parent_operation_id:
                return ToolCallResponse(
                    status="not_found",
                    workflow_id=self._state.workflow_id,
                    operation_id=req.operation_id,
                )
            parent = self._state.operations[child.parent_operation_id]
            if child.status != OperationStatus.APPROVED_AWAITING_RETRY:
                return self._nested_response(parent, child)
            if not parent.replay_safe:
                self._log(
                    "unsafe_replay_refused",
                    child,
                    {"parent_operation_id": parent.operation_id},
                )
                return self._nested_response(parent, child)

            parent.status = OperationStatus.INVOKING
            self._log(
                "explicit_nested_retry",
                child,
                {"parent_operation_id": parent.operation_id},
            )
            bump = str(parent.arguments.get("bump", ""))
            try:
                if bump:
                    # Replaying a bump-driven Tool1 reruns the whole pipeline. The
                    # mutating steps carry their original idempotency keys, so the
                    # cut and the staging promotion are found already done rather
                    # than repeated. The read and the quality gates are keyed to
                    # this pass, so production is re-read and the candidate is
                    # re-verified: that is the price of a Tool1 that could not hold
                    # the pause, and it is paid in gate minutes.
                    replay_prepare, _ = await self._orchestrate_tool1(
                        parent,
                        bump,
                        parent.idempotency_key,
                        pass_label="replay",
                        expect_version=str(child.arguments.get("version", "")),
                    )
                else:
                    replay_prepare = await self._execute_tool_activity(
                        tool_name=f"{parent.tool_name}_prepare",
                        arguments=parent.arguments,
                        idempotency_key=f"{parent.idempotency_key}:prepare",
                    )
            except ActivityError as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 replay failed: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_replay_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except InvalidVersionError as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 could not resolve a version: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_version_resolution_failed",
                    parent,
                    {"error": str(err), "bump": bump},
                )
                return self._response_for(parent)
            except PipelineVersionDrift as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Replay refused: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_replay_version_drift", parent, {"error": str(err)})
                return self._response_for(parent)
            except QualityGateFailure as err:
                # Re-verification failed on the retry. The candidate was green
                # when the approver saw it and is not green now, so the approved
                # promotion does not run.
                parent.status = OperationStatus.FAILED
                parent.error = f"Quality gates did not pass on replay: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_quality_gates_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except PipelinePolicyConflict as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Release pipeline refused to run: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_pipeline_policy_conflict", parent, {"error": str(err)}
                )
                return self._response_for(parent)
            parent.checkpoint = {
                **parent.checkpoint,
                "stage": "replayed_to_tool2_boundary",
                "tool1_replay_result": replay_prepare,
            }
            self._log("tool1_replayed", parent)
            child.status = OperationStatus.INVOKING
            await self._invoke(child)
            await self._resume_tool1(parent, child, replay=True)
            return self._response_for(parent)

    @workflow.signal
    def approve_operation(self, decision: ApprovalDecision) -> None:
        # Signal handlers only mutate state. They are synchronous, so they run to
        # completion without interleaving, and they are idempotent: a duplicate or
        # late decision on an operation that is no longer waiting is ignored.
        op = self._state.operations.get(decision.operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_APPROVAL:
            return
        self._touch()
        if (
            op.deadline_epoch is not None
            and workflow.now().timestamp() >= op.deadline_epoch
        ):
            op.status = OperationStatus.EXPIRED
            op.decided_iso = workflow.now().isoformat()
            self._log(
                "late_approval_ignored",
                op,
                {"approver": decision.approver},
            )
            self._propagate_child_terminal(op)
            return
        parent = (
            self._state.operations.get(op.parent_operation_id)
            if op.parent_operation_id
            else None
        )
        if parent is not None and not parent.controlled_tool:
            # Do not execute a protected nested action after an uncontrolled
            # Tool1 stack has unwound. Store the decision and require an explicit,
            # replay-safe retry.
            op.status = OperationStatus.APPROVED_AWAITING_RETRY
        else:
            op.status = OperationStatus.APPROVED
        op.approver = decision.approver
        op.decided_iso = workflow.now().isoformat()
        self._log(
            (
                "approved_awaiting_retry"
                if op.status == OperationStatus.APPROVED_AWAITING_RETRY
                else "approved"
            ),
            op,
            {"approver": decision.approver},
        )

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
        self._propagate_child_terminal(op)

    @workflow.signal
    def cancel_operation(self, cancellation: CancelOperation) -> None:
        op = self._state.operations.get(cancellation.operation_id)
        if op is None or op.status in TERMINAL or op.status == OperationStatus.INVOKING:
            return
        self._touch()
        op.status = OperationStatus.CANCELED
        op.approver = cancellation.canceled_by
        op.error = cancellation.reason
        op.decided_iso = workflow.now().isoformat()
        self._log(
            "canceled",
            op,
            {
                "canceled_by": cancellation.canceled_by,
                "reason": cancellation.reason,
            },
        )
        if op.child_operation_id:
            child = self._state.operations.get(op.child_operation_id)
            if child is not None and child.status not in TERMINAL:
                child.status = OperationStatus.CANCELED
                child.approver = cancellation.canceled_by
                child.error = cancellation.reason
                child.decided_iso = workflow.now().isoformat()
                self._log(
                    "canceled",
                    child,
                    {
                        "canceled_by": cancellation.canceled_by,
                        "reason": cancellation.reason,
                        "parent_canceled": True,
                    },
                )
        self._propagate_child_terminal(op)

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

    @workflow.query
    def get_workflow_owner(self) -> str:
        return self._state.owner_principal

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
        # Idle completion only applies when no work is pending. A pending
        # approved-awaiting-retry operation has no timer and should sleep until an
        # Update or Signal arrives, rather than spin after the idle deadline.
        if not self._has_pending_work():
            candidates.append(self._state.last_activity_epoch + IDLE_TIMEOUT_SECONDS)
        if not candidates:
            candidates.append(now + IDLE_TIMEOUT_SECONDS)
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
                self._propagate_child_terminal(op)

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
            if op.parent_operation_id:
                parent = self._state.operations.get(op.parent_operation_id)
                if parent is not None and parent.controlled_tool:
                    await self._resume_tool1(parent, op)
            self._touch()

    async def _invoke(self, op: Operation) -> None:
        try:
            result = await self._execute_tool_activity(
                tool_name=op.tool_name,
                arguments=op.arguments,
                idempotency_key=op.idempotency_key,
            )
            op.status = OperationStatus.COMPLETED
            op.result = result
            op.decided_iso = workflow.now().isoformat()
            self._log("completed", op)
        except ActivityError as err:
            op.status = OperationStatus.FAILED
            op.error = str(err)
            self._log("failed", op, {"error": str(err)})
            self._propagate_child_terminal(op)

    async def _execute_tool_activity(
        self, tool_name: str, arguments: dict, idempotency_key: str
    ) -> dict:
        return await workflow.execute_activity(
            invoke_tool,
            InvokeToolInput(
                tool_name=tool_name,
                arguments=arguments,
                idempotency_key=idempotency_key,
            ),
            start_to_close_timeout=INVOKE_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

    async def _orchestrate_tool1(
        self,
        parent: Operation,
        bump: str,
        idempotency_key: str,
        pass_label: str = "prepare",
        expect_version: str = "",
    ) -> tuple[dict, str]:
        """Run the release pipeline up to the target promotion, and report it.

        This is the CASE-2 story: one instruction ("deploy the next minor
        version") turns into the team's whole release pipeline, with no further
        prompting. The pipeline is fixed, which is the point of it existing at
        all. An LLM holding these tools could sequence the happy path itself; what
        it cannot do is guarantee the sequence, and in particular it cannot be
        trusted to enforce "never promote to production without green quality
        gates on staging" when a caller asks it to hurry.

            read the deployed production version
            compute the next version from the bump type
            cut that release
            promote it to staging                  (skipped for a staging target)
            run quality gates against staging      (skipped for a staging target)
            -- caller creates the target promotion as the child operation --

        A staging target gets the first leg only: the staging promotion is then
        the child, and there is nothing to qualify a candidate for.

        Every step is an ordinary Activity, so the work is in Event History and a
        Worker or gateway restart resumes rather than starting over. That matters
        far more now than when this only cut a release: the gate run is minutes of
        work sitting in the checkpoint while a human decides.

        Returns the aggregate result to checkpoint, plus the version it resolved.
        Raises InvalidVersionError if the deployed version or bump type cannot be
        interpreted, and QualityGateFailure if the gates do not pass. The caller
        turns both into a failed operation with no child, so a candidate that
        failed its gates never reaches the approval queue at all.

        Idempotency keys are split by what the step does, not by which pass it is.
        The mutations (the cut, the staging promotion) are keyed once, so a replay
        finds them already done and does not repeat them. The read and the quality
        gates are keyed per pass, so a replay re-reads production and re-verifies
        rather than trusting a verdict from before the pause. expect_version is set
        on a replay: if re-reading production yields a different next version than
        the one already approved, this raises PipelineVersionDrift before touching
        anything.

        Waiting for each step is just awaiting the Activity. There is no async
        handle to poll at this layer: the sync/async conversion is a gateway
        HTTP-boundary behavior (a client-side wait budget in gateway.server, see
        _TOOL_STRATEGY), not something a tool returns, and inside the Workflow the
        Activity await already is the durable wait for the real result.
        """
        kind = normalize_bump(bump)
        service = str(parent.arguments.get("service", ""))
        target = str(parent.arguments.get("environment", ""))

        # The bump is always computed from production, whatever the target. "The
        # next minor version" means the next one after what customers are running,
        # not the next one after whatever happens to be sitting on staging.
        deployed = await self._execute_tool_activity(
            tool_name="get_deployed_version",
            arguments={"environment": PIPELINE_PRODUCTION_ENVIRONMENT},
            idempotency_key=f"{idempotency_key}:{pass_label}:deployed",
        )
        current = deployed.get("deployed_version")
        if not current:
            raise InvalidVersionError(
                f"get_deployed_version reported no deployment in "
                f"{PIPELINE_PRODUCTION_ENVIRONMENT!r}, so there is no version "
                "to bump"
            )
        resolved = next_version(str(current), kind)
        if expect_version and resolved != expect_version:
            raise PipelineVersionDrift(
                f"production is now on {current}, so a {kind} bump resolves to "
                f"{resolved}, but {expect_version} is the approved version. "
                "Start a new release rather than promoting an unapproved version."
            )
        self._log(
            "tool1_version_resolved",
            parent,
            {
                "source_environment": PIPELINE_PRODUCTION_ENVIRONMENT,
                "current_version": str(current),
                "bump": kind,
                "resolved_version": resolved,
                "target_environment": target,
            },
        )

        # Two orchestrations started concurrently under different idempotency
        # keys both read the same deployed version and so both compute the same
        # next one. Cutting it twice is safe (the second cut is a no-op that
        # returns the existing release), but each still opens its own promotion
        # for approval, and an approver would see two cards for the same
        # promotion. Deliberately out of scope here: the dedup that prevents this
        # for a repeated call is idempotency-key based, and collapsing two
        # distinct callers would need a per-(service, environment) lease, which is
        # a larger design decision than this change. The gateway's own dedup
        # already covers the case the demo exercises, a caller retrying.
        cut = await self._execute_tool_activity(
            tool_name="cut_release",
            arguments={"service": service, "version": resolved},
            idempotency_key=f"{idempotency_key}:cut",
        )
        self._log(
            "tool1_release_cut",
            parent,
            {"version": resolved, "idempotent_replay": cut.get("idempotent_replay")},
        )

        result = {
            "orchestrated": True,
            "service": service,
            "environment": target,
            "bump": kind,
            "current_version": str(current),
            "version": resolved,
            "get_deployed_version_result": deployed,
            "cut_release_result": cut,
            "staging_promotion_result": None,
            "quality_gate_result": None,
            "next_tool": "promote_release",
        }

        if target == PIPELINE_STAGING_ENVIRONMENT:
            result["message"] = (
                f"Tool1 read {current} from production, computed {resolved} for "
                f"a {kind} bump, and cut it"
            )
            return result, resolved

        staging_arguments = {
            "service": service,
            "version": resolved,
            "environment": PIPELINE_STAGING_ENVIRONMENT,
        }
        # The pipeline's own staging promotion is still a governed action. Policy
        # is not consulted to decide the shape of the flow, but it is consulted
        # before the flow mutates an environment, so this cannot become a way to
        # reach a protected environment without an approval. The pipeline carries
        # exactly one approval gate, at its target, so a configuration that
        # protects staging too is refused rather than quietly executed.
        staging_decision = await workflow.execute_activity(
            evaluate_policy,
            EvaluatePolicyInput(
                tool_name="promote_release",
                arguments=staging_arguments,
                caller_principal=parent.requester,
            ),
            start_to_close_timeout=POLICY_TIMEOUT,
        )
        if staging_decision.requires_approval:
            raise PipelinePolicyConflict(
                f"policy protects {PIPELINE_STAGING_ENVIRONMENT}, which the "
                "release pipeline promotes to on the way to "
                f"{target}. The pipeline supports one approval gate, at its "
                "target. Promote through the single-step tools instead."
            )
        staged = await self._execute_tool_activity(
            tool_name="promote_release",
            arguments=staging_arguments,
            idempotency_key=f"{idempotency_key}:staging",
        )
        result["staging_promotion_result"] = staged
        self._log(
            "tool1_staged",
            parent,
            {
                "version": resolved,
                "environment": PIPELINE_STAGING_ENVIRONMENT,
                "idempotent_replay": staged.get("idempotent_replay"),
            },
        )

        # Quality gates deliberately do NOT reuse an idempotency key across
        # passes. Cutting and promoting are mutations, so a replay must not repeat
        # them; verifying is a read, so a replay should redo it rather than trust
        # a stale verdict from before the pause. That asymmetry is also the honest
        # cost of a Tool1 that cannot suspend: it pays for the gate run twice.
        gate_arguments = {
            "service": service,
            "version": resolved,
            "environment": PIPELINE_STAGING_ENVIRONMENT,
        }
        gates = await self._execute_tool_activity(
            tool_name="run_quality_gates",
            arguments=gate_arguments,
            idempotency_key=f"{idempotency_key}:{pass_label}:gates",
        )
        result["quality_gate_result"] = gates
        self._log(
            "tool1_quality_gates",
            parent,
            {
                "version": resolved,
                "passed": gates.get("passed"),
                "duration_seconds": gates.get("duration_seconds"),
            },
        )
        if not gates.get("passed"):
            raise QualityGateFailure(
                str(gates.get("message"))
                or f"quality gates failed for {resolved}"
            )

        result["message"] = (
            f"Tool1 read {current} from production, computed {resolved} for a "
            f"{kind} bump, cut it, promoted it to "
            f"{PIPELINE_STAGING_ENVIRONMENT}, passed quality gates, and stopped "
            f"at the protected {target} promotion"
        )
        return result, resolved

    @staticmethod
    def _promotion_sentence(arguments: dict) -> str:
        """The approver-facing sentence for a promotion resolved in-Workflow.

        The gateway builds this sentence for every other call path, but it cannot
        build this one: the version does not exist until Tool1 computes it. The
        wording is kept identical to gateway.server._requested_action so the two
        paths read the same on the approval card. That helper is CASE-1 and CASE-3
        surface, so it is duplicated here rather than moved and shared.
        """
        return (
            f"Promote {arguments.get('service', '?')} "
            f"{arguments.get('version', '?')} to "
            f"{arguments.get('environment', '?')}"
        )

    async def _resume_tool1(
        self, parent: Operation, child: Operation, replay: bool = False
    ) -> None:
        if child.status != OperationStatus.COMPLETED:
            self._propagate_child_terminal(child)
            return
        try:
            parent.status = OperationStatus.INVOKING
            self._log(
                "tool1_resuming",
                parent,
                {
                    "child_operation_id": child.operation_id,
                    "replay": replay,
                },
            )
            resume_result = await self._execute_tool_activity(
                tool_name=f"{parent.tool_name}_resume",
                arguments={
                    **parent.arguments,
                    "nested_result": child.result,
                    "replay": replay,
                },
                idempotency_key=f"{parent.idempotency_key}:resume",
            )
            parent.status = OperationStatus.COMPLETED
            parent.result = {
                "tool1_result": resume_result,
                "tool2_result": child.result,
                "resumed_from_checkpoint": True,
                "replayed": replay,
            }
            parent.checkpoint = {
                **parent.checkpoint,
                "stage": "completed",
                "nested_operation_id": child.operation_id,
            }
            parent.decided_iso = workflow.now().isoformat()
            self._log("tool1_completed", parent)
        except ActivityError as err:
            parent.status = OperationStatus.FAILED
            parent.error = str(err)
            parent.decided_iso = workflow.now().isoformat()
            self._log("tool1_resume_failed", parent, {"error": str(err)})

    def _propagate_child_terminal(self, child: Operation) -> None:
        if not child.parent_operation_id:
            return
        parent = self._state.operations.get(child.parent_operation_id)
        if parent is None or parent.status == OperationStatus.COMPLETED:
            return
        if child.status == OperationStatus.REJECTED:
            parent.status = OperationStatus.REJECTED
            parent.error = child.error or "Nested action was rejected."
        elif child.status == OperationStatus.EXPIRED:
            parent.status = OperationStatus.EXPIRED
            parent.error = "Nested approval timed out."
        elif child.status == OperationStatus.CANCELED:
            parent.status = OperationStatus.CANCELED
            parent.error = child.error or "Nested action was canceled."
        elif child.status == OperationStatus.FAILED:
            parent.status = OperationStatus.FAILED
            parent.error = child.error or "Nested action failed."
        else:
            return
        parent.decided_iso = workflow.now().isoformat()
        self._log(
            "nested_terminal_propagated",
            parent,
            {
                "child_operation_id": child.operation_id,
                "child_status": child.status.value,
            },
        )

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
        required_parent_ids = {
            op.parent_operation_id
            for op in self._state.operations.values()
            if op.status in PENDING and op.parent_operation_id
        }
        terminal_ids = [
            op_id
            for op_id, op in self._state.operations.items()
            if op.status in TERMINAL and op_id not in required_parent_ids
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
            call_path=req.correlation.call_path
            or [req.correlation.runtime, req.tool_name],
            safe_arguments=req.safe_arguments or req.arguments,
            workflow_id_source=req.correlation.workflow_id_source,
        )

    def _new_nested_parent(self, req: NestedToolCallRequest) -> Operation:
        self._state.op_seq += 1
        return Operation(
            operation_id=req.parent_operation_id
            or ("op-" + workflow.uuid4().hex[:6]),
            tool_name=req.tool1_name,
            arguments=req.tool1_arguments,
            idempotency_key=req.idempotency_key,
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=f"Run {req.tool1_name}",
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            call_path=req.correlation.call_path
            or [req.correlation.runtime, req.tool1_name],
            safe_arguments=req.safe_tool1_arguments or req.tool1_arguments,
            workflow_id_source=req.correlation.workflow_id_source,
            controlled_tool=req.controlled_tool1,
            replay_safe=req.replay_safe,
        )

    def _new_nested_child(
        self,
        req: NestedToolCallRequest,
        parent: Operation,
        arguments: dict | None = None,
        requested_action: str | None = None,
    ) -> Operation:
        """Create the Tool2 operation.

        arguments and requested_action override the request only on the
        orchestrated path, where Tool1 resolved the version the caller did not
        supply. Both default to None, so an explicit-version call is created from
        exactly what the gateway sent.
        """
        self._state.op_seq += 1
        call_path = list(parent.call_path)
        if not call_path or call_path[-1] != req.tool1_name:
            call_path.append(req.tool1_name)
        call_path.append(req.tool2_name)
        tool2_arguments = (
            arguments if arguments is not None else req.tool2_arguments
        )
        safe_tool2_arguments = req.safe_tool2_arguments or req.tool2_arguments
        if arguments is not None:
            # Keep the redacted projection in step with the resolved arguments,
            # or the approval card and the ledger would show a promotion with no
            # version while the operation carries one.
            safe_tool2_arguments = {**safe_tool2_arguments, **arguments}
        return Operation(
            operation_id=req.nested_operation_id
            or ("op-" + workflow.uuid4().hex[:6]),
            tool_name=req.tool2_name,
            arguments=tool2_arguments,
            idempotency_key=f"{req.idempotency_key}:nested",
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=(
                requested_action
                if requested_action is not None
                else req.requested_action
            ),
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            parent_operation_id=parent.operation_id,
            call_path=call_path,
            safe_arguments=safe_tool2_arguments,
            workflow_id_source=req.correlation.workflow_id_source,
            controlled_tool=req.controlled_tool1,
            replay_safe=req.replay_safe,
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
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status in (
            OperationStatus.EVALUATING,
            OperationStatus.APPROVED,
            OperationStatus.WAITING_FOR_DEPENDENCY,
            OperationStatus.INVOKING,
        ):
            return ToolCallResponse(
                status="processing",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                message="The tool call is still running.",
                poll_after_seconds=self._state.poll_after_seconds,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.APPROVED_AWAITING_RETRY:
            return ToolCallResponse(
                status="approved_retry_required",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="upstream_context_not_suspended",
                message=(
                    "Approval was granted, but Tool1 was not suspension-aware. "
                    "Retry explicitly; execution remains blocked until then."
                ),
                poll_after_seconds=self._state.poll_after_seconds,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
                retry_required=True,
            )
        if op.status == OperationStatus.COMPLETED:
            return ToolCallResponse(
                status="completed",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                result=op.result,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.REJECTED:
            return ToolCallResponse(
                status="rejected",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message="The approver rejected this operation.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.EXPIRED:
            return ToolCallResponse(
                status="expired",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="approval_timeout",
                message="Approval timed out; the downstream tool was not invoked.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.CANCELED:
            return ToolCallResponse(
                status="canceled",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message="The operation was canceled before completion.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.BLOCKED:
            return ToolCallResponse(
                status="blocked_nested_approval",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message=(
                    "A nested approval was reached in a Tool1 that cannot suspend."
                ),
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
                retry_required=True,
            )
        return ToolCallResponse(
            status="failed",
            workflow_id=wf_id,
            operation_id=op.operation_id,
            reason=op.error,
            message="The downstream tool invocation failed.",
            parent_operation_id=op.parent_operation_id,
            call_path=op.call_path,
        )

    def _nested_response(
        self, parent: Operation, child: Operation | None
    ) -> ToolCallResponse:
        if child is None:
            return self._response_for(parent)
        if child.status == OperationStatus.APPROVED_AWAITING_RETRY:
            response = self._response_for(child)
            if not parent.replay_safe:
                response.reason = "uncontrolled_tool_not_replay_safe"
                response.message = (
                    "Approval is recorded, but Tool1 did not advertise replay "
                    "safety. Agent Gateway will not execute Tool2 automatically."
                )
            return response
        if parent.status == OperationStatus.BLOCKED:
            return ToolCallResponse(
                status="blocked_nested_approval",
                workflow_id=self._state.workflow_id,
                operation_id=child.operation_id,
                parent_operation_id=parent.operation_id,
                reason="uncontrolled_tool_requires_retry",
                message=(
                    "Tool2 requires approval, but Tool1 is not suspension-aware. "
                    "The protected action remains blocked and the original call "
                    "must be retried after approval."
                ),
                poll_after_seconds=self._state.poll_after_seconds,
                call_path=child.call_path,
                retry_required=True,
            )
        if parent.status == OperationStatus.COMPLETED:
            return self._response_for(parent)
        return self._response_for(child)

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
                    parent_operation_id=op.parent_operation_id,
                    child_operation_id=op.child_operation_id,
                    call_path=op.call_path,
                    workflow_id_source=op.workflow_id_source,
                    controlled_tool=op.controlled_tool,
                    replay_safe=op.replay_safe,
                    checkpoint=op.checkpoint,
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

    def _validate_owner(self, caller_principal: str | None) -> None:
        if (
            self._state.owner_principal
            and caller_principal != self._state.owner_principal
        ):
            raise PermissionError("workflow_id is owned by a different principal")

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
