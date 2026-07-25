from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class OperationStatus(str, Enum):
    """Lifecycle states for a single approval-gated operation."""

    EVALUATING = "evaluating"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    APPROVED = "approved"
    APPROVED_AWAITING_RETRY = "approved_awaiting_retry"
    WAITING_FOR_DEPENDENCY = "waiting_for_dependency"
    INVOKING = "invoking"
    COMPLETED = "completed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELED = "canceled"
    BLOCKED = "blocked"
    FAILED = "failed"


@dataclass
class CorrelationContext:
    """Carries enough context for the gateway to resolve the chain workflow_id.

    workflow_id_source records how the ID was resolved (explicit propagation is
    the preferred contract for controlled agents such as Claude Code; the gateway
    derives one from the MCP session when no explicit id is supplied).
    """

    workflow_id: str
    workflow_id_source: str = "explicit"
    operation_id: Optional[str] = None
    parent_operation_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    agent_session_id: Optional[str] = None
    agent_run_id: Optional[str] = None
    tool_call_id: Optional[str] = None
    parent_call_id: Optional[str] = None
    request_id: Optional[str] = None
    traceparent: Optional[str] = None
    caller_service: Optional[str] = None
    caller_principal: Optional[str] = None
    end_user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    operation_type: Optional[str] = None
    target_resource: Optional[str] = None
    runtime: str = "mcp"
    call_path: list[str] = field(default_factory=list)


@dataclass
class ToolCallRequest:
    """A single tool call delivered to the chain workflow as an Update."""

    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300
    # Human sentence describing what will happen, built by the gateway.
    requested_action: str = ""
    # Optional requester-supplied reason for the request, distinct from the
    # policy's risk reason.
    justification: Optional[str] = None
    # Gateway-supplied operation id, derived from the idempotency key so the
    # gateway knows it up front (needed when a call converts to async before the
    # workflow responds).
    operation_id: str = ""
    safe_arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolCallResponse:
    """The structured response the gateway returns to the caller.

    A waiting response mirrors the requirements document shape: status,
    workflow_id, operation_id, reason, message, and poll_after_seconds.
    """

    status: str
    workflow_id: str
    operation_id: Optional[str] = None
    result: Optional[Any] = None
    reason: Optional[str] = None
    message: Optional[str] = None
    poll_after_seconds: Optional[int] = None
    parent_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    retry_required: bool = False


@dataclass
class ApprovalDecision:
    """An approve or reject decision delivered to the chain workflow as a Signal."""

    operation_id: str
    approver: str
    reason: Optional[str] = None


@dataclass
class CancelOperation:
    """A caller or operator cancellation delivered as a Signal."""

    operation_id: str
    canceled_by: str
    reason: Optional[str] = None


@dataclass
class NestedToolCallRequest:
    """A Tool1 call that discovers a nested Tool2 call through the gateway."""

    tool1_name: str
    tool1_arguments: dict[str, Any]
    tool2_name: str
    tool2_arguments: dict[str, Any]
    idempotency_key: str
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300
    requested_action: str = ""
    justification: Optional[str] = None
    parent_operation_id: str = ""
    nested_operation_id: str = ""
    controlled_tool1: bool = True
    replay_safe: bool = False
    safe_tool1_arguments: dict[str, Any] = field(default_factory=dict)
    safe_tool2_arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResumeNestedRequest:
    """Explicit retry/resume request for an uncontrolled Tool1."""

    operation_id: str
    caller_principal: str


@dataclass
class EvaluatePolicyInput:
    tool_name: str
    arguments: dict[str, Any]
    caller_principal: Optional[str] = None


@dataclass
class PolicyDecision:
    requires_approval: bool
    reason: Optional[str] = None


@dataclass
class InvokeToolInput:
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str


@dataclass
class LedgerEntry:
    """One durable audit record. The full ledger is also the Event History."""

    ts: str
    event: str
    operation_id: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Operation:
    operation_id: str
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    status: OperationStatus
    requester: Optional[str] = None
    requested_action: str = ""
    justification: Optional[str] = None
    risk_reason: Optional[str] = None
    protected: bool = False
    created_iso: str = ""
    approval_timeout_seconds: Optional[int] = None
    deadline_epoch: Optional[float] = None
    deadline_iso: Optional[str] = None
    approver: Optional[str] = None
    decided_iso: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    parent_operation_id: Optional[str] = None
    child_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    safe_arguments: dict[str, Any] = field(default_factory=dict)
    workflow_id_source: str = ""
    controlled_tool: bool = True
    replay_safe: bool = False
    checkpoint: dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationView:
    """A query-friendly projection of an Operation, with everything an approver
    or an auditor needs to see for both the pending and the decided views."""

    operation_id: str
    tool_name: str
    status: str
    requester: Optional[str] = None
    requested_action: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)
    justification: Optional[str] = None
    risk_reason: Optional[str] = None
    protected: bool = False
    created_iso: str = ""
    approval_timeout_seconds: Optional[int] = None
    deadline_epoch: Optional[float] = None
    deadline_iso: Optional[str] = None
    approver: Optional[str] = None
    decided_iso: Optional[str] = None
    decision_reason: Optional[str] = None
    result: Optional[Any] = None
    parent_operation_id: Optional[str] = None
    child_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    workflow_id_source: str = ""
    controlled_tool: bool = True
    replay_safe: bool = False
    checkpoint: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChainState:
    """All chain state. This is what gets forwarded across Continue-As-New."""

    workflow_id: str
    owner_principal: str = ""
    operations: dict[str, Operation] = field(default_factory=dict)
    # idempotency_key -> operation_id. Carried across CAN because Update IDs are
    # scoped to a single Execution and reset after Continue-As-New.
    idempotency_index: dict[str, str] = field(default_factory=dict)
    ledger: list[LedgerEntry] = field(default_factory=list)
    op_seq: int = 0
    poll_after_seconds: int = 5
    # Absolute workflow time of the last session activity. Seeded on first run and
    # updated on each tool call and decision. Drives the idle timeout. Carried
    # across Continue-As-New.
    last_activity_epoch: float = 0.0


@dataclass
class ChainInput:
    """Workflow input. state is None on first start and populated on CAN."""

    workflow_id: str
    owner_principal: str = ""
    state: Optional[ChainState] = None


@dataclass
class ChainSummary:
    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    closing: bool = False


@dataclass
class AutonomousAgentInput:
    """Input for a durable Google ADK-style autonomous agent run."""

    workflow_id: str
    operation_id: str
    idempotency_key: str
    owner_principal: str
    agent_run_id: str
    agent_identity: str
    tool_name: str
    arguments: dict[str, Any]
    safe_arguments: dict[str, Any]
    requested_action: str
    justification: Optional[str]
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300


@dataclass
class AgentRunSummary:
    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    agent_run_id: str
    agent_identity: str
    checkpoint: dict[str, Any] = field(default_factory=dict)
    dependent_action_executed: bool = False


@dataclass
class WorkflowStatusResponse:
    """Stable MCP output schema shared by chain and autonomous workflows."""

    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    closing: bool = False
    agent_run_id: Optional[str] = None
    agent_identity: Optional[str] = None
    checkpoint: dict[str, Any] = field(default_factory=dict)
    dependent_action_executed: bool = False


@dataclass
class WorkflowLedgerResponse:
    """Stable MCP output schema for the durable human-input ledger."""

    workflow_id: str
    entries: list[LedgerEntry]
