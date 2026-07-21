from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class OperationStatus(str, Enum):
    """Lifecycle states for a single approval-gated operation."""

    EVALUATING = "evaluating"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    APPROVED = "approved"
    INVOKING = "invoking"
    COMPLETED = "completed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    FAILED = "failed"


@dataclass
class CorrelationContext:
    """Carries enough context for the gateway to resolve the chain workflow_id.

    workflow_id_source records how the ID was resolved (explicit propagation is
    the preferred contract for controlled agents such as Claude Code; the gateway
    generates one only when no usable correlation exists).
    """

    workflow_id: str
    workflow_id_source: str = "explicit"
    agent_session_id: Optional[str] = None
    request_id: Optional[str] = None
    caller_principal: Optional[str] = None


@dataclass
class ToolCallRequest:
    """A single tool call delivered to the chain workflow as an Update."""

    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300


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


@dataclass
class ApprovalDecision:
    """An approve or reject decision delivered to the chain workflow as a Signal."""

    operation_id: str
    approver: str
    reason: Optional[str] = None


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
    risk_reason: Optional[str] = None
    created_iso: str = ""
    deadline_epoch: Optional[float] = None
    deadline_iso: Optional[str] = None
    approver: Optional[str] = None
    decided_iso: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class OperationView:
    """A compact, query-friendly projection of an Operation."""

    operation_id: str
    tool_name: str
    status: str
    requester: Optional[str] = None
    approver: Optional[str] = None
    created_iso: str = ""
    deadline_iso: Optional[str] = None
    risk_reason: Optional[str] = None


@dataclass
class ChainState:
    """All chain state. This is what gets forwarded across Continue-As-New."""

    workflow_id: str
    operations: dict[str, Operation] = field(default_factory=dict)
    # idempotency_key -> operation_id. Carried across CAN because Update IDs are
    # scoped to a single Execution and reset after Continue-As-New.
    idempotency_index: dict[str, str] = field(default_factory=dict)
    ledger: list[LedgerEntry] = field(default_factory=list)
    op_seq: int = 0
    poll_after_seconds: int = 5


@dataclass
class ChainInput:
    """Workflow input. state is None on first start and populated on CAN."""

    workflow_id: str
    state: Optional[ChainState] = None


@dataclass
class ChainSummary:
    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    closing: bool = False
