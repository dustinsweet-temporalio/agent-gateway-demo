"""Release Safety's own data types.

The three types at the bottom (AgentGatewayPromoteInput's response shape,
GatewayOperationResolution, CanaryVerdict) are this team's copy of the wire
contract it has with Agent Gateway. Agent Gateway keeps its own copy in
common/models.py. Neither imports the other: they are different services owned
by different teams, and a shared internal package between them would be a lie
about the boundary. The payload converter matches on field names, so the
contract is the field names, and it is checked by
tests/test_release_safety_contract.py rather than by the type system.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


CANARY_TASK_QUEUE = "release-safety-tq"
CANARY_NAMESPACE = "release-safety"

# The error rate a canary tick is allowed to report. Fixed, so the number on the
# dashboard is legible and the same on every run.
CANARY_THRESHOLD = 0.02

# The canary window. Four ticks five seconds apart is fifteen seconds of live
# wall clock: long enough that the tick counter reads as an observable wait (and
# long enough to kill the worker mid-window and show the workflow pick up where
# it left off), short enough that nobody watching a demo loses interest. These
# are constants rather than dashboard inputs on purpose. Changing them is a
# deliberate decision about the demo, not a knob to reach for.
CANARY_TICK_SECONDS = 5
CANARY_WINDOW_TICKS = 4


@dataclass
class CanaryAnalysisInput:
    """One canary window, gating one promotion.

    workflow_id is Agent Gateway's chain workflow id, not this workflow's own.
    It is the correlation context that ties two namespaces to one user-visible
    task: when canary calls back to request the promotion, the nested call has
    to land on the chain the operator started, not open a new one.
    """

    workflow_id: str
    service: str
    version: str
    environment: str = "prod"
    tick_seconds: int = CANARY_TICK_SECONDS
    window_ticks: int = CANARY_WINDOW_TICKS
    # "pass" | "fail" | "flaky_then_pass". Scripted so a demo run is repeatable.
    scripted_outcome: str = "pass"
    idempotency_key: str = ""
    # The operation in Agent Gateway's chain that handed off to this canary and
    # is parked until it resolves. Echoed back on both the pass and fail paths.
    origin_operation_id: str = ""
    # Who asked for the release. Canary acts on their behalf when it calls the
    # gateway back, so the gateway can check the request against the chain's
    # owner. See canary_activities.call_agent_gateway_promote.
    requester: str = ""
    gateway_namespace: str = "default"


@dataclass
class CanaryTickInput:
    service: str
    version: str
    tick_number: int
    window_ticks: int
    scripted_outcome: str
    canary_workflow_id: str
    environment: str = "prod"


@dataclass
class CanaryTickResult:
    tick: int
    error_rate: float
    threshold: float
    passed: bool


@dataclass
class CanaryState:
    phase: str = "running_canary"
    ticks_completed: int = 0
    window_ticks: int = CANARY_WINDOW_TICKS
    tick_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    gateway_operation_id: Optional[str] = None
    gateway_workflow_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class CanaryStatusView:
    """Read-only projection for operators and for the Temporal UI."""

    canary_workflow_id: str
    service: str
    version: str
    environment: str
    phase: str
    ticks_completed: int
    window_ticks: int
    tick_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    gateway_workflow_id: Optional[str] = None
    gateway_operation_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class CanaryResult:
    phase: str
    verdict: Optional[str] = None
    tick_results: list[dict[str, Any]] = field(default_factory=list)
    gateway_operation_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class AgentGatewayPromoteInput:
    """The nested tool call canary makes once its window closes green."""

    gateway_workflow_id: str
    gateway_namespace: str
    canary_workflow_id: str
    origin_operation_id: str
    service: str
    version: str
    environment: str
    idempotency_key: str
    requester: str
    justification: str


@dataclass
class ReportCanaryVerdictInput:
    """Tell Agent Gateway the window closed red, so it can stop waiting."""

    gateway_workflow_id: str
    gateway_namespace: str
    canary_workflow_id: str
    origin_operation_id: str
    verdict: str
    reason: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class PublishCanaryStateInput:
    """Publish window progress to the shared observability backend.

    The dashboard's canary card is rendered from this, the same way the quality
    gate card is rendered from the backend's gate state. Purely for visibility:
    the workflow's own decision never depends on it, so a failure to publish
    must never fail the window.
    """

    canary_workflow_id: str
    service: str
    version: str
    environment: str
    phase: str
    ticks_completed: int
    window_ticks: int
    threshold: float
    tick_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    gateway_operation_id: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------- wire contract


@dataclass
class GatewayOperationResolution:
    """Agent Gateway's terminal answer, delivered as a Signal.

    This team's copy of common.models.GatewayOperationResolution. Same field
    names, deliberately not the same class.
    """

    operation_id: str
    status: str
    result: Optional[Any] = None
    reason: Optional[str] = None
