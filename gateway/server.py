from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import uuid
from http.cookies import SimpleCookie
from contextvars import ContextVar
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Literal, Optional

import requests
import uvicorn
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy

from common.models import (
    ADK_SESSION_ID_HEADER,
    CALLBACK_RUN_ID_HEADER,
    CALLBACK_WORKFLOW_ID_HEADER,
    AdkTemporalSessionCallback,
    ApprovalDecision,
    AutonomousAgentInput,
    CancelOperation,
    ChainInput,
    CorrelationContext,
    LedgerEntry,
    NestedToolCallRequest,
    OperationView,
    ResumeNestedRequest,
    ToolCallRequest,
    ToolCallResponse,
    WorkflowLedgerResponse,
    WorkflowStatusResponse,
)
from common.semver import normalize_bump
from workflows.autonomous_agent import AutonomousAgentWorkflow
from workflows.chain import AgenticChainWorkflow

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
APPROVAL_TIMEOUT_SECONDS = int(os.getenv("APPROVAL_TIMEOUT_SECONDS", "300"))
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8080"))
MOCK_TOOL_STATE_URL = os.getenv(
    "MOCK_TOOL_STATE_URL", "http://mock-tool:9000/state"
)
# The service this deployment backend manages. Same env var mock_tool reads, so
# the two cannot disagree. It exists so an orchestration request does not have to
# name the service: "deploy the next minor version to staging" is a complete
# instruction in a single-service demo.
DEFAULT_SERVICE = os.getenv("DEMO_SERVICE", "delivery-matching-service")
# Where the release pipeline ends when the caller does not say. "Deploy the next
# minor version" means all the way to production, through staging and the gates.
PIPELINE_TARGET_ENVIRONMENT = "prod"
PROTECTED_ENVIRONMENTS = {
    item.strip().lower()
    for item in os.getenv("PROTECTED_ENVIRONMENTS", "prod,production").split(",")
    if item.strip()
}
GATEWAY_MCP_ALLOWED_HOSTS = [
    item.strip()
    for item in os.getenv(
        "GATEWAY_MCP_ALLOWED_HOSTS",
        "127.0.0.1:*,localhost:*,[::1]:*,gateway:*",
    ).split(",")
    if item.strip()
]
GATEWAY_MCP_ALLOWED_ORIGINS = [
    item.strip()
    for item in os.getenv(
        "GATEWAY_MCP_ALLOWED_ORIGINS",
        "http://127.0.0.1:*,http://localhost:*,http://[::1]:*",
    ).split(",")
    if item.strip()
]

TERMINAL_STATUSES = {
    "completed",
    "rejected",
    "expired",
    "canceled",
    "blocked",
    "approved_awaiting_retry",
    "failed",
}
UNVERIFIED_PRINCIPAL = "claude-code (unverified)"
GATEWAY_DEBUG = bool(os.getenv("GATEWAY_DEBUG"))
POLL_AFTER_SECONDS = int(os.getenv("POLL_AFTER_SECONDS", "5"))

# Per-tool sync/async handling.
#   "sync"        block to completion, never convert
#   "async"       return a poll handle immediately
#   <int> ms      monitor and convert: block up to this budget, then convert to
#                 async and return a poll handle while the workflow keeps running
# A tool absent from this map uses _DEFAULT_STRATEGY.
_TOOL_STRATEGY: dict[str, object] = {
    "get_deployed_version": "sync",
    # A cut runs its three steps in about four seconds, so an ordinary one
    # answers inside this budget. SLOW_CUT_VERSION deliberately does not, which
    # is what demonstrates the conversion.
    "cut_release": 5000,
    # A promotion runs four steps in about five and a half seconds. The budget
    # is set above that on purpose: an unprotected staging promotion should
    # still answer `completed` to its caller rather than converting to async
    # just because the work behind it became several steps instead of one. A
    # protected promotion never gets near this, because it returns
    # waiting_for_approval before any of those steps run.
    "promote_release": 9000,
}
_DEFAULT_STRATEGY: object = 5000

mcp = FastMCP(
    "agent-gateway",
    host=GATEWAY_HOST,
    port=GATEWAY_PORT,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=GATEWAY_MCP_ALLOWED_HOSTS,
        allowed_origins=GATEWAY_MCP_ALLOWED_ORIGINS,
    ),
)

_client: Optional[Client] = None
_client_lock = asyncio.Lock()

# One chain per MCP session. See _resolve_workflow_id.
_session_chains: dict[str, str] = {}
_DEFAULT_CHAIN = "wf-" + uuid.uuid4().hex[:6]


def _load_principals() -> dict[str, str]:
    """Static bearer token to identity map from GATEWAY_PRINCIPALS (JSON).

    Example: {"tok_dustin": "Dustin Sweet <dustin.sweet@porticour.io>"}
    """
    raw = os.getenv("GATEWAY_PRINCIPALS", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}


_PRINCIPALS = _load_principals()


def _load_approvers() -> set[str]:
    raw = os.getenv("GATEWAY_APPROVERS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}


_APPROVERS = _load_approvers()


def _load_principal_teams() -> dict[str, str]:
    """Principal to Porticour engineering team map from GATEWAY_PRINCIPAL_TEAMS.

    Example:
      {"dustin.sweet@porticour.io": "waypoint",
       "abe.roover@porticour.io": "security"}

    Explicit rather than inferred. An Operation gated by the Security team's
    pre-prod scan may only be approved by a member of the Security team (see
    _authorize_approver), and deriving team membership from an email domain, a
    token name, or any other implicit signal would be both unauditable and
    silently wrong the first time a persona's address changed.
    """
    raw = os.getenv("GATEWAY_PRINCIPAL_TEAMS", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {str(k).strip(): str(v).strip().lower() for k, v in data.items()}
    except Exception:
        return {}


_PRINCIPAL_TEAMS = _load_principal_teams()


def _team_for_principal(principal: str) -> Optional[str]:
    """The team this principal belongs to, or None if it has no mapping.

    Keys may be written either as the full principal string the token resolves
    to ("Abe Roover <abe.roover@porticour.io>") or as just the address inside
    it. Pulling the address out of the angle brackets is a lookup normalization
    against the explicit map, not an inference: the domain is never consulted,
    and an address that is not in the map has no team.
    """
    team = _PRINCIPAL_TEAMS.get(principal)
    if team is not None:
        return team
    if "<" in principal and principal.rstrip().endswith(">"):
        address = principal[principal.index("<") + 1 : principal.rindex(">")].strip()
        return _PRINCIPAL_TEAMS.get(address)
    return None

# The requester is resolved at the ASGI layer (see PrincipalMiddleware) where the
# raw HTTP headers are reliably available. The bundled mcp SDK does not reliably
# expose request headers inside a tool, so we do not read them there. The
# ContextVar carries the per-request principal. If task context does not propagate,
# the safe fallback is unverified; never reuse another request's identity.
_current_principal: ContextVar[Optional[str]] = ContextVar(
    "current_principal", default=None
)
_current_adk_callback: ContextVar[
    Optional[AdkTemporalSessionCallback]
] = ContextVar("current_adk_callback", default=None)

# Result-wait tasks that outlived the gateway's sync budget. Kept referenced so the
# event loop does not garbage collect them mid-flight; discarded on completion.
_BACKGROUND_TASKS: set = set()


def _discard_background_task(task) -> None:
    _BACKGROUND_TASKS.discard(task)
    if not task.cancelled():
        # Retrieve any exception so asyncio does not warn that it went unretrieved.
        task.exception()


def _principal_for_token(token: Optional[str]) -> str:
    if token and _PRINCIPALS and token in _PRINCIPALS:
        return _PRINCIPALS[token]
    return UNVERIFIED_PRINCIPAL


def _adk_callback_from_headers(
    headers: list[tuple[bytes, bytes]],
    principal: str,
) -> AdkTemporalSessionCallback | None:
    """Resolve a fixed Temporal callback target from authenticated MCP headers."""

    if principal == UNVERIFIED_PRINCIPAL:
        return None
    values = {
        name.decode("latin-1").lower(): value.decode("latin-1").strip()
        for name, value in headers
    }
    workflow_id = values.get(CALLBACK_WORKFLOW_ID_HEADER, "")
    session_id = values.get(ADK_SESSION_ID_HEADER, "")
    run_id = values.get(CALLBACK_RUN_ID_HEADER) or None
    if not workflow_id or not session_id:
        return None
    identifiers = [workflow_id, session_id]
    if run_id:
        identifiers.append(run_id)
    if any(
        len(value) > 255
        or not all(ch.isalnum() or ch in "-_.:" for ch in value)
        for value in identifiers
    ):
        return None
    return AdkTemporalSessionCallback(
        workflow_id=workflow_id,
        run_id=run_id,
        session_id=session_id,
    )


class PrincipalMiddleware:
    """Reads the bearer token off each HTTP request and resolves the requester.

    This is the trust boundary: identity comes from a validated credential the
    gateway checks, never from anything the agent says.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        context_token = None
        callback_token = None
        if scope.get("type") == "http":
            headers = scope.get("headers", [])
            token: Optional[str] = None
            for name, value in headers:
                if name == b"authorization":
                    raw = value.decode("latin-1")
                    if raw.lower().startswith("bearer "):
                        token = raw[7:].strip()
                    break
            if token is None:
                for name, value in headers:
                    if name == b"cookie":
                        cookie = SimpleCookie()
                        cookie.load(value.decode("latin-1"))
                        morsel = cookie.get("gateway_token")
                        if morsel is not None:
                            token = morsel.value
                        break
            principal = _principal_for_token(token)
            context_token = _current_principal.set(principal)
            callback = _adk_callback_from_headers(headers, principal)
            callback_token = _current_adk_callback.set(callback)
            if GATEWAY_DEBUG:
                path = scope.get("path", "")
                print(
                    f"[auth] path={path} header_present={token is not None} "
                    f"resolved={principal!r} "
                    f"adk_callback={callback is not None}",
                    flush=True,
                )
        try:
            await self.app(scope, receive, send)
        finally:
            if callback_token is not None:
                _current_adk_callback.reset(callback_token)
            if context_token is not None:
                _current_principal.reset(context_token)


def _resolve_principal(ctx: Optional[Context] = None) -> str:
    if ctx is not None:
        try:
            request = ctx.request_context.request
            if request is not None:
                auth = request.headers.get("authorization", "")
                token = (
                    auth[7:].strip()
                    if auth.lower().startswith("bearer ")
                    else None
                )
                if token:
                    return _principal_for_token(token)
        except Exception:
            pass
    principal = _current_principal.get()
    return principal if principal is not None else UNVERIFIED_PRINCIPAL


def _is_approver(principal: str) -> bool:
    return principal in _APPROVERS


async def get_client() -> Client:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = await Client.connect(TEMPORAL_ADDRESS)
    return _client


def _as_dict(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, list):
        return [_as_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _as_dict(value) for key, value in obj.items()}
    return obj


def _session_key(ctx: Optional[Context]) -> Optional[str]:
    if ctx is None:
        return None
    try:
        request = ctx.request_context.request
        if request is not None:
            sid = request.headers.get("mcp-session-id")
            if sid:
                return sid
    except Exception:
        pass
    try:
        return f"obj-{id(ctx.session)}"
    except Exception:
        return None


def _resolve_workflow_id(
    explicit: Optional[str], ctx: Optional[Context]
) -> tuple[str, str]:
    if explicit:
        if _resolve_principal(ctx) == UNVERIFIED_PRINCIPAL:
            raise PermissionError(
                "explicit workflow_id requires an authenticated principal"
            )
        return explicit, "explicit_authorized"
    key = _session_key(ctx)
    if key is not None:
        wf = _session_chains.setdefault(key, "wf-" + uuid.uuid4().hex[:6])
        return wf, "inferred_from_session"
    return _DEFAULT_CHAIN, "gateway_default"


def _derive_idempotency_key(workflow_id: str, tool_name: str, arguments: dict) -> str:
    payload = json.dumps(
        {"w": workflow_id, "t": tool_name, "a": arguments}, sort_keys=True
    )
    return "idem-" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def _derive_operation_id(idempotency_key: str) -> str:
    # Deterministic from the idempotency key so the gateway knows the id up front
    # and it stays stable across retries and across a sync-to-async conversion.
    return "op-" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:16]


# Tools that read state rather than change it. A read has no effect to
# deduplicate, only a value that moves, so deriving its key from its arguments
# collapses every "what is deployed in prod" within a chain onto the first call
# and hands back a stale version that looks fresh: same operation_id, same
# executed_at, and idempotent_replay false, because the dedup happened in the
# Workflow and the tool backend was never asked. Mutations keep the derived key,
# which is what makes a retried cut_release safe.
#
# This belongs here and not in the Workflow's dedup check. The key is computed
# before the Update is sent and is carried in Event History, so replay reads back
# whatever this returned at the time. Branching on tool name inside
# request_tool_call would instead re-decide on every replay, and editing this set
# later would make in-flight histories replay down a different path.
_READ_ONLY_TOOLS = {"get_deployed_version"}


def _caller_idempotency_key(
    workflow_id: str,
    tool_name: str,
    arguments: dict,
    supplied: Optional[str],
    principal: Optional[str] = None,
) -> str:
    if supplied:
        resolved_principal = principal or _resolve_principal()
        payload = f"{resolved_principal}:{workflow_id}:{supplied}"
        return "idem-" + hashlib.sha256(payload.encode()).hexdigest()[:24]
    if tool_name in _READ_ONLY_TOOLS:
        # A caller that genuinely wants the earlier answer can still supply its
        # own key above and get the deduplicated operation back.
        return "idem-" + uuid.uuid4().hex[:24]
    return _derive_idempotency_key(workflow_id, tool_name, arguments)


_SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "password",
    "secret",
    "token",
}


def _safe_arguments(arguments: dict) -> dict:
    """Redact credential-like values before query, ledger, or UI exposure."""

    def redact(value: Any, key: str = "") -> Any:
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in _SENSITIVE_KEYS):
            return "[REDACTED]"
        if isinstance(value, dict):
            return {str(k): redact(v, str(k)) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value

    return redact(arguments)


async def _authorized_handle(workflow_id: str, ctx: Optional[Context]):
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    owner = await handle.query("get_workflow_owner", result_type=str)
    principal = _resolve_principal(ctx)
    if owner != principal:
        raise PermissionError("workflow_id is owned by a different principal")
    if principal == UNVERIFIED_PRINCIPAL:
        session_workflow_id, _ = _resolve_workflow_id(None, ctx)
        if session_workflow_id != workflow_id:
            raise PermissionError(
                "unverified callers may only access their current MCP session"
            )
    return handle


def _async_handle(
    workflow_id: str, operation_id: str
) -> ToolCallResponse:
    return ToolCallResponse(
        status="processing",
        workflow_id=workflow_id,
        operation_id=operation_id,
        message=(
            "The tool call is still running and was converted to async. Poll "
            "get_operation_result with this workflow_id and operation_id."
        ),
        poll_after_seconds=POLL_AFTER_SECONDS,
    )


def _requested_action(tool_name: str, arguments: dict) -> str:
    if tool_name == "promote_release":
        return (
            f"Promote {arguments.get('service', '?')} "
            f"{arguments.get('version', '?')} to {arguments.get('environment', '?')}"
        )
    if tool_name == "cut_release":
        return (
            f"Cut release {arguments.get('version', '?')} "
            f"of {arguments.get('service', '?')}"
        )
    if tool_name == "get_deployed_version":
        return f"Read the deployed version in {arguments.get('environment', '?')}"
    return f"Call {tool_name}"


async def _submit_tool_call(
    tool_name: str,
    arguments: dict,
    explicit_workflow_id: Optional[str],
    justification: Optional[str],
    ctx: Optional[Context],
    supplied_idempotency_key: Optional[str] = None,
    runtime: str = "ClaudeCode",
) -> ToolCallResponse:
    client = await get_client()

    workflow_id, source = _resolve_workflow_id(explicit_workflow_id, ctx)
    caller_principal = _resolve_principal(ctx)
    idem = _caller_idempotency_key(
        workflow_id,
        tool_name,
        arguments,
        supplied_idempotency_key,
        caller_principal,
    )
    operation_id = _derive_operation_id(idem)
    correlation = CorrelationContext(
        workflow_id=workflow_id,
        workflow_id_source=source,
        idempotency_key=idem,
        agent_session_id=_session_key(ctx),
        caller_principal=caller_principal,
        runtime=runtime,
        call_path=[runtime, tool_name],
    )
    req = ToolCallRequest(
        tool_name=tool_name,
        arguments=arguments,
        idempotency_key=idem,
        correlation=correlation,
        approval_timeout_seconds=APPROVAL_TIMEOUT_SECONDS,
        requested_action=_requested_action(tool_name, arguments),
        justification=justification,
        operation_id=operation_id,
        safe_arguments=_safe_arguments(arguments),
    )

    # Deliver the tool call as an Update. wait_for_stage=ACCEPTED returns the handle
    # once the workflow has accepted the update, so the gateway can then bound how
    # long it blocks on the result independently.
    start_op = WithStartWorkflowOperation(
        AgenticChainWorkflow.run,
        ChainInput(
            workflow_id=workflow_id,
            owner_principal=caller_principal,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    handle = await client.start_update_with_start_workflow(
        AgenticChainWorkflow.request_tool_call,
        req,
        wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        start_workflow_operation=start_op,
    )

    strategy = _TOOL_STRATEGY.get(tool_name, _DEFAULT_STRATEGY)

    if strategy == "async":
        # Async mode: hand back a poll handle immediately; the workflow runs on.
        return _async_handle(workflow_id, operation_id)

    if strategy == "sync":
        # Sync mode: block to completion.
        return await handle.result()

    # Convert mode: block up to the budget, then convert to async. asyncio.wait
    # stops blocking WITHOUT cancelling the result poll, so the Temporal SDK does
    # not raise a cancellation error and the workflow is unaffected. The result is
    # recovered later by polling get_operation_result.
    budget_seconds = float(strategy) / 1000.0
    result_task = asyncio.ensure_future(handle.result())
    done, _pending = await asyncio.wait({result_task}, timeout=budget_seconds)
    if result_task in done:
        return result_task.result()
    # Budget elapsed. Detach the still-running wait and hand back a poll handle.
    _BACKGROUND_TASKS.add(result_task)
    result_task.add_done_callback(_discard_background_task)
    if GATEWAY_DEBUG:
        print(
            f"[convert] tool={tool_name} op={operation_id} exceeded "
            f"{budget_seconds}s, converting to async",
            flush=True,
        )
    return _async_handle(workflow_id, operation_id)


# --------------------------------------------------------------------- MCP tools


@mcp.tool(structured_output=True)
async def get_deployed_version(
    environment: str,
    ctx: Context,
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Return the release version currently deployed in an environment.

    environment is one of staging or prod. Read only; never requires approval.
    """
    return await _submit_tool_call(
        "get_deployed_version",
        {"environment": environment},
        workflow_id or None,
        None,
        ctx,
        idempotency_key or None,
    )


@mcp.tool(structured_output=True)
async def cut_release(
    service: str,
    version: str,
    ctx: Context,
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Cut a release candidate for a service from an already built artifact.

    Registers a promotable release. Changes nothing running, so it never requires
    approval.
    """
    return await _submit_tool_call(
        "cut_release",
        {"service": service, "version": version},
        workflow_id or None,
        None,
        ctx,
        idempotency_key or None,
    )


@mcp.tool(structured_output=True)
async def promote_release(
    service: str,
    version: str,
    environment: str,
    ctx: Context,
    justification: str = "",
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Promote a release to an environment.

    Promotion to staging runs immediately. Promotion to prod requires human
    approval. Use justification for the business reason for the promotion; do not
    put requester identity in it, since the gateway records the authenticated
    requester independently.
    """
    return await _submit_tool_call(
        "promote_release",
        {"service": service, "version": version, "environment": environment},
        workflow_id or None,
        justification or None,
        ctx,
        idempotency_key or None,
    )


async def _submit_nested_release(
    service: str,
    environment: str,
    ctx: Context,
    tool1_mode: str,
    replay_safe: bool,
    justification: str,
    workflow_id: str,
    idempotency_key: str,
    version: str = "",
    bump: str = "",
) -> ToolCallResponse:
    """Start a CASE-2 Tool1 -> Tool2 nested release call.

    Shared by both CASE-2 entry points. Exactly one of version and bump is set:
    version names the release to promote up front, bump asks Tool1 to work it out
    from what is deployed. Everything downstream of that choice, including the
    nested-approval mechanics, is identical for both.
    """
    normalized_mode = tool1_mode.strip().lower()
    if normalized_mode not in {"controlled", "uncontrolled"}:
        raise ValueError("tool1_mode must be controlled or uncontrolled")
    if bool(version) == bool(bump):
        raise ValueError(
            "supply exactly one of version or bump: version promotes a named "
            "release, bump derives one from the deployed version"
        )
    normalized_bump = normalize_bump(bump) if bump else ""

    client = await get_client()
    resolved_workflow_id, source = _resolve_workflow_id(
        workflow_id or None, ctx
    )
    principal = _resolve_principal(ctx)
    tool1_name = "release_orchestrator"
    tool2_name = "promote_release"
    if normalized_bump:
        # No version yet: Tool1 reads the deployed version inside the workflow
        # and computes it. The idempotency key is derived from the bump type, so
        # it is stable across a retry that has not resolved a version yet, and a
        # duplicate call still dedups to the same operation pair rather than
        # deriving a second version and cutting a second release.
        tool1_arguments = {
            "service": service,
            "bump": normalized_bump,
            "environment": environment,
        }
        # The version key is deliberately absent. The workflow adds it to the
        # child operation once the release is actually cut, which is also when
        # the approver-facing sentence can name it.
        tool2_arguments = {"service": service, "environment": environment}
        requested_action = (
            f"Promote the next {normalized_bump} version of {service} "
            f"to {environment}"
        )
    else:
        tool1_arguments = {
            "service": service,
            "version": version,
            "environment": environment,
        }
        tool2_arguments = dict(tool1_arguments)
        requested_action = _requested_action(tool2_name, tool2_arguments)
    idem = _caller_idempotency_key(
        resolved_workflow_id,
        f"{tool1_name}->{tool2_name}",
        tool1_arguments,
        idempotency_key or None,
        principal,
    )
    parent_operation_id = _derive_operation_id(f"{idem}:tool1")
    child_operation_id = _derive_operation_id(f"{idem}:tool2")
    correlation = CorrelationContext(
        workflow_id=resolved_workflow_id,
        workflow_id_source=source,
        operation_id=child_operation_id,
        parent_operation_id=parent_operation_id,
        idempotency_key=idem,
        agent_session_id=_session_key(ctx),
        caller_principal=principal,
        runtime="ClaudeCode",
        call_path=["ClaudeCode", tool1_name],
    )
    request = NestedToolCallRequest(
        tool1_name=tool1_name,
        tool1_arguments=tool1_arguments,
        tool2_name=tool2_name,
        tool2_arguments=tool2_arguments,
        idempotency_key=idem,
        correlation=correlation,
        approval_timeout_seconds=APPROVAL_TIMEOUT_SECONDS,
        requested_action=requested_action,
        justification=justification or None,
        parent_operation_id=parent_operation_id,
        nested_operation_id=child_operation_id,
        bump=normalized_bump,
        controlled_tool1=normalized_mode == "controlled",
        replay_safe=replay_safe,
        safe_tool1_arguments=_safe_arguments(tool1_arguments),
        safe_tool2_arguments=_safe_arguments(tool2_arguments),
    )
    start_op = WithStartWorkflowOperation(
        AgenticChainWorkflow.run,
        ChainInput(
            workflow_id=resolved_workflow_id,
            owner_principal=principal,
        ),
        id=resolved_workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    update = await client.start_update_with_start_workflow(
        AgenticChainWorkflow.request_nested_tool_call,
        request,
        wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        start_workflow_operation=start_op,
    )
    return await update.result()


@mcp.tool(structured_output=True)
async def run_nested_release(
    service: str,
    version: str,
    environment: str,
    ctx: Context,
    tool1_mode: str = "controlled",
    replay_safe: bool = False,
    justification: str = "",
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Run Tool1 -> Tool2 where Tool2 is a release promotion.

    A controlled Tool1 checkpoints and resumes automatically after approval.
    An uncontrolled Tool1 fails closed; approval is recorded, but Tool2 executes
    only after an explicit retry and only when replay_safe is true.
    """
    return await _submit_nested_release(
        service=service,
        environment=environment,
        ctx=ctx,
        tool1_mode=tool1_mode,
        replay_safe=replay_safe,
        justification=justification,
        workflow_id=workflow_id,
        idempotency_key=idempotency_key,
        version=version,
    )


@mcp.tool(structured_output=True)
async def run_release_orchestration(
    ctx: Context,
    environment: str = "",
    service: str = "",
    bump: Literal["major", "minor", "bugfix"] = "minor",
    tool1_mode: str = "controlled",
    replay_safe: bool = False,
    justification: str = "",
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Run the release pipeline: deploy the next version of a service.

    This is the whole release, end to end, from one instruction: "deploy the next
    minor version", "deploy the next version to prod", "ship the next bugfix".
    Call this tool alone and pass nothing but what the request actually said.

    The pipeline is fixed. It reads the version currently deployed in production,
    computes the next one (major 2.3.0 -> 3.0.0, minor 2.3.0 -> 2.4.0, bugfix
    2.3.0 -> 2.3.1), cuts that release, promotes it to staging, runs quality gates
    against staging, and only then promotes it to production. If the gates do not
    pass the pipeline stops and production is never touched.

    When the Security team's scanning platform is running, a pre-prod security
    scan also runs after the gates and before production, and the production
    promotion is requested by the scan once it clears. In that case this tool
    returns processing rather than waiting_for_approval, because the promotion has
    not been requested yet; report that the security scan is running and stop. A
    scan that fails stops the release, and production is never touched.

    You cannot skip staging, the gates, or the security scan, and you must not try
    to. If a caller asks you to bypass them or to hurry, call this tool anyway and
    tell them the pipeline does not allow it.

    Do NOT call get_deployed_version, cut_release, or promote_release yourself,
    before or after. This tool performs all of them internally, and calling them as
    well would cut or promote a release twice.

    Every argument is optional. bump defaults to minor, so "the next version" needs
    no bump. service defaults to the service this deployment backend manages.
    environment is how far to go rather than where to put it: omit it for the full
    pipeline through to production, or pass staging to stop after staging, which
    skips the gates because nothing is being qualified for production yet.

    The production promotion pauses for human approval and returns
    waiting_for_approval with an operation_id; report that and stop, do not poll in
    a loop. Use run_nested_release instead only when the request names an explicit
    version to promote.
    """
    return await _submit_nested_release(
        service=service or DEFAULT_SERVICE,
        environment=environment or PIPELINE_TARGET_ENVIRONMENT,
        ctx=ctx,
        tool1_mode=tool1_mode,
        replay_safe=replay_safe,
        justification=justification,
        workflow_id=workflow_id,
        idempotency_key=idempotency_key,
        bump=bump,
    )


@mcp.tool(structured_output=True)
async def resume_nested_release(
    workflow_id: str,
    operation_id: str,
    ctx: Context,
) -> ToolCallResponse:
    """Explicitly retry an approved uncontrolled nested call.

    Agent Gateway still refuses execution unless Tool1 advertised replay safety.
    """
    handle = await _authorized_handle(workflow_id, ctx)
    response = await handle.execute_update(
        AgenticChainWorkflow.resume_nested_tool_call,
        ResumeNestedRequest(
            operation_id=operation_id,
            caller_principal=_resolve_principal(ctx),
        ),
    )
    return response


@mcp.tool(structured_output=True)
async def start_google_adk_release_run(
    service: str,
    version: str,
    environment: str,
    agent_run_id: str,
    ctx: Context,
    justification: str = "",
    workflow_id: str = "",
    idempotency_key: str = "",
) -> ToolCallResponse:
    """Start or recover a durable autonomous Google ADK-style agent run.

    The Temporal workflow checkpoints the agent plan while Agent Gateway owns the
    approval decision. Dependent external work is executed only after the
    protected promotion completes.
    """
    principal = _resolve_principal(ctx)
    callback = _current_adk_callback.get()
    if principal == UNVERIFIED_PRINCIPAL:
        raise PermissionError(
            "autonomous agents must authenticate to Agent Gateway"
        )
    if not agent_run_id:
        raise ValueError("agent_run_id is required")
    if workflow_id:
        resolved_workflow_id, source = _resolve_workflow_id(workflow_id, ctx)
    else:
        seed = f"{principal}:{agent_run_id}"
        resolved_workflow_id = (
            "wf-adk-" + hashlib.sha256(seed.encode()).hexdigest()[:16]
        )
        source = "inferred_from_agent_run"
    arguments = {
        "service": service,
        "version": version,
        "environment": environment,
    }
    idem = _caller_idempotency_key(
        resolved_workflow_id,
        "promote_release",
        {**arguments, "agent_run_id": agent_run_id},
        idempotency_key or agent_run_id,
        principal,
    )
    operation_id = _derive_operation_id(idem)
    correlation = CorrelationContext(
        workflow_id=resolved_workflow_id,
        workflow_id_source=source,
        operation_id=operation_id,
        idempotency_key=idem,
        agent_session_id=(
            callback.session_id if callback is not None else _session_key(ctx)
        ),
        agent_run_id=agent_run_id,
        caller_service="google-adk-agent",
        caller_principal=principal,
        runtime="GoogleADKAgent",
        call_path=["GoogleADKAgent", "AgentGateway", "promote_release"],
    )
    input = AutonomousAgentInput(
        workflow_id=resolved_workflow_id,
        operation_id=operation_id,
        idempotency_key=idem,
        owner_principal=principal,
        agent_run_id=agent_run_id,
        agent_identity=principal,
        tool_name="promote_release",
        arguments=arguments,
        safe_arguments=_safe_arguments(arguments),
        requested_action=_requested_action("promote_release", arguments),
        justification=justification or None,
        correlation=correlation,
        approval_timeout_seconds=APPROVAL_TIMEOUT_SECONDS,
        callback=callback,
    )
    client = await get_client()
    handle = await client.start_workflow(
        AutonomousAgentWorkflow.run,
        input,
        id=resolved_workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    if callback is not None:
        await handle.signal(
            AutonomousAgentWorkflow.register_callback,
            callback,
        )
    owner = await handle.query(
        AutonomousAgentWorkflow.get_workflow_owner
    )
    if owner != principal:
        raise PermissionError("workflow_id is owned by a different principal")

    # The first Workflow Task performs policy evaluation. Briefly wait for the
    # externally useful pause/completion state without tying agent liveness to the
    # caller connection.
    for _ in range(100):
        response = await handle.query(
            AutonomousAgentWorkflow.get_operation_result,
            operation_id,
        )
        if response.status not in {"processing"}:
            return response
        await asyncio.sleep(0.05)
    return response


@mcp.tool(structured_output=True)
async def get_operation_status(
    workflow_id: str, operation_id: str, ctx: Context
) -> ToolCallResponse:
    """Return the current status of one approval-gated operation."""
    handle = await _authorized_handle(workflow_id, ctx)
    resp = await handle.query(
        "get_operation_status",
        operation_id,
        result_type=ToolCallResponse,
    )
    return resp


@mcp.tool(structured_output=True)
async def get_operation_result(
    workflow_id: str, operation_id: str, ctx: Context
) -> ToolCallResponse:
    """Return the result of an operation once it has completed."""
    handle = await _authorized_handle(workflow_id, ctx)
    resp = await handle.query(
        "get_operation_result",
        operation_id,
        result_type=ToolCallResponse,
    )
    return resp


@mcp.tool(structured_output=True)
async def get_workflow_status(
    workflow_id: str, ctx: Context
) -> WorkflowStatusResponse:
    """Return a summary of the whole agentic chain."""
    handle = await _authorized_handle(workflow_id, ctx)
    summary = await handle.query("get_workflow_status")
    raw = _as_dict(summary)
    return WorkflowStatusResponse(
        workflow_id=raw["workflow_id"],
        run_id=raw["run_id"],
        total_operations=raw["total_operations"],
        status_counts=raw["status_counts"],
        operations=[
            op if isinstance(op, OperationView) else OperationView(**op)
            for op in raw["operations"]
        ],
        closing=raw.get("closing", False),
        agent_run_id=raw.get("agent_run_id"),
        agent_identity=raw.get("agent_identity"),
        checkpoint=raw.get("checkpoint", {}),
        dependent_action_executed=raw.get(
            "dependent_action_executed", False
        ),
    )


@mcp.tool(structured_output=True)
async def get_workflow_ledger(
    workflow_id: str, ctx: Context
) -> WorkflowLedgerResponse:
    """Return the durable approval and resume ledger for later review."""
    handle = await _authorized_handle(workflow_id, ctx)
    entries = await handle.query("get_ledger")
    raw_entries = _as_dict(entries)
    return WorkflowLedgerResponse(
        workflow_id=workflow_id,
        entries=[
            entry
            if isinstance(entry, LedgerEntry)
            else LedgerEntry(**entry)
            for entry in raw_entries
        ],
    )


@mcp.tool(structured_output=True)
async def cancel_operation(
    workflow_id: str,
    operation_id: str,
    ctx: Context,
    reason: str = "",
) -> ToolCallResponse:
    """Cancel a pending operation without invoking its protected action."""
    handle = await _authorized_handle(workflow_id, ctx)
    await handle.signal(
        "cancel_operation",
        CancelOperation(
            operation_id=operation_id,
            canceled_by=_resolve_principal(ctx),
            reason=reason or "Canceled by caller",
        ),
    )
    response = await handle.query(
        "get_operation_status",
        operation_id,
        result_type=ToolCallResponse,
    )
    return response


# ------------------------------------------------------- approver UI and actions


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


@mcp.custom_route("/whoami", methods=["GET"])
async def whoami(request: Request) -> JSONResponse:
    """Diagnostic. Reports what the gateway resolves for the incoming request.

    Test the gateway side directly (bypasses Claude Code):
      curl -s -H "Authorization: Bearer tok_dustin" http://localhost:8080/whoami
    If this returns your identity, the gateway, middleware, and principal map all
    work, and any remaining "unverified" is Claude Code not sending the header.
    A 404 here means the container is running an older image; rebuild it.
    """
    auth = request.headers.get("authorization", "")
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else None
    return JSONResponse(
        {
            "authorization_header_present": bool(auth),
            "principals_loaded": len(_PRINCIPALS),
            "approvers_loaded": len(_APPROVERS),
            "resolved_from_this_request": _principal_for_token(token),
            "resolved_from_middleware_contextvar": _current_principal.get(),
            "is_approver": _is_approver(_resolve_principal()),
            # Which Porticour team this token approves as. Only matters for an
            # operation that carries a team restriction; null everywhere else.
            "team": _team_for_principal(_principal_for_token(token)),
        }
    )


@mcp.custom_route("/login", methods=["GET", "POST"])
async def login(request: Request) -> Response:
    if request.method == "GET":
        return HTMLResponse(_render_login())
    form = await request.form()
    token = str(form.get("token") or "")
    principal = _principal_for_token(token)
    if not token or not _is_approver(principal):
        return HTMLResponse(
            _render_login("That token is not authorized for approvals."),
            status_code=403,
        )
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        "gateway_token",
        token,
        httponly=True,
        samesite="strict",
        secure=False,
        max_age=8 * 60 * 60,
    )
    return response


@mcp.custom_route("/logout", methods=["POST"])
async def logout(request: Request) -> RedirectResponse:
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("gateway_token")
    return response


@mcp.custom_route("/", methods=["GET"])
async def dashboard(request: Request) -> HTMLResponse:
    if not _is_approver(_resolve_principal()):
        return HTMLResponse(_render_login(), status_code=401)
    client = await get_client()
    pending: list[tuple[str, Any]] = []
    history: list[tuple[str, Any]] = []
    queries = [
        (
            'WorkflowType = "AgenticChainWorkflow"',
            AgenticChainWorkflow.get_workflow_status,
        ),
        (
            'WorkflowType = "AutonomousAgentWorkflow"',
            AutonomousAgentWorkflow.get_workflow_status,
        ),
    ]
    for query, status_query in queries:
        async for wf in client.list_workflows(query):
            handle = client.get_workflow_handle(wf.id, run_id=wf.run_id)
            try:
                summary = await handle.query(status_query)
            except Exception:
                continue
            for op in summary.operations:
                if op.status == "waiting_for_approval":
                    pending.append((wf.id, op))
                elif op.status in TERMINAL_STATUSES:
                    history.append((wf.id, op))
    history.sort(key=lambda row: (row[1].decided_iso or ""), reverse=True)
    return HTMLResponse(_render_dashboard(pending, history))


def _fetch_fleet_state() -> dict:
    resp = requests.get(MOCK_TOOL_STATE_URL, timeout=3)
    resp.raise_for_status()
    return resp.json()


@mcp.custom_route("/fleet", methods=["GET"])
async def fleet(request: Request) -> JSONResponse:
    """Fleet state for the dashboard panel, polled on its own cadence.

    Separate from the dashboard HTML so a promotion can be animated in place
    instead of appearing after a full page reload. Read only, and gated on the
    same approver identity as the dashboard.
    """
    if not _is_approver(_resolve_principal()):
        return JSONResponse({"error": "not authorized"}, status_code=401)
    try:
        # requests is synchronous; run it off the event loop so a slow or hung
        # deployment backend cannot stall the MCP transport on the same port.
        state = await asyncio.to_thread(_fetch_fleet_state)
    except Exception as err:
        return JSONResponse(
            {
                "available": False,
                "error": f"deployment backend unreachable: {err}",
                "environments": [],
                "releases": [],
            }
        )
    state["available"] = True
    state["protected_environments"] = sorted(PROTECTED_ENVIRONMENTS)
    return JSONResponse(state)


async def _authorize_approver(handle, operation_id: str, principal: str) -> None:
    """Refuse a decision from someone not on the team this operation requires.

    Being in GATEWAY_APPROVERS is necessary and, for almost everything,
    sufficient: CASE-1's ordinary production promotions, CASE-2a runs with no
    security scan in play, and CASE-3 runs all carry no team restriction and are
    decided by any approver, unchanged.

    What is new is the operation a Security scan asked for. Promoting to
    production off the back of a pre-prod security scan is the Security team's
    call to stand behind, so a member of that team has to be the one who makes
    it. That authorization requirement is precisely what makes the Security
    team's own workflow the only correct caller for the promotion: the requester
    identity on the operation genuinely reflects who is accountable, rather than
    being a decision relayed on their behalf.

    Raised as a PermissionError, and turned into a 403 by the caller, so an
    approver who cannot act on an entry finds out immediately instead of
    watching a decision quietly fail to register.
    """
    required = await handle.query(
        "get_required_approver_team", operation_id, result_type=str
    )
    if not required:
        return
    team = _team_for_principal(principal)
    if team != required:
        raise PermissionError(
            f"operation {operation_id} requires approval from the {required} "
            f"team; {principal!r} is not authorized to decide it"
        )


def _decision_forbidden(err: PermissionError) -> HTMLResponse:
    return HTMLResponse(_render_forbidden(str(err)), status_code=403)


@mcp.custom_route("/approve", methods=["POST"])
async def approve(request: Request) -> Response:
    principal = _resolve_principal()
    if not _is_approver(principal):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    operation_id = str(form["operation_id"])
    client = await get_client()
    handle = client.get_workflow_handle(str(form["workflow_id"]))
    try:
        await _authorize_approver(handle, operation_id, principal)
    except PermissionError as err:
        return _decision_forbidden(err)
    await handle.signal(
        "approve_operation",
        ApprovalDecision(
            operation_id=operation_id,
            approver=principal,
            # Resolved here, at the trust boundary, from the validated bearer
            # token. The Workflow enforces the match but does not decide team
            # membership: that is environment-driven configuration, which
            # Workflow code must not read.
            approver_team=_team_for_principal(principal) or "",
        ),
    )
    return RedirectResponse("/", status_code=303)


@mcp.custom_route("/reject", methods=["POST"])
async def reject(request: Request) -> Response:
    principal = _resolve_principal()
    if not _is_approver(principal):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    operation_id = str(form["operation_id"])
    client = await get_client()
    handle = client.get_workflow_handle(str(form["workflow_id"]))
    try:
        await _authorize_approver(handle, operation_id, principal)
    except PermissionError as err:
        return _decision_forbidden(err)
    await handle.signal(
        "reject_operation",
        ApprovalDecision(
            operation_id=operation_id,
            approver=principal,
            reason=str(form.get("reason") or "Rejected by approver"),
            approver_team=_team_for_principal(principal) or "",
        ),
    )
    return RedirectResponse("/", status_code=303)


# --------------------------------------------------------------------- rendering


def _fmt(value: Optional[str]) -> str:
    return html.escape(value) if value else ""


def _args_summary(arguments: dict) -> str:
    return html.escape(", ".join(f"{k}={v}" for k, v in arguments.items()))


def _result_summary(result: Any) -> str:
    if result is None:
        return ""
    if isinstance(result, dict):
        msg = result.get("message")
        if msg:
            return html.escape(str(msg))
        return html.escape(json.dumps(result))
    return html.escape(str(result))


def _short_ts(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso).strftime("%m-%d %H:%M:%S")
    except Exception:
        return iso.split(".")[0].replace("T", " ")


def _team_badge(op: Any) -> str:
    """Label an entry only this team's approvers may decide.

    Shown so a non-Security approver understands why they cannot act on the
    entry before they try and are refused, rather than the restriction only
    surfacing as a 403 after the fact.
    """
    required = getattr(op, "required_approver_team", None)
    if not required:
        return ""
    return (
        f'<div class="team-req">{html.escape(required.title())} approval '
        "required</div>"
    )


def _render_pending_row(workflow_id: str, op: Any) -> str:
    wf = html.escape(workflow_id)
    op_id = html.escape(op.operation_id)
    deadline = op.deadline_epoch or 0
    return f"""
    <tr>
      <td class="mono">{wf}</td>
      <td class="mono">{op_id}</td>
      <td>{_fmt(op.requested_action)}{_team_badge(op)}</td>
      <td>{_fmt(op.requester)}</td>
      <td class="mono">{_args_summary(op.arguments)}</td>
      <td>{_fmt(op.justification)}</td>
      <td>{_fmt(op.risk_reason)}</td>
      <td class="mono">{html.escape(" -> ".join(op.call_path))}</td>
      <td class="mono">{html.escape(_short_ts(op.created_iso))}</td>
      <td class="mono"><span class="countdown" data-deadline="{deadline}"></span></td>
      <td>
        <form method="post" action="/approve" class="inline">
          <input type="hidden" name="workflow_id" value="{wf}">
          <input type="hidden" name="operation_id" value="{op_id}">
          <button class="approve" type="submit">Approve</button>
        </form>
        <form method="post" action="/reject" class="inline">
          <input type="hidden" name="workflow_id" value="{wf}">
          <input type="hidden" name="operation_id" value="{op_id}">
          <input type="text" name="reason" class="reason" placeholder="reason for rejection">
          <button class="reject" type="submit">Reject</button>
        </form>
      </td>
    </tr>
    """


def _render_history_row(workflow_id: str, op: Any) -> str:
    wf = html.escape(workflow_id)
    prot = "Y" if op.protected else "N"
    prot_class = "prot-y" if op.protected else "prot-n"
    reason = op.decision_reason
    if not reason and op.status == "expired":
        reason = "approval timed out"
    return f"""
    <tr data-wf="{wf}">
      <td class="mono">{html.escape(_short_ts(op.decided_iso))}</td>
      <td class="mono">{wf}</td>
      <td>{_fmt(op.requester)}</td>
      <td class="mono">{html.escape(op.operation_id)}</td>
      <td class="mono">{html.escape(op.tool_name)}</td>
      <td class="mono">{html.escape(" -> ".join(op.call_path))}</td>
      <td class="{prot_class}">{prot}</td>
      <td>{_fmt(op.requested_action)}</td>
      <td>{_result_summary(op.result)}</td>
      <td><span class="badge badge-{html.escape(op.status)}">{html.escape(op.status)}</span></td>
      <td>{_fmt(op.approver)}</td>
      <td>{_fmt(reason)}</td>
    </tr>
    """


_CSS = """
:root {
  --bg: #0f1117; --fg: #e6e8eb; --muted: #8b93a1; --border: #232733;
  --accent: #13c4b0; --th: #8b93a1; --field: #161a22;
  --card: #151924; --card-edge: #1c2130; --protect: #f59e0b;
  --on-accent: #04211e; --accent-glow: rgba(19,196,176,0.34);
  --accent-wash: rgba(19,196,176,0.16); --shine: rgba(19,196,176,0.16);
}
[data-theme="light"] {
  --bg: #ffffff; --fg: #0f1117; --muted: #6b7280; --border: #e5e7eb;
  --accent: #0f9b8e; --th: #6b7280; --field: #ffffff;
  --card: #fbfcfd; --card-edge: #eef1f4; --protect: #b45309;
  --on-accent: #ffffff; --accent-glow: rgba(15,155,142,0.26);
  --accent-wash: rgba(15,155,142,0.12); --shine: rgba(15,155,142,0.12);
}
body { background: var(--bg); color: var(--fg); font-family: Inter, system-ui, sans-serif; margin: 2rem; }
header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
header h1 { margin: 0; }
.brandline { margin: 0.25rem 0 0; max-width: 44rem; }
h1 { font-size: 1.25rem; }
h2 { font-size: 1rem; margin: 0; }
table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }
th, td { text-align: left; padding: 0.45rem 0.6rem; border-bottom: 1px solid var(--border); vertical-align: top; font-size: 0.85rem; }
th { font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--th); }
.mono { font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 0.74rem; }
.muted { color: var(--muted); font-size: 0.82rem; }
.empty { color: var(--muted); text-align: center; padding: 1.5rem; }
.inline { display: block; margin: 0 0 0.35rem 0; }
input[type=text] { padding: 0.28rem 0.4rem; border: 1px solid var(--border); border-radius: 4px; background: var(--field); color: var(--fg); }
input.reason { width: 15rem; }
button { padding: 0.3rem 0.7rem; border: 0; border-radius: 4px; color: white; cursor: pointer; margin-left: 0.25rem; }
.approve { background: #13c4b0; }
.reject { background: #ef4444; }
.toggle { background: transparent; border: 1px solid var(--border); color: var(--fg); }
.countdown.urgent { color: #ef4444; font-weight: 600; }
/* Only one Porticour team may decide this entry. Reads as a constraint on the
   row rather than a status of the operation, so it sits under the requested
   action instead of in the outcome column. */
.team-req { display: inline-block; margin-top: 0.3rem; padding: 0.12rem 0.45rem;
        border: 1px solid var(--protect); border-radius: 999px; color: var(--protect);
        font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.06em; }
.forbidden { max-width: 34rem; margin: 12vh auto; padding: 2rem;
        border: 1px solid #ef4444; border-radius: 8px; }
.forbidden h1 { color: #ef4444; }
.prot-y { color: #f59e0b; font-weight: 600; }
.prot-n { color: var(--muted); }
.badge { padding: 0.1rem 0.45rem; border-radius: 999px; font-size: 0.7rem; color: white; }
.badge-completed { background: #13c4b0; }
.badge-rejected { background: #ef4444; }
.badge-expired { background: #f59e0b; }
.badge-failed { background: #6b7280; }
.filterbar { display: flex; gap: 0.5rem; align-items: center; margin-top: 1.75rem; }
.login { max-width: 28rem; margin: 12vh auto; padding: 2rem; border: 1px solid var(--border); border-radius: 8px; }
.login input { width: 100%; box-sizing: border-box; margin: 0.5rem 0 1rem; }

/* --------------------------------------------------------- release fleet panel
   Cards, not a table. The approval queue below answers "what needs a decision";
   this answers "what is running", so it is deliberately a different visual form
   while sharing the palette, radii, and type scale. */
/* One row: what is ready, then the environments it can move through. Bounded so
   the cards stay card-shaped instead of stretching across the whole window. */
.fleet { margin-top: 1.5rem; --fleet-width: 66rem; }
.fleet:has(.gate) { --fleet-width: 80rem; }
.fleet .subtle, .fleet .fleet-flow { max-width: var(--fleet-width); }
.fleet-flow { display: flex; align-items: stretch; margin-top: 0.85rem; }
.ready { flex: 0 0 13.5rem; min-width: 0; }
.ready .rail-head { margin-top: 0; }
.fleet-service { font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 1.05rem;
       font-weight: 600; letter-spacing: -0.01em; }
.section-head { display: flex; align-items: center; gap: 0; }
.section-head .live { margin-left: 0.7rem; }
.eyebrow { font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--th); }
.subtle { margin: 0.3rem 0 0; font-size: 0.78rem; }
.divider { border: 0; border-top: 1px solid var(--border); margin: 1.9rem 0 0; }
.queue-head { margin-top: 1.6rem; }
.live { display: inline-flex; align-items: center; gap: 0.38rem; font-size: 0.68rem;
        text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
.live-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--accent); animation: dot-pulse 2.6s ease-out infinite; }
.live.stale { color: var(--protect); text-transform: none; letter-spacing: 0; }
.live.stale .live-dot { background: var(--protect); animation: none; }
@keyframes dot-pulse {
  0% { box-shadow: 0 0 0 0 var(--accent-glow); }
  70% { box-shadow: 0 0 0 6px rgba(0,0,0,0); }
  100% { box-shadow: 0 0 0 0 rgba(0,0,0,0); }
}

/* Ready -> staging -> production, left to right. The first connector carries the
   divider that separates what is promotable from what is running. */
.pipeline { display: flex; flex: 1 1 0; min-width: 0; }
.link { flex: 0 0 2.6rem; position: relative; display: flex; align-items: center; }
.link[data-into="gate"], .pipeline:has(.gate) .link[data-into="prod"] { flex-basis: 1.8rem; }
.link:first-child { flex-basis: 3.1rem; margin-right: 0.9rem; }
.link:first-child .link-line { margin-right: 0.55rem; }
.link:first-child::after { right: 0.55rem; }
/* The pipe: promotable on the left, running on the right. Faded at both ends so a
   full-height rule does not cut the row in half. */
.link:first-child::before { content: ""; position: absolute; right: 0; top: -0.35rem; bottom: -0.35rem;
       width: 1px; background: linear-gradient(180deg, transparent, var(--th), transparent); opacity: 0.8; }
.link-line { position: relative; flex: 1 1 auto; height: 2px; border-radius: 2px;
             background: var(--border); overflow: hidden; }
.link-line::after { content: ""; position: absolute; inset: 0; opacity: 0;
             background: linear-gradient(90deg, transparent, var(--accent), transparent); }
.link.flowing .link-line::after { animation: flow 0.9s ease-in-out 2; }
.link::after { content: ""; position: absolute; right: 0; top: 50%; margin-top: -4px;
             border-top: 4px solid transparent; border-bottom: 4px solid transparent;
             border-left: 6px solid var(--border); }
.link.flowing::after { border-left-color: var(--accent); }
@keyframes flow { from { transform: translateX(-100%); opacity: 1; } to { transform: translateX(100%); opacity: 1; } }

.env { flex: 1 1 0; min-width: 0; position: relative; overflow: hidden; --stripe: var(--accent);
       padding: 0.85rem 1.1rem 0.95rem; border: 1px solid var(--card-edge); border-radius: 14px;
       background: var(--card); transition: border-color 0.5s ease, box-shadow 0.5s ease; }
.env.protected { --stripe: var(--protect); }
.env::before { content: ""; position: absolute; inset: 0; pointer-events: none; opacity: 0;
       background: radial-gradient(90% 130% at 22% 130%, var(--accent-wash), transparent 65%); }
.env::after { content: ""; position: absolute; left: 0; right: 0; top: 0; height: 2px;
       opacity: 0.55; background: linear-gradient(90deg, transparent, var(--stripe), transparent); }
/* Wraps rather than truncates: once the gate card is in the row there is not
   always room for "PRODUCTION" and "approval required" side by side, and the tag
   dropping to its own line reads better than either being cut off. */
.env-top { display: flex; align-items: center; justify-content: space-between;
       gap: 0.2rem 0.5rem; flex-wrap: wrap; }
.env-body { min-width: 0; }
.env-side { min-width: 0; }
.env-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
       font-size: 0.68rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.12em; color: var(--th); }
.tag { flex: none; white-space: nowrap; font-size: 0.6rem; text-transform: uppercase; letter-spacing: 0.06em;
       padding: 0.14rem 0.44rem; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
.tag-protected { color: var(--protect); border-color: var(--protect); }
/* Clips the version swap so it reads as an odometer roll rather than two numbers
   drifting over the rest of the card. */
.version-wrap { position: relative; flex: 1 1 auto; height: 2.15rem; margin: 0.45rem 0 0.05rem; overflow: hidden; }
.version { position: absolute; left: 0; right: 0; top: 0; display: flex; align-items: center; height: 100%;
       font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 1.5rem; font-weight: 600;
       letter-spacing: -0.02em; font-variant-numeric: tabular-nums; white-space: nowrap; }
.env-service { font-size: 0.74rem; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.env-meta { display: flex; align-items: center; gap: 0.45rem;
       margin-top: 0.3rem; font-size: 0.68rem; color: var(--muted); }
.env-meta .from { font-family: "JetBrains Mono", ui-monospace, monospace; }
.env-meta .sep { opacity: 0.5; }
.shine { position: absolute; top: 0; bottom: 0; left: -60%; width: 45%; opacity: 0; pointer-events: none;
       background: linear-gradient(100deg, transparent, var(--shine), transparent); }

.env.promoting { border-color: var(--accent); box-shadow: 0 14px 38px -18px var(--accent-glow); }
.env.promoting::before { animation: wash 1.9s ease-out; }
.version.out { animation: v-out 0.42s cubic-bezier(0.4,0,1,1) forwards; }
/* Slight delay so the outgoing version clears before the new one arrives rather
   than the two crossing over each other. */
.version.in { animation: v-in 0.6s cubic-bezier(0.16,1,0.3,1) 0.14s both; }
.env.protected.promoting .shine { animation: shine 1.3s cubic-bezier(0.22,1,0.36,1) 0.08s; }
@keyframes wash { 0% { opacity: 0; } 18% { opacity: 1; } 100% { opacity: 0; } }
@keyframes v-out { 0% { transform: translateY(0); opacity: 1; filter: blur(0); }
                  70% { opacity: 0; }
                  100% { transform: translateY(-105%); opacity: 0; filter: blur(4px); } }
@keyframes v-in { 0% { transform: translateY(105%); opacity: 0; filter: blur(4px); }
                  55% { opacity: 1; }
                  100% { transform: translateY(0); opacity: 1; filter: blur(0); } }
@keyframes shine { 0% { left: -60%; opacity: 0; } 22% { opacity: 1; } 100% { left: 115%; opacity: 0; } }

/* ------------------------------------------------------- quality gate card
   Always on the row, from container startup, in whichever of idle / running /
   passed / failed is honestly true. Gates are not a capability that appears
   partway through the demo: they react whenever anything reaches staging, from
   any phase, so an empty card saying "no candidate staged yet" is the accurate
   picture at t=0 rather than something to hide. Narrower than an environment
   card and visually a checkpoint rather than a destination: nothing is deployed
   here, it is the thing a candidate has to get past.

   Only the SECURITY SCAN card keeps the first-appearance reveal, because that
   one really does arrive mid-demo when another team's platform starts. */
.gate { flex: 0 0 11rem; min-width: 0; position: relative; overflow: hidden;
        padding: 0.7rem 0.8rem 0.8rem; border: 1px dashed var(--card-edge);
        border-radius: 14px; background: var(--card);
        transition: border-color 0.4s ease, box-shadow 0.4s ease; }
.gate.scan.appearing { animation: gate-in 0.6s cubic-bezier(0.16,1,0.3,1); }
@keyframes gate-in { from { transform: scale(0.9); opacity: 0; } to { transform: none; opacity: 1; } }
/* Nothing staged yet. Deliberately quiet: present, legible, and clearly not
   reporting a verdict about anything. */
.gate.idle { border-style: dashed; border-color: var(--card-edge); opacity: 0.75; }
.gate.idle .gate-verdict { color: var(--muted); }
.gate-top { display: flex; align-items: center; justify-content: space-between; gap: 0.4rem; }
.gate-name { font-size: 0.66rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.1em; color: var(--th); }
.gate-verdict { font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 0.95rem;
        font-weight: 600; letter-spacing: -0.01em; margin: 0.4rem 0 0.1rem; }
.gate-sub { font-size: 0.66rem; color: var(--muted); }
.gate-checks { list-style: none; margin: 0.45rem 0 0; padding: 0; display: flex;
        flex-direction: column; gap: 0.18rem; }
.gate-checks li { display: flex; align-items: baseline; gap: 0.35rem; font-size: 0.66rem;
        line-height: 1.25; color: var(--muted); }
.gate-checks li span:last-child { min-width: 0; }
.gate-checks .mark { flex: none; font-weight: 700; }
.gate-checks li.ok .mark { color: var(--accent); }
.gate-checks li.bad .mark { color: #ef4444; }
/* Running: dashed border animates, verdict is a working label. */
.gate.running { border-color: var(--accent); }
.gate.running::after { content: ""; position: absolute; left: 0; right: 0; bottom: 0; height: 2px;
        background: linear-gradient(90deg, transparent, var(--accent), transparent);
        animation: gate-scan 1.4s linear infinite; }
@keyframes gate-scan { from { transform: translateX(-100%); } to { transform: translateX(100%); } }
.gate.passed { border-style: solid; border-color: var(--accent);
        box-shadow: 0 12px 30px -20px var(--accent-glow); }
.gate.passed .gate-verdict { color: var(--accent); }
.gate.failed { border-style: solid; border-color: #ef4444; }
.gate.failed .gate-verdict { color: #ef4444; }
/* A failed gate stops the flow, so the outbound connector reads as dead. */
.link.blocked .link-line { background: #ef4444; opacity: 0.5; }
.link.blocked::after { border-left-color: #ef4444; opacity: 0.5; }

/* ---------------------------------------------------- security scan card
   Absent for the whole of CASE-1 and CASE-2a, and appears in place the first
   time the Security team runs a scan, using the same shape-change reveal the
   gate card uses. It sits between the gate and production, because that is
   where it sits in the pipeline: the last checkpoint before the protected
   promotion. Marked with its owner, because unlike every other card in this row
   it is not the Waypoint team's -- it belongs to another Porticour engineering
   team, running in another Temporal namespace, and the demo's whole CASE-2b
   beat is that distinction. */
.gate.scan { flex: 0 0 12rem; }
.gate-owner { margin-top: 0.05rem; font-size: 0.58rem; text-transform: uppercase;
        letter-spacing: 0.09em; color: var(--muted); opacity: 0.85; }
.scan-stages { display: flex; align-items: center; gap: 0.28rem; margin: 0.5rem 0 0.35rem; }
.scan-dot { flex: 0 0 auto; width: 0.5rem; height: 0.5rem; border-radius: 50%;
        border: 1px solid var(--card-edge); background: transparent; }
.scan-dot.ok { background: var(--accent); border-color: var(--accent); }
.scan-dot.bad { background: #ef4444; border-color: #ef4444; }
/* The stage currently running pulses, so a fifteen second scan reads as
   something in progress rather than a card that stopped updating. */
.scan-dot.live { border-color: var(--accent); animation: stage-pulse 1.2s ease-in-out infinite; }
@keyframes stage-pulse { 0%, 100% { opacity: 0.35; } 50% { opacity: 1; } }
/* Which stage, and what it found. Its own line rather than crowded into
   .gate-sub, because "container_image_scan" does not fit next to a version
   string at this card width, and the stage that found something is the useful
   half of a failure report. */
.scan-stage { display: flex; align-items: baseline; justify-content: space-between;
        gap: 0.4rem; font-size: 0.62rem; color: var(--muted);
        font-family: "JetBrains Mono", ui-monospace, monospace; }
.scan-stage .name { min-width: 0; overflow: hidden; text-overflow: ellipsis;
        white-space: nowrap; }
.scan-stage .finding { flex: none; }
.scan-stage .finding.ok { color: var(--accent); }
.scan-stage .finding.bad { color: #ef4444; }
.gate.waiting { border-style: solid; border-color: var(--protect); }
.gate.waiting .gate-verdict { color: var(--protect); }

@media (prefers-reduced-motion: reduce) {
  .gate.scan.appearing, .gate.running::after, .scan-dot.live { animation: none; }
}

.rail-head { margin-top: 1.1rem; }
/* Stacked in the ready column, so each release is a block that wraps its badges
   rather than a pill that would overflow a narrow column. */
.candidates { display: flex; flex-direction: column; align-items: stretch; gap: 0.4rem; margin-top: 0.5rem; }
.chip { display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem;
        padding: 0.34rem 0.5rem 0.34rem 0.62rem;
        border: 1px solid var(--card-edge); border-radius: 10px; background: var(--card); }
.chip .when { margin-left: auto; }
.chip .cv { font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 0.76rem; font-weight: 600; }
.chip .cw { font-size: 0.66rem; color: var(--muted); }
.where { font-size: 0.6rem; text-transform: uppercase; letter-spacing: 0.06em; padding: 0.1rem 0.42rem;
        border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
/* Outline for staging, filled for production: the same version can carry both, and
   the pair should read as a progression. */
.where-staging { border-color: var(--accent); color: var(--accent); }
.where-prod { background: var(--accent); border-color: var(--accent); color: var(--on-accent); font-weight: 600; }
.where-past { border-style: dashed; opacity: 0.75; }
.chip.fresh { animation: chip-in 0.55s cubic-bezier(0.16,1,0.3,1); border-color: var(--accent); }
.rail-empty { font-size: 0.78rem; color: var(--muted); }
@keyframes chip-in { from { transform: scale(0.88) translateY(5px); opacity: 0; } to { transform: none; opacity: 1; } }

.fleet-toast { position: fixed; top: 1.2rem; right: 1.2rem; z-index: 30; display: flex; align-items: center;
        gap: 0.65rem; max-width: 22rem; padding: 0.7rem 0.95rem; border: 1px solid var(--accent);
        border-radius: 12px; background: var(--card); box-shadow: 0 18px 44px -16px var(--accent-glow);
        animation: toast-in 0.45s cubic-bezier(0.16,1,0.3,1); }
.fleet-toast.leaving { animation: toast-out 0.35s ease forwards; }
.toast-mark { display: grid; place-items: center; flex: none; width: 1.4rem; height: 1.4rem; border-radius: 50%;
        background: var(--accent); color: var(--on-accent); font-size: 0.8rem; font-weight: 700; }
.toast-title { font-size: 0.85rem; font-weight: 600; }
.toast-sub { font-size: 0.7rem; color: var(--muted); }
@keyframes toast-in { from { transform: translateY(-10px) scale(0.97); opacity: 0; } to { transform: none; opacity: 1; } }
@keyframes toast-out { to { transform: translateY(-8px); opacity: 0; } }

@media (max-width: 900px) {
  /* Not enough width for three columns: the ready list goes back to a wrapping
     row above the two environment cards. */
  .fleet-flow { flex-direction: column; }
  .ready { flex: 0 0 auto; }
  .candidates { flex-direction: row; flex-wrap: wrap; }
  .chip { border-radius: 999px; }
  .chip .when { margin-left: 0; }
  .pipeline { flex: 0 0 auto; margin-top: 1rem; }
  /* The ready list is above the cards now, not to their left, so an inlet arrow
     from the page edge would point at nothing. */
  .link:first-child { display: none; }
}
@media (prefers-reduced-motion: reduce) {
  .live-dot, .env.promoting::before, .env.promoting .shine, .link.flowing .link-line::after,
  .chip.fresh, .fleet-toast { animation: none; }
  .version.out { display: none; }
  .version.in { animation: none; }
}
"""

_THEME_BOOT_JS = """
try { document.documentElement.setAttribute('data-theme', localStorage.getItem('gw-theme') || 'dark'); } catch (e) {}
"""

_FLEET_JS = """
// Release fleet panel. Polls /fleet on its own 2s cadence and updates the cards in
// place, so a promotion is visible as a transition instead of appearing after the
// page reload. The panel is the only part of the dashboard that mutates without a
// reload; the queue and history tables still come from the server render.
var FLEET = (function () {
  var LABELS = { staging: 'Staging', prod: 'Production' };
  var KNOWN_ENVS = { staging: 1, prod: 1 };
  var ANIM_MS = 2200;
  var pipeline, rail, statusEl, serviceEl;
  var protectedEnvs = {};
  var seen = {};
  var railSeen = {};
  var railPainted = false;
  var holdUntil = 0;

  function label(env) { return LABELS[env] || env; }
  function readJson(key, fallback) {
    try { return JSON.parse(sessionStorage.getItem(key)) || fallback; } catch (e) { return fallback; }
  }
  function writeJson(key, value) {
    try { sessionStorage.setItem(key, JSON.stringify(value)); } catch (e) {}
  }

  // Numeric segment by segment, lexical when a segment is not a plain number, so
  // 2.10.0 sorts above 2.9.0 and a suffixed build still orders deterministically.
  function cmpVersion(a, b) {
    var x = String(a).split('.'), y = String(b).split('.');
    for (var i = 0; i < Math.max(x.length, y.length); i++) {
      var p = x[i] === undefined ? '' : x[i], q = y[i] === undefined ? '' : y[i];
      var np = parseInt(p, 10), nq = parseInt(q, 10);
      if (String(np) === p && String(nq) === q) {
        if (np !== nq) return np < nq ? -1 : 1;
      } else if (p !== q) {
        return p < q ? -1 : 1;
      }
    }
    return 0;
  }

  function ago(epoch) {
    var value = parseFloat(epoch);
    if (!value) return '';
    var s = Math.max(0, Math.floor(Date.now() / 1000 - value));
    if (s < 5) return 'just now';
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    if (s < 86400) return Math.floor(s / 3600) + 'h ago';
    return Math.floor(s / 86400) + 'd ago';
  }

  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function cardFor(env) {
    var cards = pipeline.querySelectorAll('.env');
    for (var i = 0; i < cards.length; i++) {
      if (cards[i].getAttribute('data-env') === env) return cards[i];
    }
    return null;
  }

  function linkInto(env) {
    var links = pipeline.querySelectorAll('.link');
    for (var i = 0; i < links.length; i++) {
      if (links[i].getAttribute('data-into') === env) return links[i];
    }
    return null;
  }

  function buildGate() {
    var card = el('div', 'gate');
    card.setAttribute('data-gate', '1');
    var top = el('div', 'gate-top');
    top.appendChild(el('span', 'gate-name', 'Quality gates'));
    card.appendChild(top);
    card.appendChild(el('div', 'gate-verdict'));
    card.appendChild(el('div', 'gate-sub'));
    card.appendChild(el('ul', 'gate-checks'));
    return card;
  }

  function buildScan() {
    var card = el('div', 'gate scan');
    card.setAttribute('data-scan', '1');
    var top = el('div', 'gate-top');
    top.appendChild(el('span', 'gate-name', 'Security scan'));
    card.appendChild(top);
    // The only card in this row that names an owner, because it is the only one
    // that belongs to a different team.
    card.appendChild(el('div', 'gate-owner', 'Security team'));
    card.appendChild(el('div', 'gate-verdict'));
    card.appendChild(el('div', 'gate-sub'));
    card.appendChild(el('div', 'scan-stages'));
    var stage = el('div', 'scan-stage');
    stage.appendChild(el('span', 'name'));
    stage.appendChild(el('span', 'finding'));
    card.appendChild(stage);
    return card;
  }

  function buildPipeline(records, hasScan) {
    pipeline.innerHTML = '';
    records.forEach(function (rec) {
      // The gate sits on the path into production, so a candidate visibly has to
      // pass through it rather than around it. Always built, never conditional:
      // an idle gate is a state, not an absence.
      if (rec.environment === 'prod') {
        var gateLink = el('div', 'link');
        gateLink.setAttribute('data-into', 'gate');
        gateLink.appendChild(el('span', 'link-line'));
        pipeline.appendChild(gateLink);
        pipeline.appendChild(buildGate());
      }
      // The scan comes after the gate and before production: a candidate is
      // qualified first, then scanned, then promoted.
      if (hasScan && rec.environment === 'prod') {
        var scanLink = el('div', 'link');
        scanLink.setAttribute('data-into', 'scan');
        scanLink.appendChild(el('span', 'link-line'));
        pipeline.appendChild(scanLink);
        pipeline.appendChild(buildScan());
      }
      // Every card gets an inbound connector, including the first: releases flow
      // into staging from the ready list sitting directly above it.
      var link = el('div', 'link');
      link.setAttribute('data-into', rec.environment);
      link.appendChild(el('span', 'link-line'));
      pipeline.appendChild(link);
      var card = el('div', 'env' + (protectedEnvs[rec.environment] ? ' protected' : ''));
      card.setAttribute('data-env', rec.environment);
      card.appendChild(el('span', 'shine'));
      var top = el('div', 'env-top');
      top.appendChild(el('span', 'env-name'));
      top.appendChild(el('span', 'tag'));
      card.appendChild(top);
      var body = el('div', 'env-body');
      body.appendChild(el('div', 'version-wrap'));
      var side = el('div', 'env-side');
      side.appendChild(el('div', 'env-service'));
      var meta = el('div', 'env-meta');
      meta.appendChild(el('span', 'from'));
      meta.appendChild(el('span', 'sep', '\\u00b7'));
      meta.appendChild(el('span', 'when'));
      side.appendChild(meta);
      body.appendChild(side);
      card.appendChild(body);
      pipeline.appendChild(card);
    });
  }

  // Returns true when the version actually changed and was animated.
  function setVersion(card, version, animate) {
    var wrap = card.querySelector('.version-wrap');
    var current = wrap.querySelector('.version:not(.out)');
    if (current && current.textContent === version) return false;
    if (!current || !animate) {
      wrap.innerHTML = '';
      wrap.appendChild(el('span', 'version', version));
      return false;
    }
    current.classList.add('out');
    setTimeout(function () { current.remove(); }, 800);
    wrap.appendChild(el('span', 'version in', version));
    return true;
  }

  function toast(rec) {
    var node = el('div', 'fleet-toast');
    node.appendChild(el('span', 'toast-mark', '\\u2713'));
    var body = el('div');
    body.appendChild(el('div', 'toast-title', 'Live in ' + label(rec.environment) + ' \\u00b7 ' + rec.version));
    body.appendChild(el('div', 'toast-sub', (rec.service || '') +
      (rec.previous_version ? ' \\u00b7 replaced ' + rec.previous_version : '')));
    node.appendChild(body);
    document.body.appendChild(node);
    setTimeout(function () {
      node.classList.add('leaving');
      setTimeout(function () { node.remove(); }, 400);
    }, 4500);
  }

  function announce(card, rec) {
    card.classList.remove('promoting');
    void card.offsetWidth;  // restart the keyframes on a repeat promotion
    card.classList.add('promoting');
    var link = linkInto(rec.environment);
    if (link) { link.classList.remove('flowing'); void link.offsetWidth; link.classList.add('flowing'); }
    setTimeout(function () {
      card.classList.remove('promoting');
      if (link) link.classList.remove('flowing');
    }, ANIM_MS);
    // A protected environment is the point of the whole demo, so it gets the
    // loudest treatment: card shine plus a toast.
    if (protectedEnvs[rec.environment]) toast(rec);
    hold();
  }

  function hold() { holdUntil = Date.now() + ANIM_MS + 600; }

  // The gate card is always on the row, in whichever of four states is true:
  //   idle     nothing has ever reached staging (the state at container start)
  //   running  a gate workflow is evaluating a specific staged candidate
  //   passed   the most recent evaluated candidate cleared all four checks
  //   failed   the most recent evaluated candidate failed at least one
  // No first-appearance animation, unlike the security scan card: gates do not
  // arrive partway through the demo, they are standing infrastructure that
  // reacts whenever anything lands on staging.
  var GATE_VERDICTS = {
    idle: 'idle', running: 'running', passed: 'passed', failed: 'failed'
  };

  function paintGate(gate) {
    var card = pipeline.querySelector('.gate:not(.scan)');
    if (!card) return;
    gate = gate || { status: 'idle', checks: [] };
    var status = GATE_VERDICTS[gate.status] ? gate.status : 'idle';
    var running = status === 'running';
    var failed = status === 'failed';
    var checks = gate.checks || [];
    var total = gate.check_count || 4;
    var done = gate.checks_completed === undefined ? checks.length : gate.checks_completed;
    card.className = 'gate ' + status;
    card.querySelector('.gate-verdict').textContent = GATE_VERDICTS[status];

    if (status === 'idle') {
      card.querySelector('.gate-sub').textContent = 'no candidate staged yet';
    } else {
      var sub = (gate.version || '') + ' on ' + label(gate.environment || 'staging');
      if (running) sub += ' \\u00b7 ' + done + ' / ' + total + ' checks complete';
      else if (failed) {
        var bad = checks.filter(function (c) { return !c.passed; })
          .map(function (c) { return c.check_name; });
        if (bad.length) sub += ' \\u00b7 ' + bad.join(', ');
      }
      card.querySelector('.gate-sub').textContent = sub;
    }

    // Which checks have reported, and how they went. Empty while idle, filling
    // in as the four concurrent checks land.
    var list = card.querySelector('.gate-checks');
    list.innerHTML = '';
    checks.forEach(function (check) {
      var li = el('li', check.passed ? 'ok' : 'bad');
      li.appendChild(el('span', 'mark', check.passed ? '\\u2713' : '\\u2717'));
      li.appendChild(el('span', null, check.check_name));
      list.appendChild(li);
    });

    // A failed gate means nothing moved past it, so say so with the connector
    // rather than leaving a green arrow pointing at an untouched production card.
    // The connector it blocks is whichever one leads onward, which is the
    // security scan card once that exists.
    var out = linkInto(pipeline.querySelector('.gate.scan') ? 'scan' : 'prod');
    if (out) out.classList.toggle('blocked', failed);
    if (running) hold();
  }

  // phase -> [card class, verdict label]. The phases come straight from
  // SecurityScanWorkflow's own state, so what the card says and what the
  // workflow believes cannot drift apart.
  var SCAN_PHASES = {
    running_scan: ['running', 'scanning'],
    awaiting_prod_approval: ['waiting', 'awaiting approval'],
    scan_failed: ['failed', 'failed'],
    completed: ['passed', 'promoted'],
    rejected: ['failed', 'rejected'],
    expired: ['failed', 'expired'],
    failed: ['failed', 'failed']
  };

  // Stage names are what the scan publishes; this is only the fallback for a
  // stage that has not reported yet, so the card can name what is running.
  var SCAN_STAGE_NAMES = ['dependency_scan', 'container_image_scan',
    'secret_detection', 'static_analysis'];

  function paintScan(scan, appearing) {
    var card = pipeline.querySelector('.gate.scan');
    if (!card || !scan) return;
    var phase = SCAN_PHASES[scan.phase] || ['running', scan.phase];
    var total = scan.stage_count || 4;
    var done = scan.stages_completed || 0;
    var running = phase[0] === 'running';
    card.className = 'gate scan ' + phase[0] + (appearing ? ' appearing' : '');
    card.querySelector('.gate-verdict').textContent = phase[1];

    // The stage number, the stage name, and the severity all refer to the SAME
    // stage: the last one to report. Pairing the in-flight stage's name with the
    // previous stage's finding would read as that stage having found it, which is
    // the one thing a security card must not imply. Progress is carried by the
    // dots below, where the pulsing one is the stage actually running.
    var results = scan.stage_results || [];
    var latest = results.length ? results[results.length - 1] : null;
    var shown = Math.max(Math.min(done, total), 1);
    var sub = scan.version || '';
    if (running) sub += ' \\u00b7 stage ' + shown + '/' + total;
    else if (scan.phase === 'scan_failed') sub += ' \\u00b7 failed at stage ' + done;
    else sub += ' \\u00b7 ' + done + '/' + total + ' clean';
    card.querySelector('.gate-sub').textContent = sub;

    // One dot per stage: filled as each stage reports, pulsing on the one running
    // right now, red on the one that ended the scan.
    var dots = card.querySelector('.scan-stages');
    dots.innerHTML = '';
    for (var i = 0; i < total; i++) {
      var result = results[i];
      var cls = 'scan-dot';
      if (result) cls += result.passed ? ' ok' : ' bad';
      else if (running && i === done) cls += ' live';
      dots.appendChild(el('span', cls));
    }

    // Which stage, and what that same stage found. Before the first stage
    // reports there is nothing to attribute, so the name of the stage about to
    // run goes up on its own rather than leaving the line blank.
    var stageEl = card.querySelector('.scan-stage');
    var findingEl = stageEl.querySelector('.finding');
    var text = '';
    var findingCls = 'finding';
    var current = latest
      ? (latest.stage_name || '')
      : (SCAN_STAGE_NAMES[0] || '');
    if (latest && !latest.passed) {
      text = latest.findings + ' \\u00b7 ' + latest.max_severity;
      findingCls += ' bad';
    } else if (latest) {
      text = latest.max_severity;
      findingCls += ' ok';
    }
    stageEl.querySelector('.name').textContent = current;
    findingEl.className = findingCls;
    findingEl.textContent = text;

    var out = linkInto('prod');
    if (out) out.classList.toggle('blocked', phase[0] === 'failed');
    if (running || appearing) hold();
  }

  function paint(state, animate) {
    var records = state.environments || [];
    if (!records.length) return;
    protectedEnvs = {};
    (state.protected_environments || ['prod']).forEach(function (env) { protectedEnvs[env] = true; });

    if (serviceEl) {
      var service = records[0].service || '';
      serviceEl.textContent = service ? ': ' + service : '';
    }

    // The gate card is part of the row's fixed shape now: it is on the
    // dashboard from container startup, idle, because gates are standing
    // infrastructure that reacts whenever anything reaches staging rather than a
    // capability the team builds partway through the demo.
    //
    // The security scan is still the other contract. No scan state means the
    // Security team has not scanned anything here, which through CASE-1 and
    // CASE-2a is the truth, so that card genuinely does not exist yet and
    // animates in the first time their platform runs one.
    var gate = state.quality_gates || null;
    var scan = state.security_scan || null;
    var previous = pipeline.getAttribute('data-shape') || '';
    var shape = records.map(function (rec) { return rec.environment; }).join('|') +
      '|gate' + (scan ? '|scan' : '');
    var scanAppearing = false;
    if (previous !== shape) {
      scanAppearing = !!scan && previous.indexOf('scan') < 0;
      buildPipeline(records, !!scan);
      pipeline.setAttribute('data-shape', shape);
    }
    paintGate(gate);
    paintScan(scan, scanAppearing);

    var animated = false;
    records.forEach(function (rec) {
      var card = cardFor(rec.environment);
      if (!card) return;
      var isProtected = !!protectedEnvs[rec.environment];
      card.querySelector('.env-name').textContent = label(rec.environment);
      var tag = card.querySelector('.tag');
      tag.className = 'tag' + (isProtected ? ' tag-protected' : '');
      tag.textContent = isProtected ? 'approval required' : 'auto-promote';
      card.querySelector('.env-service').textContent = rec.service || '';
      card.querySelector('.from').textContent = rec.previous_version
        ? 'from ' + rec.previous_version : 'initial state';
      card.querySelector('.when').setAttribute('data-epoch', rec.updated_at || '');
      if (setVersion(card, rec.version, animate)) {
        announce(card, rec);
        animated = true;
      }
      seen[rec.environment] = rec.version;
    });

    paintRail(state);
    tickTimes();
    // Persist only once the transition has played, so a reload landing mid
    // animation replays it instead of swallowing it.
    if (animated) setTimeout(function () { writeJson('gw-fleet-seen', seen); }, ANIM_MS);
    else writeJson('gw-fleet-seen', seen);
  }

  function paintRail(state) {
    var records = state.environments || [];
    // A version can be running in more than one environment, so keep every one of
    // them. Environments arrive ordered staging -> prod, which is the order the
    // badges read in.
    var where = {};
    records.forEach(function (rec) {
      var key = rec.service + '@' + rec.version;
      (where[key] = where[key] || []).push(rec.environment);
    });

    // Anything older than the oldest version still running somewhere is history,
    // not something to promote, so it drops off the rail.
    var floor = null;
    records.forEach(function (rec) {
      if (floor === null || cmpVersion(rec.version, floor) < 0) floor = rec.version;
    });
    var releases = (state.releases || []).filter(function (rel) {
      return floor === null || cmpVersion(rel.version, floor) >= 0;
    });

    rail.innerHTML = '';
    if (!releases.length) {
      rail.appendChild(el('span', 'rail-empty', 'No releases are cut and ready to promote.'));
      return;
    }
    var fresh = false;
    releases.forEach(function (rel) {
      var key = rel.service + '@' + rel.version;
      var isNew = railPainted && !railSeen[key];
      railSeen[key] = true;
      if (isNew) fresh = true;
      var chip = el('span', 'chip' + (isNew ? ' fresh' : ''));
      chip.appendChild(el('span', 'cv', rel.version));
      var envs = where[key] || [];
      var history = rel.seen_in || [];
      if (envs.length) {
        envs.forEach(function (env) {
          chip.appendChild(el('span', 'where' + (KNOWN_ENVS[env] ? ' where-' + env : ''), 'in ' + env));
        });
      } else if (history.length) {
        chip.appendChild(el('span', 'where where-past', 'was in ' + history[history.length - 1]));
      } else {
        chip.appendChild(el('span', 'cw', 'ready'));
      }
      var when = el('span', 'cw when');
      when.setAttribute('data-epoch', rel.cut_at || '');
      chip.appendChild(when);
      rail.appendChild(chip);
    });
    railPainted = true;
    if (fresh) hold();
  }

  function setStatus(ok, message) {
    if (!statusEl) return;
    statusEl.className = ok ? 'live' : 'live stale';
    var text = statusEl.querySelector('.live-text');
    if (text) text.textContent = ok ? 'live' : (message || 'unavailable');
  }

  function tickTimes() {
    document.querySelectorAll('.when').forEach(function (node) {
      node.textContent = ago(node.getAttribute('data-epoch'));
    });
  }

  function poll() {
    fetch('/fleet', { credentials: 'same-origin', cache: 'no-store' })
      .then(function (resp) { return resp.ok ? resp.json() : Promise.reject(resp.status); })
      .then(function (state) {
        if (state.available === false) { setStatus(false, 'deployment backend unreachable'); return; }
        setStatus(true);
        writeJson('gw-fleet-state', state);
        paint(state, true);
      })
      .catch(function () { setStatus(false, 'fleet state unavailable'); });
  }

  function init() {
    pipeline = document.getElementById('fleet-pipeline');
    rail = document.getElementById('fleet-rail');
    statusEl = document.getElementById('fleet-status');
    serviceEl = document.getElementById('fleet-service');
    if (!pipeline || !rail) return;
    // Paint from the last known state first so a reload does not flash an empty
    // panel, then rewind any version whose transition has not been shown yet.
    var cached = readJson('gw-fleet-state', null);
    var pending = readJson('gw-fleet-seen', {});
    if (cached) {
      paint(cached, false);
      Object.keys(pending).forEach(function (env) {
        var card = cardFor(env);
        if (card) setVersion(card, pending[env], false);
      });
    }
    poll();
    setInterval(poll, 2000);
  }

  return { init: init, tickTimes: tickTimes, holdsReload: function () { return Date.now() < holdUntil; } };
})();
"""

_MAIN_JS = """
(function () {
  function applyTheme(t) { document.documentElement.setAttribute('data-theme', t === 'light' ? 'light' : 'dark'); }
  var btn = document.getElementById('theme-btn');
  function label() { return (localStorage.getItem('gw-theme') || 'dark') === 'dark' ? 'Light mode' : 'Dark mode'; }
  if (btn) {
    btn.textContent = label();
    btn.addEventListener('click', function () {
      var next = ((localStorage.getItem('gw-theme') || 'dark') === 'dark') ? 'light' : 'dark';
      localStorage.setItem('gw-theme', next); applyTheme(next); btn.textContent = label();
    });
  }

  function fmt(ms) {
    if (ms <= 0) return 'expired';
    var s = Math.floor(ms / 1000), m = Math.floor(s / 60), r = s % 60;
    return m + ':' + (r < 10 ? '0' : '') + r;
  }
  function tick() {
    var now = Date.now();
    document.querySelectorAll('.countdown').forEach(function (el) {
      var dl = parseFloat(el.getAttribute('data-deadline'));
      if (!dl) { el.textContent = '-'; return; }
      var ms = dl * 1000 - now;
      el.textContent = fmt(ms);
      if (ms <= 60000) { el.classList.add('urgent'); } else { el.classList.remove('urgent'); }
    });
    FLEET.tickTimes();
  }
  FLEET.init();
  tick(); setInterval(tick, 1000);

  var filter = document.getElementById('wf-filter');
  function applyFilter(v) {
    document.querySelectorAll('tr[data-wf]').forEach(function (tr) {
      tr.style.display = (!v || tr.getAttribute('data-wf') === v) ? '' : 'none';
    });
  }
  if (filter) {
    filter.value = localStorage.getItem('gw-filter') || '';
    applyFilter(filter.value);
    filter.addEventListener('input', function () {
      localStorage.setItem('gw-filter', filter.value); applyFilter(filter.value);
    });
  }
  var clearBtn = document.getElementById('wf-clear');
  if (clearBtn) {
    clearBtn.addEventListener('click', function () {
      if (filter) filter.value = '';
      localStorage.setItem('gw-filter', ''); applyFilter('');
    });
  }

  setInterval(function () {
    var a = document.activeElement;
    if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA')) return;
    // Never reload through a fleet transition; the animation is the point.
    if (FLEET.holdsReload()) return;
    var reasons = document.querySelectorAll('input.reason');
    for (var i = 0; i < reasons.length; i++) { if (reasons[i].value.trim() !== '') return; }
    window.location.reload();
  }, 5000);
})();
"""


def _render_dashboard(
    pending: list[tuple[str, Any]], history: list[tuple[str, Any]]
) -> str:
    pending_rows = (
        "".join(_render_pending_row(wf, op) for wf, op in pending)
        if pending
        else '<tr><td colspan="11" class="empty">No operations are waiting for approval.</td></tr>'
    )
    history_rows = (
        "".join(_render_history_row(wf, op) for wf, op in history)
        if history
        else '<tr><td colspan="12" class="empty">No tool calls recorded yet.</td></tr>'
    )
    wf_ids = sorted({wf for wf, _ in history})
    options = "".join(f'<option value="{html.escape(w)}">' for w in wf_ids)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Porticour Release Control</title>
  <script>{_THEME_BOOT_JS}</script>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <div>
      <h1>Porticour Release Control</h1>
      <p class="muted brandline">Release management for the Waypoint engineering
      team &middot; delivery matching. Approvers from other Porticour teams sign
      in here too.</p>
    </div>
    <div>
      <span class="muted">{html.escape(_resolve_principal())}</span>
      <button id="theme-btn" class="toggle" type="button">Light mode</button>
      <form method="post" action="/logout" class="inline" style="display:inline">
        <button class="toggle" type="submit">Log out</button>
      </form>
    </div>
  </header>
  <section class="fleet" id="fleet">
    <div class="section-head">
      <h2>Release fleet</h2>
      <span class="fleet-service" id="fleet-service"></span>
      <span class="live" id="fleet-status"><span class="live-dot"></span><span class="live-text">live</span></span>
    </div>
    <p class="muted subtle">What is cut, what is running where, and every promotion as it lands.</p>
    <div class="fleet-flow">
      <div class="ready">
        <div class="rail-head eyebrow">Ready for release</div>
        <div class="candidates" id="fleet-rail"></div>
      </div>
      <div class="pipeline" id="fleet-pipeline"></div>
    </div>
  </section>

  <hr class="divider">

  <h2 class="queue-head">Waiting for approval</h2>
  <table>
    <thead>
      <tr>
        <th>Workflow</th><th>Operation</th><th>Requested action</th><th>Requester</th>
        <th>Arguments</th><th>Justification</th><th>Risk reason</th><th>Call path</th>
        <th>Submitted</th><th>Deadline</th><th>Decision</th>
      </tr>
    </thead>
    <tbody>{pending_rows}</tbody>
  </table>

  <div class="filterbar">
    <h2>Tool Call History</h2>
    <input id="wf-filter" list="wf-list" type="text" placeholder="filter by workflow id">
    <datalist id="wf-list">{options}</datalist>
    <button id="wf-clear" class="toggle" type="button">Clear</button>
  </div>
  <table>
    <thead>
      <tr>
        <th>Timestamp</th><th>Workflow</th><th>Requester</th><th>Operation</th><th>Tool</th><th>Call path</th>
        <th>Protected</th><th>Requested action</th><th>Result</th><th>Outcome</th>
        <th>Approver</th><th>Reason</th>
      </tr>
    </thead>
    <tbody>{history_rows}</tbody>
  </table>

  <script>{_FLEET_JS}</script>
  <script>{_MAIN_JS}</script>
</body>
</html>"""


def _render_forbidden(message: str) -> str:
    """The visible half of the authorization boundary.

    A demoable moment rather than an error to avoid: it is what makes the
    Security team's approval requirement real instead of a label on a row.
    """
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Not authorized &middot; Porticour Release Control</title>
  <script>{_THEME_BOOT_JS}</script>
  <style>{_CSS}</style>
</head>
<body>
  <main class="forbidden">
    <h1>Not authorized to decide this operation</h1>
    <p>{html.escape(message)}</p>
    <p class="muted">This promotion was requested by another Porticour team's
    pre-prod security scan, so a member of that team has to be the one who
    approves or rejects it. Sign in with that team's approver token and the
    entry becomes actionable.</p>
    <p><a href="/">Back to the queue</a></p>
  </main>
</body>
</html>"""


def _render_login(error: str = "") -> str:
    message = (
        f'<p style="color:#ef4444">{html.escape(error)}</p>' if error else ""
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Porticour Release Control</title>
  <script>{_THEME_BOOT_JS}</script>
  <style>{_CSS}</style>
</head>
<body>
  <main class="login">
    <h1>Approver sign in</h1>
    <p class="muted">Porticour Release Control. Use a gateway-issued approver
    token. Decisions are recorded under the identity mapped to the token, not a
    form-supplied name, and the token is what determines which Porticour team
    you approve as.</p>
    {message}
    <form method="post" action="/login">
      <label for="token">Approver token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required>
      <button class="approve" type="submit">Sign in</button>
    </form>
  </main>
</body>
</html>"""


if __name__ == "__main__":
    # Build the ASGI app and wrap it so the bearer token is read at the HTTP layer,
    # where request headers are reliably available. Serves MCP at /mcp plus the
    # approver routes on the same port.
    print(f"[startup] loaded {len(_PRINCIPALS)} principal(s)", flush=True)
    app = mcp.streamable_http_app()
    uvicorn.run(PrincipalMiddleware(app), host=GATEWAY_HOST, port=GATEWAY_PORT)
