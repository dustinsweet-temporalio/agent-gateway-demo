from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import uuid
from contextvars import ContextVar
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Optional

import uvicorn
from mcp.server.fastmcp import Context, FastMCP
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy

from common.models import (
    ApprovalDecision,
    ChainInput,
    CorrelationContext,
    ToolCallRequest,
)
from workflows.chain import AgenticChainWorkflow

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
APPROVAL_TIMEOUT_SECONDS = int(os.getenv("APPROVAL_TIMEOUT_SECONDS", "300"))
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8080"))

TERMINAL_STATUSES = {"completed", "rejected", "expired", "failed"}
UNVERIFIED_PRINCIPAL = "claude-code (unverified)"
GATEWAY_DEBUG = bool(os.getenv("GATEWAY_DEBUG"))
POLL_AFTER_SECONDS = int(os.getenv("POLL_AFTER_SECONDS", "5"))

# Per-tool sync/async handling, annotated in code (design scenarios #1, #2, #3).
#   "sync"        block to completion, never convert (scenario #1)
#   "async"       return a poll handle immediately (scenario #2)
#   <int> ms      monitor and convert: block up to this budget, then convert to
#                 async and return a poll handle while the workflow keeps running
#                 (scenario #3)
# A tool absent from this map uses _DEFAULT_STRATEGY.
_TOOL_STRATEGY: dict[str, object] = {
    "get_deployed_version": "sync",
    "cut_release": 5000,
    "promote_release": 5000,
}
_DEFAULT_STRATEGY: object = 5000

mcp = FastMCP("agent-gateway")
mcp.settings.host = GATEWAY_HOST
mcp.settings.port = GATEWAY_PORT

_client: Optional[Client] = None
_client_lock = asyncio.Lock()

# One chain per MCP session. See _resolve_workflow_id.
_session_chains: dict[str, str] = {}
_DEFAULT_CHAIN = "wf-" + uuid.uuid4().hex[:6]


def _load_principals() -> dict[str, str]:
    """Static bearer token to identity map from GATEWAY_PRINCIPALS (JSON).

    Example: {"tok_dustin": "Dustin Sweet <dustin.sweet@temporal.io>"}
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

# The requester is resolved at the ASGI layer (see PrincipalMiddleware) where the
# raw HTTP headers are reliably available. The bundled mcp SDK does not reliably
# expose request headers inside a tool, so we do not read them there. The
# ContextVar carries the per-request principal; the module fallback covers the
# single-session demo case if the ContextVar does not propagate into the tool task.
_current_principal: ContextVar[Optional[str]] = ContextVar(
    "current_principal", default=None
)
_last_principal = UNVERIFIED_PRINCIPAL

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


class PrincipalMiddleware:
    """Reads the bearer token off each HTTP request and resolves the requester.

    This is the trust boundary: identity comes from a validated credential the
    gateway checks, never from anything the agent says.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            token: Optional[str] = None
            for name, value in scope.get("headers", []):
                if name == b"authorization":
                    raw = value.decode("latin-1")
                    if raw.lower().startswith("bearer "):
                        token = raw[7:].strip()
                    break
            principal = _principal_for_token(token)
            _current_principal.set(principal)
            global _last_principal
            _last_principal = principal
            if GATEWAY_DEBUG:
                path = scope.get("path", "")
                print(
                    f"[auth] path={path} header_present={token is not None} "
                    f"resolved={principal!r}",
                    flush=True,
                )
        await self.app(scope, receive, send)


def _resolve_principal() -> str:
    principal = _current_principal.get()
    return principal if principal is not None else _last_principal


async def get_client() -> Client:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = await Client.connect(TEMPORAL_ADDRESS)
    return _client


def _as_dict(obj: Any) -> Any:
    return asdict(obj) if is_dataclass(obj) else obj


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
        # NOTE: the requirements state caller-supplied IDs must be authorized
        # before use. This demo trusts an explicit workflow_id as-is.
        return explicit, "explicit"
    key = _session_key(ctx)
    if key is not None:
        wf = _session_chains.setdefault(key, "wf-" + uuid.uuid4().hex[:6])
        return wf, "mcp_session"
    return _DEFAULT_CHAIN, "gateway_default"


def _derive_idempotency_key(workflow_id: str, tool_name: str, arguments: dict) -> str:
    payload = json.dumps(
        {"w": workflow_id, "t": tool_name, "a": arguments}, sort_keys=True
    )
    return "idem-" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def _derive_operation_id(idempotency_key: str) -> str:
    # Deterministic from the idempotency key so the gateway knows the id up front
    # and it stays stable across retries and across a sync-to-async conversion.
    return "op-" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:6]


def _async_handle(workflow_id: str, operation_id: str) -> dict:
    return {
        "status": "processing",
        "workflow_id": workflow_id,
        "operation_id": operation_id,
        "message": (
            "The tool call is still running and was converted to async. Poll "
            "get_operation_result with this workflow_id and operation_id."
        ),
        "poll_after_seconds": POLL_AFTER_SECONDS,
    }


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
) -> dict:
    client = await get_client()

    workflow_id, source = _resolve_workflow_id(explicit_workflow_id, ctx)
    caller_principal = _resolve_principal()
    idem = _derive_idempotency_key(workflow_id, tool_name, arguments)
    operation_id = _derive_operation_id(idem)
    correlation = CorrelationContext(
        workflow_id=workflow_id,
        workflow_id_source=source,
        caller_principal=caller_principal,
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
    )

    # Deliver the tool call as an Update. wait_for_stage=ACCEPTED returns the handle
    # once the workflow has accepted the update, so the gateway can then bound how
    # long it blocks on the result independently.
    start_op = WithStartWorkflowOperation(
        AgenticChainWorkflow.run,
        ChainInput(workflow_id=workflow_id),
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
        # Scenario #2: hand back a poll handle immediately; the workflow runs on.
        return _async_handle(workflow_id, operation_id)

    if strategy == "sync":
        # Scenario #1: block to completion.
        return _as_dict(await handle.result())

    # Scenario #3: block up to the budget, then convert to async. asyncio.wait
    # stops blocking WITHOUT cancelling the result poll, so the Temporal SDK does
    # not raise a cancellation error and the workflow is unaffected. The result is
    # recovered later by polling get_operation_result.
    budget_seconds = float(strategy) / 1000.0
    result_task = asyncio.ensure_future(handle.result())
    done, _pending = await asyncio.wait({result_task}, timeout=budget_seconds)
    if result_task in done:
        return _as_dict(result_task.result())
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


@mcp.tool()
async def get_deployed_version(
    environment: str, ctx: Context, workflow_id: str = ""
) -> dict:
    """Return the release version currently deployed in an environment.

    environment is one of test, staging, or prod. Read only; never requires approval.
    """
    return await _submit_tool_call(
        "get_deployed_version", {"environment": environment}, workflow_id or None, None, ctx
    )


@mcp.tool()
async def cut_release(
    service: str, version: str, ctx: Context, workflow_id: str = ""
) -> dict:
    """Cut a release candidate for a service from an already built artifact.

    Registers a promotable release. Changes nothing running, so it never requires
    approval.
    """
    return await _submit_tool_call(
        "cut_release", {"service": service, "version": version}, workflow_id or None, None, ctx
    )


@mcp.tool()
async def promote_release(
    service: str,
    version: str,
    environment: str,
    ctx: Context,
    justification: str = "",
    workflow_id: str = "",
) -> dict:
    """Promote a release to an environment.

    Promotion to test or staging runs immediately. Promotion to prod requires human
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
    )


@mcp.tool()
async def get_operation_status(workflow_id: str, operation_id: str) -> dict:
    """Return the current status of one approval-gated operation."""
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    resp = await handle.query(AgenticChainWorkflow.get_operation_status, operation_id)
    return _as_dict(resp)


@mcp.tool()
async def get_operation_result(workflow_id: str, operation_id: str) -> dict:
    """Return the result of an operation once it has completed."""
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    resp = await handle.query(AgenticChainWorkflow.get_operation_result, operation_id)
    return _as_dict(resp)


@mcp.tool()
async def get_workflow_status(workflow_id: str) -> dict:
    """Return a summary of the whole agentic chain."""
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    return _as_dict(summary)


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
            "token_seen": token,
            "principals_loaded": len(_PRINCIPALS),
            "resolved_from_this_request": _principal_for_token(token),
            "resolved_from_middleware_contextvar": _current_principal.get(),
            "resolved_last_principal": _last_principal,
        }
    )


@mcp.custom_route("/", methods=["GET"])
async def dashboard(request: Request) -> HTMLResponse:
    client = await get_client()
    pending: list[tuple[str, Any]] = []
    history: list[tuple[str, Any]] = []
    query = 'WorkflowType = "AgenticChainWorkflow" AND ExecutionStatus = "Running"'
    async for wf in client.list_workflows(query):
        handle = client.get_workflow_handle(wf.id)
        try:
            summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
        except Exception:
            continue
        for op in summary.operations:
            if op.status == "waiting_for_approval":
                pending.append((wf.id, op))
            elif op.status in TERMINAL_STATUSES:
                history.append((wf.id, op))
    history.sort(key=lambda row: (row[1].decided_iso or ""), reverse=True)
    return HTMLResponse(_render_dashboard(pending, history))


@mcp.custom_route("/approve", methods=["POST"])
async def approve(request: Request) -> RedirectResponse:
    form = await request.form()
    client = await get_client()
    handle = client.get_workflow_handle(str(form["workflow_id"]))
    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id=str(form["operation_id"]),
            approver=str(form.get("approver") or "approver@demo"),
        ),
    )
    return RedirectResponse("/", status_code=303)


@mcp.custom_route("/reject", methods=["POST"])
async def reject(request: Request) -> RedirectResponse:
    form = await request.form()
    client = await get_client()
    handle = client.get_workflow_handle(str(form["workflow_id"]))
    await handle.signal(
        AgenticChainWorkflow.reject_operation,
        ApprovalDecision(
            operation_id=str(form["operation_id"]),
            approver=str(form.get("approver") or "approver@demo"),
            reason=str(form.get("reason") or "Rejected by approver"),
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


def _render_pending_row(workflow_id: str, op: Any) -> str:
    wf = html.escape(workflow_id)
    op_id = html.escape(op.operation_id)
    deadline = op.deadline_epoch or 0
    return f"""
    <tr>
      <td class="mono">{wf}</td>
      <td class="mono">{op_id}</td>
      <td>{_fmt(op.requested_action)}</td>
      <td>{_fmt(op.requester)}</td>
      <td class="mono">{_args_summary(op.arguments)}</td>
      <td>{_fmt(op.justification)}</td>
      <td class="mono">{html.escape(_short_ts(op.created_iso))}</td>
      <td class="mono"><span class="countdown" data-deadline="{deadline}"></span></td>
      <td>
        <form method="post" action="/approve" class="inline">
          <input type="hidden" name="workflow_id" value="{wf}">
          <input type="hidden" name="operation_id" value="{op_id}">
          <input type="text" name="approver" value="approver@demo">
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
}
[data-theme="light"] {
  --bg: #ffffff; --fg: #0f1117; --muted: #6b7280; --border: #e5e7eb;
  --accent: #0f9b8e; --th: #6b7280; --field: #ffffff;
}
body { background: var(--bg); color: var(--fg); font-family: Inter, system-ui, sans-serif; margin: 2rem; }
header { display: flex; align-items: center; justify-content: space-between; }
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
.prot-y { color: #f59e0b; font-weight: 600; }
.prot-n { color: var(--muted); }
.badge { padding: 0.1rem 0.45rem; border-radius: 999px; font-size: 0.7rem; color: white; }
.badge-completed { background: #13c4b0; }
.badge-rejected { background: #ef4444; }
.badge-expired { background: #f59e0b; }
.badge-failed { background: #6b7280; }
.filterbar { display: flex; gap: 0.5rem; align-items: center; margin-top: 1.75rem; }
"""

_THEME_BOOT_JS = """
try { document.documentElement.setAttribute('data-theme', localStorage.getItem('gw-theme') || 'dark'); } catch (e) {}
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
  }
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
        else '<tr><td colspan="9" class="empty">No operations are waiting for approval.</td></tr>'
    )
    history_rows = (
        "".join(_render_history_row(wf, op) for wf, op in history)
        if history
        else '<tr><td colspan="11" class="empty">No tool calls recorded yet.</td></tr>'
    )
    wf_ids = sorted({wf for wf, _ in history})
    options = "".join(f'<option value="{html.escape(w)}">' for w in wf_ids)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Agent Gateway Approvals</title>
  <script>{_THEME_BOOT_JS}</script>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <h1>Agent Gateway Approvals</h1>
    <button id="theme-btn" class="toggle" type="button">Light mode</button>
  </header>
  <p class="muted">Operations paused pending human approval. Auto-refreshes every 5 seconds while you are not typing.</p>

  <h2>Waiting for approval</h2>
  <table>
    <thead>
      <tr>
        <th>Workflow</th><th>Operation</th><th>Requested action</th><th>Requester</th>
        <th>Arguments</th><th>Justification</th><th>Submitted</th><th>Deadline</th><th>Decision</th>
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
        <th>Timestamp</th><th>Workflow</th><th>Requester</th><th>Operation</th><th>Tool</th>
        <th>Protected</th><th>Requested action</th><th>Result</th><th>Outcome</th>
        <th>Approver</th><th>Reason</th>
      </tr>
    </thead>
    <tbody>{history_rows}</tbody>
  </table>

  <script>{_MAIN_JS}</script>
</body>
</html>"""


if __name__ == "__main__":
    # Build the ASGI app and wrap it so the bearer token is read at the HTTP layer,
    # where request headers are reliably available. Serves MCP at /mcp plus the
    # approver routes on the same port.
    print(f"[startup] loaded {len(_PRINCIPALS)} principal(s)", flush=True)
    app = mcp.streamable_http_app()
    uvicorn.run(PrincipalMiddleware(app), host=GATEWAY_HOST, port=GATEWAY_PORT)
