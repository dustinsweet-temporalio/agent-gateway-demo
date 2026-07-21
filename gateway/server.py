from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP
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

mcp = FastMCP("agent-gateway")
mcp.settings.host = GATEWAY_HOST
mcp.settings.port = GATEWAY_PORT

_client: Optional[Client] = None
_client_lock = asyncio.Lock()


async def get_client() -> Client:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = await Client.connect(TEMPORAL_ADDRESS)
    return _client


def _as_dict(obj: Any) -> Any:
    return asdict(obj) if is_dataclass(obj) else obj


def _derive_idempotency_key(
    workflow_id: str, tool_name: str, arguments: dict, provided: Optional[str]
) -> str:
    if provided:
        return provided
    # Stable key from content so a retried identical call reuses the same
    # operation instead of creating a duplicate approval gate.
    payload = json.dumps(
        {"w": workflow_id, "t": tool_name, "a": arguments}, sort_keys=True
    )
    return "idem-" + hashlib.sha256(payload.encode()).hexdigest()[:24]


async def _submit_tool_call(
    tool_name: str,
    arguments: dict,
    workflow_id: Optional[str],
    idempotency_key: Optional[str],
    caller_principal: str,
) -> dict:
    client = await get_client()

    source = "explicit"
    if not workflow_id:
        workflow_id = "wf-" + uuid.uuid4().hex[:16]
        source = "gateway_generated"

    idem = _derive_idempotency_key(workflow_id, tool_name, arguments, idempotency_key)
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
    )

    # update-with-start: create the chain workflow if it does not exist and deliver
    # the tool-call request as an Update in a single round trip. USE_EXISTING makes
    # this an upsert against the chain identified by workflow_id.
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
        wait_for_stage=WorkflowUpdateStage.COMPLETED,
        start_workflow_operation=start_op,
    )
    resp = await handle.result()
    return _as_dict(resp)


# --------------------------------------------------------------------- MCP tools
# Downstream tools exposed with stable names and schemas. Claude Code should reuse
# the returned workflow_id on later calls in the same task, and poll the lifecycle
# tools when a call returns status "waiting_for_approval".


@mcp.tool()
async def read_metrics(
    service: str, workflow_id: str = "", idempotency_key: str = ""
) -> dict:
    """Read metrics for a service. Safe read; does not require approval."""
    return await _submit_tool_call(
        "read_metrics",
        {"service": service},
        workflow_id or None,
        idempotency_key or None,
        "claude-code",
    )


@mcp.tool()
async def deploy_service(
    service: str, version: str, workflow_id: str = "", idempotency_key: str = ""
) -> dict:
    """Deploy a version of a service. Protected action that requires human approval."""
    return await _submit_tool_call(
        "deploy_service",
        {"service": service, "version": version},
        workflow_id or None,
        idempotency_key or None,
        "claude-code",
    )


@mcp.tool()
async def transfer_funds(
    account: str, amount: float, workflow_id: str = "", idempotency_key: str = ""
) -> dict:
    """Transfer funds. Protected action that requires human approval."""
    return await _submit_tool_call(
        "transfer_funds",
        {"account": account, "amount": amount},
        workflow_id or None,
        idempotency_key or None,
        "claude-code",
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


@mcp.custom_route("/", methods=["GET"])
async def dashboard(request: Request) -> HTMLResponse:
    client = await get_client()
    pending: list[tuple[str, Any]] = []
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
    return HTMLResponse(_render_dashboard(pending))


@mcp.custom_route("/approve", methods=["POST"])
async def approve(request: Request) -> RedirectResponse:
    form = await request.form()
    workflow_id = str(form["workflow_id"])
    operation_id = str(form["operation_id"])
    approver = str(form.get("approver") or "approver@demo")
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(operation_id=operation_id, approver=approver),
    )
    return RedirectResponse("/", status_code=303)


@mcp.custom_route("/reject", methods=["POST"])
async def reject(request: Request) -> RedirectResponse:
    form = await request.form()
    workflow_id = str(form["workflow_id"])
    operation_id = str(form["operation_id"])
    approver = str(form.get("approver") or "approver@demo")
    reason = str(form.get("reason") or "Rejected by approver")
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(
        AgenticChainWorkflow.reject_operation,
        ApprovalDecision(operation_id=operation_id, approver=approver, reason=reason),
    )
    return RedirectResponse("/", status_code=303)


def _render_dashboard(pending: list[tuple[str, Any]]) -> str:
    rows = []
    for workflow_id, op in pending:
        wf = html.escape(workflow_id)
        op_id = html.escape(op.operation_id)
        tool = html.escape(op.tool_name)
        risk = html.escape(op.risk_reason or "")
        deadline = html.escape(op.deadline_iso or "")
        rows.append(
            f"""
            <tr>
              <td>{tool}</td>
              <td class="mono">{op_id}</td>
              <td class="mono">{wf}</td>
              <td>{risk}</td>
              <td class="mono">{deadline}</td>
              <td>
                <form method="post" action="/approve" class="inline">
                  <input type="hidden" name="workflow_id" value="{wf}">
                  <input type="hidden" name="operation_id" value="{op_id}">
                  <input type="text" name="approver" placeholder="approver" value="approver@demo">
                  <button class="approve" type="submit">Approve</button>
                </form>
                <form method="post" action="/reject" class="inline">
                  <input type="hidden" name="workflow_id" value="{wf}">
                  <input type="hidden" name="operation_id" value="{op_id}">
                  <input type="text" name="reason" placeholder="reason">
                  <button class="reject" type="submit">Reject</button>
                </form>
              </td>
            </tr>
            """
        )
    body = (
        "".join(rows)
        if rows
        else '<tr><td colspan="6" class="empty">No operations are waiting for approval.</td></tr>'
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>Agent Gateway Approvals</title>
  <style>
    body {{ font-family: Inter, system-ui, sans-serif; margin: 2rem; color: #0f1117; }}
    h1 {{ font-size: 1.25rem; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
    th, td {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
    th {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: #6b7280; }}
    .mono {{ font-family: "JetBrains Mono", ui-monospace, monospace; font-size: 0.8rem; }}
    .empty {{ color: #6b7280; text-align: center; padding: 2rem; }}
    .inline {{ display: inline-block; margin: 0 0.25rem 0.25rem 0; }}
    input[type=text] {{ padding: 0.25rem 0.4rem; border: 1px solid #d1d5db; border-radius: 4px; }}
    button {{ padding: 0.3rem 0.7rem; border: 0; border-radius: 4px; color: white; cursor: pointer; }}
    .approve {{ background: #13c4b0; }}
    .reject {{ background: #ef4444; }}
    .hint {{ color: #6b7280; font-size: 0.85rem; margin-top: 0.5rem; }}
  </style>
</head>
<body>
  <h1>Agent Gateway Approvals</h1>
  <p class="hint">Operations paused pending human approval. This page refreshes every 5 seconds.</p>
  <table>
    <thead>
      <tr><th>Tool</th><th>Operation</th><th>Chain</th><th>Risk reason</th><th>Deadline</th><th>Decision</th></tr>
    </thead>
    <tbody>{body}</tbody>
  </table>
</body>
</html>"""


if __name__ == "__main__":
    # Serves the MCP endpoint at /mcp plus the approver routes on the same port.
    mcp.run(transport="streamable-http")
