"""Google ADK agent that drives Scenario 3 through Agent Gateway over MCP."""

from __future__ import annotations

import os
from dataclasses import dataclass

from google.adk import Agent
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import (
    StreamableHTTPConnectionParams,
)

DEFAULT_GATEWAY_URL = "http://localhost:8080/mcp"
DEFAULT_DEMO_TOKEN = "tok_adk"
DEFAULT_MODEL = "gemini-2.5-flash"

# Deliberately excludes the generic promote_release tool. This agent must enter
# the dedicated autonomous workflow so its plan and dependent action are durable.
SCENARIO_3_TOOL_NAMES = (
    "start_google_adk_release_run",
    "get_operation_status",
    "get_operation_result",
    "get_workflow_status",
    "get_workflow_ledger",
    "cancel_operation",
)

AGENT_INSTRUCTION = """
You are the release approval agent for Scenario 3.

To request a release, call start_google_adk_release_run. Never call a generic
promotion tool. Require service, version, environment, and a stable agent_run_id
from the user. Pass through their justification. If an idempotency_key is
provided, preserve it exactly.

Treat waiting_for_approval as a successful durable pause, not an error. Report the
workflow_id and operation_id exactly as returned and explain that a human must
decide the request in Agent Gateway. Do not claim that the release completed.

For status or recovery, use the returned workflow_id and operation_id with the
status/result tools. Reuse the original agent_run_id if the start request itself
must be retried. Never invent a replacement ID for an in-flight run. Poll only
when the user asks you to check again; do not busy-loop.

Only report success when get_operation_result returns completed. If it returns
rejected, expired, canceled, or failed, stop and report that terminal state. The
Temporal workflow, not this chat session, owns the checkpoint and executes the
dependent follow-up only after the approved protected action succeeds.
""".strip()


@dataclass(frozen=True)
class AgentGatewaySettings:
    """Runtime settings for the ADK-to-gateway MCP connection."""

    mcp_url: str = DEFAULT_GATEWAY_URL
    bearer_token: str = DEFAULT_DEMO_TOKEN
    model: str = DEFAULT_MODEL

    @classmethod
    def from_env(cls) -> "AgentGatewaySettings":
        return cls(
            mcp_url=os.getenv("AGENT_GATEWAY_MCP_URL", DEFAULT_GATEWAY_URL),
            bearer_token=os.getenv(
                "AGENT_GATEWAY_TOKEN",
                DEFAULT_DEMO_TOKEN,
            ),
            model=os.getenv("ADK_MODEL", DEFAULT_MODEL),
        )


def build_agent(
    settings: AgentGatewaySettings | None = None,
) -> Agent:
    """Build the ADK agent with a least-privilege Agent Gateway toolset."""

    resolved = settings or AgentGatewaySettings.from_env()
    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=resolved.mcp_url,
            headers={
                "Authorization": f"Bearer {resolved.bearer_token}",
            },
        ),
        tool_filter=list(SCENARIO_3_TOOL_NAMES),
    )
    return Agent(
        name="release_approval_agent",
        model=resolved.model,
        description=(
            "Triggers and recovers the durable, approval-gated Scenario 3 "
            "release workflow through Agent Gateway."
        ),
        instruction=AGENT_INSTRUCTION,
        tools=[toolset],
    )


root_agent = build_agent()
