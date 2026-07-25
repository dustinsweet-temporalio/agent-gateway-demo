from __future__ import annotations

from adk_agents.release_approval_agent.agent import (
    AGENT_INSTRUCTION,
    DEFAULT_DEMO_TOKEN,
    DEFAULT_GATEWAY_URL,
    SCENARIO_3_TOOL_NAMES,
    AgentGatewaySettings,
    build_agent,
)


def test_adk_agent_uses_scenario_3_only_tool_allowlist() -> None:
    agent = build_agent(
        AgentGatewaySettings(
            mcp_url="http://gateway.test/mcp",
            bearer_token="test-agent-token",
            model="gemini-test",
        )
    )

    assert agent.name == "release_approval_agent"
    assert agent.model == "gemini-test"
    assert len(agent.tools) == 1
    toolset = agent.tools[0]
    assert tuple(toolset.tool_filter) == SCENARIO_3_TOOL_NAMES
    assert toolset._connection_params.url == "http://gateway.test/mcp"
    assert toolset._connection_params.headers == {
        "Authorization": "Bearer test-agent-token"
    }
    assert "promote_release" not in SCENARIO_3_TOOL_NAMES


def test_adk_agent_has_durable_pause_and_recovery_instructions() -> None:
    assert "start_google_adk_release_run" in AGENT_INSTRUCTION
    assert "waiting_for_approval" in AGENT_INSTRUCTION
    assert "workflow_id and operation_id" in AGENT_INSTRUCTION
    assert "Never invent a replacement ID" in AGENT_INSTRUCTION
    assert "get_operation_result returns completed" in AGENT_INSTRUCTION


def test_adk_settings_read_gateway_identity_from_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "AGENT_GATEWAY_MCP_URL",
        "http://agent-gateway.internal/mcp",
    )
    monkeypatch.setenv("AGENT_GATEWAY_TOKEN", "runtime-token")
    monkeypatch.setenv("ADK_MODEL", "gemini-runtime")

    settings = AgentGatewaySettings.from_env()

    assert settings.mcp_url == "http://agent-gateway.internal/mcp"
    assert settings.bearer_token == "runtime-token"
    assert settings.model == "gemini-runtime"


def test_adk_settings_have_local_demo_defaults(monkeypatch) -> None:
    monkeypatch.delenv("AGENT_GATEWAY_MCP_URL", raising=False)
    monkeypatch.delenv("AGENT_GATEWAY_TOKEN", raising=False)
    monkeypatch.delenv("ADK_MODEL", raising=False)

    settings = AgentGatewaySettings.from_env()

    assert settings.mcp_url == DEFAULT_GATEWAY_URL
    assert settings.bearer_token == DEFAULT_DEMO_TOKEN
