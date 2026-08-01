from __future__ import annotations

import pytest

from common.models import (
    ADK_SESSION_ID_HEADER,
    CALLBACK_RUN_ID_HEADER,
    CALLBACK_WORKFLOW_ID_HEADER,
)
from gateway import server


def test_mcp_transport_allows_local_and_compose_hosts() -> None:
    transport_security = server.mcp.settings.transport_security

    assert transport_security is not None
    assert transport_security.enable_dns_rebinding_protection is True
    assert "localhost:*" in transport_security.allowed_hosts
    assert "gateway:*" in transport_security.allowed_hosts


def test_authenticated_adk_callback_headers_are_resolved() -> None:
    callback = server._adk_callback_from_headers(
        [
            (
                CALLBACK_WORKFLOW_ID_HEADER.encode(),
                b"adk-session-workflow",
            ),
            (CALLBACK_RUN_ID_HEADER.encode(), b"adk-session-run"),
            (ADK_SESSION_ID_HEADER.encode(), b"adk-session-123"),
        ],
        "release-agent@google-adk",
    )

    assert callback is not None
    assert callback.workflow_id == "adk-session-workflow"
    assert callback.run_id == "adk-session-run"
    assert callback.session_id == "adk-session-123"
    assert (
        server._adk_callback_from_headers(
            [
                (
                    CALLBACK_WORKFLOW_ID_HEADER.encode(),
                    b"adk-session-workflow",
                ),
                (ADK_SESSION_ID_HEADER.encode(), b"adk-session-123"),
            ],
            server.UNVERIFIED_PRINCIPAL,
        )
        is None
    )


def test_mcp_exposes_all_required_scenario_and_recovery_tools() -> None:
    tools = server.mcp._tool_manager.list_tools()
    names = {tool.name for tool in tools}
    assert {
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "run_nested_release",
        "resume_nested_release",
        "start_google_adk_release_run",
        "get_operation_status",
        "get_operation_result",
        "get_workflow_status",
        "get_workflow_ledger",
        "cancel_operation",
    }.issubset(names)
    assert all(tool.output_schema is not None for tool in tools)
    operation_schema = next(
        tool.output_schema
        for tool in tools
        if tool.name == "promote_release"
    )
    assert {
        "status",
        "workflow_id",
        "operation_id",
        "poll_after_seconds",
    }.issubset(operation_schema["properties"])


def test_sensitive_arguments_are_redacted_recursively() -> None:
    safe = server._safe_arguments(
        {
            "service": "delivery-matching-service",
            "token": "secret-token",
            "nested": {
                "api_key": "secret-key",
                "password": "secret-password",
                "environment": "prod",
            },
        }
    )
    assert safe == {
        "service": "delivery-matching-service",
        "token": "[REDACTED]",
        "nested": {
            "api_key": "[REDACTED]",
            "password": "[REDACTED]",
            "environment": "prod",
        },
    }


def test_explicit_workflow_ids_require_authenticated_identity() -> None:
    token = server._current_principal.set(server.UNVERIFIED_PRINCIPAL)
    try:
        with pytest.raises(PermissionError):
            server._resolve_workflow_id("wf-caller-supplied", None)
    finally:
        server._current_principal.reset(token)


def test_caller_idempotency_keys_are_stable_and_principal_scoped() -> None:
    first = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "alice@example.com",
    )
    duplicate = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "alice@example.com",
    )
    other_principal = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "mallory@example.com",
    )
    assert first == duplicate
    assert first != other_principal
