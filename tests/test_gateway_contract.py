from __future__ import annotations

import pytest

from gateway import server


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
