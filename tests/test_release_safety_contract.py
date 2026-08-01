"""The wire contract between Agent Gateway and Release Safety.

The two teams deliberately do not share an internal Python package. Each keeps
its own copy of the payloads they exchange, which is what two independently
owned services actually do, and which is what makes the boundary in CASE-2b real
rather than decorative. The cost of that choice is that nothing stops the copies
drifting apart, so this suite is the thing that does.

It also pins the four strings that make up the rest of the integration surface:
two message names and two topology values. Renaming a Signal handler on one side
without the other is exactly the kind of change that looks harmless in review and
silently strands a suspended workflow in production.
"""

from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path

import activities.gateway_activities as gateway_activities
import common.models as gateway_models
import release_safety.canary_activities as canary_activities
import release_safety.models as safety_models
from release_safety.canary_workflow import CanaryAnalysisWorkflow
from workflows.chain import AgenticChainWorkflow

REPO_ROOT = Path(__file__).resolve().parent.parent


def _fields(cls) -> dict[str, str]:
    return {f.name: str(f.type) for f in dataclasses.fields(cls)}


def test_operation_resolution_payload_matches_on_both_sides() -> None:
    """The Signal that wakes a suspended canary.

    Agent Gateway builds it, Release Safety receives it, and neither imports the
    other's definition. Field names are the contract, because that is what the
    JSON payload converter matches on.
    """
    assert set(_fields(gateway_models.GatewayOperationResolution)) == set(
        _fields(safety_models.GatewayOperationResolution)
    )


def test_start_canary_payload_covers_what_the_canary_workflow_needs() -> None:
    """Everything canary is started with has somewhere to land.

    The gateway starts the workflow by type name with a plain dict, so a field it
    invents that CanaryAnalysisInput does not declare would be silently dropped
    and the window would run with a default nobody chose.
    """
    accepted = set(_fields(safety_models.CanaryAnalysisInput))
    source = inspect.getsource(gateway_activities.start_canary_analysis)
    sent = {
        "workflow_id",
        "service",
        "version",
        "environment",
        "scripted_outcome",
        "tick_seconds",
        "window_ticks",
        "idempotency_key",
        "origin_operation_id",
        "requester",
        "gateway_namespace",
    }
    for name in sent:
        assert f'"{name}"' in source, f"the gateway no longer sends {name}"
        assert name in accepted, f"canary cannot accept {name}"
    # The correlation context is the field the whole nested-call story rests on:
    # it is the chain's workflow_id, not canary's own, and losing it would split
    # one user-visible task into two unrelated ones.
    assert "workflow_id" in accepted


def test_nested_call_fields_canary_sends_exist_on_the_gateway_request() -> None:
    """Canary builds request_nested_tool_call as a dict, by hand.

    It has to: NestedToolCallRequest lives in the gateway's package, which is not
    on Release Safety's path. So the field names it types out are checked here
    against the real dataclass instead of by an import.
    """
    accepted = set(_fields(gateway_models.NestedToolCallRequest))
    sent = {
        "tool1_name",
        "tool1_arguments",
        "tool2_name",
        "tool2_arguments",
        "idempotency_key",
        "correlation",
        "requested_action",
        "justification",
        "controlled_tool1",
        "replay_safe",
        "callback_workflow_id",
        "callback_namespace",
        "origin_operation_id",
    }
    source = inspect.getsource(canary_activities.call_agent_gateway_promote)
    for name in sent:
        assert f'"{name}"' in source, f"canary no longer sends {name}"
        assert name in accepted, f"the gateway no longer accepts {name}"

    correlation = set(_fields(gateway_models.CorrelationContext))
    for name in (
        "workflow_id",
        "workflow_id_source",
        "idempotency_key",
        "caller_principal",
        "caller_service",
        "runtime",
        "call_path",
    ):
        assert name in correlation, f"CorrelationContext no longer has {name}"


def test_canary_verdict_payload_matches_the_signal_the_chain_declares() -> None:
    sent = {
        "origin_operation_id",
        "canary_workflow_id",
        "verdict",
        "reason",
        "detail",
    }
    assert sent == set(_fields(gateway_models.CanaryVerdict))
    source = inspect.getsource(canary_activities.report_canary_verdict)
    for name in sent:
        assert f'"{name}"' in source, f"canary no longer sends {name}"


def test_message_names_agree_on_both_sides() -> None:
    """Two Updates and two Signals, referred to by string across the boundary."""
    assert (
        canary_activities.GATEWAY_NESTED_CALL_UPDATE
        == AgenticChainWorkflow.request_nested_tool_call.__name__
    )
    assert (
        canary_activities.GATEWAY_CANARY_VERDICT_SIGNAL
        == AgenticChainWorkflow.canary_verdict_reported.__name__
    )
    assert (
        gateway_activities.RELEASE_SAFETY_RESOLUTION_SIGNAL
        == CanaryAnalysisWorkflow.gateway_operation_resolved.__name__
    )
    assert (
        gateway_activities.RELEASE_SAFETY_WORKFLOW_TYPE
        == CanaryAnalysisWorkflow.__name__
    )


def _imported_packages(path: Path) -> set[str]:
    """Top-level packages a module imports, from its import statements only."""
    packages: set[str] = set()
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("from "):
            packages.add(line.split()[1].split(".")[0])
        elif line.startswith("import "):
            packages.add(line.split()[1].split(".")[0])
    return packages


def test_release_safety_does_not_import_the_gateway() -> None:
    """The boundary, enforced rather than described.

    If this fails, the two teams have quietly become one codebase again and
    CASE-2b is back to being an internal function call wearing a costume.
    """
    forbidden = {"common", "workflows", "activities", "gateway", "mock_tool"}
    for path in sorted((REPO_ROOT / "release_safety").glob("*.py")):
        leaked = _imported_packages(path) & forbidden
        assert not leaked, f"{path.name} imports {sorted(leaked)} from the gateway"


def test_the_gateway_does_not_import_release_safety() -> None:
    """And the same in the other direction.

    The gateway starts canary by workflow type name and signals it by signal
    name, on purpose. Importing their workflow class would put their code on this
    worker's image and make their deploys the gateway team's problem. Names like
    signal_release_safety_workflow are fine -- an Activity that talks to another
    team is not the same thing as a dependency on their code.
    """
    for module in ("workflows", "activities", "gateway", "common"):
        for path in sorted((REPO_ROOT / module).glob("*.py")):
            assert "release_safety" not in _imported_packages(path), (
                f"{module}/{path.name} imports the release_safety package"
            )
