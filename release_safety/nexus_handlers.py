"""Release Safety's Nexus service handler.

This team's front door. Behind the `release-safety` Nexus Endpoint, registered on
this team's worker, in this team's namespace. Agent Gateway addresses the
endpoint and nothing else -- it cannot see the namespace, the task queue, or the
workflow type, so all three can change here without anyone else's deploy.
"""

from __future__ import annotations

import uuid

from nexusrpc.handler import service_handler, sync_operation
from nexusrpc.handler import StartOperationContext
from temporalio import nexus

from release_safety import models
from release_safety.canary_workflow import CanaryAnalysisWorkflow
from release_safety.models import CanaryAnalysisInput
from release_safety.nexus_contracts import (
    CanaryWindowOpened,
    OpenCanaryWindowInput,
    ReleaseSafetyService,
)


@service_handler(service=ReleaseSafetyService)
class ReleaseSafetyServiceHandler:
    @sync_operation
    async def open_canary_window(
        self, ctx: StartOperationContext, input: OpenCanaryWindowInput
    ) -> CanaryWindowOpened:
        """Open a canary window and hand back its id.

        Synchronous on purpose, and this is the one design decision in the file
        worth reading twice. The obvious alternative -- an asynchronous operation
        the pipeline awaits for the whole window -- would put a Nexus operation
        handle inside AgenticChainWorkflow, which is a long-lived entity workflow
        that continues-as-new. Handles do not survive Continue-As-New, so the
        window would be orphaned mid-approval. Returning immediately means the
        pipeline holds an id, which is plain data and carries across CAN like any
        other state.

        The window's shape -- how many ticks, how far apart, at what threshold --
        is decided here, from this team's own constants. It is not an argument,
        because it is not the caller's decision.
        """
        client = nexus.client()
        canary_workflow_id = (
            f"release-safety::canary::{input.service}::{input.version}::"
            f"{uuid.uuid4().hex[:8]}"
        )
        await client.start_workflow(
            CanaryAnalysisWorkflow.run,
            CanaryAnalysisInput(
                workflow_id=input.gateway_workflow_id,
                service=input.service,
                version=input.version,
                environment=input.environment,
                idempotency_key=input.idempotency_key,
                origin_operation_id=input.origin_operation_id,
                requester=input.requester,
                # Read at call time rather than bound as dataclass defaults, so
                # a test can shorten the window through the same path the real
                # worker uses instead of a test-only branch.
                tick_seconds=models.CANARY_TICK_SECONDS,
                window_ticks=models.CANARY_WINDOW_TICKS,
                scripted_outcome=_scripted_outcome(input.version),
            ),
            id=canary_workflow_id,
            task_queue=models.CANARY_TASK_QUEUE,
        )
        return CanaryWindowOpened(canary_workflow_id=canary_workflow_id)


def _scripted_outcome(version: str) -> str:
    """Demo knob, and this team's business rather than the caller's.

    Which versions fail canary used to be relayed from the shared registry
    through the pipeline, which meant the caller was telling the canary what
    verdict to reach. It is read here now, where it belongs.
    """
    return "fail" if version in models.canary_fail_versions() else "pass"
