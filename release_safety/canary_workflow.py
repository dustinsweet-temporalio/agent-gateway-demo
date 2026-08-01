from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from release_safety.models import (
        CanaryAnalysisInput,
        CanaryResult,
        CanaryState,
        CanaryStatusView,
        CanaryTickInput,
        CANARY_THRESHOLD,
        PublishCanaryStateInput,
    )
    from release_safety.nexus_contracts import (
        AGENT_GATEWAY_ENDPOINT,
        AgentGatewayService,
        ProtectedActionRequest,
        ToolOutcomeReport,
    )
    from release_safety.canary_activities import (
        publish_canary_state,
        run_canary_tick,
    )

TICK_TIMEOUT = timedelta(seconds=15)
PUBLISH_TIMEOUT = timedelta(seconds=10)
# The gateway holds this open across a human decision, so there is no useful
# upper bound short of the approval window itself plus slack.
PROTECTED_ACTION_TIMEOUT = timedelta(minutes=30)
REPORT_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class CanaryAnalysisWorkflow:
    """Release Safety's canary window, and the checkpoint that outlives a pause.

    This is Tool1 in the requirements document's sense: a tool that is owned and
    operated by another team, is independently durable, and can therefore be
    told "waiting_for_approval" and survive it. Nothing about that is simulated
    here. This workflow runs in the release-safety namespace, on the
    release-safety-tq task queue, in a worker process the Agent Gateway team does
    not deploy. It is not a child workflow of the chain: the two are peers,
    correlated by a shared workflow_id value that is passed in explicitly.

    The shape of the run is:

        watch the canary window, tick by tick, entirely on its own
        if any tick is over threshold, stop; the gateway is never asked
        otherwise, call Agent Gateway's Nexus endpoint to request the promotion
        SUSPEND on that operation until a human somewhere decides

    The stop is the whole point, and it is a real suspension of this workflow's
    own execution rather than of Agent Gateway's. Kill this worker while the
    operation is pending and the window's results, the verdict, and the pending
    operation all come back on replay, with the wait resuming exactly where it
    was.

    Note how little this workflow knows about the other side. It calls an
    endpoint by name. It does not know Agent Gateway's namespace, holds no
    credentials for it, names none of its workflows, and does not construct any
    of its internal types. There is no callback Signal to receive, because the
    Nexus operation's own completion is the answer.
    """

    @workflow.init
    def __init__(self, input: CanaryAnalysisInput) -> None:
        self._input = input
        self._state = CanaryState(
            phase="running_canary",
            window_ticks=input.window_ticks,
            gateway_workflow_id=input.workflow_id,
        )

    @workflow.run
    async def run(self, input: CanaryAnalysisInput) -> CanaryResult:
        await self._publish()

        # Phase 1: the canary window. Entirely local to Release Safety; Agent
        # Gateway does not know this is happening and does not need to. Each tick
        # is its own Activity so it lands in Event History individually: a worker
        # restart mid-window resumes at the next un-run tick instead of starting
        # the window over, which is the property being demonstrated when the
        # container is killed between tick 2 and tick 3.
        for tick_number in range(1, input.window_ticks + 1):
            tick = await workflow.execute_activity(
                run_canary_tick,
                CanaryTickInput(
                    service=input.service,
                    version=input.version,
                    tick_number=tick_number,
                    window_ticks=input.window_ticks,
                    scripted_outcome=input.scripted_outcome,
                    canary_workflow_id=workflow.info().workflow_id,
                    environment=input.environment,
                ),
                start_to_close_timeout=TICK_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            self._state.tick_results.append(tick)
            self._state.ticks_completed = tick_number
            if not tick["passed"]:
                return await self._fail_window(tick)
            await self._publish()
            if tick_number < input.window_ticks:
                await workflow.sleep(timedelta(seconds=input.tick_seconds))

        self._state.verdict = "pass"

        # Phase 2: the nested call. Canary is now itself a tool caller, and it
        # goes through Agent Gateway rather than promoting anything directly,
        # because promoting to production is a protected action and canary has no
        # more right to do it unsupervised than anyone else does.
        #
        # Straight from workflow code. No Activity, no client, no credentials for
        # anyone else's namespace: a Nexus call to a published endpoint.
        gateway = workflow.create_nexus_client(
            service=AgentGatewayService, endpoint=AGENT_GATEWAY_ENDPOINT
        )
        handle = await gateway.start_operation(
            AgentGatewayService.request_protected_action,
            ProtectedActionRequest(
                gateway_workflow_id=input.workflow_id,
                tool_name="promote_release",
                arguments={
                    "service": input.service,
                    "version": input.version,
                    "environment": input.environment,
                },
                idempotency_key=(
                    input.idempotency_key or workflow.info().workflow_id
                ),
                caller_service="release-safety",
                caller_principal=input.requester,
                caller_workflow_id=workflow.info().workflow_id,
                origin_operation_id=input.origin_operation_id,
                justification=(
                    f"Canary passed {input.window_ticks} consecutive ticks for "
                    f"{input.service} {input.version}, all under the "
                    f"{CANARY_THRESHOLD:.0%} error-rate threshold"
                ),
            ),
            schedule_to_close_timeout=PROTECTED_ACTION_TIMEOUT,
        )
        self._state.gateway_workflow_id = input.workflow_id
        self._state.gateway_operation_token = handle.operation_token
        self._state.phase = "awaiting_prod_approval"
        await self._publish()

        # Phase 3: the checkpoint. Execution genuinely stops here, on a pending
        # Nexus operation, for as long as it takes a person to decide.
        #
        # Everything above this line -- every tick, the verdict, the outbound
        # operation -- is already in this workflow's own Event History, in this
        # namespace, under this team's retention policy. Kill the release-safety
        # worker now and replay rebuilds all of it without re-running a single
        # tick, and this await picks up exactly where it left off.
        outcome = await handle
        self._state.gateway_operation_id = outcome.operation_id

        if outcome.status == "completed":
            self._state.phase = "completed"
            self._state.promotion_result = outcome.result
        elif outcome.status in ("rejected", "canceled"):
            self._state.phase = "rejected"
            self._state.error = outcome.reason or f"the promotion was {outcome.status}"
        elif outcome.status == "expired":
            self._state.phase = "expired"
            self._state.error = outcome.reason or "approval window expired"
        else:
            self._state.phase = "failed"
            self._state.error = (
                outcome.reason or f"Agent Gateway returned {outcome.status!r}"
            )
        await self._publish()
        return self._result()

    # ------------------------------------------------------------- handlers
    #
    # There is no resolution Signal handler here any more, and its absence is
    # the clearest single measure of what Nexus bought. Agent Gateway used to
    # need this workflow's id and a signal name to wake it; now the operation
    # this workflow is suspended on simply completes.

    @workflow.query
    def get_status(self) -> CanaryStatusView:
        return CanaryStatusView(
            canary_workflow_id=workflow.info().workflow_id,
            service=self._input.service,
            version=self._input.version,
            environment=self._input.environment,
            phase=self._state.phase,
            ticks_completed=self._state.ticks_completed,
            window_ticks=self._state.window_ticks,
            tick_results=list(self._state.tick_results),
            verdict=self._state.verdict,
            gateway_workflow_id=self._state.gateway_workflow_id,
            gateway_operation_token=self._state.gateway_operation_token,
            gateway_operation_id=self._state.gateway_operation_id,
            promotion_result=self._state.promotion_result,
            error=self._state.error,
        )

    # -------------------------------------------------------------- helpers

    async def _fail_window(self, tick: dict) -> CanaryResult:
        """One tick over threshold ends the window.

        Deliberately not averaged, retried past, or given a second chance. The
        gateway is never called, so a release that failed its canary never
        reaches the approval queue at all -- the same fail-closed shape the
        quality gate already has, one checkpoint further along.
        """
        self._state.phase = "canary_failed"
        self._state.verdict = "fail"
        self._state.error = (
            f"Tick {tick['tick']} of {self._state.window_ticks} reported an "
            f"error rate of {tick['error_rate']:.1%}, over the "
            f"{tick['threshold']:.1%} threshold"
        )
        await self._publish()
        await self._report_verdict("fail", self._state.error)
        return self._result()

    async def _report_verdict(self, verdict: str, reason: str | None) -> None:
        """Tell Agent Gateway the promotion is not coming.

        Only needed when the window closes red. A green window reports itself by
        requesting the promotion; a red one never asks for anything, and without
        this the release pipeline operation that handed off would sit waiting on
        a request that is never going to arrive.

        Synchronous, because there is nothing to wait for.
        """
        if not self._input.origin_operation_id:
            return
        gateway = workflow.create_nexus_client(
            service=AgentGatewayService, endpoint=AGENT_GATEWAY_ENDPOINT
        )
        try:
            await gateway.execute_operation(
                AgentGatewayService.report_tool_outcome,
                ToolOutcomeReport(
                    gateway_workflow_id=self._input.workflow_id,
                    origin_operation_id=self._input.origin_operation_id,
                    caller_workflow_id=workflow.info().workflow_id,
                    outcome=verdict,
                    reason=reason,
                    detail={
                        "tick_results": list(self._state.tick_results),
                        "threshold": CANARY_THRESHOLD,
                    },
                ),
                schedule_to_close_timeout=REPORT_TIMEOUT,
            )
        except Exception as err:
            # The window's own verdict is already durable here. Failing to
            # deliver it does not change what canary observed, so it is recorded
            # and the workflow still completes with the right answer.
            workflow.logger.error(
                "could not deliver the canary verdict to Agent Gateway",
                extra={"error": str(err)},
            )

    async def _publish(self) -> None:
        """Push window progress to the shared observability backend.

        Visibility only. A publish failure must never fail a canary window, so
        this swallows Activity errors rather than propagating them: whether the
        dashboard can draw a card has nothing to do with whether the release is
        safe.
        """
        try:
            await workflow.execute_activity(
                publish_canary_state,
                PublishCanaryStateInput(
                    canary_workflow_id=workflow.info().workflow_id,
                    service=self._input.service,
                    version=self._input.version,
                    environment=self._input.environment,
                    phase=self._state.phase,
                    ticks_completed=self._state.ticks_completed,
                    window_ticks=self._state.window_ticks,
                    threshold=CANARY_THRESHOLD,
                    tick_results=list(self._state.tick_results),
                    verdict=self._state.verdict,
                    gateway_operation_id=self._state.gateway_operation_id,
                    error=self._state.error,
                ),
                start_to_close_timeout=PUBLISH_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=2),
            )
        except ActivityError as err:
            workflow.logger.warning(
                "could not publish canary state", extra={"error": str(err)}
            )

    def _result(self) -> CanaryResult:
        return CanaryResult(
            phase=self._state.phase,
            verdict=self._state.verdict,
            tick_results=list(self._state.tick_results),
            gateway_operation_id=self._state.gateway_operation_id,
            promotion_result=self._state.promotion_result,
            error=self._state.error,
        )
