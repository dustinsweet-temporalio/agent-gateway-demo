from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from release_safety.models import (
        AgentGatewayPromoteInput,
        CanaryAnalysisInput,
        CanaryResult,
        CanaryState,
        CanaryStatusView,
        CanaryTickInput,
        CANARY_THRESHOLD,
        GatewayOperationResolution,
        PublishCanaryStateInput,
        ReportCanaryVerdictInput,
    )
    from release_safety.canary_activities import (
        call_agent_gateway_promote,
        publish_canary_state,
        report_canary_verdict,
        run_canary_tick,
    )

TICK_TIMEOUT = timedelta(seconds=15)
GATEWAY_CALL_TIMEOUT = timedelta(seconds=30)
PUBLISH_TIMEOUT = timedelta(seconds=10)


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
        if any tick is over threshold, stop; the gateway is never called
        otherwise, call Agent Gateway to request the prod promotion
        the gateway says waiting_for_approval, so record that and STOP HERE
        wake on the Signal the gateway sends once a human decides

    The stop is the whole point. It is a real suspension of this workflow's own
    execution, not of Agent Gateway's: kill this worker while the wait is open
    and the window's results, the verdict, and the operation id all come back on
    replay, with the wait resuming exactly where it was.
    """

    @workflow.init
    def __init__(self, input: CanaryAnalysisInput) -> None:
        self._input = input
        self._state = CanaryState(
            phase="running_canary",
            window_ticks=input.window_ticks,
            gateway_workflow_id=input.workflow_id,
        )
        # Set before any handler can run, because the gateway's resolution Signal
        # can in principle arrive before the Update that requested it has been
        # observed as returning here.
        self._pending_resolution: GatewayOperationResolution | None = None

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
        # more right to do it unsupervised than anyone else does. The call is made
        # from an Activity, never from workflow code, because it needs a Temporal
        # client pointed at the other namespace and workflow code must stay
        # deterministic.
        gateway = await workflow.execute_activity(
            call_agent_gateway_promote,
            AgentGatewayPromoteInput(
                gateway_workflow_id=input.workflow_id,
                gateway_namespace=input.gateway_namespace,
                canary_workflow_id=workflow.info().workflow_id,
                origin_operation_id=input.origin_operation_id,
                service=input.service,
                version=input.version,
                environment=input.environment,
                idempotency_key=(
                    input.idempotency_key or workflow.info().workflow_id
                ),
                requester=input.requester,
                justification=(
                    f"Canary passed {input.window_ticks} consecutive ticks for "
                    f"{input.service} {input.version}, all under the "
                    f"{CANARY_THRESHOLD:.0%} error-rate threshold"
                ),
            ),
            start_to_close_timeout=GATEWAY_CALL_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        self._state.gateway_workflow_id = (
            gateway.get("workflow_id") or input.workflow_id
        )
        self._state.gateway_operation_id = gateway.get("operation_id")

        status = gateway.get("status")
        if status == "completed":
            # Policy did not protect this promotion. Not what the demo's policy
            # does for prod, but the workflow should be correct under a policy
            # configuration it does not control.
            self._state.phase = "completed"
            self._state.promotion_result = gateway.get("result")
            await self._publish()
            return self._result()

        if status != "waiting_for_approval":
            # The gateway declined before any human saw it: a rejected update, a
            # failed policy evaluation, a chain that is not accepting work.
            # Canary has no promotion to wait for, so it stops.
            self._state.phase = "rejected" if status == "rejected" else "failed"
            self._state.error = (
                gateway.get("message")
                or gateway.get("reason")
                or f"Agent Gateway returned {status!r}"
            )
            await self._publish()
            await self._report_verdict(
                "gateway_declined", self._state.error
            )
            return self._result()

        self._state.phase = "awaiting_prod_approval"
        await self._publish()

        # Phase 3: the checkpoint. Execution genuinely stops here. Everything
        # above this line -- every tick, the verdict, the operation id -- is
        # already in this workflow's own Event History, in this namespace, under
        # this team's retention policy. Kill the release-safety worker now and
        # replay rebuilds all of it without re-running a single tick, and this
        # wait picks up exactly where it left off.
        await workflow.wait_condition(lambda: self._pending_resolution is not None)
        resolution = self._pending_resolution
        assert resolution is not None

        if resolution.status == "completed":
            self._state.phase = "completed"
            self._state.promotion_result = resolution.result
        elif resolution.status == "rejected":
            self._state.phase = "rejected"
            self._state.error = resolution.reason or "the approver rejected it"
        elif resolution.status == "canceled":
            self._state.phase = "rejected"
            self._state.error = resolution.reason or "the operation was canceled"
        else:
            self._state.phase = "expired"
            self._state.error = resolution.reason or "approval window expired"
        await self._publish()
        return self._result()

    # ------------------------------------------------------------- handlers

    @workflow.signal
    def gateway_operation_resolved(
        self, resolution: GatewayOperationResolution
    ) -> None:
        """Agent Gateway's callback once a human decided.

        Mutates state only. The run method's wait_condition observes it, which is
        the same discipline the chain workflow follows on its side: handlers do
        not drive Activities.
        """
        if self._pending_resolution is not None:
            # Already resolved. A duplicate delivery is not an error; Signals are
            # at-least-once and the gateway retries this Activity.
            return
        expected = self._state.gateway_operation_id
        if expected is not None and resolution.operation_id != expected:
            workflow.logger.warning(
                "ignoring resolution for an unrelated operation",
                extra={
                    "received": resolution.operation_id,
                    "expected": expected,
                },
            )
            return
        self._pending_resolution = resolution

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
        calling promote_release; a red one never calls, and without this the
        release pipeline operation that handed off would sit waiting on a request
        that is never going to arrive.
        """
        if not self._input.origin_operation_id:
            return
        try:
            await workflow.execute_activity(
                report_canary_verdict,
                ReportCanaryVerdictInput(
                    gateway_workflow_id=self._input.workflow_id,
                    gateway_namespace=self._input.gateway_namespace,
                    canary_workflow_id=workflow.info().workflow_id,
                    origin_operation_id=self._input.origin_operation_id,
                    verdict=verdict,
                    reason=reason,
                    detail={
                        "tick_results": list(self._state.tick_results),
                        "threshold": CANARY_THRESHOLD,
                    },
                ),
                start_to_close_timeout=GATEWAY_CALL_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
        except ActivityError as err:
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
