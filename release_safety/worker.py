"""Release Safety's worker. A separate process from Agent Gateway's.

Separate namespace, separate task queue, separate container, separate identity in
the Temporal UI. None of that is decoration: the claim CASE-2b makes is that
canary analysis is another team's independently durable system, and a class name
in someone else's worker would not be that.

Run it with:  python -m release_safety.worker
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os

import requests
from temporalio.client import Client
from temporalio.worker import Worker

from release_safety.canary_activities import (
    call_agent_gateway_promote,
    publish_canary_state,
    report_canary_verdict,
    run_canary_tick,
)
from release_safety.canary_workflow import CanaryAnalysisWorkflow
from release_safety.models import (
    CANARY_NAMESPACE,
    CANARY_TASK_QUEUE,
    CANARY_TICK_SECONDS,
    CANARY_WINDOW_TICKS,
)

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", CANARY_NAMESPACE)
TASK_QUEUE = os.getenv("TASK_QUEUE", CANARY_TASK_QUEUE)
REGISTRY_URL = os.getenv("RELEASE_SAFETY_METRICS_URL", "http://mock-tool:9000/invoke")
# How often this worker tells the shared platform it is here. The registration
# carries a TTL on the other side, so stopping this container makes the
# capability go away on its own rather than lingering as a stale advertisement.
HEARTBEAT_SECONDS = float(os.getenv("RELEASE_SAFETY_HEARTBEAT_SECONDS", "5"))


async def _announce_forever() -> None:
    """Advertise canary analysis to the shared platform, on a heartbeat.

    This is how Waypoint's release pipeline discovers that the capability exists
    at all. Before Release Safety shipped it, the pipeline went from green
    quality gates straight to opening the production promotion, because there was
    nothing else to route through. It still does exactly that whenever this
    worker is not running. The pipeline is not configured for canary; it looks to
    see whether canary is there.
    """
    while True:
        try:
            await asyncio.to_thread(
                lambda: requests.post(
                    REGISTRY_URL,
                    json={
                        "tool_name": "register_canary_capability",
                        "arguments": {
                            "provider": "release-safety",
                            "namespace": NAMESPACE,
                            "task_queue": TASK_QUEUE,
                            "workflow_type": "CanaryAnalysisWorkflow",
                            # This team's window shape, published so callers
                            # know what they are waiting for rather than
                            # deciding it for us.
                            "tick_seconds": CANARY_TICK_SECONDS,
                            "window_ticks": CANARY_WINDOW_TICKS,
                            "ttl_seconds": HEARTBEAT_SECONDS * 4,
                        },
                    },
                    timeout=5,
                ).raise_for_status()
            )
        except Exception as err:  # noqa: BLE001 - advertising is best effort
            print(f"capability heartbeat failed: {err}", flush=True)
        await asyncio.sleep(HEARTBEAT_SECONDS)


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
    heartbeat = asyncio.create_task(_announce_forever())
    # run_canary_tick and publish_canary_state are synchronous, so they need a
    # thread pool; the two gateway-facing Activities are async and do not.
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[CanaryAnalysisWorkflow],
            activities=[
                run_canary_tick,
                call_agent_gateway_promote,
                report_canary_verdict,
                publish_canary_state,
            ],
            activity_executor=executor,
            identity=f"release-safety-worker@{os.getenv('HOSTNAME', 'local')}",
        )
        print(
            f"release safety worker started, namespace {NAMESPACE!r}, "
            f"task queue {TASK_QUEUE!r}",
            flush=True,
        )
        try:
            await worker.run()
        finally:
            heartbeat.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
