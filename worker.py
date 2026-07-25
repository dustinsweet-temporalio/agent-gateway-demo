from __future__ import annotations

import asyncio
import concurrent.futures
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities.gateway_activities import evaluate_policy, invoke_tool
from workflows.autonomous_agent import AutonomousAgentWorkflow
from workflows.chain import AgenticChainWorkflow

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS)
    # Sync activities use requests, so they run on a thread pool executor.
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[AgenticChainWorkflow, AutonomousAgentWorkflow],
            activities=[evaluate_policy, invoke_tool],
            activity_executor=executor,
        )
        print(f"worker started, polling task queue '{TASK_QUEUE}'", flush=True)
        await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
