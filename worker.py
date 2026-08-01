from __future__ import annotations

import asyncio
import concurrent.futures
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities.gateway_activities import (
    evaluate_policy,
    invoke_tool,
    signal_operation_callback,
    submit_nested_tool_call,
)
from workflows.nexus_handlers import AgentGatewayServiceHandler
from workflows.protected_action import ProtectedActionWorkflow
from adk_agents.release_approval_agent.temporal_integration import (
    build_google_adk_plugin,
)
from workflows.adk_session import TemporalAdkSessionWorkflow
from workflows.autonomous_agent import AutonomousAgentWorkflow
from workflows.chain import AgenticChainWorkflow

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")


async def main() -> None:
    adk_plugin = build_google_adk_plugin()
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        plugins=[adk_plugin],
    )
    # Sync activities use requests, so they run on a thread pool executor.
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[
                AgenticChainWorkflow,
                AutonomousAgentWorkflow,
                TemporalAdkSessionWorkflow,
                # Services one external tool request each, for as long as the
                # approval takes.
                ProtectedActionWorkflow,
            ],
            activities=[
                evaluate_policy,
                invoke_tool,
                # Both stay inside this namespace. Nothing on this worker holds
                # a client for anyone else's cluster.
                submit_nested_tool_call,
                signal_operation_callback,
            ],
            # The gateway's Nexus front door, behind the `agent-gateway`
            # Endpoint. This is how another team's tool asks for a protected
            # action without any access to this namespace.
            nexus_service_handlers=[AgentGatewayServiceHandler()],
            activity_executor=executor,
        )
        print(f"worker started, polling task queue '{TASK_QUEUE}'", flush=True)
        await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
