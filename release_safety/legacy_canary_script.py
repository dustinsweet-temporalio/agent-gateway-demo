"""Legacy canary check, for services that predate Release Safety's platform.

This script has no memory. It runs, it checks, and it either finishes or it does
not. There is no workflow, no Event History, no task queue, no worker, and no
supervisor that brings it back. Nothing here imports temporalio, and that is the
whole point: it is the requirements document's uncontrolled tool, the kind that
"cannot assume that the upstream tool can preserve stack state, hold open
execution, retry safely, or accept a later resumed result".

If Agent Gateway pauses the promotion this script triggers, the script cannot
wait for a human. It prints the operation id it was given and exits non-zero.
Somebody has to come back later and finish the promotion by hand. Compare
release_safety/canary_workflow.py, which checkpoints and resumes, and which is
the same job done by a system that can hold its own state.

    python -m release_safety.legacy_canary_script \\
        --service delivery-matching-service --version 2.3.0 \\
        --gateway-workflow-id wf-abc123

It speaks MCP to the gateway rather than posting raw JSON, because that is the
only interface the gateway has -- streamable HTTP MCP needs an initialize
handshake and a session id before a tool call, so a plain curl cannot do it. An
MCP client is still just a client. It buys this script no durability whatsoever.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

GATEWAY_MCP_URL = os.getenv("GATEWAY_MCP_URL", "http://localhost:8080/mcp")
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN", "tok_dustin")

THRESHOLD = 0.02
# The same scripted sequences the platform canary uses, so the two paths are
# compared on how they behave at the pause and nothing else.
SCHEDULE = {
    "pass": [0.004, 0.006, 0.003, 0.002],
    "fail": [0.031, 0.0, 0.0, 0.0],
}


def run_window(scripted_outcome: str, tick_seconds: int, window_ticks: int) -> bool:
    rates = SCHEDULE[scripted_outcome]
    for tick in range(1, window_ticks + 1):
        error_rate = rates[min(tick - 1, len(rates) - 1)]
        print(
            f"tick {tick}/{window_ticks}: error_rate={error_rate:.3f} "
            f"threshold={THRESHOLD:.3f}",
            flush=True,
        )
        if error_rate > THRESHOLD:
            print(f"CANARY FAILED at tick {tick}. Stopping.", flush=True)
            return False
        if tick < window_ticks:
            time.sleep(tick_seconds)
    return True


async def request_promotion(
    url: str,
    token: str,
    service: str,
    version: str,
    environment: str,
    gateway_workflow_id: str,
) -> dict:
    async with streamablehttp_client(
        url, headers={"Authorization": f"Bearer {token}"}
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "run_nested_release",
                {
                    "service": service,
                    "version": version,
                    "environment": environment,
                    # Honest self-description. This caller cannot hold a pause,
                    # so it says so, and Agent Gateway fails closed accordingly.
                    "tool1_mode": "uncontrolled",
                    "workflow_id": gateway_workflow_id,
                    "justification": (
                        f"Legacy canary passed for {service} {version}"
                    ),
                },
            )
            if result.isError:
                text = result.content[0].text if result.content else "tool error"
                raise SystemExit(f"gateway call failed: {text}")
            return result.structuredContent or {}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--environment", default="prod")
    parser.add_argument("--scripted-outcome", default="pass", choices=sorted(SCHEDULE))
    parser.add_argument("--gateway-workflow-id", required=True)
    parser.add_argument("--tick-seconds", type=int, default=5)
    parser.add_argument("--window-ticks", type=int, default=4)
    parser.add_argument("--url", default=GATEWAY_MCP_URL)
    parser.add_argument("--token", default=GATEWAY_TOKEN)
    args = parser.parse_args()

    if not run_window(args.scripted_outcome, args.tick_seconds, args.window_ticks):
        sys.exit(1)

    print(
        "Canary passed. Requesting the production promotion via Agent Gateway.",
        flush=True,
    )
    body = asyncio.run(
        request_promotion(
            args.url,
            args.token,
            args.service,
            args.version,
            args.environment,
            args.gateway_workflow_id,
        )
    )

    status = body.get("status")
    if status in ("waiting_for_approval", "blocked_nested_approval"):
        # Everything this process knows is about to stop existing. All it can do
        # is print the identifiers and hope somebody writes them down.
        print(
            "\nApproval is required and this script cannot wait for it.\n"
            "Record these to finish the promotion by hand once it is approved:\n"
            f"  workflow_id:  {body.get('workflow_id')}\n"
            f"  operation_id: {body.get('operation_id')}\n\n"
            "  python gateway_call.py resume_nested_release "
            f"workflow_id={body.get('workflow_id')} "
            f"operation_id={body.get('operation_id')}\n",
            flush=True,
        )
        sys.exit(2)

    print(json.dumps(body, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
