"""Legacy pre-prod scanner, for services that predate the Security team's platform.

A handful of services owned by the Waypoint team still gate through this old
scanner rather than
the workflow next door. It is on the list to be migrated and it is not what the
Security team runs today; it is here because comparing the two at the pause is
the entire point of this file.

This script has no memory. It runs, it scans, and it either finishes or it does
not. There is no workflow, no Event History, no task queue, no worker, and no
supervisor that brings it back. Nothing here imports temporalio, and that is the
whole point: it is the requirements document's uncontrolled tool, the kind that
"cannot assume that the upstream tool can preserve stack state, hold open
execution, retry safely, or accept a later resumed result".

If Agent Gateway pauses the promotion this script triggers, the script cannot
wait for a human. It prints the operation id it was given and exits non-zero.
Somebody has to come back later and finish the promotion by hand. Compare
security_scan/security_scan_workflow.py, which checkpoints and resumes, and which
is the same job done by a system that can hold its own state.

    python -m security_scan.legacy_security_scan_script \\
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

SEVERITY_ORDER = ["none", "low", "medium", "high", "critical"]
SEVERITY_THRESHOLD = "medium"
STAGE_NAMES = [
    "dependency_scan",
    "container_image_scan",
    "secret_detection",
    "static_analysis",
]
# The same scripted sequences the platform scan uses, so the two paths are
# compared on how they behave at the pause and nothing else.
SCHEDULE = {
    "pass": ["none", "low", "none", "none"],
    "fail": ["high", "none", "none", "none"],
}


def _blocking(severity: str) -> bool:
    try:
        return SEVERITY_ORDER.index(severity) >= SEVERITY_ORDER.index(
            SEVERITY_THRESHOLD
        )
    except ValueError:
        return True


def run_scan(scripted_outcome: str, check_seconds: int, stage_count: int) -> bool:
    severities = SCHEDULE[scripted_outcome]
    for stage in range(1, stage_count + 1):
        name = STAGE_NAMES[min(stage - 1, len(STAGE_NAMES) - 1)]
        max_severity = severities[min(stage - 1, len(severities) - 1)]
        print(
            f"stage {stage}/{stage_count} {name}: max_severity={max_severity} "
            f"threshold={SEVERITY_THRESHOLD}",
            flush=True,
        )
        if _blocking(max_severity):
            print(
                f"SECURITY SCAN FAILED at {name} (1 finding, {max_severity}). "
                f"Stopping.",
                flush=True,
            )
            return False
        if stage < stage_count:
            time.sleep(check_seconds)
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
                        f"Legacy security scan passed for {service} {version}"
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
    parser.add_argument("--check-seconds", type=int, default=5)
    parser.add_argument("--stage-count", type=int, default=4)
    parser.add_argument("--url", default=GATEWAY_MCP_URL)
    parser.add_argument("--token", default=GATEWAY_TOKEN)
    args = parser.parse_args()

    if not run_scan(args.scripted_outcome, args.check_seconds, args.stage_count):
        sys.exit(1)

    print(
        "Security scan passed. Requesting the production promotion via Agent "
        "Gateway.",
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
