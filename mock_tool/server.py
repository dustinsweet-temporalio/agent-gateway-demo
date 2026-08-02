from __future__ import annotations

import asyncio
import os
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

# Pretend deployment backend. Holds just enough in-memory state to keep the demo
# story internally consistent: cut releases are remembered, and a promotion moves
# the target environment to that version so a later read reflects it.
_deployed: dict[str, str] = {"test": "2.3.0", "staging": "2.2.0", "prod": "2.1.0"}
_releases: set[tuple[str, str]] = set()
_autonomous_followups: set[str] = set()

# Idempotency store keyed by the Idempotency-Key header. A replayed activity
# attempt with the same key returns the original result and does not apply the
# effect a second time.
_seen: dict[str, dict] = {}

# Demo knob: cutting this release runs long, which trips the gateway's
# monitor-and-convert budget and demonstrates the sync-to-async conversion. The
# delay stays under the invoke Activity's request timeout.
SLOW_CUT_VERSION = os.getenv("SLOW_CUT_VERSION", "2.3.1")
SLOW_CUT_DELAY_SECONDS = float(os.getenv("SLOW_CUT_DELAY_SECONDS", "8"))


async def _maybe_delay(tool_name: str, arguments: dict) -> None:
    if (
        tool_name == "cut_release"
        and str(arguments.get("version", "")) == SLOW_CUT_VERSION
    ):
        await asyncio.sleep(SLOW_CUT_DELAY_SECONDS)


def _handle(tool_name: str, arguments: dict) -> dict:
    if tool_name == "get_deployed_version":
        env = str(arguments.get("environment", ""))
        version = _deployed.get(env)
        return {
            "environment": env,
            "deployed_version": version,
            "message": (
                f"{version} is currently deployed in {env}"
                if version
                else f"no known deployment in {env}"
            ),
        }

    if tool_name == "cut_release":
        service = str(arguments.get("service", ""))
        version = str(arguments.get("version", ""))
        _releases.add((service, version))
        return {
            "service": service,
            "version": version,
            "release": "cut",
            "message": f"release {version} of {service} is cut and ready to promote",
        }

    if tool_name == "promote_release":
        service = str(arguments.get("service", ""))
        version = str(arguments.get("version", ""))
        env = str(arguments.get("environment", ""))
        _deployed[env] = version
        return {
            "service": service,
            "version": version,
            "environment": env,
            "promoted": True,
            "message": f"promoted {service} {version} to {env}",
        }

    if tool_name == "release_orchestrator_prepare":
        return {
            "checkpointed": True,
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "next_tool": "promote_release",
            "message": "Tool1 checkpointed before its nested Tool2 call",
        }

    if tool_name == "release_orchestrator_resume":
        nested_result = arguments.get("nested_result")
        return {
            "resumed": True,
            "replayed": bool(arguments.get("replay")),
            "nested_result_received": nested_result is not None,
            "message": "Tool1 resumed with the durable Tool2 result",
        }

    if tool_name == "record_autonomous_followup":
        run_id = str(arguments.get("agent_run_id", ""))
        _autonomous_followups.add(run_id)
        return {
            "agent_run_id": run_id,
            "followup_recorded": True,
            "message": "Autonomous dependent action executed after approval",
        }

    return {"tool_name": tool_name, "message": f"executed {tool_name}"}


async def invoke(request: Request) -> JSONResponse:
    body = await request.json()
    key = request.headers.get("Idempotency-Key", "")

    if key and key in _seen:
        prior = dict(_seen[key])
        prior["idempotent_replay"] = True
        return JSONResponse(prior)

    tool_name = body.get("tool_name", "")
    arguments = body.get("arguments", {}) or {}
    await _maybe_delay(tool_name, arguments)

    result = _handle(tool_name, arguments)
    result["executed_at"] = time.time()
    result["idempotent_replay"] = False
    if key:
        _seen[key] = result
    return JSONResponse(result)


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


app = Starlette(
    routes=[
        Route("/invoke", invoke, methods=["POST"]),
        Route("/healthz", healthz, methods=["GET"]),
    ]
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("MOCK_TOOL_PORT", "9000")))
