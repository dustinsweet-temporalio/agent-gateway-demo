from __future__ import annotations

import asyncio
import os
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

DEFAULT_SERVICE = os.getenv("DEMO_SERVICE", "delivery-matching-service")
ENVIRONMENTS = ("staging", "prod")

_BOOT = time.time()

# Pretend deployment backend. Holds just enough in-memory state to keep the demo
# story internally consistent: cut releases are remembered, and a promotion moves
# the target environment to that version so a later read reflects it. Each record
# also keeps the version it replaced and when it landed, which is what the fleet
# panel needs to render a promotion as a transition rather than a bare value.
# Seed times are backdated so a fresh boot reads as an existing fleet rather than
# deployments that all landed the moment the demo started.
_deployed: dict[str, dict] = {
    env: {
        "service": DEFAULT_SERVICE,
        "version": version,
        "previous_version": previous,
        "updated_at": _BOOT - age,
        "promotions": 0,
    }
    for env, version, previous, age in (
        ("staging", "2.2.0", None, 3300.0),
        ("prod", "2.2.0", "2.1.0", 2700.0),
    )
}
# Insertion ordered, so the fleet rail can show newest releases first. Seeded with
# the version running everywhere and the one production moved past. seen_in records
# every environment a release has ever reached, in promotion order, so the rail can
# tell a release that is ready to go from one already superseded.
_releases: dict[tuple[str, str], dict] = {
    (DEFAULT_SERVICE, version): {
        "service": DEFAULT_SERVICE,
        "version": version,
        "cut_at": _BOOT - offset,
        "seen_in": list(seen_in),
    }
    for version, offset, seen_in in (
        ("2.1.0", 5400.0, ("prod",)),
        ("2.2.0", 3600.0, ("staging", "prod")),
    )
}
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
        record = _deployed.get(env)
        version = record["version"] if record else None
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
        prior_cut = _releases.pop((service, version), None)
        _releases[(service, version)] = {
            "service": service,
            "version": version,
            "cut_at": time.time(),
            "seen_in": list(prior_cut["seen_in"]) if prior_cut else [],
        }
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
        prior = _deployed.get(env)
        _deployed[env] = {
            "service": service,
            "version": version,
            "previous_version": prior["version"] if prior else None,
            "updated_at": time.time(),
            "promotions": (prior["promotions"] + 1) if prior else 1,
        }
        # A version can be promoted without a matching cut_release in this demo
        # backend, so register it here too. Nothing reaches an environment without
        # appearing in the release list.
        release = _releases.get((service, version))
        if release is None:
            release = {
                "service": service,
                "version": version,
                "cut_at": time.time(),
                "seen_in": [],
            }
            _releases[(service, version)] = release
        if env in ENVIRONMENTS and env not in release["seen_in"]:
            release["seen_in"].append(env)
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


async def state(request: Request) -> JSONResponse:
    """Read only view of the fleet: what is running where, and what is promotable.

    The approval dashboard renders this next to the approval queue so an approver
    can see the effect of a decision, not just the decision. It is a projection of
    the same in-memory state the tools mutate, so it can never disagree with what
    get_deployed_version returns.
    """
    return JSONResponse(
        {
            "generated_at": time.time(),
            "environments": [
                {"environment": env, **_deployed[env]}
                for env in ENVIRONMENTS
                if env in _deployed
            ],
            "releases": [
                dict(record)
                for record in sorted(
                    _releases.values(),
                    key=lambda item: item["cut_at"],
                    reverse=True,
                )
            ],
        }
    )


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


app = Starlette(
    routes=[
        Route("/invoke", invoke, methods=["POST"]),
        Route("/state", state, methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
    ]
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("MOCK_TOOL_PORT", "9000")))
