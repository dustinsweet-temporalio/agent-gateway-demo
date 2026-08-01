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

# Last quality gate run, or None if the gates have never run. None is meaningful:
# the fleet panel renders the gate card only once this exists, so the card is
# absent for the whole of CASE-1 and appears the first time the CASE-2 pipeline
# reaches it. That is the narrative, not a demo toggle: the capability shows up
# when the team builds it. Restarting this service restores the "before" picture.
_quality_gates: dict | None = None

# Last canary window, or None if canary analysis has never run. Same contract as
# _quality_gates above: the fleet panel renders the canary card only once this
# exists, so the card is absent for CASE-1 and CASE-2a and appears the first time
# Release Safety's platform opens a window. Restarting this service restores the
# "before" picture for both cards.
_canary: dict | None = None

# Who is currently offering canary analysis, and when they last said so. The
# Release Safety worker heartbeats into this while it is running; the entry goes
# stale on its own once it stops, so the capability disappears when their
# platform goes away rather than lingering as an advertisement for something
# nobody is serving. This is the shared platform's service registry, and it is
# what makes "Release Safety shipped canary" a thing that happens by starting
# their container rather than by editing a config flag.
_canary_provider: dict | None = None

# How long a gate run takes. Long enough to watch the card work and to trip the
# gateway's sync budget honestly, short enough to stay well inside the invoke
# Activity's request timeout.
QUALITY_GATE_SECONDS = float(os.getenv("QUALITY_GATE_SECONDS", "12"))
# Demo knob: gates fail for these versions, so the fail-closed path can be shown
# on demand. Empty by default, so gates pass.
QUALITY_GATE_FAIL_VERSIONS = {
    item.strip()
    for item in os.getenv("QUALITY_GATE_FAIL_VERSIONS", "").split(",")
    if item.strip()
}

# Idempotency store keyed by the Idempotency-Key header. A replayed activity
# attempt with the same key returns the original result and does not apply the
# effect a second time.
_seen: dict[str, dict] = {}

# Demo knob: cutting this release runs long, which trips the gateway's
# monitor-and-convert budget and demonstrates the sync-to-async conversion. The
# delay stays under the invoke Activity's request timeout.
SLOW_CUT_VERSION = os.getenv("SLOW_CUT_VERSION", "2.3.1")
SLOW_CUT_DELAY_SECONDS = float(os.getenv("SLOW_CUT_DELAY_SECONDS", "8"))


def _gate_checks(version: str, passing: bool) -> list[dict]:
    """The individual checks shown on the gate card and the approval request.

    Fixed content: this is a demo backend, and the point is that the approver has
    concrete evidence in front of them, not that the numbers are real.
    """
    if passing:
        return [
            {"name": "functional tests", "detail": "412 passed", "ok": True},
            {"name": "user acceptance tests", "detail": "38 scenarios passed", "ok": True},
            {"name": "performance tests", "detail": "p99 180ms, within budget", "ok": True},
            {"name": "security scan", "detail": "no new findings", "ok": True},
        ]
    return [
        {"name": "functional tests", "detail": "412 passed", "ok": True},
        {"name": "user acceptance tests", "detail": "3 scenarios failed", "ok": False},
        {"name": "performance tests", "detail": "p99 940ms, over budget", "ok": False},
        {"name": "security scan", "detail": "no new findings", "ok": True},
    ]


async def _maybe_delay(tool_name: str, arguments: dict) -> None:
    if (
        tool_name == "cut_release"
        and str(arguments.get("version", "")) == SLOW_CUT_VERSION
    ):
        await asyncio.sleep(SLOW_CUT_DELAY_SECONDS)
    if tool_name == "run_quality_gates":
        # Publish the running state before sleeping, so the fleet panel can show
        # the gate card appear and work while the pipeline waits on it. Without
        # this the card would only exist after the run finished, and the audience
        # would lose the causality between the gate and the promotion that waited
        # on it.
        global _quality_gates
        _quality_gates = {
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "environment": str(arguments.get("environment", "staging")),
            "status": "running",
            "passed": None,
            "checks": [],
            "started_at": time.time(),
            "completed_at": None,
            "duration_seconds": None,
        }
        await asyncio.sleep(QUALITY_GATE_SECONDS)


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
        # Cutting a version that is already cut is a no-op that reports the
        # existing release, rather than an error or a second release. The
        # orchestrated CASE-2 path can legitimately reach this twice for the same
        # computed version (a replay-safe retry reruns Tool1's real work), and the
        # gateway's own idempotency key already suppresses a repeat of the same
        # attempt, so this covers the case where the two cuts are genuinely
        # separate calls. cut_at is deliberately left alone: a re-cut must not
        # reorder the release rail or make an old release look new.
        already_cut = (service, version) in _releases
        if not already_cut:
            _releases[(service, version)] = {
                "service": service,
                "version": version,
                "cut_at": time.time(),
                "seen_in": [],
            }
        return {
            "service": service,
            "version": version,
            "release": "cut",
            "already_cut": already_cut,
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

    if tool_name == "run_quality_gates":
        global _quality_gates
        service = str(arguments.get("service", ""))
        version = str(arguments.get("version", ""))
        env = str(arguments.get("environment", "staging"))
        passed = version not in QUALITY_GATE_FAIL_VERSIONS
        checks = _gate_checks(version, passed)
        started = (
            _quality_gates["started_at"]
            if _quality_gates and _quality_gates.get("status") == "running"
            else time.time()
        )
        now = time.time()
        _quality_gates = {
            "service": service,
            "version": version,
            "environment": env,
            "status": "passed" if passed else "failed",
            "passed": passed,
            "checks": checks,
            "started_at": started,
            "completed_at": now,
            "duration_seconds": round(now - started, 1),
        }
        failures = [check["name"] for check in checks if not check["ok"]]
        return {
            "service": service,
            "version": version,
            "environment": env,
            "passed": passed,
            "checks": checks,
            "duration_seconds": _quality_gates["duration_seconds"],
            "message": (
                f"quality gates passed for {version} in {env}"
                if passed
                else f"quality gates failed for {version} in {env}: "
                + ", ".join(failures)
            ),
        }

    if tool_name == "register_canary_capability":
        global _canary_provider
        _canary_provider = {
            "provider": str(arguments.get("provider", "release-safety")),
            "namespace": str(arguments.get("namespace", "release-safety")),
            "task_queue": str(arguments.get("task_queue", "release-safety-tq")),
            "endpoint": str(arguments.get("endpoint", "release-safety")),
            "ttl_seconds": float(arguments.get("ttl_seconds", 20.0)),
            "last_seen": time.time(),
        }
        return {
            "registered": True,
            "provider": _canary_provider["provider"],
            "message": (
                f"{_canary_provider['provider']} is offering canary analysis"
            ),
        }

    if tool_name == "get_release_safety_status":
        # The release pipeline's capability lookup: is anyone offering canary
        # analysis right now? Available means somebody is currently running a
        # canary platform, not that somebody once did -- a registration that has
        # not been refreshed inside its TTL is treated as gone, so stopping the
        # Release Safety worker takes the capability with it and the pipeline
        # goes back to opening the promotion itself.
        #
        # The answer is who, and nothing about how. How long a window runs, what
        # threshold it applies, and which versions it rejects are all decided
        # behind the canary team's Nexus endpoint, where the caller cannot reach
        # them and has no business trying.
        record = _canary_provider
        fresh = bool(
            record
            and (time.time() - record["last_seen"]) <= record["ttl_seconds"]
        )
        if not fresh:
            return {
                "available": False,
                "message": "no canary analysis provider is currently registered",
            }
        return {
            "available": True,
            "provider": record["provider"],
            "endpoint": record["endpoint"],
            "message": f"{record['provider']} is offering canary analysis",
        }

    if tool_name == "record_canary_state":
        # Visibility only. Release Safety publishes window progress here so the
        # approval dashboard can draw it next to the gate card; nothing about the
        # canary's own verdict depends on this call succeeding.
        global _canary
        _canary = {
            "canary_workflow_id": str(arguments.get("canary_workflow_id", "")),
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "environment": str(arguments.get("environment", "prod")),
            "phase": str(arguments.get("phase", "running_canary")),
            "ticks_completed": int(arguments.get("ticks_completed", 0)),
            "window_ticks": int(arguments.get("window_ticks", 4)),
            "threshold": float(arguments.get("threshold", 0.02)),
            "tick_results": list(arguments.get("tick_results") or []),
            "verdict": arguments.get("verdict"),
            "gateway_operation_id": arguments.get("gateway_operation_id"),
            "error": arguments.get("error"),
            "updated_at": time.time(),
        }
        return {"recorded": True, "phase": _canary["phase"]}

    if tool_name == "release_safety_canary_prepare":
        return {
            "checkpointed": True,
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "canary_workflow_id": str(arguments.get("canary_workflow_id", "")),
            "next_tool": "promote_release",
            "message": (
                "Release Safety's canary window closed green and is requesting "
                "the production promotion"
            ),
        }

    if tool_name == "release_safety_canary_resume":
        return {
            "resumed": True,
            "nested_result_received": arguments.get("nested_result") is not None,
            "message": (
                "Release Safety's canary run recorded the approved promotion"
            ),
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
            # Absent until the gates have run at least once. The dashboard keys the
            # gate card on presence, so "the team has not built this yet" and "the
            # team built it" are the same code path with different state.
            "quality_gates": dict(_quality_gates) if _quality_gates else None,
            # Absent until Release Safety has opened a canary window at least
            # once. Same "the card exists because the capability ran" contract as
            # the gate above, one checkpoint further along the pipeline.
            "canary": dict(_canary) if _canary else None,
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
