from __future__ import annotations

import os
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

# In-memory idempotency store keyed by the Idempotency-Key header. A replayed
# activity attempt with the same key returns the original result with a stable
# executed_at, which shows that at-least-once delivery did not double-execute.
_seen: dict[str, dict] = {}


async def invoke(request: Request) -> JSONResponse:
    body = await request.json()
    key = request.headers.get("Idempotency-Key", "")

    if key and key in _seen:
        prior = dict(_seen[key])
        prior["idempotent_replay"] = True
        return JSONResponse(prior)

    result = {
        "tool_name": body.get("tool_name"),
        "arguments": body.get("arguments"),
        "executed_at": time.time(),
        "message": f"executed {body.get('tool_name')}",
        "idempotent_replay": False,
    }
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
