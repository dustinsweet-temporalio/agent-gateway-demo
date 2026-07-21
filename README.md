# Agent Gateway: durable suspend and resume for agentic tool calls

A runnable demo of CASE-1 from the "Suspend/Resume Primitive for Agentic Work"
requirements. An MCP server (the Agent Gateway) pauses protected tool calls for
human approval. The pause is held by a chain-scoped Temporal entity workflow, so
it survives process restarts, and it resumes exactly where it left off once an
approver decides.

The demo scenario is a deployment flow. You work in a code session (Claude Code in
any repo, the repo does not have to be this one) and drive a release through
environments. Reading what is deployed, cutting a release, and promoting to test
or staging all run immediately. Promoting to prod is the protected action: the
gateway pauses it and a human approves or rejects before it proceeds.

## Architecture

```
Claude Code ---MCP/HTTP---> Agent Gateway ---update-with-start---> Chain Workflow
                                 ^                                      |
                                 | approve / reject (Signal)            | invoke (Activity)
                            Approver UI                                 v
                                                                   Mock Deploy Backend
```

- One long-lived workflow per agentic chain, keyed by `workflow_id`. Operations
  live in a map inside workflow state.
- Chain correlation is automatic per session. Every tool call in one Claude Code
  session resolves to the same `workflow_id`, derived from the MCP session id, so
  neither the user nor the model has to thread an id between calls. An explicit
  `workflow_id` argument overrides the session derivation.
- A tool call arrives as an Update via update-with-start. It returns fast with a
  completed result (no approval) or a waiting payload (approval required).
- Approve and reject arrive as Signals that only mutate state. The workflow's main
  loop reacts to the state change and invokes the downstream tool.
- Status and result recovery are Queries, so handlers stay short and
  Continue-As-New can drain them.
- Policy evaluation and the downstream call are Activities. Policy is a function of
  the tool and its arguments: `promote_release` to prod requires approval.
- A chain rolls over with Continue-As-New to keep history bounded, and completes
  on its own after an idle window (24 hours) once it has no pending work.

Services in `docker-compose.yml`: `temporal` (dev server plus Web UI), `worker`,
`gateway`, and `mock-tool` (the pretend deployment backend).

## Tools

- `get_deployed_version(environment)` returns the version currently deployed in an
  environment (test, staging, prod). Read only, never gated.
- `cut_release(service, version)` registers a release candidate from a built
  artifact. Changes nothing running, never gated.
- `promote_release(service, version, environment, justification)` requests that an
  environment move to a release. Test and staging run immediately; prod pauses for
  approval. `justification` is optional and is shown to the approver.
- Lifecycle tools: `get_operation_status(workflow_id, operation_id)`,
  `get_operation_result(workflow_id, operation_id)`,
  `get_workflow_status(workflow_id)`.

The default service name in the demo is `delivery-matching-service`.

## Sync, async, and convert-to-async

Each tool is annotated in the gateway (`_TOOL_STRATEGY` in `gateway/server.py`) with
how the gateway should wait on it:

- Sync: block until the tool completes and return the real result.
- Async: return a poll handle immediately and let the tool run in the background.
- Convert (monitor-and-convert): block up to a sync budget in milliseconds; if the
  tool finishes in time return the real result, otherwise convert the call to async
  and return a poll handle while the workflow keeps running.

The tool runs as an Activity inside the workflow from the start, so the sync budget
is only a client-side wait in the gateway. Giving up on that wait does not cancel or
affect the run; it only stops the gateway from blocking, and the result is recovered
later by polling `get_operation_result`. In this demo `get_deployed_version` is sync,
and `cut_release` and `promote_release` use convert-to-async with a 5 second budget.
A prod promotion returns its waiting-for-approval response well within the budget, so
it still comes back synchronously.

## Run it

Requires Docker with Compose v2.

```
docker compose up --build
```

Wait for the worker to log that it is polling and the gateway to start. Then:

- Temporal Web UI: http://localhost:8233
- Approver dashboard: http://localhost:8080
- Gateway MCP endpoint: http://localhost:8080/mcp

## Register the gateway with Claude Code

Project scope (writes `.mcp.json`, one is already included here):

```
claude mcp add --transport http agent-gateway http://localhost:8080/mcp
```

To have the approval card show a verified requester, connect with a bearer token
that matches a principal in `GATEWAY_PRINCIPALS` (the compose file ships with
`tok_dustin`):

```
claude mcp add --transport http agent-gateway http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_dustin"
```

Confirm it is connected:

```
claude mcp list
```

## Chain correlation

One Claude Code session is one agentic chain. The gateway derives the chain
`workflow_id` from the MCP session id, so every tool call in that session lands on
the same workflow without the model passing anything along. The returned
`workflow_id` is stable across the session, which is what lets the demo below show
several calls on one chain from natural prompts. If you drive the gateway from a
different client, pass an explicit `workflow_id` to place calls on a chain of your
choosing.

## Verified requester

The approval card's requester comes from a validated credential, not from anything
the agent writes. The gateway reads the bearer token on each call and looks it up
in `GATEWAY_PRINCIPALS` (a static token to identity map). A matching token sets the
requester to that identity; a missing or unknown token, or no map configured, shows
`claude-code (unverified)`. This is the demo-scale stand-in for an identity provider
issued token: bearer auth proves the caller holds the token, and the gateway, not
the agent, decides who the requester is. It satisfies the requirement that
caller-supplied identity be authorized before use. Tell the agent not to put a
"requested by" line in the justification; the requester field is authoritative on
its own.

## Demo script

Drive these with natural prompts to Claude Code. The tool names below are what the
gateway exposes; the model maps to them.

1. Read current state. Ask what is deployed in staging and prod
   (`get_deployed_version`). You get canned versions, `status: completed`, and a
   `workflow_id`. Every later call in this session lands on that same chain.

2. Cut a release, and watch it convert to async. Ask it to cut release `2.3.1` of
   `delivery-matching-service` (`cut_release`). This build runs long. The gateway
   blocks for its sync budget (5 seconds), then converts the call to async: it
   returns `status: processing` with a `workflow_id`, an `operation_id`, and
   `poll_after_seconds`, while the cut keeps running in the workflow. Poll
   `get_operation_result` with those ids; it reads `processing` until the cut
   finishes (about 8 seconds), then flips to `completed`. Cuts of other versions
   are fast and return synchronously without converting.

3. Promote to staging (not gated). Ask it to promote `2.3.1` to staging
   (`promote_release`). Runs immediately and completes. Read staging again and it
   now shows `2.3.1`.

4. Promote to prod (gated). Ask it to promote `2.3.1` to prod, with a reason such
   as "ship the matching latency fix". The response is `status: waiting_for_approval`
   with an `operation_id` and `poll_after_seconds`. Prod has not changed.

5. Approve and resume. Open http://localhost:8080. The waiting row shows the
   requested action, requester, arguments, your justification, the risk reason, the
   submit time, and the deadline. Approve it, then poll `get_operation_result`. The
   status flips to `completed`, reading prod now shows `2.3.1`, and the decision
   appears under "Tool Call History" with the approver and time.

6. Reject. Promote a different version to prod, reject it in the dashboard with a
   reason, then poll. The status is `rejected` with your reason, prod is unchanged,
   and the rejection shows in the history table.

7. Timeout and expire. Set a short window and trigger a prod promotion:

   ```
   docker compose stop gateway
   APPROVAL_TIMEOUT_SECONDS=20 docker compose up -d gateway
   ```

   Promote to prod, wait past the window without deciding, then poll. The status is
   `expired` and prod was not changed.

8. Durability. Trigger a prod promotion so an operation is waiting. Restart the
   worker and the gateway:

   ```
   docker compose restart worker gateway
   ```

   The waiting operation is still there. Approve it and it resumes and completes.
   Nothing was lost, because the pause lives in Temporal, not in a process.

9. Idempotency. Promote the same version to the same environment twice in one
   session. The second call returns the same `operation_id` rather than opening a
   second approval gate. On any retried downstream attempt the mock backend reports
   `idempotent_replay: true` with a stable `executed_at`, so the environment is
   never promoted twice.

10. Continue-As-New. Fire many calls on one chain to grow history. When the server
    suggests it, the chain rolls over: the `workflow_id` stays the same and a new
    Run Id appears in the Web UI. Waiting operations and their deadlines carry
    across the rollover unchanged.

11. Idle completion (optional). With the default 24 hour idle window a chain stays
    open for the length of any demo. To show the automatic close, set
    `IDLE_TIMEOUT_SECONDS` in `workflows/chain.py` to a small value such as `60`,
    rebuild the worker, run one call, and let the chain sit with nothing pending.
    After the window it completes on its own as Completed (not Terminated). A new
    prompt in the same session then starts a fresh chain run under the same
    `workflow_id`.

## Approval payload

When a prod promotion pauses, the approver sees the full context the requirements
call for: the requested action as a sentence, the requester, an arguments summary,
the requester justification, the policy risk reason, the submit time, and the
approval window and deadline. On decision, the ledger and the "Tool Call History"
table record who approved or rejected, when, and the rejection reason.

## How this maps to Temporal best practices

- Determinism. All non-deterministic work and all I/O live in Activities. The
  workflow uses `workflow.now()` and `workflow.logger`, never wall clock or print.
  The idle window is a code constant rather than an environment read for the same
  reason: every Worker replaying the workflow must agree on it.
- Workflow Id as a business identifier. The chain id is the `workflow_id`, derived
  once per Claude Code session so all calls in a session share one chain. It acts
  as the running-workflow uniqueness constraint. An explicit `workflow_id`
  overrides the session derivation.
- Idempotency. Deduplication rides on a content-derived idempotency key carried in
  workflow state, not on Update IDs, because Update IDs are scoped to one Execution
  and reset after Continue-As-New.
- Signals mutate state only. Approve and reject handlers never invoke Activities;
  the main loop reacts to state changes and performs the invocation.
- Updates return values and may invoke Activities. The tool-call Update returns the
  completed result or the waiting payload, and it validates the request first so a
  malformed call leaves no trace in history.
- Queries are read only. Status, result, and ledger reads never mutate state.
- Explicit Activity timeouts. Every Activity sets a start-to-close timeout, and the
  downstream invocation sets a retry policy with a non-retryable class for client
  errors.
- Continue-As-New done safely. CAN is called only from the main method, after
  `workflow.all_handlers_finished` drains in-flight handlers, with state forwarded
  through the input dataclass. Approval deadlines and the idle clock are stored as
  absolute times in state rather than as Timers, because Timers do not carry across
  CAN.

## Honest caveats

- The mock deploy backend is a stub. The demo shows the gateway pausing and
  resuming a promotion request, not a real pipeline or GitOps sync. In production
  `promote_release` would call your CD system's promote or sync API.
- Result recovery works while the worker is up and the chain is not terminated. A
  Query needs a worker polling the task queue to serve it.
- A completed or idle-closed chain is still queryable within the namespace
  retention window. A terminated chain is not. This demo completes a chain through
  the idle timeout or the `close_chain` signal rather than terminating it, so it
  stays queryable.
- Chain lifecycle and reopen. A chain completes on its own after 24 hours with no
  pending work. It is never cut off mid-approval, because a waiting operation
  counts as pending work. After an idle close, a new prompt in the same session
  starts a fresh chain run under the same `workflow_id` via the default Allow
  Duplicate reuse policy. Prior operations remain in the closed run and are visible
  in the Web UI by Run Id, but the gateway query tools read the latest run.
- Caller-supplied IDs are trusted as-is. The requirements state that an explicit
  `workflow_id` must be authorized before use. This demo adopts it without that
  check; there is a marked seam in `gateway/server.py` where the authorization
  would go.
- Session correlation assumes one chain per MCP session on the default stateful
  transport. A stateless deployment, or concurrent sessions that must share a
  chain, would need explicit `workflow_id` propagation instead.
- The approver dashboard lists operations from Running chains. Decisions on a chain
  that has since completed are visible in the Temporal Web UI, not the dashboard.

## Configuration

Set via environment in `docker-compose.yml`.

- `TEMPORAL_ADDRESS` Temporal frontend address. Default `temporal:7233`.
- `TASK_QUEUE` Task queue name. Default `agentic-gateway`.
- `PROTECTED_ENVIRONMENTS` Comma separated environments that require approval for
  `promote_release`. Default `prod,production`.
- `APPROVAL_TIMEOUT_SECONDS` Approval window before an operation expires. Default
  `300`.
- `GATEWAY_PORT` Gateway HTTP port. Default `8080`.
- `GATEWAY_PRINCIPALS` JSON map of bearer token to requester identity, for
  example `{"tok_dustin":"Dustin Sweet <dustin.sweet@temporal.io>"}`. When unset,
  the requester is reported as `claude-code (unverified)`.
- `MOCK_TOOL_URL` Downstream backend endpoint used by the invoke Activity.

The idle timeout is not an environment variable. It is the `IDLE_TIMEOUT_SECONDS`
constant in `workflows/chain.py`, set to 24 hours, kept in code so every Worker
agrees on it. Lower it and rebuild the worker to demo the idle close.

## Production notes

- Run at least two workers for high availability. This demo runs one.
- The Temporal dev server is for development only. Use a self-hosted cluster or
  Temporal Cloud for production.
- Continue-As-New input is bounded by the 2 MB payload limit. This demo keeps every
  pending operation plus the most recent terminal operations and drops older
  terminal records. A production system offloads older records to external storage.

## Layout

```
common/models.py            shared dataclasses and enums
workflows/chain.py          AgenticChainWorkflow (entity workflow, CAN, idle close)
activities/gateway_activities.py  evaluate_policy (prod gate), invoke_tool
worker.py                   registers the workflow and activities
gateway/server.py           MCP HTTP server, deployment tools, approver UI
mock_tool/server.py         pretend deploy backend with idempotency and continuity
```
