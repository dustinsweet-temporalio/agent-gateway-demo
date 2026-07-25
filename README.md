# Agent Gateway: durable suspend and resume for agentic tool calls

A runnable implementation of all three scenarios in the "Suspend/Resume Primitive
for Agentic Work" requirements. An MCP server (the Agent Gateway) pauses protected
tool calls for human approval. Temporal holds the chain, nested Tool1 checkpoint,
autonomous-agent checkpoint, approval deadline, ledger, and recovery handles, so
execution survives process restarts and resumes from durable state.

For a step-by-step, execution-first guide, use
[WALKTHROUGH.md](WALKTHROUGH.md).

The demo scenario is a deployment flow. You work in a code session (Claude Code in
any repo, the repo does not have to be this one) and drive a release through
environments. Reading what is deployed, cutting a release, and promoting to test
or staging all run immediately. Promoting to prod is the protected action: the
gateway pauses it and a human approves or rejects before it proceeds.

## Architecture

```
Claude Code / Google ADK agent ---MCP/HTTP---> Agent Gateway
                                                     |
                    +--------------------------------+------------------+
                    |                                                   |
          Chain entity workflow                           Autonomous-agent workflow
          (CASE-1 and CASE-2)                              (CASE-3 checkpoint)
                    |                                                   |
                    +---------- Activities -> Mock Deploy Backend ------+
                                                     ^
                                       approve/reject/cancel Signals
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
- Controlled nested Tool1 calls checkpoint before Tool2 and resume with Tool2's
  durable result. Uncontrolled Tool1 calls fail closed. An approved Tool2 is not
  executed until the caller explicitly retries, and replay is refused unless
  Tool1 advertised replay safety.
- The Google ADK agent connects to the gateway over streamable HTTP MCP with its
  own bearer identity and a Scenario 3-only tool allowlist. Its Temporal workflow
  checkpoints the current/next agent step while approval is pending and runs no
  dependent external side effect until the protected action has completed.
- Explicit caller-provided workflow IDs are accepted only from authenticated
  principals. Every workflow records its owner; lifecycle queries and retries
  must come from that owner.
- Approver identity is derived from an authenticated gateway token. The approve
  and reject forms do not accept a caller-supplied approver name.
- Every MCP tool publishes a concrete output schema as well as its input schema,
  including the shared status/recovery envelope.

## Requirements scenario mapping

| Scenario | Entry tool | Durable behavior |
| --- | --- | --- |
| CASE-1: simple tool | `promote_release` | Policy gate, wait payload, approve/reject/expire/cancel, invoke, poll result |
| CASE-2: nested Tool1 -> Tool2 | `run_nested_release` | Full call path, child operation, controlled checkpoint/resume, uncontrolled fail-closed/retry |
| CASE-3: autonomous agent | `start_google_adk_release_run` | Agent-run correlation, plan checkpoint, gateway-owned decision, protected action then dependent action |

Services in `docker-compose.yml`: `temporal` (dev server plus Web UI), `worker`,
`gateway`, and `mock-tool` (the pretend deployment backend). The optional `adk`
profile adds a real Google ADK Web service for CASE-3.

## Tools

- `get_deployed_version(environment)` returns the version currently deployed in an
  environment (test, staging, prod). Read only, never gated.
- `cut_release(service, version)` registers a release candidate from a built
  artifact. Changes nothing running, never gated.
- `promote_release(service, version, environment, justification)` requests that an
  environment move to a release. Test and staging run immediately; prod pauses for
  approval. `justification` is optional and is shown to the approver.
- `run_nested_release(..., tool1_mode, replay_safe)` runs the CASE-2 chain.
  `tool1_mode=controlled` resumes automatically. `tool1_mode=uncontrolled`
  returns `blocked_nested_approval`.
- `resume_nested_release(workflow_id, operation_id)` explicitly retries an
  approved uncontrolled nested call; it succeeds only when `replay_safe=true`.
- `start_google_adk_release_run(..., agent_run_id)` starts or recovers CASE-3.
- Lifecycle tools: `get_operation_status(workflow_id, operation_id)`,
  `get_operation_result(workflow_id, operation_id)`,
  `get_workflow_status(workflow_id)`, `get_workflow_ledger(workflow_id)`, and
  `cancel_operation(workflow_id, operation_id, reason)`.

All call tools accept an optional caller idempotency key. Reusing it under the same
authenticated principal and workflow returns the same operation rather than
opening a duplicate approval gate.

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

## Test it

Install `requirements-dev.txt` and run:

```
python3 -m pytest -q
```

The workflow tests start an ephemeral local Temporal dev server with the installed
Temporal CLI and exercise all three cases plus rejection, expiry, cancellation,
deduplication, and the uncontrolled-tool safety boundary.

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

## Run CASE-3 with Google ADK

The agent at `adk_agents/release_approval_agent` uses Google ADK 2.x
`McpToolset` to discover the dedicated Scenario 3 start and lifecycle tools. It
authenticates as `release-agent@google-adk`; it cannot see the generic
`promote_release` entrypoint.

Start it with a Gemini API key:

```
export GOOGLE_API_KEY="<your Gemini API key>"
docker compose --profile adk up --build adk-agent
```

Open http://localhost:8000, select `release_approval_agent`, and ask:

```
Start Scenario 3 for delivery-matching-service version 2.5.0 to prod.
Use agent_run_id walkthrough-adk-run-1.
```

The agent returns the durable `workflow_id` and `operation_id` when approval is
needed. Decide it at http://localhost:8080, then ask the agent for the operation
result using those IDs. See [WALKTHROUGH.md](WALKTHROUGH.md) for the exact flow.

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
   finishes (about 30 seconds), then flips to `completed`. Cuts of other versions
   are fast and return synchronously without converting.

3. Promote to staging (not gated). Ask it to promote `2.3.1` to staging
   (`promote_release`). Runs immediately and completes. Read staging again and it
   now shows `2.3.1`.

4. Promote to prod (gated). Ask it to promote `2.3.1` to prod, with a reason such
   as "ship the matching latency fix". The response is `status: waiting_for_approval`
   with an `operation_id` and `poll_after_seconds`. Prod has not changed.

5. Approve and resume. Open http://localhost:8080, sign in with the demo approver
   token `tok_approver`, and review the operation. The waiting row shows the
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

### CASE-2: nested downstream approval

1. Call `run_nested_release` for prod with `tool1_mode=controlled`. The response
   contains the child Tool2 `operation_id`, parent Tool1 operation, and call path
   `ClaudeCode -> release_orchestrator -> promote_release`.
2. Approve the child. Temporal invokes Tool2, supplies the durable result to the
   Tool1 checkpoint, and completes Tool1.
3. Repeat with `tool1_mode=uncontrolled`. The response is
   `blocked_nested_approval`; approving records `approved_retry_required` but does
   not invoke Tool2.
4. With `replay_safe=false`, `resume_nested_release` remains blocked. With
   `replay_safe=true`, the explicit resume invokes Tool2 idempotently and replays
   Tool1.

### CASE-3: autonomous Google ADK-style run

1. Call `start_google_adk_release_run` with bearer token `tok_adk`, a stable
   `agent_run_id`, and a prod target.
2. The response is `waiting_for_approval`. `get_workflow_status` shows checkpoint
   `waiting_for_approval`, the next planned step, and both external-side-effect
   flags as false.
3. Approve as `tok_approver`. The Temporal run invokes the protected action, then
   and only then records the dependent autonomous follow-up. Polling returns the
   combined result.
4. Rejecting, canceling, or allowing the gate to expire completes the run without
   either external action.

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
- Idempotency. Deduplication rides on a caller-provided or content-derived key
  carried in workflow state, not on Update IDs, because Update IDs are scoped to
  one Execution and reset after Continue-As-New.
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
- Autonomous checkpoint. CASE-3 stores the agent's current step, next step, and
  side-effect flags in workflow state. A worker or gateway restart replays that
  checkpoint; the caller connection is not the source of liveness.
- Nested fail-closed boundary. CASE-2 never treats an uncontrolled Tool1 stack as
  resumable. Approval and execution are separate states, and explicit replay is
  permitted only with an advertised replay-safe contract.

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
- Session correlation assumes one chain per MCP session on the default stateful
  transport. A stateless deployment, or concurrent sessions that must share a
  chain, would need explicit `workflow_id` propagation instead.
- The approver dashboard reads operations within Temporal retention. A production
  UI should page visibility results and export the ledger to the organization's
  compliance store.
- ADK Web is included only as a local development surface, not as a production
  agent deployment. A production deployment should inject the gateway URL and
  bearer credential from its platform secret store and persist ADK sessions in a
  supported session service.

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
- `GATEWAY_APPROVERS` comma-separated principal identities authorized to review
  and decide operations. The compose demo maps `tok_approver` to
  `approver@demo`.
- `MOCK_TOOL_URL` Downstream backend endpoint used by the invoke Activity.
- `AGENT_GATEWAY_MCP_URL` streamable HTTP MCP endpoint used by the ADK agent.
  The Compose `adk` profile sets it to `http://gateway:8080/mcp`.
- `AGENT_GATEWAY_TOKEN` bearer identity used by the ADK agent. The local demo
  defaults to `tok_adk`; production should inject a real secret.
- `ADK_MODEL` model used by the ADK agent. Default `gemini-2.5-flash`.
- `GOOGLE_API_KEY` Gemini API credential used by local ADK Web.

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
workflows/autonomous_agent.py  CASE-3 durable autonomous-agent checkpoint
adk_agents/release_approval_agent/  real Google ADK MCP client for CASE-3
activities/gateway_activities.py  evaluate_policy (prod gate), invoke_tool
worker.py                   registers the workflow and activities
gateway/server.py           MCP HTTP server, deployment tools, approver UI
mock_tool/server.py         pretend deploy backend with idempotency and continuity
tests/                      Temporal scenarios, gateway contract, and ADK agent tests
```
