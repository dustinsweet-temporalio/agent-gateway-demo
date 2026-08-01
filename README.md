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
environments. Reading what is deployed, cutting a release, and promoting to
staging all run immediately. Promoting to prod is the protected action: the
gateway pauses it and a human approves or rejects before it proceeds.

## Architecture

```
namespace: default                                   namespace: release-safety
================================================     ==========================

Claude Code ----------------MCP/HTTP--------> Agent Gateway
                                                       |
Google ADK Web -> TemporalAdkSessionWorkflow -> MCP Activity
                         |                             |
                         +-- model Activity            |
                                                       |
                    +----------------------------------+--------+
                    |                                           |
          Chain entity workflow                   Autonomous-agent workflow
       (CASE-1, CASE-2a, CASE-2b)                  (CASE-3 checkpoint)
                    |          ^                            |
                    |          |                            |
     start_canary_analysis     | request_nested_tool_call    |
                    |          | (Tool1 -> Tool2)            |
                    v          |                            |
              +---------------------------+                 |
              |  CanaryAnalysisWorkflow    | <-- Release Safety's worker,
              |  task queue:               |     their namespace, their
              |  release-safety-tq         |     deploy cadence
              +---------------------------+                 |
                    ^          |                            |
   gateway_operation_resolved  | canary_verdict_reported     |
        (wakes the pause)      v                            |
                    +----------+---------------------------+
                    |
                    +---------- Activities -> Mock Deploy Backend
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
- Canary analysis (CASE-2b) is a second, independently owned system, not a
  module. `CanaryAnalysisWorkflow` runs in the `release-safety` namespace, on the
  `release-safety-tq` task queue, in a worker the gateway team does not deploy,
  from a Python package that imports nothing from `gateway/`, `workflows/`,
  `activities/`, or `common/`. It is deliberately **not** a child workflow: the
  two are peers correlated by a shared `workflow_id` value, because a child would
  share the parent's namespace and lifecycle and so would look separated without
  being separated.
- Canary is the first Tool1 in this demo that genuinely calls Tool2 *through* the
  gateway. Once its window closes green it invokes `request_nested_tool_call` on
  the chain workflow, carrying the original `workflow_id`, so one user-visible
  task spans two namespaces. It then suspends its own execution on a
  `wait_condition` and is woken by a `gateway_operation_resolved` Signal after a
  human decides. Kill Release Safety's worker mid-pause and its ticks, verdict,
  and operation id all come back from its own Event History.
- Every cross-namespace call goes through an Activity that creates its own
  namespace-scoped `Client`. Workflow code never holds a client.
- The pipeline discovers canary rather than being configured for it. After the
  quality gates pass it asks the shared platform whether anyone is offering
  canary analysis; Release Safety's worker heartbeats that registration while it
  runs, and the entry expires on its own when it stops. With no provider, the
  pipeline opens the production promotion itself, exactly as it did before canary
  existed.
- Each ADK Web session maps to one running `TemporalAdkSessionWorkflow`. ADK Web
  submits later user turns as Updates to that same workflow, which owns one ADK
  runner and its conversation state. A new workflow ID is created only when no
  saved workflow is running. ADK Web does not execute Gemini or MCP calls
  directly; the worker runs both through Temporal Activities with a Scenario
  3-only tool allowlist and the `release-agent@google-adk` identity.
- The Temporal ADK tool activity attaches its workflow, run, and ADK session IDs
  to MCP calls. Agent Gateway persists that callback with the protected run and
  sends a durable `agent_gateway_approval_resolved` signal after the operation
  completes, rejects, expires, cancels, or fails. Dashy sends an internal resume
  turn to the existing ADK session and reports the terminal result automatically.
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
| CASE-2a: pipeline and gates | `run_release_orchestration` (the pipeline) or `run_nested_release` (explicit version) | Fixed release pipeline, quality gates, full call path, child operation, controlled checkpoint/resume, uncontrolled fail-closed/retry |
| CASE-2b: canary as a separate Tool1 | same pipeline, with Release Safety's worker running | Cross-namespace handoff, a real nested Tool1 -> Tool2 call back into the same `workflow_id`, Tool1 suspending its own execution, Signal-driven wake, fail-closed on a red window |
| CASE-2b: uncontrolled Tool1 | `release_safety/legacy_canary_script.py` | A stateless process that cannot hold the pause: prints the operation id, exits non-zero, and requires a human `resume_nested_release` |
| CASE-3: autonomous agent | `start_google_adk_release_run` | Agent-run correlation, plan checkpoint, gateway-owned decision, protected action then dependent action |

Services in `docker-compose.yml`: `temporal` (dev server plus Web UI, with both
the `default` and `release-safety` namespaces), `worker`, `gateway`, `mock-tool`
(the pretend deployment backend), `adk-agent` (Google ADK Web for CASE-3), and
`release-safety-worker` (Release Safety's canary platform).

Tear the whole thing down with:

```
docker compose --profile '*' down -v
```

The `--profile '*'` matters. `release-safety-worker` sits behind a Compose
profile, and plain `docker compose down` leaves services from inactive profiles
running, which would carry the canary capability into your next run-through and
give away the CASE-2b reveal before you get to it.

`release-safety-worker` sits behind a Compose profile and does **not** start with
`docker compose up`. That is the CASE-2b beat: for CASE-1 and CASE-2a the
capability does not exist, and you ship it live, mid-demo, with

```
docker compose up -d release-safety-worker
```

after which the next pipeline run routes through canary and the card animates
into the dashboard. `docker compose stop release-safety-worker` takes it away
again.

## Tools

- `get_deployed_version(environment)` returns the version currently deployed in an
  environment (staging or prod). Read only, never gated.
- `cut_release(service, version)` registers a release candidate from a built
  artifact. Changes nothing running, never gated.
- `promote_release(service, version, environment, justification)` requests that an
  environment move to a release. Test and staging run immediately; prod pauses for
  approval. `justification` is optional and is shown to the approver.
- `run_nested_release(service, version, environment, tool1_mode, replay_safe)`
  runs the CASE-2 chain for a named version. `tool1_mode=controlled` resumes
  automatically. `tool1_mode=uncontrolled` returns `blocked_nested_approval`.
- `run_release_orchestration(bump, environment, service, tool1_mode, replay_safe)`
  runs the team's release pipeline from one instruction. Every argument is
  optional: `bump` defaults to `minor`, `service` to the demo service, and
  `environment` means how far to go rather than where to put it, defaulting to the
  full pipeline. Tool1 reads the version deployed in production, computes the next
  one, cuts it, promotes it to staging, runs quality gates there, and only then
  reaches the production promotion. If the gates fail the pipeline stops and no
  approval is ever requested. Same nested-approval mechanics, flags, and statuses
  as `run_nested_release`.
- `run_quality_gates(service, version, environment)` qualifies a candidate on
  staging. Deliberately **not** exposed as an MCP tool: the pipeline calls it
  internally, so an agent cannot run the gates itself and then promote around
  them. Its last result is what the dashboard's gate card renders.
- Canary analysis has no MCP tool at all, and could not have one: it is not
  Agent Gateway's capability to expose. It is started by the pipeline in another
  team's namespace, and the only thing that surfaces on the MCP side is the
  `promote_release` request canary itself makes once its window closes green.
  The same reasoning as the gates, one step further: an agent cannot run the
  canary and then promote around it, because an agent cannot run the canary.
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

The ADK Web agent at `adk_agents/release_approval_agent` is a thin proxy to one
long-lived `TemporalAdkSessionWorkflow` per ADK session. Every normal user turn
is a Temporal Update on that workflow, so the same ADK runner and conversation
history are reused. The worker uses Temporal's Google ADK plugin to run Gemini
model calls and streamable HTTP MCP calls as Activities. Its MCP toolset
authenticates as `release-agent@google-adk`, exposes only the Scenario 3 start and
lifecycle tools, and cannot see the generic `promote_release` entrypoint.

Copy the environment template and put your Gemini API key in the local `.env`
file:

```
cp .env.example .env
# Edit .env to replace GOOGLE_API_KEY and, optionally, change ADK_MODEL.
docker compose up --build worker adk-agent
```

Docker Compose injects the key into the worker when the container starts. It is
not baked into any image, and both `.env` and `.env.*` are excluded from the
Docker build context. `ADK_MODEL` defaults to `gemini-3.6-flash` in the template
and is passed to new ADK session workflows, so either value can be changed
without rebuilding the image; recreate the worker after changing `.env`.

Open http://localhost:8000, select `release_approval_agent`, and ask:

```
Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and
ready to go.
```

Service, version, and environment are the only facts the agent needs. It derives
the durable run key itself as `release-<service>-<version>-<environment>`, so
repeating the request recovers the same run rather than opening a second
approval. Override it by naming one: `use agent_run_id walkthrough-adk-run-2`.

`ADK_MODEL` must name a model the API key can call. Google closes older models
such as `gemini-2.5-flash` to new keys, and the session turn then fails with a
404; list the reachable models with
`curl -s "https://generativelanguage.googleapis.com/v1beta/models?key=$GOOGLE_API_KEY"`.

The agent returns the durable `workflow_id` and `operation_id` when approval is
needed. Leave the ADK turn open and decide it at http://localhost:8080. Agent
Gateway signals the originating ADK session, Dashy continues with an internal
resume prompt, and the final result appears in the same turn without manual
polling.

If the browser disconnects, reopen the same ADK Web session and send `resume` or
`check the status`. The session stores its Temporal workflow ID and active Update
ID, then reattaches instead of starting a duplicate turn. Subsequent normal
prompts are new Updates on the same workflow. If the saved workflow is no longer
running, the proxy starts a new session workflow and persists its new ID. A
normal container restart preserves the local demo session database; removing or
recreating the container does not. Use a durable ADK session service in
production.

### Use the command-line client

The command-line client starts or attaches to the same long-lived
`TemporalAdkSessionWorkflow` used by ADK Web, then submits its prompt as a
Temporal Update. It is an alternate client, not a second execution model:

With the base stack running, start a session from the worker container:

```
docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id walkthrough-temporal-adk-1 \
  --prompt 'Start an autonomous release run for delivery-matching-service 2.5.0 to prod, with agent_run_id walkthrough-temporal-adk-run-1. The rollout is validated and ready to go.'
```

The command waits while the workflow is durably paused. Approve the operation at
http://localhost:8080 with `tok_approver`. The gateway-owned approval workflow
executes the protected and dependent actions and then signals the original
`TemporalAdkSessionWorkflow`. Dashy resumes the same ADK session and the command
prints both the initial pause response and the callback-resumed final response.

The callback uses the fixed signal name
`agent_gateway_approval_resolved`. Callback headers are honored only for an
authenticated gateway principal, and arbitrary signal names are never accepted.

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
   finishes (about 12 seconds), then flips to `completed`. Cuts of other versions
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

### CASE-2 orchestrated: the team evolved the solution

CASE-1 is the engineering team's first pass at automating a manual process:
AI-assisted, one step at a time, three prompts for three tool calls, with the plan
between them living in the operator's head.

CASE-2 is the same team later. They built a release pipeline that always runs in
the same order, and quality gates that qualify a candidate on staging before
anything reaches production. That pipeline is what makes this a nested tool call:
it has to reach the protected promotion from inside itself.

1. Say `Deploy the next minor version.` No version, no environment, no service, no
   flags. Claude Code makes one call, to `run_release_orchestration`.
2. Inside Tool1 the workflow reads the production version, computes the next one,
   cuts it, promotes it to staging, and runs quality gates against staging. Every
   step is an Activity, so the ledger shows `tool1_version_resolved`,
   `tool1_release_cut`, `tool1_staged`, and `tool1_quality_gates`, and a Worker
   restart mid-flow resumes rather than starting over. No prompting happens
   between them.
3. Only once the candidate is green does the child `promote_release` operation
   appear. It is the only protected step and the only row on the approval queue:
   four tool calls, one decision. The approval card names the computed version,
   which the caller never supplied, and the gate card directly above it is the
   evidence the approver decides on.
4. Approve it. Tool1 resumes from its checkpoint, and the gates do **not** run
   again. That is the value of the pause being durable: the minutes the gates took
   are in Temporal, not in a context window.
5. Ask it to skip staging or the gates and it runs them anyway. A fixed flow can
   refuse; a prompt cannot.
6. If the gates fail, the pipeline stops, production is untouched, and nothing is
   ever put in front of an approver. The system declines to ask.
7. With `environment=staging` the pipeline runs its first leg and stops, skipping
   the gates because nothing is being qualified for production yet.
8. With `tool1_mode=uncontrolled` the fail-closed and replay-safe retry behavior is
   unchanged, but the cost is now visible. The replay finds the cut and the staging
   promotion already done, because mutations are keyed once, and reruns the quality
   gates, because a verdict from before the pause is not evidence about now.

The dashboard's quality gate card is keyed on whether gate state exists, so it is
absent for the whole of CASE-1 and appears the first time the pipeline reaches it.
Restarting `mock-tool` restores that "before" state for the next run-through.

### CASE-2b: canary analysis, owned by a different team

Everything above is still one team's work. Cutting a release, staging it, and
gating it are genuinely Waypoint's own job, and modelling those steps as ordinary
Activities inside the chain workflow is the honest way to write them. Nothing so
far is a nested call between two systems, whatever the labels say: there is one
workflow, one namespace, one worker, one owner.

CASE-2b is the same team later still, plus a second team. Release Safety owns
canary analysis. Their platform is durable for reasons that have nothing to do
with this demo -- a canary window needs an auditable record of what was observed,
independent of who asked for the release, and has to survive their own deploys.
So this is where a real Tool1 shows up, and where the requirements document's
checkpoint/resume contract finally has something to describe.

```
docker compose up -d release-safety-worker
```

1. Say `Deploy the next minor version.` again. Same tool, same prompt, no flags.
   The pipeline does exactly what it did in CASE-2a up to the gates.
2. After the gates pass, the pipeline asks the shared platform whether anyone is
   offering canary analysis. This run, somebody is. The ledger shows
   `canary_capability_discovered` and then `canary_handoff`, and the response is
   `processing` rather than `waiting_for_approval` -- nothing has been asked of an
   approver, because the promotion has not been requested yet.
3. The pipeline operation parks in `waiting_for_dependency` and a workflow appears
   **in the other namespace**, named
   `release-safety::canary::delivery-matching-service::2.4.0::a1b2c3d4`. Switch
   the Temporal UI's namespace selector to `release-safety` to watch it. It is a
   peer of the chain workflow, not a child of it.
4. The canary card animates into the dashboard between the gate and production,
   labelled with its owner, and ticks four times over fifteen seconds.
5. Only when the window closes green does canary call back into Agent Gateway,
   with the *original* `workflow_id`, and only then does the production promotion
   reach the approval queue. Its call path reads
   `release-safety -> release_safety_canary -> promote_release`: one chain, two
   systems.
6. **Kill Release Safety's worker while canary is suspended.**
   `docker compose stop release-safety-worker`. Approve the promotion anyway. The
   promotion runs, the fleet panel rolls production over, and the ledger records
   `controlled_caller_notified` -- the gateway delivered the decision across the
   namespace boundary to a workflow whose worker is not even running.
7. `docker compose start release-safety-worker`. Canary wakes on the Signal that
   was waiting for it, records the promotion it never performed and never saw
   happen, and completes. Its Event History shows four ticks, one nested gateway
   call, one Signal, and no tick re-runs. That history is the checkpoint the
   requirements document is asking for, and it belongs to Release Safety, not to
   Agent Gateway.
8. To show the fail-closed path, set `CANARY_FAIL_VERSIONS` on `mock-tool` and run
   the pipeline again. The window dies on its first bad reading, canary signals
   `canary_verdict_reported`, the pipeline operation fails, and production is
   never touched. Nobody is asked to approve anything: the same shape as a failed
   quality gate, one checkpoint later.

### CASE-2b: a Tool1 that genuinely cannot suspend

The contrast case is not a flag. It is a different program.
`release_safety/legacy_canary_script.py` is the old canary check that still gates
a couple of services that predate Release Safety's platform. It imports no
`temporalio`, has no workflow, no task queue, no worker, and no supervisor.

```
.venv/bin/python -m release_safety.legacy_canary_script \
    --service delivery-matching-service --version 2.5.0 \
    --gateway-workflow-id wf-legacy
```

It runs the same window, calls the same gateway, and is told the same
`waiting_for_approval`. Then it prints the operation id and exits `2`, because
there is nothing else it can do. The process is gone; if nobody writes the id
down, the approved promotion is orphaned. Finishing it needs a human running
`resume_nested_release` by hand, and Agent Gateway still refuses unless Tool1
advertised replay safety.

This is the point of the distinction, and why it could not have been shown
before: both sides are now real systems, and the difference between them is
whether one of them has any state to keep.

### CASE-3: autonomous run with the Google ADK agent

Use the ADK agent for this scenario rather than calling
`start_google_adk_release_run` from Claude Code. With the base stack still
running, start ADK Web and recreate the worker so it receives the `.env` values:

```
docker compose up --build worker adk-agent
```

1. Open http://localhost:8000 and select `release_approval_agent`. This agent
   starts one durable `TemporalAdkSessionWorkflow` for the ADK session. Later
   turns are Updates on that same workflow. The worker authenticates its MCP
   Activities with `tok_adk` and restricts them to the CASE-3 start and lifecycle
   tools.
2. Ask the agent:

   ```
   Start an autonomous release run for delivery-matching-service 2.5.0 to
   prod, with agent_run_id walkthrough-adk-run-1. The rollout is validated
   and ready to go.
   ```

   The first response is `waiting_for_approval` and includes the durable
   `workflow_id` and `operation_id`. The ADK turn stays attached to the Temporal
   session. Reusing the same `agent_run_id` recovers this run instead of creating
   a duplicate.
3. Open http://localhost:8080, sign in with `tok_approver`, and approve the
   operation. Agent Gateway signals the ADK session, which sends Dashy an
   internal resume/status prompt. The completed result then appears in the same
   ADK turn and contains both the protected promotion and dependent autonomous
   follow-up.
4. To exercise a terminal path, repeat with a new `agent_run_id`, then reject,
   cancel, or let the request expire. Dashy reports that terminal state
   automatically and neither external action is reported as successful.

If the browser disconnects while waiting, reopen the same ADK session and send
`resume` or `check the status`; the web proxy reattaches to its saved Temporal
workflow. See
[WALKTHROUGH.md](WALKTHROUGH.md#9-case-3-trigger-it-with-the-google-adk-agent)
for the expanded flow.

## Approval payload

When a prod promotion pauses, the approver sees the full context the requirements
call for: the requested action as a sentence, the requester, an arguments summary,
the requester justification, the policy risk reason, the submit time, and the
approval window and deadline. On decision, the ledger and the "Tool Call History"
table record who approved or rejected, when, and the rejection reason.

## Release fleet panel

Above the approval queue, the dashboard shows the fleet the decision acts on: the
releases that are cut and ready under "Ready for release", then the staging card
directly above the production card, matching the promotion path. Production is
marked as the environment that requires approval. A release older than the oldest
version still running drops off the ready list, and a version running in both
environments carries a badge for each.

The queue answers "what needs a decision"; the fleet answers "what is running", so
it is cards rather than a table. It polls `GET /fleet` every two seconds and
updates in place, which is what lets an approval read as a transition: the version
rolls over, the card and the inbound connector light up, and a promotion into a
protected environment also raises a toast. The rest of the page still refreshes on
its five second reload, which pauses while a transition is playing.

Two cards on that path exist only once the capability behind them has run. The
quality gate card appears the first time the pipeline reaches its gates, and the
canary card appears the first time Release Safety opens a window, each animating
into place between staging and production. Neither is a display toggle: the panel
keys on whether the backend holds any state for them, so "the team has not built
this yet" and "the team built it" are the same code path with different state.
That is what lets one live run tell the roadmap in order. The canary card is the
only one that names an owner, because it is the only one that has a different
one. Restarting `mock-tool` restores the "before" picture for both.

`GET /fleet` is a read only projection of the same in-memory state the tools
mutate, proxied from the deployment backend (`MOCK_TOOL_STATE_URL`) and gated on
the same approver identity as the dashboard. It can never disagree with what
`get_deployed_version` returns. If the backend is unreachable, the panel keeps the
last known fleet on screen and marks itself stale rather than blanking.

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
- Cross-namespace calls from Activities, never from workflow code. Both
  directions of the CASE-2b boundary (`start_canary_analysis`,
  `signal_release_safety_workflow` on one side, `call_agent_gateway_promote` and
  `report_canary_verdict` on the other) create their own namespace-scoped
  `Client` inside an Activity, so workflow code stays deterministic.
- Peers, not parent and child. `CanaryAnalysisWorkflow` is started with
  `start_workflow`, by type name, in another namespace, with no parent/child link
  to the chain. A child workflow would have been easier and would have quietly
  reintroduced shared ownership and a shared lifecycle.
- Signal handlers still only mutate state, on both sides. The chain's
  `canary_verdict_reported` handler and canary's `gateway_operation_resolved`
  handler each record and return; the Activities that follow are driven from the
  main loop, off durable operation state rather than an in-memory queue, so a
  Worker restart mid-notification picks the work back up.
- Contracts across a team boundary are duplicated on purpose. Each side declares
  its own copy of the payloads they exchange rather than sharing an internal
  package, and `tests/test_release_safety_contract.py` is what stops them
  drifting.

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
  agent deployment. It contains no Gemini key or gateway bearer token; those
  runtime secrets belong to the worker. A production deployment should inject
  them from its platform secret store and replace ADK Web's local SQLite session
  database with a supported durable session service.
- Canary is scripted, not sampled. `run_canary_tick` returns a fixed per-tick
  error-rate sequence so a demo run is repeatable. In a real Release Safety
  platform that Activity queries a metrics store; nothing else about the workflow
  changes.
- Canary acts on the requester's behalf. The nested `promote_release` call
  carries the chain owner as `caller_principal`, because the gateway checks a
  nested call against the chain's owner, with canary's own identity alongside it
  in `caller_service` and the call path. A production system would model this as
  explicit delegation -- canary holding its own principal and the gateway
  authorizing it to act for the requester -- rather than relaying the identity.
- A parked pipeline operation has no deadline of its own. It waits on canary's
  verdict, and canary's approval request carries the normal approval window, but
  a canary workflow that is never scheduled (no worker ever starts on
  `release-safety-tq`) leaves the pipeline operation waiting. Production should
  give the handoff its own timeout.
- Two namespaces on one dev server is the right shape but not the full claim.
  Real independent ownership also means separate deployments, separate retention
  policy, and separate operators; this demo gets the boundary and the failure
  domain right and shares a cluster.
- The Temporal Google ADK integration is currently experimental. This demo uses
  a fixed signal and authenticated callback metadata; production should also
  issue a signed, single-use callback registration token and authorize the target
  workflow type before signaling it.

## Configuration

Set via environment in `docker-compose.yml`.

- `TEMPORAL_ADDRESS` Temporal frontend address. Default `temporal:7233`.
- `TASK_QUEUE` Task queue name. Default `agentic-gateway`.
- `PROTECTED_ENVIRONMENTS` Comma separated environments that require approval for
  `promote_release`. Default `prod,production`.
- `APPROVAL_TIMEOUT_SECONDS` Approval window before an operation expires. Default
  `300`.
- `GATEWAY_PORT` Gateway HTTP port. Default `8080`.
- `GATEWAY_MCP_ALLOWED_HOSTS` comma-separated HTTP `Host` values accepted by
  MCP DNS-rebinding protection. Defaults include local clients and the Compose
  service hostname `gateway:*`.
- `GATEWAY_MCP_ALLOWED_ORIGINS` comma-separated browser origins accepted by MCP
  DNS-rebinding protection. Defaults include local browser origins.
- `GATEWAY_PRINCIPALS` JSON map of bearer token to requester identity, for
  example `{"tok_dustin":"Dustin Sweet <dustin.sweet@temporal.io>"}`. When unset,
  the requester is reported as `claude-code (unverified)`.
- `GATEWAY_APPROVERS` comma-separated principal identities authorized to review
  and decide operations. The compose demo authorizes `tok_approver`
  (`approver@demo`), `tok_dustin`, and `tok_abe`.
- `MOCK_TOOL_URL` Downstream backend endpoint used by the invoke Activity.
- `AGENT_GATEWAY_MCP_URL` streamable HTTP MCP endpoint used by the worker's
  Temporal MCP Activities. Compose sets it to `http://gateway:8080/mcp`.
- `AGENT_GATEWAY_TOKEN` bearer identity used by the worker's MCP Activities. The
  local demo defaults to `tok_adk`; production should inject a real secret.
- `ADK_MODEL` model used by the ADK agent. Default `gemini-3.6-flash`.
- `GOOGLE_API_KEY` Gemini API credential injected into the worker at runtime. It
  is deliberately absent from the Docker image and ADK Web container.
- `RELEASE_SAFETY_NAMESPACE` / `RELEASE_SAFETY_TASK_QUEUE` where the gateway
  worker reaches Release Safety's canary platform. Defaults `release-safety` and
  `release-safety-tq`.
- `TEMPORAL_NAMESPACE` namespace the Release Safety worker serves. Default
  `release-safety`.
- `AGENT_GATEWAY_NAMESPACE` namespace the canary Activities call back into.
  Default `default`.
- `RELEASE_SAFETY_METRICS_URL` shared observability backend the canary reports
  window progress and its capability heartbeat to. Default
  `http://mock-tool:9000/invoke`.
- `RELEASE_SAFETY_HEARTBEAT_SECONDS` how often the Release Safety worker
  re-advertises canary analysis. Default `5`; the registration's TTL is four
  times this, so stopping the worker withdraws the capability within ~20s.
- `CANARY_FAIL_VERSIONS` comma separated versions whose canary window fails, so
  the fail-closed path can be shown on demand. Empty by default.

The idle timeout is not an environment variable. It is the `IDLE_TIMEOUT_SECONDS`
constant in `workflows/chain.py`, set to 24 hours, kept in code so every Worker
agrees on it. Lower it and rebuild the worker to demo the idle close.

The canary window is not an environment variable either. It is
`CANARY_TICK_SECONDS` and `CANARY_WINDOW_TICKS` in `release_safety/models.py`,
four ticks five seconds apart, because how long a canary window runs is the
Release Safety team's decision and not a caller's. Their worker publishes the
shape it is using in its capability registration and the pipeline relays it back
when it starts a window, so the two can never disagree.

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
common/semver.py            version bump arithmetic for the release pipeline
gateway_call.py             call one gateway tool directly, for operator actions
workflows/chain.py          AgenticChainWorkflow (entity workflow, CAN, idle close)
workflows/autonomous_agent.py  CASE-3 durable autonomous-agent checkpoint
workflows/adk_session.py     long-lived ADK session, turn Updates, callback receiver
adk_agents/release_approval_agent/  Dashy agent, Temporal integration, Web proxy
adk_agents/run_temporal_session.py  CLI client for the same ADK session workflow
activities/gateway_activities.py  evaluate_policy, invoke_tool, and the two
                            Activities that reach into the release-safety namespace
worker.py                   registers the workflow and activities
gateway/server.py           MCP HTTP server, deployment tools, approver UI
mock_tool/server.py         pretend deploy backend with idempotency and continuity
tests/                      Temporal scenarios, gateway contract, and ADK agent tests

release_safety/             the Release Safety team's canary platform. A separate
                            package, a separate namespace, a separate worker, and
                            no imports from anything above this line.
  models.py                 their dataclasses, and their copy of the wire contract
  canary_workflow.py        CanaryAnalysisWorkflow: the window, the nested call,
                            and the suspension that outlives their worker
  canary_activities.py      the tick, the call into Agent Gateway, the verdict
  worker.py                 release-safety-tq worker and its capability heartbeat
  legacy_canary_script.py   the uncontrolled Tool1: a plain process, no temporalio
```
