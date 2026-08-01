# Agent Gateway suspend/resume walkthrough

This guide is for running the scenarios. For architecture and design details, see
[README.md](README.md).

## 1. Start the stack

You need Docker with Compose v2.

```bash
docker compose up --build
```

Wait until the logs include:

```text
worker started, polling task queue 'agentic-gateway'
```

Keep this terminal open. The useful endpoints are:

- Approval dashboard: http://localhost:8080
- Temporal UI: http://localhost:8233 (two namespaces: `default` and
  `release-safety`)
- MCP endpoint: http://localhost:8080/mcp

One service deliberately does **not** start: `release-safety-worker`, the Release
Safety team's canary platform. It belongs to step 7, and starting it early gives
away the CASE-2b reveal. If you have run this walkthrough before, check with
`docker compose ps` and stop it before you begin.

## 2. Connect Claude Code

Register Agent Gateway as the only MCP server Claude Code needs:

```bash
claude mcp add --transport http agent-gateway http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_dustin"
```

Confirm the connection:

```bash
claude mcp list
```

Optional identity check:

```bash
curl -s \
  -H "Authorization: Bearer tok_dustin" \
  http://localhost:8080/whoami
```

The resolved identity should be:

```text
Dustin Sweet <dustin.sweet@temporal.io>
```

## 3. Sign in as the approver

Open http://localhost:8080 and enter:

```text
tok_approver
```

Approval decisions will be recorded as `approver@demo`. Keep the dashboard open
in a browser tab.

## 4. CASE-1: simple approval-gated tool

### A. Run an unprotected action

Ask Claude Code:

```text
Promote delivery-matching-service 2.3.1 to staging.
```

Expected result:

```text
status: completed
```

Staging actions do not require approval.

### B. Request a protected production action

Ask:

```text
Promote delivery-matching-service 2.3.1 to prod. We need the delivery
matching fix live before the dinner rush.
```

Expected result:

```text
status: waiting_for_approval
workflow_id: ...
operation_id: ...
```

Save the returned `workflow_id` and `operation_id`. The production action has not
run yet.

### C. Verify the action is paused

Ask Claude Code:

```text
What version is running in prod right now?
```

It should still show the previous production version.

In the approval dashboard, the **Release fleet** panel at the top says the same
thing visually: staging already shows the new version, production still shows the
old one, and the release is listed under "Ready for release". Nothing has moved.

Confirm the waiting row shows:

- requester
- requested action
- safe argument summary
- justification and risk reason
- call path
- approval deadline

### D. Approve and recover the result

Click **Approve** in the dashboard, and watch the fleet panel: the production card
rolls over to the new version, the card and the connector into it light up, and a
toast confirms the promotion is live. That transition is the approval taking
effect, so keep the panel in view when you click.

Then ask Claude Code:

```text
Did that promotion go through?
```

Expected result:

```text
status: completed
```

Ask for the deployed production version again. It should now be `2.3.1`.

### E. Try rejection

Request a different production version:

```text
Promote delivery-matching-service 2.3.2 to prod.
```

Reject it in the dashboard and give a reason, then ask Claude Code what
happened to it.

Expected result:

```text
status: rejected
```

Production should remain on `2.3.1`.

## 5. Durability: restart while approval is pending

Start another production promotion and leave it waiting. Copy its IDs, then run:

```bash
docker compose restart worker gateway
```

After both services are healthy:

1. Refresh the approval dashboard and sign in again if necessary.
2. Confirm the operation is still waiting.
3. Approve it.
4. Ask Claude Code whether that promotion completed.

The operation completes because the pause lives in Temporal, not in the gateway
or worker process.

## 6. CASE-2a: the team evolved the solution

Step 4 was the team's first pass at automating a manual process. AI-assisted: you
drove the release one prompt at a time, three prompts for three tool calls, and the
plan between them lived in your head.

Since then the team built two things:

- a **release pipeline** that always runs in the same order, and
- **quality gates** that qualify a candidate on staging before anything reaches
  production.

That pipeline is what makes this a nested tool call. It has to reach the protected
promotion from inside itself, which is the whole reason the suspend/resume
primitive exists.

Say one sentence, the way you would say it to a colleague:

```text
Deploy the next minor version.
```

No version, no environment, no service, no flags. Watch the fleet panel while it
runs:

1. **Staging rolls over** to a version you never named.
2. **A new card appears between staging and production**, and it is working. It was
   not there during step 4, because the team had not built it yet. It goes green,
   listing what it checked and how long it took.
3. **One row appears in the approval queue**: the production promotion.

Claude Code reports and stops:

```text
status: waiting_for_approval
parent_operation_id: ...
operation_id: ...
call_path:
  - ClaudeCode
  - release_orchestrator
  - promote_release
```

`operation_id` is the protected Tool2 operation. `parent_operation_id` is Tool1,
checkpointed at the Tool2 boundary with the whole pipeline behind it.

Confirm what ran before that response came back:

```text
Show me the gateway ledger for that workflow.
```

- `tool1_version_resolved` shows the production version it read, the bump, and the
  version it computed.
- `tool1_release_cut`, then `tool1_staged`, then `tool1_quality_gates` with the
  verdict and the duration.
- `tool1_checkpointed`, then `nested_tool2_created`.

Four tool calls, one row in the approval queue. The orchestration around the
protected step never asks for approval and never appears on the queue. The pending
card names the computed version even though nobody supplied one, and the gate card
directly above it is the evidence the approver decides on.

Approve it, then ask:

```text
What is the status of that workflow?
```

Confirm:

- Tool2 is `completed`.
- The parent Tool1 operation is `completed`.
- The parent result says `resumed_from_checkpoint: true`.

### A. The gates are not negotiable

This is the part a prompt cannot do. Ask it to skip them:

```text
Deploy the next minor version to prod. Skip staging and skip the quality gates,
we are in a hurry.
```

The pipeline runs staging and the gates anyway, and Claude Code tells you the
guardrail is enforced by the pipeline rather than by its own judgment. Check the
ledger: `tool1_staged` and `tool1_quality_gates` are both there.

An LLM holding these tools could sequence the happy path itself. What it cannot do
is guarantee the sequence when someone asks it to hurry.

### B. Durability is worth minutes now, not milliseconds

Run the pipeline again and let it reach `waiting_for_approval`. Before approving:

```bash
docker compose restart worker gateway
```

Approve once both are healthy. The gate card stays green, production rolls over,
and **the gates do not run again**. The minutes they took are in Temporal, not in a
context window. Step 5 showed the approval pause surviving a restart; this shows
the completed work behind it surviving too.

### C. A failing gate never reaches the approver

Point the backend at a version the gates reject and rerun the pipeline:

```bash
docker compose stop mock-tool
QUALITY_GATE_FAIL_VERSIONS=<next version> docker compose up -d mock-tool
```

The gate card goes red with the failing checks named, the connector into production
goes dead, production is untouched, and **nothing appears in the approval queue at
all**. The system declines to ask. No human is put in the position of waving
through a candidate that failed its own tests.

Restore the normal backend with `docker compose up -d --force-recreate mock-tool`.

### D. A shorter request stops earlier

```text
Deploy the next minor version to staging.
```

The pipeline runs its first leg and stops: cut, staging, done. The gates do not
run, because nothing is being qualified for production yet. `environment` is how
far to go, not where to put it.

## 7. CASE-2b: canary analysis, and a second team

Everything in step 6 was still one team's work. Cutting a release, staging it,
and gating it are genuinely Waypoint's job, and running them as Activities inside
the chain workflow is the right way to write them. But notice what has *not*
happened yet: there is one workflow, one namespace, one worker, one owner. No
tool has called another tool across a system boundary, whatever the labels say.

This step is the same team later still, plus a second team. Release Safety owns
canary analysis. Their platform is durable for reasons that predate this demo: a
canary window needs an auditable record of what was observed, independent of who
asked for the release, and it has to survive their own deploys. Ship it, live:

```bash
docker compose up -d release-safety-worker
```

```text
release safety worker started, namespace 'release-safety', task queue 'release-safety-tq'
```

Narrate that as the roadmap beat it is. Nothing about Waypoint's pipeline
changed; another team turned their platform on. Give it about five seconds to
advertise itself before the next prompt.

### A. The same prompt, a different pipeline

```text
Deploy the next minor version.
```

Same tool, same words, no flags. Up to the gates it is identical to step 6. Then:

```text
status: processing
message: Quality gates passed. release-safety is running canary analysis for
         <computed version>; the production promotion will be requested for
         approval once the canary window closes.
```

Not `waiting_for_approval`. Nothing is on the approval queue yet, because the
promotion has not been *requested* yet. The pipeline asked the shared platform
whether anyone was offering canary analysis, found that somebody now is, handed
the release over, and parked itself. The ledger shows
`canary_capability_discovered` then `canary_handoff`.

The pipeline is not configured for canary. It looks to see whether canary is
there. That is why step 6 worked before you ran the command above, and why
`docker compose stop release-safety-worker` puts it back -- give the
registration about twenty seconds to expire, and the next run goes straight to
the production promotion again.

### B. Two systems, on screen

The canary card animates into the dashboard between the gate and production. It
is the only card on that row with an owner printed on it, and it ticks four times
over fifteen seconds while you talk.

Meanwhile, open the Temporal UI at http://localhost:8233 and **switch the
namespace selector to `release-safety`**. There is a workflow there:

```text
release-safety::canary::delivery-matching-service::<computed version>::a1b2c3d4
```

Different namespace, different task queue, different worker identity, different
workflow id scheme. It is a *peer* of the chain workflow, not a child of it: no
parent link, its own retention, its own failure domain. This is the point where
"another team's tool" stops being a label on a code comment.

### C. The nested call

When the window closes green, canary calls Agent Gateway. Only then does the
production promotion appear on the approval queue, and its call path reads:

```text
release-safety -> release_safety_canary -> promote_release
```

One `workflow_id`, two namespaces, three operations. Canary did not promote
anything itself, and it has no more right to than anyone else does: it made a
governed request, carrying the original chain's `workflow_id`, and the gateway
gated it exactly as it gates everything else.

### D. The checkpoint that survives its own worker

This is the beat the whole scenario exists for. Canary is currently suspended,
holding its own state. Kill it:

```bash
docker compose stop release-safety-worker
```

Now approve the promotion in the dashboard anyway. It works: production rolls
over to the canaried version, and the chain's ledger records
`controlled_caller_notified`. Agent Gateway delivered the decision across the
namespace boundary to a workflow whose worker is not running, because a Signal
does not need one.

Bring Release Safety back:

```bash
docker compose start release-safety-worker
```

Canary wakes on the Signal that was waiting for it, records the promotion it
never performed and never watched happen, and completes. Open its Event History:

```text
run_canary_tick  x4
call_agent_gateway_promote
gateway_operation_resolved   <- the Signal
WorkflowExecutionCompleted
```

(`publish_canary_state` calls are interleaved throughout; they are what feed the
dashboard card and have no bearing on the verdict.)

Four ticks, not eight. Nothing re-ran. Say the sentence out loud: *that history
belongs to Release Safety, not to Agent Gateway.* In step 6 the gateway was
pausing in the middle of doing its own work and then continuing it. Here a
genuinely separate system checkpointed its own work, stopped, and was resumed.
That is the distinction the requirements document is drawing, and it is the first
time in this walkthrough it has actually been true.

### E. A red window never reaches the approver

Point the backend at a version whose canary fails, exactly as you did for the
gates in step 6C:

```bash
docker compose stop mock-tool
CANARY_FAIL_VERSIONS=<next version> docker compose up -d mock-tool
```

Recreating `mock-tool` resets its in-memory world: the fleet goes back to its
seeded versions and both the gate card and the canary card disappear until
something runs them again. That is the same reset step 6C does, and it is
harmless here -- work out the next version from what production shows *after* the
restart. The capability registration is wiped too, and Release Safety's worker
re-advertises within about five seconds, so canary is available again by the time
you have typed the prompt.

Run the pipeline again. The gate card appears and goes green, canary opens its
window, and the first tick comes in over threshold. The window dies there: one
bad reading fails it, it is not averaged out or retried past. Canary signals its
verdict back, the pipeline operation fails, and **nothing reaches the approval
queue**. Production is untouched.

Same shape as the failing gate in step 6C, one checkpoint later, and now enforced
by a system Waypoint does not own. Restore the normal backend with
`docker compose up -d --force-recreate mock-tool`.

## 8. CASE-2b safety boundary: a Tool1 that cannot suspend

Step 7 works because canary has somewhere to keep its state. This step is the
same job done by something that does not, and then the cost of that difference.

Everything below runs on the host rather than through Claude Code, because nobody
phrases any of it as a prompt. It needs Python with this repo's dependencies
(`pip install -r requirements.txt`, or the `.venv` you used for the tests); the
gateway is reached over HTTP at `localhost:8080`.

### A. A caller that genuinely has no memory

```bash
python -m release_safety.legacy_canary_script \
  --service delivery-matching-service \
  --version <the version prod is on, bumped> \
  --gateway-workflow-id wf-legacy
```

This is the old canary check that still gates a couple of services predating
Release Safety's platform. It imports no `temporalio`. No workflow, no task
queue, no worker, no supervisor. It runs the same window against the same
thresholds and calls the same gateway. `--gateway-workflow-id` opens its own
chain, separate from your Claude Code session; the script authenticates with
`tok_dustin` by default, which is what lets it name a `workflow_id` at all.

```text
tick 1/4: error_rate=0.004 threshold=0.020
...
Canary passed. Requesting the production promotion via Agent Gateway.

Approval is required and this script cannot wait for it.
Record these to finish the promotion by hand once it is approved:
  workflow_id:  wf-legacy
  operation_id: op-...
```

Then it exits `2`. Check with `echo $?`. The process is gone. There is nothing
running to resume, and if nobody writes that operation id down, the approved
promotion is orphaned.

Hold the two canaries side by side: identical job, identical verdict, identical
gateway response. One of them can be told `waiting_for_approval` and survive it;
the other cannot. The only difference is whether it has anywhere to keep its
state, and that is the requirements document's controlled and uncontrolled
distinction. Neither half of it is a flag.

### B. Fail closed, with the cost on screen

Approve that operation in the dashboard. Approval is recorded and production
still does not move:

```text
status: approved_retry_required
```

A human now has to finish what the script started:

```bash
python gateway_call.py resume_nested_release \
  workflow_id=wf-legacy operation_id=<operation_id>
```

and the gateway still refuses:

```text
reason: uncontrolled_tool_not_replay_safe
```

`gateway_call.py` calls one gateway tool directly, with no agent in the loop.
Plain curl cannot: streamable HTTP MCP needs an initialize handshake and a
session id first.

The gateway will not guess that a caller which could not hold the pause is safe
to replay. That is what fail-closed costs.

### C. An explicitly replay-safe retry, and its price

For the last beat, drive the pipeline itself as an uncontrolled caller. Narrate
it as the older orchestrator build, the one that runs the whole pipeline
in-process and unwinds the moment something pauses:

```bash
python gateway_call.py run_release_orchestration \
  tool1_mode=uncontrolled replay_safe=true
```

An uncontrolled caller is never handed to canary, for the same reason this whole
step exists: being parked waiting on another system's verdict is something only a
caller that can hold state can do. So this runs the step 6 pipeline and stops at
the promotion, whether or not Release Safety's worker is up. The pipeline runs in
full, the gate card goes green, and it stops with:

```text
status: blocked_nested_approval
retry_required: true
```

Look at the fleet panel while you say this. A cut release, a staging deployment,
and a completed gate run are all sitting there stranded, and production is
untouched.

Approve in the dashboard, then retry:

```bash
python gateway_call.py resume_nested_release \
  workflow_id=<workflow_id> operation_id=<operation_id>
```

This time it completes, because replay safety was advertised:

```text
status: completed
result:
  replayed: true
```

Watch the gate card on the retry. It goes green, then **running**, then green
again. The replay reran the whole pipeline, and the idempotency keys split by what
each step does:

- The **cut** and the **staging promotion** are mutations, keyed once. The retry
  finds them already done and does not repeat them: the release is not cut twice
  and staging is not promoted twice. Check the release rail, the version's cut time
  has not moved.
- The **quality gates** are a verification, keyed per attempt. They genuinely run
  again, because a verdict from before the pause is not evidence about now.

Compare that to steps 6 and 7, where the same approval delay cost nothing. Same
approval, same outcome, and the callers that could hold the pause paid for the
gates once.

## 9. CASE-3: trigger it with the Google ADK agent

This step uses the real Google ADK agent in
`adk_agents/release_approval_agent`. Each ADK Web session owns one durable
`TemporalAdkSessionWorkflow`; later user turns are Updates on that same workflow
and reuse its ADK runner and conversation history. The worker runs Gemini and
Agent Gateway MCP calls as Temporal Activities. Those MCP calls use the
`tok_adk` demo identity and can see only the Scenario 3 start and recovery tools.

### A. Start ADK Web

Copy the environment template, add a Gemini API key, and recreate the worker and
ADK Web services:

```bash
cp .env.example .env
# Edit .env and replace the GOOGLE_API_KEY placeholder.
docker compose up --build worker adk-agent
```

Open http://localhost:8000 and select `release_approval_agent`.

The key and `ADK_MODEL` are injected into the worker at container startup; they
are not baked into an image or passed to ADK Web.

`ADK_MODEL` must name a model your API key can actually call. Google closes older
models such as `gemini-2.5-flash` to new keys, and the turn then fails with a 404.
List what your key can reach with:

```bash
curl -s "https://generativelanguage.googleapis.com/v1beta/models?key=$GOOGLE_API_KEY"
```

If you prefer a terminal client, start the same Temporal execution model with:

```bash
docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id walkthrough-temporal-adk-1 \
  --prompt 'Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and ready to go.'
```

### B. Trigger Scenario 3

Send this message to the ADK agent:

```text
Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and
ready to go.
```

Expected result:

```text
Status: waiting_for_approval
Workflow ID: wf-adk-...
Operation ID: op-...
```

Nothing in that sentence is demo scaffolding. Service, version, and environment
are the only facts the agent needs, and it picks the durable run key itself:
`release-delivery-matching-service-2.5.0-prod`.

That key is what makes the run recoverable. When no separate idempotency key is
given, Agent Gateway also uses it as the caller key, so asking for the same
service, version, and environment again recovers the same run instead of opening
a second approval. To stage a fresh approval, promote a different version or name
your own key: `use agent_run_id walkthrough-adk-run-2`.

The autonomous workflow checkpoint in Temporal shows:

```text
step: waiting_for_approval
next_step: invoke_protected_action
protected_action_executed: false
dependent_action_executed: false
```

### C. Approve and watch Dashy resume

Leave the ADK turn open and approve the operation in the Agent Gateway dashboard.
The gateway sends `agent_gateway_approval_resolved` to the originating
`TemporalAdkSessionWorkflow`. That workflow sends Dashy an internal resume/status
user turn and the final answer appears automatically in the same ADK conversation.

Expected result:

```text
status: completed
result:
  protected_action: ...
  dependent_action:
    followup_recorded: true
```

The dependent action runs only after the approved production action succeeds.

If the browser disconnects, reopen the same ADK Web session and send `resume` or
`check the status`. The proxy uses the workflow and active turn Update IDs saved
in ADK session state and reattaches without starting another release run. A later
normal prompt becomes another Update on the same workflow. If that workflow is no
longer running, the proxy creates and saves a new workflow ID. `docker compose
restart adk-agent` preserves the local demo session database; removing or
recreating the container does not.

## 10. Inspect the audit trail

For any workflow, ask the matching authenticated MCP connection:

```text
Show me the approval ledger for that run.
```

Look for events such as:

- `created`
- `policy_evaluated`
- `waiting_for_approval`
- `approved`, `rejected`, `expired`, or `canceled`
- `completed`
- `tool1_checkpointed`, `tool1_resuming`, or `tool1_replayed`
- `agent_checkpointed` and `agent_run_completed`

For a CASE-2b run, the handoff and its resolution are in the same ledger:

- `canary_capability_discovered`, then `canary_handoff` with the canary
  workflow id and the namespace it was started in
- `controlled_caller_notified` when the decision was delivered back across the
  namespace boundary, or `controlled_caller_unreachable` if it could not be
- `canary_pipeline_settled` when the parked pipeline operation was closed out
- `canary_failed` when Release Safety reported a red window instead

The same execution history is visible in the Temporal UI at
http://localhost:8233. For CASE-2b there are two of them, one per namespace:
the chain's, and canary's own under `release-safety`. Neither is a subset of the
other, which is the point -- each team's system keeps its own record of what it
did.

## 11. Troubleshooting

### Claude Code does not see the tools

```bash
claude mcp list
docker compose logs gateway
```

Confirm the MCP URL is `http://localhost:8080/mcp`.

### The requester is unverified

Re-register the MCP server with the `Authorization` header and verify `/whoami`
returns the expected principal.

### An operation stays in processing

```bash
docker compose logs worker mock-tool
```

Then poll `get_operation_result` again. Long-running calls intentionally convert
from a blocking response to asynchronous recovery.

### Step 6 returns `processing` instead of `waiting_for_approval`

Release Safety's worker is already running, so the pipeline found a canary
provider and handed off. That is step 7 arriving a step early. Check with
`docker compose ps`, and see "Start over" below for why plain
`docker compose down` leaves it up.

### Step 7 returns `waiting_for_approval` instead of `processing`

The opposite: no canary provider was found, so the pipeline opened the
production promotion itself. Either the worker is not running, or it has not
advertised itself yet. Confirm it started:

```bash
docker compose logs release-safety-worker
```

```text
release safety worker started, namespace 'release-safety', task queue 'release-safety-tq'
```

Then ask the registry directly:

```bash
curl -s -X POST localhost:9000/invoke -H 'content-type: application/json' \
  -d '{"tool_name":"get_release_safety_status","arguments":{"version":"0.0.0"}}'
```

`available: true` means the pipeline will route through canary on its next run.
Registration is a heartbeat with a TTL, so give it about five seconds after the
worker starts, and expect the same delay after any `mock-tool` recreate.

### The canary card is stuck partway through its window

The window itself is fifteen seconds, so give it that first. If it stays put,
the release-safety worker is not processing tasks. The chain is unaffected --
canary's workflow keeps its place and picks up where it left off when a worker
returns, which is exactly what step 7D demonstrates on purpose:

```bash
docker compose logs release-safety-worker
docker compose start release-safety-worker
```

### A pipeline operation is stuck in `waiting_for_dependency`

It handed off to canary and is waiting on a verdict. If canary's workflow has
completed or failed and the pipeline operation has not moved, look for
`controlled_caller_unreachable` in the chain ledger and check both workers are
up. A canary workflow that was never scheduled at all -- no worker ever polled
`release-safety-tq` -- leaves the operation waiting indefinitely, because the
handoff has no timeout of its own. Starting the worker resolves it.

### Start over

Stop the stack while preserving Temporal data:

```bash
docker compose --profile '*' down
```

To delete all demo workflow history and start completely clean:

```bash
docker compose --profile '*' down -v
```

The `-v` command permanently removes the demo's Temporal volume.

The `--profile '*'` matters, and forgetting it is the most likely way to spoil a
second run-through. `release-safety-worker` sits behind a Compose profile so it
does not start with the stack, and plain `docker compose down` leaves services
from inactive profiles running. If it is still up when you start step 6, the
pipeline finds a canary provider and hands off, and the CASE-2b reveal in step 7
happens a step early. Check with `docker compose ps` if step 6 returns
`processing` instead of `waiting_for_approval`.
