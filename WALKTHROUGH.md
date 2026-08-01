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

The `nexus-endpoints` service runs once and exits, having registered the two
Nexus Endpoints the two teams call each other on. Its log ends with a listing of
both; if it is missing, step 6's Act Two will fail with an operation timeout.

Keep this terminal open. The useful endpoints are:

- Approval dashboard: http://localhost:8080
- Temporal UI: http://localhost:8233 (two namespaces: `default` and
  `security`)
- MCP endpoint: http://localhost:8080/mcp

One service deliberately does **not** start: `security-scan-worker`, the Security
team's pre-prod scanning platform. It belongs to step 6's Act Two, and starting it
early gives away the reveal -- Act One is the mandate landing *before* that team has
adopted Temporal. If you have run this walkthrough before, check with
`docker compose ps` and stop it before you begin.

If one is left running, nothing breaks: the mandate switch is off until step 6, and
with it off the pipeline has no scan step at all, so steps 4 and 5 run scan-free
either way.

If you have changed any code since you last ran this, build with the profile or
the security scan worker keeps a stale image:

```bash
docker compose --profile '*' build
```

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
Dustin Sweet <dustin.sweet@quickmeals.com>
```

## 3. Sign in as the approver

Open http://localhost:8080 and enter:

```text
tok_approver
```

Approval decisions will be recorded as `approver@demo`. Keep the dashboard open
in a browser tab.

## 4. CASE-1: simple approval-gated tool

Before you run anything, look at the **Release fleet** panel. Between staging and
production there is a quality gate card, and it is empty: dashed, muted, reading
"no candidate staged yet". That is the honest state at container startup, and it
is worth pointing at, because the card being *present* and empty is the claim.
Gates here are standing infrastructure that react to anything reaching staging,
not something a release pipeline invokes.

### A. Run an unprotected action

Ask Claude Code:

```text
Promote delivery-matching-service 2.3.1 to staging.
```

Expected result:

```text
status: completed
```

Staging actions do not require approval. Now watch the gate card, without touching
anything else: within a second or two it goes to `running`, counts its four checks
off as they finish concurrently, and lands on `passed` after roughly seven
seconds. One manual CASE-1 tool call, no pipeline anywhere, and the candidate that
just reached staging has been qualified.

Nothing waited on that. `promote_release` returned as soon as staging was actually
promoted; the gate ran alongside. That distinction matters for the rest of the
demo: this is ambient infrastructure reacting to a state change, closer to CI
kicking off a test run on a push than to an orchestrated pipeline step. The
CASE-2a release pipeline is the thing that *waits*.

While you are there, open the Temporal UI and look at what those two calls
produced. `promote_release` is a `PromoteReleaseChildWorkflow` with four
Activities in sequence -- `deploy_binaries`, `health_check_new_instances`,
`update_traffic_routing`, `update_release_notes` -- each with its own elapsed
time, and the gate is a separate `QualityGateChildWorkflow` whose id names the
service and version it is about. Not two flat tool calls.

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

The gate card above the queue is showing `passed` for the exact version in the
pending row, which is a beat worth taking: the approver is not deciding on a
sentence, they have the four checks in front of them. Nothing forces them to look
— CASE-1 blocks on nothing, and you can approve or reject this row whatever the
gate says — but the evidence is there because the promotion to staging produced
it.

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

## 6. CASE-2: the mandate, and the two ways a team meets it

Everything up to here was one team, one namespace, one worker, one owner. No tool
has called another tool across a system boundary yet, whatever the labels say.

This step is where that changes, and it is **two acts and nothing else**. A
company-wide mandate lands: no engineering team promotes its own service to
production on its own say-so any more. Every production release goes through the
Security team's scanning process, and Security -- not the owning team -- approves
and initiates it.

Act One is that mandate arriving before the Security team has adopted Temporal.
Act Two is the same mandate after they have. Each act ends in exactly one worker
kill, and each proves exactly one thing. Nothing else belongs in this step; see
[Deliberately not in this walkthrough](#deliberately-not-in-this-walkthrough) at
the end of the section.

### Prerequisite: a staged candidate that has passed the gates

Both acts act on a release that has already cleared the Waypoint team's own
pipeline and is sitting on staging. That is CASE-2a, which is built and demoed on
its own; do not re-run its mechanics here. If you need one, say
`Deploy the next minor version to staging.` to Claude Code and let the pipeline
cut it, put it on staging, and turn the gate card green. Note the version it
computed -- both acts below take it as `<staged version>`.

### Act One: the mandate lands, and Security is not yet on Temporal

> A company-wide mandate just came down: no team promotes to prod on their own
> anymore. Security has to clear it first. But Security hasn't adopted Temporal
> yet -- their scanning process today is a plain script.

**1. Enact the mandate.** On the approval dashboard at http://localhost:8080, flip
the small switch at the top right, under **Log out**. It turns green, and a line
appears under it:

```text
Production releases require Security Team approval.
```

Point at it. It stays there for the rest of the demo, so you can come back to it
without re-querying anything. Flipping it did two things: production promotions are
now Security's to approve, and the release pipeline now routes candidates through
the Security team's scan on the way to production -- which is the shape Act Two
runs in. Until this moment there was no scan step in the flow at all, which is why
everything before this step ran without one.

The fleet panel says the second half of that immediately: a **Security scan** card
animates in between the quality gates and production, owned by the Security team,
reading `idle` because nothing has been through it yet. The pipeline grew a
checkpoint the instant the rule landed, before any release has met it. Nothing has
run; the shape changed.

**2. Run the Security team's scanning process.** This is a plain Python script on
the host, not a prompt: nobody phrases a scanner run as a sentence to an agent. It
needs this repo's dependencies (`pip install -r requirements.txt`, or the `.venv`
you used for the tests).

```bash
.venv/bin/python -m security_scan.legacy_security_scan_script \
  --service delivery-matching-service \
  --version <staged version> \
  --gateway-workflow-id wf-legacy
```

It imports no `temporalio`. No workflow, no Event History, no task queue, no
worker, no supervisor. All four stages run and pass:

```text
stage 1/4 dependency_scan: max_severity=none threshold=medium
stage 2/4 container_image_scan: max_severity=low threshold=medium
stage 3/4 secret_detection: max_severity=none threshold=medium
stage 4/4 static_analysis: max_severity=none threshold=medium
Security scan passed. Requesting the production promotion via Agent Gateway.
```

**3. The scan asks for the promotion, and the gateway holds it.** The script calls
`run_nested_release` with `tool1_mode=uncontrolled` -- an honest self-description,
because it cannot hold a pause. The mandate is on, so the Operation the gateway
creates carries `required_approver_team="security"`, and the answer comes back
`blocked_nested_approval`.

**4. The script dies on its own.** It prints the identifiers and exits `2`:

```text
Approval is required and this script cannot wait for it.
Record these to finish the promotion by hand once it is approved:
  workflow_id:  wf-legacy
  operation_id: op-...
```

Narrate this as the script dying, not as something you killed. Nobody stopped it;
it had nowhere to wait. Check with `echo $?`. The process is gone.

**5. The kill.** Now crash the gateway's own worker -- the process serving the
`agentic-gateway` task queue and hosting `AgenticChainWorkflow`:

```bash
docker compose kill worker && docker compose up -d worker
```

Not the script. The script is already gone.

**6. Look at what survived.** Open the Temporal Web UI at http://localhost:8233,
namespace `default`, and find `wf-legacy`. The pending Operation is still there,
still `waiting_for_approval`, sitting in `AgenticChainWorkflow`'s Event History,
completely unaffected by the restart.

> The script that asked for this is gone -- it doesn't exist anymore, we'd have to
> re-run all four scan stages from scratch if we needed to redo its part. But the
> request itself survived, because it was never the script's state to begin with
> -- it was already durable, on the gateway side, the moment the script made this
> call.

**7. Try to approve it as Waypoint.** Sign in to the dashboard as `tok_dustin` and
click **Approve** on that row.

```text
Not authorized to decide this operation
operation op-... requires approval from the security team;
'Dustin Sweet <dustin.sweet@quickmeals.com>' is not authorized to decide it
```

Even now, mandate or not, Waypoint cannot clear this. It has to be Security.

**8. Approve it as Security, and finish it.** Sign out, sign in as `tok_abe`, and
approve. Then run the command the script printed, because the script is not coming
back and a human has to be the one who finishes what it started:

```bash
.venv/bin/python gateway_call.py resume_nested_release \
  workflow_id=wf-legacy operation_id=<operation_id>
```

`PromoteReleaseChildWorkflow` runs its four steps -- `deploy_binaries`,
`health_check_new_instances`, `update_traffic_routing`, `update_release_notes` --
and the fleet panel rolls production over.

**Act One in one sentence:** durability is a property of what is built on
Temporal, not a property of the system as a whole. The non-durable half, the
script, is gone for good and would have to be redone; the durable half, the
gateway's Operation, survived a hard crash without losing anything.

### Act Two: Security has adopted Temporal

> Security took this seriously. They learned Temporal, and they rebuilt their
> scanning process as a proper, durable workflow, behind their own Nexus endpoint,
> in their own namespace. Watch what changes.

**1. Bring up the Security team's own platform.** A separate process, a separate
namespace, a separate task queue, a separate worker identity:

```bash
docker compose up -d security-scan-worker
```

```text
security scan worker started, namespace 'security', task queue 'security-tq'
```

Give it about five seconds to advertise itself.

**2. Kick off a new run.** Leave the mandate switch on -- it is what puts the scan
in the pipeline's path, and Act Two is the same mandate as Act One. Then get a fresh
staged candidate and let the pipeline reach it, the same way the prerequisite did:

```text
Deploy the next minor version.
```

The Waypoint pipeline runs as it always has and then hands off. `SecurityScanWorkflow`
appears in the `security` namespace and runs its own four stages over about
fifteen seconds -- this time as a real, durable workflow rather than a stateless
script. The scan card that has been sitting `idle` since Act One fills in: a dot
per stage, the stage running right now, and the highest severity it turned up.

**3. The scan makes the nested call.** On a clean pass, `SecurityScanWorkflow`
calls `AgentGatewayService.request_protected_action` -- a Nexus operation, straight
from workflow code -- requesting the prod promotion. The workflow then suspends
directly on the pending operation (`await handle`). The promotion appears on the
approval queue with the call path `security -> security_scan -> promote_release`.

**4. The kill.** With the scan genuinely suspended on that pending operation --
not before the nested call, not after approval, precisely during the suspension:

```bash
docker compose kill security-scan-worker && \
  docker compose up -d security-scan-worker
```

**5. Look at what survived.** In the Temporal Web UI, switch the namespace
selector to `security` and open the scan's Event History. Every stage, the
verdict, and the outbound Nexus call are all already recorded, untouched by the
restart:

```text
run_scan_check  x4
NexusOperationScheduled     request_protected_action
NexusOperationStarted
    ... still pending, across the kill and the restart ...
```

> Nothing here has to be redone. Not one stage re-runs. This workflow doesn't know
> or care that its own worker just died -- it's picking up exactly where it left
> off, waiting on the exact same pending decision.

**6. Approve as Security.** Sign in as `tok_abe` and approve. Same rule, same
mechanism as Act One, now reached through the Nexus path rather than the direct
Signal path.

**7. Watch the whole chain finish.** `NexusOperationCompleted` lands in the scan's
history, `SecurityScanWorkflow` resumes and completes, and production rolls over
on the fleet panel. Nobody ran a command to finish it.

**Act Two in one sentence:** now that the calling tool is itself durable, there is
nothing left to redo, anywhere, no matter what gets killed or when -- the entire
chain, both sides of the boundary, survives.

### The contrast

> Same mandate, same approval requirement, same kind of crash -- the only thing
> that changed between these two acts is whether Security's own tool was built on
> Temporal. That's the entire difference between losing work and losing nothing.

Stop there. Nothing follows it.

### Deliberately not in this walkthrough

Stated so a future editor does not put them back:

- **No individual sub-Activity demo.** `check_license_compliance` and the other
  `dependency_scan` sub-steps exist for Event History richness, not as a
  walkthrough beat.
- **No failure paths.** No failed scan, no failed quality gate, no failed health
  check. All three are implemented and correct; none is part of the live run.
- **No second kill in either act.** One kill per act, two in the whole of CASE-2.
- **No separate section for CASE-2a's pipeline-and-gates mechanics.** It is the
  one-paragraph prerequisite above and nothing more. It is a phase that produces
  the staged candidate these two acts act on, not a third use case alongside them.
- **No re-explanation of the Nexus mechanism's internals** -- endpoints,
  contracts, `ProtectedActionWorkflow` -- during the live sequence. That belongs
  in [README.md](README.md) for anyone who asks afterwards.

## 7. CASE-3: trigger it with the Google ADK agent

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

## 8. Inspect the audit trail

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

For a step 6 Act Two run, the handoff and its resolution are in the same ledger:

- `scan_capability_discovered`, then `security_scan_handoff` with the scan
  workflow id and the **endpoint** it was reached on. No namespace is recorded,
  because the pipeline was never told one
- `controlled_caller_notified` when the decision was handed to the workflow
  holding the request open, or `controlled_caller_unreachable` if it could not
  be. Both are same-namespace: the boundary is crossed by the Nexus operation
  completing, not by this
- `security_scan_pipeline_settled` when the parked pipeline operation was closed
  out
- `security_scan_failed` when the Security team reported a blocking finding
  instead

The same execution history is visible in the Temporal UI at
http://localhost:8233. For an Act Two run there are three executions across two
namespaces: the chain and its `protected-action::...` adapter under `default`,
and the scan's own under `security`. None is a subset of the others, which is
the point -- each team's system keeps its own record of what it did, under its
own retention policy.

## 9. Troubleshooting

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

### The prerequisite pipeline returns `processing`, not `waiting_for_approval`

The Security team's worker is already running, so the pipeline found a scan
provider and handed off. That is step 6's Act Two arriving during Act One. Check
with `docker compose ps`, and see "Start over" below for why plain
`docker compose down` leaves it up.

### Act Two fails with "Could not start a pre-prod security scan"

The Nexus operation timed out, which means nothing was polling `security-tq` in
the `security` namespace for Nexus tasks. Two causes, in order of likelihood:

**A stale image.** `docker compose build` skips services behind a profile, so
`security-scan-worker` can be running last week's code. Check that the Nexus
handler is even in the image:

```bash
docker compose exec security-scan-worker ls /app/security_scan/
```

If `nexus_handlers.py` is missing, rebuild properly:

```bash
docker compose --profile '*' build security-scan-worker
docker compose up -d --force-recreate security-scan-worker
```

**Missing endpoints.** Confirm both are registered:

```bash
docker compose exec temporal temporal operator nexus endpoint list --address temporal:7233
```

You want `agent-gateway` → `default`/`agentic-gateway` and `security` →
`security`/`security-tq`. If they are absent, rerun the registration
with `docker compose up nexus-endpoints`.

### Act Two returns `waiting_for_approval` instead of `processing`

No scan provider was found, so the pipeline opened the production promotion
itself. Either the worker is not running, or it has not advertised itself yet.
Confirm it started:

```bash
docker compose logs security-scan-worker
```

```text
security scan worker started, namespace 'security', task queue 'security-tq'
```

Then ask the registry directly:

```bash
curl -s -X POST localhost:9000/invoke -H 'content-type: application/json' \
  -d '{"tool_name":"get_security_scan_status","arguments":{}}'
```

`available: true` means the pipeline will route through the scan on its next run.
Note how little the answer contains -- a provider and an endpoint, no namespace
and no task queue -- which is the same restraint the code shows. Registration is
a heartbeat with a TTL, so give it about five seconds after the worker starts,
and expect the same delay after any `mock-tool` recreate.

### The security scan card is stuck partway through

The scan itself is fifteen seconds, so give it that first. If it stays put, the
security scan worker is not processing tasks. The chain is unaffected -- the
scan's workflow keeps its place and picks up where it left off when a worker
returns, which is exactly what Act Two's kill demonstrates on purpose:

```bash
docker compose logs security-scan-worker
docker compose start security-scan-worker
```

### A pipeline operation is stuck in `waiting_for_dependency`

It handed off to the scan and is waiting on a verdict. Note that a scan worker
which was never running at all does **not** produce this: the handoff operation
has a thirty second schedule-to-close timeout, so that case fails fast with
"Could not start a pre-prod security scan" instead (see above).

This state means the scan was started and then stopped progressing -- the scan
worker died after the handler ran. The scan itself has no deadline, so the
operation waits indefinitely. Bring the worker back and the scan resumes where
it left off:

```bash
docker compose logs security-scan-worker
docker compose start security-scan-worker
```

If the scan's workflow has completed or failed and the pipeline operation still
has not moved, look for `controlled_caller_unreachable` in the chain ledger.

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
second run-through. `security-scan-worker` sits behind a Compose profile so it
does not start with the stack, and plain `docker compose down` leaves services
from inactive profiles running. If it is still up when you start step 6, the
pipeline finds a scan provider and hands off, and Act Two's reveal happens during
Act One. Check with `docker compose ps` if the prerequisite pipeline returns
`processing` instead of `waiting_for_approval`.

The same flag applies to `docker compose build`, which otherwise leaves that
worker on a stale image. Both omissions fail quietly, in opposite directions.
