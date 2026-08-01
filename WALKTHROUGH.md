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
- Temporal UI: http://localhost:8233
- MCP endpoint: http://localhost:8080/mcp

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

## 6. CASE-2: the team evolved the solution

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

## 7. CASE-2 safety boundary: a Tool1 that cannot suspend

The pipeline in step 6 is suspension-aware: it holds its own progress in Temporal
across the approval. This step shows what happens when the orchestrator is not, and
what that costs now that its work is real.

Nobody types "use an uncontrolled Tool1", so drive this one yourself rather than
through a prompt, and narrate it: *suppose this is the older orchestrator build,
the one that runs the pipeline in-process and unwinds its stack the moment
something pauses.*

```bash
python gateway_call.py run_release_orchestration tool1_mode=uncontrolled
```

`gateway_call.py` calls one gateway tool directly, with no agent in the loop. Plain
curl cannot: streamable HTTP MCP needs an initialize handshake and a session id
first.

### A. Fail closed, with the cost on screen

The pipeline runs in full: the release is cut, staging rolls over, the gate card
appears and goes green. Then:

```text
status: blocked_nested_approval
retry_required: true
```

Approve it in the dashboard. Approval is recorded and production still does not
move:

```text
status: approved_retry_required
```

Retry it explicitly and the gateway refuses:

```bash
python gateway_call.py resume_nested_release \
  workflow_id=<workflow_id> operation_id=<operation_id>
```

```text
reason: uncontrolled_tool_not_replay_safe
```

Look at the fleet panel while you say this. A cut release, a staging deployment,
and a completed gate run are all sitting there stranded, and production is
untouched. The gateway will not guess that an orchestrator which cannot suspend is
safe to replay. That is what fail-closed costs, and it is the argument for the
step 6 pipeline.

### B. An explicitly replay-safe retry, and its price

Repeat with replay safety advertised, approve in the dashboard, then retry:

```bash
python gateway_call.py run_release_orchestration \
  tool1_mode=uncontrolled replay_safe=true
python gateway_call.py resume_nested_release \
  workflow_id=<workflow_id> operation_id=<operation_id>
```

It completes:

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

Compare that to step 6, where the same approval delay cost nothing. Same approval,
same outcome, and the suspension-aware pipeline paid for the gates once.

## 8. CASE-3: trigger it with the Google ADK agent

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

If you prefer a terminal client, start the same Temporal execution model with:

```bash
docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id walkthrough-temporal-adk-1 \
  --prompt 'Start Scenario 3 for delivery-matching-service version 2.5.0 to prod. Use agent_run_id walkthrough-adk-run-1 and justification "autonomous walkthrough".'
```

### B. Trigger Scenario 3

Send this message to the ADK agent:

```text
Start an autonomous release run for delivery-matching-service 2.5.0 to
prod, with agent_run_id walkthrough-adk-run-1. The rollout is validated and
ready to go.
```

Expected result:

```text
status: waiting_for_approval
workflow_id: wf-adk-...
operation_id: ...
```

The `agent_run_id` is the durable run key. When no separate idempotency key is
given, Agent Gateway also uses it as the caller key. Repeating this exact request
recovers the same run; use `walkthrough-adk-run-2` for a new run.

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

## 9. Inspect the audit trail

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

The same execution history is visible in the Temporal UI at
http://localhost:8233.

## 10. Troubleshooting

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

### Start over

Stop the stack while preserving Temporal data:

```bash
docker compose down
```

To delete all demo workflow history and start completely clean:

```bash
docker compose down -v
```

The `-v` command permanently removes the demo's Temporal volume.
