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
Using agent-gateway, promote delivery-matching-service version 2.3.1
to staging. Use idempotency key walkthrough-case1-staging.
```

Expected result:

```text
status: completed
```

Staging actions do not require approval.

### B. Request a protected production action

Ask:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.1
to prod. The justification is "walkthrough production promotion".
Use idempotency key walkthrough-case1-prod.
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
Using agent-gateway, get the version currently deployed in prod.
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
Using agent-gateway, get the operation result for workflow <workflow_id>
and operation <operation_id>.
```

Expected result:

```text
status: completed
```

Ask for the deployed production version again. It should now be `2.3.1`.

### E. Try rejection

Request a different production version with a new idempotency key:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.2
to prod. Use idempotency key walkthrough-case1-reject.
```

Reject it in the dashboard and provide a reason. Poll its operation result.

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
4. Ask Claude Code to poll `get_operation_result` with the saved IDs.

The operation completes because the pause lives in Temporal, not in the gateway
or worker process.

## 6. CASE-2: controlled nested Tool1 -> Tool2

Ask Claude Code:

```text
Using agent-gateway, run a nested release for delivery-matching-service
version 2.4.0 to prod. Set tool1_mode to controlled, replay_safe to false,
the justification to "controlled nested walkthrough", and the idempotency
key to walkthrough-case2-controlled.
```

Expected result:

```text
status: waiting_for_approval
parent_operation_id: ...
operation_id: ...
call_path:
  - ClaudeCode
  - release_orchestrator
  - promote_release
```

Here, `operation_id` is the protected Tool2 operation. Temporal has checkpointed
Tool1 at the Tool2 boundary.

Approve the operation, then ask:

```text
Using agent-gateway, get workflow status for <workflow_id>.
```

Confirm:

- Tool2 is `completed`.
- The parent Tool1 operation is `completed`.
- The parent result says `resumed_from_checkpoint: true`.

## 7. CASE-2 safety boundary: uncontrolled Tool1

### A. Demonstrate fail-closed behavior

Ask:

```text
Using agent-gateway, run a nested release for delivery-matching-service
version 2.4.1 to prod. Set tool1_mode to uncontrolled, replay_safe to false,
and idempotency key to walkthrough-case2-unsafe.
```

Expected result:

```text
status: blocked_nested_approval
retry_required: true
```

Approve it in the dashboard. Approval is now recorded, but Tool2 is still not
executed. Polling the operation returns:

```text
status: approved_retry_required
```

Ask Claude Code to call `resume_nested_release` with the returned IDs. Expected:

```text
reason: uncontrolled_tool_not_replay_safe
```

The gateway refuses to guess that an uncontrolled Tool1 is safe to replay.

### B. Demonstrate an explicitly replay-safe retry

Repeat with:

```text
version: 2.4.2
tool1_mode: uncontrolled
replay_safe: true
idempotency_key: walkthrough-case2-replay-safe
```

Approve Tool2, then call `resume_nested_release`.

Expected result:

```text
status: completed
result:
  replayed: true
```

Temporal re-enters Tool1 with the original idempotency key, invokes the approved
Tool2 action, and completes the replayed Tool1 call.

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
Start Scenario 3 for delivery-matching-service version 2.5.0 to prod.
Use agent_run_id walkthrough-adk-run-1 and justification
"autonomous walkthrough".
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
Get the workflow ledger for <workflow_id>.
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
