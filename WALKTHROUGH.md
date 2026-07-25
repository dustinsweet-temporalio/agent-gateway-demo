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

In the approval dashboard, confirm the waiting row shows:

- requester
- requested action
- safe argument summary
- justification and risk reason
- call path
- approval deadline

### D. Approve and recover the result

Click **Approve** in the dashboard.

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
`adk_agents/release_approval_agent`. It reaches Agent Gateway over MCP with the
`tok_adk` demo identity and can see only the Scenario 3 start and recovery tools.

### A. Start ADK Web

In a second terminal, provide a Gemini API key and start the optional Compose
profile:

```bash
export GOOGLE_API_KEY="<your Gemini API key>"
docker compose --profile adk up --build adk-agent
```

Open http://localhost:8000 and select `release_approval_agent`.

If you prefer the terminal, install the requirements and run:

```bash
export GOOGLE_API_KEY="<your Gemini API key>"
export AGENT_GATEWAY_MCP_URL="http://localhost:8080/mcp"
export AGENT_GATEWAY_TOKEN="tok_adk"
.venv/bin/adk run adk_agents/release_approval_agent
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

### C. Inspect the durable pause

Ask the same ADK agent:

```text
Get the workflow status for <workflow_id>.
```

Confirm the checkpoint shows:

```text
step: waiting_for_approval
next_step: invoke_protected_action
protected_action_executed: false
dependent_action_executed: false
```

### D. Approve and recover

Approve the operation in the Agent Gateway dashboard. Then ask the ADK agent:

```text
Get the operation result for workflow <workflow_id> and operation
<operation_id>.
```

Expected result:

```text
status: completed
result:
  protected_action: ...
  dependent_action:
    followup_recorded: true
```

The dependent action runs only after the approved production action succeeds.
Stopping ADK Web while the request waits does not lose it; restart ADK and poll
using the same returned IDs.

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
