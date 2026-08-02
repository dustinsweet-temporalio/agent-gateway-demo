# Agent Gateway suspend/resume walkthrough

This guide demonstrates the three suspend/resume scenarios end to end. It is
written for a first-time evaluator who wants to observe the policy decision,
durable pause, human decision, resumed execution, and audit evidence. For the
architecture and implementation details, see [README.md](README.md).

> **Local demo only.** The included identities and bearer tokens are static
> examples, endpoints use plain HTTP, and Temporal runs in development mode. Do
> not expose these services to an untrusted network or reuse this configuration
> in production.

Every approval request in this demo expires after **five minutes**. Approve or
reject it before the dashboard countdown reaches zero. Restarting a service does
not reset the deadline.

## 1. Prerequisites

Run all commands from the repository root in a Bash- or Zsh-compatible shell.
You need:

- Docker with Compose v2;
- Claude Code, installed and authenticated, for Scenarios 1 and 2;
- a Gemini API key for Scenario 3;
- network access while Docker builds the images and while Gemini is in use; and
- local ports `7233`, `8000`, `8080`, `8233`, and `9000` available.

Choose a short value that is unique to this walkthrough run, such as
`sam-20260726-a`. Replace every literal `<run-suffix>` below with that value.
Unique idempotency keys and agent run IDs prevent an earlier durable execution
from being mistaken for a new request.

If you intentionally want to erase an earlier demo first, use the destructive
reset described in [Section 12](#12-stop-or-reset-the-demo).

## 2. Configure and start the stack

Create `.env` only if it does not already exist:

```bash
test -f .env || cp .env.example .env
```

Edit `.env`, replace the `GOOGLE_API_KEY` placeholder, and optionally select a
different `ADK_MODEL`. Do not commit `.env`.

Start the complete stack:

```bash
docker compose up --build -d
```

Check container state and the two application health endpoints:

```bash
docker compose ps
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:9000/healthz
docker compose logs worker
```

If a health request races service startup, wait a few seconds and repeat it.
Both health endpoints should return `{"ok":true}`. The worker logs should
include:

```text
worker started, polling task queue 'agentic-gateway'
```

The local endpoints are:

| Service | URL |
| --- | --- |
| Approval dashboard | http://localhost:8080 |
| Google ADK Web | http://localhost:8000 |
| Temporal Web UI | http://localhost:8233 |
| Agent Gateway MCP | http://localhost:8080/mcp |

Docker Compose injects `GOOGLE_API_KEY` only into the worker. It injects
`ADK_MODEL` into both the worker and ADK Web so new ADK session workflows use
the selected model. Neither value is baked into a Docker image.

## 3. Connect the operator and approver

### Connect Claude Code

Register the gateway in the local Claude Code scope:

```bash
claude mcp add --scope local --transport http \
  agent-gateway http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_dustin"
```

If an `agent-gateway` entry already exists, inspect `claude mcp list` before
replacing it. Remove an obsolete entry with `claude mcp remove agent-gateway`,
then run the registration command again.

Confirm that Claude Code can reach the server:

```bash
claude mcp list
```

Verify the identity that the gateway derives from the bearer token:

```bash
curl -fsS \
  -H "Authorization: Bearer tok_dustin" \
  http://localhost:8080/whoami
```

The JSON response should include:

```json
{
  "resolved_from_this_request": "Dustin Sweet <dustin.sweet@temporal.io>",
  "is_approver": false
}
```

The response contains additional diagnostic fields; the two values above are
the important checks.

### Sign in as the approver

Open http://localhost:8080 and enter:

```text
tok_approver
```

The dashboard should identify you as `approver@demo`. Decisions are attributed
to this authenticated identity rather than to a name supplied in a form. Keep
the dashboard open; it refreshes automatically when you are not typing.

## 4. Scenario 1: a directly protected tool call

Scenario 1 shows an immediate unprotected call, a production approval pause,
approval and result recovery, and rejection.

### Record the production baseline

Ask Claude Code:

```text
Using agent-gateway, call get_deployed_version for prod and show the
structured tool result.
```

Record `result.deployed_version`. A clean mock backend starts at `2.1.0`, but
the rest of this walkthrough compares against the value you actually observe.

### Run an unprotected staging action

Ask Claude Code:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.1
to staging. Use idempotency key
walkthrough-case1-staging-<run-suffix>. Show the structured tool result.
```

Confirm:

```text
status: completed
result:
  environment: staging
  version: 2.3.1
  promoted: true
```

Staging is not protected by the demo policy, so no approval row is created.

### Request a protected production action

Ask Claude Code:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.1
to prod. Use justification "walkthrough production promotion" and
idempotency key walkthrough-case1-prod-<run-suffix>. Show the structured
tool result.
```

Confirm and save both returned IDs:

```text
status: waiting_for_approval
workflow_id: ...
operation_id: ...
reason: approval_required
```

The production action has not run. Query the deployed production version again
and confirm that it still matches the baseline you recorded.

In the approval dashboard, locate the row with the saved `operation_id` and
confirm that it includes:

- requester `Dustin Sweet <dustin.sweet@temporal.io>`;
- requested action;
- safe argument summary;
- requester justification and policy risk reason;
- complete call path; and
- approval countdown.

### Approve and recover the result

Click **Approve** before the countdown expires. Then ask Claude Code:

```text
Using agent-gateway, call get_operation_result with workflow_id
<workflow_id> and operation_id <operation_id>. Show the structured result.
```

If the result is still `processing`, wait for its returned
`poll_after_seconds` value and repeat the same query. The terminal result is:

```text
status: completed
result:
  environment: prod
  version: 2.3.1
  promoted: true
```

Query the deployed production version again and confirm that it is now `2.3.1`.

### Reject a different production action

Ask Claude Code:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.2
to prod. Use justification "walkthrough rejection" and idempotency key
walkthrough-case1-reject-<run-suffix>. Show the structured tool result.
```

Save the new `workflow_id` and `operation_id`. Enter a reason in the dashboard
and click **Reject**, then ask:

```text
Using agent-gateway, call get_operation_result with workflow_id
<workflow_id> and operation_id <operation_id>. Show the structured result.
```

Confirm:

```text
status: rejected
```

The deployed production version should remain `2.3.1`.

## 5. Durability: restart while approval is pending

Start one more protected request:

```text
Using agent-gateway, promote delivery-matching-service version 2.3.3
to prod. Use justification "restart durability walkthrough" and
idempotency key walkthrough-restart-<run-suffix>. Show the structured
tool result.
```

Save its `workflow_id` and `operation_id`, leave it waiting, and restart the two
processes that do not own the durable pause:

```bash
docker compose restart worker gateway
```

The original five-minute approval deadline continues during the restart.
Verify readiness again:

```bash
curl -fsS http://localhost:8080/healthz
docker compose logs worker
```

Refresh the dashboard and sign in again if needed. The operation should still
be waiting with the same IDs. Approve it, then use `get_operation_result` with
the saved IDs until it returns `completed`.

This succeeds because Temporal, rather than the gateway or worker process, owns
the pause. This is the durability test; `docker compose down` has different
persistence consequences described in [Section 12](#12-stop-or-reset-the-demo).

## 6. Scenario 2: controlled Tool1 to Tool2

Scenario 2 first shows a suspension-aware Tool1. Temporal checkpoints Tool1 at
the protected Tool2 boundary and resumes it automatically after approval.

Ask Claude Code:

```text
Using agent-gateway, run a nested release for delivery-matching-service
version 2.4.0 to prod. Set tool1_mode to controlled, replay_safe to false,
justification to "controlled nested walkthrough", and idempotency key to
walkthrough-case2-controlled-<run-suffix>. Show the structured tool result.
```

Confirm and save all three IDs:

```text
status: waiting_for_approval
workflow_id: ...
parent_operation_id: ...
operation_id: ...
call_path:
  - ClaudeCode
  - release_orchestrator
  - promote_release
```

`operation_id` identifies the protected Tool2 child.
`parent_operation_id` identifies Tool1.

Approve the child operation. Then ask:

```text
Using agent-gateway, call get_workflow_status for workflow_id
<workflow_id>. Show the structured result.
```

If either operation is still processing, wait five seconds and query again.
Locate the operations by the IDs you saved and confirm:

- child Tool2 is `completed`;
- parent Tool1 is `completed`;
- the parent result contains `resumed_from_checkpoint: true`; and
- the parent result contains `replayed: false`.

Production should now report version `2.4.0`.

## 7. Scenario 2 safety boundary: uncontrolled Tool1

An uncontrolled Tool1 cannot be suspended safely after it reaches a nested
approval. The gateway records the approval but does not guess that replaying
Tool1 is safe.

### Demonstrate fail-closed behavior

Ask Claude Code:

```text
Using agent-gateway, run a nested release for delivery-matching-service
version 2.4.1 to prod. Set tool1_mode to uncontrolled, replay_safe to
false, justification to "uncontrolled fail-closed walkthrough", and
idempotency key to walkthrough-case2-unsafe-<run-suffix>. Show the
structured tool result.
```

Confirm and save all returned IDs:

```text
status: blocked_nested_approval
workflow_id: ...
parent_operation_id: ...
operation_id: ...
retry_required: true
reason: uncontrolled_tool_requires_retry
```

The returned `operation_id` is again the protected Tool2 child. Approve that
child in the dashboard. Approval is now recorded, but Tool2 has not executed.
Query `get_operation_result` with the workflow and child operation IDs:

```text
status: approved_retry_required
retry_required: true
reason: uncontrolled_tool_not_replay_safe
```

Now ask Claude Code:

```text
Using agent-gateway, call resume_nested_release with workflow_id
<workflow_id> and operation_id <child-operation-id>. Show the structured
tool result.
```

It remains fail-closed:

```text
status: approved_retry_required
reason: uncontrolled_tool_not_replay_safe
```

Production should still report `2.4.0`.

### Demonstrate an explicitly replay-safe retry

Start a new nested request:

```text
Using agent-gateway, run a nested release for delivery-matching-service
version 2.4.2 to prod. Set tool1_mode to uncontrolled, replay_safe to
true, justification to "explicit replay-safe walkthrough", and
idempotency key to walkthrough-case2-replay-safe-<run-suffix>. Show the
structured tool result.
```

Save its `workflow_id`, `parent_operation_id`, and child `operation_id`. Approve
the child. As before, the approval produces `approved_retry_required`; it does
not execute Tool2 automatically.

Explicitly resume with the child ID:

```text
Using agent-gateway, call resume_nested_release with workflow_id
<workflow_id> and operation_id <child-operation-id>. Show the structured
tool result.
```

Confirm:

```text
status: completed
result:
  resumed_from_checkpoint: true
  replayed: true
```

Temporal re-enters Tool1 with the original idempotency key, reaches the same
Tool2 boundary, invokes the already approved action, and completes Tool1.
Production should now report `2.4.2`.

## 8. Scenario 3: autonomous Google ADK agent

Scenario 3 uses Dashy in `adk_agents/release_approval_agent`. Each ADK Web
session owns one running `TemporalAdkSessionWorkflow`. Normal user turns become
Updates on that workflow and reuse its ADK runner and conversation history.
Gemini calls and Agent Gateway MCP calls execute as Temporal Activities in the
worker.

The ADK MCP connection authenticates with the `tok_adk` demo identity and can
see only the Scenario 3 start and lifecycle tools. It cannot bypass the
autonomous workflow by calling the generic promotion tool.

This section uses ADK Web. The command-line client in
[Section 9](#9-optional-scenario-3-command-line-client) is an alternative, not
an additional step. Do not run two clients concurrently with the same
`agent_run_id`.

### Start the autonomous run

Open http://localhost:8000 and select `release_approval_agent`. Send:

```text
Start Scenario 3 for delivery-matching-service version 2.5.0 to prod.
Use agent_run_id walkthrough-adk-web-<run-suffix> and justification
"autonomous Web walkthrough".
```

Dashy should report these fields from the structured tool result:

```text
status: waiting_for_approval
workflow_id: wf-adk-...
operation_id: ...
reason: approval_required
```

Save the gateway `workflow_id` and `operation_id`. The `agent_run_id` is the
durable run key; repeating it recovers that run rather than creating another
approval. Use a new ID for a genuinely new run.

To inspect the autonomous checkpoint directly, query the returned gateway
workflow:

```bash
docker compose exec temporal temporal workflow query \
  --address 127.0.0.1:7233 \
  --workflow-id "<workflow_id>" \
  --type get_workflow_status \
  --output json
```

The checkpoint should include:

```text
step: waiting_for_approval
next_step: invoke_protected_action
protected_action_executed: false
dependent_action_executed: false
```

### Approve and watch Dashy resume

Leave the ADK turn open. In the approval dashboard, locate the saved operation
and click **Approve**.

Agent Gateway completes the autonomous workflow and signals
`agent_gateway_approval_resolved` to the originating
`TemporalAdkSessionWorkflow`. That workflow gives Dashy an internal
resume/status turn. The terminal result should appear automatically in the same
ADK conversation:

```text
status: completed
result:
  protected_action:
    environment: prod
    version: 2.5.0
    promoted: true
  dependent_action:
    followup_recorded: true
```

The dependent action runs only after the approved production action succeeds.

### Recover from a browser disconnect

If the browser connection drops while the `adk-agent` container remains intact,
reopen the same ADK Web session and send `resume` or `check the status`. The
proxy reuses the Temporal workflow and active Update IDs stored in ADK session
state. A later normal prompt becomes another Update on the same workflow.

`docker compose restart adk-agent` preserves the container-local ADK session
database. Recreating or removing that container, including with
`docker compose down`, removes the Web-to-Temporal mapping. The Temporal
workflow itself remains in Temporal, but a newly created Web session cannot
automatically rediscover that mapping. A production deployment should use a
durable ADK session service.

## 9. Optional Scenario 3 command-line client

Use this section instead of Section 8 if you prefer a terminal client. It uses
the same Temporal execution model but a distinct session and `agent_run_id`, so
it cannot take over the Web walkthrough's callback.

Replace `<run-suffix>`, then run:

```bash
CLI_PROMPT='Start Scenario 3 for delivery-matching-service version 2.5.1
to prod. Use agent_run_id walkthrough-adk-cli-<run-suffix> and
justification "autonomous CLI walkthrough".'

docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id "walkthrough-adk-cli-<run-suffix>" \
  --prompt "$CLI_PROMPT"
```

The command waits while the workflow is paused, so it does not return to the
shell after the initial model response. Find the new request in the approval
dashboard and approve it before the countdown expires.

The gateway completes the protected and dependent actions, signals the CLI's
`TemporalAdkSessionWorkflow`, and Dashy resumes the same ADK session. The
command then prints one JSON result containing both `initial_response` and
`resumed_response`; the latter should report `status: completed`.

## 10. Inspect the audit evidence

Gateway lifecycle queries enforce workflow ownership:

- for Scenario 1 and Scenario 2 workflow IDs, ask the authenticated Claude Code
  connection; and
- for a Scenario 3 `wf-adk-...` workflow ID, ask Dashy, whose MCP connection
  owns that workflow as `release-agent@google-adk`.

For the matching connection, ask:

```text
Get the workflow ledger for <gateway-workflow-id> and show the structured
result.
```

Depending on the scenario and decision, look for application ledger events
such as:

- `created` or `agent_run_started`;
- `policy_evaluated`;
- `waiting_for_approval` or `agent_checkpointed`;
- `approved`, `rejected`, `expired`, or `canceled`;
- `completed` or `agent_run_completed`; and
- `tool1_checkpointed`, `tool1_resuming`, or `tool1_replayed`.

The gateway ledger is the application's audit projection. Temporal Web at
http://localhost:8233 separately shows the underlying Workflow Event History,
including Updates, Signals, timers, Activity scheduling, retries, and
completion.

## 11. Troubleshooting

### A service is not ready

```bash
docker compose ps
docker compose logs temporal gateway worker mock-tool adk-agent
```

Confirm that the required ports are not already in use and that the gateway and
mock health endpoints return `{"ok":true}`.

### Claude Code does not see the gateway tools

```bash
claude mcp list
curl -fsS http://localhost:8080/healthz
docker compose logs gateway
```

Confirm that the configured URL is `http://localhost:8080/mcp`. If a stale MCP
entry has the same name, remove it and repeat the registration in Section 3.

### The requester appears as unverified

Run the `/whoami` request from Section 3. If
`resolved_from_this_request` is correct, the gateway identity map works and
Claude Code is not sending the configured authorization header. Re-register
the MCP entry with the exact bearer header.

### An approval row disappears

Approval requests expire after five minutes. Query `get_operation_result` with
the saved IDs. An expired request returns:

```text
status: expired
reason: approval_timeout
```

The downstream protected action was not invoked. Start a new request with a new
idempotency key or `agent_run_id`.

### An operation remains in processing

```bash
docker compose logs worker mock-tool
```

The gateway intentionally converts a slow call into asynchronous recovery after
its synchronous wait budget. Wait for `poll_after_seconds`, then query
`get_operation_result` again. If the worker or mock backend is unavailable,
restore it and let Temporal retry the Activity.

### Dashy reports a Gemini authentication or model error

Check that `.env` does not contain the placeholder and inspect:

```bash
docker compose logs worker adk-agent
```

After correcting `GOOGLE_API_KEY` or `ADK_MODEL`, recreate both consumers of
the environment values:

```bash
docker compose up -d --force-recreate worker adk-agent
```

Do this before starting an ADK walkthrough turn: recreating `adk-agent` removes
its container-local session mapping.

### ADK Web cannot find the previous session

A container restart preserves the local ADK session database; a container
recreation does not. Confirm that the underlying `adk-session-...` workflow is
still visible in Temporal Web. Its durable execution has not been deleted, but
a new ADK Web session cannot automatically attach without the saved mapping.
Use a fresh unique run ID for a new Web demonstration.

### A supposedly new request returns an earlier result

The caller reused an idempotency key or `agent_run_id`. This is expected
deduplication. Use a new `<run-suffix>`, or perform the destructive reset below
if retaining old Temporal history is not important.

## 12. Stop or reset the demo

To stop containers while preserving their writable layers and the Temporal
volume, use:

```bash
docker compose stop
```

Resume them without rebuilding or changing the Compose configuration:

```bash
docker compose up -d
```

This stop/start pattern preserves the complete local demo state, including the
mock backend and ADK Web's container-local session database.

To remove all containers and the project network while preserving only the
named Temporal volume, use:

```bash
docker compose down
```

The next `docker compose up` can recover Temporal workflow history, but the
mock backend's in-memory deployment state, the gateway's MCP-session mapping,
and ADK Web's container-local session database will be new.

To remove the containers **and permanently delete all Temporal demo history**:

```bash
docker compose down -v
```

Use `down -v` before a completely clean walkthrough only when deleting prior
demo history is intentional.
