# Agent Gateway: durable suspend and resume for agentic tool calls

A runnable demo of CASE-1 from the "Suspend/Resume Primitive for Agentic Work"
requirements. An MCP server (the Agent Gateway) pauses protected tool calls for
human approval. The pause is held by a chain-scoped Temporal entity workflow, so
it survives process restarts, and it resumes exactly where it left off once an
approver decides.

Claude Code connects to the gateway over MCP. When it calls a protected tool, the
call does not execute. The gateway returns a waiting response, a human approves or
rejects in a small web page, and the tool runs only after approval.

## Architecture

```
Claude Code ---MCP/HTTP---> Agent Gateway ---update-with-start---> Chain Workflow
                                 ^                                      |
                                 | approve / reject (Signal)            | invoke (Activity)
                            Approver UI                                 v
                                                                   Mock Tool
```

- One long-lived workflow per agentic chain, keyed by `workflow_id`. Operations
  live in a map inside workflow state.
- A tool call arrives as an Update via update-with-start. It returns fast with a
  completed result (no approval) or a waiting payload (approval required).
- Approve and reject arrive as Signals that only mutate state. The workflow's main
  loop reacts to the state change and invokes the downstream tool.
- Status and result recovery are Queries, so handlers stay short and
  Continue-As-New can drain them.
- Policy evaluation and the downstream call are Activities.

Services in `docker-compose.yml`: `temporal` (dev server plus Web UI), `worker`,
`gateway`, and `mock-tool`.

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

Confirm it is connected:

```
claude mcp list
```

## Demo script

1. Safe call completes synchronously. Ask Claude Code to read metrics, for
   example call `read_metrics` with `service=checkout`. The response is
   `status: completed` with a result, and a `workflow_id` is returned. Reuse that
   `workflow_id` on later calls in the same task.

2. Protected call pauses. Call `deploy_service` with `service=checkout`,
   `version=1.4.2`, and the same `workflow_id`. The response is
   `status: waiting_for_approval` with an `operation_id` and `poll_after_seconds`.
   The tool has not run.

3. Approve and resume. Open http://localhost:8080, approve the waiting operation,
   then poll `get_operation_result` with the `workflow_id` and `operation_id`. The
   status flips to `completed` and the result appears.

4. Reject. Trigger another protected call, reject it in the dashboard, then poll.
   The status is `rejected` with your reason. The tool never ran.

5. Timeout and expire. Set a short window and trigger a protected call:

   ```
   docker compose stop gateway
   APPROVAL_TIMEOUT_SECONDS=20 docker compose up -d gateway
   ```

   Trigger `deploy_service`, wait past the window without deciding, then poll. The
   status is `expired` and the tool was not invoked.

6. Durability. Trigger a protected call so an operation is waiting. Restart the
   worker and the gateway:

   ```
   docker compose restart worker gateway
   ```

   The waiting operation is still there. Approve it and it resumes and completes.
   Nothing was lost, because the pause lives in Temporal, not in a process.

7. Idempotency. Call the same protected tool twice with the same `workflow_id` and
   arguments. The second call returns the same `operation_id` rather than opening a
   second approval gate. After the tool runs, the mock tool reports
   `idempotent_replay: true` on any retried attempt, with a stable `executed_at`.

8. Continue-As-New. Fire many calls against one `workflow_id` to grow history. When
   the server suggests it, the chain rolls over: the `workflow_id` stays the same
   and a new Run Id appears in the Web UI at http://localhost:8233. Waiting
   operations and their deadlines carry across the rollover unchanged.

## How this maps to Temporal best practices

- Determinism. All non-deterministic work and all I/O live in Activities. The
  workflow uses `workflow.now()` and `workflow.logger`, never wall clock or print.
- Workflow Id as a business identifier. The chain id is the `workflow_id`, and it
  acts as the running-workflow uniqueness constraint for the chain.
- Idempotency. Deduplication rides on a gateway-supplied or content-derived
  idempotency key carried in workflow state, not on Update IDs, because Update IDs
  are scoped to one Execution and reset after Continue-As-New.
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
  through the input dataclass. Approval deadlines are stored as absolute times in
  state rather than as Timers, because Timers do not carry across CAN.

## Honest caveats

- Result recovery works while the worker is up and the chain is not terminated. A
  Query needs a worker polling the task queue to serve it.
- A completed or timed-out chain is still queryable within the namespace retention
  window. A terminated chain is not queryable. This demo lets a chain complete
  through the `close_chain` signal rather than terminating it, so it stays
  queryable.

## Configuration

Set via environment in `docker-compose.yml`.

- `TEMPORAL_ADDRESS` Temporal frontend address. Default `temporal:7233`.
- `TASK_QUEUE` Task queue name. Default `agentic-gateway`.
- `PROTECTED_TOOLS` Comma separated tool names that require approval. Default
  `deploy_service,delete_resource,transfer_funds`.
- `APPROVAL_AMOUNT_THRESHOLD` Numeric `amount` at or above this requires approval.
  Default `1000`.
- `APPROVAL_TIMEOUT_SECONDS` Approval window before an operation expires. Default
  `300`.
- `GATEWAY_PORT` Gateway HTTP port. Default `8080`.
- `MOCK_TOOL_URL` Downstream tool endpoint used by the invoke Activity.

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
workflows/chain.py          AgenticChainWorkflow (entity workflow, CAN)
activities/gateway_activities.py  evaluate_policy, invoke_tool
worker.py                   registers the workflow and activities
gateway/server.py           MCP HTTP server, approver UI, approval endpoints
mock_tool/server.py         downstream protected tool with idempotency dedup
```
