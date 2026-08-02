# Agent Gateway: durable suspend and resume for agentic tool calls

This repository is a runnable demonstration of three human-approval patterns for
agentic work. An MCP server accepts tool calls, Temporal owns the durable
execution state, and a human decides protected operations in an approval
dashboard. Work survives gateway and worker restarts because the pause is stored
in Temporal rather than in a client connection or process.

The sample domain is release deployment. Reads, release creation, and
non-production promotions run immediately. A production promotion pauses until
an authorized approver approves, rejects, cancels, or lets it expire.

> [!WARNING]
> This is a local demonstration, not a production deployment. It ships with
> public demo tokens, plaintext HTTP, a Temporal development server, an
> in-memory mock deployment backend, and a development-only ADK Web UI. Never
> reuse its credentials or security configuration in a real environment.

For an execution-first guide to all three scenarios, see
[WALKTHROUGH.md](WALKTHROUGH.md).

## Quick start

### Prerequisites

- Docker Desktop or Docker Engine with Compose v2
- Free local ports `7233`, `8000`, `8080`, `8233`, and `9000`
- Claude Code only if you want to drive Scenarios 1 and 2 through Claude
- A Gemini API key only if you want to run the Google ADK agent in Scenario 3

For Scenario 3, create the local environment file before starting the stack:

```bash
cp .env.example .env
# Replace GOOGLE_API_KEY with a Gemini API key.
# ADK_MODEL may be changed from its documented default.
```

Scenarios 1 and 2 do not use Gemini, so `.env` is optional for those paths.

Start all services:

```bash
docker compose up --build
```

Wait for the worker log:

```text
worker started, polling task queue 'agentic-gateway'
```

The local endpoints are:

| Surface | URL |
| --- | --- |
| Approval dashboard | [http://localhost:8080](http://localhost:8080) |
| Google ADK Web | [http://localhost:8000](http://localhost:8000) |
| Temporal Web UI | [http://localhost:8233](http://localhost:8233) |
| Agent Gateway MCP | `http://localhost:8080/mcp` |
| Gateway health check | [http://localhost:8080/healthz](http://localhost:8080/healthz) |

Sign in to the approval dashboard with the local demo token
`tok_approver`. Decisions are recorded as `approver@demo`.

Stop containers while preserving Temporal history:

```bash
docker compose down
```

This removes container-local ADK sessions and mock state. To delete the named
Temporal volume too:

```bash
docker compose down -v
```

The `-v` operation permanently deletes all demo workflow history.

## Scenarios

The names `CASE-1`, `CASE-2`, and `CASE-3` in the source requirements correspond
to Scenarios 1, 2, and 3 below.

| Scenario | Entry point | Durable behavior |
| --- | --- | --- |
| 1: approval-gated tool | `promote_release` | Evaluate policy, pause a production action, record a decision, invoke after approval, and recover the result |
| 2: nested Tool1 → Tool2 | `run_nested_release` | Checkpoint a controlled caller before protected Tool2; fail closed and require an explicit replay-safe retry for an uncontrolled caller |
| 3: autonomous agent | `start_google_adk_release_run` | Checkpoint an autonomous run, execute a protected action after approval, run its dependent follow-up, and signal the originating ADK session |

## Architecture

```text
Claude Code ── MCP/HTTP ───────────────────────────────┐
                                                      ▼
Google ADK Web                                  Agent Gateway
      │                                               │
      ▼                                               ├── AgenticChainWorkflow
TemporalAdkSessionWorkflow                            │   (Scenarios 1 and 2)
      ├── Gemini model Activity                       │
      └── MCP tool Activity ──────────────────────────┤
                                                      └── AutonomousAgentWorkflow
                                                          (one Scenario 3 run)
                                                                  │
                                                invoke Activities │
                                                                  ▼
                                                      Mock deployment backend

Approver dashboard ── approve/reject Signals ────────────────► workflows
AutonomousAgentWorkflow ── terminal callback Signal ────────► ADK session workflow
```

The Scenario 3 path intentionally contains two workflow types:

| Workflow | Lifetime and responsibility |
| --- | --- |
| `AgenticChainWorkflow` | One running execution per Claude/MCP chain. Owns Scenario 1 and 2 operations, approvals, ledger state, idle close, and Continue-As-New |
| `TemporalAdkSessionWorkflow` | One long-running workflow per ADK Web session. Owns one ADK runner and conversation history; later user turns are serialized Temporal Updates on the same workflow |
| `AutonomousAgentWorkflow` | One Scenario 3 release run per authenticated principal and `agent_run_id`. Owns the protected promotion, approval checkpoint, and dependent follow-up |

ADK Web is a thin client. The worker executes Gemini and Agent Gateway MCP calls
as Activities using Temporal's Google ADK integration. Its MCP toolset
authenticates as `release-agent@google-adk`, exposes only Scenario 3 start and
lifecycle tools, and cannot call generic `promote_release`.

### Identifiers

| Identifier | Meaning |
| --- | --- |
| MCP session ID | Transport-level session used by the gateway to infer a Scenario 1 or 2 chain while the gateway process remains running |
| `workflow_id` | The gateway operation workflow returned by tools: an `AgenticChainWorkflow` for Scenarios 1 and 2 or an `AutonomousAgentWorkflow` for Scenario 3 |
| `operation_id` | One operation inside the returned gateway workflow; use it with status, result, cancellation, and nested-resume tools |
| ADK session ID | Conversation session selected in ADK Web |
| ADK session workflow ID | Internal `TemporalAdkSessionWorkflow` ID stored in ADK session state; it is distinct from the Scenario 3 `workflow_id` returned by Agent Gateway |
| `agent_run_id` | Caller-supplied stable key for a Scenario 3 autonomous run. Reuse recovers that run; use a new value for a different request |
| idempotency key | Optional caller key scoped to the authenticated principal and workflow. Reuse returns the existing operation |

Retry an `agent_run_id` only for the same release intent. The gateway derives its
Scenario 3 workflow ID from the authenticated principal and this value.

### State ownership and restart behavior

| State | Storage | Restart behavior |
| --- | --- | --- |
| Workflow checkpoints, approval deadlines, results, and workflow ledger | Temporal development server on `temporal-data` | Survives worker and gateway restarts and `docker compose down`; deleted by `docker compose down -v` |
| MCP-session-to-chain correlation | Gateway process memory | Lost when the gateway restarts; retain returned IDs and pass them explicitly for recovery |
| Mock deployed versions and downstream idempotency records | Mock service memory | Lost when `mock-tool` is restarted or recreated |
| ADK Web session metadata and saved Temporal handles | ADK Web local storage inside its container | Survives `docker compose restart adk-agent`; lost when the container is removed or recreated |
| Gemini and gateway credentials | Container runtime environment | Not copied into either application image; production should use a secret manager |

## Execution model

For Scenarios 1 and 2, update-with-start delivers a call to
`AgenticChainWorkflow`. The Update returns a completed result, a waiting
approval handle, or an asynchronous polling handle.

- Policy evaluation and downstream invocation are Activities; production
  promotion requires approval by default.
- Approve, reject, cancel, and close messages are Signals. Handlers mutate
  state, and the workflow run loop performs downstream work.
- Status, result, workflow summary, owner, and ledger reads are Queries.
- Controlled nested Tool1 checkpoints before protected Tool2 and resumes with
  Tool2's durable result.
- Uncontrolled Tool1 fails closed. Approval records intent but does not invoke
  Tool2 until an explicit retry, and retry is refused unless Tool1 declared
  itself replay-safe.
- After 24 hours without pending work the chain completes. It uses
  Continue-As-New when recommended, retaining pending and bounded recent state.

For Scenario 3, `start_google_adk_release_run` starts or recovers an
`AutonomousAgentWorkflow`, which pauses before production mutation. At a
terminal outcome it sends the fixed `agent_gateway_approval_resolved` Signal to
the originating session workflow. Dashy receives an internal resume/status turn
in the same runner and reports the authoritative result.

After an ADK Web disconnect, reopen the same session and send `resume` or
`check the status`. The proxy reattaches to saved workflow and Update handles;
it creates a new session workflow only when no saved workflow is running.

## Tool contract

Every MCP tool publishes concrete schemas. Operation responses share `status`,
`workflow_id`, `operation_id`, result or reason fields, and polling guidance.

| Tool | Purpose |
| --- | --- |
| `get_deployed_version(environment)` | Read test, staging, or production state; never approval-gated |
| `cut_release(service, version)` | Register a release candidate; never approval-gated |
| `promote_release(service, version, environment, justification)` | Promote a release; production pauses for approval |
| `run_nested_release(..., tool1_mode, replay_safe)` | Run Scenario 2 with a controlled or uncontrolled Tool1 |
| `resume_nested_release(workflow_id, operation_id)` | Explicitly replay an approved uncontrolled path when it is declared safe |
| `start_google_adk_release_run(..., agent_run_id)` | Start or recover the dedicated Scenario 3 autonomous workflow |
| `get_operation_status` / `get_operation_result` | Query one operation |
| `get_workflow_status` / `get_workflow_ledger` | Query a workflow summary or bounded application ledger |
| `cancel_operation` | Cancel an owned pending operation before invocation |

### Synchronous and asynchronous responses

The direct Scenario 1 tools have client-wait strategies in
[`gateway/server.py`](gateway/server.py):

| Tool | Strategy |
| --- | --- |
| `get_deployed_version` | Synchronous |
| `cut_release` | Monitor for 5 seconds, then convert to a polling handle |
| `promote_release` | Monitor for 5 seconds, then convert to a polling handle |

The workflow owns the request from the start. Conversion only stops the gateway
from waiting for the Update result; it does not cancel workflow execution.
Downstream invocation runs as an Activity when policy permits it or after
approval. The demo makes release `2.3.1` take about 30 seconds to cut so the
convert-to-async path is observable. No current tool uses the gateway's
always-asynchronous strategy.

Nested, autonomous, and lifecycle entry points use purpose-specific behavior
rather than `_TOOL_STRATEGY`.

## Connect clients

### Claude Code

Register the gateway explicitly in Claude Code's local scope:

```bash
claude mcp add \
  --scope local \
  --transport http \
  agent-gateway \
  http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_dustin"

claude mcp list
```

The token maps to a verified demo requester through `GATEWAY_PRINCIPALS`. Use
`--scope project` only to intentionally create a project `.mcp.json`, and review
it before sharing. Exact prompts and results are in
[WALKTHROUGH.md](WALKTHROUGH.md).

### Google ADK

The worker needs `GOOGLE_API_KEY`; the ADK Web proxy and CLI use `ADK_MODEL` when
they start a new ADK session workflow. After changing `.env`, recreate both
services:

```bash
docker compose up -d --force-recreate worker adk-agent
```

Changing the key affects subsequent model Activities after worker recreation.
Changing the model affects newly created ADK session workflows after
`adk-agent` recreation; an already-running session retains the model in its
workflow input. The command-line client reads the worker container's model and
can override it with `--model`.

Open [http://localhost:8000](http://localhost:8000), select
`release_approval_agent`, and follow the
[Scenario 3 walkthrough](WALKTHROUGH.md#8-scenario-3-autonomous-google-adk-agent),
which also covers the CLI client for the same execution model.

## Trust and safety model

- Bearer tokens map to requester and approver identities through
  `GATEWAY_PRINCIPALS`; agent-supplied identity text is not trusted. Only
  `GATEWAY_APPROVERS` identities may decide dashboard requests.
- Explicit caller-provided workflow IDs require an authenticated principal.
  Gateway operation workflows record an owner, and lifecycle access must match
  it. Unverified callers are limited to their current MCP session.
- Caller idempotency keys are principal- and workflow-scoped. Without one, the
  gateway derives it from the workflow, tool, and arguments.
- Downstream Activities pass stable idempotency keys because Activity execution
  is at least once. The mock backend returns the original result on replay.
- Credential-like arguments are recursively redacted from query, ledger, and
  dashboard projections.
- ADK callback headers require an authenticated principal, and the callback
  signal name is fixed.
- The protected action and Scenario 3 dependent follow-up never run while the
  operation is waiting, rejected, expired, or canceled.

## Automated tests

Local workflow tests require Python, the development dependencies, and a
Temporal CLI available on `PATH`. Python 3.12 is the container baseline.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
temporal --version
python -m pytest -q
```

The tests start an ephemeral Temporal server and cover all three workflow
scenarios, terminal outcomes, deduplication, nested replay safety, MCP schemas,
authentication helpers, redaction, ADK tool filtering, and Web proxy reuse.

Workflow tests use fake policy and downstream Activities; they do not call live
Gemini or the Compose network. Use the [walkthrough](WALKTHROUGH.md) for live
ADK-to-MCP-to-gateway validation.

## Temporal design notes

- Workflow code uses Temporal time and deterministic UUIDs; I/O runs in
  Activities with explicit timeouts. Downstream calls get at most five attempts,
  while HTTP 4xx responses are non-retryable.
- Signals are idempotent for decided operations. Update validators reject
  malformed or cross-owner calls before application state is created.
- The chain initiates Continue-As-New after handlers drain and carries absolute
  deadlines, its idle clock, all pending operations, 50 recent terminal
  operations, and 500 recent ledger entries.
- Event History is the execution record by Run ID. Dashboard and ledger views
  are bounded projections, not a compliance archive.

## Configuration

Application components read the variables below. Compose supplies the displayed
local values. A host `.env` file is interpolated only where
[`docker-compose.yml`](docker-compose.yml) uses `${...}`—currently
`GOOGLE_API_KEY` and `ADK_MODEL`. Override other values in Compose or in the
target deployment environment.

| Variable | Component | Local value or code default |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | gateway, worker, ADK Web/CLI | `temporal:7233` in Compose |
| `TASK_QUEUE` | gateway, worker, ADK Web/CLI | `agentic-gateway` |
| `PROTECTED_ENVIRONMENTS` | policy Activity | `prod,production` |
| `APPROVAL_TIMEOUT_SECONDS` | gateway | `300` seconds |
| `POLL_AFTER_SECONDS` | gateway | `5` seconds |
| `GATEWAY_HOST` / `GATEWAY_PORT` | gateway | `0.0.0.0` / `8080` |
| `GATEWAY_MCP_ALLOWED_HOSTS` | gateway | local hosts plus `gateway:*` |
| `GATEWAY_MCP_ALLOWED_ORIGINS` | gateway | local browser origins |
| `GATEWAY_PRINCIPALS` | gateway | static JSON demo token-to-identity map |
| `GATEWAY_APPROVERS` | gateway | `approver@demo` |
| `GATEWAY_DEBUG` | gateway | enabled by Compose; unset disables it |
| `MOCK_TOOL_URL` | worker | `http://mock-tool:9000/invoke` |
| `SLOW_CUT_VERSION` | mock backend | `2.3.1` |
| `SLOW_CUT_DELAY_SECONDS` | mock backend | `30` seconds in Compose |
| `AGENT_GATEWAY_MCP_URL` | worker ADK Activities | `http://gateway:8080/mcp` |
| `AGENT_GATEWAY_TOKEN` | worker ADK Activities | `tok_adk` |
| `ADK_MODEL` | ADK Web and CLI session creation | `gemini-2.5-flash` |
| `GOOGLE_API_KEY` | worker model Activities | empty until supplied through `.env` |

The chain idle timeout is the deterministic
`IDLE_TIMEOUT_SECONDS` constant in
[`workflows/chain.py`](workflows/chain.py), set to 24 hours. It is deliberately
not read from process environment inside workflow code.

## Limitations and production hardening

- The deployment backend is a stub. Its state and idempotency records are
  process-local, so it does not demonstrate durable CD-system integration.
- The Temporal development server is not production infrastructure. Use a
  supported deployment with suitable retention, encryption, and access control.
- The demo runs one worker. A production deployment needs multiple workers,
  health checks, controlled rollouts, observability, and compatible workflow
  code for replay.
- Static tokens, plaintext HTTP, and the local cookie are demo-only. Add TLS,
  federated identity, scoped credentials, CSRF protection, and rate limiting.
- The ADK callback uses authenticated fixed metadata. Production should also
  issue a signed single-use callback registration, authorize the target
  workflow type and tenant, and prevent arbitrary target selection.
- MCP session correlation is gateway-memory state. Persist the mapping or
  propagate a controlled explicit workflow ID in a stateless or horizontally
  scaled gateway.
- ADK Web is a development surface. Its local session database is
  container-local; replace it with a supported durable session service.
- `TemporalAdkSessionWorkflow` currently has no idle close or
  Continue-As-New. Its history grows across turns, and one approval-blocked turn
  serializes later turns behind it. Bound session lifetime and history before
  production use.
- Queries require a compatible worker; closed histories are retention-bound.
  Lifecycle tools without a Run ID address the latest run.
- The approval dashboard scans workflow visibility results and does not page or
  export a complete audit record. Send audit events to a compliance store in
  production.
- Continue-As-New input shares Temporal's payload limits. Offload large or old
  operation data rather than carrying it indefinitely.
- Dependency files use version floors, and the Temporal CLI image downloads the
  current CLI during build. Pin and lock the full software supply chain for
  reproducible releases.

## Repository map

| Path | Responsibility |
| --- | --- |
| [`common/models.py`](common/models.py) | Shared request, response, operation, callback, and workflow dataclasses |
| [`workflows/chain.py`](workflows/chain.py) | Scenario 1 and 2 entity workflow, Signals, Queries, Continue-As-New, and idle close |
| [`workflows/autonomous_agent.py`](workflows/autonomous_agent.py) | Scenario 3 approval checkpoint, protected action, dependent action, and callback |
| [`workflows/adk_session.py`](workflows/adk_session.py) | Long-running Temporal-owned ADK runner and turn Updates |
| [`activities/gateway_activities.py`](activities/gateway_activities.py) | Policy evaluation and idempotent downstream invocation |
| [`gateway/server.py`](gateway/server.py) | MCP tools, correlation, authentication, response strategies, and approval dashboard |
| [`mock_tool/server.py`](mock_tool/server.py) | In-memory deployment backend |
| [`adk_agents/release_approval_agent/`](adk_agents/release_approval_agent/) | Dashy prompt, Temporal ADK plugin integration, and Web proxy |
| [`adk_agents/run_temporal_session.py`](adk_agents/run_temporal_session.py) | CLI for the same long-running ADK session workflow |
| [`worker.py`](worker.py) | Workflow, Activity, and Google ADK plugin registration |
| [`tests/`](tests/) | Workflow scenarios, gateway contract tests, and ADK session/proxy tests |
| [`WALKTHROUGH.md`](WALKTHROUGH.md) | Step-by-step live demonstration |
