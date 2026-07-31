# CLAUDE.md — dispatch

## What This Is

Signal-driven task orchestrator for MCP servers. One LLM dispatches multiple
concurrent tool calls, then goes idle. dispatch runs those tasks in parallel
and only wakes the LLM when a signal arrives — a task completes, fails, or
needs attention.

Core principle: **one brain, many hands.** Multi-agent-level parallelism
without loading multiple LLM instances.

## Role in the JARVIS Ecosystem

dispatch is the execution engine. Project-JARVIS's Python adapter
(`jarvis/dispatch/adapter.py`) wraps the dispatch binary, manages its
subprocess lifecycle, translates Python calls into MCP JSON-RPC, and surfaces
signals back to the JARVIS event loop.

dispatch depends on dmcp (must be on PATH) for MCP server discovery and
invocation.

```
LLM → dispatch (orchestrator) → dmcp (server manager) → MCP servers
```

## Tech Stack

- Rust (2021 edition), Tokio async runtime
- tokio::sync::mpsc for signal queue
- tokio::process::Command for MCP child processes
- serde / serde_json for JSON-RPC
- tracing / tracing-subscriber for structured logging
- chrono for time management
- thiserror for error types
- getrandom for provenance-nonce generation
- libc (Unix) / windows-sys (Windows) for process-group / Job Object teardown — killing a task kills the entire dmcp → MCP-server subtree

## Architecture

```
src/
├── main.rs           CLI entry point (dispatch serve)
├── lib.rs            Module declarations
├── orchestrator.rs   Core event loop: task spawning, signal routing, reminders
├── task.rs           Task state machine (Init → Running → Exit/Killed)
├── signal.rs         Signal types + rolling signal window (last 20 entries)
├── pid.rs            Internal PID assignment and tracking
├── reminder.rs       Timer-based reminder system
├── mcp_client.rs     Client for calling dmcp and MCP servers
├── mcp_server.rs     MCP server interface (JSON-RPC 2.0 over stdio)
├── nonce.rs          128-bit output-provenance boundary nonces (CSPRNG) wrapping tool output in EXIT signals
└── error.rs          Custom error types
```

### MCP Tools Exposed

| Tool | Purpose |
|------|---------|
| `dispatch` | Dispatch a list of tasks for concurrent execution (per-task `remind_after`/`fire_wake`/`defer_output`; top-level `strategy`/`session_id`) |
| `respond` | Answer a question a task's server asked (accept/decline/cancel), resuming a parked task |
| `kill` | Terminate running tasks by PID |
| `wait` | Acknowledge reminder, keep task running |
| `status` | Get current state of all active tasks |
| `log` | Get signal window (last N entries, default 20) |
| `get_output` | Retrieve full output from completed tasks (incl. `defer_output` tasks) |
| `timer` | Set a one-shot timer that fires REMIND signal |
| `browse_servers` | Vector-search the MCP registry index |
| `browse_servers_batch` | Batch vector search (many queries, one call) |
| `server_count` | Number of servers in the registry index |
| `embedding_spec` | Embedding model/version the index expects |
| `sync_index` | Sync the local vector index with installed servers |
| `index_server` | Add/update one server in the vector index |

### Signal Types

| Signal | Meaning |
|--------|---------|
| `INIT` | Task started |
| `EXIT` | Task finished (success or failure) |
| `REMIND` | Task running beyond timeout |
| `WAIT` | LLM acknowledged reminder |
| `KILL` | Task terminated |
| `NEEDS_ACTION` | A task's server asked for input mid-call; the task is parked, alive, awaiting `respond` |

### Mid-execution interaction (NEEDS_ACTION)

Some tools cannot go quiet and cannot be answered up front — `fdisk`,
`mysql_secure_installation`, a REPL — because they ask a **sequence** of
questions that only appears as the work unfolds. MCP's `elicitation/create`
carries these: the tool call does not return while a question is outstanding, so
the server stays alive and blocked.

A **session task** (stateful + `session_id`) therefore runs through
`dmcp call --session <sid> --interactive`, whose stdout is a tagged JSON stream.
`DmcpClient::call_tool_interactive` parses it, relays each `prompt` to the
orchestrator, and writes the answer back on dmcp's stdin. Non-session tasks call
exactly as before.

The task then enters **`TaskState::Waiting`** and a `NEEDS_ACTION` signal is
emitted. `Waiting` is a phase of running, not an end state: `Task::is_running`
counts it, so a parked task is never reaped, produces no output, emits no EXIT,
and **holds its session open** — settling it would tear down the very server that
is waiting to be answered. `respond` (pid + accept/decline/cancel + content)
delivers the answer and returns the task to `Running`.

The prompt pump runs **concurrently** with the in-flight call (biased select),
for the same reason it does inside dmcp: the server is blocked until answered, so
awaiting the call first would deadlock against the answer needed to finish it.

**The question text belongs to the server, not to JARVIS.** Every
`NEEDS_ACTION` payload carries the asking server's id and `untrusted: true`, and
the `respond` tool description says so — a community server can phish through
this channel, so it is never rendered as the assistant asking, and a
credential-shaped prompt is declined here and left to a human. dispatch only
carries the answer; whether an LLM may give one at all is the daemon's call under
`CONFIRMATION_MODE`.

Every breakdown resolves to a **decline** rather than a hang — a vanished task, a
dropped channel, an unparsable line — because a decline is a real protocol
outcome the server already handles, while silence parks the session forever.

## Build & Test

```bash
cargo build --release
cargo test
cargo clippy
cargo fmt --check
```

## Run

```bash
dispatch serve                          # Run as MCP server (stdio)
RUST_LOG=dispatch=debug dispatch serve  # With debug logging
```

## Conventions

- `cargo fmt` + `cargo clippy` clean before pushing
- Commit messages: imperative mood
- No comments explaining what code does; only non-obvious WHY

## Changelog — corrected claims

*2026-07-30:* mid-execution interaction (Project-JARVIS#210, dispatch half). `SignalKind::NeedsAction` (wire name pinned to `NEEDS_ACTION`, since `rename_all = "UPPERCASE"` would emit `NEEDSACTION` and diverge from `Display`) and `TaskState::Waiting`, which `Task::is_running` counts so a parked task keeps its slot and its session. `DmcpClient::call_tool_interactive` drives `dmcp call --session --interactive` and parses its tagged JSON stream; the orchestrator pumps prompts concurrently with the call (biased select), registers the answer channel in `pending_answers`, and emits a `NEEDS_ACTION` signal attributing the question to the asking server with `untrusted: true`. New `respond` MCP tool (accept/decline/cancel) resumes the task; a stale or duplicate answer is a reported error, not a panic, and a question for a task that has gone is declined so the server unblocks. 41 tests pass.

*2026-07-22:* MCP tools table extended to the full 13 tools; `nonce.rs` description corrected (output-provenance boundary nonces, not JSON-RPC); getrandom/libc/windows-sys added to the tech stack.
