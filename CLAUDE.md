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

*2026-07-22:* MCP tools table extended to the full 13 tools; `nonce.rs` description corrected (output-provenance boundary nonces, not JSON-RPC); getrandom/libc/windows-sys added to the tech stack.
