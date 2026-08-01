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
├── tail.rs           Bounded per-task live stderr ring (64 KiB) feeding REMIND/status tails
├── pid.rs            Internal PID assignment and tracking
├── reminder.rs       Timer-based reminder system
├── mcp_client.rs     Client for calling dmcp and MCP servers
├── mcp_server.rs     MCP server interface (JSON-RPC 2.0 over stdio)
├── nonce.rs          128-bit output-provenance boundary nonces (CSPRNG) wrapping tool output in EXIT signals
├── tool_meta.rs      Per-tool manifest metadata (`blocking`/`suggestedRemindAfter`) + the reminder policy derived from it
└── error.rs          Custom error types
```

### MCP Tools Exposed

| Tool | Purpose |
|------|---------|
| `dispatch` | Dispatch a list of tasks for concurrent execution (per-task `remind_after`/`fire_wake`/`defer_output`/`stateful`; top-level `strategy`/`session_id`). `remind_after`: positive = the caller's interval, `0` = opt out, omitted = no preference, in which case a tool whose manifest marks it `blocking` gets its `suggestedRemindAfter` (or the 30s built-in default), disclosed on INIT and in `status` |
| `kill` | Terminate running tasks by PID |
| `wait` | Acknowledge reminder, keep task running |
| `status` | Get current state of all active tasks (optional `tail`: latest N chars of each running task's live stderr, provenance-wrapped; `auto_remind_after` on any task whose reminder dispatch armed itself) |
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
| `REMIND` | Task running beyond its reminder interval — the caller's `remind_after` or a blocking tool's auto-armed one (MCP tasks: carries the latest 4096 chars of live stderr, provenance-wrapped) |
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

*2026-08-01:* reminders for tools that can park awaiting input (#38, mcp-registry #68). `tool_meta.rs` added — `ToolMeta` (the per-tool `blocking` / `suggestedRemindAfter` a server manifest may declare) plus `plan_reminder`, the whole policy as one pure function: caller `Some(n>0)` wins, caller `Some(0)` is an **explicit opt-out** that wins too, and only an *absent* `remind_after` is treated as no preference — the sole case in which a blocking tool's suggestion, or `DEFAULT_BLOCKING_REMIND_SECS` (30) when it names none, is armed. Previously such a task produced no signal at all: the LLM went idle believing work was in flight while the tool sat on a question nobody could see. dispatch had **no manifest-derived metadata path at all** — `TaskDef.stateful` looks like one but is caller-supplied — so this adds the first: `DmcpClient::server_manifest` (`dmcp info <id> --json`, a local read of the installed manifest) feeding `mcp_server::prepare_tasks`, which resolves each distinct server once, concurrently, **before** the orchestrator lock and **only** for tasks that supplied no `remind_after` (for the rest the manifest could not change the outcome). The pair travels as `PreparedTask { def, tool_meta }` rather than being folded into `TaskDef`, keeping the LLM's request and the server author's declaration distinguishable. An unreadable manifest yields no metadata, i.e. the pre-change behavior — a manifest read never fails a dispatch. Injected behavior is never silent: the INIT message carries `[auto-remind Ns — tool declares blocking, …]`, `TaskStatus.auto_remind_after` reports it (skipped when absent, so an ordinary task's status stays byte-identical), and an `info!` records it. A blocking tool cannot silence itself either — a `suggestedRemindAfter` of 0, negative, or non-integer is no suggestion, so the built-in default applies. Verified by driving the real `dispatch serve` against a fake `dmcp` that serves manifests from `info` and a `run_job` tool that asks a question and waits (`tests/blocking_reminder.rs`, harness extracted to `tests/common/`): the auto-armed REMIND is asserted on the **pushed notification**, not on internal state, alongside explicit-interval-wins, opt-out-honored, byte-identical status for a tool that declares nothing, and the built-in-default disclosure.

*2026-07-31:* live output for running tasks (#36). `tail.rs` added — a `TAIL_BUFFER_CAP` (64 KiB) byte ring per running MCP task, fed incrementally from the dmcp child's stderr by a reader spawned beside `wait_with_output` in `mcp_client::call_tool` (stdout stays the result wire, read at exit; both pipes still drain concurrently). REMIND now carries the newest `REMIND_TAIL_CHARS` (4096) characters of that ring, wrapped `[hash=h] <h>…</h>` exactly like EXIT output — the tail is the same tool-authored, untrusted class of data — but under a **fresh nonce per emission**: the task's EXIT nonce seals output the tool is still producing, and disclosing it mid-run would let a tool that learns it forge boundaries in bytes it has yet to write. `status {"tail": n}` returns the newest n characters per running task (`tail`/`tail_hash` fields, same treatment, clamped to the ring); without `tail` the status response is regression-tested byte-identical to before. Tail extraction counts characters over a lossy decode, cut on char boundaries — the ring drops oldest *bytes*, so a partial sequence at its front renders U+FFFD, and a cut never splits a multi-byte char. The ring is dropped when the task settles (EXIT/KILL) — EXIT already carries the real output. Failure detail still prefers stdout and falls back to stderr, now read from the ring (identical up to the cap). Verified end to end by driving the real `dispatch serve` over stdio JSON-RPC against a fake `dmcp` on PATH (`tests/fixtures/fake_dmcp.sh`) that writes stderr mid-run: the pushed REMIND notification itself carries the wrapped tail (a recorded-but-never-emitted signal fails these tests), status tail present while running / absent after, byte-identical no-tail status, EXIT/`get_output` unchanged on success and both failure-detail arms.

*2026-07-22:* MCP tools table extended to the full 13 tools; `nonce.rs` description corrected (output-provenance boundary nonces, not JSON-RPC); getrandom/libc/windows-sys added to the tech stack.
