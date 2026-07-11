use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::mcp_client::DmcpClient;
use crate::orchestrator::Orchestrator;
use crate::task::{TaskDef, TimerDef};

// --- JSON-RPC types ---

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

// --- MCP protocol constants ---

const MCP_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "dispatch";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

// --- Tool definitions ---

fn tool_definitions() -> Value {
    json!([
        {
            "name": "dispatch",
            "description": "Dispatch a list of tasks for concurrent execution via MCP servers. Each task specifies a server, tool, parameters, and optional settings. Blocks until any task signals (EXIT, REMIND, or KILL), then returns the signal window. The LLM receives each task's output as it arrives and can loop back with 'wait' for remaining tasks. Strategy is prepended to every wakeup response if set.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "server": { "type": "string", "description": "MCP server ID (as known to dmcp)" },
                                "tool": { "type": "string", "description": "Tool name on the server" },
                                "params": { "type": "object", "description": "Parameters to pass to the tool" },
                                "remind_after": { "type": "integer", "description": "Seconds before firing a reminder (omit for no reminder)" },
                                "fire_wake": {
                                    "type": "boolean",
                                    "description": "Default: true. Set to false for fire-and-forget background tasks — suppresses per-task wakeup so the LLM is only woken when all fire_wake=false tasks complete together or a reminder fires."
                                },
                                "defer_output": {
                                    "type": "boolean",
                                    "description": "If true, store output out-of-band and show only '[hash=h] 200 (deferred)' in the EXIT signal. Use for large payloads to keep the signal window compact. Default: false (output inlined as '[hash=h] 200 <h>output</h>')."
                                }
                            },
                            "required": ["server", "tool"]
                        },
                        "description": "List of tasks to dispatch concurrently"
                    },
                    "strategy": {
                        "type": "string",
                        "description": "Current goal state and workflow plan. Stored in the orchestrator and prepended to every LLM wakeup response for this session. Update it on each dispatch call to keep context across wakeup cycles."
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Optional session identifier (e.g. the goal ID from the orchestrating LLM). When provided, the signal window returned on each wakeup is filtered to only show signals for PIDs belonging to this session — preventing historical entries from earlier goals from polluting the LLM context."
                    }
                },
                "required": ["tasks"]
            }
        },
        {
            "name": "kill",
            "description": "Terminate running tasks by PID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pids": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "description": "PIDs of tasks to kill"
                    }
                },
                "required": ["pids"]
            }
        },
        {
            "name": "wait",
            "description": "Acknowledge a reminder and let the task continue running. A new reminder will fire after the same interval. After acknowledging, blocks until the next event (task completion or reminder).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pids": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "description": "PIDs of tasks to continue waiting on"
                    }
                },
                "required": ["pids"]
            }
        },
        {
            "name": "status",
            "description": "Get current state of all active tasks.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "log",
            "description": "Get the signal window (last N entries).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                        "description": "Number of entries to return (default: 20)"
                    }
                }
            }
        },
        {
            "name": "get_output",
            "description": "Retrieve the full output from one or more completed MCP tasks. By default, output is already inlined in the EXIT signal as '[hash=h] 200 <h>output</h>' — use this tool to re-read output, or to retrieve output from tasks that used defer_output: true (those show only '[hash=h] 200 (deferred)' in the signal window). Failed tasks (500) have no stored output; check the signal log for error details.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pids": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "description": "PIDs of completed tasks to retrieve output for. Multiple PIDs can be fetched in one call."
                    }
                },
                "required": ["pids"]
            }
        },
        {
            "name": "timer",
            "description": "Set a one-shot timer. Fires a REMIND signal after the specified duration, then exits. No MCP server involved — pure delay. Use for goal deferral, scheduled re-checks, or any timed reminder. Killable via the kill tool.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "label": {
                        "type": "string",
                        "description": "Human-readable label for the timer (appears in signals and status)"
                    },
                    "duration": {
                        "type": "integer",
                        "description": "Seconds until the REMIND signal fires"
                    },
                    "metadata": {
                        "type": "object",
                        "description": "Arbitrary key-value data passed through to the REMIND signal (opaque to dispatch)"
                    }
                },
                "required": ["label", "duration"]
            }
        },
        {
            "name": "browse_servers",
            "description": "Search the MCP server registry using a vector (embedding). Returns the top-k most similar servers above the minimum score threshold.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "vector": {
                        "type": "array",
                        "items": { "type": "number" },
                        "description": "Query embedding vector"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    },
                    "min_score": {
                        "type": "number",
                        "description": "Minimum similarity score threshold (0.0–1.0)"
                    }
                },
                "required": ["vector", "top_k", "min_score"]
            }
        },
        {
            "name": "browse_servers_batch",
            "description": "Search the MCP server registry with multiple vectors in a single call. Returns results grouped by input vector index. Use when dispatching a plan with multiple sub-tasks that each need server discovery.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "vectors": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": { "type": "number" }
                        },
                        "description": "Array of query embedding vectors"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Maximum number of results per vector"
                    },
                    "min_score": {
                        "type": "number",
                        "description": "Minimum similarity score threshold (0.0–1.0)"
                    }
                },
                "required": ["vectors", "top_k", "min_score"]
            }
        },
        {
            "name": "server_count",
            "description": "Get the number of MCP servers visible in the registry. Use this to decide whether to use vector or keyword search.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "embedding_spec",
            "description": "Get the embedding model specification used by the registry (model name, version, dimensions). Use this to produce vectors compatible with the registry's index.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "sync_index",
            "description": "Refresh the local vector index from the registry. Run this before vector searches when the index may be stale.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "index_server",
            "description": "Add a non-approved server to the local vector index so it can be found via vector search.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "server_id": {
                        "type": "string",
                        "description": "ID of the server to index"
                    },
                    "server": {
                        "type": "array",
                        "items": { "type": "number" },
                        "description": "Server-level embedding vector"
                    },
                    "tools": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "array",
                            "items": { "type": "number" }
                        },
                        "description": "Optional per-tool embedding vectors, keyed by tool name"
                    }
                },
                "required": ["server_id", "server"]
            }
        }
    ])
}

// --- Server ---

pub async fn serve() -> io::Result<()> {
    let orchestrator = Arc::new(Mutex::new(Orchestrator::new()));

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                warn!("JSON-RPC parse error: {}", e);
                let resp =
                    JsonRpcResponse::error(Value::Null, -32700, format!("Parse error: {}", e));
                write_response(&mut stdout, &resp).await?;
                continue;
            }
        };

        let _ = &request.jsonrpc;

        let id = request.id.clone().unwrap_or(Value::Null);
        debug!(method = %request.method, "received JSON-RPC request");

        let response = match request.method.as_str() {
            "initialize" => {
                info!("client initializing");
                handle_initialize(id)
            }

            "notifications/initialized" => {
                info!("client initialized");
                continue;
            }

            "tools/list" => handle_tools_list(id),

            "tools/call" => handle_tools_call(id, request.params, Arc::clone(&orchestrator)).await,

            "ping" => JsonRpcResponse::success(id, json!({})),

            _ => {
                if request.id.is_none() {
                    continue;
                }
                warn!(method = %request.method, "unknown method");
                JsonRpcResponse::error(id, -32601, "Method not found")
            }
        };

        write_response(&mut stdout, &response).await?;
    }

    orchestrator.lock().await.shutdown();
    Ok(())
}

fn handle_initialize(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        json!({
            "protocolVersion": MCP_VERSION,
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION
            }
        }),
    )
}

fn handle_tools_list(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        json!({
            "tools": tool_definitions()
        }),
    )
}

async fn handle_tools_call(
    id: Value,
    params: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    info!(tool = tool_name, "tools/call");
    match tool_name {
        "dispatch" => handle_dispatch(id, arguments, orchestrator).await,
        "kill" => handle_kill(id, arguments, orchestrator).await,
        "wait" => handle_wait(id, arguments, orchestrator).await,
        "status" => handle_status(id, orchestrator).await,
        "log" => handle_log(id, arguments, orchestrator).await,
        "get_output" => handle_get_output(id, arguments, orchestrator).await,
        "timer" => handle_timer(id, arguments, orchestrator).await,
        "browse_servers" => handle_browse_servers(id, arguments).await,
        "browse_servers_batch" => handle_browse_servers_batch(id, arguments).await,
        "server_count" => handle_server_count(id).await,
        "embedding_spec" => handle_embedding_spec(id).await,
        "sync_index" => handle_sync_index(id).await,
        "index_server" => handle_index_server(id, arguments).await,
        _ => {
            warn!(tool = tool_name, "unknown tool called");
            JsonRpcResponse::error(id, -32602, format!("Unknown tool: {}", tool_name))
        }
    }
}

async fn handle_dispatch(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let tasks: Vec<TaskDef> = match arguments.get("tasks") {
        Some(tasks_val) => match serde_json::from_value(tasks_val.clone()) {
            Ok(t) => t,
            Err(e) => {
                return JsonRpcResponse::error(id, -32602, format!("Invalid tasks: {}", e));
            }
        },
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing 'tasks' parameter");
        }
    };

    if tasks.is_empty() {
        return JsonRpcResponse::error(id, -32602, "Empty task list");
    }

    let strategy = arguments
        .get("strategy")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let session_id = arguments
        .get("session_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Fire-and-return: dispatch synchronously enqueues INIT signals, then we
    // render the current window and return immediately. We do NOT block on
    // wait_for_event — the daemon captures INIT from this return and receives
    // later EXIT/REMIND signals via its signal-window poller (#22). The lock is
    // held only across synchronous calls, never across an await, so a
    // concurrent kill/status/log is instantly serviceable.
    let mut orch = orchestrator.lock().await;
    let pids = orch.dispatch(tasks, strategy, session_id);
    let window = orch.drain_and_context();
    drop(orch);

    JsonRpcResponse::success(
        id,
        json!({
            "content": [{
                "type": "text",
                "text": window
            }],
            "pids": pids
        }),
    )
}

async fn handle_kill(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let pids: Vec<u64> = match arguments.get("pids") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, -32602, format!("Invalid pids: {}", e));
            }
        },
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing 'pids' parameter");
        }
    };

    let mut orch = orchestrator.lock().await;
    match orch.kill(&pids) {
        Ok(killed) => {
            let window = orch.log_text(20);
            JsonRpcResponse::success(
                id,
                json!({
                    "content": [{
                        "type": "text",
                        "text": window
                    }],
                    "killed": killed
                }),
            )
        }
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn handle_wait(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let pids: Vec<u64> = match arguments.get("pids") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, -32602, format!("Invalid pids: {}", e));
            }
        },
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing 'pids' parameter");
        }
    };

    {
        let mut orch = orchestrator.lock().await;
        if let Err(e) = orch.wait(&pids) {
            return JsonRpcResponse::error(id, -32000, e.to_string());
        }
    }

    // `wait` deliberately blocks until the next event: the LLM (via the daemon's
    // `wait` action) asks to block until these PIDs complete, and the caller
    // consumes the returned EXIT signals. Unlike `dispatch`/`timer` this is an
    // intentional block, not incidental contention; making it fire-and-return
    // requires the coordinated daemon change tracked in #22 (consume completion
    // from the poller/queue instead of this return).
    let signal_window = orchestrator.lock().await.wait_for_event().await;

    match signal_window {
        Ok(window) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": window
                }],
                "waited": pids
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, format!("Dispatch error: {}", e)),
    }
}

async fn handle_status(id: Value, orchestrator: Arc<Mutex<Orchestrator>>) -> JsonRpcResponse {
    let mut orch = orchestrator.lock().await;
    orch.drain_results();
    let statuses = orch.status();
    JsonRpcResponse::success(
        id,
        json!({
            "content": [{
                "type": "text",
                "text": serde_json::to_string_pretty(&statuses).unwrap_or_default()
            }],
            "tasks": statuses
        }),
    )
}

async fn handle_log(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let count = arguments
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(20) as usize;

    let mut orch = orchestrator.lock().await;
    orch.drain_results();
    let window_text = orch.log_text(count);
    let window_json = orch.log_json(count);

    JsonRpcResponse::success(
        id,
        json!({
            "content": [{
                "type": "text",
                "text": window_text
            }],
            "entries": window_json
        }),
    )
}

async fn handle_get_output(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let pids: Vec<u64> = match arguments.get("pids") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, -32602, format!("Invalid pids: {}", e));
            }
        },
        None => return JsonRpcResponse::error(id, -32602, "Missing 'pids' parameter"),
    };

    if pids.is_empty() {
        return JsonRpcResponse::error(id, -32602, "Empty pids list");
    }

    let mut orch = orchestrator.lock().await;
    orch.drain_results();
    let mut text_parts: Vec<String> = Vec::new();
    let mut outputs_map = serde_json::Map::new();

    for &pid in &pids {
        match orch.get_output(pid) {
            Some(out) => {
                let header = match orch.get_nonce(pid) {
                    Some(h) => format!("PID {} [hash={}]", pid, h),
                    None => format!("PID {}", pid),
                };
                text_parts.push(format!("{}\n{}", header, out));
                outputs_map.insert(
                    pid.to_string(),
                    json!({
                        "hash": orch.get_nonce(pid),
                        "output": out
                    }),
                );
            }
            None => {
                text_parts.push(format!(
                    "PID {}: no stored output (task failed or PID unknown — check signal log)",
                    pid
                ));
                outputs_map.insert(pid.to_string(), Value::Null);
            }
        }
    }

    JsonRpcResponse::success(
        id,
        json!({
            "content": [{"type": "text", "text": text_parts.join("\n\n---\n\n")}],
            "outputs": outputs_map
        }),
    )
}

async fn handle_timer(
    id: Value,
    arguments: Value,
    orchestrator: Arc<Mutex<Orchestrator>>,
) -> JsonRpcResponse {
    let label = match arguments.get("label").and_then(|v| v.as_str()) {
        Some(l) => l.to_string(),
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing 'label' parameter");
        }
    };

    let duration = match arguments.get("duration").and_then(|v| v.as_u64()) {
        Some(d) => d,
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing or invalid 'duration' parameter");
        }
    };

    let metadata = arguments.get("metadata").cloned();

    let def = TimerDef {
        label,
        duration,
        metadata,
    };

    // Fire-and-return: dispatch_timer synchronously enqueues the INIT signal;
    // the REMIND fires later and is delivered via the daemon's poller (#22).
    let mut orch = orchestrator.lock().await;
    let pid = orch.dispatch_timer(def);
    let window = orch.drain_and_context();
    drop(orch);

    JsonRpcResponse::success(
        id,
        json!({
            "content": [{
                "type": "text",
                "text": window
            }],
            "pid": pid
        }),
    )
}

async fn handle_browse_servers(id: Value, arguments: Value) -> JsonRpcResponse {
    let vector: Vec<f64> = match arguments.get("vector") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(vec) => vec,
            Err(e) => return JsonRpcResponse::error(id, -32602, format!("Invalid vector: {}", e)),
        },
        None => return JsonRpcResponse::error(id, -32602, "Missing 'vector' parameter"),
    };
    let top_k = match arguments.get("top_k").and_then(|v| v.as_u64()) {
        Some(n) => n,
        None => return JsonRpcResponse::error(id, -32602, "Missing or invalid 'top_k' parameter"),
    };
    let min_score = match arguments.get("min_score").and_then(|v| v.as_f64()) {
        Some(f) => f,
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing or invalid 'min_score' parameter")
        }
    };

    match DmcpClient::browse_vector(&vector, top_k, min_score).await {
        Ok(results) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": results.to_string() }],
                "results": results
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn handle_browse_servers_batch(id: Value, arguments: Value) -> JsonRpcResponse {
    let vectors: Vec<Vec<f64>> = match arguments.get("vectors") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(vecs) => vecs,
            Err(e) => return JsonRpcResponse::error(id, -32602, format!("Invalid vectors: {}", e)),
        },
        None => return JsonRpcResponse::error(id, -32602, "Missing 'vectors' parameter"),
    };
    let top_k = match arguments.get("top_k").and_then(|v| v.as_u64()) {
        Some(n) => n,
        None => return JsonRpcResponse::error(id, -32602, "Missing or invalid 'top_k' parameter"),
    };
    let min_score = match arguments.get("min_score").and_then(|v| v.as_f64()) {
        Some(f) => f,
        None => {
            return JsonRpcResponse::error(id, -32602, "Missing or invalid 'min_score' parameter")
        }
    };

    match DmcpClient::browse_vectors(&vectors, top_k, min_score).await {
        Ok(results) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": results.to_string() }],
                "results": results
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn handle_server_count(id: Value) -> JsonRpcResponse {
    match DmcpClient::server_count().await {
        Ok(count) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": count.to_string() }],
                "count": count
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn handle_embedding_spec(id: Value) -> JsonRpcResponse {
    match DmcpClient::embedding_spec().await {
        Ok(spec) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": spec.to_string() }],
                "spec": spec
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn handle_sync_index(id: Value) -> JsonRpcResponse {
    match DmcpClient::sync_index().await {
        Ok(output) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": output }]
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

/// Build the `--vectors` payload dmcp expects: `{"server": [...], "tools": {...}}`.
/// Returns an error message if the server vector is missing or not an array.
fn index_payload_from_args(arguments: &Value) -> std::result::Result<Value, String> {
    let server_vec = match arguments.get("server") {
        Some(v) if v.is_array() => v.clone(),
        _ => return Err("Missing or invalid 'server' vector".to_string()),
    };
    let tools = arguments.get("tools").cloned().unwrap_or_else(|| json!({}));
    Ok(json!({ "server": server_vec, "tools": tools }))
}

async fn handle_index_server(id: Value, arguments: Value) -> JsonRpcResponse {
    let server_id = match arguments.get("server_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return JsonRpcResponse::error(id, -32602, "Missing 'server_id' parameter"),
    };
    let payload = match index_payload_from_args(&arguments) {
        Ok(p) => p,
        Err(e) => return JsonRpcResponse::error(id, -32602, e),
    };

    match DmcpClient::index_server(&server_id, &payload).await {
        Ok(output) => JsonRpcResponse::success(
            id,
            json!({
                "content": [{ "type": "text", "text": output }]
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
    }
}

async fn write_response(stdout: &mut io::Stdout, response: &JsonRpcResponse) -> io::Result<()> {
    let json = serde_json::to_string(response).unwrap();
    stdout.write_all(json.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_payload_is_dmcp_object_shape() {
        // dmcp reads .get("server") and .get("tools"); the payload must be an
        // object, not the array-of-arrays that always failed dmcp's parser.
        let args = json!({"server_id": "x", "server": [0.1, 0.2], "tools": {"t": [0.3]}});
        let p = index_payload_from_args(&args).unwrap();
        assert!(p.is_object());
        assert!(p.get("server").unwrap().is_array());
        assert!(p.get("tools").unwrap().is_object());
    }

    #[test]
    fn index_payload_defaults_tools_and_requires_server() {
        let p = index_payload_from_args(&json!({"server": [1.0]})).unwrap();
        assert_eq!(p["tools"], json!({}));
        assert!(index_payload_from_args(&json!({})).is_err());
        assert!(index_payload_from_args(&json!({"server": "not-an-array"})).is_err());
    }
}
