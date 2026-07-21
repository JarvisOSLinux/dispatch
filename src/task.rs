use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// Task state machine: Init → Running → Exit | Killed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Running,
    Exited,
    Killed,
}

fn default_fire_wake() -> bool {
    true
}

/// A task definition as received from the LLM (MCP server call).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDef {
    pub server: String,
    pub tool: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub remind_after: Option<u64>,
    /// Wake the LLM when this task exits, even if other tasks are still running.
    /// Default: true. Set to false for fire-and-forget background tasks where
    /// you don't need per-task wakeups — the LLM will only be woken when all
    /// fire_wake=false tasks finish together or a reminder fires.
    #[serde(default = "default_fire_wake")]
    pub fire_wake: bool,
    /// Store output out-of-band instead of inlining it in the EXIT signal.
    /// Use for large payloads where inline content would bloat the signal window.
    #[serde(default)]
    pub defer_output: bool,
}

/// A timer definition as received from the LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerDef {
    pub label: String,
    pub duration: u64,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// What kind of task this is.
#[derive(Debug)]
pub enum TaskKind {
    Mcp(TaskDef),
    Timer(TimerDef),
}

/// A live task being tracked by the orchestrator.
#[derive(Debug)]
pub struct Task {
    pub pid: u64,
    pub kind: TaskKind,
    pub state: TaskState,
    pub started_at: Instant,
    /// Provenance nonce for MCP tasks; None for timers (no external output).
    pub nonce: Option<String>,
    /// Handle to cancel the running task.
    pub abort_handle: Option<tokio::task::AbortHandle>,
}

impl Task {
    pub fn new_mcp(pid: u64, def: TaskDef) -> Self {
        Self {
            pid,
            kind: TaskKind::Mcp(def),
            state: TaskState::Running,
            started_at: Instant::now(),
            nonce: Some(crate::nonce::generate()),
            abort_handle: None,
        }
    }

    pub fn new_timer(pid: u64, def: TimerDef) -> Self {
        Self {
            pid,
            kind: TaskKind::Timer(def),
            state: TaskState::Running,
            started_at: Instant::now(),
            nonce: None,
            abort_handle: None,
        }
    }

    pub fn is_running(&self) -> bool {
        self.state == TaskState::Running
    }

    pub fn mark_exited(&mut self) {
        self.state = TaskState::Exited;
    }

    pub fn mark_killed(&mut self) {
        self.state = TaskState::Killed;
    }

    /// Short description for signal messages.
    pub fn description(&self) -> String {
        match &self.kind {
            TaskKind::Mcp(def) => {
                let params_str = if def.params.is_null() || def.params == serde_json::json!({}) {
                    String::new()
                } else {
                    format!(" {}", def.params)
                };
                format!("{}/{}{}", def.server, def.tool, params_str)
            }
            TaskKind::Timer(def) => {
                format!("timer \"{}\" ({}s)", def.label, def.duration)
            }
        }
    }

    /// For MCP tasks, return the TaskDef. Panics if called on a timer.
    pub fn mcp_def(&self) -> &TaskDef {
        match &self.kind {
            TaskKind::Mcp(def) => def,
            TaskKind::Timer(_) => panic!("mcp_def() called on a timer task"),
        }
    }

    /// For timer tasks, return the TimerDef. Panics if called on an MCP task.
    pub fn timer_def(&self) -> &TimerDef {
        match &self.kind {
            TaskKind::Timer(def) => def,
            TaskKind::Mcp(_) => panic!("timer_def() called on an MCP task"),
        }
    }
}

/// Status snapshot of a task, suitable for serialization.
#[derive(Debug, Serialize)]
pub struct TaskStatus {
    pub pid: u64,
    #[serde(flatten)]
    pub kind: TaskStatusKind,
    pub state: TaskState,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum TaskStatusKind {
    #[serde(rename = "mcp")]
    Mcp { server: String, tool: String },
    #[serde(rename = "timer")]
    Timer {
        label: String,
        /// Seconds until the timer fires (0 if already fired).
        fires_in: u64,
    },
}

impl From<&Task> for TaskStatus {
    fn from(task: &Task) -> Self {
        let kind = match &task.kind {
            TaskKind::Mcp(def) => TaskStatusKind::Mcp {
                server: def.server.clone(),
                tool: def.tool.clone(),
            },
            TaskKind::Timer(def) => {
                let elapsed = task.started_at.elapsed().as_secs();
                let fires_in = def.duration.saturating_sub(elapsed);
                TaskStatusKind::Timer {
                    label: def.label.clone(),
                    fires_in,
                }
            }
        };
        Self {
            pid: task.pid,
            kind,
            state: task.state.clone(),
        }
    }
}
