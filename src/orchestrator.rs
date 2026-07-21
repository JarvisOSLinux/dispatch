use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::error::{DispatchError, Result};
use crate::mcp_client::DmcpClient;
use crate::pid;
use crate::reminder::{ReminderEvent, ReminderManager};
use crate::signal::{SignalEntry, SignalKind, SignalWindow};
use crate::task::{Task, TaskDef, TaskKind, TaskState, TaskStatus, TimerDef};

const DEFAULT_WINDOW_SIZE: usize = 20;
const REMINDER_CHANNEL_SIZE: usize = 64;
const TASK_CHANNEL_SIZE: usize = 64;

struct TaskResult {
    pid: u64,
    kind: TaskResultKind,
}

enum TaskResultKind {
    McpComplete(std::result::Result<String, String>),
    TimerExpired {
        label: String,
        metadata: Option<serde_json::Value>,
        elapsed: u64,
    },
}

/// How the serve loop should push a drained signal (#28).
///
/// `fire_wake` is the sole merge control: `fire_wake=true` signals (and
/// reminders) flow one by one as `Single`; a `fire_wake=false` group, held until
/// its session settles, is merged into one `Batch` so the daemon runs it as a
/// single LLM turn.
pub enum Emission {
    Single(SignalEntry),
    Batch(Vec<SignalEntry>),
}

/// The orchestrator manages tasks, signals, reminders, and completed output.
pub struct Orchestrator {
    tasks: HashMap<u64, Task>,
    signal_window: SignalWindow,
    reminder_mgr: ReminderManager,
    reminder_rx: mpsc::Receiver<ReminderEvent>,
    task_result_rx: mpsc::Receiver<TaskResult>,
    task_result_tx: mpsc::Sender<TaskResult>,
    /// Output store for completed MCP tasks.
    outputs: HashMap<u64, String>,
    /// Current goal strategy set by the LLM via the dispatch tool's `strategy` field.
    strategy: Option<String>,
    /// Maps each dispatched PID to the session_id it belongs to.
    /// Used to scope signal window output per goal/session.
    pid_to_session: HashMap<u64, String>,
    /// The session_id from the most recent dispatch call.
    /// format_wakeup_context() filters the window to this session when set.
    current_session_id: Option<String>,
    /// EXIT signals from fire_wake=false tasks, held until their session settles
    /// (no task in that session still running) and then flushed together, so a
    /// coalesced batch is delivered as a group instead of dropped (#28). Keyed by
    /// session (None = the no-session group).
    held: HashMap<Option<String>, Vec<SignalEntry>>,
    /// Notified whenever a background task result or reminder becomes available,
    /// so the serve loop drains and pushes signals to the LLM immediately
    /// instead of waiting for the next request (#26).
    wake: Arc<Notify>,
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        let (reminder_tx, reminder_rx) = mpsc::channel(REMINDER_CHANNEL_SIZE);
        let (task_result_tx, task_result_rx) = mpsc::channel(TASK_CHANNEL_SIZE);
        let wake = Arc::new(Notify::new());
        Self {
            tasks: HashMap::new(),
            signal_window: SignalWindow::new(DEFAULT_WINDOW_SIZE),
            reminder_mgr: ReminderManager::new(reminder_tx, wake.clone()),
            reminder_rx,
            task_result_rx,
            task_result_tx,
            outputs: HashMap::new(),
            strategy: None,
            pid_to_session: HashMap::new(),
            current_session_id: None,
            held: HashMap::new(),
            wake,
        }
    }

    /// A handle the serve loop awaits (`wake.notified()`) to learn when a
    /// background task result or reminder is ready to drain and push (#26).
    pub fn wake_handle(&self) -> Arc<Notify> {
        self.wake.clone()
    }

    /// Dispatch a batch of MCP tasks for concurrent execution.
    /// `strategy` replaces the current goal strategy if provided.
    /// `session_id` scopes the signal window for this batch — only PIDs
    /// belonging to this session appear in wakeup responses.
    pub fn dispatch(
        &mut self,
        task_defs: Vec<TaskDef>,
        strategy: Option<String>,
        session_id: Option<String>,
    ) -> Vec<u64> {
        if let Some(s) = strategy {
            self.strategy = Some(s);
        }
        if session_id.is_some() {
            self.current_session_id = session_id.clone();
        }
        info!(count = task_defs.len(), "dispatching MCP tasks");
        let mut pids = Vec::with_capacity(task_defs.len());

        for def in task_defs {
            let task_pid = pid::next_pid();
            let remind_after = def.remind_after;
            let mut task = Task::new_mcp(task_pid, def);
            debug!(pid = task_pid, desc = %task.description(), "spawning MCP task");

            if let Some(ref sid) = session_id {
                self.pid_to_session.insert(task_pid, sid.clone());
            }

            self.signal_window.push(SignalEntry::new(
                task_pid,
                SignalKind::Init,
                task.description(),
            ));

            if let Some(secs) = remind_after {
                if secs > 0 {
                    self.reminder_mgr.start(task_pid, secs);
                }
            }

            let tx = self.task_result_tx.clone();
            let wake = self.wake.clone();
            let mcp_def = task.mcp_def();
            let server = mcp_def.server.clone();
            let tool = mcp_def.tool.clone();
            let params = mcp_def.params.clone();

            let join_handle = tokio::spawn(async move {
                let result = DmcpClient::call_tool(&server, &tool, &params).await;
                let output = match result {
                    Ok(stdout) => Ok(stdout),
                    Err(e) => Err(format!("Error: {}", e)),
                };
                let _ = tx
                    .send(TaskResult {
                        pid: task_pid,
                        kind: TaskResultKind::McpComplete(output),
                    })
                    .await;
                wake.notify_one();
            });

            task.abort_handle = Some(join_handle.abort_handle());
            self.tasks.insert(task_pid, task);
            pids.push(task_pid);
        }

        pids
    }

    pub fn dispatch_timer(&mut self, def: TimerDef) -> u64 {
        let task_pid = pid::next_pid();
        info!(pid = task_pid, label = %def.label, duration = def.duration, "dispatching timer");
        let mut task = Task::new_timer(task_pid, def.clone());

        self.signal_window.push(SignalEntry::with_payload(
            task_pid,
            SignalKind::Init,
            task.description(),
            json!({
                "pid": task_pid,
                "type": "INIT",
                "label": def.label,
                "metadata": def.metadata,
                "duration": def.duration,
            }),
        ));

        let tx = self.task_result_tx.clone();
        let wake = self.wake.clone();
        let duration = def.duration;
        let label = def.label.clone();
        let metadata = def.metadata.clone();

        let join_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(duration)).await;
            let _ = tx
                .send(TaskResult {
                    pid: task_pid,
                    kind: TaskResultKind::TimerExpired {
                        label,
                        metadata,
                        elapsed: duration,
                    },
                })
                .await;
            wake.notify_one();
        });

        task.abort_handle = Some(join_handle.abort_handle());
        self.tasks.insert(task_pid, task);
        task_pid
    }

    pub fn kill(&mut self, pids: &[u64]) -> Result<Vec<u64>> {
        debug!(pids = ?pids, "kill requested");
        let mut killed = Vec::new();

        for &task_pid in pids {
            let task = self
                .tasks
                .get_mut(&task_pid)
                .ok_or(DispatchError::TaskNotFound(task_pid))?;

            if !task.is_running() {
                debug!(pid = task_pid, "skip kill — task not running");
                continue;
            }

            if let Some(handle) = task.abort_handle.take() {
                handle.abort();
            }

            let message = match &task.kind {
                TaskKind::Mcp(_) => "Terminated by LLM".to_string(),
                TaskKind::Timer(def) => format!("timer \"{}\" cancelled", def.label),
            };

            task.mark_killed();
            self.reminder_mgr.cancel(task_pid);

            info!(pid = task_pid, %message, "task killed");
            self.signal_window
                .push(SignalEntry::new(task_pid, SignalKind::Kill, message));

            killed.push(task_pid);
        }

        // Killing a session's last running task settles it, so wake the serve
        // loop to flush any held fire_wake=false signals for that session (#28).
        if !killed.is_empty() {
            self.wake.notify_one();
        }

        Ok(killed)
    }

    pub fn wait(&mut self, pids: &[u64]) -> Result<Vec<u64>> {
        let mut waited = Vec::new();

        for &task_pid in pids {
            let task = self
                .tasks
                .get(&task_pid)
                .ok_or(DispatchError::TaskNotFound(task_pid))?;

            if !task.is_running() {
                continue;
            }

            self.signal_window.push(SignalEntry::new(
                task_pid,
                SignalKind::Wait,
                "LLM decided to continue waiting",
            ));

            waited.push(task_pid);
        }

        Ok(waited)
    }

    pub fn status(&self) -> Vec<TaskStatus> {
        self.tasks.values().map(TaskStatus::from).collect()
    }

    pub fn get_output(&self, pid: u64) -> Option<&str> {
        self.outputs.get(&pid).map(|s| s.as_str())
    }

    pub fn get_nonce(&self, pid: u64) -> Option<&str> {
        self.tasks.get(&pid).and_then(|t| t.nonce.as_deref())
    }

    pub fn log_text(&self, count: usize) -> String {
        self.signal_window.format_window(count)
    }

    pub fn log_json(&self, count: usize) -> serde_json::Value {
        self.signal_window.to_json(count)
    }

    pub fn has_running_tasks(&self) -> bool {
        self.tasks.values().any(|t| t.state == TaskState::Running)
    }

    /// All PIDs belonging to a given session.
    fn session_pids(&self, session_id: &str) -> HashSet<u64> {
        self.pid_to_session
            .iter()
            .filter(|(_, sid)| sid.as_str() == session_id)
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// True if any task in the given session is still running (None = the
    /// no-session group). Scopes fire_wake=false "settle" per session so a held
    /// batch in one goal isn't kept waiting by an unrelated running task in
    /// another concurrent goal (#28, #142).
    fn session_has_running(&self, session: Option<&str>) -> bool {
        self.tasks.values().any(|t| {
            t.state == TaskState::Running
                && self.pid_to_session.get(&t.pid).map(|s| s.as_str()) == session
        })
    }

    /// Format the signal window prepended with the current strategy (if set).
    /// When current_session_id is set, the window is filtered to only show
    /// entries for PIDs belonging to that session.
    fn format_wakeup_context(&self, count: usize) -> String {
        let window = match self.current_session_id.as_deref() {
            None => self.signal_window.format_window(count),
            Some(sid) => {
                let pids = self.session_pids(sid);
                if pids.is_empty() {
                    self.signal_window.format_window(count)
                } else {
                    self.signal_window.format_window_for_pids(count, &pids)
                }
            }
        };
        match &self.strategy {
            Some(s) => format!("Current strategy: {s}\n\n{window}"),
            None => window,
        }
    }

    pub async fn wait_for_event(&mut self) -> Result<String> {
        if !self.has_running_tasks() {
            debug!("wait_for_event: no running tasks, returning immediately");
            return Ok(self.format_wakeup_context(DEFAULT_WINDOW_SIZE));
        }

        debug!("wait_for_event: blocking until next event");
        loop {
            tokio::select! {
                Some(result) = self.task_result_rx.recv() => {
                    let pid = result.pid;
                    let fire_wake = self.tasks.get(&pid)
                        .and_then(|t| if let TaskKind::Mcp(def) = &t.kind { Some(def.fire_wake) } else { None })
                        .unwrap_or(false);
                    self.handle_task_result(result);
                    if !self.has_running_tasks() || fire_wake {
                        debug!(fire_wake, "waking LLM");
                        return Ok(self.format_wakeup_context(DEFAULT_WINDOW_SIZE));
                    }
                }
                Some(event) = self.reminder_rx.recv() => {
                    if let Some(task) = self.tasks.get(&event.pid) {
                        if task.is_running() {
                            info!(pid = event.pid, elapsed = event.elapsed_secs, "reminder fired, waking LLM");
                            self.signal_window.push(SignalEntry::new(
                                event.pid,
                                SignalKind::Remind,
                                format!("Running for {}s", event.elapsed_secs),
                            ));
                            return Ok(self.format_wakeup_context(DEFAULT_WINDOW_SIZE));
                        }
                    }
                }
                else => {
                    warn!("event channels closed unexpectedly");
                    return Err(DispatchError::ChannelClosed);
                }
            }
        }
    }

    pub fn drain_results(&mut self) {
        while let Ok(result) = self.task_result_rx.try_recv() {
            let _ = self.handle_task_result(result);
        }
    }

    /// Drain completed task results and fired reminders into the signal window,
    /// returning the signals that should be pushed to the LLM as notifications
    /// (EXIT — respecting `fire_wake` — and REMIND). INIT/WAIT/KILL are never
    /// returned: INIT is delivered inline by the dispatch call, WAIT/KILL are
    /// synchronous to the LLM's own tool calls.
    ///
    /// This is the SOLE proactive drainer of the result/reminder channels on
    /// the request-free path. Request handlers must NOT drain (that would steal
    /// results before they can be pushed) — they render whatever is already in
    /// the window (#26).
    pub fn drain_emittable(&mut self) -> Vec<Emission> {
        let mut emit = Vec::new();

        while let Ok(result) = self.task_result_rx.try_recv() {
            let pid = result.pid;
            let fire_wake = self
                .tasks
                .get(&pid)
                .map(|t| match &t.kind {
                    TaskKind::Mcp(def) => def.fire_wake,
                    TaskKind::Timer(_) => true,
                })
                .unwrap_or(true);
            let session = self.pid_to_session.get(&pid).cloned();
            let produced = self.handle_task_result(result);
            if fire_wake {
                // Per-task: each signal flows one by one, one LLM turn each.
                emit.extend(produced.into_iter().map(Emission::Single));
            } else {
                // Hold until this task's session settles, then flush the group
                // merged as one batch — never drop it (#28).
                self.held.entry(session).or_default().extend(produced);
            }
        }

        while let Ok(event) = self.reminder_rx.try_recv() {
            if let Some(task) = self.tasks.get(&event.pid) {
                if task.is_running() {
                    info!(
                        pid = event.pid,
                        elapsed = event.elapsed_secs,
                        "reminder fired, pushing to LLM"
                    );
                    let entry = SignalEntry::new(
                        event.pid,
                        SignalKind::Remind,
                        format!("Running for {}s", event.elapsed_secs),
                    );
                    self.signal_window.push(entry.clone());
                    emit.push(Emission::Single(entry));
                }
            }
        }

        self.flush_settled_sessions(&mut emit);

        emit
    }

    /// Move held fire_wake=false signals into `emit` for every session whose
    /// tasks have all finished (#28). Also called after a kill, since killing a
    /// session's last running task settles it.
    fn flush_settled_sessions(&mut self, emit: &mut Vec<Emission>) {
        let settled: Vec<Option<String>> = self
            .held
            .keys()
            .filter(|session| !self.session_has_running(session.as_deref()))
            .cloned()
            .collect();
        for session in settled {
            if let Some(signals) = self.held.remove(&session) {
                if !signals.is_empty() {
                    // One settled fire_wake=false group -> one merged batch.
                    emit.push(Emission::Batch(signals));
                }
            }
        }
    }

    /// Render the current signal window for the request path without draining —
    /// completed results are folded in and pushed by the serve loop's wake path
    /// (`drain_emittable`), so `dispatch`/`timer` must not block or steal them
    /// here (#22, #26).
    pub fn wakeup_context(&self) -> String {
        self.format_wakeup_context(DEFAULT_WINDOW_SIZE)
    }

    fn handle_task_result(&mut self, result: TaskResult) -> Vec<SignalEntry> {
        self.reminder_mgr.cancel(result.pid);
        let task_nonce = self.tasks.get(&result.pid).and_then(|t| t.nonce.clone());
        let mut produced = Vec::new();

        match result.kind {
            TaskResultKind::McpComplete(output) => {
                let exit_entry = match output {
                    Ok(out) => {
                        info!(pid = result.pid, "MCP task completed");
                        let raw = if out.is_empty() {
                            "(no output)".to_string()
                        } else {
                            out
                        };

                        let defer = self
                            .tasks
                            .get(&result.pid)
                            .and_then(|t| {
                                if let TaskKind::Mcp(def) = &t.kind {
                                    Some(def.defer_output)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(false);

                        self.outputs.insert(result.pid, raw.clone());

                        let message = if defer {
                            match task_nonce.as_deref() {
                                Some(h) => format!("[hash={h}] 200 (deferred)"),
                                None => "200 (deferred)".to_string(),
                            }
                        } else {
                            match task_nonce.as_deref() {
                                Some(h) => format!("[hash={h}] 200 <{h}>{raw}</{h}>"),
                                None => format!("200 <raw>{raw}</raw>"),
                            }
                        };

                        let entry = SignalEntry::new(result.pid, SignalKind::Exit, message);
                        match task_nonce {
                            Some(h) => entry.with_nonce(h),
                            None => entry,
                        }
                    }
                    Err(err) => {
                        warn!(pid = result.pid, error = %err, "MCP task failed");
                        let message = match task_nonce.as_deref() {
                            Some(h) => format!("[hash={h}] 500 <{h}>{err}</{h}>"),
                            None => format!("500 {err}"),
                        };
                        let entry = SignalEntry::new(result.pid, SignalKind::Exit, message);
                        match task_nonce {
                            Some(h) => entry.with_nonce(h),
                            None => entry,
                        }
                    }
                };
                self.signal_window.push(exit_entry.clone());
                produced.push(exit_entry);
            }
            TaskResultKind::TimerExpired {
                label,
                metadata,
                elapsed,
            } => {
                info!(pid = result.pid, %label, elapsed, "timer expired");
                let remind_entry = SignalEntry::with_payload(
                    result.pid,
                    SignalKind::Remind,
                    format!("timer \"{}\" — {}s elapsed", label, elapsed),
                    json!({
                        "pid": result.pid,
                        "type": "REMIND",
                        "label": label,
                        "metadata": metadata,
                        "elapsed": elapsed,
                    }),
                );
                self.signal_window.push(remind_entry.clone());
                produced.push(remind_entry);

                let exit_entry = SignalEntry::new(result.pid, SignalKind::Exit, "timer completed");
                self.signal_window.push(exit_entry.clone());
                produced.push(exit_entry);
            }
        }

        if let Some(task) = self.tasks.get_mut(&result.pid) {
            task.mark_exited();
        }

        produced
    }

    pub fn shutdown(&mut self) {
        info!("shutting down orchestrator");
        let running_pids: Vec<u64> = self
            .tasks
            .values()
            .filter(|t| t.is_running())
            .map(|t| t.pid)
            .collect();
        for task_pid in running_pids {
            if let Some(task) = self.tasks.get_mut(&task_pid) {
                if let Some(handle) = task.abort_handle.take() {
                    handle.abort();
                }
                task.mark_killed();
            }
        }
        self.reminder_mgr.cancel_all();
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::SignalKind;
    use crate::task::TimerDef;

    fn timer_def(label: &str, duration: u64, metadata: Option<serde_json::Value>) -> TimerDef {
        TimerDef {
            label: label.to_string(),
            duration,
            metadata,
        }
    }

    // -- fire_wake=false held-batch helpers (#28) --------------------------

    fn insert_running_mcp(
        orch: &mut Orchestrator,
        pid: u64,
        session: Option<&str>,
        fire_wake: bool,
    ) {
        let def = TaskDef {
            server: "s".into(),
            tool: "t".into(),
            params: json!({}),
            remind_after: None,
            fire_wake,
            defer_output: false,
        };
        orch.tasks.insert(pid, Task::new_mcp(pid, def));
        if let Some(sid) = session {
            orch.pid_to_session.insert(pid, sid.to_string());
        }
    }

    fn complete_mcp(orch: &Orchestrator, pid: u64, output: &str) {
        orch.task_result_tx
            .try_send(TaskResult {
                pid,
                kind: TaskResultKind::McpComplete(Ok(output.to_string())),
            })
            .expect("channel has capacity");
    }

    fn flatten(emissions: &[Emission]) -> Vec<SignalEntry> {
        let mut out = Vec::new();
        for e in emissions {
            match e {
                Emission::Single(s) => out.push(s.clone()),
                Emission::Batch(sigs) => out.extend(sigs.iter().cloned()),
            }
        }
        out
    }

    fn exit_pids(emissions: &[Emission]) -> Vec<u64> {
        flatten(emissions)
            .into_iter()
            .filter(|s| s.kind == SignalKind::Exit)
            .map(|s| s.pid)
            .collect()
    }

    #[tokio::test]
    async fn fire_wake_false_batch_delivered_together_on_settle() {
        let mut orch = Orchestrator::new();
        insert_running_mcp(&mut orch, 1, Some("goalA"), false);
        insert_running_mcp(&mut orch, 2, Some("goalA"), false);

        // Task 1 finishes while task 2 runs: held, nothing pushed yet.
        complete_mcp(&orch, 1, "out1");
        assert!(orch.drain_emittable().is_empty());

        // Task 2 finishes: the session settles, so BOTH are flushed as ONE
        // merged batch (not two separate singles).
        complete_mcp(&orch, 2, "out2");
        let emissions = orch.drain_emittable();
        assert_eq!(emissions.len(), 1, "one merged batch, not per-signal");
        match &emissions[0] {
            Emission::Batch(sigs) => {
                let pids: Vec<u64> = sigs
                    .iter()
                    .filter(|s| s.kind == SignalKind::Exit)
                    .map(|s| s.pid)
                    .collect();
                assert_eq!(pids, vec![1, 2]);
            }
            Emission::Single(_) => panic!("expected a merged Batch"),
        }
    }

    #[tokio::test]
    async fn fire_wake_false_settle_is_session_scoped() {
        // A held task in goalA must not wait on an unrelated running task in goalB.
        let mut orch = Orchestrator::new();
        insert_running_mcp(&mut orch, 1, Some("goalA"), false);
        insert_running_mcp(&mut orch, 2, Some("goalB"), false); // stays running

        complete_mcp(&orch, 1, "out1");
        assert_eq!(exit_pids(&orch.drain_emittable()), vec![1]);
    }

    #[tokio::test]
    async fn kill_flushes_held_signals_for_settled_session() {
        let mut orch = Orchestrator::new();
        insert_running_mcp(&mut orch, 1, Some("goalA"), false);
        insert_running_mcp(&mut orch, 2, Some("goalA"), false);

        complete_mcp(&orch, 1, "out1");
        assert!(orch.drain_emittable().is_empty());

        // Killing task 2 settles the session; the held task-1 exit flushes.
        orch.kill(&[2]).expect("kill succeeds");
        assert_eq!(exit_pids(&orch.drain_emittable()), vec![1]);
    }

    #[tokio::test]
    async fn fire_wake_true_still_emits_immediately_with_running_sibling() {
        // A fire_wake=true task is never held or merged — it emits as a Single
        // even while a sibling runs.
        let mut orch = Orchestrator::new();
        insert_running_mcp(&mut orch, 1, Some("goalA"), true);
        insert_running_mcp(&mut orch, 2, Some("goalA"), false);

        complete_mcp(&orch, 1, "out1");
        let emissions = orch.drain_emittable();
        assert_eq!(emissions.len(), 1);
        assert!(
            matches!(&emissions[0], Emission::Single(s) if s.kind == SignalKind::Exit && s.pid == 1),
            "fire_wake=true must emit as a Single"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn basic_timer_fires_init_remind_exit() {
        let mut orch = Orchestrator::new();
        let pid = orch.dispatch_timer(timer_def("test_timer", 2, None));
        assert!(pid > 0);
        let signals = orch.signal_window.all();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, SignalKind::Init);
        assert!(signals[0].message.contains("test_timer"));
        assert!(signals[0].nonce.is_none());
        assert!(orch.has_running_tasks());
        let result = orch.wait_for_event().await;
        assert!(result.is_ok());
        let signals = orch.signal_window.all();
        assert_eq!(signals.len(), 3);
        assert_eq!(signals[0].kind, SignalKind::Init);
        assert_eq!(signals[1].kind, SignalKind::Remind);
        assert_eq!(signals[2].kind, SignalKind::Exit);
        let payload = signals[1]
            .payload
            .as_ref()
            .expect("REMIND should have payload");
        assert_eq!(payload["type"], "REMIND");
        assert_eq!(payload["label"], "test_timer");
        assert_eq!(payload["elapsed"], 2);
        assert_eq!(payload["pid"], pid);
        assert!(signals[2].message.contains("timer completed"));
        assert!(signals[2].nonce.is_none());
        assert!(!orch.has_running_tasks());
    }

    #[tokio::test(start_paused = true)]
    async fn wakeup_context_returns_immediately_with_running_task() {
        // Fire-and-return (#22): a still-running task must not make the request
        // path block. dispatch_timer enqueues INIT synchronously; the snapshot
        // must reflect that INIT and return at once, without awaiting the
        // REMIND/EXIT that only fire much later.
        let mut orch = Orchestrator::new();
        let pid = orch.dispatch_timer(timer_def("long_task", 3600, None));
        assert!(orch.has_running_tasks());

        let window = orch.wakeup_context();

        // The task is still running and only INIT is present — we did not wait.
        assert!(orch.has_running_tasks());
        let signals = orch.signal_window.all();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, SignalKind::Init);
        assert_eq!(signals[0].pid, pid);
        assert!(!window.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn drain_emittable_returns_exit_signals_for_pushing() {
        // The serve loop's wake path drains completed results and returns the
        // EXIT/REMIND signals to push to the LLM (#26).
        let mut orch = Orchestrator::new();
        orch.dispatch_timer(timer_def("quick", 1, None));
        // Nothing has fired yet: draining is a no-op.
        assert!(orch.drain_emittable().is_empty());

        // Let the spawned timer task register its sleep before advancing the
        // (paused) clock past it, then let it wake, send, and notify.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        // A timer is fire_wake=true, so its REMIND + EXIT come as Singles.
        let emitted = flatten(&orch.drain_emittable());
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].kind, SignalKind::Remind);
        assert_eq!(emitted[1].kind, SignalKind::Exit);
        assert!(!orch.has_running_tasks());
        // Draining again yields nothing — the result was consumed once.
        assert!(orch.drain_emittable().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn kill_timer_prevents_remind() {
        let mut orch = Orchestrator::new();
        let pid = orch.dispatch_timer(timer_def("kill_me", 60, None));
        tokio::time::advance(Duration::from_secs(1)).await;
        let killed = orch.kill(&[pid]).expect("kill should succeed");
        assert_eq!(killed, vec![pid]);
        let signals = orch.signal_window.all();
        assert_eq!(signals.len(), 2);
        assert_eq!(signals[0].kind, SignalKind::Init);
        assert_eq!(signals[1].kind, SignalKind::Kill);
        assert!(signals[1].message.contains("cancelled"));
        tokio::time::advance(Duration::from_secs(120)).await;
        orch.drain_results();
        assert_eq!(orch.signal_window.all().len(), 2);
        assert!(!orch.has_running_tasks());
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_timers_fire_independently() {
        let mut orch = Orchestrator::new();
        let pid1 = orch.dispatch_timer(timer_def("fast", 1, None));
        let pid2 = orch.dispatch_timer(timer_def("medium", 3, None));
        let pid3 = orch.dispatch_timer(timer_def("slow", 5, None));
        assert_eq!(orch.signal_window.all().len(), 3);
        let _ = orch.wait_for_event().await;
        let signals = orch.signal_window.all();
        assert_eq!(signals.len(), 9);
        let reminds: Vec<_> = signals
            .iter()
            .filter(|s| s.kind == SignalKind::Remind)
            .collect();
        assert_eq!(reminds.len(), 3);
        let mut remind_pids: Vec<u64> = reminds.iter().map(|s| s.pid).collect();
        remind_pids.sort();
        remind_pids.dedup();
        assert_eq!(remind_pids.len(), 3);
        assert!(remind_pids.contains(&pid1));
        assert!(remind_pids.contains(&pid2));
        assert!(remind_pids.contains(&pid3));
        assert!(!orch.has_running_tasks());
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_passthrough() {
        let meta = json!({"goal_id": "abc123", "type": "goal_defer", "priority": 5});
        let mut orch = Orchestrator::new();
        let pid = orch.dispatch_timer(timer_def("goal_reminder", 2, Some(meta.clone())));
        let init_payload = orch.signal_window.all()[0]
            .payload
            .as_ref()
            .expect("INIT should have payload");
        assert_eq!(init_payload["metadata"], meta);
        let _ = orch.wait_for_event().await;
        let signals = orch.signal_window.all();
        let remind = signals
            .iter()
            .find(|s| s.kind == SignalKind::Remind)
            .unwrap();
        let remind_payload = remind.payload.as_ref().expect("REMIND should have payload");
        assert_eq!(remind_payload["metadata"], meta);
        assert_eq!(remind_payload["label"], "goal_reminder");
        assert_eq!(remind_payload["pid"], pid);
    }

    #[tokio::test(start_paused = true)]
    async fn status_shows_timer_with_remaining_time() {
        let mut orch = Orchestrator::new();
        orch.dispatch_timer(timer_def("check_build", 60, None));
        tokio::time::advance(Duration::from_secs(10)).await;
        let statuses = orch.status();
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.state, TaskState::Running);
        match &status.kind {
            crate::task::TaskStatusKind::Timer { label, fires_in } => {
                assert_eq!(label, "check_build");
                assert!(*fires_in <= 50);
                assert!(*fires_in >= 49);
            }
            other => panic!("Expected timer status, got {:?}", other),
        }
    }

    #[test]
    fn kill_nonexistent_pid_returns_error() {
        let mut orch = Orchestrator::new();
        assert!(orch.kill(&[999]).is_err());
    }

    #[test]
    fn get_output_returns_none_for_unknown_pid() {
        let orch = Orchestrator::new();
        assert!(orch.get_output(42).is_none());
        assert!(orch.get_nonce(42).is_none());
    }

    #[test]
    fn strategy_stored_and_returns_none_initially() {
        let orch = Orchestrator::new();
        let context = orch.format_wakeup_context(20);
        assert!(!context.contains("Current strategy:"));
    }

    #[test]
    fn session_pids_returns_only_matching_pids() {
        let mut orch = Orchestrator::new();
        orch.pid_to_session.insert(1, "goal_a".to_string());
        orch.pid_to_session.insert(2, "goal_a".to_string());
        orch.pid_to_session.insert(3, "goal_b".to_string());
        let pids_a = orch.session_pids("goal_a");
        assert!(pids_a.contains(&1));
        assert!(pids_a.contains(&2));
        assert!(!pids_a.contains(&3));
        let pids_b = orch.session_pids("goal_b");
        assert!(pids_b.contains(&3));
        assert!(!pids_b.contains(&1));
    }
}
