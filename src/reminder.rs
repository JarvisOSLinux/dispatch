use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};
use tracing::debug;

use crate::tail::TaskTail;

/// A reminder event sent when a task exceeds its remind_after duration.
#[derive(Debug)]
pub struct ReminderEvent {
    pub pid: u64,
    pub elapsed_secs: u64,
}

/// Manages per-task reminder timers.
pub struct ReminderManager {
    timers: HashMap<u64, JoinHandle<()>>,
    /// One-shot watchers that remind once on a task's first output.
    first_output: HashMap<u64, JoinHandle<()>>,
    tx: mpsc::Sender<ReminderEvent>,
    /// Wakes the serve loop so a fired reminder is drained and pushed to the
    /// LLM without waiting for the next request (#26).
    wake: Arc<Notify>,
}

impl ReminderManager {
    pub fn new(tx: mpsc::Sender<ReminderEvent>, wake: Arc<Notify>) -> Self {
        Self {
            timers: HashMap::new(),
            first_output: HashMap::new(),
            tx,
            wake,
        }
    }

    /// Start a recurring reminder timer for a task.
    /// Fires every `interval_secs` until cancelled.
    pub fn start(&mut self, pid: u64, interval_secs: u64) {
        debug!(pid, interval_secs, "starting reminder timer");
        let tx = self.tx.clone();
        let wake = self.wake.clone();
        let handle = tokio::spawn(async move {
            let interval = Duration::from_secs(interval_secs);
            let mut elapsed: u64 = 0;
            loop {
                time::sleep(interval).await;
                elapsed += interval_secs;
                if tx
                    .send(ReminderEvent {
                        pid,
                        elapsed_secs: elapsed,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
                wake.notify_one();
            }
        });
        self.timers.insert(pid, handle);
    }

    /// Remind once as soon as a task first writes output, after `settle` so the
    /// rest of what it is printing arrives too. A tool that blocks waiting on a
    /// person usually prints what it waits on first (a sign-in code, a
    /// prompt); with the periodic reminder minutes away, nobody would see it
    /// until then. The periodic timer is unaffected.
    pub fn nudge_on_first_output(&mut self, pid: u64, tail: TaskTail, settle: Duration) {
        let tx = self.tx.clone();
        let wake = self.wake.clone();
        let started = Instant::now();
        let handle = tokio::spawn(async move {
            tail.first_output().await;
            time::sleep(settle).await;
            let event = ReminderEvent {
                pid,
                elapsed_secs: started.elapsed().as_secs(),
            };
            if tx.send(event).await.is_ok() {
                wake.notify_one();
            }
        });
        self.first_output.insert(pid, handle);
    }

    /// Whether a reminder timer is currently running for a task.
    #[cfg(test)]
    pub fn is_armed(&self, pid: u64) -> bool {
        self.timers.contains_key(&pid)
    }

    /// Cancel the reminder timer for a task.
    pub fn cancel(&mut self, pid: u64) {
        if let Some(handle) = self.timers.remove(&pid) {
            debug!(pid, "cancelling reminder timer");
            handle.abort();
        }
        if let Some(handle) = self.first_output.remove(&pid) {
            handle.abort();
        }
    }

    /// Cancel all reminder timers.
    pub fn cancel_all(&mut self) {
        for (_, handle) in self.timers.drain().chain(self.first_output.drain()) {
            handle.abort();
        }
    }
}

impl Drop for ReminderManager {
    fn drop(&mut self) {
        self.cancel_all();
    }
}
