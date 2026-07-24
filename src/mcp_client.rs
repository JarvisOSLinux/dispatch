use crate::error::{DispatchError, Result};
use std::process::Stdio;
use tokio::process::Command;
use tracing::{debug, warn};

/// SIGKILLs a child's whole process group on drop, so aborting a task tears
/// down the entire dmcp → MCP-server tree rather than orphaning grandchildren.
/// Disarmed once the child has been reaped normally.
#[cfg(unix)]
struct GroupKiller(Option<u32>);

#[cfg(unix)]
impl GroupKiller {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

#[cfg(unix)]
impl Drop for GroupKiller {
    fn drop(&mut self) {
        if let Some(pgid) = self.0 {
            // The child was spawned as a group leader, so pgid == its pid.
            unsafe {
                libc::killpg(pgid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

/// Windows equivalent of GroupKiller: the child is assigned to a Job Object
/// with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, so terminating (or closing) the
/// job tears down the whole dmcp → MCP-server tree, mirroring killpg on Unix.
/// Disarmed once the child has been reaped normally.
#[cfg(windows)]
struct JobKiller(Option<windows_sys::Win32::Foundation::HANDLE>);

// HANDLE is an opaque numeric identifier with no thread affinity; every
// Win32 call we make on it (TerminateJobObject, CloseHandle) is thread-safe.
#[cfg(windows)]
unsafe impl Send for JobKiller {}

#[cfg(windows)]
impl JobKiller {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

#[cfg(windows)]
impl Drop for JobKiller {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        if let Some(job) = self.0 {
            unsafe {
                // Kills every process still assigned to the job (the whole tree).
                TerminateJobObject(job, 1);
                CloseHandle(job);
            }
        }
    }
}

/// Create a Job Object that kills all member processes when the job handle is
/// closed, and assign `child_handle` to it. Returns None (best-effort, logged)
/// on any Win32 failure rather than failing the whole dispatch.
#[cfg(windows)]
fn assign_to_killable_job(
    child_handle: windows_sys::Win32::Foundation::HANDLE,
) -> Option<windows_sys::Win32::Foundation::HANDLE> {
    use std::mem::size_of;
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };

    unsafe {
        let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if job.is_null() {
            warn!("failed to create Job Object for dmcp process tree");
            return None;
        }

        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        let ok = SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &info as *const _ as *const std::ffi::c_void,
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        if ok == 0 {
            warn!("failed to configure Job Object kill-on-close limit");
            CloseHandle(job);
            return None;
        }

        if AssignProcessToJobObject(job, child_handle) == 0 {
            warn!("failed to assign dmcp process to Job Object");
            CloseHandle(job);
            return None;
        }

        Some(job)
    }
}

/// Client for invoking dmcp commands.
/// dispatch delegates all MCP server management to dmcp.
pub struct DmcpClient;

impl DmcpClient {
    /// Check that dmcp is available on PATH.
    pub async fn check_available() -> Result<()> {
        let output = Command::new("dmcp")
            .arg("paths")
            .output()
            .await
            .map_err(|_| DispatchError::DmcpNotFound)?;

        if !output.status.success() {
            return Err(DispatchError::DmcpNotFound);
        }
        Ok(())
    }

    /// Argv (after the program name) for a `dmcp call`. A stateful task carrying
    /// a session gets `--session <id>` appended so the broker reuses one live
    /// server; without a session it is the byte-for-byte one-shot argv. Factored
    /// out so the session threading is unit-testable without spawning dmcp.
    fn call_args(
        server: &str,
        tool: &str,
        params: &serde_json::Value,
        session: Option<&str>,
    ) -> Vec<String> {
        let mut args = vec!["call".to_string(), server.to_string(), tool.to_string()];
        if !params.is_null() && params != &serde_json::json!({}) {
            args.push("--args".to_string());
            args.push(params.to_string());
        }
        if let Some(sid) = session {
            args.push("--session".to_string());
            args.push(sid.to_string());
        }
        args
    }

    /// Call a tool on an MCP server via `dmcp call <server> <tool> --args <json>`,
    /// optionally threading `--session <id>` for a stateful task. Returns the
    /// stdout output as a string.
    ///
    /// The child is spawned in its own process group with kill-on-drop, so if
    /// the orchestrator aborts this task (the `kill` tool), dropping this future
    /// tears down the whole dmcp → MCP-server tree instead of leaving it running.
    /// For a `--session` task the process-group kill only reaps the thin dmcp
    /// client — the broker-held server survives, so the session is torn down
    /// explicitly via `session_close` on settle/kill.
    pub async fn call_tool(
        server: &str,
        tool: &str,
        params: &serde_json::Value,
        session: Option<&str>,
    ) -> Result<String> {
        debug!(server, tool, "calling dmcp tool");
        let mut cmd = Command::new("dmcp");
        cmd.args(Self::call_args(server, tool, params, session));

        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Own process group so the whole tree can be signalled on abort.
        #[cfg(unix)]
        cmd.process_group(0);

        let child = cmd.spawn().map_err(|e| {
            warn!(server, tool, error = %e, "failed to spawn dmcp");
            DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e))
        })?;

        #[cfg(unix)]
        let mut guard = GroupKiller(child.id());

        // Assign to a killable Job Object immediately after spawn so the whole
        // tree can be torn down on abort, mirroring the Unix process group.
        #[cfg(windows)]
        let mut guard = {
            let job = child
                .raw_handle()
                .and_then(|h| assign_to_killable_job(h as windows_sys::Win32::Foundation::HANDLE));
            JobKiller(job)
        };

        let output = child.wait_with_output().await.map_err(|e| {
            warn!(server, tool, error = %e, "failed to run dmcp");
            DispatchError::DmcpError(format!("failed to run dmcp: {}", e))
        })?;

        #[cfg(any(unix, windows))]
        guard.disarm();

        // Status is read from the exit code, never by sniffing a sentinel in the
        // output: dmcp exits 0 only when the tool succeeded, and non-zero on a
        // tool-reported error (is_error) as well as on RPC failure.
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            debug!(server, tool, "dmcp call succeeded");
            Ok(stdout.trim().to_string())
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            // The tool's own error detail is on stdout; prefer it, falling back
            // to stderr (used for RPC/spawn failures).
            let msg = if !stdout.trim().is_empty() {
                stdout.trim().to_string()
            } else {
                stderr.trim().to_string()
            };
            warn!(server, tool, error = %msg, "dmcp call failed");
            Err(DispatchError::DmcpError(msg))
        }
    }

    /// Browse servers via `dmcp browse -k <keywords> --json`.
    pub async fn browse(keywords: &[String]) -> Result<serde_json::Value> {
        let mut cmd = Command::new("dmcp");
        cmd.arg("browse").arg("--json");
        for kw in keywords {
            cmd.arg("-k").arg(kw);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Single vector search via `dmcp browse --vector <json> --top-k N --min-score F --json`.
    pub async fn browse_vector(
        vector: &[f64],
        top_k: u64,
        min_score: f64,
    ) -> Result<serde_json::Value> {
        let vec_json = serde_json::to_string(vector)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vector: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("browse")
            .arg("--vector")
            .arg(&vec_json)
            .arg("--top-k")
            .arg(top_k.to_string())
            .arg("--min-score")
            .arg(min_score.to_string())
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Batch vector search via `dmcp browse --vectors <json> --top-k N --min-score F --json`.
    pub async fn browse_vectors(
        vectors: &[Vec<f64>],
        top_k: u64,
        min_score: f64,
    ) -> Result<serde_json::Value> {
        let vecs_json = serde_json::to_string(vectors)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vectors: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("browse")
            .arg("--vectors")
            .arg(&vecs_json)
            .arg("--top-k")
            .arg(top_k.to_string())
            .arg("--min-score")
            .arg(min_score.to_string())
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Get visible server count via `dmcp count`.
    pub async fn server_count() -> Result<u64> {
        let output = Command::new("dmcp")
            .arg("count")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .trim()
                .parse::<u64>()
                .map_err(|e| DispatchError::DmcpError(format!("invalid count from dmcp: {}", e)))
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Get the registry's embedding model spec via `dmcp embedding-spec`.
    pub async fn embedding_spec() -> Result<serde_json::Value> {
        let output = Command::new("dmcp")
            .arg("embedding-spec")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp embedding-spec: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Refresh the local vector index via `dmcp sync-index`.
    pub async fn sync_index() -> Result<String> {
        let output = Command::new("dmcp")
            .arg("sync-index")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Index a non-approved server via `dmcp index-server <id> --vectors <json>`.
    ///
    /// `payload` must be dmcp's expected object shape:
    /// `{"server": [f32...], "tools": {"<tool>": [f32...]}}`. Passing a bare
    /// array-of-arrays (the previous behavior) always failed dmcp's parser.
    pub async fn index_server(server_id: &str, payload: &serde_json::Value) -> Result<String> {
        let vecs_json = serde_json::to_string(payload)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vectors: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("index-server")
            .arg(server_id)
            .arg("--vectors")
            .arg(&vecs_json)
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// List tools for a server via `dmcp tools <id> --json`.
    pub async fn list_tools(server: &str) -> Result<serde_json::Value> {
        let output = Command::new("dmcp")
            .arg("tools")
            .arg(server)
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp tools: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Program used to spawn dmcp subcommands. Defaults to `dmcp` on PATH;
    /// overridable via `DMCP_BIN` so a test can point the session-close spawn at
    /// a stub rather than a real dmcp binary.
    fn dmcp_bin() -> String {
        std::env::var("DMCP_BIN").unwrap_or_else(|_| "dmcp".to_string())
    }

    /// Argv (after the program name) for `dmcp session close <session_id>`.
    /// Factored out so the teardown command is assertable without spawning dmcp.
    fn session_close_args(session_id: &str) -> Vec<String> {
        vec![
            "session".to_string(),
            "close".to_string(),
            session_id.to_string(),
        ]
    }

    /// Fire `dmcp session close <session_id>` and await its exit. Idempotent on
    /// the dmcp side (exit 0 even if nothing was open). Used to tear down a
    /// broker-held stateful server, which survives the thin client's
    /// process-group kill.
    async fn session_close(session_id: &str) -> Result<()> {
        let output = Command::new(Self::dmcp_bin())
            .args(Self::session_close_args(session_id))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Best-effort, fire-and-forget `dmcp session close <session_id>`: spawn it
    /// onto the current runtime and return immediately, never blocking the
    /// orchestrator's event loop. Failures are logged, not propagated — the
    /// broker's idle TTL is the backstop. A no-op when called outside a Tokio
    /// runtime (e.g. a synchronous unit test), so it never panics.
    pub fn spawn_session_close(session_id: &str) {
        let sid = session_id.to_string();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(e) = Self::session_close(&sid).await {
                        warn!(session = %sid, error = %e, "dmcp session close failed");
                    } else {
                        debug!(session = %sid, "dmcp session closed");
                    }
                });
            }
            Err(_) => {
                debug!(session = %sid, "no Tokio runtime; skipping session close spawn");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn as_strs(args: &[String]) -> Vec<&str> {
        args.iter().map(String::as_str).collect()
    }

    #[test]
    fn call_args_one_shot_has_no_session_flag() {
        let args = DmcpClient::call_args("srv", "tool", &json!({"a": 1}), None);
        assert_eq!(
            as_strs(&args),
            vec!["call", "srv", "tool", "--args", "{\"a\":1}"]
        );
        assert!(!args.iter().any(|a| a == "--session"));
    }

    #[test]
    fn call_args_stateful_without_session_is_one_shot() {
        // stateful is a task property; the argv only gains --session when the
        // caller passes one. No session => unchanged one-shot argv.
        let args = DmcpClient::call_args("srv", "tool", &json!({"a": 1}), None);
        assert_eq!(
            as_strs(&args),
            vec!["call", "srv", "tool", "--args", "{\"a\":1}"]
        );
    }

    #[test]
    fn call_args_session_appended_after_args() {
        let args = DmcpClient::call_args("srv", "tool", &json!({"k": "v"}), Some("goal-1"));
        assert_eq!(
            as_strs(&args),
            vec![
                "call",
                "srv",
                "tool",
                "--args",
                "{\"k\":\"v\"}",
                "--session",
                "goal-1"
            ]
        );
    }

    #[test]
    fn call_args_session_with_empty_params_omits_args_flag() {
        // Empty params are omitted exactly as the one-shot path does, but the
        // session flag is still threaded through.
        let args = DmcpClient::call_args("srv", "tool", &json!({}), Some("goal-1"));
        assert_eq!(
            as_strs(&args),
            vec!["call", "srv", "tool", "--session", "goal-1"]
        );
    }

    #[test]
    fn session_close_args_shape() {
        assert_eq!(
            as_strs(&DmcpClient::session_close_args("goal-9")),
            vec!["session", "close", "goal-9"]
        );
    }
}
