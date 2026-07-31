//! Integration tests for live output on running tasks.
//!
//! These drive the real `dispatch serve` binary over stdio JSON-RPC, with a
//! fake `dmcp` (`tests/fixtures/fake_dmcp.sh`) planted on PATH that writes
//! stderr progressively before returning its stdout result. Assertions are
//! made against what the binary actually writes — responses AND pushed
//! notifications — so a signal that is recorded but never emitted to the LLM
//! fails here, not just in a unit test.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A running `dispatch serve` with the fake dmcp first on its PATH. Reads the
/// child's stdout on a thread into a channel so responses and asynchronously
/// pushed notifications can both be awaited with timeouts.
struct Serve {
    child: Child,
    stdin: ChildStdin,
    rx: mpsc::Receiver<Value>,
    stash: Vec<Value>,
    next_id: u64,
    root: PathBuf,
}

impl Serve {
    fn start() -> Self {
        let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("dispatch-live-tail-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&root).expect("create test root");

        let fake_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_dmcp.sh");
        let fake_dst = root.join("dmcp");
        std::fs::copy(&fake_src, &fake_dst).expect("install fake dmcp");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_dst, std::fs::Permissions::from_mode(0o755))
            .expect("make fake dmcp executable");

        let path = format!(
            "{}:{}",
            root.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = Command::new(env!("CARGO_BIN_EXE_dispatch"))
            .arg("serve")
            .env("PATH", path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn dispatch serve");

        let stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if tx.send(v).is_err() {
                    break;
                }
            }
        });

        let mut serve = Serve {
            child,
            stdin,
            rx,
            stash: Vec::new(),
            next_id: 0,
            root,
        };
        let init = serve.request("initialize", json!({}));
        assert!(init.get("result").is_some(), "initialize failed: {init}");
        serve.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        serve
    }

    fn send(&mut self, v: &Value) {
        writeln!(self.stdin, "{v}").expect("write to dispatch stdin");
        self.stdin.flush().expect("flush dispatch stdin");
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        self.wait_for(Duration::from_secs(30), |v| {
            v.get("id").and_then(Value::as_u64) == Some(id)
        })
        .unwrap_or_else(|| panic!("no response to {method} (id {id})"))
    }

    /// tools/call that must succeed; returns the `result` object.
    fn call(&mut self, tool: &str, args: Value) -> Value {
        let resp = self.request("tools/call", json!({"name": tool, "arguments": args}));
        resp.get("result")
            .cloned()
            .unwrap_or_else(|| panic!("tool {tool} errored: {resp}"))
    }

    /// Pull messages (stashing non-matches so nothing is lost) until `pred`
    /// matches or the timeout passes.
    fn wait_for(&mut self, timeout: Duration, pred: impl Fn(&Value) -> bool) -> Option<Value> {
        if let Some(i) = self.stash.iter().position(&pred) {
            return Some(self.stash.remove(i));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            match self.rx.recv_timeout(left) {
                Ok(v) if pred(&v) => return Some(v),
                Ok(v) => self.stash.push(v),
                Err(_) => return None,
            }
        }
    }

    /// Await a pushed `dispatch.signal` notification matching `pred`, returning
    /// its `data` (the serialized SignalEntry).
    fn wait_signal(&mut self, timeout: Duration, pred: impl Fn(&Value) -> bool) -> Option<Value> {
        self.wait_for(timeout, |v| {
            v.get("method").and_then(Value::as_str) == Some("notifications/message")
                && v["params"]["logger"] == "dispatch.signal"
                && pred(&v["params"]["data"])
        })
        .map(|v| v["params"]["data"].clone())
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Extract the text between `<h>` and `</h>` in a boundary-wrapped message.
fn wrapped_text<'a>(message: &'a str, h: &str) -> &'a str {
    let open = format!("<{h}>");
    let close = format!("</{h}>");
    let start = message.find(&open).expect("open boundary tag") + open.len();
    let end = message.rfind(&close).expect("close boundary tag");
    &message[start..end]
}

/// (a) + (e): a REMIND pushed mid-run carries the stderr seen so far, wrapped
/// in a provenance boundary under a nonce that is NOT the task's EXIT nonce;
/// the EXIT that follows is byte-for-byte the pre-change shape, stdout only.
#[test]
fn remind_carries_live_stderr_tail_and_exit_is_unchanged() {
    let mut s = Serve::start();
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "slow_progress", "remind_after": 1}]}),
    );
    assert_eq!(res["pids"], json!([1]));

    let remind = s
        .wait_signal(Duration::from_secs(10), |d| {
            d["kind"] == "REMIND" && d.get("nonce").is_some()
        })
        .expect("a REMIND with a tail must be PUSHED to the client");
    let msg = remind["message"].as_str().unwrap();
    let h = remind["nonce"].as_str().unwrap();
    assert!(msg.starts_with("Running for "), "got: {msg}");
    assert!(msg.contains(&format!("[hash={h}]")), "got: {msg}");
    assert!(
        wrapped_text(msg, h).contains("phase-one started"),
        "the tail must carry the stderr emitted so far, got: {msg}"
    );

    let exit = s
        .wait_signal(Duration::from_secs(15), |d| d["kind"] == "EXIT")
        .expect("EXIT must be pushed");
    let e = exit["nonce"].as_str().unwrap();
    assert_eq!(
        exit["message"].as_str().unwrap(),
        format!("[hash={e}] 200 <{e}>slow-result</{e}>"),
        "EXIT must stay stdout-only in the pre-change shape"
    );
    assert_ne!(
        e, h,
        "REMIND must not have disclosed the EXIT nonce mid-run"
    );

    let out = s.call("get_output", json!({"pids": [1]}));
    assert_eq!(out["outputs"]["1"]["output"], "slow-result");
    assert_eq!(out["outputs"]["1"]["hash"], e);
}

/// (b): status {"tail": n} returns the latest n CHARACTERS of a running
/// task's stderr (clamped to the buffer), wrapped like EXIT output; once the
/// task completes the field is absent.
#[test]
fn status_tail_while_running_and_absent_after_exit() {
    let mut s = Serve::start();
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "slow_progress"}]}),
    );

    // Poll until the first stderr chunk has reached the ring.
    let deadline = Instant::now() + Duration::from_secs(5);
    let task = loop {
        let res = s.call("status", json!({"tail": 5}));
        let task = res["tasks"][0].clone();
        if task.get("tail").is_some() {
            break task;
        }
        assert!(
            Instant::now() < deadline,
            "running task never exposed a tail: {res}"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let h = task["tail_hash"].as_str().expect("tail_hash present");
    // Latest 5 chars of "phase-one started\n".
    assert_eq!(
        task["tail"].as_str().unwrap(),
        format!("[hash={h}] <{h}>rted\n</{h}>")
    );

    let clamped = s.call("status", json!({"tail": 100000}));
    let big = clamped["tasks"][0]["tail"].as_str().expect("tail present");
    assert!(
        big.contains("phase-one started\n"),
        "over-large n clamps to everything held, got: {big}"
    );

    s.wait_signal(Duration::from_secs(15), |d| d["kind"] == "EXIT")
        .expect("EXIT pushed");
    let done = s.call("status", json!({"tail": 5}));
    assert_eq!(done["tasks"][0]["state"], "exited");
    assert!(
        done["tasks"][0].get("tail").is_none() && done["tasks"][0].get("tail_hash").is_none(),
        "completed tasks must carry no tail: {done}"
    );
}

/// (c): status WITHOUT "tail" is byte-identical to the pre-change response,
/// even while a buffer with content exists for the running task.
#[test]
fn status_without_tail_is_byte_identical() {
    let mut s = Serve::start();
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "slow_progress"}]}),
    );

    // Make sure stderr HAS arrived, so the absence below is not vacuous.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let res = s.call("status", json!({"tail": 1}));
        if res["tasks"][0].get("tail").is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "tail never appeared: {res}");
        std::thread::sleep(Duration::from_millis(100));
    }

    let res = s.call("status", json!({}));
    let expected_text = "[\n  {\n    \"pid\": 1,\n    \"type\": \"mcp\",\n    \"server\": \"fake\",\n    \"tool\": \"slow_progress\",\n    \"state\": \"running\"\n  }\n]";
    assert_eq!(
        res["content"][0]["text"].as_str().unwrap(),
        expected_text,
        "plain status text must not change shape"
    );
    assert_eq!(
        res["tasks"],
        json!([{
            "pid": 1,
            "type": "mcp",
            "server": "fake",
            "tool": "slow_progress",
            "state": "running"
        }])
    );
}

/// (e): failure-detail semantics are unchanged — stderr becomes the 500 detail
/// when stdout is empty, and stdout is preferred when both exist.
#[test]
fn exit_failure_detail_still_prefers_stdout_then_stderr() {
    let mut s = Serve::start();
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "fail_with_stderr"}]}),
    );
    let exit = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "EXIT")
        .expect("EXIT pushed");
    let e = exit["nonce"].as_str().unwrap();
    let msg = exit["message"].as_str().unwrap();
    assert!(msg.contains(&format!("[hash={e}] 500 ")), "got: {msg}");
    assert!(
        wrapped_text(msg, e).contains("failure detail on stderr"),
        "stderr must still reach the failure detail, got: {msg}"
    );

    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "fail_with_stdout"}]}),
    );
    let exit = s
        .wait_signal(Duration::from_secs(10), |d| {
            d["kind"] == "EXIT" && d["pid"] == 2
        })
        .expect("second EXIT pushed");
    let e = exit["nonce"].as_str().unwrap();
    let msg = exit["message"].as_str().unwrap();
    let detail = wrapped_text(msg, e);
    assert!(detail.contains("tool-reported error detail"), "got: {msg}");
    assert!(
        !detail.contains("noise on stderr"),
        "stdout detail must still win over stderr, got: {msg}"
    );
}

/// (e): a quick success is untouched end to end — EXIT inlines stdout under
/// the task nonce and get_output returns it verbatim.
#[test]
fn quick_exit_output_is_unchanged() {
    let mut s = Serve::start();
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "quick"}]}),
    );
    let exit = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "EXIT")
        .expect("EXIT pushed");
    let e = exit["nonce"].as_str().unwrap();
    assert_eq!(
        exit["message"].as_str().unwrap(),
        format!("[hash={e}] 200 <{e}>quick-result</{e}>")
    );
    let out = s.call("get_output", json!({"pids": [1]}));
    assert_eq!(out["outputs"]["1"]["output"], "quick-result");
}
