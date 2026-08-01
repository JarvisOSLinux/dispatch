//! Integration tests for live output on running tasks.
//!
//! These drive the real `dispatch serve` binary over stdio JSON-RPC, with a
//! fake `dmcp` (`tests/fixtures/fake_dmcp.sh`) planted on PATH that writes
//! stderr progressively before returning its stdout result. Assertions are
//! made against what the binary actually writes — responses AND pushed
//! notifications — so a signal that is recorded but never emitted to the LLM
//! fails here, not just in a unit test.

#![cfg(unix)]

mod common;

use std::time::{Duration, Instant};

use serde_json::json;

use common::{wrapped_text, Serve};

/// (a) + (e): a REMIND pushed mid-run carries the stderr seen so far, wrapped
/// in a provenance boundary under a nonce that is NOT the task's EXIT nonce;
/// the EXIT that follows is byte-for-byte the pre-change shape, stdout only.
#[test]
fn remind_carries_live_stderr_tail_and_exit_is_unchanged() {
    let mut s = Serve::start("live-tail");
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
    let mut s = Serve::start("live-tail");
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
    let mut s = Serve::start("live-tail");
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
    let mut s = Serve::start("live-tail");
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

/// Pipe fds currently open in a process, counted via /proc. The stderr reader
/// holds its pipe's read end for exactly as long as it lives, so this is an
/// outside-observable proxy for "the reader task ended".
#[cfg(target_os = "linux")]
fn pipe_fd_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .expect("read /proc fd dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            std::fs::read_link(e.path())
                .map(|t| t.to_string_lossy().starts_with("pipe:"))
                .unwrap_or(false)
        })
        .count()
}

/// When a grandchild of the dmcp child inherits the stderr pipe and outlives
/// it, the drain deadline must ABORT the reader, not just stop waiting for
/// it: a dropped-but-live reader keeps the pipe fd (and the ring) alive for
/// the daemon's lifetime, one leak per call to a daemonizing server.
#[cfg(target_os = "linux")]
#[test]
fn drain_timeout_releases_the_stderr_pipe_fd() {
    let mut s = Serve::start("live-tail");
    let baseline = pipe_fd_count(s.child.id());

    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "daemonize"}]}),
    );
    let exit = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "EXIT")
        .expect("EXIT must be pushed once the drain deadline passes");
    let e = exit["nonce"].as_str().unwrap();
    assert_eq!(
        exit["message"].as_str().unwrap(),
        format!("[hash={e}] 200 <{e}>daemon-started</{e}>"),
        "the lingering grandchild must not change the call's result"
    );

    // The abort lands asynchronously just before the EXIT is emitted; give
    // the scheduler a moment to drop the cancelled reader.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let open = pipe_fd_count(s.child.id());
        if open <= baseline {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "stderr pipe fd leaked past the drain timeout: {open} open pipes vs baseline {baseline}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// (e): a quick success is untouched end to end — EXIT inlines stdout under
/// the task nonce and get_output returns it verbatim.
#[test]
fn quick_exit_output_is_unchanged() {
    let mut s = Serve::start("live-tail");
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
