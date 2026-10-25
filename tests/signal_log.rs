//! Integration tests for reading the signal window as data (`log`).
//!
//! The daemon polls `log` to catch up on signals it was never pushed: a KILL,
//! which dispatch never pushes, and any push it lost. These drive the real
//! `dispatch serve` and read `structuredContent` the way an MCP client does: a
//! key outside the tool-result schema is dropped by the client, so asserting
//! on one would pass here and still give the daemon nothing.

#![cfg(unix)]

mod common;

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use common::Serve;

fn entries(s: &mut Serve) -> Vec<Value> {
    let res = s.call("log", json!({}));
    res["structuredContent"]["entries"]
        .as_array()
        .unwrap_or_else(|| panic!("log must return structuredContent.entries: {res}"))
        .clone()
}

fn find<'a>(window: &'a [Value], pid: u64, kind: &str) -> Option<&'a Value> {
    window.iter().find(|e| e["pid"] == pid && e["kind"] == kind)
}

#[test]
fn a_pushed_exit_reads_back_identically_from_the_log() {
    let mut s = Serve::start("signal-log");
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "quick"}]}),
    );
    let pushed = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "EXIT")
        .expect("EXIT must be pushed");

    let window = entries(&mut s);
    let logged = find(&window, 1, "EXIT").expect("the EXIT is in the window");
    // Same timestamp, message and nonce: the daemon's (pid, type, timestamp)
    // key agrees, so a signal seen both ways is delivered once.
    assert_eq!(logged, &pushed);
    let init = find(&window, 1, "INIT").expect("the INIT is in the window");
    assert!(init.get("held").is_none());
}

#[test]
fn a_kill_is_in_the_log_and_never_pushed() {
    let mut s = Serve::start("signal-log");
    s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "slow_progress", "remind_after": 0}]}),
    );
    s.call("kill", json!({"pids": [1]}));

    let window = entries(&mut s);
    let kill = find(&window, 1, "KILL").expect("the KILL is in the window");
    assert_eq!(kill["message"], "Terminated by LLM");
    assert!(
        s.wait_signal(Duration::from_secs(2), |d| d["kind"] == "KILL")
            .is_none(),
        "dispatch does not push a KILL; the log is the only way to read it"
    );
}

#[test]
fn an_exit_held_for_its_batch_is_marked_until_the_batch_is_pushed() {
    let mut s = Serve::start("signal-log");
    s.call(
        "dispatch",
        json!({"tasks": [
            {"server": "fake", "tool": "quick", "fire_wake": false},
            {"server": "fake", "tool": "slow_progress", "fire_wake": false, "remind_after": 0}
        ]}),
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    let held = loop {
        let window = entries(&mut s);
        if let Some(exit) = find(&window, 1, "EXIT") {
            break exit.clone();
        }
        assert!(Instant::now() < deadline, "quick never exited: {window:?}");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(held["held"], true, "held while its sibling runs: {held}");

    let batch = s
        .wait_for(Duration::from_secs(15), |v| {
            v["method"] == "notifications/message" && v["params"]["data"].is_array()
        })
        .expect("the settled group is pushed as one batch");
    assert_eq!(batch["params"]["data"].as_array().unwrap().len(), 2);

    let window = entries(&mut s);
    let exit = find(&window, 1, "EXIT").expect("still in the window");
    assert!(exit.get("held").is_none(), "pushed now: {exit}");
}
