//! Integration tests for reminders on tools that can park awaiting input (#38).
//!
//! Driven against the real `dispatch serve` binary with a fake `dmcp` on PATH
//! (`tests/fixtures/fake_dmcp.sh`) that serves manifests from `info` and a
//! `run_job` tool that asks a question and then waits. What matters here is
//! what the client actually receives: the INIT text returned by the dispatch
//! call, the `status` response, and the REMIND notification the binary pushes.

#![cfg(unix)]

mod common;

use std::time::Duration;

use serde_json::json;

use common::Serve;
use dispatch::tool_meta::DEFAULT_BLOCKING_REMIND_SECS;

/// The reason this issue exists: a task on a blocking tool, dispatched with no
/// `remind_after`, used to produce no wake-up at all. It must now arm the
/// manifest's suggested interval, disclose that dispatch did so, and actually
/// EMIT the REMIND to the client.
#[test]
fn blocking_tool_without_remind_after_emits_an_auto_armed_remind() {
    let mut s = Serve::start("blocking-auto");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "blocking", "tool": "run_job"}]}),
    );

    let init = res["content"][0]["text"].as_str().unwrap();
    assert!(
        init.contains("auto-remind 2s") && init.contains("tool declares blocking"),
        "INIT must disclose the reminder dispatch armed itself, got: {init}"
    );

    let status = s.call("status", json!({}));
    assert_eq!(
        status["tasks"][0]["auto_remind_after"],
        json!(2),
        "status must report the injected interval: {status}"
    );

    let remind = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "REMIND")
        .expect("a REMIND must be PUSHED to the client, not merely recorded");
    let msg = remind["message"].as_str().unwrap();
    assert!(
        msg.starts_with("Running for 2s"),
        "the reminder must fire at the manifest's suggested interval, got: {msg}"
    );
    assert_eq!(remind["pid"], json!(1));
}

/// An explicit interval always wins over the manifest's suggestion, and nothing
/// is disclosed — dispatch decided nothing.
#[test]
fn explicit_remind_after_wins_over_the_manifest_suggestion() {
    let mut s = Serve::start("blocking-explicit");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "blocking", "tool": "run_job", "remind_after": 1}]}),
    );

    let init = res["content"][0]["text"].as_str().unwrap();
    assert!(
        !init.contains("auto-remind"),
        "the caller's own interval is not an injection, got: {init}"
    );
    let status = s.call("status", json!({}));
    assert!(
        status["tasks"][0].get("auto_remind_after").is_none(),
        "status must not claim dispatch chose the caller's interval: {status}"
    );

    let remind = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "REMIND")
        .expect("REMIND pushed");
    let msg = remind["message"].as_str().unwrap();
    assert!(
        msg.starts_with("Running for 1s"),
        "the caller's 1s must be used, not the manifest's 2s, got: {msg}"
    );
}

/// `remind_after: 0` is the explicit opt-out and outranks the manifest too: a
/// caller that says "no reminder" gets none, even on a blocking tool.
#[test]
fn explicit_zero_opts_out_of_reminders_on_a_blocking_tool() {
    let mut s = Serve::start("blocking-optout");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "blocking", "tool": "run_job", "remind_after": 0}]}),
    );
    assert!(!res["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("auto-remind"));

    assert!(
        s.wait_signal(Duration::from_secs(6), |d| d["kind"] == "REMIND")
            .is_none(),
        "no REMIND may be pushed after an explicit opt-out"
    );
    // Not a vacuous absence: the task ran on past several 2s intervals and only
    // then exited.
    s.wait_signal(Duration::from_secs(15), |d| d["kind"] == "EXIT")
        .expect("the task must still run to completion");
}

/// The regression that matters most: a tool that declares nothing behaves
/// exactly as before — no reminder, no disclosure, byte-identical status.
#[test]
fn non_blocking_tool_without_remind_after_is_unchanged() {
    let mut s = Serve::start("blocking-none");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "fake", "tool": "slow_progress"}]}),
    );
    assert!(!res["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("auto-remind"));

    let status = s.call("status", json!({}));
    assert_eq!(
        status["tasks"],
        json!([{
            "pid": 1,
            "type": "mcp",
            "server": "fake",
            "tool": "slow_progress",
            "state": "running"
        }]),
        "the status shape must not gain keys for an ordinary task"
    );

    s.wait_signal(Duration::from_secs(15), |d| d["kind"] == "EXIT")
        .expect("EXIT pushed");
    assert!(
        s.wait_signal(Duration::from_millis(200), |d| d["kind"] == "REMIND")
            .is_none(),
        "a tool that declares nothing must get no reminder"
    );
}

/// A manifest that marks a tool blocking but suggests no interval falls back to
/// dispatch's built-in default, and says which it used. The emission itself is
/// covered by the suggested-interval test above — waiting out the 30s default
/// would only re-prove the same path.
#[test]
fn blocking_without_a_suggestion_uses_the_builtin_default() {
    let mut s = Serve::start("blocking-default");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "blocking_no_suggestion", "tool": "run_job"}]}),
    );

    let init = res["content"][0]["text"].as_str().unwrap();
    assert!(
        init.contains(&format!("auto-remind {DEFAULT_BLOCKING_REMIND_SECS}s"))
            && init.contains("dispatch default"),
        "INIT must name the interval and that it was dispatch's own, got: {init}"
    );
    let status = s.call("status", json!({}));
    assert_eq!(
        status["tasks"][0]["auto_remind_after"],
        json!(DEFAULT_BLOCKING_REMIND_SECS)
    );
}

/// A manifest dispatch cannot read (server unknown to dmcp, dmcp missing) must
/// never fail a dispatch: the task runs exactly as it did before this feature.
#[test]
fn unreadable_manifest_falls_back_to_the_pre_change_behavior() {
    let mut s = Serve::start("blocking-nomanifest");
    let res = s.call(
        "dispatch",
        json!({"tasks": [{"server": "not_in_any_manifest", "tool": "quick"}]}),
    );
    assert_eq!(res["pids"], json!([1]));
    assert!(!res["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("auto-remind"));

    let exit = s
        .wait_signal(Duration::from_secs(10), |d| d["kind"] == "EXIT")
        .expect("EXIT pushed");
    let h = exit["nonce"].as_str().unwrap();
    assert_eq!(
        exit["message"].as_str().unwrap(),
        format!("[hash={h}] 200 <{h}>quick-result</{h}>")
    );
}
