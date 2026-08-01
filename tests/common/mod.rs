//! Shared harness for the integration tests: a real `dispatch serve` driven
//! over stdio JSON-RPC, with a fake `dmcp` (`tests/fixtures/fake_dmcp.sh`)
//! planted first on its PATH. Assertions are made against what the binary
//! actually writes — responses AND pushed notifications — so a signal that is
//! recorded but never emitted to the LLM fails the test.

// Each test binary includes this module and uses a different part of it.
#![allow(dead_code)]

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
pub struct Serve {
    pub child: Child,
    stdin: ChildStdin,
    rx: mpsc::Receiver<Value>,
    stash: Vec<Value>,
    next_id: u64,
    root: PathBuf,
}

impl Serve {
    /// Start a server in its own temp PATH root. `label` only names that
    /// directory, so a failing run is traceable to the test that made it.
    pub fn start(label: &str) -> Self {
        let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("dispatch-{}-{}-{}", label, std::process::id(), n));
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

    pub fn send(&mut self, v: &Value) {
        writeln!(self.stdin, "{v}").expect("write to dispatch stdin");
        self.stdin.flush().expect("flush dispatch stdin");
    }

    pub fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        self.wait_for(Duration::from_secs(30), |v| {
            v.get("id").and_then(Value::as_u64) == Some(id)
        })
        .unwrap_or_else(|| panic!("no response to {method} (id {id})"))
    }

    /// tools/call that must succeed; returns the `result` object.
    pub fn call(&mut self, tool: &str, args: Value) -> Value {
        let resp = self.request("tools/call", json!({"name": tool, "arguments": args}));
        resp.get("result")
            .cloned()
            .unwrap_or_else(|| panic!("tool {tool} errored: {resp}"))
    }

    /// Pull messages (stashing non-matches so nothing is lost) until `pred`
    /// matches or the timeout passes.
    pub fn wait_for(&mut self, timeout: Duration, pred: impl Fn(&Value) -> bool) -> Option<Value> {
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
    pub fn wait_signal(
        &mut self,
        timeout: Duration,
        pred: impl Fn(&Value) -> bool,
    ) -> Option<Value> {
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
pub fn wrapped_text<'a>(message: &'a str, h: &str) -> &'a str {
    let open = format!("<{h}>");
    let close = format!("</{h}>");
    let start = message.find(&open).expect("open boundary tag") + open.len();
    let end = message.rfind(&close).expect("close boundary tag");
    &message[start..end]
}
