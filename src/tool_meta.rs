//! Per-tool metadata a server manifest declares about its own tools, and the
//! reminder policy dispatch derives from it (#38, mcp-registry #68).
//!
//! A tool that parks waiting for input — an installer with no `-y`, a shell job
//! that asks a question — produces nothing further until someone answers. A
//! caller that dispatched it without `remind_after` is never woken, so the LLM
//! goes idle believing work is still in flight. `blocking: true` is the tool
//! author's own declaration that this can happen; this module turns that
//! declaration into a reminder interval, and records that dispatch (not the
//! caller) chose it.

use serde_json::Value;

/// Reminder interval for a tool whose manifest marks it `blocking` but names no
/// `suggestedRemindAfter`. Long enough that ordinary work is not announced every
/// few seconds, short enough that a tool parked on a question is noticed while
/// the answer still matters.
pub const DEFAULT_BLOCKING_REMIND_SECS: u64 = 30;

/// What a server manifest declares about one of its tools. Both keys are
/// optional and opt-in: a manifest that declares neither yields
/// `ToolMeta::default()`, which is today's behavior exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ToolMeta {
    /// The tool can park indefinitely awaiting input.
    pub blocking: bool,
    /// The reminder interval the server recommends for this tool, in seconds.
    pub suggested_remind_after: Option<u64>,
}

impl ToolMeta {
    /// Read the entry for `tool` out of a manifest's `tools` array (the shape
    /// `dmcp info <id> --json` prints). Returns `None` when the manifest has no
    /// entry for this tool — indistinguishable, by design, from an entry that
    /// declares nothing.
    pub fn from_manifest(manifest: &Value, tool: &str) -> Option<Self> {
        let entry = manifest
            .get("tools")?
            .as_array()?
            .iter()
            .find(|t| t.get("name").and_then(Value::as_str) == Some(tool))?;
        Some(Self {
            // Only a literal `true` opts in. Absent, `false`, or a non-bool
            // (a manifest typo) all leave the tool non-blocking: a key that
            // cannot be read must not start arming timers nobody asked for.
            blocking: entry
                .get("blocking")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            // A non-positive or non-integer suggestion is no suggestion. It must
            // not error — the key is optional and advisory — and it must not
            // become a way for a manifest to disable the reminder its own
            // `blocking` flag exists to request.
            suggested_remind_after: entry
                .get("suggestedRemindAfter")
                .and_then(Value::as_u64)
                .filter(|secs| *secs > 0),
        })
    }
}

/// Where an auto-applied interval came from, for the INIT disclosure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemindSource {
    /// The tool's manifest named it (`suggestedRemindAfter`).
    ToolSuggested,
    /// The manifest marked the tool blocking but named no interval.
    BuiltInDefault,
}

/// The reminder decision for one dispatched task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemindPlan {
    /// No reminder timer: the caller opted out with `remind_after: 0`, or
    /// nothing asked for one.
    None,
    /// The caller's own interval.
    Caller(u64),
    /// An interval dispatch chose because the tool declares itself blocking and
    /// the caller expressed no preference. Disclosed on the task's INIT signal
    /// and in `status`.
    Applied { secs: u64, source: RemindSource },
}

impl RemindPlan {
    /// The interval to arm, if any.
    pub fn interval(self) -> Option<u64> {
        match self {
            RemindPlan::None => None,
            RemindPlan::Caller(secs) => Some(secs),
            RemindPlan::Applied { secs, .. } => Some(secs),
        }
    }

    /// The interval only when dispatch chose it itself — what `status` reports
    /// so a caller can tell the reminder was not its own doing.
    pub fn auto_applied(self) -> Option<u64> {
        match self {
            RemindPlan::Applied { secs, .. } => Some(secs),
            _ => None,
        }
    }

    /// Note appended to the task's INIT message when dispatch chose the
    /// interval. `None` when the caller decided: there is nothing to disclose,
    /// and the INIT text stays exactly what it has always been.
    pub fn applied_note(self) -> Option<String> {
        match self {
            RemindPlan::Applied {
                secs,
                source: RemindSource::ToolSuggested,
            } => Some(format!(
                "[auto-remind {secs}s — tool declares blocking, interval suggested by its manifest]"
            )),
            RemindPlan::Applied {
                secs,
                source: RemindSource::BuiltInDefault,
            } => Some(format!(
                "[auto-remind {secs}s — tool declares blocking, manifest suggested none, dispatch default]"
            )),
            _ => None,
        }
    }
}

/// Decide a task's reminder from what the caller asked for and what the tool's
/// manifest declares.
///
/// The caller always wins, including the opt-out: `remind_after: 0` means "no
/// reminder, and I mean it", so a blocking tool dispatched that way stays
/// silent. Only an *absent* `remind_after` is treated as no preference, which
/// is what lets a blocking tool's suggestion apply.
pub fn plan_reminder(caller: Option<u64>, meta: Option<ToolMeta>) -> RemindPlan {
    match caller {
        Some(0) => RemindPlan::None,
        Some(secs) => RemindPlan::Caller(secs),
        None => match meta {
            // `suggestedRemindAfter` without `blocking: true` is meaningless,
            // not an error: it is simply never read here.
            Some(m) if m.blocking => match m.suggested_remind_after {
                Some(secs) => RemindPlan::Applied {
                    secs,
                    source: RemindSource::ToolSuggested,
                },
                None => RemindPlan::Applied {
                    secs: DEFAULT_BLOCKING_REMIND_SECS,
                    source: RemindSource::BuiltInDefault,
                },
            },
            _ => RemindPlan::None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest(tools: Value) -> Value {
        json!({ "id": "srv", "tools": tools })
    }

    #[test]
    fn reads_blocking_and_suggestion_for_the_named_tool() {
        let m = manifest(json!([
            {"name": "other", "blocking": true, "suggestedRemindAfter": 99},
            {"name": "run_job", "blocking": true, "suggestedRemindAfter": 45}
        ]));
        assert_eq!(
            ToolMeta::from_manifest(&m, "run_job"),
            Some(ToolMeta {
                blocking: true,
                suggested_remind_after: Some(45)
            })
        );
    }

    #[test]
    fn absent_keys_mean_the_pre_change_default() {
        let m = manifest(json!([{"name": "add", "description": "adds"}]));
        assert_eq!(
            ToolMeta::from_manifest(&m, "add"),
            Some(ToolMeta::default())
        );
        assert!(!ToolMeta::from_manifest(&m, "add").unwrap().blocking);
    }

    #[test]
    fn unknown_tool_or_missing_tools_array_yields_no_metadata() {
        let m = manifest(json!([{"name": "add"}]));
        assert_eq!(ToolMeta::from_manifest(&m, "nope"), None);
        assert_eq!(ToolMeta::from_manifest(&json!({"id": "srv"}), "add"), None);
        assert_eq!(
            ToolMeta::from_manifest(&manifest(json!("not-an-array")), "add"),
            None
        );
        // String tool entries carry no metadata and must not panic.
        assert_eq!(
            ToolMeta::from_manifest(&manifest(json!(["add"])), "add"),
            None
        );
    }

    #[test]
    fn malformed_declarations_fall_back_instead_of_erroring() {
        let m = manifest(json!([
            {"name": "a", "blocking": "yes", "suggestedRemindAfter": "soon"},
            {"name": "b", "blocking": true, "suggestedRemindAfter": 0},
            {"name": "c", "blocking": true, "suggestedRemindAfter": -5},
            {"name": "d", "blocking": true, "suggestedRemindAfter": 2.5}
        ]));
        assert_eq!(ToolMeta::from_manifest(&m, "a"), Some(ToolMeta::default()));
        for tool in ["b", "c", "d"] {
            let meta = ToolMeta::from_manifest(&m, tool).unwrap();
            assert!(meta.blocking);
            assert_eq!(
                meta.suggested_remind_after, None,
                "an unreadable suggestion is no suggestion ({tool})"
            );
            assert_eq!(
                plan_reminder(None, Some(meta)),
                RemindPlan::Applied {
                    secs: DEFAULT_BLOCKING_REMIND_SECS,
                    source: RemindSource::BuiltInDefault
                },
                "a blocking tool still gets a reminder ({tool})"
            );
        }
    }

    #[test]
    fn caller_interval_always_wins() {
        let blocking = Some(ToolMeta {
            blocking: true,
            suggested_remind_after: Some(45),
        });
        assert_eq!(plan_reminder(Some(7), blocking), RemindPlan::Caller(7));
        assert_eq!(plan_reminder(Some(7), None), RemindPlan::Caller(7));
        assert!(plan_reminder(Some(7), blocking).auto_applied().is_none());
        assert!(plan_reminder(Some(7), blocking).applied_note().is_none());
    }

    #[test]
    fn zero_is_the_explicit_opt_out_even_for_a_blocking_tool() {
        let blocking = Some(ToolMeta {
            blocking: true,
            suggested_remind_after: Some(45),
        });
        assert_eq!(plan_reminder(Some(0), blocking), RemindPlan::None);
        assert_eq!(plan_reminder(Some(0), blocking).interval(), None);
    }

    #[test]
    fn blocking_tool_without_caller_interval_uses_the_suggestion() {
        let plan = plan_reminder(
            None,
            Some(ToolMeta {
                blocking: true,
                suggested_remind_after: Some(45),
            }),
        );
        assert_eq!(
            plan,
            RemindPlan::Applied {
                secs: 45,
                source: RemindSource::ToolSuggested
            }
        );
        assert_eq!(plan.interval(), Some(45));
        assert_eq!(plan.auto_applied(), Some(45));
        assert!(plan.applied_note().unwrap().contains("auto-remind 45s"));
    }

    #[test]
    fn blocking_tool_without_a_suggestion_uses_the_builtin_default() {
        let plan = plan_reminder(
            None,
            Some(ToolMeta {
                blocking: true,
                suggested_remind_after: None,
            }),
        );
        assert_eq!(plan.interval(), Some(DEFAULT_BLOCKING_REMIND_SECS));
        assert!(plan.applied_note().unwrap().contains("dispatch default"));
    }

    #[test]
    fn non_blocking_tool_without_caller_interval_is_unchanged() {
        // Including the meaningless-but-legal suggestion-without-blocking case.
        let suggestion_only = Some(ToolMeta {
            blocking: false,
            suggested_remind_after: Some(45),
        });
        assert_eq!(plan_reminder(None, suggestion_only), RemindPlan::None);
        assert_eq!(
            plan_reminder(None, Some(ToolMeta::default())),
            RemindPlan::None
        );
        assert_eq!(plan_reminder(None, None), RemindPlan::None);
    }
}
