use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};

#[derive(Default, Clone, Copy)]
pub struct Counts {
    pub calls: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct Stats {
    per_tool: BTreeMap<String, Counts>,
    per_workspace: BTreeMap<String, Counts>,
    logs: usize,
    first_ms: Option<u64>,
    last_ms: Option<u64>,
}

/// The MCP tool an operation belongs to. Operations with no tool of their own
/// keep their protocol name, so a log entry is never silently dropped.
fn display_name(op: &str) -> Option<&str> {
    Some(match op {
        "list" => "list_directory",
        "read" => "read_file",
        "create" => "create_file",
        "edit" => "edit_file",
        "delete" => "delete_file",
        "exec" => "run_command",
        "upload_prepare" => "upload_file",
        "download_record" => "download_file",
        // The second step of an upload already counted at its prepare, not a
        // separate call the agent chose to make.
        "upload_commit" | "upload_abort" => return None,
        other => other,
    })
}

/// Aggregates the request/response logs written by `remote-workspace-mcp
/// --log-dir`. One log per workspace, named `<workspace>.jsonl`.
pub fn collect(dir: &Path) -> Result<Stats> {
    let entries =
        std::fs::read_dir(dir).with_context(|| format!("read log directory {}", dir.display()))?;
    let mut stats = Stats::default();
    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .collect();
    paths.sort();
    for path in paths {
        let workspace = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        stats.read_log(&path, &workspace)?;
    }
    Ok(stats)
}

impl Stats {
    fn read_log(&mut self, path: &Path, workspace: &str) -> Result<()> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        // request_id -> tool, so the response line can be attributed. Ids carry
        // a timestamp and pid, so they stay unique across server restarts and
        // one map covers the whole file.
        let mut pending: BTreeMap<String, String> = BTreeMap::new();
        self.logs += 1;
        for line in text.lines() {
            let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let request_id = entry["request_id"].as_str().unwrap_or_default().to_string();
            if let Some(ts) = entry["ts_ms"].as_u64() {
                self.first_ms = Some(self.first_ms.map_or(ts, |t| t.min(ts)));
                self.last_ms = Some(self.last_ms.map_or(ts, |t| t.max(ts)));
            }
            match entry["kind"].as_str() {
                Some("request") => {
                    let Some(raw) = entry["line"].as_str() else {
                        continue;
                    };
                    let Ok(body) = serde_json::from_str::<serde_json::Value>(raw) else {
                        continue;
                    };
                    let Some(op) = body["op"].as_str() else {
                        continue;
                    };
                    let Some(name) = display_name(op) else {
                        continue;
                    };
                    self.per_tool.entry(name.to_string()).or_default().calls += 1;
                    self.per_workspace
                        .entry(workspace.to_string())
                        .or_default()
                        .calls += 1;
                    pending.insert(request_id, name.to_string());
                }
                Some("response") => {
                    // A protocol error carries a code where a result carries a
                    // type. Transport failures never produce a response line at
                    // all, so they are absent from these counts by nature.
                    if entry["message"]["code"].is_null() {
                        continue;
                    }
                    let Some(name) = pending.get(&request_id) else {
                        continue;
                    };
                    self.per_tool.entry(name.clone()).or_default().errors += 1;
                    self.per_workspace
                        .entry(workspace.to_string())
                        .or_default()
                        .errors += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        let total = self
            .per_tool
            .values()
            .fold(Counts::default(), |a, c| Counts {
                calls: a.calls + c.calls,
                errors: a.errors + c.errors,
            });
        if total.calls == 0 {
            return format!("no calls recorded in {} log file(s)\n", self.logs);
        }

        let mut rows: Vec<(&String, &Counts)> = self.per_tool.iter().collect();
        rows.sort_by_key(|(name, c)| (std::cmp::Reverse(c.calls), name.as_str()));
        out.push_str(&format!("{:<16} {:>8} {:>8}\n", "tool", "calls", "errors"));
        for (name, c) in rows {
            out.push_str(&format!("{:<16} {:>8} {:>8}\n", name, c.calls, c.errors));
        }
        out.push_str(&format!(
            "{:<16} {:>8} {:>8}\n",
            "total", total.calls, total.errors
        ));

        if self.per_workspace.len() > 1 {
            out.push_str("\nby workspace\n");
            let mut rows: Vec<(&String, &Counts)> = self.per_workspace.iter().collect();
            rows.sort_by_key(|(name, c)| (std::cmp::Reverse(c.calls), name.as_str()));
            for (name, c) in rows {
                out.push_str(&format!("{:<16} {:>8} {:>8}\n", name, c.calls, c.errors));
            }
        }

        if let (Some(first), Some(last)) = (self.first_ms, self.last_ms) {
            let days = (last.saturating_sub(first)) as f64 / 86_400_000.0;
            out.push_str(&format!(
                "\n{} log file(s), spanning {days:.1} days\n",
                self.logs
            ));
        }
        // Reads are absent from the server's operation log by design, so these
        // counts exist only for the period the MCP server ran with --log-dir.
        out.push_str("counts cover requests logged by remote-workspace-mcp --log-dir only\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_line(kind: &str, request_id: &str, op: &str, error: bool) -> String {
        match kind {
            "request" => serde_json::json!({
                "kind": "request", "request_id": request_id, "ts_ms": 1_000_000_u64,
                "line": serde_json::json!({"request_id": request_id, "op": op}).to_string(),
            })
            .to_string(),
            _ => {
                let message = if error {
                    serde_json::json!({"request_id": request_id, "code": "not_found", "message": "x"})
                } else {
                    serde_json::json!({"request_id": request_id, "type": "read"})
                };
                serde_json::json!({
                    "kind": "response", "request_id": request_id, "ts_ms": 1_000_001_u64,
                    "message": message,
                })
                .to_string()
            }
        }
    }

    #[test]
    fn counts_calls_and_errors_per_tool() {
        let dir = tempfile::tempdir().unwrap();
        let lines = [
            log_line("request", "r1", "exec", false),
            log_line("response", "r1", "exec", false),
            log_line("request", "r2", "read", false),
            log_line("response", "r2", "read", true),
            log_line("request", "r3", "exec", false),
            log_line("response", "r3", "exec", false),
            // An upload is one call, not two: the commit step is not counted.
            log_line("request", "r4", "upload_prepare", false),
            log_line("response", "r4", "upload_prepare", false),
            log_line("request", "r5", "upload_commit", false),
            log_line("response", "r5", "upload_commit", false),
        ];
        std::fs::write(dir.path().join("ws.jsonl"), lines.join("\n")).unwrap();

        let stats = collect(dir.path()).unwrap();
        assert_eq!(stats.per_tool["run_command"].calls, 2);
        assert_eq!(stats.per_tool["run_command"].errors, 0);
        assert_eq!(stats.per_tool["read_file"].calls, 1);
        assert_eq!(stats.per_tool["read_file"].errors, 1);
        assert_eq!(stats.per_tool["upload_file"].calls, 1);
        assert!(!stats.per_tool.contains_key("upload_commit"));
        assert_eq!(stats.per_workspace["ws"].calls, 4);

        let text = stats.render();
        assert!(text.contains("run_command"), "{text}");
        assert!(text.contains("total"), "{text}");
    }

    #[test]
    fn empty_log_dir_is_reported_not_crashed() {
        let dir = tempfile::tempdir().unwrap();
        let stats = collect(dir.path()).unwrap();
        assert!(stats.render().contains("no calls recorded"));
    }
}
