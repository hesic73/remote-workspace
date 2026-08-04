use std::io::Write;
use std::path::{Path, PathBuf};

/// Trim the log at startup once it grows past this, keeping the newest lines.
const MAX_BYTES: u64 = 256 * 1024;
const KEEP_LINES: usize = 200;

/// Append-only lifecycle record at `<state_dir>/server.jsonl`: one JSON line
/// per event, with the pid that wrote it.
///
/// The server's stderr belongs to whoever is attached over SSH. When a server
/// outlives the client that started it -- the case that leaves a workspace
/// looking occupied -- nobody is attached, so this file is the only thing on
/// the remote that can say when that process started and whether it ever
/// exited. Kept deliberately coarse (start, lock refusal, exit): per-request
/// detail belongs in the operation log, which already has it.
pub struct SessionLog {
    path: PathBuf,
}

impl SessionLog {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            path: state_dir.join("server.jsonl"),
        }
    }

    pub fn record(&self, event: &str, fields: serde_json::Value) {
        let mut line = serde_json::json!({
            "ts_ms": now_ms(),
            "pid": std::process::id(),
            "event": event,
        });
        if let (Some(obj), serde_json::Value::Object(extra)) = (line.as_object_mut(), fields) {
            obj.extend(extra);
        }
        if let Err(e) = self.append(&line.to_string()) {
            // Same rule as the client log: a diagnostic that cannot be written
            // is reported, never swallowed. It must not take the server down.
            tracing::error!(path = ?self.path, error = %e, "session log write failed");
        }
    }

    fn append(&self, line: &str) -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{line}")
    }

    /// Drop all but the newest `KEEP_LINES` lines once the file passes the size
    /// cap. Called once at startup, so a workspace reconnected many times a day
    /// for years still costs a bounded amount of remote disk.
    pub fn trim_if_large(&self) {
        match std::fs::metadata(&self.path) {
            Ok(m) if m.len() > MAX_BYTES => {}
            _ => return,
        }
        if let Err(e) = self.trim() {
            tracing::error!(path = ?self.path, error = %e, "session log trim failed");
        }
    }

    fn trim(&self) -> std::io::Result<()> {
        let text = std::fs::read_to_string(&self.path)?;
        let kept: Vec<&str> = text
            .lines()
            .skip(text.lines().count().saturating_sub(KEEP_LINES))
            .collect();
        let dir = self.path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "session log has no parent",
            )
        })?;
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        for line in kept {
            writeln!(tmp, "{line}")?;
        }
        tmp.persist(&self.path)
            .map_err(|e| std::io::Error::other(format!("persist {:?}: {e}", self.path)))?;
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_one_json_line_per_event_with_pid() {
        let dir = tempfile::tempdir().unwrap();
        let log = SessionLog::new(dir.path());
        log.record("started", serde_json::json!({ "root": "/ws" }));
        log.record("exit", serde_json::json!({ "reason": "stdin_eof" }));

        let text = std::fs::read_to_string(dir.path().join("server.jsonl")).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["event"], "started");
        assert_eq!(lines[0]["root"], "/ws");
        assert_eq!(lines[0]["pid"], std::process::id());
        assert!(lines[0]["ts_ms"].as_u64().unwrap() > 0);
        assert_eq!(lines[1]["reason"], "stdin_eof");
    }

    #[test]
    fn trim_keeps_the_newest_lines_once_over_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let log = SessionLog::new(dir.path());
        // Enough lines to pass both bounds: over the byte cap and over the
        // number of lines trimming keeps.
        let filler = "x".repeat(1024);
        for i in 0..(MAX_BYTES as usize / 1024).max(KEEP_LINES + 50) {
            log.record("started", serde_json::json!({ "n": i, "pad": filler }));
        }
        let before = std::fs::read_to_string(&log.path).unwrap();
        assert!(before.lines().count() > KEEP_LINES);
        let last = before.lines().next_back().unwrap().to_string();

        log.trim_if_large();

        let after = std::fs::read_to_string(&log.path).unwrap();
        assert_eq!(after.lines().count(), KEEP_LINES);
        assert_eq!(after.lines().next_back().unwrap(), last);
    }

    #[test]
    fn trim_leaves_a_small_log_alone() {
        let dir = tempfile::tempdir().unwrap();
        let log = SessionLog::new(dir.path());
        log.record("started", serde_json::json!({}));
        log.trim_if_large();
        assert_eq!(
            std::fs::read_to_string(&log.path).unwrap().lines().count(),
            1
        );
    }
}
