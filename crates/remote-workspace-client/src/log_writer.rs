use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use remote_workspace_protocol::ServerMessage;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Client-side interaction log: records requests sent and responses received
/// (including truncation flags carried in the responses). Append-only JSONL.
///
/// I/O errors (disk full, permissions) are surfaced via `tracing::error` and
/// permanently disable further logging so they are never silently swallowed.
#[derive(Clone)]
pub struct ClientLog {
    file: Arc<Mutex<tokio::fs::File>>,
    /// Set to true if any write/fsync fails. Once set, subsequent writes are
    /// silently skipped to avoid compounding errors.
    errored: Arc<AtomicBool>,
}

impl ClientLog {
    pub async fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            errored: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn log_request(&self, request_id: &str, raw_line: &str) {
        self.append_json(serde_json::json!({
            "kind": "request",
            "request_id": request_id,
            "ts_ms": now_ms(),
            "line": raw_line,
        }))
        .await;
    }

    pub async fn log_response(&self, request_id: &str, msg: &ServerMessage) {
        let body = serde_json::to_value(msg).unwrap_or(serde_json::Value::Null);
        self.append_json(serde_json::json!({
            "kind": "response",
            "request_id": request_id,
            "ts_ms": now_ms(),
            "message": body,
        }))
        .await;
    }

    pub async fn log_raw(&self, request_id: &str, raw_line: &str) {
        self.append_json(serde_json::json!({
            "kind": "raw",
            "request_id": request_id,
            "ts_ms": now_ms(),
            "line": raw_line,
        }))
        .await;
    }

    async fn append_json(&self, value: serde_json::Value) {
        if self.errored.load(Ordering::Relaxed) {
            return;
        }
        let mut line = serde_json::to_string(&value).unwrap_or_default();
        line.push('\n');
        let mut f = self.file.lock().await;
        if let Err(e) = f.write_all(line.as_bytes()).await {
            tracing::error!(error = %e, "client log write failed; disabling further logging");
            self.errored.store(true, Ordering::Relaxed);
            return;
        }
        if let Err(e) = f.flush().await {
            tracing::error!(error = %e, "client log flush failed; disabling further logging");
            self.errored.store(true, Ordering::Relaxed);
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
