use std::process::Stdio;
use std::sync::Arc;

use agent_remote_protocol::{
    ErrorCode, ExecResult, FileEntry, ListResult, MutationResult, OperationDetails, ProtocolError,
    ReadResult, Request, RequestBody, RequestId, RequestStatusResult, ServerMessage, UndoResult,
};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};

pub mod deploy;
pub mod fleet;
mod log_writer;
mod transfer;

pub use log_writer::ClientLog;
pub use transfer::{download_file, upload_file, Endpoint};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("protocol error from server: {0}")]
    Server(ProtocolError),
    #[error("transport io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server closed connection")]
    Closed,
    #[error("request timed out")]
    Timeout,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("transfer failed: {0}")]
    Transfer(String),
}

type DispMap = Arc<Mutex<std::collections::HashMap<RequestId, oneshot::Sender<ServerMessage>>>>;

/// Hard cap on how long a non-streaming request waits for a reply. Guards
/// against a server that stays connected but never responds. Long-running work
/// goes through `exec`, which has its own server-side `timeout_ms`.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 5 * 60 * 1000;

/// Spawns the remote process (ssh or local). Implementations return the child
/// and its stdin/stdout pipes.
pub trait Transport: Send {
    fn spawn(&mut self) -> std::io::Result<(Child, ChildStdin, ChildStdout)>;
}

/// Default transport: spawns the given argv as a subprocess. For SSH use
/// argv like `["ssh", host, "agent-remote-server", "--root", path]`; for tests
/// use the local server binary directly.
pub struct ArgvTransport {
    pub argv: Vec<String>,
}

impl Transport for ArgvTransport {
    fn spawn(&mut self) -> std::io::Result<(Child, ChildStdin, ChildStdout)> {
        let mut cmd = Command::new(&self.argv[0]);
        cmd.args(&self.argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        // Die with the parent: if the consumer (CLI/MCP) is killed -- even
        // with SIGKILL, where no destructor runs -- the transport child must
        // not outlive it as an orphan holding the remote session (and the
        // server-side state lock) open.
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok((child, stdin, stdout))
    }
}

pub struct Client {
    stdin: Arc<Mutex<ChildStdin>>,
    reply_map: DispMap,
    /// Persistent close flag: once the transport EOFs, this stays true so any
    /// later request on this Client fails immediately instead of hanging.
    closed: Arc<std::sync::atomic::AtomicBool>,
    closed_notify: Arc<tokio::sync::Notify>,
    log: Option<Arc<ClientLog>>,
}

impl Client {
    pub async fn connect<T: Transport + 'static>(
        mut transport: T,
        log: Option<ClientLog>,
    ) -> Result<Self, ClientError> {
        let (mut _child, stdin, stdout) = transport.spawn()?;
        // Keep the child alive for the connection lifetime by leaking its
        // handle into a detached task that also reaps it on drop. We hold it
        // via the reader task.
        let stdin = Arc::new(Mutex::new(stdin));
        let reply_map: DispMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_notify = Arc::new(tokio::sync::Notify::new());
        let log = log.map(Arc::new);

        let reader_reply = reply_map.clone();
        let drain_reply = reply_map.clone();
        let reader_closed = closed.clone();
        let reader_notify = closed_notify.clone();
        let reader_log = log.clone();
        tokio::spawn(async move {
            reader_loop(stdout, reader_reply, reader_log).await;
            // Mark the connection persistently closed so future requests on this
            // Client fail fast, then wake any current waiters.
            reader_closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drain_waiters(&drain_reply).await;
            reader_notify.notify_waiters();
            // When stdout ends, the child has exited.
            let _ = _child.wait().await;
        });

        Ok(Self {
            stdin,
            reply_map,
            closed,
            closed_notify,
            log,
        })
    }

    /// True once the transport has EOF'd. A closed Client never recovers;
    /// callers that need resilience must build a fresh Client.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolves once the connection is closed. Checks the persistent flag first
    /// (so a Client reused after EOF returns immediately) and otherwise waits
    /// for the reader task's notify.
    async fn wait_closed(&self) {
        if self.is_closed() {
            return;
        }
        loop {
            // Either the flag is already set, or we register for the notify
            // before re-checking, avoiding the lost-wakeup window of a bare
            // Notify (which does not remember past notifications).
            let notified = self.closed_notify.notified();
            if self.is_closed() {
                return;
            }
            notified.await;
            if self.is_closed() {
                return;
            }
        }
    }

    fn next_request_id(&self) -> RequestId {
        format!("req-{}", unique_id())
    }

    async fn send_request(
        &self,
        body: RequestBody,
    ) -> Result<(RequestId, ServerMessage), ClientError> {
        self.send_request_with_timeout(body, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    async fn send_request_with_timeout(
        &self,
        body: RequestBody,
        timeout: std::time::Duration,
    ) -> Result<(RequestId, ServerMessage), ClientError> {
        if self.is_closed() {
            return Err(ClientError::Closed);
        }
        let request_id = self.next_request_id();
        let req = Request {
            request_id: request_id.clone(),
            body,
        };
        let (tx, rx) = oneshot::channel::<ServerMessage>();
        self.reply_map.lock().await.insert(request_id.clone(), tx);
        let line = serde_json::to_string(&req)?;
        if let Some(l) = &self.log {
            l.log_request(&request_id, &line).await;
        }
        {
            let mut w = self.stdin.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            w.flush().await?;
        }
        // Race the reply against connection close and a hard request timeout:
        // if the server/SSH disappears, the reader drains reply_map (closing
        // `tx`) and notifies here; if the server stalls, the timeout fires.
        // Either way we never block indefinitely.
        let msg = tokio::select! {
            biased;
            () = self.wait_closed() => return Err(ClientError::Closed),
            () = tokio::time::sleep(timeout) => {
                self.reply_map.lock().await.remove(&request_id);
                return Err(ClientError::Timeout);
            }
            m = rx => m.map_err(|_| ClientError::Closed)?,
        };
        if let Some(l) = &self.log {
            l.log_response(&request_id, &msg).await;
        }
        Ok((request_id, msg))
    }

    fn unpack(msg: ServerMessage) -> Result<agent_remote_protocol::ResultBody, ClientError> {
        match msg {
            ServerMessage::Result { result, .. } => Ok(result),
            ServerMessage::Error { error, .. } => Err(ClientError::Server(error)),
        }
    }

    pub async fn list(
        &self,
        path: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<ListResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::List {
                path: path.into(),
                offset,
                limit,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::List(result) => Ok(result),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for list",
            ))),
        }
    }

    pub async fn stat(&self, path: &str) -> Result<FileEntry, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Stat { path: path.into() })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Stat { stat } => Ok(stat),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for stat",
            ))),
        }
    }

    pub async fn read(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<ReadResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Read {
                path: path.into(),
                offset,
                limit,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Read(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for read",
            ))),
        }
    }

    /// Create a new text file; fails if the target already exists.
    pub async fn create(&self, path: &str, content: &str) -> Result<MutationResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Create {
                path: path.into(),
                content: content.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Mutation(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for create",
            ))),
        }
    }

    /// Replace an exact occurrence of `old_text` with `new_text` in an
    /// existing file. `base_hash` is required for optimistic concurrency.
    pub async fn edit(
        &self,
        path: &str,
        base_hash: &str,
        old_text: &str,
        new_text: &str,
        replace_all: bool,
    ) -> Result<MutationResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Edit {
                path: path.into(),
                base_hash: base_hash.into(),
                old_text: old_text.into(),
                new_text: new_text.into(),
                replace_all,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Mutation(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for edit",
            ))),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<MutationResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Delete { path: path.into() })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Mutation(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for delete",
            ))),
        }
    }

    /// Run a command synchronously and return its bounded output preview.
    pub async fn exec(
        &self,
        argv: Vec<String>,
        cwd: Option<String>,
        profile: Option<String>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecResult, ClientError> {
        let wait = std::time::Duration::from_millis(
            timeout_ms
                .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS)
                .saturating_add(30_000),
        );
        let (_, msg) = self
            .send_request_with_timeout(
                RequestBody::Exec {
                    argv,
                    cwd,
                    profile,
                    timeout_ms,
                },
                wait,
            )
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Exec(result) => Ok(result),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for exec",
            ))),
        }
    }

    pub async fn upload_prepare(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<agent_remote_protocol::UploadPrepareResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::UploadPrepare {
                path: path.into(),
                overwrite,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::UploadPrepare(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for upload_prepare",
            ))),
        }
    }

    pub async fn upload_commit(
        &self,
        transfer_id: &str,
        size: u64,
        sha256: &str,
        duration_ms: u64,
    ) -> Result<agent_remote_protocol::TransferResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::UploadCommit {
                transfer_id: transfer_id.into(),
                size,
                sha256: sha256.into(),
                duration_ms,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Transfer(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for upload_commit",
            ))),
        }
    }

    pub async fn upload_abort(&self, transfer_id: &str) -> Result<(), ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::UploadAbort {
                transfer_id: transfer_id.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::UploadAbort { .. } => Ok(()),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for upload_abort",
            ))),
        }
    }

    pub async fn download_record(
        &self,
        path: &str,
        size: u64,
        sha256: &str,
        duration_ms: u64,
    ) -> Result<agent_remote_protocol::TransferResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::DownloadRecord {
                path: path.into(),
                size,
                sha256: sha256.into(),
                duration_ms,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Transfer(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for download_record",
            ))),
        }
    }

    pub async fn undo(&self, operation_id: &str) -> Result<UndoResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Undo {
                operation_id: operation_id.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Undo(u) => Ok(u),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for undo",
            ))),
        }
    }

    pub async fn history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<agent_remote_protocol::AnyOperationRecord>, ClientError> {
        let (_, msg) = self.send_request(RequestBody::History { limit }).await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::History { operations } => Ok(operations),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for history",
            ))),
        }
    }

    pub async fn operation_get(&self, operation_id: &str) -> Result<OperationDetails, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::OperationGet {
                operation_id: operation_id.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Operation(o) => Ok(o),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for operation_get",
            ))),
        }
    }

    pub async fn gc(
        &self,
        keep: Option<usize>,
    ) -> Result<agent_remote_protocol::GcResult, ClientError> {
        let (_, msg) = self.send_request(RequestBody::Gc { keep }).await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Gc(g) => Ok(g),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for gc",
            ))),
        }
    }

    pub async fn request_status(&self, target: &str) -> Result<RequestStatusResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::RequestStatus {
                target: target.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::RequestStatus(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for request_status",
            ))),
        }
    }
}

/// On connection close, fail every waiter by removing and dropping their
/// senders. Dropping a oneshot sender wakes the request with Closed.
async fn drain_waiters(reply_map: &DispMap) {
    reply_map.lock().await.clear();
}

async fn reader_loop(stdout: ChildStdout, reply_map: DispMap, log: Option<Arc<ClientLog>>) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                warn!(error = ?e, "client reader error");
                break;
            }
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: ServerMessage = match serde_json::from_str(trimmed) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, line = trimmed, "could not parse server message");
                continue;
            }
        };
        let rid = match &msg {
            ServerMessage::Result { request_id, .. } | ServerMessage::Error { request_id, .. } => {
                request_id.clone()
            }
        };
        if let Some(l) = &log {
            l.log_raw(&rid, trimmed).await;
        }
        debug!(request_id = %rid, "recv");
        let reply_tx = { reply_map.lock().await.remove(&rid) };
        if let Some(tx) = reply_tx {
            let _ = tx.send(msg);
        } else {
            warn!(request_id = %rid, "no handler for message");
        }
    }
}

/// Shell-quote a string for safe use inside a remote command line: wrapped in
/// single quotes, embedded single quotes escaped.
pub fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Request IDs must be globally unique because the server dedupes on them for
/// idempotent replay. Timestamp separates processes over time, pid separates
/// concurrent processes, and the counter separates requests within a process.
fn unique_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{ts:016x}-{:08x}-{n:08x}", std::process::id())
}

#[cfg(test)]
mod quote_tests {
    use super::{shell_quote, Endpoint};

    #[test]
    fn quotes_empty_spaces_and_metacharacters() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("$(rm -rf /);`x`|&"), "'$(rm -rf /);`x`|&'");
        assert_eq!(shell_quote("it's"), r#"'it'\''s'"#);
    }

    fn ssh_endpoint() -> Endpoint {
        Endpoint::Ssh {
            host: "host".into(),
            remote_bin: "agent-remote-server".into(),
            root: "/data/my project".into(),
            state_base: Some("/data/sicheng/agent state".into()),
            config: None,
        }
    }

    #[test]
    fn ssh_argv_is_one_quoted_remote_command() {
        let argv = ssh_endpoint().control_argv();
        assert_eq!(argv[0], "ssh");
        // Keepalive/batch options come before the host.
        assert!(argv.contains(&"BatchMode=yes".to_string()));
        let host_pos = argv.iter().position(|a| a == "host").unwrap();
        assert_eq!(host_pos, argv.len() - 2, "host is second to last");
        assert_eq!(
            argv[argv.len() - 1],
            "'agent-remote-server' '--root' '/data/my project' '--state-base' '/data/sicheng/agent state'"
        );
    }

    #[test]
    fn ssh_transfer_argvs_are_quoted_remote_commands() {
        let ep = ssh_endpoint();
        let recv = ep.transfer_receive_argv("/data/my project/.f.x.part", 42);
        assert_eq!(recv[0], "ssh");
        assert!(recv.contains(&"BatchMode=yes".to_string()));
        assert_eq!(
            recv[recv.len() - 1],
            "'agent-remote-server' '--transfer-receive' '/data/my project/.f.x.part' '--expect-size' '42'"
        );
        let send = ep.transfer_send_argv("@scratch/big file.bin");
        assert_eq!(
            send[send.len() - 1],
            "'agent-remote-server' '--transfer-send' '@scratch/big file.bin' \
             '--root' '/data/my project' '--state-base' '/data/sicheng/agent state'"
        );
    }

    #[test]
    fn local_argvs_are_plain() {
        let ep = Endpoint::Local {
            server_bin: "/bin/agent-remote-server".into(),
            root: "/ws".into(),
            state_base: None,
            config: Some("/cfg.toml".into()),
        };
        assert_eq!(
            ep.control_argv(),
            vec![
                "/bin/agent-remote-server",
                "--root",
                "/ws",
                "--config",
                "/cfg.toml"
            ]
        );
        assert_eq!(
            ep.transfer_send_argv("f.bin"),
            vec![
                "/bin/agent-remote-server",
                "--transfer-send",
                "f.bin",
                "--root",
                "/ws"
            ]
        );
    }
}
