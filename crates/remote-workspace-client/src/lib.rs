use std::process::Stdio;
use std::sync::Arc;

// EditSpec is re-exported: callers build an `edit` without having to depend on
// the protocol crate directly.
pub use remote_workspace_protocol::EditSpec;

use remote_workspace_protocol::{
    ErrorCode, ExecResult, FileEntry, ListResult, MutationResult, OperationDetails, ProtocolError,
    ReadResult, Request, RequestBody, RequestId, RequestStatusResult, ServerMessage,
};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    serde::Deserialize,
    serde::Serialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum RemoteShell {
    #[default]
    Posix,
    Powershell,
}

impl std::fmt::Display for RemoteShell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Posix => f.write_str("posix"),
            Self::Powershell => f.write_str("powershell"),
        }
    }
}

pub mod deploy;
pub mod fleet;
mod log_writer;
mod platform;
pub mod stats;
mod transfer;

pub use log_writer::ClientLog;
pub use transfer::{download_file, upload_file, Endpoint};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("protocol error from server: {0}")]
    Server(ProtocolError),
    #[error("transport io error: {0}")]
    Io(#[from] std::io::Error),
    /// `detail` is the tail of what the remote printed before dying, already
    /// formatted with its separator, or empty if it said nothing. A server
    /// that refuses to start explains itself on stderr and exits; without
    /// that text this error can only report the symptom.
    #[error("server closed connection{detail}")]
    Closed { detail: String },
    #[error("request timed out")]
    Timeout,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("transfer failed: {0}")]
    Transfer(String),
}

type DispMap = Arc<Mutex<std::collections::HashMap<RequestId, oneshot::Sender<ServerMessage>>>>;

/// Default deadline for a reply, guarding against a server that stays connected
/// but never responds. `exec` overrides it with one derived from the
/// server-side `timeout_ms`.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 5 * 60 * 1000;

/// A spawned transport process and the pipes the client drives it through.
pub struct Spawned {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    /// Piped stderr, drained into the connection's error tail. `None` when the
    /// transport routes stderr elsewhere, which costs the reason a failed
    /// connection would otherwise carry.
    pub stderr: Option<ChildStderr>,
}

/// Spawns the remote process (ssh or local).
pub trait Transport: Send {
    fn spawn(&mut self) -> std::io::Result<Spawned>;
}

/// Default transport: spawns the given argv as a subprocess. For SSH use
/// argv like `["ssh", host, "remote-workspace-server", "--root", path]`; for tests
/// use the local server binary directly.
pub struct ArgvTransport {
    pub argv: Vec<String>,
}

impl Transport for ArgvTransport {
    fn spawn(&mut self) -> std::io::Result<Spawned> {
        let mut cmd = Command::new(&self.argv[0]);
        cmd.args(&self.argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Piped, not inherited: over SSH this carries the remote server's
            // own diagnosis of why it is about to exit, and inheriting it
            // scatters that into the host's log where the caller never sees it.
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Die with the parent: if the consumer (CLI/MCP) is killed -- even
        // with SIGKILL, where no destructor runs -- the transport child must
        // not outlive it as an orphan holding the remote session (and the
        // server-side state lock) open.
        platform::configure_parent_death(&mut cmd);
        let mut child = cmd.spawn()?;
        if let Err(error) = platform::attach_parent_death(&mut child) {
            let _ = child.start_kill();
            return Err(error);
        }
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take();
        Ok(Spawned {
            child,
            stdin,
            stdout,
            stderr,
        })
    }
}

/// Bounded tail of the transport's stderr, so a closed connection can say what
/// the far end printed on its way out instead of only that it closed.
struct StderrTail {
    lines: Mutex<std::collections::VecDeque<String>>,
    finished: std::sync::atomic::AtomicBool,
    finished_notify: tokio::sync::Notify,
}

const STDERR_TAIL_LINES: usize = 8;
const STDERR_LINE_LIMIT: usize = 400;
/// How long `text` waits for the stderr reader to reach EOF. stdout and stderr
/// close together when the far end dies, so the reason is often still in
/// flight at the moment the missing reply surfaces as an error.
const STDERR_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

impl StderrTail {
    fn new() -> Self {
        Self {
            lines: Mutex::new(std::collections::VecDeque::new()),
            finished: std::sync::atomic::AtomicBool::new(false),
            finished_notify: tokio::sync::Notify::new(),
        }
    }

    async fn push(&self, line: &str) {
        let line = line.trim_end();
        if line.is_empty() {
            return;
        }
        let mut g = self.lines.lock().await;
        if g.len() == STDERR_TAIL_LINES {
            g.pop_front();
        }
        g.push_back(clip(line, STDERR_LINE_LIMIT));
    }

    fn finish(&self) {
        self.finished
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.finished_notify.notify_waiters();
    }

    /// The tail, after giving the reader a moment to finish. Keeps the last
    /// lines rather than the first: shells on the far end print their own
    /// noise at startup, and what matters is the last thing said.
    async fn text(&self) -> String {
        if !self.finished.load(std::sync::atomic::Ordering::SeqCst) {
            // Registering before the re-check closes the window where the
            // reader finishes between them; the timeout bounds the rest.
            let notified = self.finished_notify.notified();
            if !self.finished.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = tokio::time::timeout(STDERR_DRAIN_GRACE, notified).await;
            }
        }
        let g = self.lines.lock().await;
        g.iter().cloned().collect::<Vec<_>>().join("; ")
    }
}

fn clip(s: &str, limit: usize) -> String {
    if s.len() <= limit {
        return s.to_string();
    }
    let mut end = limit;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

pub struct Client {
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    reply_map: DispMap,
    /// Persistent close flag: once the transport EOFs, this stays true so any
    /// later request on this Client fails immediately instead of hanging.
    closed: Arc<std::sync::atomic::AtomicBool>,
    closed_notify: Arc<tokio::sync::Notify>,
    stderr: Arc<StderrTail>,
    log: Option<Arc<ClientLog>>,
    shutdown: Option<oneshot::Sender<()>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
}

impl Client {
    pub async fn connect<T: Transport + 'static>(
        mut transport: T,
        log: Option<ClientLog>,
    ) -> Result<Self, ClientError> {
        let Spawned {
            mut child,
            stdin,
            stdout,
            stderr,
        } = transport.spawn()?;
        let stdin = Arc::new(Mutex::new(Some(stdin)));
        let reply_map: DispMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_notify = Arc::new(tokio::sync::Notify::new());
        let log = log.map(Arc::new);
        let tail = Arc::new(StderrTail::new());

        match stderr {
            // Drained continuously, not on demand: an unread pipe fills at
            // 64 KiB and blocks the far end mid-write.
            Some(stderr) => {
                let tail = tail.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stderr);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {
                                // Also traced, so nothing the far end says is
                                // lost to the tail's bound when someone is
                                // watching with RUST_LOG.
                                debug!(line = %line.trim_end(), "transport stderr");
                                tail.push(&line).await;
                            }
                        }
                    }
                    tail.finish();
                });
            }
            None => tail.finish(),
        }

        let reader_reply = reply_map.clone();
        let drain_reply = reply_map.clone();
        let reader_closed = closed.clone();
        let reader_notify = closed_notify.clone();
        let reader_log = log.clone();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let reader_task = tokio::spawn(async move {
            tokio::select! {
                _ = reader_loop(stdout, reader_reply, reader_log) => {}
                _ = shutdown_rx => {
                    let _ = child.start_kill();
                }
            }
            // Mark the connection persistently closed so future requests on this
            // Client fail fast, then wake any current waiters.
            reader_closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drain_waiters(&drain_reply).await;
            reader_notify.notify_waiters();
            // When stdout ends, the child has exited.
            let _ = child.wait().await;
        });

        Ok(Self {
            stdin,
            reply_map,
            closed,
            closed_notify,
            stderr: tail,
            log,
            shutdown: Some(shutdown),
            reader_task: Some(reader_task),
        })
    }

    pub async fn close(self) {
        self.close_with_grace(std::time::Duration::from_secs(7))
            .await;
    }

    pub async fn close_with_grace(mut self, grace: std::time::Duration) {
        if !self.is_closed() {
            if let Some(stdin) = self.stdin.lock().await.take() {
                let _ = close_control_stdin(stdin);
            }
            let _ = tokio::time::timeout(grace, self.wait_closed()).await;
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(reader_task) = self.reader_task.take() {
            let _ = reader_task.await;
        }
    }

    /// The close error, carrying whatever the far end printed before dying.
    async fn closed_error(&self) -> ClientError {
        let tail = self.stderr.text().await;
        ClientError::Closed {
            detail: if tail.is_empty() {
                String::new()
            } else {
                format!(": {tail}")
            },
        }
    }

    /// True once the transport has EOF'd. A closed Client never recovers;
    /// callers that need resilience must build a fresh Client.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolves once the connection is closed.
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
            return Err(self.closed_error().await);
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
            let Some(w) = w.as_mut() else {
                self.reply_map.lock().await.remove(&request_id);
                return Err(self.closed_error().await);
            };
            let written = async {
                w.write_all(line.as_bytes()).await?;
                w.write_all(b"\n").await?;
                w.flush().await
            }
            .await;
            if let Err(e) = written {
                // A refused server exits before the first request is even
                // written, so this -- not the missing reply -- is where its
                // death usually surfaces. It is the same event as a close and
                // owes the caller the same explanation.
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    return Err(self.closed_error().await);
                }
                return Err(ClientError::Io(e));
            }
        }
        // Race the reply against connection close and a hard request timeout:
        // if the server/SSH disappears, the reader drains reply_map (closing
        // `tx`) and notifies here; if the server stalls, the timeout fires.
        // Either way we never block indefinitely.
        let msg = tokio::select! {
            biased;
            () = self.wait_closed() => return Err(self.closed_error().await),
            () = tokio::time::sleep(timeout) => {
                self.reply_map.lock().await.remove(&request_id);
                return Err(ClientError::Timeout);
            }
            m = rx => match m {
                Ok(m) => m,
                Err(_) => return Err(self.closed_error().await),
            },
        };
        if let Some(l) = &self.log {
            l.log_response(&request_id, &msg).await;
        }
        Ok((request_id, msg))
    }

    fn unpack(msg: ServerMessage) -> Result<remote_workspace_protocol::ResultBody, ClientError> {
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
            remote_workspace_protocol::ResultBody::List(result) => Ok(result),
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
            remote_workspace_protocol::ResultBody::Stat { stat } => Ok(stat),
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
            remote_workspace_protocol::ResultBody::Read(r) => Ok(r),
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
            remote_workspace_protocol::ResultBody::Mutation(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for create",
            ))),
        }
    }

    /// Apply exact text replacements to an existing file, in order, each to
    /// the result of the one before it. `base_hash` is required for optimistic
    /// concurrency and pins the content the first replacement is stated
    /// against.
    pub async fn edit(
        &self,
        path: &str,
        base_hash: &str,
        edits: Vec<EditSpec>,
    ) -> Result<MutationResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Edit {
                path: path.into(),
                base_hash: base_hash.into(),
                edits,
            })
            .await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::Mutation(w) => Ok(w),
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
            remote_workspace_protocol::ResultBody::Mutation(w) => Ok(w),
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
            remote_workspace_protocol::ResultBody::Exec(result) => Ok(result),
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
    ) -> Result<remote_workspace_protocol::UploadPrepareResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::UploadPrepare {
                path: path.into(),
                overwrite,
            })
            .await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::UploadPrepare(r) => Ok(r),
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
    ) -> Result<remote_workspace_protocol::TransferResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::UploadCommit {
                transfer_id: transfer_id.into(),
                size,
                sha256: sha256.into(),
                duration_ms,
            })
            .await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::Transfer(r) => Ok(r),
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
            remote_workspace_protocol::ResultBody::UploadAbort { .. } => Ok(()),
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
    ) -> Result<remote_workspace_protocol::TransferResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::DownloadRecord {
                path: path.into(),
                size,
                sha256: sha256.into(),
                duration_ms,
            })
            .await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::Transfer(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for download_record",
            ))),
        }
    }

    pub async fn history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<remote_workspace_protocol::AnyOperationRecord>, ClientError> {
        let (_, msg) = self.send_request(RequestBody::History { limit }).await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::History { operations } => Ok(operations),
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
            remote_workspace_protocol::ResultBody::Operation(o) => Ok(o),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for operation_get",
            ))),
        }
    }

    pub async fn gc(
        &self,
        keep: Option<usize>,
    ) -> Result<remote_workspace_protocol::GcResult, ClientError> {
        let (_, msg) = self.send_request(RequestBody::Gc { keep }).await?;
        match Self::unpack(msg)? {
            remote_workspace_protocol::ResultBody::Gc(g) => Ok(g),
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
            remote_workspace_protocol::ResultBody::RequestStatus(r) => Ok(r),
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

impl Drop for Client {
    fn drop(&mut self) {
        // Drop cannot await a graceful EOF handshake. Call close() where the
        // owner has an async teardown path; this remains the orphan-safe fallback.
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(windows)]
fn close_control_stdin(stdin: ChildStdin) -> std::io::Result<()> {
    drop(stdin.into_owned_handle()?);
    Ok(())
}

#[cfg(not(windows))]
fn close_control_stdin(stdin: ChildStdin) -> std::io::Result<()> {
    drop(stdin);
    Ok(())
}

pub fn powershell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub(crate) fn windows_command_line_arg(arg: &str) -> String {
    if !arg.is_empty() && !arg.chars().any(|c| c.is_whitespace() || c == '"') {
        return arg.to_string();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0;
    for c in arg.chars() {
        if c == '\\' {
            backslashes += 1;
        } else if c == '"' {
            quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
            quoted.push('"');
            backslashes = 0;
        } else {
            quoted.push_str(&"\\".repeat(backslashes));
            backslashes = 0;
            quoted.push(c);
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

pub(crate) fn remote_argv_command(shell: RemoteShell, argv: &[String]) -> String {
    match shell {
        RemoteShell::Posix => argv
            .iter()
            .map(|arg| shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" "),
        RemoteShell::Powershell => {
            let executable = argv.first().map(String::as_str).unwrap_or_default();
            let arguments = argv
                .iter()
                .skip(1)
                .map(|arg| windows_command_line_arg(arg))
                .collect::<Vec<_>>()
                .join(" ");
            format!(
                "$ErrorActionPreference = 'Stop'; try {{ $psi = [System.Diagnostics.ProcessStartInfo]::new(); $psi.FileName = {}; $psi.Arguments = {}; $psi.UseShellExecute = $false; $p = [System.Diagnostics.Process]::Start($psi); $p.WaitForExit(); exit $p.ExitCode }} catch {{ [Console]::Error.WriteLine($_.Exception.Message); exit 1 }}",
                powershell_quote(executable),
                powershell_quote(&arguments),
            )
        }
    }
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
    use super::{
        powershell_quote, remote_argv_command, shell_quote, windows_command_line_arg,
        ArgvTransport, Client, Endpoint, RemoteShell,
    };

    #[cfg(windows)]
    fn exits_on_stdin_eof_argv() -> Vec<String> {
        vec![
            "powershell.exe".into(),
            "-NoLogo".into(),
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-Command".into(),
            "$input | Out-Null".into(),
        ]
    }

    #[cfg(not(windows))]
    fn exits_on_stdin_eof_argv() -> Vec<String> {
        vec!["sh".into(), "-c".into(), "cat >/dev/null".into()]
    }

    #[tokio::test]
    async fn graceful_close_delivers_real_stdin_eof() {
        let client = Client::connect(
            ArgvTransport {
                argv: exits_on_stdin_eof_argv(),
            },
            None,
        )
        .await
        .unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.close_with_grace(std::time::Duration::from_secs(30)),
        )
        .await
        .expect("graceful close must make the child observe EOF");
    }

    #[test]
    fn quotes_empty_spaces_and_metacharacters() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("$(rm -rf /);`x`|&"), "'$(rm -rf /);`x`|&'");
        assert_eq!(shell_quote("it's"), r#"'it'\''s'"#);
        assert_eq!(powershell_quote("it's"), "'it''s'");
        let command = remote_argv_command(
            RemoteShell::Powershell,
            &["C:\\Program Files\\server.exe".into(), "it's".into()],
        );
        assert!(command.contains("[System.Diagnostics.ProcessStartInfo]::new"));
        assert!(command.contains("'C:\\Program Files\\server.exe'"));
        assert!(command.contains("'it''s'"));
        assert_eq!(
            windows_command_line_arg(r"C:\path with space\"),
            r#""C:\path with space\\""#
        );
    }

    fn ssh_endpoint() -> Endpoint {
        Endpoint::Ssh {
            host: "host".into(),
            remote_shell: RemoteShell::Posix,
            remote_bin: "remote-workspace-server".into(),
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
            "'remote-workspace-server' '--root' '/data/my project' '--state-base' '/data/sicheng/agent state'"
        );
    }

    #[test]
    #[cfg(windows)]
    fn powershell_ssh_argv_uses_exact_native_argument_serialization() {
        let endpoint = Endpoint::Ssh {
            host: "windows".into(),
            remote_shell: RemoteShell::Powershell,
            remote_bin: r"C:\Program Files\Remote Workspace\server.exe".into(),
            root: r"C:\work\it's here".into(),
            state_base: None,
            config: None,
        };
        let argv = endpoint.control_argv();
        assert_eq!(argv[0], "powershell.exe");
        assert!(argv.contains(&"-NoProfile".to_string()));
        assert!(argv.contains(&"-EncodedCommand".to_string()));
        let receive = endpoint
            .transfer_receive_argv(r"C:\stage\it's.part", 42)
            .last()
            .unwrap()
            .clone();
        assert!(receive.contains("[System.Diagnostics.Process]::Start"));
        assert!(receive.contains("--transfer-base64"));
        let send = endpoint
            .transfer_send_argv("file.bin")
            .last()
            .unwrap()
            .clone();
        assert!(send.contains("[System.Diagnostics.Process]::Start"));
        assert!(send.contains("--transfer-base64"));
    }

    #[test]
    fn ssh_transfer_argvs_are_quoted_remote_commands() {
        let ep = ssh_endpoint();
        let recv = ep.transfer_receive_argv("/data/my project/.f.x.part", 42);
        assert_eq!(recv[0], "ssh");
        assert!(recv.contains(&"BatchMode=yes".to_string()));
        assert_eq!(
            recv[recv.len() - 1],
            "'remote-workspace-server' '--transfer-receive' '/data/my project/.f.x.part' '--expect-size' '42'"
        );
        let send = ep.transfer_send_argv("@scratch/big file.bin");
        assert_eq!(
            send[send.len() - 1],
            "'remote-workspace-server' '--transfer-send' '@scratch/big file.bin' \
             '--root' '/data/my project' '--state-base' '/data/sicheng/agent state'"
        );
    }

    #[test]
    fn local_argvs_are_plain() {
        let ep = Endpoint::Local {
            server_bin: "/bin/remote-workspace-server".into(),
            root: "/ws".into(),
            state_base: None,
            config: Some("/cfg.toml".into()),
        };
        assert_eq!(
            ep.control_argv(),
            vec![
                "/bin/remote-workspace-server",
                "--root",
                "/ws",
                "--config",
                "/cfg.toml"
            ]
        );
        assert_eq!(
            ep.transfer_send_argv("f.bin"),
            vec![
                "/bin/remote-workspace-server",
                "--transfer-send",
                "f.bin",
                "--root",
                "/ws"
            ]
        );
    }
}
