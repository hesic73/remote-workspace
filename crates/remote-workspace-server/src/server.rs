use std::path::PathBuf;
use std::sync::Arc;

use remote_workspace_protocol::{
    ErrorCode, GcResult, OperationDetails, ProtocolError, ReadOptions, Request, RequestBody,
    ResultBody, ServerInfoResult, ServerMessage, PROTOCOL_VERSION,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config::ServerConfig;
use crate::exec;
use crate::fs_ops;
use crate::scratch;
use crate::session_log::SessionLog;
use crate::store::{OperationStore, StoredResult};
use crate::transfer;
use crate::workspace::Workspace;

const HISTORY_DEFAULT_LIMIT: usize = 50;
const HISTORY_MAX_LIMIT: usize = 100;
const OPPORTUNISTIC_SHARD_THRESHOLD: usize = 64;
const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(2);
const ABORT_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

pub struct Server {
    pub workspace: Arc<Workspace>,
    pub store: OperationStore,
    pub config: Arc<ServerConfig>,
    history_limit: Option<usize>,
    scratch_max_age: Option<std::time::Duration>,
    idle_timeout: Option<std::time::Duration>,
    session_log: Arc<SessionLog>,
    uploads: transfer::UploadRegistry,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

pub struct ServerOptions {
    pub root: PathBuf,
    pub state_dir: PathBuf,
    pub config_path: Option<PathBuf>,
    /// Keep only this many recent operations on `gc` (and opportunistically at
    /// startup once closed shards exceed `OPPORTUNISTIC_SHARD_THRESHOLD`).
    pub history_limit: Option<usize>,
    pub scratch_max_age: Option<std::time::Duration>,
    pub idle_timeout: Option<std::time::Duration>,
}

impl Server {
    pub fn new(opts: ServerOptions) -> anyhow::Result<Self> {
        let state_dir = opts.state_dir.clone();
        let root = opts.root.clone();
        let workspace = Arc::new(Workspace::new(opts.root, opts.state_dir.join("scratch"))?);
        let store = OperationStore::new(opts.state_dir).map_err(|e| anyhow::anyhow!(e))?;
        let session_log = Arc::new(SessionLog::new(&state_dir));
        session_log.trim_if_large();

        if let Some(keep) = opts.history_limit {
            if store.shard_count_exceeds(OPPORTUNISTIC_SHARD_THRESHOLD) {
                match store.prune(keep) {
                    Ok(stats) if stats.removed_operations > 0 => tracing::info!(
                        removed_operations = stats.removed_operations,
                        "compacted operation shards at startup"
                    ),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "opportunistic shard compaction failed"),
                }
            }
        }

        if let Some(age) = opts.scratch_max_age {
            match scratch::sweep_if_due(&state_dir, &workspace.scratch_root, age) {
                Ok(Some(u)) if u.removed_files > 0 => tracing::info!(
                    removed_files = u.removed_files,
                    removed_bytes = u.removed_bytes,
                    "swept scratch at startup"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "scratch sweep at startup failed"),
            }
        }

        let config = match opts.config_path {
            Some(p) => {
                let text = std::fs::read_to_string(&p)
                    .map_err(|e| anyhow::anyhow!("read config {p:?}: {e}"))?;
                Arc::new(ServerConfig::load_from_str(&text)?)
            }
            None => Arc::new(ServerConfig::default()),
        };
        let uploads = transfer::UploadRegistry::new(&state_dir);

        session_log.record(
            "started",
            serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "root": root.display().to_string(),
                "idle_timeout_ms": opts.idle_timeout.map(|d| d.as_millis() as u64),
            }),
        );
        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            root = %root.display(),
            state_dir = %state_dir.display(),
            "server started"
        );
        Ok(Self {
            workspace,
            store,
            config,
            history_limit: opts.history_limit,
            scratch_max_age: opts.scratch_max_age,
            idle_timeout: opts.idle_timeout,
            session_log,
            uploads,
        })
    }

    pub async fn run_stdio(self) -> std::io::Result<()> {
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        self.run(stdin, stdout).await
    }

    pub async fn run<R, W>(self, read: R, write: W) -> std::io::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let idle_timeout = self.idle_timeout;
        let session_log = self.session_log.clone();
        let started = std::time::Instant::now();
        let server = Arc::new(self);
        let stdout: Arc<tokio::sync::Mutex<W>> = Arc::new(tokio::sync::Mutex::new(write));

        let (lines_tx, mut lines) = tokio::sync::mpsc::channel::<std::io::Result<Vec<u8>>>(1);
        let reading = tokio::spawn(async move {
            let mut reader = BufReader::new(read);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if lines_tx.send(Ok(line)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = lines_tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        let mut handlers = tokio::task::JoinSet::new();
        let mut served: u64 = 0;
        let mut read_error: Option<std::io::Error> = None;
        let mut idle_since = tokio::time::Instant::now();

        let reason = loop {
            let event = match idle_timeout {
                None => lines.recv().await,
                Some(d) => tokio::select! {
                    biased;
                    ev = lines.recv() => ev,
                    _ = handlers.join_next(), if !handlers.is_empty() => {
                        idle_since = tokio::time::Instant::now();
                        continue;
                    }
                    _ = tokio::time::sleep_until(idle_since + d) => {
                        if handlers.is_empty() {
                            break "idle_timeout";
                        }
                        idle_since = tokio::time::Instant::now();
                        continue;
                    }
                },
            };
            let line = match event {
                Some(Ok(line)) => line,
                Some(Err(e)) => {
                    read_error = Some(e);
                    break "read_error";
                }
                None => break "stdin_eof",
            };
            idle_since = tokio::time::Instant::now();
            let text = String::from_utf8_lossy(&line);
            let trimmed = text.trim().trim_start_matches('\u{feff}');
            if trimmed.is_empty() {
                continue;
            }
            let parsed = match std::str::from_utf8(&line) {
                Ok(_) => serde_json::from_str::<Request>(trimmed).map_err(|e| e.to_string()),
                Err(e) => Err(format!("not UTF-8: {e}")),
            };
            let req = match parsed {
                Ok(r) => r,
                Err(e) => {
                    let request_id = serde_json::from_str::<serde_json::Value>(trimmed)
                        .ok()
                        .and_then(|v| v.get("request_id")?.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "(parse-error)".into());
                    let msg = ServerMessage::Error {
                        request_id,
                        error: ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            format!("invalid request line: {e}"),
                        ),
                    };
                    write_line(&stdout, &msg).await;
                    continue;
                }
            };
            served += 1;
            let server = server.clone();
            let stdout = stdout.clone();
            handlers.spawn(async move {
                server.handle(req, stdout).await;
            });
            reap(&mut handlers);
        };

        let drained = tokio::time::timeout(SHUTDOWN_DRAIN, async {
            while handlers.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        handlers.abort_all();
        let _ = tokio::time::timeout(ABORT_GRACE, async {
            while handlers.join_next().await.is_some() {}
        })
        .await;
        reading.abort();
        let _ = tokio::time::timeout(ABORT_GRACE, reading).await;

        let uptime_ms = started.elapsed().as_millis() as u64;
        session_log.record(
            "exit",
            serde_json::json!({
                "reason": reason,
                "uptime_ms": uptime_ms,
                "requests": served,
                "drained": drained,
            }),
        );
        tracing::info!(
            reason,
            uptime_ms,
            requests = served,
            drained,
            "server exiting"
        );
        match read_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn handle<W: tokio::io::AsyncWrite + Unpin + Send>(
        self: Arc<Self>,
        req: Request,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        let request_id = req.request_id;

        if let RequestBody::Wait {
            argv,
            cwd,
            profile,
            timeout_ms,
        } = req.body
        {
            let result = exec::wait(
                &self.workspace,
                &self.config,
                cwd.as_deref(),
                profile.as_deref(),
                &argv,
                timeout_ms,
            )
            .await;
            let msg = reply_for(request_id, result.map(ResultBody::Wait));
            write_line(&stdout, &msg).await;
            return;
        }

        let unpersisted = match &req.body {
            RequestBody::ServerInfo {} => {
                let srv = self.clone();
                Some(run_blocking(move || srv.server_info()).await)
            }
            RequestBody::List {
                path,
                offset,
                limit,
            } => {
                let srv = self.clone();
                let path = path.clone();
                let (offset, limit) = (*offset, *limit);
                Some(run_blocking(move || fs_ops::list(&srv.workspace, &path, offset, limit)).await)
            }
            RequestBody::Stat { path } => {
                let srv = self.clone();
                let path = path.clone();
                Some(run_blocking(move || fs_ops::stat(&srv.workspace, &path)).await)
            }
            RequestBody::Read {
                path,
                offset,
                limit,
                start_line,
                max_lines,
                tail_lines,
                tail_bytes,
            } => {
                let srv = self.clone();
                let path = path.clone();
                let opts = ReadOptions {
                    offset: *offset,
                    limit: *limit,
                    start_line: *start_line,
                    max_lines: *max_lines,
                    tail_lines: *tail_lines,
                    tail_bytes: *tail_bytes,
                };
                Some(run_blocking(move || fs_ops::read(&srv.workspace, &path, opts)).await)
            }
            RequestBody::History { limit } => {
                let srv = self.clone();
                let limit = *limit;
                Some(run_blocking(move || srv.history(limit)).await)
            }
            RequestBody::OperationGet { operation_id } => {
                let srv = self.clone();
                let operation_id = operation_id.clone();
                Some(run_blocking(move || srv.operation_get(&operation_id)).await)
            }
            RequestBody::UploadPrepare { path, overwrite } => {
                let srv = self.clone();
                let path = path.clone();
                let overwrite = *overwrite;
                Some(
                    run_blocking(move || {
                        transfer::upload_prepare(&srv.workspace, &srv.uploads, &path, overwrite)
                    })
                    .await,
                )
            }
            RequestBody::UploadAbort { transfer_id } => {
                let srv = self.clone();
                let transfer_id = transfer_id.clone();
                Some(run_blocking(move || transfer::upload_abort(&srv.uploads, &transfer_id)).await)
            }
            _ => None,
        };
        if let Some(result) = unpersisted {
            let msg = reply_for(request_id, result);
            write_line(&stdout, &msg).await;
            return;
        }

        if let Some(existing) = self.store.claim_request(&request_id) {
            let msg = match existing {
                Some(StoredResult::Done(m)) => m,
                Some(StoredResult::Error(e)) => ServerMessage::Error {
                    request_id,
                    error: e,
                },
                None => ServerMessage::Error {
                    request_id,
                    error: ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "request already in progress",
                    ),
                },
            };
            write_line(&stdout, &msg).await;
            return;
        }

        match req.body {
            RequestBody::Create { path, content } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let rid = request_id.clone();
                let result = run_blocking(move || {
                    fs_ops::create(&srv.workspace, &srv.store, &guard, &rid, &path, &content)
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::Edit {
                path,
                base_hash,
                edits,
                replace_content,
            } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let rid = request_id.clone();
                let result = run_blocking(move || {
                    fs_ops::edit(
                        &srv.workspace,
                        &srv.store,
                        &guard,
                        &rid,
                        &path,
                        &base_hash,
                        &edits,
                        replace_content.as_deref(),
                    )
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::Exec {
                argv,
                cwd,
                profile,
                timeout_ms,
            } => {
                self.handle_exec(&request_id, argv, cwd, profile, timeout_ms, stdout)
                    .await;
            }
            RequestBody::Delete { path } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let rid = request_id.clone();
                let result = run_blocking(move || {
                    fs_ops::delete(&srv.workspace, &srv.store, &guard, &rid, &path)
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::UploadCommit {
                transfer_id,
                size,
                sha256,
                duration_ms,
            } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let rid = request_id.clone();
                let result = run_blocking(move || {
                    transfer::upload_commit(
                        &srv.store,
                        &guard,
                        &rid,
                        &srv.uploads,
                        &transfer_id,
                        size,
                        &sha256,
                        duration_ms,
                    )
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::DownloadRecord {
                path,
                size,
                sha256,
                duration_ms,
            } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let rid = request_id.clone();
                let result = run_blocking(move || {
                    transfer::download_record(
                        &srv.workspace,
                        &srv.store,
                        &guard,
                        &rid,
                        &path,
                        size,
                        &sha256,
                        duration_ms,
                    )
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::Gc { keep } => {
                let guard = self.store.write_guard().await;
                let srv = self.clone();
                let result = run_blocking(move || {
                    let _guard = guard;
                    srv.gc(keep)
                })
                .await;
                let msg = self.finish(&request_id, result);
                write_line(&stdout, &msg).await;
            }
            RequestBody::ServerInfo { .. }
            | RequestBody::List { .. }
            | RequestBody::Wait { .. }
            | RequestBody::Stat { .. }
            | RequestBody::Read { .. }
            | RequestBody::History { .. }
            | RequestBody::OperationGet { .. }
            | RequestBody::UploadPrepare { .. }
            | RequestBody::UploadAbort { .. } => {
                unreachable!("handled before the idempotency claim")
            }
        }
    }

    fn server_info(&self) -> Result<ResultBody, ProtocolError> {
        match std::fs::metadata(&self.workspace.root) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(ProtocolError::new(
                    ErrorCode::NotADirectory,
                    format!(
                        "workspace root is not a directory: {}",
                        self.workspace.root.display()
                    ),
                ))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ProtocolError::new(
                    ErrorCode::NotFound,
                    format!(
                        "workspace root not found: {}",
                        self.workspace.root.display()
                    ),
                ))
            }
            Err(e) => {
                return Err(ProtocolError::new(
                    ErrorCode::IoError,
                    format!("workspace root check failed: {e}"),
                ))
            }
        }
        Ok(ResultBody::ServerInfo(ServerInfoResult {
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: PROTOCOL_VERSION,
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            default_profile: self.config.default_profile.clone(),
            profiles: self.config.profile_names(),
        }))
    }

    fn gc(&self, keep: Option<usize>) -> Result<ResultBody, ProtocolError> {
        let Some(k) = keep.or(self.history_limit) else {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "server has no history limit configured; pass keep explicitly",
            ));
        };
        let stats = self.store.prune(k)?;
        let in_flight = transfer::in_flight_staging(&self.uploads);
        let removed_stale_staging = transfer::sweep_stale_staging_tree(
            &self.workspace.root,
            &in_flight,
            transfer::STALE_STAGING_MAX_AGE,
        ) + transfer::sweep_stale_staging_tree(
            &self.workspace.scratch_root,
            &in_flight,
            transfer::STALE_STAGING_MAX_AGE,
        );
        let scratch = scratch::enforce(&self.workspace.scratch_root, self.scratch_max_age)
            .map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("scratch sweep failed: {e}"))
            })?;
        Ok(ResultBody::Gc(GcResult {
            removed_operations: stats.removed_operations,
            retained_operations: stats.retained_operations,
            removed_stale_staging,
            scratch,
        }))
    }

    fn history(&self, limit: Option<usize>) -> Result<ResultBody, ProtocolError> {
        let limit = limit.unwrap_or(HISTORY_DEFAULT_LIMIT);
        if limit > HISTORY_MAX_LIMIT {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("history limit must not exceed {HISTORY_MAX_LIMIT}"),
            ));
        }
        Ok(ResultBody::History {
            operations: self.store.history(Some(limit))?,
        })
    }

    fn operation_get(&self, operation_id: &str) -> Result<ResultBody, ProtocolError> {
        match self.store.find_record(operation_id)? {
            Some(record) if record.is_committed() => {
                Ok(ResultBody::Operation(OperationDetails { record }))
            }
            None | Some(_) => Err(ProtocolError::new(
                ErrorCode::OperationNotFound,
                format!("operation not found: {operation_id}"),
            )),
        }
    }

    async fn handle_exec<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        request_id: &str,
        argv: Vec<String>,
        cwd: Option<String>,
        profile: Option<String>,
        timeout_ms: Option<u64>,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        let operation_id = self.store.next_operation_id();
        let ws = self.workspace.clone();
        let config = self.config.clone();

        let outcome = exec::exec(
            &ws,
            &config,
            cwd.as_deref(),
            profile.as_deref(),
            &argv,
            timeout_ms,
        )
        .await;

        match outcome {
            Ok(o) => {
                let disposition = if matches!(
                    o.termination,
                    remote_workspace_protocol::ExecTermination::TimedOut
                ) {
                    remote_workspace_protocol::ExecDisposition::TimedOut
                } else {
                    remote_workspace_protocol::ExecDisposition::Completed
                };
                let result = remote_workspace_protocol::ExecResult {
                    operation_id: operation_id.clone(),
                    termination: o.termination,
                    duration_ms: o.duration_ms,
                    drain_timed_out: o.drain_timed_out,
                    stdout: o.stdout.clone(),
                    stderr: o.stderr.clone(),
                };
                let record = remote_workspace_protocol::ExecOperationRecord {
                    operation_id,
                    request_id: request_id.to_string(),
                    argv,
                    cwd,
                    profile,
                    timeout_ms: Some(timeout_ms.unwrap_or(exec::DEFAULT_TIMEOUT_MS)),
                    disposition,
                    termination: Some(o.termination),
                    drain_timed_out: o.drain_timed_out,
                    duration_ms: o.duration_ms,
                    timestamp_ms: now_ms(),
                    error: if matches!(
                        o.termination,
                        remote_workspace_protocol::ExecTermination::TimedOut
                    ) {
                        Some(format!(
                            "killed after {} ms timeout",
                            timeout_ms.unwrap_or(exec::DEFAULT_TIMEOUT_MS)
                        ))
                    } else {
                        None
                    },
                    error_code: if matches!(
                        o.termination,
                        remote_workspace_protocol::ExecTermination::TimedOut
                    ) {
                        Some(ErrorCode::ExecFailed)
                    } else {
                        None
                    },
                    stdout: o.stdout,
                    stderr: o.stderr,
                };
                let store = self.store.clone();
                let appended =
                    tokio::task::spawn_blocking(move || store.append_exec_record(record))
                        .await
                        .map_err(|e| {
                            ProtocolError::new(
                                ErrorCode::IoError,
                                format!("blocking task failed: {e}"),
                            )
                        })
                        .and_then(|r| r);
                if let Err(e) = appended {
                    self.store.remember_error(request_id, e.clone());
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id: request_id.to_string(),
                            error: e,
                        },
                    )
                    .await;
                    return;
                }
                let msg = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: ResultBody::Exec(result),
                };
                self.store.remember_result(request_id, msg.clone());
                write_line(&stdout, &msg).await;
            }
            Err(e) => {
                let record = remote_workspace_protocol::ExecOperationRecord {
                    operation_id,
                    request_id: request_id.to_string(),
                    argv,
                    cwd,
                    profile,
                    timeout_ms,
                    disposition: remote_workspace_protocol::ExecDisposition::Rejected,
                    termination: None,
                    drain_timed_out: false,
                    duration_ms: 0,
                    timestamp_ms: now_ms(),
                    error: Some(e.message.clone()),
                    error_code: Some(e.code),
                    stdout: remote_workspace_protocol::ExecOutput::default(),
                    stderr: remote_workspace_protocol::ExecOutput::default(),
                };
                let store = self.store.clone();
                let record_err =
                    tokio::task::spawn_blocking(move || store.append_exec_record(record))
                        .await
                        .map_err(|join_err| {
                            ProtocolError::new(
                                ErrorCode::IoError,
                                format!("blocking task failed: {join_err}"),
                            )
                        })
                        .and_then(|r| r)
                        .err();
                let report = record_err.unwrap_or(e);
                self.store.remember_error(request_id, report.clone());
                write_line(
                    &stdout,
                    &ServerMessage::Error {
                        request_id: request_id.to_string(),
                        error: report,
                    },
                )
                .await;
            }
        }
    }

    fn finish(&self, request_id: &str, result: Result<ResultBody, ProtocolError>) -> ServerMessage {
        match result {
            Ok(body) => {
                let msg = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: body,
                };
                self.store.remember_result(request_id, msg.clone());
                msg
            }
            Err(e) => {
                self.store.remember_error(request_id, e.clone());
                ServerMessage::Error {
                    request_id: request_id.to_string(),
                    error: e,
                }
            }
        }
    }
}

async fn run_blocking<F>(f: F) -> Result<ResultBody, ProtocolError>
where
    F: FnOnce() -> Result<ResultBody, ProtocolError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("blocking task failed: {e}")))?
}

fn reply_for(request_id: String, result: Result<ResultBody, ProtocolError>) -> ServerMessage {
    match result {
        Ok(body) => ServerMessage::Result {
            request_id,
            result: body,
        },
        Err(error) => ServerMessage::Error { request_id, error },
    }
}

fn reap<T: 'static>(handlers: &mut tokio::task::JoinSet<T>) {
    while handlers.try_join_next().is_some() {}
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .expect("system clock is set before 1970")
}

async fn write_line<W: tokio::io::AsyncWrite + Unpin + Send>(
    stdout: &Arc<tokio::sync::Mutex<W>>,
    msg: &ServerMessage,
) {
    let mut line = serde_json::to_string(msg).expect("server message must serialize");
    line.push('\n');
    let mut g = stdout.lock().await;
    let _ = g.write_all(line.as_bytes()).await;
    let _ = g.flush().await;
}
