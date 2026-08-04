use std::path::PathBuf;
use std::sync::Arc;

use remote_workspace_protocol::{
    ErrorCode, OperationDetails, Request, RequestBody, ResultBody, ServerMessage,
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

pub struct Server {
    pub workspace: Arc<Workspace>,
    pub store: OperationStore,
    pub config: Arc<ServerConfig>,
    history_limit: Option<usize>,
    scratch_max_age: Option<std::time::Duration>,
    idle_timeout: Option<std::time::Duration>,
    session_log: Arc<SessionLog>,
    /// Pending uploads (staging file created, commit not yet received).
    /// In-memory only: staging paths must never be persisted, and the staging
    /// files die with the connection anyway.
    uploads: transfer::UploadRegistry,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

pub struct ServerOptions {
    pub root: PathBuf,
    /// Resolved state directory (operation log, blobs, request table).
    pub state_dir: PathBuf,
    pub config_path: Option<PathBuf>,
    /// Keep only this many recent operations; pruned automatically at startup
    /// and on `gc`. `None` disables automatic pruning.
    pub history_limit: Option<usize>,
    /// Evict scratch files idle beyond this, on `gc` and at most once a day at
    /// startup. `None` disables sweeping.
    pub scratch_max_age: Option<std::time::Duration>,
    /// Exit after this long with no request arriving and none still running.
    /// The only other way this server ever stops is EOF on stdin, which
    /// depends on the far end of the SSH session noticing that its peer is
    /// gone -- something a suspended laptop or a dropped link can delay for
    /// hours, during which the state lock makes the workspace look occupied.
    /// `None` disables the timeout and restores that dependency.
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
        // Run WAL recovery before serving: reconcile any prepared markers left
        // by a crash, and clear requests stuck InProgress so they become retryable.
        let actions = store
            .recover(&workspace)
            .map_err(|e| anyhow::anyhow!("startup recovery failed: {e}"))?;
        for a in &actions {
            tracing::info!(action = ?a, "recovery");
        }
        if actions
            .iter()
            .any(|a| matches!(a, crate::store::RecoveryAction::Conflict { .. }))
        {
            tracing::warn!("startup recovery encountered one or more conflicts; affected requests are marked Done with an error");
        }
        if let Some(keep) = opts.history_limit {
            let stats = store
                .prune(keep)
                .map_err(|e| anyhow::anyhow!("startup prune failed: {e}"))?;
            if stats.removed_operations > 0 || stats.removed_requests > 0 {
                tracing::info!(
                    removed_operations = stats.removed_operations,
                    removed_requests = stats.removed_requests,
                    "pruned history at startup"
                );
            }
        }
        // Rate-limited inside: a server starts on every reconnect, so the
        // common path here must be a single stat.
        if let Some(u) = opts
            .scratch_max_age
            .and_then(|age| scratch::sweep_if_due(&state_dir, &workspace.scratch_root, age))
        {
            if u.removed_files > 0 {
                tracing::info!(
                    removed_files = u.removed_files,
                    removed_bytes = u.removed_bytes,
                    "swept scratch at startup"
                );
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
        // Last, so that a `started` with no matching `exit` means one thing
        // only: that server reached the point of serving and has not recorded
        // stopping. A start that fails never claims to have begun; it reports
        // itself on stderr, which the client now carries.
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
            uploads: transfer::UploadRegistry::default(),
        })
    }

    pub async fn run_stdio(self) -> std::io::Result<()> {
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        self.run(stdin, stdout).await
    }

    pub async fn run<R, W>(self, read: R, write: W) -> std::io::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let idle_timeout = self.idle_timeout;
        let session_log = self.session_log.clone();
        let started = std::time::Instant::now();
        let server = Arc::new(self);
        let mut reader = BufReader::new(read);
        let stdout: Arc<tokio::sync::Mutex<W>> = Arc::new(tokio::sync::Mutex::new(write));
        let mut line = String::new();
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut served: u64 = 0;

        let reason = loop {
            // `read_line` appends and is cancel-safe in that sense: a line the
            // idle timeout interrupts part-way stays in `line` and is finished
            // by the next call, so the buffer is cleared only once a whole
            // line has been consumed.
            let read = match idle_timeout {
                Some(d) => tokio::time::timeout(d, reader.read_line(&mut line)).await,
                None => Ok(reader.read_line(&mut line).await),
            };
            let n = match read {
                Ok(r) => r?,
                Err(_elapsed) => {
                    // A single `exec` may legitimately run for an hour without
                    // the client sending anything, so idleness means no work in
                    // flight either -- otherwise the timeout would kill the
                    // very request it is waiting on.
                    if in_flight.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                        break "idle_timeout";
                    }
                    continue;
                }
            };
            if n == 0 {
                break "stdin_eof";
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                line.clear();
                continue;
            }
            let parsed = serde_json::from_str::<Request>(trimmed);
            line.clear();
            let req = match parsed {
                Ok(r) => r,
                Err(e) => {
                    let msg = ServerMessage::Error {
                        request_id: "(parse-error)".into(),
                        error: remote_workspace_protocol::ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            format!("invalid request line: {e}"),
                        ),
                    };
                    write_line(&stdout, &msg).await;
                    continue;
                }
            };
            served += 1;
            // Counted here rather than inside the task, so there is no window
            // in which an accepted request is invisible to the idle check.
            let flight = InFlight::enter(&in_flight);
            let server = server.clone();
            let stdout = stdout.clone();
            tokio::spawn(async move {
                let _flight = flight;
                server.handle(req, stdout).await;
            });
        };

        let uptime_ms = started.elapsed().as_millis() as u64;
        session_log.record(
            "exit",
            serde_json::json!({
                "reason": reason,
                "uptime_ms": uptime_ms,
                "requests": served,
            }),
        );
        tracing::info!(reason, uptime_ms, requests = served, "server exiting");
        Ok(())
    }

    async fn handle<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        req: Request,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        let request_id = req.request_id.clone();

        // upload_prepare/upload_abort bypass the idempotency store entirely:
        // their results carry the staging path, which must never be persisted
        // (requests.jsonl included), and the in-memory upload registry dies
        // with this process, so replaying either after a reconnect could not
        // succeed anyway.
        let unpersisted = match &req.body {
            RequestBody::UploadPrepare { path, overwrite } => Some(transfer::upload_prepare(
                &self.workspace,
                &self.uploads,
                path,
                *overwrite,
            )),
            RequestBody::UploadAbort { transfer_id } => {
                Some(transfer::upload_abort(&self.uploads, transfer_id))
            }
            _ => None,
        };
        if let Some(result) = unpersisted {
            let msg = match result {
                Ok(body) => ServerMessage::Result {
                    request_id,
                    result: body,
                },
                Err(e) => ServerMessage::Error {
                    request_id,
                    error: e,
                },
            };
            write_line(&stdout, &msg).await;
            return;
        }

        // Idempotency via atomic claim: if we have seen this request_id, replay
        // its stored result without re-executing. Otherwise this call wins
        // ownership and proceeds. claim_request is a single locked
        // check-and-insert, so concurrent duplicate requests cannot both run.
        let op_kind = op_kind_str(&req.body);
        match self.store.claim_request(&request_id, op_kind) {
            Ok(None) => {} // won ownership; proceed to dispatch below.
            Ok(Some(entry)) => match entry.result {
                Some(StoredResult::Done(m)) => {
                    write_line(&stdout, &m).await;
                    return;
                }
                Some(StoredResult::Error(e)) => {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id,
                            error: e,
                        },
                    )
                    .await;
                    return;
                }
                // A genuinely in-flight request should not happen in a
                // single-connection server, but if it does, refuse rather than
                // re-execute.
                None => {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id,
                            error: remote_workspace_protocol::ProtocolError::new(
                                ErrorCode::InvalidRequest,
                                "request already in progress",
                            ),
                        },
                    )
                    .await;
                    return;
                }
            },
            // Claiming the request failed (e.g. request log is not writable).
            // Surface the error; do NOT execute, since we cannot record state.
            Err(e) => {
                write_line(
                    &stdout,
                    &ServerMessage::Error {
                        request_id,
                        error: e,
                    },
                )
                .await;
                return;
            }
        }

        match req.body {
            RequestBody::List {
                path,
                offset,
                limit,
            } => {
                self.finish(
                    &request_id,
                    fs_ops::list(&self.workspace, &path, offset, limit),
                )
                .await
                .with_stdout(&stdout)
                .await;
            }
            RequestBody::Stat { path } => {
                self.finish(&request_id, fs_ops::stat(&self.workspace, &path))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Read {
                path,
                offset,
                limit,
            } => {
                self.finish(
                    &request_id,
                    fs_ops::read(&self.workspace, &path, offset, limit),
                )
                .await
                .with_stdout(&stdout)
                .await;
            }
            RequestBody::Create { path, content } => {
                let guard = self.store.write_guard().await;
                let result = fs_ops::create(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    &content,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Edit {
                path,
                base_hash,
                old_text,
                new_text,
                replace_all,
            } => {
                let guard = self.store.write_guard().await;
                let result = fs_ops::edit(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    &base_hash,
                    &old_text,
                    &new_text,
                    replace_all,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
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
                let result =
                    fs_ops::delete(&self.workspace, &self.store, &guard, &request_id, &path);
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::UploadPrepare { .. } | RequestBody::UploadAbort { .. } => {
                unreachable!("handled before the idempotency claim")
            }
            RequestBody::UploadCommit {
                transfer_id,
                size,
                sha256,
                duration_ms,
            } => {
                let guard = self.store.write_guard().await;
                let result = transfer::upload_commit(
                    &self.store,
                    &guard,
                    &request_id,
                    &self.uploads,
                    &transfer_id,
                    size,
                    &sha256,
                    duration_ms,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::DownloadRecord {
                path,
                size,
                sha256,
                duration_ms,
            } => {
                let guard = self.store.write_guard().await;
                let result = transfer::download_record(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    size,
                    &sha256,
                    duration_ms,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::History { limit } => {
                let limit = limit.unwrap_or(HISTORY_DEFAULT_LIMIT);
                let result = if limit > HISTORY_MAX_LIMIT {
                    Err(remote_workspace_protocol::ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        format!("history limit must not exceed {HISTORY_MAX_LIMIT}"),
                    ))
                } else {
                    Ok(ResultBody::History {
                        operations: self.store.history(Some(limit)),
                    })
                };
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::OperationGet { operation_id } => {
                match self.store.find_record(&operation_id) {
                    Some(
                        record @ (remote_workspace_protocol::AnyOperationRecord::Fs(_)
                        | remote_workspace_protocol::AnyOperationRecord::Exec(_)
                        | remote_workspace_protocol::AnyOperationRecord::Transfer(_)),
                    ) => {
                        self.finish(
                            &request_id,
                            Ok(ResultBody::Operation(OperationDetails { record })),
                        )
                        .await
                        .with_stdout(&stdout)
                        .await;
                    }
                    None | Some(_) => {
                        // Prepared/Aborted already filtered by find_record
                        let err = remote_workspace_protocol::ProtocolError::new(
                            ErrorCode::OperationNotFound,
                            format!("operation not found: {operation_id}"),
                        );
                        self.finish_err(&request_id, err)
                            .await
                            .with_stdout(&stdout)
                            .await;
                    }
                }
            }
            RequestBody::RequestStatus { target: rid } => {
                let result = self.store.status_for_request(&rid);
                self.finish(&request_id, Ok(ResultBody::RequestStatus(result)))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Gc { keep } => {
                let guard = self.store.write_guard().await;
                let result = match keep.or(self.history_limit) {
                    Some(k) => self.store.prune(k).map(|s| {
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
                        let scratch =
                            scratch::enforce(&self.workspace.scratch_root, self.scratch_max_age);
                        ResultBody::Gc(remote_workspace_protocol::GcResult {
                            removed_operations: s.removed_operations,
                            removed_requests: s.removed_requests,
                            retained_operations: s.retained_operations,
                            removed_stale_staging,
                            scratch,
                        })
                    }),
                    None => Err(remote_workspace_protocol::ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "server has no history limit configured; pass keep explicitly",
                    )),
                };
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
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
        // Allocate the operation id up front so that even a rejected exec
        // (bad profile, empty argv, missing cwd) consumes an id and is
        // recorded. This keeps ids monotonic and lets operation.get/history
        // report the attempted command.
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
            operation_id.clone(),
        )
        .await;

        match outcome {
            Ok(o) => {
                // disposition reflects what actually happened: Completed if it
                // ran to an exit code, TimedOut if killed by timeout (but it
                // DID run, so duration and captured output are meaningful).
                let disposition = if matches!(
                    o.termination,
                    remote_workspace_protocol::ExecTermination::TimedOut
                ) {
                    remote_workspace_protocol::ExecDisposition::TimedOut
                } else {
                    remote_workspace_protocol::ExecDisposition::Completed
                };
                let result = remote_workspace_protocol::ExecResult {
                    operation_id: o.operation_id.clone(),
                    termination: o.termination,
                    duration_ms: o.duration_ms,
                    drain_timed_out: o.drain_timed_out,
                    stdout: o.stdout.clone(),
                    stderr: o.stderr.clone(),
                };
                let record = remote_workspace_protocol::ExecOperationRecord {
                    operation_id: o.operation_id.clone(),
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
                        Some(remote_workspace_protocol::ErrorCode::ExecFailed)
                    } else {
                        None
                    },
                    stdout: o.stdout,
                    stderr: o.stderr,
                };
                if let Err(e) = self.store.append_exec_record(record) {
                    let _ = self.store.remember_error(request_id, e.clone());
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
                let body = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: ResultBody::Exec(result),
                };
                if let Err(log_err) = self.store.remember_result(request_id, body.clone()) {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id: request_id.to_string(),
                            error: log_err,
                        },
                    )
                    .await;
                    return;
                }
                write_line(&stdout, &body).await;
            }
            Err(e) => {
                // Rejected: the command never started (bad profile/cwd/argv, or
                // spawn failure). It consumed an id, so record it with the
                // Rejected disposition. Logging failures are surfaced, not
                // swallowed.
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
                let record_err = self.store.append_exec_record(record).err();
                let remember_err = self.store.remember_error(request_id, e.clone()).err();
                let report = remember_err.or(record_err).unwrap_or(e);
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

    /// Wrap a sync result into a ServerMessage, remember it, and return it for
    /// writing. If persisting the result to the request log fails, the client
    /// is told the operation failed (with an IO error), so the server never
    /// reports success for state it could not durably record. This honors the
    /// repo's no-silent-failure rule.
    async fn finish(
        &self,
        request_id: &str,
        result: Result<ResultBody, remote_workspace_protocol::ProtocolError>,
    ) -> FinishResult {
        match result {
            Ok(body) => {
                let msg = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: body,
                };
                match self.store.remember_result(request_id, msg.clone()) {
                    Ok(()) => FinishResult::Msg(msg),
                    Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                        request_id: request_id.to_string(),
                        error: log_err,
                    }),
                }
            }
            Err(e) => match self.store.remember_error(request_id, e.clone()) {
                Ok(()) => FinishResult::Msg(ServerMessage::Error {
                    request_id: request_id.to_string(),
                    error: e,
                }),
                Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                    request_id: request_id.to_string(),
                    error: log_err,
                }),
            },
        }
    }

    async fn finish_err(
        &self,
        request_id: &str,
        err: remote_workspace_protocol::ProtocolError,
    ) -> FinishResult {
        match self.store.remember_error(request_id, err.clone()) {
            Ok(()) => FinishResult::Msg(ServerMessage::Error {
                request_id: request_id.to_string(),
                error: err,
            }),
            Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                request_id: request_id.to_string(),
                error: log_err,
            }),
        }
    }
}

/// Counts a request from the moment it is accepted until its handler task
/// ends, including a handler that panics. Only a count that never drifts up
/// can be trusted by the idle timeout, since a phantom in-flight request would
/// keep the server -- and the workspace's state lock -- alive indefinitely.
struct InFlight(Arc<std::sync::atomic::AtomicUsize>);

impl InFlight {
    fn enter(counter: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

enum FinishResult {
    Msg(ServerMessage),
}

impl FinishResult {
    async fn with_stdout<W: tokio::io::AsyncWrite + Unpin + Send>(
        self,
        stdout: &Arc<tokio::sync::Mutex<W>>,
    ) {
        match self {
            FinishResult::Msg(m) => write_line(stdout, &m).await,
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn op_kind_str(body: &RequestBody) -> &str {
    match body {
        RequestBody::List { .. } => "list",
        RequestBody::Stat { .. } => "stat",
        RequestBody::Read { .. } => "read",
        RequestBody::Create { .. } => "create",
        RequestBody::Edit { .. } => "edit",
        RequestBody::Exec { .. } => "exec",
        RequestBody::Delete { .. } => "delete",
        RequestBody::UploadPrepare { .. } => "upload_prepare",
        RequestBody::UploadCommit { .. } => "upload_commit",
        RequestBody::UploadAbort { .. } => "upload_abort",
        RequestBody::DownloadRecord { .. } => "download_record",
        RequestBody::History { .. } => "history",
        RequestBody::OperationGet { .. } => "operation_get",
        RequestBody::RequestStatus { .. } => "request_status",
        RequestBody::Gc { .. } => "gc",
    }
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
