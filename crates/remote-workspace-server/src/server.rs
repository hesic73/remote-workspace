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
/// How long, after stdin ends, handlers still running get to finish and write
/// their replies. Deliberately short: shutdown must not become the reason a
/// successor cannot start, and a request that needs longer than this is not
/// one worth holding the connection open for.
const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(2);
/// How long the abort that follows a timed-out drain gets to take effect.
const ABORT_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

pub struct Server {
    pub workspace: Arc<Workspace>,
    pub store: OperationStore,
    pub config: Arc<ServerConfig>,
    history_limit: Option<usize>,
    scratch_max_age: Option<std::time::Duration>,
    idle_timeout: Option<std::time::Duration>,
    session_log: Arc<SessionLog>,
    /// Pending uploads (staging file created, commit not yet received).
    uploads: transfer::UploadRegistry,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

pub struct ServerOptions {
    pub root: PathBuf,
    /// Resolved state directory (operation log, request table, scratch).
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
            tracing::warn!("startup recovery encountered one or more conflicts; affected requests are marked Error");
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
        // A failed sweep is reported, not fatal: refusing to start would also
        // take away the commands that could fix what made it fail.
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
        let uploads = transfer::UploadRegistry::new(&state_dir)?;
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
        // Reading happens in its own task and is never raced against anything.
        // `read_until` is not cancellation-safe -- bytes it has already consumed
        // are LOST when its future is dropped, which would silently truncate a
        // request that arrived in pieces -- so the idle timeout must not be
        // able to interrupt it. Receiving from this channel is cancel-safe, and
        // its capacity of one keeps the backpressure of reading a line at a
        // time.
        let (lines_tx, mut lines) = tokio::sync::mpsc::channel::<std::io::Result<Vec<u8>>>(1);
        let reading = tokio::spawn(async move {
            let mut reader = BufReader::new(read);
            loop {
                // Bytes, not `read_line`: a line that is not UTF-8 is one bad
                // request to answer, not a broken stream to shut down over.
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    // EOF: dropping the sender is what reports it.
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
        // Owns every handler task, which answers all three questions asked
        // about them: how many are still running (the idle timeout's), when the
        // last one finishes (shutdown's), and how to let go of the ones that
        // never will -- dropping the set aborts them, releasing the `Arc<Server>`
        // they hold and with it the state lock.
        let mut handlers = tokio::task::JoinSet::new();
        let mut served: u64 = 0;
        let mut read_error: Option<std::io::Error> = None;
        let mut idle_since = tokio::time::Instant::now();

        let reason = loop {
            // Idleness is time since the last thing that happened -- a line
            // arriving OR a handler finishing -- not time since the last read.
            // Measured from the read alone, a request that completes just
            // before the deadline is followed by an exit milliseconds later,
            // closing a connection the client has every reason to believe is
            // still good because it just got its reply on it.
            let event = match idle_timeout {
                None => lines.recv().await,
                Some(d) => tokio::select! {
                    biased;
                    ev = lines.recv() => ev,
                    // Armed only while something is running: on an empty set
                    // `join_next` returns immediately and would spin.
                    _ = handlers.join_next(), if !handlers.is_empty() => {
                        idle_since = tokio::time::Instant::now();
                        continue;
                    }
                    _ = tokio::time::sleep_until(idle_since + d) => {
                        // A single `exec` may legitimately run for an hour
                        // without the client sending anything, so idleness
                        // means no work in flight either -- otherwise the
                        // timeout would kill the very request it waits on.
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
                // Broken input is a shutdown like any other: it must still
                // drain and record its exit, so the error travels out rather
                // than returning from here.
                Some(Err(e)) => {
                    read_error = Some(e);
                    break "read_error";
                }
                None => break "stdin_eof",
            };
            idle_since = tokio::time::Instant::now();
            // Decoded lossily only to find the request id: a request that is not
            // UTF-8 is refused, never run against a path with U+FFFD in it.
            let text = String::from_utf8_lossy(&line);
            // Windows PowerShell 5.1 can put a UTF-8 byte order mark in front of
            // what it writes to a child's stdin, and `trim` keeps it.
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
                    // A request this server cannot read -- an op from a newer
                    // client, say -- usually still names itself, and echoing
                    // that id is what lets the sender stop waiting for a reply.
                    let request_id = serde_json::from_str::<serde_json::Value>(trimmed)
                        .ok()
                        .and_then(|v| v.get("request_id")?.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "(parse-error)".into());
                    let msg = ServerMessage::Error {
                        request_id,
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
            let server = server.clone();
            let stdout = stdout.clone();
            handlers.spawn(async move {
                server.handle(req, stdout).await;
            });
            // Finished handlers stay in the set until collected, so reap on the
            // way past: a long session serves thousands of requests and the set
            // must not grow with them.
            reap(&mut handlers);
        };

        // Handlers run in their own tasks, so returning straight from the loop
        // would drop whatever is still running: a mutation already applied to
        // the workspace would never report its result, and the caller could not
        // tell whether it landed. Stdin ending does not imply stdout has --
        // a client that closes its end of the pipe is still listening -- so
        // finishing the reply is worth a short wait. Bounded, because it must
        // stay well inside the successor's grace for the state lock, and
        // because a long `exec` is not something to wait for here: its request
        // is resolved by recovery on the next start.
        let drained = tokio::time::timeout(SHUTDOWN_DRAIN, async {
            while handlers.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        // Whatever is left is asked to stop rather than carried along: each
        // task holds an `Arc<Server>`, and with it the state lock. An `exec`'s
        // process tree dies with its task (see `ProcessGroup`).
        //
        // Aborting only *requests* cancellation, which a handler acts on no
        // earlier than its next yield -- a `gc` sweeping a large tree is
        // synchronous from one end to the other. So this is bounded, not
        // guaranteed: a handler still working when the grace expires keeps the
        // lock until it finishes, and a successor starting in that window is
        // correctly refused, because the work the lock protects really is still
        // running. In the shipped binary the process exit right after this
        // releases the lock either way; `drained` records which happened.
        handlers.abort_all();
        let _ = tokio::time::timeout(ABORT_GRACE, async {
            while handlers.join_next().await.is_some() {}
        })
        .await;
        // The reader is blocked in a read that only a closed input can end, and
        // dropping the receiving half does not wake it. Nothing is going to be
        // read again, so it is aborted rather than left holding the stream for
        // as long as the runtime lives -- which for an embedder is not the
        // process exit the shipped binary can rely on.
        reading.abort();
        // Awaited, because aborting only asks: until the task is polled again
        // it still holds the input stream, and returning from here is supposed
        // to mean it does not.
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
        &self,
        req: Request,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        let request_id = req.request_id.clone();

        // A wait only observes, so like the reads below it is neither recorded
        // nor replayed: asking again answers with the state as it is now.
        if let RequestBody::Wait {
            argv,
            cwd,
            profile,
            timeout_ms,
        } = &req.body
        {
            let result = exec::wait(
                &self.workspace,
                &self.config,
                cwd.as_deref(),
                profile.as_deref(),
                argv,
                *timeout_ms,
            )
            .await;
            let msg = match result {
                Ok(r) => ServerMessage::Result {
                    request_id,
                    result: ResultBody::Wait(r),
                },
                Err(e) => ServerMessage::Error {
                    request_id,
                    error: e,
                },
            };
            write_line(&stdout, &msg).await;
            return;
        }

        // These skip the request idempotency store. Re-running a read-only
        // operation is harmless and answers with the state as it is now, while
        // storing its result would keep file contents on disk and in memory for
        // nothing. Prepare and abort use their own pending-upload registry:
        // replaying prepare would expose an obsolete staging path and replaying
        // abort has no useful result.
        let unpersisted = match &req.body {
            RequestBody::List {
                path,
                offset,
                limit,
            } => Some(fs_ops::list(&self.workspace, path, *offset, *limit)),
            RequestBody::Stat { path } => Some(fs_ops::stat(&self.workspace, path)),
            RequestBody::Read {
                path,
                offset,
                limit,
            } => Some(fs_ops::read(&self.workspace, path, *offset, *limit)),
            RequestBody::History { limit } => Some(self.history(*limit)),
            RequestBody::OperationGet { operation_id } => Some(self.operation_get(operation_id)),
            RequestBody::RequestStatus { target } => Some(Ok(ResultBody::RequestStatus(
                self.store.status_for_request(target),
            ))),
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
            Ok(None) => {}
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
                // Two requests sharing an id can be in flight at once, since
                // handlers run concurrently. No shipped client does it; refuse
                // rather than re-execute.
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
                edits,
            } => {
                let guard = self.store.write_guard().await;
                let result = fs_ops::edit(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    &base_hash,
                    &edits,
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
            RequestBody::List { .. }
            | RequestBody::Wait { .. }
            | RequestBody::Stat { .. }
            | RequestBody::Read { .. }
            | RequestBody::History { .. }
            | RequestBody::OperationGet { .. }
            | RequestBody::RequestStatus { .. }
            | RequestBody::UploadPrepare { .. }
            | RequestBody::UploadAbort { .. } => {
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
            RequestBody::Gc { keep } => {
                let guard = self.store.write_guard().await;
                let result = match keep.or(self.history_limit) {
                    Some(k) => self.store.prune(k).and_then(|s| {
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
                            scratch::enforce(&self.workspace.scratch_root, self.scratch_max_age)
                                .map_err(|e| {
                                    remote_workspace_protocol::ProtocolError::new(
                                        ErrorCode::IoError,
                                        format!("scratch sweep failed: {e}"),
                                    )
                                })?;
                        Ok(ResultBody::Gc(remote_workspace_protocol::GcResult {
                            removed_operations: s.removed_operations,
                            removed_requests: s.removed_requests,
                            retained_operations: s.retained_operations,
                            removed_stale_staging,
                            scratch,
                        }))
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

    fn history(
        &self,
        limit: Option<usize>,
    ) -> Result<ResultBody, remote_workspace_protocol::ProtocolError> {
        let limit = limit.unwrap_or(HISTORY_DEFAULT_LIMIT);
        if limit > HISTORY_MAX_LIMIT {
            return Err(remote_workspace_protocol::ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("history limit must not exceed {HISTORY_MAX_LIMIT}"),
            ));
        }
        Ok(ResultBody::History {
            operations: self.store.history(Some(limit)),
        })
    }

    fn operation_get(
        &self,
        operation_id: &str,
    ) -> Result<ResultBody, remote_workspace_protocol::ProtocolError> {
        match self.store.find_record(operation_id) {
            Some(
                record @ (remote_workspace_protocol::AnyOperationRecord::Fs(_)
                | remote_workspace_protocol::AnyOperationRecord::Exec(_)
                | remote_workspace_protocol::AnyOperationRecord::Transfer(_)),
            ) => Ok(ResultBody::Operation(OperationDetails { record })),
            // Prepared/Aborted already filtered by find_record
            None | Some(_) => Err(remote_workspace_protocol::ProtocolError::new(
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
                    operation_id: operation_id.clone(),
                    termination: o.termination,
                    duration_ms: o.duration_ms,
                    drain_timed_out: o.drain_timed_out,
                    stdout: o.stdout.clone(),
                    stderr: o.stderr.clone(),
                };
                let record = remote_workspace_protocol::ExecOperationRecord {
                    operation_id: operation_id.clone(),
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
    /// reports success for state it could not durably record.
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
}

/// Collect every handler that has finished, so what is left in the set is what
/// is still running. A panicking handler is collected too (as an `Err`), which
/// is what keeps the count from drifting up -- and an over-count would mean the
/// idle timeout never fires and the workspace stays locked forever.
fn reap<T: 'static>(handlers: &mut tokio::task::JoinSet<T>) {
    while handlers.try_join_next().is_some() {}
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
        .expect("system clock is set before 1970")
}

fn op_kind_str(body: &RequestBody) -> &str {
    match body {
        RequestBody::List { .. } => "list",
        RequestBody::Stat { .. } => "stat",
        RequestBody::Read { .. } => "read",
        RequestBody::Create { .. } => "create",
        RequestBody::Edit { .. } => "edit",
        RequestBody::Exec { .. } => "exec",
        RequestBody::Wait { .. } => "wait",
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
