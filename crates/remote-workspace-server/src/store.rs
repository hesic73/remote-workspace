use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use remote_workspace_protocol::{
    AbortedRecord, AnyOperationRecord, ErrorCode, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord, ProtocolError, RequestStatus, RequestStatusResult,
    ServerMessage,
};
use tokio::sync::Mutex as AsyncMutex;

/// Result of a request, stored so that reconnects can query status and
/// replayed request_ids are not executed twice.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum StoredResult {
    Done(ServerMessage),
    Error(ProtocolError),
}

#[derive(Clone)]
pub struct RequestEntry {
    pub status: RequestStatus,
    pub result: Option<StoredResult>,
    pub op: Option<String>,
}

/// One action taken by startup recovery (for logging/observability).
#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// Prepared marker dropped: the rename never took effect (file == before).
    Dropped { operation_id: String },
    /// Commit synthesized: rename completed but commit-log write had not.
    Synthesized { operation_id: String },
    /// Recovery conflict: file state matches neither before nor expected-after.
    Conflict {
        operation_id: String,
        reason: String,
    },
    /// A request stuck InProgress (crash mid-request) was cleared so it can be retried.
    ClearedStuck { request_id: String },
    /// An exec request stuck InProgress was permanently marked Error (per DESIGN,
    /// exec must not auto-retry after disconnection).
    StuckExecMarkedError { request_id: String },
}

enum RecoveryOutcome {
    NoOp,
    Committed,
    Conflict(String),
}

/// On-disk line in requests.jsonl.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RequestLogLine {
    request_id: String,
    status: RequestStatus,
    /// For terminal statuses, the serialized ServerMessage (Done) or error.
    #[serde(skip_serializing_if = "Option::is_none")]
    result_done: Option<ServerMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_error: Option<ProtocolError>,
    /// The original request kind ("exec", "write", ...). Used at recovery to
    /// decide whether a stuck InProgress request is retryable (fs, read-only)
    /// or must be permanently marked Error (exec, per DESIGN "不得自动重试").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    op: Option<String>,
}

/// Operation store: durable logs of all operations (fs + exec) and the request
/// idempotency table. All filesystem state lives in `log_dir`:
///
///   operations.jsonl   AnyOperationRecord per completed operation
///   requests.jsonl     one line per request lifecycle
///   scratch/           agent-visible runtime artifacts
///
/// The async `write_lock` serializes mutating handlers, so their in-memory
/// tables and workspace effects do not interleave. It does NOT cover the logs:
/// request-table entries and exec records are appended without it, so handler
/// tasks reach the files concurrently. That is what `log_append` is for, and
/// why `prune` -- which replaces the files wholesale -- takes it too.
#[derive(Clone)]
pub struct OperationStore {
    log_dir: PathBuf,
    operations_path: PathBuf,
    requests_path: PathBuf,
    next_id: Arc<Mutex<u64>>,
    records: Arc<Mutex<Vec<AnyOperationRecord>>>,
    requests: Arc<Mutex<HashMap<String, RequestEntry>>>,
    write_lock: Arc<AsyncMutex<()>>,
    /// Serializes physical appends to both logs. Distinct from `write_lock`,
    /// which serializes mutating handlers: request-table and exec records are
    /// appended without it, so handler tasks do reach the logs concurrently.
    log_append: Arc<Mutex<()>>,
    /// Set when a partial line could not be rolled back. Nothing may be
    /// appended after a fragment, so this stops trying -- see `append_line`.
    log_torn: Arc<std::sync::atomic::AtomicBool>,
    /// Exclusive flock on `<log_dir>/lock`, held for the store's lifetime so a
    /// second server on the same state directory fails fast instead of
    /// corrupting the shared logs (interleaved appends, colliding op ids).
    /// The kernel releases it automatically when the process dies.
    _dir_lock: Arc<std::fs::File>,
}

pub struct PruneStats {
    pub removed_operations: usize,
    pub removed_requests: usize,
    pub retained_operations: usize,
}

impl OperationStore {
    pub fn new(log_dir: PathBuf) -> Result<Self, ProtocolError> {
        std::fs::create_dir_all(&log_dir).map_err(io_to_protocol)?;
        // Grace period covers reconnects racing a predecessor that is still
        // shutting down after its stdin EOF.
        let dir_lock = acquire_dir_lock(&log_dir, std::time::Duration::from_secs(5))?;
        // Before-content blobs existed only to serve `undo`. With that
        // operation gone nothing can ever read them again, so a state
        // directory written by an older server hands its blobs back as disk.
        let legacy_blobs = log_dir.join("blobs");
        if legacy_blobs.is_dir() {
            match std::fs::remove_dir_all(&legacy_blobs) {
                Ok(()) => tracing::info!("removed legacy undo blobs"),
                Err(e) => tracing::warn!(error = %e, "could not remove legacy undo blobs"),
            }
        }
        let operations_path = log_dir.join("operations.jsonl");
        let requests_path = log_dir.join("requests.jsonl");

        let mut max_id = 0u64;
        // Pruning may empty the operation log; the counter file preserves the
        // high-water mark so pruned operation ids are never reallocated (a
        // stale id held by a client must fail, not resolve to a new operation).
        let counter_path = log_dir.join("op-counter");
        match std::fs::read_to_string(&counter_path) {
            Ok(s) => {
                let n = s.trim().parse::<u64>().map_err(|e| {
                    ProtocolError::new(
                        ErrorCode::IoError,
                        format!("corrupt op-counter file {counter_path:?}: {e}"),
                    )
                })?;
                max_id = max_id.max(n);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_to_protocol(e)),
        }
        // Read every appended record. A committed record supersedes an earlier
        // prepared marker for the same id; a prepared marker with NO matching
        // commit is left in the map for `recover()` to resolve against the
        // actual workspace state (it cannot be resolved here, before the
        // Workspace is available).
        let mut by_id: std::collections::BTreeMap<String, AnyOperationRecord> =
            std::collections::BTreeMap::new();
        let ops_lines = read_lines(&operations_path).map_err(io_to_protocol)?;
        let mut ops_needs_truncate = false;
        for (idx, line) in ops_lines.iter().enumerate() {
            let rec: AnyOperationRecord = match parse_log_line_strict(line, &operations_path, idx)?
            {
                ParseResult::Parsed(r) => r,
                ParseResult::SkippedCrashTruncated => {
                    ops_needs_truncate = true;
                    continue;
                }
                ParseResult::SkippedBlank => continue,
            };
            if let Some(n) = rec.operation_id().strip_prefix("op-") {
                if let Ok(parsed) = n.parse::<u64>() {
                    max_id = max_id.max(parsed);
                }
            }
            // A Prepared marker is provisional; a later committed record for
            // the same id overwrites it. Otherwise it stays for recovery.
            by_id.insert(rec.operation_id().to_string(), rec);
        }
        // Repair the log file: crash-truncated partial records need physical
        // removal; valid records without a trailing newline (crash between
        // write and newline) need the missing \n appended.
        if ops_needs_truncate {
            truncate_trailing_garbage(&ops_lines, &operations_path)?;
        } else {
            append_missing_newline(&ops_lines, &operations_path)?;
        }
        // Aborted markers have done their work by the time the log is read:
        // they exist only to stop the load from resurrecting the prepared line
        // they supersede, which is exactly what collapsing by id above just
        // did. Keeping them would give WAL bookkeeping a place in the table
        // `prune` treats as history, where an internal marker could take a
        // retained slot from an operation a caller asked to keep.
        let mut records: Vec<AnyOperationRecord> = by_id
            .into_values()
            .filter(|r| !matches!(r, AnyOperationRecord::Aborted(_)))
            .collect();
        records.sort_by_key(|r| r.timestamp_ms());

        let mut requests = HashMap::new();
        let req_lines = read_lines(&requests_path).map_err(io_to_protocol)?;
        let mut req_needs_truncate = false;
        for (idx, line) in req_lines.iter().enumerate() {
            let entry: RequestLogLine = match parse_log_line_strict(line, &requests_path, idx)? {
                ParseResult::Parsed(e) => e,
                ParseResult::SkippedCrashTruncated => {
                    req_needs_truncate = true;
                    continue;
                }
                ParseResult::SkippedBlank => continue,
            };
            let result = match (&entry.result_done, &entry.result_error) {
                (Some(m), None) => Some(StoredResult::Done(m.clone())),
                (None, Some(e)) => Some(StoredResult::Error(e.clone())),
                _ => None,
            };
            requests.insert(
                entry.request_id.clone(),
                RequestEntry {
                    status: entry.status,
                    result,
                    op: entry.op,
                },
            );
        }
        if req_needs_truncate {
            truncate_trailing_garbage(&req_lines, &requests_path)?;
        } else {
            append_missing_newline(&req_lines, &requests_path)?;
        }

        Ok(Self {
            log_dir,
            operations_path,
            requests_path,
            next_id: Arc::new(Mutex::new(max_id)),
            records: Arc::new(Mutex::new(records)),
            requests: Arc::new(Mutex::new(requests)),
            write_lock: Arc::new(AsyncMutex::new(())),
            log_append: Arc::new(Mutex::new(())),
            log_torn: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            _dir_lock: Arc::new(dir_lock),
        })
    }

    pub async fn write_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().await
    }

    pub fn next_operation_id(&self) -> String {
        let mut g = self.next_id.lock();
        *g += 1;
        format!("op-{}", *g)
    }

    /// Resolve WAL prepared markers against the actual workspace state, and
    /// clear stuck InProgress requests. Must be called once at startup, after
    /// the Workspace exists, before serving traffic.
    ///
    /// For each prepared-without-commit operation:
    ///   - current file hash == before_hash (or file absent and before None):
    ///     the rename never took effect; drop the prepared marker.
    ///   - current file hash == expected_after_hash: the rename completed but
    ///     the commit log write did not; synthesize the committed FsOperationRecord
    ///     and mark the owning request Done with a reconstructed result.
    ///   - any other value: recovery conflict; the file was changed by something
    ///     we cannot account for. Drop the prepared marker and mark the owning
    ///     request Done with an UNDO_CONFLICT-style error so it surfaces.
    ///
    /// Returns the list of recovery actions taken, for logging.
    pub fn recover(
        &self,
        ws: &crate::workspace::Workspace,
    ) -> Result<Vec<RecoveryAction>, ProtocolError> {
        let mut actions = Vec::new();
        // Snapshot the prepared markers.
        let prepareds: Vec<remote_workspace_protocol::PreparedRecord> = {
            let g = self.records.lock();
            g.iter()
                .filter_map(|r| match r {
                    AnyOperationRecord::Prepared(p) => Some(p.clone()),
                    _ => None,
                })
                .collect()
        };

        for p in &prepareds {
            let abs = match ws.resolve(&p.path) {
                Ok(abs) => abs,
                Err(_) => {
                    // Path no longer resolvable; treat as conflict.
                    self.resolve_prepared_as_conflict(p)?;
                    actions.push(RecoveryAction::Conflict {
                        operation_id: p.operation_id.clone(),
                        reason: "path no longer resolvable".into(),
                    });
                    continue;
                }
            };
            let current = crate::hash::hash_file(&abs)?;
            let outcome = match (&p.before_hash, &current) {
                (Some(b), Some(c)) if b == c => RecoveryOutcome::NoOp,
                (None, None) => RecoveryOutcome::NoOp,
                // "sha256:" sentinel means the file should NOT exist after the
                // mutation (it was deleted).
                (_, None) if p.expected_after_hash == FILE_DELETED_SENTINEL => {
                    RecoveryOutcome::Committed
                }
                (_, Some(c)) if c == &p.expected_after_hash => RecoveryOutcome::Committed,
                (Some(_b), None) => RecoveryOutcome::Conflict(
                    "file expected to exist after mutation but is absent".into(),
                ),
                (_, _) => RecoveryOutcome::Conflict(format!(
                    "file hash {current:?} matches neither before nor expected-after"
                )),
            };
            match outcome {
                RecoveryOutcome::NoOp => {
                    self.abort_prepared(&p.operation_id)?;
                    actions.push(RecoveryAction::Dropped {
                        operation_id: p.operation_id.clone(),
                    });
                }
                RecoveryOutcome::Committed => {
                    self.synthesize_commit(p)?;
                    actions.push(RecoveryAction::Synthesized {
                        operation_id: p.operation_id.clone(),
                    });
                }
                RecoveryOutcome::Conflict(reason) => {
                    self.resolve_prepared_as_conflict_with(p, &reason)?;
                    actions.push(RecoveryAction::Conflict {
                        operation_id: p.operation_id.clone(),
                        reason,
                    });
                }
            }
        }

        // Resolve stuck InProgress requests (crash mid-request).
        // A stuck request may already have a committed operation record on disk
        // (the crash happened after the commit but before the terminal result
        // was written). In that case, reconstruct the result from the committed
        // record rather than re-executing.
        let stuck: Vec<(String, Option<String>)> = {
            let g = self.requests.lock();
            g.iter()
                .filter_map(|(k, v)| {
                    if v.status == RequestStatus::InProgress && v.result.is_none() {
                        Some((k.clone(), v.op.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };
        for (rid, op) in stuck {
            // Before applying the default policy, check whether a committed
            // operation record for this request_id already exists on disk.
            if let Some(committed) = self.find_committed_by_request_id(&rid) {
                let msg = self.reconstruct_result(&committed);
                self.remember_result(&rid, msg)?;
                actions.push(RecoveryAction::Synthesized {
                    operation_id: committed.operation_id().to_string(),
                });
                continue;
            }

            if op.as_deref() == Some("exec") {
                let err = ProtocolError::new(
                    ErrorCode::ExecFailed,
                    "exec in progress at time of crash; retry with a new request_id",
                );
                self.remember_error(&rid, err)?;
                actions.push(RecoveryAction::StuckExecMarkedError { request_id: rid });
            } else if op.is_none() {
                // Old log records without an `op` field: we cannot determine
                // the request type. The safe default is to mark them as Error
                // so they are never re-executed silently.
                let err = ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    "request was in progress at time of crash and request type is unknown; retry with a new request_id",
                );
                self.remember_error(&rid, err)?;
                actions.push(RecoveryAction::StuckExecMarkedError { request_id: rid });
            } else {
                // Read-only or fs-without-prepared: the mutation never completed
                // its bookkeeping, so forget the request so it can be retried.
                self.requests.lock().remove(&rid);
                actions.push(RecoveryAction::ClearedStuck { request_id: rid });
            }
        }

        Ok(actions)
    }

    /// Supersede a prepared marker with a durable aborted one, for a mutation
    /// that was decided against after the marker was written. Without it,
    /// recovery would judge the marker against whatever the file holds later
    /// and could synthesize a commit for a mutation that never happened.
    ///
    /// Callers return this error in place of the one that made them withdraw:
    /// a log that cannot be written is the more serious fault, and reporting
    /// the refusal instead would claim the withdrawal was recorded when it was
    /// not.
    pub fn abort_prepared(&self, operation_id: &str) -> Result<(), ProtocolError> {
        // Durable first. The log is what recovery reads, so dropping the marker
        // from memory before the aborted record is on disk would leave a
        // failed append looking withdrawn to this process while the next server
        // still finds the prepared line.
        self.append_record_tracked(
            AnyOperationRecord::Aborted(AbortedRecord {
                operation_id: operation_id.to_string(),
                timestamp_ms: now_ms(),
            }),
            |g| {
                g.retain(|r| {
                    !(r.operation_id() == operation_id
                        && matches!(r, AnyOperationRecord::Prepared(_)))
                })
            },
        )
    }

    /// Append a real committed FsOperationRecord for a prepared op whose
    /// workspace effect was confirmed, and mark its request Done.
    fn synthesize_commit(
        &self,
        p: &remote_workspace_protocol::PreparedRecord,
    ) -> Result<(), ProtocolError> {
        let record = FsOperationRecord {
            operation_id: p.operation_id.clone(),
            request_id: p.request_id.clone(),
            kind: p.kind,
            path: p.path.clone(),
            before_hash: p.before_hash.clone(),
            after_hash: p.expected_after_hash.clone(),
            timestamp_ms: now_ms(),
        };
        let any = AnyOperationRecord::Fs(record.clone());
        let op_id = p.operation_id.clone();
        self.append_record_tracked(any.clone(), move |g| {
            match g.iter_mut().find(|r| r.operation_id() == op_id) {
                Some(slot) => *slot = any,
                None => g.push(any),
            }
        })?;
        // Reconstruct the terminal result so replay/status work after restart.
        let msg = self.reconstruct_result(&AnyOperationRecord::Fs(record));
        self.remember_result(&p.request_id, msg)?;
        Ok(())
    }

    fn resolve_prepared_as_conflict(
        &self,
        p: &remote_workspace_protocol::PreparedRecord,
    ) -> Result<(), ProtocolError> {
        self.resolve_prepared_as_conflict_with(p, "recovery conflict")
    }

    fn resolve_prepared_as_conflict_with(
        &self,
        p: &remote_workspace_protocol::PreparedRecord,
        reason: &str,
    ) -> Result<(), ProtocolError> {
        self.abort_prepared(&p.operation_id)?;
        let err = ProtocolError::new(
            ErrorCode::StaleFile,
            format!(
                "startup recovery could not reconcile operation {}: {reason}",
                p.operation_id
            ),
        );
        self.remember_error(&p.request_id, err)?;
        Ok(())
    }

    /// Atomically claim a request. Returns the existing entry if the request
    /// has been seen before, or `None` if the caller has won ownership. The
    /// in-memory map is updated ONLY after the durable write succeeds, so a
    /// failed persist never leaves a phantom InProgress entry. The `op` string
    /// enables startup recovery to distinguish exec (must not be retried) from
    /// fs/read-only requests.
    pub fn claim_request(
        &self,
        request_id: &str,
        op: &str,
    ) -> Result<Option<RequestEntry>, ProtocolError> {
        // Known already: no need to serialize with anything.
        if let Some(entry) = self.requests.lock().get(request_id).cloned() {
            return Ok(Some(entry));
        }
        // Otherwise decide and record under one lock. Deciding first and
        // appending after would let two callers racing on one request_id both
        // write an InProgress line -- and the loser's, landing after the
        // winner's terminal one, is the line a restart would read, undoing a
        // result that had already been reported and making the request
        // executable a second time.
        let _appends = self.log_append.lock();
        if let Some(entry) = self.requests.lock().get(request_id).cloned() {
            return Ok(Some(entry));
        }
        // Persist FIRST; only update memory on success.
        self.append_request_line(RequestLogLine {
            request_id: request_id.to_string(),
            status: RequestStatus::InProgress,
            result_done: None,
            result_error: None,
            op: Some(op.to_string()),
        })?;
        self.requests.lock().insert(
            request_id.to_string(),
            RequestEntry {
                status: RequestStatus::InProgress,
                result: None,
                op: Some(op.to_string()),
            },
        );
        Ok(None)
    }

    pub fn remember_result(
        &self,
        request_id: &str,
        msg: ServerMessage,
    ) -> Result<(), ProtocolError> {
        self.append_request_tracked(
            RequestLogLine {
                request_id: request_id.to_string(),
                status: RequestStatus::Done,
                result_done: Some(msg.clone()),
                result_error: None,
                op: None,
            },
            |g| {
                g.insert(
                    request_id.to_string(),
                    RequestEntry {
                        status: RequestStatus::Done,
                        result: Some(StoredResult::Done(msg)),
                        op: None,
                    },
                );
            },
        )
    }

    pub fn remember_error(
        &self,
        request_id: &str,
        err: ProtocolError,
    ) -> Result<(), ProtocolError> {
        self.append_request_tracked(
            RequestLogLine {
                request_id: request_id.to_string(),
                status: RequestStatus::Error,
                result_done: None,
                result_error: Some(err.clone()),
                op: None,
            },
            |g| {
                g.insert(
                    request_id.to_string(),
                    RequestEntry {
                        status: RequestStatus::Error,
                        result: Some(StoredResult::Error(err)),
                        op: None,
                    },
                );
            },
        )
    }

    pub fn lookup_request(&self, request_id: &str) -> Option<RequestEntry> {
        self.requests.lock().get(request_id).cloned()
    }

    // ---- fs operation records (WAL: prepare then commit) ----

    /// Write a "prepared" marker line for an fs mutation BEFORE touching the
    /// workspace, so a crash between the rename and the commit log can be
    /// detected on startup. The prepared record stores everything recovery
    /// needs: the kind, the resolved path, the before hash (None = creation)
    /// and the expected after hash. Returns the operation id to use for this
    /// mutation.
    pub fn prepare_fs_record(
        &self,
        request_id: &str,
        kind: OperationKind,
        path: &str,
        before_hash: Option<String>,
        expected_after_hash: String,
    ) -> Result<String, ProtocolError> {
        let op_id = self.next_operation_id();
        let prepared = PreparedRecord {
            operation_id: op_id.clone(),
            request_id: request_id.to_string(),
            kind,
            path: path.to_string(),
            before_hash,
            expected_after_hash,
            timestamp_ms: now_ms(),
        };
        let appends = self.log_append.lock();
        match self.append_record_locked(AnyOperationRecord::Prepared(prepared.clone())) {
            Ok(()) => {}
            Err(AppendFailure::Unsynced(e)) => {
                // The line is in the log even though the sync meant to make it
                // durable failed, and the caller is about to abandon the
                // mutation. Supersede it, or a later start could read it as
                // work that was under way and credit this request with whatever
                // the file holds by then. Best effort by necessity: on a disk
                // that just refused a sync there is nothing better left to try,
                // and the error being returned already says so.
                let _ = self.append_record_locked(AnyOperationRecord::Aborted(AbortedRecord {
                    operation_id: op_id,
                    timestamp_ms: now_ms(),
                }));
                return Err(e);
            }
            Err(AppendFailure::Incomplete(e)) => {
                // Nothing may follow a possibly-partial line: an unterminated
                // tail is what the loader discards, and writing after it would
                // make the pair a terminated line that cannot parse, which it
                // must refuse instead.
                return Err(e);
            }
        }
        // Also into the in-memory table, which is what `prune` rewrites the log
        // from: a marker missing there is erased by any `gc` that runs before
        // the next start, taking with it the only record that a mutation was
        // under way. `history` filters WAL markers out, so this stays invisible
        // to callers, and `prune` never removes one however small its `keep`.
        self.records
            .lock()
            .push(AnyOperationRecord::Prepared(prepared));
        drop(appends);
        Ok(op_id)
    }

    /// Commit an fs mutation by appending the final FsOperationRecord. Because
    /// operations.jsonl is append-only, the committed record follows the
    /// prepared one; on load the committed record supersedes the prepared
    /// marker.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_fs_record(
        &self,
        operation_id: &str,
        request_id: &str,
        kind: OperationKind,
        path: &str,
        before_hash: Option<String>,
        after_hash: String,
    ) -> Result<FsOperationRecord, ProtocolError> {
        let record = FsOperationRecord {
            operation_id: operation_id.to_string(),
            request_id: request_id.to_string(),
            kind,
            path: path.to_string(),
            before_hash,
            after_hash,
            timestamp_ms: now_ms(),
        };
        let tracked = record.clone();
        self.append_record_tracked(AnyOperationRecord::Fs(record.clone()), move |g| {
            // The commit supersedes this operation's prepared marker.
            match g
                .iter_mut()
                .find(|r| r.operation_id() == tracked.operation_id)
            {
                Some(existing) => *existing = AnyOperationRecord::Fs(tracked),
                None => g.push(AnyOperationRecord::Fs(tracked)),
            }
        })?;
        Ok(record)
    }

    pub fn append_exec_record(&self, record: ExecOperationRecord) -> Result<(), ProtocolError> {
        self.append_record_tracked(AnyOperationRecord::Exec(record.clone()), |g| {
            g.push(AnyOperationRecord::Exec(record))
        })
    }

    pub fn append_transfer_record(
        &self,
        record: remote_workspace_protocol::TransferOperationRecord,
    ) -> Result<(), ProtocolError> {
        self.append_record_tracked(AnyOperationRecord::Transfer(record.clone()), |g| {
            g.push(AnyOperationRecord::Transfer(record))
        })
    }

    pub fn find_record(&self, operation_id: &str) -> Option<AnyOperationRecord> {
        self.records
            .lock()
            .iter()
            .find(|r| r.operation_id() == operation_id && r.is_committed())
            .cloned()
    }

    /// Find the first committed (Fs or Exec) record whose `request_id` field
    /// matches. Used during recovery to detect that a stuck InProgress request
    /// actually completed its operation — only the terminal result line was
    /// lost in the crash.
    pub fn find_committed_by_request_id(&self, request_id: &str) -> Option<AnyOperationRecord> {
        self.records
            .lock()
            .iter()
            .find(|r| {
                if !r.is_committed() {
                    return false;
                }
                match r {
                    AnyOperationRecord::Fs(f) => f.request_id == request_id,
                    AnyOperationRecord::Exec(e) => e.request_id == request_id,
                    AnyOperationRecord::Transfer(t) => t.request_id == request_id,
                    _ => false,
                }
            })
            .cloned()
    }

    /// Reconstruct a ServerMessage::Result from a committed operation record,
    /// so a stuck InProgress request's terminal result can be restored from
    /// the already-durable operation log.
    pub fn reconstruct_result(&self, record: &AnyOperationRecord) -> ServerMessage {
        match record {
            AnyOperationRecord::Fs(fs) => {
                let body = match fs.kind {
                    OperationKind::Create
                    | OperationKind::Edit
                    | OperationKind::Write
                    | OperationKind::Patch
                    | OperationKind::Delete
                    // A legacy undo record replays as the mutation it was.
                    | OperationKind::Undo => remote_workspace_protocol::ResultBody::Mutation(
                        remote_workspace_protocol::MutationResult {
                            operation_id: fs.operation_id.clone(),
                            old_hash: fs.before_hash.clone(),
                            new_hash: fs.after_hash.clone(),
                        },
                    ),
                };
                ServerMessage::Result {
                    request_id: fs.request_id.clone(),
                    result: body,
                }
            }
            AnyOperationRecord::Exec(e) => {
                // Rejected execs must reconstruct as Error, not Exit, so a
                // replayed request returns the same wire-level type as the
                // original invocation (idempotency). TimedOut and Completed
                // return Exit (the command did run and produced an exit code).
                match e.disposition {
                    remote_workspace_protocol::ExecDisposition::Rejected => {
                        let err = ProtocolError::new(
                            e.error_code.unwrap_or(ErrorCode::ExecFailed),
                            e.error.clone().unwrap_or_else(|| {
                                "exec was rejected at time of crash; retry with a new request_id"
                                    .into()
                            }),
                        );
                        ServerMessage::Error {
                            request_id: e.request_id.clone(),
                            error: err,
                        }
                    }
                    remote_workspace_protocol::ExecDisposition::Completed
                    | remote_workspace_protocol::ExecDisposition::TimedOut => match e.termination {
                        Some(termination) => ServerMessage::Result {
                            request_id: e.request_id.clone(),
                            result: remote_workspace_protocol::ResultBody::Exec(
                                remote_workspace_protocol::ExecResult {
                                    operation_id: e.operation_id.clone(),
                                    termination,
                                    duration_ms: e.duration_ms,
                                    drain_timed_out: e.drain_timed_out,
                                    stdout: e.stdout.clone(),
                                    stderr: e.stderr.clone(),
                                },
                            ),
                        },
                        None => ServerMessage::Error {
                            request_id: e.request_id.clone(),
                            error: ProtocolError::new(
                                ErrorCode::IoError,
                                "corrupt exec record: completed operation has no termination",
                            ),
                        },
                    },
                }
            }
            AnyOperationRecord::Transfer(t) => ServerMessage::Result {
                request_id: t.request_id.clone(),
                result: remote_workspace_protocol::ResultBody::Transfer(
                    remote_workspace_protocol::TransferResult {
                        operation_id: t.operation_id.clone(),
                        direction: t.direction,
                        path: t.path.clone(),
                        size: t.size,
                        sha256: t.sha256.clone(),
                        duration_ms: t.duration_ms,
                    },
                ),
            },
            _ => unreachable!("only committed records are passed"),
        }
    }

    pub fn history(&self, limit: Option<usize>) -> Vec<AnyOperationRecord> {
        let g = self.records.lock();
        let mut committed: Vec<AnyOperationRecord> =
            g.iter().filter(|r| r.is_committed()).cloned().collect();
        if let Some(n) = limit {
            if committed.len() > n {
                let start = committed.len() - n;
                committed = committed.split_off(start);
            }
        }
        for record in &mut committed {
            if let AnyOperationRecord::Exec(exec) = record {
                exec.stdout.prefix.clear();
                exec.stdout.suffix.clear();
                exec.stdout.omitted_bytes = exec.stdout.total_bytes;
                exec.stderr.prefix.clear();
                exec.stderr.suffix.clear();
                exec.stderr.omitted_bytes = exec.stderr.total_bytes;
            }
        }
        committed
    }

    pub fn status_for_request(&self, request_id: &str) -> RequestStatusResult {
        match self.lookup_request(request_id) {
            None => RequestStatusResult {
                target: request_id.to_string(),
                status: RequestStatus::Unknown,
                error: None,
            },
            Some(entry) => RequestStatusResult {
                target: request_id.to_string(),
                status: entry.status,
                error: match entry.result {
                    Some(StoredResult::Error(e)) => Some(e),
                    _ => None,
                },
            },
        }
    }

    /// Drop all but the `keep` most recent operation records, and drop request
    /// entries no longer referenced by a retained record (in-flight entries are
    /// always kept). Both JSONL files are rewritten atomically. Callers must hold the write guard, or run before
    /// serving traffic.
    ///
    /// Dropping a request entry shrinks the idempotency window: replaying a
    /// pruned request_id re-executes instead of returning the stored result.
    pub fn prune(&self, keep: usize) -> Result<PruneStats, ProtocolError> {
        // Replacing a log is an append's opposite number and has to exclude
        // them the same way: a handler that appends between the snapshot below
        // and the rename would have its record replaced away, having already
        // been told the operation succeeded. `append_line` takes no other lock
        // while it holds this one, so taking it first here cannot cycle.
        let _appends = self.log_append.lock();
        let mut records = self.records.lock();
        let mut requests = self.requests.lock();
        // A prepared marker is pending state, not history: it is the only thing
        // that lets the next start work out what became of a mutation whose
        // outcome is still unknown -- a rename that landed but could not be
        // made durable, say. So `keep` counts history only, and no value of it,
        // zero included, prunes a marker away.
        let prunable = records.iter().filter(|r| !is_pending(r)).count();
        let mut to_remove = prunable.saturating_sub(keep);
        let before_records = records.len();
        records.retain(|r| {
            if to_remove > 0 && !is_pending(r) {
                to_remove -= 1;
                false
            } else {
                true
            }
        });
        let removed_operations = before_records - records.len();
        let retained_ids: std::collections::HashSet<&str> =
            records.iter().filter_map(record_request_id).collect();
        let before_requests = requests.len();
        requests.retain(|id, e| {
            e.status == RequestStatus::InProgress || retained_ids.contains(id.as_str())
        });
        let removed_requests = before_requests - requests.len();
        // Persist the id high-water mark first: once records leave the log,
        // it is the only thing preventing operation-id reuse after a restart.
        let counter = *self.next_id.lock();
        let counter_path = self.log_dir.join("op-counter");
        // Written whole or not at all: this file is the only thing keeping a
        // pruned operation id from being handed out again, and `std::fs::write`
        // truncates first -- a write that then failed part-way would leave a
        // smaller number that the loader has no way to recognise as damaged
        // once the operations it came from have been pruned away.
        write_atomically(&counter_path, format!("{counter}\n").as_bytes())?;
        rewrite_jsonl(&self.operations_path, records.iter())?;
        let request_lines: Vec<RequestLogLine> = requests
            .iter()
            .map(|(id, e)| {
                let (result_done, result_error) = match &e.result {
                    Some(StoredResult::Done(m)) => (Some(m.clone()), None),
                    Some(StoredResult::Error(err)) => (None, Some(err.clone())),
                    None => (None, None),
                };
                RequestLogLine {
                    request_id: id.clone(),
                    status: e.status,
                    result_done,
                    result_error,
                    op: e.op.clone(),
                }
            })
            .collect();
        rewrite_jsonl(&self.requests_path, request_lines.iter())?;
        Ok(PruneStats {
            removed_operations,
            removed_requests,
            // History only, matching what `keep` counts and what a
            // caller can actually go on to read.
            retained_operations: records.iter().filter(|r| !is_pending(r)).count(),
        })
    }

    fn append_request_line(&self, line: RequestLogLine) -> Result<(), ProtocolError> {
        let serialized = serde_json::to_string(&line).map_err(|e| {
            ProtocolError::new(
                ErrorCode::IoError,
                format!("failed to serialize request log: {e}"),
            )
        })?;
        self.append_line(&self.requests_path, &serialized, "request log")
            .map_err(|f| match f {
                AppendFailure::Incomplete(e) | AppendFailure::Unsynced(e) => e,
            })
    }

    /// The request-log counterpart of `append_record_tracked`: the durable
    /// append and the matching change to the request table happen under one
    /// lock, so `prune` -- which rewrites the file from that table -- cannot
    /// snapshot it between the two and write back a file missing an entry that
    /// is already in it.
    fn append_request_tracked(
        &self,
        line: RequestLogLine,
        apply: impl FnOnce(&mut HashMap<String, RequestEntry>),
    ) -> Result<(), ProtocolError> {
        let _appends = self.log_append.lock();
        self.append_request_line(line)?;
        apply(&mut self.requests.lock());
        Ok(())
    }

    /// Append a record and make the matching change to the in-memory table
    /// without letting go in between.
    ///
    /// `prune` rewrites the file from that table, so the two must never be seen
    /// apart: a record already durable while the table has yet to hear about it
    /// is exactly the snapshot a concurrent prune would write back, dropping an
    /// operation its handler has already been told committed.
    fn append_record_tracked(
        &self,
        record: AnyOperationRecord,
        apply: impl FnOnce(&mut Vec<AnyOperationRecord>),
    ) -> Result<(), ProtocolError> {
        let _appends = self.log_append.lock();
        self.append_record_locked(record).map_err(|f| match f {
            AppendFailure::Incomplete(e) | AppendFailure::Unsynced(e) => e,
        })?;
        apply(&mut self.records.lock());
        Ok(())
    }

    /// The append itself, for callers already holding `log_append`.
    fn append_record_locked(&self, record: AnyOperationRecord) -> Result<(), AppendFailure> {
        let serialized = serde_json::to_string(&record).map_err(|e| {
            AppendFailure::Incomplete(ProtocolError::new(
                ErrorCode::IoError,
                format!("failed to serialize operation record: {e}"),
            ))
        })?;
        self.append_line(&self.operations_path, &serialized, "op log")
    }

    /// Append one line to a JSONL log, undoing a write that cannot be
    /// completed.
    ///
    /// The roll-back is the point. These logs are append-only and their loader
    /// repairs a tail with no newline -- a crash between the write and the
    /// newline -- by discarding or completing it. What it cannot repair is a
    /// fragment that a LATER append has welded onto and terminated: that is a
    /// newline-terminated line which does not parse, and the loader must treat
    /// it as a corrupted authoritative log and refuse. A server that stays up
    /// after a partial write (a volume that filled and was then freed, say)
    /// would produce exactly that.
    ///
    /// So a partial write is truncated away. The caller holds `log_append`,
    /// which is what makes the length taken beforehand still the end of the
    /// file -- handler tasks append concurrently, and the state flock only
    /// excludes other processes.
    /// If even the truncation fails the fragment is there to stay, and this
    /// store stops appending rather than weld onto it; the next start repairs
    /// the unterminated tail.
    fn append_line(
        &self,
        path: &std::path::Path,
        serialized: &str,
        what: &str,
    ) -> Result<(), AppendFailure> {
        let incomplete =
            |msg: String| AppendFailure::Incomplete(ProtocolError::new(ErrorCode::IoError, msg));
        if self.log_torn.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(incomplete(format!(
                "refusing to append to {what}: an earlier partial line could not be undone, \
                 and appending after it would leave a log the next start cannot read; \
                 restart the server to repair it"
            )));
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| incomplete(format!("open {what}: {e}")))?;
        let len_before = f
            .metadata()
            .map_err(|e| incomplete(format!("stat {what}: {e}")))?
            .len();
        if let Err(e) = writeln!(f, "{serialized}") {
            if let Err(t) = f.set_len(len_before) {
                self.log_torn
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(incomplete(format!(
                    "write {what}: {e}; the partial line could not be undone ({t}), so this \
                     server will not append again -- restart to repair"
                )));
            }
            return Err(incomplete(format!("write {what}: {e}")));
        }
        f.sync_all().map_err(|e| {
            AppendFailure::Unsynced(ProtocolError::new(
                ErrorCode::IoError,
                format!("fsync {what}: {e}"),
            ))
        })?;
        Ok(())
    }
}

/// How far a failed append got, which decides whether anything may be appended
/// after it.
///
/// A write that failed part-way can leave a line with no newline, and that is
/// precisely the shape the loader knows how to discard as a crash-truncated
/// tail. Appending after it would weld the next record onto the fragment and
/// terminate the pair with a newline, turning a repairable tail into a
/// newline-terminated line that does not parse -- which the loader must treat
/// as a corrupted authoritative log and refuse.
enum AppendFailure {
    /// The record may be a partial line. Nothing more may be appended.
    Incomplete(ProtocolError),
    /// The line is whole on disk; only its durability is unconfirmed.
    Unsynced(ProtocolError),
}

/// True for records that describe a mutation still in flight rather than one
/// that happened. They are reconciled by recovery and must outlive pruning.
fn is_pending(r: &AnyOperationRecord) -> bool {
    matches!(r, AnyOperationRecord::Prepared(_))
}

/// Sentinel value used as `expected_after_hash` when the mutation results in a
/// deleted file. Recovery treats `current == None` as matching this sentinel.
pub(crate) const FILE_DELETED_SENTINEL: &str = "sha256:";

fn record_request_id(r: &AnyOperationRecord) -> Option<&str> {
    match r {
        AnyOperationRecord::Fs(r) => Some(&r.request_id),
        AnyOperationRecord::Exec(r) => Some(&r.request_id),
        AnyOperationRecord::Transfer(r) => Some(&r.request_id),
        AnyOperationRecord::Prepared(r) => Some(&r.request_id),
        AnyOperationRecord::Aborted(_) => None,
    }
}

/// Take an exclusive advisory lock on `<log_dir>/lock`, retrying until
/// `grace` elapses. Held for the process lifetime; the kernel drops it on
/// exit, so a crash never leaves a stale lock.
fn acquire_dir_lock(
    log_dir: &std::path::Path,
    grace: std::time::Duration,
) -> Result<std::fs::File, ProtocolError> {
    use std::os::fd::AsRawFd;
    let path = log_dir.join("lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(io_to_protocol)?;
    let deadline = std::time::Instant::now() + grace;
    loop {
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // Record our pid (best effort) so a conflicting starter can name
            // the holder -- e.g. a zombie server on a half-dead ssh session.
            let _ = f.set_len(0);
            let _ = writeln!(&f, "{}", std::process::id());
            let _ = f.sync_all();
            return Ok(f);
        }
        if std::time::Instant::now() >= deadline {
            let holder_pid = std::fs::read_to_string(&path)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            // Recorded from the refused side: the holder writes nothing when it
            // is stuck, so this is what shows a workspace being knocked on
            // while something older still owns it.
            crate::session_log::SessionLog::new(log_dir).record(
                "lock_denied",
                serde_json::json!({ "holder_pid": holder_pid }),
            );
            let holder = holder_pid
                .map(|pid| format!(" (held by pid {pid})"))
                .unwrap_or_default();
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                format!(
                    "state directory {log_dir:?} is locked by another remote-workspace-server{holder}; \
                     one server serves a workspace root at a time, so another agent session is \
                     already using this workspace -- use a different root or wait for it to exit"
                ),
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Replace a JSONL file's contents atomically: write to a temp file in the
/// same directory, fsync, rename over the original, fsync the directory.
/// Replace a file's contents whole: write a temp file beside it, sync, rename.
/// A caller that cannot tolerate a half-written file uses this instead of
/// `std::fs::write`, which truncates before it writes.
fn write_atomically(path: &std::path::Path, bytes: &[u8]) -> Result<(), ProtocolError> {
    let dir = path.parent().ok_or_else(|| {
        ProtocolError::new(ErrorCode::IoError, format!("no parent dir for {path:?}"))
    })?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(io_to_protocol)?;
    tmp.write_all(bytes).map_err(io_to_protocol)?;
    tmp.as_file().sync_all().map_err(io_to_protocol)?;
    tmp.persist(path)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("persist {path:?}: {e}")))?;
    crate::fsync::fsync_file_or_dir(path).map_err(io_to_protocol)?;
    Ok(())
}

fn rewrite_jsonl<T: serde::Serialize>(
    path: &std::path::Path,
    items: impl Iterator<Item = T>,
) -> Result<(), ProtocolError> {
    let dir = path.parent().ok_or_else(|| {
        ProtocolError::new(ErrorCode::IoError, format!("no parent dir for {path:?}"))
    })?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(io_to_protocol)?;
    for item in items {
        let line = serde_json::to_string(&item)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("serialize: {e}")))?;
        writeln!(tmp, "{line}").map_err(io_to_protocol)?;
    }
    tmp.as_file().sync_all().map_err(io_to_protocol)?;
    tmp.persist(path)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("persist {path:?}: {e}")))?;
    crate::fsync::fsync_file_or_dir(path).map_err(io_to_protocol)?;
    Ok(())
}

/// A single logical line plus the byte offset in the file at which its content
/// (excluding the terminating newline) begins, and whether the file content
/// ended with a newline after this line. The trailing-newline flag lets the
/// loader distinguish a crash-truncated last record (no newline) from a
/// genuinely corrupted complete record (newline present but invalid JSON).
struct LineEntry {
    /// Valid UTF-8 text content. Empty string if the raw bytes are not valid
    /// UTF-8 (handled by the caller as a crash-truncated trailing line).
    text: String,
    /// Byte offset of the START of this line's content within the file.
    start_offset: usize,
    /// True if this line is terminated by a \n byte.
    has_newline: bool,
    /// True if the raw bytes of this line are valid UTF-8. An invalid trailing
    /// line (no newline, invalid UTF-8) is treated as crash-truncated.
    utf8_ok: bool,
    /// Byte length of this line's raw content, excluding the \n terminator
    /// (if present). Used to compute the byte range to remove when truncating.
    raw_len: usize,
}

fn read_lines(path: &PathBuf) -> std::io::Result<Vec<LineEntry>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < bytes.len() {
        match bytes[start..].iter().position(|&b| b == b'\n') {
            Some(i) => {
                let nl = start + i;
                let raw = &bytes[start..nl];
                let (text, utf8_ok) = bytes_to_text(raw);
                out.push(LineEntry {
                    text,
                    start_offset: start,
                    has_newline: true,
                    utf8_ok,
                    raw_len: raw.len(),
                });
                start = nl + 1;
            }
            None => {
                let raw = &bytes[start..];
                let (text, utf8_ok) = bytes_to_text(raw);
                out.push(LineEntry {
                    text,
                    start_offset: start,
                    has_newline: false,
                    utf8_ok,
                    raw_len: raw.len(),
                });
                break;
            }
        }
    }
    Ok(out)
}

fn bytes_to_text(bytes: &[u8]) -> (String, bool) {
    match std::str::from_utf8(bytes) {
        Ok(s) => (s.to_string(), true),
        Err(_) => (String::new(), false),
    }
}

/// The byte offset just past the last newline-terminated line. Used for
/// truncation.
fn valid_content_end_offset(entries: &[LineEntry]) -> usize {
    entries
        .iter()
        .filter(|e| e.has_newline)
        .map(|e| e.start_offset + e.raw_len + 1)
        .max()
        .unwrap_or(0)
}

/// Parse a JSONL line strictly. A corrupted line in the MIDDLE of the file is
/// a real data-integrity problem: silently skipping it would lose authoritative
/// operation records, allow operation-id reuse, or break request idempotency
/// (violating the repo's no-silent-failure rule). So we fail startup with the
/// file path and 1-based line number.
///
/// The return value distinguishes three outcomes:
///   Parsed(v)     — successfully deserialized; caller MUST keep the record.
///   SkippedBlank  — trailing blank line; skip it (no truncation needed).
///   SkippedCrashTruncated — parse failed on a trailing line WITHOUT a newline
///     (crash truncated a partial write). The caller should skip this record
///     AND signal to truncate the log file.
///
/// A line WITH a newline that fails to parse is a complete-but-corrupted record
/// and is a hard Err (startup must abort).
///
/// Returns:
///   Ok(Parsed(v))               — valid record
///   Ok(SkippedCrashTruncated)   — trailing, no newline, parse failed → truncate later
///   Ok(SkippedBlank)            — trailing blank (no-op)
///   Err(..)                     — corrupted middle line or blank middle line
#[allow(clippy::large_enum_variant)]
enum ParseResult<T> {
    Parsed(T),
    SkippedCrashTruncated,
    SkippedBlank,
}

fn parse_log_line_strict<T: serde::de::DeserializeOwned>(
    entry: &LineEntry,
    path: &std::path::Path,
    idx: usize,
) -> Result<ParseResult<T>, ProtocolError> {
    // Invalid UTF-8: if this is the last line (no newline) it's a
    // crash-truncated partial codepoint; otherwise it's a corrupted file.
    if !entry.utf8_ok {
        if !entry.has_newline {
            tracing::warn!(
                file = %path.display(),
                line = idx + 1,
                "dropping trailing line with invalid UTF-8 (crash mid-codepoint)"
            );
            return Ok(ParseResult::SkippedCrashTruncated);
        }
        return Err(ProtocolError::new(
            ErrorCode::IoError,
            format!(
                "corrupted log {:?}: line {} is not valid UTF-8; refusing to start",
                path.display(),
                idx + 1
            ),
        ));
    }
    let trimmed = entry.text.trim();
    if trimmed.is_empty() {
        // A trailing blank with no newline is just an empty tail end.
        // A blank WITH a newline in the middle is malformed JSONL.
        if !entry.has_newline {
            return Ok(ParseResult::SkippedBlank);
        }
        return Err(ProtocolError::new(
            ErrorCode::IoError,
            format!(
                "corrupted log {:?}: line {} is empty (jsonl records must not be blank)",
                path.display(),
                idx + 1
            ),
        ));
    }
    match serde_json::from_str::<T>(trimmed) {
        Ok(v) => Ok(ParseResult::Parsed(v)),
        Err(e) => {
            // No newline + parse failure → crash truncated.
            if !entry.has_newline {
                tracing::warn!(
                    file = %path.display(),
                    line = idx + 1,
                    error = %e,
                    "dropping crash-truncated trailing log line (no newline + invalid JSON)"
                );
                return Ok(ParseResult::SkippedCrashTruncated);
            }
            Err(ProtocolError::new(
                ErrorCode::IoError,
                format!(
                    "corrupted log {:?}: line {} is not valid JSON ({e}); \
                     refusing to start with a damaged authoritative record",
                    path.display(),
                    idx + 1
                ),
            ))
        }
    }
}

/// If the log file's last line lacks a terminating newline (a crash-truncated
/// partial record), physically truncate the file back to the end of the last
/// valid newline-terminated line and fsync. Without this, the next append
/// concatenates onto the partial JSON and permanently poisons the log, turning
/// a recoverable crash into an unrecoverable one.
fn truncate_trailing_garbage(
    entries: &[LineEntry],
    path: &std::path::Path,
) -> Result<(), ProtocolError> {
    let valid_end = valid_content_end_offset(entries);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(io_to_protocol)?;
    f.set_len(valid_end as u64).map_err(io_to_protocol)?;
    f.sync_all().map_err(io_to_protocol)?;
    if let Some(parent) = path.parent() {
        let dir = std::fs::OpenOptions::new()
            .read(true)
            .open(parent)
            .map_err(io_to_protocol)?;
        dir.sync_all().map_err(io_to_protocol)?;
    }
    tracing::info!(
        file = %path.display(),
        truncated_to_bytes = valid_end,
        "truncated crash-truncated trailing record from log"
    );
    Ok(())
}

fn append_missing_newline(
    entries: &[LineEntry],
    path: &std::path::Path,
) -> Result<(), ProtocolError> {
    let last = match entries.last() {
        Some(l) => l,
        None => return Ok(()),
    };
    if last.has_newline || last.text.trim().is_empty() || !last.utf8_ok {
        return Ok(());
    }
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(io_to_protocol)?;
    std::io::Write::write_all(&mut f, b"\n").map_err(io_to_protocol)?;
    f.sync_all().map_err(io_to_protocol)?;
    if let Some(parent) = path.parent() {
        let dir = std::fs::OpenOptions::new()
            .read(true)
            .open(parent)
            .map_err(io_to_protocol)?;
        dir.sync_all().map_err(io_to_protocol)?;
    }
    tracing::info!(
        file = %path.display(),
        "appended missing trailing newline to last valid record"
    );
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn io_to_protocol(e: std::io::Error) -> ProtocolError {
    ProtocolError::new(ErrorCode::IoError, e.to_string())
}

#[cfg(test)]
mod lock_tests {
    use super::acquire_dir_lock;
    use std::time::Duration;

    #[test]
    fn second_lock_on_same_dir_fails() {
        let dir = tempfile::tempdir().unwrap();
        let held = acquire_dir_lock(dir.path(), Duration::ZERO).unwrap();
        let err = acquire_dir_lock(dir.path(), Duration::ZERO).unwrap_err();
        assert!(err.message.contains("locked"), "unexpected: {err}");
        drop(held);
        acquire_dir_lock(dir.path(), Duration::ZERO).unwrap();
    }

    #[test]
    fn grace_period_waits_for_release() {
        let dir = tempfile::tempdir().unwrap();
        let held = acquire_dir_lock(dir.path(), Duration::ZERO).unwrap();
        let path = dir.path().to_path_buf();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            drop(held);
        });
        // Must succeed once the holder releases, well within the grace period.
        acquire_dir_lock(&path, Duration::from_secs(5)).unwrap();
        t.join().unwrap();
    }
}
