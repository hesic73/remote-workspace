use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;

pub type RequestId = String;
pub type OperationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub request_id: RequestId,
    #[serde(flatten)]
    pub body: RequestBody,
}

/// One replacement inside an `Edit`.
///
/// A list of these is applied **in order, each to the result of the one before
/// it**, and the file is written once at the end. So a replacement may match
/// text an earlier one produced, and a failure anywhere -- at any position in
/// the list -- leaves the file byte-for-byte unchanged.
///
/// `old_text` must occur in the content the preceding replacements produced:
/// zero occurrences fail with NO_MATCH, several with AMBIGUOUS_MATCH unless
/// `replace_all` is set. An empty `new_text` deletes the matched text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditSpec {
    pub old_text: String,
    pub new_text: String,
    #[serde(default)]
    pub replace_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RequestBody {
    List {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    Stat {
        path: String,
    },
    Read {
        path: String,
        #[serde(default)]
        offset: Option<u64>,
        #[serde(default)]
        limit: Option<u64>,
    },
    /// Create a new text file. Fails if the target already exists; existing
    /// files are modified only through `Edit`.
    Create {
        path: String,
        content: String,
    },
    /// Apply exact text replacements to an existing text file. `base_hash`
    /// pins the content the FIRST replacement is stated against; each later one
    /// is stated against what the ones before it produced. See `EditSpec`.
    Edit {
        path: String,
        base_hash: String,
        edits: Vec<EditSpec>,
    },
    Exec {
        argv: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    Delete {
        path: String,
    },
    /// Reserve an upload target and create a staging file next to it. The
    /// returned staging path is client-internal plumbing for the raw receiver;
    /// it must never surface in MCP tool results, history, or logs.
    UploadPrepare {
        path: String,
        overwrite: bool,
    },
    /// Atomically install a fully-staged upload. `size`/`sha256`/`duration_ms`
    /// are the client-verified transfer metadata to record.
    UploadCommit {
        transfer_id: String,
        size: u64,
        sha256: String,
        duration_ms: u64,
    },
    /// Drop a pending upload and delete its staging file.
    UploadAbort {
        transfer_id: String,
    },
    /// Record a completed download (data flowed through the raw sender; this
    /// only appends the metadata-only operation record).
    DownloadRecord {
        path: String,
        size: u64,
        sha256: String,
        duration_ms: u64,
    },
    History {
        #[serde(default)]
        limit: Option<usize>,
    },
    OperationGet {
        operation_id: OperationId,
    },
    RequestStatus {
        #[serde(rename = "target_request_id")]
        target: RequestId,
    },
    /// Prune stored history to the `keep` most recent operations and the
    /// request entries they reference; also sweeps scratch and stale upload
    /// staging. `None` uses the server's configured history limit.
    Gc {
        #[serde(default)]
        keep: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum ServerMessage {
    Result {
        request_id: RequestId,
        #[serde(flatten)]
        result: ResultBody,
    },
    Error {
        request_id: RequestId,
        #[serde(flatten)]
        error: ProtocolError,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResultBody {
    #[serde(rename = "list")]
    List(ListResult),
    #[serde(rename = "stat")]
    Stat { stat: FileEntry },
    #[serde(rename = "read")]
    Read(ReadResult),
    /// Result of any single-file mutation (create, edit, delete). The wire tag
    /// stays "write" so request logs recorded before the create/edit protocol
    /// still deserialize.
    #[serde(rename = "write")]
    Mutation(MutationResult),
    #[serde(rename = "exec")]
    Exec(ExecResult),
    /// Legacy result from the removed undo operation. No longer produced, kept
    /// so request logs written by older servers still deserialize.
    #[serde(rename = "undo")]
    Undo(UndoResult),
    #[serde(rename = "upload_prepare")]
    UploadPrepare(UploadPrepareResult),
    #[serde(rename = "upload_abort")]
    UploadAbort { transfer_id: String },
    #[serde(rename = "transfer")]
    Transfer(TransferResult),
    #[serde(rename = "history")]
    History {
        operations: Vec<crate::record::AnyOperationRecord>,
    },
    #[serde(rename = "operation")]
    Operation(OperationDetails),
    #[serde(rename = "request_status")]
    RequestStatus(RequestStatusResult),
    #[serde(rename = "gc")]
    Gc(GcResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEntry {
    pub name: String,
    pub kind: ListKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResult {
    pub entries: Vec<ListEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub kind: ListKind,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<FileMode>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FileMode {
    pub readable: bool,
    pub writable: bool,
    pub executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResult {
    pub operation_id: OperationId,
    pub old_hash: Option<String>,
    pub new_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResult {
    pub operation_id: OperationId,
    pub termination: ExecTermination,
    pub duration_ms: u64,
    /// True when output collection stopped before the pipes reached EOF: a
    /// descendant still held stdout/stderr at the drain deadline and the
    /// process group was killed. Output may be missing trailing bytes.
    #[serde(default)]
    pub drain_timed_out: bool,
    pub stdout: ExecOutput,
    pub stderr: ExecOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecTermination {
    Exited {
        code: i32,
    },
    TimedOut,
    /// Unix process terminated by a signal. Windows reports process exit codes
    /// through `Exited` because `ExitStatus` has no signal representation.
    Signaled {
        signal: i32,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecOutput {
    pub prefix: String,
    pub suffix: String,
    pub total_bytes: u64,
    pub omitted_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPrepareResult {
    pub transfer_id: String,
    /// Absolute staging path on the remote host, for the raw receiver only.
    /// Client-internal: never shown to the agent or persisted anywhere.
    pub staging_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResult {
    pub operation_id: OperationId,
    pub direction: crate::record::TransferDirection,
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub duration_ms: u64,
}

/// Legacy shape from the removed undo operation; see `ResultBody::Undo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoResult {
    pub operation_id: OperationId,
    pub restored_hash: Option<String>,
    pub new_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcResult {
    pub removed_operations: usize,
    pub removed_requests: usize,
    pub retained_operations: usize,
    /// Stale upload staging files (interrupted uploads) deleted by this gc.
    #[serde(default)]
    pub removed_stale_staging: usize,
    #[serde(default)]
    pub scratch: ScratchUsage,
}

/// What scratch holds after this gc, and what it removed to get there.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScratchUsage {
    pub files: usize,
    pub bytes: u64,
    /// Days since the least recently used surviving file was written or read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_days: Option<u32>,
    #[serde(default)]
    pub removed_files: usize,
    #[serde(default)]
    pub removed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationDetails {
    pub record: crate::record::AnyOperationRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestStatusResult {
    #[serde(rename = "target_request_id")]
    pub target: RequestId,
    pub status: RequestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestStatus {
    Unknown,
    InProgress,
    Done,
    Error,
}
