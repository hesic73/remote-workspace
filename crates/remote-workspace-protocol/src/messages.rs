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
    /// Replace an exact occurrence of `old_text` with `new_text` in an
    /// existing text file. Zero matches fail with NO_MATCH; multiple matches
    /// fail with AMBIGUOUS_MATCH unless `replace_all` is set.
    Edit {
        path: String,
        base_hash: String,
        old_text: String,
        new_text: String,
        #[serde(default)]
        replace_all: bool,
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
    Undo {
        operation_id: OperationId,
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
    /// Prune stored history: keep only the `keep` most recent operations
    /// (dropping their blobs) and the request entries they reference. `None`
    /// uses the server's configured history limit.
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
    Exited { code: i32 },
    TimedOut,
    Signaled { signal: i32 },
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoResult {
    pub operation_id: OperationId,
    pub restored_hash: Option<String>,
    pub new_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GcResult {
    pub removed_operations: usize,
    pub removed_requests: usize,
    pub retained_operations: usize,
    /// Stale upload staging files (interrupted uploads) deleted by this gc.
    #[serde(default)]
    pub removed_stale_staging: usize,
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
