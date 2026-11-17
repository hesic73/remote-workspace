mod error;
mod messages;
mod record;
mod version;

pub use error::{ErrorCode, ProtocolError};
pub use messages::{
    EditSpec, ExecOutput, ExecResult, ExecTermination, FileEntry, FileMode, GcResult, ListEntry,
    ListKind, ListResult, MutationResult, OperationDetails, OperationId, ReadOptions, ReadResult,
    Request, RequestBody, RequestId, ResultBody, ScratchUsage, ServerInfoResult, ServerMessage,
    TransferResult, UndoResult, UploadPrepareResult, WaitCheck, WaitOutcome, WaitResult,
};
pub use record::{
    AbortedRecord, AnyOperationRecord, ExecDisposition, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord, TransferDirection, TransferOperationRecord,
};
pub use version::{preflight, should_replace, InstallOutcome, Preflight, VersionInfo};

/// 5: removes `request_status` and `GcResult::removed_requests`; adds
/// `server_info`, line/tail `read` options and metadata on `ReadResult`, and
/// `replace_content` on `edit`.
///
/// 4: the `wait` request. An older server cannot parse it, so it must be
/// upgraded before the client can offer the tool.
///
/// 3: `edit` carries a list of replacements instead of a single one. The old
/// single-replacement shape no longer deserializes, so a client that still
/// sends it must be refused up front rather than fail per-call.
///
/// 2: the `undo` operation was removed. A client that still sends it must be
/// refused rather than fail per-call, so this is an incompatible change.
pub const PROTOCOL_VERSION: u32 = 5;

/// One run of a `wait` check may take this long; a slower check fails the wait.
pub const WAIT_CHECK_TIMEOUT_MS: u64 = 30_000;
