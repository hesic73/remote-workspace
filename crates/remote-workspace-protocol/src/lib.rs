mod error;
mod messages;
mod record;
mod version;

pub use error::{ErrorCode, ProtocolError};
pub use messages::{
    EditSpec, ExecOutput, ExecResult, ExecTermination, FileEntry, FileMode, GcResult, ListEntry,
    ListKind, ListResult, MutationResult, OperationDetails, OperationId, ReadResult, Request,
    RequestBody, RequestId, RequestStatus, RequestStatusResult, ResultBody, ScratchUsage,
    ServerMessage, TransferResult, UndoResult, UploadPrepareResult,
};
pub use record::{
    AbortedRecord, AnyOperationRecord, ExecDisposition, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord, TransferDirection, TransferOperationRecord,
};
pub use version::{preflight, should_replace, InstallOutcome, Preflight, VersionInfo};

/// 3: `edit` carries a list of replacements instead of a single one. The old
/// single-replacement shape no longer deserializes, so a client that still
/// sends it must be refused up front rather than fail per-call.
///
/// 2: the `undo` operation was removed. A client that still sends it must be
/// refused rather than fail per-call, so this is an incompatible change.
pub const PROTOCOL_VERSION: u32 = 3;
