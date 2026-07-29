mod error;
mod messages;
mod record;
mod version;

pub use error::{ErrorCode, ProtocolError};
pub use messages::{
    ExecOutput, ExecResult, ExecTermination, FileEntry, FileMode, GcResult, ListEntry, ListKind,
    ListResult, MutationResult, OperationDetails, OperationId, ReadResult, Request, RequestBody,
    RequestId, RequestStatus, RequestStatusResult, ResultBody, ScratchUsage, ServerMessage,
    TransferResult, UndoResult, UploadPrepareResult,
};
pub use record::{
    AbortedRecord, AnyOperationRecord, ExecDisposition, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord, TransferDirection, TransferOperationRecord,
};
pub use version::{preflight, should_replace, InstallOutcome, Preflight, VersionInfo};

pub const PROTOCOL_VERSION: u32 = 1;
