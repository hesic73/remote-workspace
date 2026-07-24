mod error;
mod messages;
mod record;
mod version;

pub use error::{ErrorCode, ProtocolError};
pub use version::{
    preflight, should_replace, software_cmp, InstallOutcome, Preflight, VersionInfo,
};
pub use messages::{
    ExecOutput, ExecResult, ExecTermination, FileEntry, FileMode, GcResult, ListEntry, ListKind,
    ListResult, MutationResult, OperationDetails, OperationId, ReadResult, Request, RequestBody,
    RequestId, RequestStatus, RequestStatusResult, ResultBody, ServerMessage, TransferResult,
    UndoResult, UploadPrepareResult,
};
pub use record::{
    AbortedRecord, AnyOperationRecord, ExecDisposition, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord, TransferDirection, TransferOperationRecord,
};

pub const PROTOCOL_VERSION: u32 = 1;
