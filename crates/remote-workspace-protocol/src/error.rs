use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidRequest,
    UnknownOperation,
    PathOutsideRoot,
    NotFound,
    NotAFile,
    NotADirectory,
    IsADirectory,
    InvalidHash,
    StaleFile,
    IoError,
    ExecFailed,
    UndoConflict,
    OperationNotFound,
    RequestNotFound,
    /// Legacy code from the removed line-based patch operation. No longer
    /// produced, kept so request logs written by older servers deserialize.
    PatchFailed,
    AlreadyExists,
    NoMatch,
    AmbiguousMatch,
}

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_hash: Option<String>,
}

impl ProtocolError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            expected_hash: None,
            actual_hash: None,
        }
    }

    pub fn with_hashes(mut self, expected: String, actual: String) -> Self {
        self.expected_hash = Some(expected);
        self.actual_hash = Some(actual);
        self
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        let code = if e.kind() == std::io::ErrorKind::NotFound {
            ErrorCode::NotFound
        } else {
            ErrorCode::IoError
        };
        ProtocolError::new(code, e.to_string())
    }
}
