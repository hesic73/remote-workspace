use remote_workspace_protocol::{
    ErrorCode, FsOperationRecord, OperationKind, ProtocolError, ResultBody, UndoResult,
};
use tokio::sync::MutexGuard;

use crate::hash::{hash_bytes, hash_file};
use crate::store::{OperationStore, FILE_DELETED_SENTINEL};
use crate::workspace::Workspace;

/// Apply an undo to `target`. The undo itself is wrapped in the same WAL
/// (prepare → mutation → commit) as any other fs operation, so a crash during
/// undo is recoverable just like a crash during write. Allocates its own
/// operation id and returns it via the UndoResult.
pub fn undo(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    target: &FsOperationRecord,
) -> Result<ResultBody, ProtocolError> {
    if matches!(target.kind, OperationKind::Undo) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "cannot undo an undo operation",
        ));
    }
    let abs = ws.resolve(&target.path)?;
    let current = hash_file(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("hash failed: {e}")))?;

    // Determine (before_hash, expected_after_hash) for the undo action:
    // - If the target was a creation (before_hash = None), the undo deletes the
    //   file. So the prepared record will carry before_hash=Some(current) and
    //   expected_after_hash="sha256:" (FILE_DELETED_SENTINEL).
    // - Otherwise, the undo restores the before blob. The prepared record
    //   carries before_hash=Some(current) and expected_after_hash=the target's
    //   original before hash.
    let (undo_before_hash, undo_expected_after, undo_blob) = match (&target.before_hash, &current) {
        // Creation undo: undo removes the file.
        (None, Some(actual)) => {
            if actual != &target.after_hash {
                return Err(conflict(&target.after_hash, actual));
            }
            let before_blob = std::fs::read(&abs).ok();
            (Some(actual.clone()), "sha256:".to_string(), before_blob)
        }
        // Modification undo: undo restores the before content.
        (Some(_before), Some(actual)) => {
            if actual != &target.after_hash {
                return Err(conflict(&target.after_hash, actual));
            }
            let before_bytes = store
                .load_before_blob(&target.operation_id)
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::UndoConflict,
                        "before-content blob missing; cannot restore",
                    )
                })?;
            let restored_hash = hash_bytes(&before_bytes);
            let current_blob = std::fs::read(&abs).ok();
            (Some(actual.clone()), restored_hash, current_blob)
        }
        // File does not currently exist.
        (Some(_before), None) if target.after_hash == FILE_DELETED_SENTINEL => {
            // Undo of a delete: the file was removed by the target op. Restore
            // the before-content blob.
            let before_bytes = store
                .load_before_blob(&target.operation_id)
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::UndoConflict,
                        "before-content blob missing; cannot restore",
                    )
                })?;
            let restored_hash = hash_bytes(&before_bytes);
            (None, restored_hash, None)
        }
        (_, None) => {
            return Err(ProtocolError::new(
                ErrorCode::UndoConflict,
                format!("file no longer exists: {}", target.path),
            ))
        }
    };

    // ---- WAL: prepare ----
    let undo_op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Undo,
        &target.path,
        undo_before_hash.clone(),
        undo_expected_after.clone(),
        undo_blob.as_deref(),
    )?;

    // ---- mutation ----
    match &target.before_hash {
        // Creation undo: remove the file.
        None => {
            std::fs::remove_file(&abs).map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("remove failed: {e}"))
            })?;
            // Sync the parent dir so the removal is durable.
            if let Some(parent) = abs.parent() {
                crate::fsync::fsync_dir(parent).map_err(|e| {
                    ProtocolError::new(ErrorCode::IoError, format!("fsync after remove: {e}"))
                })?;
            }
        }
        // Modification undo: restore the before content.
        Some(_) => {
            let before_bytes = store
                .load_before_blob(&target.operation_id)
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::UndoConflict,
                        "before-content blob missing; cannot restore",
                    )
                })?;
            let parent = abs.parent().ok_or_else(|| {
                ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent")
            })?;
            let tmp = tempfile::NamedTempFile::new_in(parent).map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("temp file create: {e}"))
            })?;
            std::fs::write(tmp.path(), &before_bytes).map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}"))
            })?;
            if let Ok(orig_meta) = std::fs::metadata(&abs) {
                use std::os::unix::fs::PermissionsExt;
                let mode = orig_meta.permissions().mode();
                let _ = std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode));
            }
            tmp.persist(&abs).map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("atomic persist failed: {e}"))
            })?;
            crate::fsync::fsync_file_or_dir(&abs).map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("fsync after undo persist: {e}"))
            })?;
        }
    }

    // ---- WAL: commit ----
    store.commit_fs_record(
        &undo_op_id,
        request_id,
        OperationKind::Undo,
        &target.path,
        undo_before_hash,
        undo_expected_after.clone(),
    )?;

    Ok(ResultBody::Undo(UndoResult {
        operation_id: undo_op_id,
        restored_hash: target.before_hash.clone(),
        new_hash: undo_expected_after,
    }))
}

fn conflict(expected: &str, actual: &str) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UndoConflict,
        "file changed since target operation; refusing to overwrite",
    )
    .with_hashes(expected.to_string(), actual.to_string())
}
