use std::io::{Read, Seek};
use std::path::Path;

use remote_workspace_protocol::{
    ErrorCode, FileEntry, ListEntry, ListKind, MutationResult, OperationKind, ProtocolError,
    ReadResult, ResultBody,
};
use tokio::sync::MutexGuard;

use crate::hash::hash_file;
use crate::store::OperationStore;
use crate::workspace::Workspace;

pub const LIST_DEFAULT_LIMIT: usize = 1000;
pub const LIST_MAX_LIMIT: usize = 1000;
/// Upper bound on create/edit text inputs, on the file being edited, and on
/// the resulting file. Larger or binary files go through upload_file.
pub const MAX_TEXT_BYTES: usize = 4 * 1024 * 1024;

pub fn list(
    ws: &Workspace,
    path: &str,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<ResultBody, ProtocolError> {
    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(LIST_DEFAULT_LIMIT);
    if limit == 0 || limit > LIST_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("list limit must be between 1 and {LIST_MAX_LIMIT} entries"),
        ));
    }
    let abs = ws.resolve(path)?;
    let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("list failed: {e}"))
        }
    })?;
    if !meta.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::NotADirectory,
            format!("not a directory: {path}"),
        ));
    }
    let mut entries: Vec<ListEntry> = std::fs::read_dir(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read_dir failed: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let m = e.metadata().ok();
            let kind = file_kind(&e.path());
            let size = m.as_ref().map(|m| m.len());
            ListEntry { name, kind, size }
        })
        .filter(|e| e.name != ".remote-workspace")
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let end = offset.saturating_add(limit).min(entries.len());
    let page = if offset >= entries.len() {
        Vec::new()
    } else {
        entries[offset..end].to_vec()
    };
    Ok(ResultBody::List(remote_workspace_protocol::ListResult {
        entries: page,
        next_offset: (end < entries.len()).then_some(end),
    }))
}

pub fn stat(ws: &Workspace, path: &str) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("stat failed: {e}"))
        }
    })?;
    let entry = entry_for(path, &abs, &meta);
    Ok(ResultBody::Stat { stat: entry })
}

pub fn read(
    ws: &Workspace,
    path: &str,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let meta = std::fs::metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}"))
        }
    })?;
    if meta.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::IsADirectory,
            format!("is a directory: {path}"),
        ));
    }
    let file_len = meta.len();
    let limit = limit.unwrap_or(READ_DEFAULT_LIMIT);
    if limit == 0 || limit > READ_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("read limit must be between 1 and {READ_MAX_LIMIT} bytes"),
        ));
    }
    let start = offset.unwrap_or(0);
    if start > file_len {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("offset {start} is past end of file ({file_len} bytes)"),
        ));
    }

    // Read only the requested window (plus enough slack to finish a codepoint
    // straddling its end), never the whole file: paging through a multi-GB exec
    // log is the documented way to read large output, so cost per page must
    // depend on the page, not on the file.
    let mut file = std::fs::File::open(&abs).map_err(io_read_error)?;
    file.seek(std::io::SeekFrom::Start(start))
        .map_err(io_read_error)?;
    let mut window = Vec::new();
    file.take(limit + UTF8_TAIL_SLACK)
        .read_to_end(&mut window)
        .map_err(io_read_error)?;

    // offset/limit are BYTE positions but must land on UTF-8 char boundaries.
    // In valid UTF-8 a boundary is any byte that is not a continuation byte;
    // content that is not valid UTF-8 is rejected below. Reject (not truncate)
    // a non-boundary offset so a bad request cannot yield mojibake.
    if window.first().is_some_and(|b| is_continuation(*b)) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("offset {start} is not on a UTF-8 character boundary"),
        ));
    }
    // Cut at `limit`, then walk FORWARD to the next char boundary. If we
    // instead rounded down, a multi-byte first codepoint with a tiny limit
    // would produce an empty page with truncated=true forever (the caller could
    // never make progress). Rounding up guarantees at least one codepoint is
    // returned whenever data remains and limit > 0.
    let mut end = window.len().min(limit as usize);
    while end < window.len() && is_continuation(window[end]) {
        end += 1;
    }
    window.truncate(end);
    // Reject non-UTF-8 content rather than silently returning a lossy
    // conversion: a coding agent should not edit a binary file through a
    // text-oriented API.
    let content = String::from_utf8(window).map_err(|_| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            "file is not valid UTF-8; binary reads are not supported",
        )
    })?;
    let next_offset = start + end as u64;
    let truncated = next_offset < file_len;
    // The hash exists to be passed back as `edit`'s base_hash, and `edit`
    // refuses files above MAX_TEXT_BYTES. Hashing a larger file would re-read
    // it in full on every page for a value nothing can consume.
    let hash = if file_len <= MAX_TEXT_BYTES as u64 {
        crate::hash::hash_file(&abs).map_err(io_read_error)?
    } else {
        None
    };
    Ok(ResultBody::Read(ReadResult {
        content,
        hash,
        truncated,
        next_offset: truncated.then_some(next_offset),
    }))
}

/// Extra bytes read past `limit` so a page ending mid-codepoint can be
/// extended to the next boundary; a UTF-8 sequence is at most 4 bytes.
const UTF8_TAIL_SLACK: u64 = 3;

fn is_continuation(byte: u8) -> bool {
    byte & 0xC0 == 0x80
}

fn io_read_error(e: std::io::Error) -> ProtocolError {
    ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}"))
}

pub const READ_DEFAULT_LIMIT: u64 = 65536;
pub const READ_MAX_LIMIT: u64 = 64 * 1024;

/// Validate base_hash and return the current hash. If `base_hash` is given and
/// does not match the file's current content, returns StaleFile.
fn check_base_hash(
    abs: &Path,
    base_hash: &Option<String>,
) -> Result<Option<String>, ProtocolError> {
    let current = hash_file(abs)?;
    if let Some(expected) = base_hash {
        match &current {
            Some(actual) if actual == expected => Ok(current),
            Some(actual) => Err(ProtocolError::new(
                ErrorCode::StaleFile,
                "file changed since base_hash",
            )
            .with_hashes(expected.clone(), actual.clone())),
            None => Err(ProtocolError::new(
                ErrorCode::StaleFile,
                "file does not exist but base_hash was given",
            )
            .with_hashes(expected.clone(), "sha256:".into())),
        }
    } else {
        Ok(current)
    }
}

/// Atomically write bytes to abs: temp file in the same directory, then
/// persist via rename. Preserves the original file's mode when overwriting an
/// existing file (so a 0755 script stays 0755). Returns (old_hash, new_hash).
fn atomic_write_bytes(
    abs: &Path,
    content: &[u8],
) -> Result<(Option<String>, String), ProtocolError> {
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mkdir failed: {e}")))?;
    let old_hash = hash_file(abs)?;
    let new_hash = crate::hash::hash_bytes(content);
    let tmp = tempfile::NamedTempFile::new_in(parent).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("temp file create failed: {e}"))
    })?;
    std::fs::write(tmp.path(), content)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}")))?;
    // Preserve the original file's permissions (and best-effort ownership) so
    // a write does not silently strip an executable bit or chmod.
    if let Ok(orig_meta) = std::fs::metadata(abs) {
        use std::os::unix::fs::PermissionsExt;
        let mode = orig_meta.permissions().mode();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).map_err(
            |e| ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}")),
        )?;
    }
    tmp.persist(abs).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("atomic persist failed: {e}"))
    })?;
    // Sync the parent directory so the rename is durable on journaling/COW
    // filesystems.
    crate::fsync::fsync_file_or_dir(abs).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("fsync after write failed: {e}"))
    })?;
    Ok((old_hash, new_hash))
}

/// Atomically install a NEW file at abs without ever replacing an existing
/// one: temp file in the same directory, then hard_link into place (which
/// fails with AlreadyExists instead of clobbering, even against a concurrent
/// creator such as a command run in the workspace).
fn atomic_create_bytes(abs: &Path, content: &[u8], client_path: &str) -> Result<(), ProtocolError> {
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mkdir failed: {e}")))?;
    let tmp = tempfile::NamedTempFile::new_in(parent).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("temp file create failed: {e}"))
    })?;
    std::fs::write(tmp.path(), content)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}")))?;
    // Temp files are 0600; a fresh workspace file should get conventional
    // permissions instead of inheriting that.
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644)).map_err(
            |e| ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}")),
        )?;
    }
    std::fs::hard_link(tmp.path(), abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            ProtocolError::new(
                ErrorCode::AlreadyExists,
                format!("already exists: {client_path}; modify existing files with edit"),
            )
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("link into place failed: {e}"))
        }
    })?;
    crate::fsync::fsync_file_or_dir(abs).map_err(|e| {
        ProtocolError::new(
            ErrorCode::IoError,
            format!("fsync after create failed: {e}"),
        )
    })?;
    Ok(())
}

/// Create a new text file. Refuses to touch an existing path: existing files
/// are modified only through `edit`, so there is exactly one editing path.
pub fn create(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    content: &str,
) -> Result<ResultBody, ProtocolError> {
    if content.len() > MAX_TEXT_BYTES {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("content exceeds {MAX_TEXT_BYTES} bytes; use upload_file for large files"),
        ));
    }
    let abs = ws.resolve(path)?;
    match std::fs::symlink_metadata(&abs) {
        Ok(_) => {
            return Err(ProtocolError::new(
                ErrorCode::AlreadyExists,
                format!("already exists: {path}; modify existing files with edit"),
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let new_hash = crate::hash::hash_bytes(content.as_bytes());
    // WAL step 1: durably record the prepared marker BEFORE touching the
    // workspace, so a crash mid-mutation is recoverable on startup.
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Create,
        path,
        None,
        new_hash.clone(),
    )?;
    // WAL step 2: exclusive atomic install.
    atomic_create_bytes(&abs, content.as_bytes(), path)?;
    // WAL step 3: commit.
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Create,
        path,
        None,
        new_hash.clone(),
    )?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: None,
        new_hash,
    }))
}

/// Replace an exact occurrence of `old_text` with `new_text` in an existing
/// UTF-8 file. The complete new content is built (and validated) before any
/// mutation; installation reuses the atomic-rename path and preserves mode.
#[allow(clippy::too_many_arguments)]
pub fn edit(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    base_hash: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
) -> Result<ResultBody, ProtocolError> {
    if old_text.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "old_text must not be empty",
        ));
    }
    if old_text == new_text {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "old_text and new_text are identical; nothing to change",
        ));
    }
    if old_text.len() > MAX_TEXT_BYTES || new_text.len() > MAX_TEXT_BYTES {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("old_text/new_text exceed {MAX_TEXT_BYTES} bytes"),
        ));
    }
    let abs = ws.resolve(path)?;
    // Enforce the size cap from metadata, before hashing or reading: a file
    // above the cap is rejected either way, and reading it first is precisely
    // the cost the cap exists to avoid.
    match std::fs::metadata(&abs) {
        Ok(meta) if meta.len() > MAX_TEXT_BYTES as u64 => {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!(
                    "file exceeds {MAX_TEXT_BYTES} bytes; edit does not support files this large"
                ),
            ))
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ProtocolError::new(
                ErrorCode::NotFound,
                format!("not found: {path}; new files are created with create, not edit"),
            ))
        }
        Err(e) => return Err(ProtocolError::new(ErrorCode::IoError, format!("{e}"))),
    }
    let current = check_base_hash(&abs, &Some(base_hash.to_string()))?;
    let original = std::fs::read(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}")))?;
    let original_str = String::from_utf8(original).map_err(|_| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            "file is not valid UTF-8; editing binary files is unsupported",
        )
    })?;
    let matches = original_str.matches(old_text).count();
    if matches == 0 {
        return Err(ProtocolError::new(
            ErrorCode::NoMatch,
            format!(
                "old_text not found in {path}; re-read the file and copy the current text exactly"
            ),
        ));
    }
    if matches > 1 && !replace_all {
        return Err(ProtocolError::new(
            ErrorCode::AmbiguousMatch,
            format!(
                "old_text occurs {matches} times in {path}; extend it with surrounding context \
                 to make it unique, or pass replace_all=true"
            ),
        ));
    }
    let new_content = if replace_all {
        original_str.replace(old_text, new_text)
    } else {
        original_str.replacen(old_text, new_text, 1)
    };
    if new_content.len() > MAX_TEXT_BYTES {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("resulting file would exceed {MAX_TEXT_BYTES} bytes"),
        ));
    }
    let new_hash = crate::hash::hash_bytes(new_content.as_bytes());
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Edit,
        path,
        current.clone(),
        new_hash.clone(),
    )?;
    let old_hash = current.clone();
    atomic_write_bytes(&abs, new_content.as_bytes())?;
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Edit,
        path,
        old_hash.clone(),
        new_hash.clone(),
    )?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash,
        new_hash,
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn delete(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    if abs.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::IsADirectory,
            format!("not a file: {path}"),
        ));
    }
    let before_hash = hash_file(&abs)?;
    if before_hash.is_none() {
        return Err(ProtocolError::new(
            ErrorCode::NotFound,
            format!("not found: {path}"),
        ));
    }
    // For a delete, "expected after" is the empty-file hash sentinel, matching
    // the deleted state (file absent).
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Delete,
        path,
        before_hash.clone(),
        "sha256:".into(),
    )?;
    std::fs::remove_file(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("remove failed: {e}")))?;
    if let Some(parent) = abs.parent() {
        crate::fsync::fsync_dir(parent).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("fsync after delete: {e}"))
        })?;
    }
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Delete,
        path,
        before_hash.clone(),
        "sha256:".into(),
    )?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: before_hash,
        new_hash: "sha256:".into(),
    }))
}

fn file_kind(path: &Path) -> ListKind {
    let ft = match std::fs::symlink_metadata(path) {
        Ok(m) => m.file_type(),
        Err(_) => return ListKind::File,
    };
    if ft.is_symlink() {
        ListKind::Symlink
    } else if ft.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    }
}

fn entry_for(client_path: &str, abs: &Path, meta: &std::fs::Metadata) -> FileEntry {
    use std::os::unix::fs::PermissionsExt;
    let kind = if meta.file_type().is_symlink() {
        ListKind::Symlink
    } else if meta.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    };
    let mode = meta.permissions().mode();
    FileEntry {
        path: client_path.to_string(),
        kind,
        size: meta.len(),
        hash: if meta.is_file() {
            hash_file(abs).ok().flatten()
        } else {
            None
        },
        mode: Some(remote_workspace_protocol::FileMode {
            readable: mode & 0o400 != 0,
            writable: mode & 0o200 != 0,
            executable: mode & 0o111 != 0,
        }),
    }
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use tempfile::tempdir;

    fn ws_with(path: &str, content: &str) -> (tempfile::TempDir, Workspace) {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(path), content).unwrap();
        let w = Workspace {
            root: dir.path().to_path_buf(),
            scratch_root: dir.path().join("scratch"),
        };
        (dir, w)
    }

    #[test]
    fn read_multibyte_offset_on_boundary_works() {
        // "éx" as UTF-8 is [0xC3,0xA9,0x78]; offset 2 lands on 'x'.
        let (_d, w) = ws_with("f.txt", "éx");
        let r = read(&w, "f.txt", Some(2), Some(1)).unwrap();
        match r {
            ResultBody::Read(r) => assert_eq!(r.content, "x"),
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_multibyte_offset_off_boundary_rejected_not_panic() {
        let (_d, w) = ws_with("f.txt", "éx");
        // offset 1 is mid-codepoint; must return an error, NOT panic.
        let res = read(&w, "f.txt", Some(1), Some(1));
        match res {
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            }) => {}
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn read_limit_mid_codepoint_snaps_up_to_boundary() {
        // "aébc": bytes a=1, é=2, b=1, c=1 -> total 5. limit 2 from offset 0
        // would end mid-é (byte 2); round UP to the next boundary (byte 3,
        // after 'é') so the page makes progress and never returns empty.
        let (_d, w) = ws_with("f.txt", "aébc");
        let r = read(&w, "f.txt", Some(0), Some(2)).unwrap();
        match r {
            ResultBody::Read(r) => {
                assert_eq!(r.content, "aé");
                assert!(r.truncated, "more content remains");
                assert_eq!(r.next_offset, Some(3));
            }
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_huge_offset_rejected_without_overflow() {
        let (_d, w) = ws_with("f.txt", "hi");
        let res = read(&w, "f.txt", Some(u64::MAX), Some(u64::MAX));
        assert!(matches!(
            res,
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
    }

    #[test]
    fn read_pages_large_file_without_hashing_it() {
        // Above the edit cap the hash has no consumer, so a page must not pay
        // for one -- the file is only touched over the requested window.
        let dir = tempdir().unwrap();
        let big = vec![b'x'; MAX_TEXT_BYTES + 1024];
        std::fs::write(dir.path().join("big.log"), &big).unwrap();
        let w = Workspace {
            root: dir.path().to_path_buf(),
            scratch_root: dir.path().join("scratch"),
        };
        let offset = MAX_TEXT_BYTES as u64;
        match read(&w, "big.log", Some(offset), Some(64)).unwrap() {
            ResultBody::Read(r) => {
                assert_eq!(r.content.len(), 64);
                assert_eq!(r.hash, None, "no hash above the edit cap");
                assert!(r.truncated);
                assert_eq!(r.next_offset, Some(offset + 64));
            }
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_at_end_of_file_returns_empty_final_page() {
        let (_d, w) = ws_with("f.txt", "hi");
        match read(&w, "f.txt", Some(2), Some(8)).unwrap() {
            ResultBody::Read(r) => {
                assert_eq!(r.content, "");
                assert!(!r.truncated);
                assert_eq!(r.next_offset, None);
            }
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_rejects_limit_above_hard_maximum() {
        let (_d, w) = ws_with("f.txt", "hi");
        let result = read(&w, "f.txt", None, Some(READ_MAX_LIMIT + 1));
        assert!(matches!(
            result,
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
    }
}
