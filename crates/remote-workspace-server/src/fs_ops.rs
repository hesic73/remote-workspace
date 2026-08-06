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
/// Upper bound on replacements in one `edit`. Each is separately bounded by
/// MAX_TEXT_BYTES; this bounds the request as a whole.
pub const MAX_EDITS: usize = 100;

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

/// True for a hash in the form this server issues. A caller that invents a
/// placeholder ("auto", "sha256:REPLACE") must not be told the file is stale:
/// that sends it to re-read and retry a call that will fail identically.
fn is_well_formed_hash(hash: &str) -> bool {
    match hash.strip_prefix("sha256:") {
        Some(hex) => hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
        None => false,
    }
}

/// The content the caller decided to overwrite still being what is on disk.
/// Taken as late as possible -- with the replacement already staged -- because
/// everything before it (reading, building the result, appending to the log) is
/// time in which `exec`, which deliberately runs without the mutation guard, or
/// any process on the host can write to the same path. Once the rename lands,
/// that write is gone.
fn verify_unchanged(abs: &Path, expected: &str, client_path: &str) -> Result<(), ProtocolError> {
    let now = hash_file(abs)?;
    if now.as_deref() != Some(expected) {
        return Err(ProtocolError::new(
            ErrorCode::StaleFile,
            format!("{client_path} changed while the edit was being prepared"),
        )
        .with_hashes(
            expected.to_string(),
            now.unwrap_or_else(|| FILE_ABSENT_HASH.into()),
        ));
    }
    Ok(())
}

/// Stands in for "no file" where a hash is expected.
const FILE_ABSENT_HASH: &str = "sha256:";

/// A replacement written to a temp file beside its target, ready to be renamed
/// over it.
///
/// Staging is separate from installing so the caller can take its last look at
/// the target with the new content already on disk: only the rename then
/// separates that look from the write it authorizes. It also makes each failure
/// unambiguous -- everything up to `install` leaves the workspace untouched.
struct StagedWrite {
    tmp: tempfile::NamedTempFile,
    new_hash: String,
}

/// Write `content` to a temp file in the target's directory, preserving the
/// target's mode if it exists (so a 0755 script stays 0755).
fn stage_write(abs: &Path, content: &[u8]) -> Result<StagedWrite, ProtocolError> {
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
    #[cfg(unix)]
    if let Ok(orig_meta) = std::fs::metadata(abs) {
        use std::os::unix::fs::PermissionsExt;
        let mode = orig_meta.permissions().mode();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).map_err(
            |e| ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}")),
        )?;
    }
    Ok(StagedWrite {
        tmp,
        new_hash: crate::hash::hash_bytes(content),
    })
}

/// The `create` half of the write-ahead sequence: record the intent, install
/// exclusively, record the result. Returns the operation id and the hash.
fn write_ahead_create(
    store: &OperationStore,
    request_id: &str,
    path: &str,
    abs: &Path,
    content: &[u8],
) -> Result<(String, String), ProtocolError> {
    let new_hash = crate::hash::hash_bytes(content);
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Create,
        path,
        None,
        new_hash.clone(),
    )?;
    if let Err(f) = atomic_create_bytes(abs, content, path) {
        // The path being empty right now is not a reason to leave the marker:
        // by the time recovery runs, another writer may have put the requested
        // bytes there, and recovery cannot tell them from this create's own.
        if !f.changed {
            store.abort_prepared(&op_id)?;
        }
        return Err(f.error);
    }
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Create,
        path,
        None,
        new_hash.clone(),
    )?;
    Ok((op_id, new_hash))
}

/// The `delete` half of the write-ahead sequence. Returns the operation id.
fn write_ahead_delete(
    store: &OperationStore,
    request_id: &str,
    path: &str,
    abs: &Path,
    before_hash: &Option<String>,
) -> Result<String, ProtocolError> {
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Delete,
        path,
        before_hash.clone(),
        FILE_ABSENT_HASH.into(),
    )?;
    if let Err(e) = std::fs::remove_file(abs) {
        // Withdrawn whatever went wrong: an absent file reads as success for a
        // delete marker, so a removal someone else did would be credited to
        // this request.
        store.abort_prepared(&op_id)?;
        return Err(ProtocolError::new(
            ErrorCode::IoError,
            format!("remove failed: {e}"),
        ));
    }
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
        FILE_ABSENT_HASH.into(),
    )?;
    Ok(op_id)
}

/// The `edit` half: record the intent, take the last look, install, record the
/// result. The order of these four is the whole correctness argument, and they
/// are one function so the unwinding stays with them. Returns the operation id
/// and the installed hash.
fn write_ahead_install(
    store: &OperationStore,
    request_id: &str,
    path: &str,
    abs: &Path,
    before: &str,
    staged: StagedWrite,
) -> Result<(String, String), ProtocolError> {
    let new_hash = staged.new_hash.clone();
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Edit,
        path,
        Some(before.to_string()),
        new_hash.clone(),
    )?;
    // The last look, with the replacement already staged: one syscall stands
    // between it and the rename. It cannot be made atomic -- POSIX offers no
    // "rename only if the target still hashes to X", and the writers this
    // guards against (`exec`, anything else on the host) hold no lock to
    // coordinate with -- so this narrows the window to its floor rather than
    // closing it.
    if let Err(e) = verify_unchanged(abs, before, path) {
        store.abort_prepared(&op_id)?;
        return Err(e);
    }
    if let Err(f) = staged.install(abs) {
        // A durability failure after the rename keeps its marker for the next
        // start to re-hash: committing here would claim a change survived a
        // crash when the sync guaranteeing that is what failed. Such a marker
        // is exempt from pruning, so a disk whose fsync keeps failing grows the
        // log by a line per attempt until restart.
        if !f.changed {
            store.abort_prepared(&op_id)?;
        }
        return Err(f.error);
    }
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Edit,
        path,
        Some(before.to_string()),
        new_hash.clone(),
    )?;
    Ok((op_id, new_hash))
}

/// A failed install, and whether it left the workspace changed.
///
/// This decides the prepared marker's fate, and neither mistake is recoverable
/// from: a marker kept for a mutation that never happened can be matched later
/// against a file some other writer produced and synthesized into a commit that
/// never occurred, while a marker withdrawn for a mutation that DID happen
/// loses the record of a real change. So the failing step has to say which.
#[derive(Debug)]
struct InstallFailure {
    changed: bool,
    error: ProtocolError,
}

impl StagedWrite {
    /// Rename into place, then sync the parent so the rename survives a crash
    /// on journaling/COW filesystems.
    fn install(self, abs: &Path) -> Result<String, InstallFailure> {
        self.tmp.persist(abs).map_err(|e| InstallFailure {
            changed: false,
            error: ProtocolError::new(ErrorCode::IoError, format!("atomic persist failed: {e}")),
        })?;
        crate::fsync::fsync_file_or_dir(abs).map_err(|e| InstallFailure {
            changed: true,
            error: ProtocolError::new(ErrorCode::IoError, format!("fsync after write failed: {e}")),
        })?;
        Ok(self.new_hash)
    }
}

/// Atomically install a NEW file at abs without ever replacing an existing
/// one: temp file in the same directory, then hard_link into place (which
/// fails with AlreadyExists instead of clobbering, even against a concurrent
/// creator such as a command run in the workspace).
fn atomic_create_bytes(
    abs: &Path,
    content: &[u8],
    client_path: &str,
) -> Result<(), InstallFailure> {
    // Only the link creates the FILE, and only the sync after it can fail with
    // the file already there -- which is what the marker's fate turns on. Note
    // that a failure before the link can still leave parent directories behind:
    // they are made unconditionally, and removing them again would race every
    // other writer that may have started using them.
    let not_yet = |e: ProtocolError| InstallFailure {
        changed: false,
        error: e,
    };
    let parent = abs.parent().ok_or_else(|| {
        not_yet(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "path has no parent",
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(|e| {
        not_yet(ProtocolError::new(
            ErrorCode::IoError,
            format!("mkdir failed: {e}"),
        ))
    })?;
    let tmp = tempfile::NamedTempFile::new_in(parent).map_err(|e| {
        not_yet(ProtocolError::new(
            ErrorCode::IoError,
            format!("temp file create failed: {e}"),
        ))
    })?;
    std::fs::write(tmp.path(), content).map_err(|e| {
        not_yet(ProtocolError::new(
            ErrorCode::IoError,
            format!("temp write failed: {e}"),
        ))
    })?;
    // Temp files are 0600; a fresh workspace file should get conventional
    // permissions instead of inheriting that.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644)).map_err(
            |e| {
                not_yet(ProtocolError::new(
                    ErrorCode::IoError,
                    format!("chmod temp failed: {e}"),
                ))
            },
        )?;
    }
    std::fs::hard_link(tmp.path(), abs).map_err(|e| {
        not_yet(if e.kind() == std::io::ErrorKind::AlreadyExists {
            ProtocolError::new(
                ErrorCode::AlreadyExists,
                format!("already exists: {client_path}; modify existing files with edit"),
            )
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("link into place failed: {e}"))
        })
    })?;
    crate::fsync::fsync_file_or_dir(abs).map_err(|e| InstallFailure {
        changed: true,
        error: ProtocolError::new(
            ErrorCode::IoError,
            format!("fsync after create failed: {e}"),
        ),
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
    let (op_id, new_hash) = write_ahead_create(store, request_id, path, &abs, content.as_bytes())?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: None,
        new_hash,
    }))
}

/// Position of a replacement in the list, for error messages. A lone
/// replacement needs no index and reads better without one.
fn edit_prefix(index: usize, total: usize) -> String {
    if total == 1 {
        String::new()
    } else {
        format!("edit {} of {}: ", index + 1, total)
    }
}

/// Apply exact text replacements to an existing UTF-8 file.
///
/// The replacements are applied in order, each to the result of the one before
/// it, and the complete new content is built and validated before anything is
/// written. One atomic rename installs it, so a failure at any position leaves
/// the file byte-for-byte unchanged and the whole list is one operation.
pub fn edit(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    base_hash: &str,
    edits: &[remote_workspace_protocol::EditSpec],
) -> Result<ResultBody, ProtocolError> {
    if edits.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "edits must not be empty",
        ));
    }
    if edits.len() > MAX_EDITS {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("edits must not exceed {MAX_EDITS} replacements"),
        ));
    }
    // Every argument is checked before the file is touched at all, so a
    // malformed replacement late in the list costs nothing.
    for (i, e) in edits.iter().enumerate() {
        let at = edit_prefix(i, edits.len());
        if e.old_text.is_empty() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("{at}old_text must not be empty"),
            ));
        }
        if e.old_text == e.new_text {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("{at}old_text and new_text are identical; nothing to change"),
            ));
        }
        if e.old_text.len() > MAX_TEXT_BYTES || e.new_text.len() > MAX_TEXT_BYTES {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("{at}old_text/new_text exceed {MAX_TEXT_BYTES} bytes"),
            ));
        }
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
    if !is_well_formed_hash(base_hash) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "base_hash {base_hash:?} is not a hash; pass the sha256:... value returned by read_file"
            ),
        ));
    }
    let original = std::fs::read(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}")))?;
    // base_hash is checked against the bytes actually read, not against a
    // separate hash of the file. Hashing it separately would leave a gap in
    // which the content could change and change back, so the replacements
    // would be built from a version the caller never pinned and the check
    // would still pass.
    let current = crate::hash::hash_bytes(&original);
    if current != base_hash {
        return Err(
            ProtocolError::new(ErrorCode::StaleFile, "file changed since base_hash")
                .with_hashes(base_hash.to_string(), current),
        );
    }
    let original_str = String::from_utf8(original).map_err(|_| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            "file is not valid UTF-8; editing binary files is unsupported",
        )
    })?;
    // Each replacement sees what the previous ones produced, so a later one
    // may legitimately match text an earlier one introduced -- and its match
    // count must be taken against that content, not the original.
    let mut new_content = original_str;
    for (i, e) in edits.iter().enumerate() {
        let at = edit_prefix(i, edits.len());
        let matches = new_content.matches(&e.old_text).count();
        if matches == 0 {
            return Err(ProtocolError::new(
                ErrorCode::NoMatch,
                format!(
                    "{at}old_text not found in {path}; re-read the file and copy the current \
                     text exactly"
                ),
            ));
        }
        if matches > 1 && !e.replace_all {
            return Err(ProtocolError::new(
                ErrorCode::AmbiguousMatch,
                format!(
                    "{at}old_text occurs {matches} times in {path}; extend it with surrounding \
                     context to make it unique, or pass replace_all=true"
                ),
            ));
        }
        // Checked BEFORE allocating, not after: replacements compound, so a
        // handful of growing ones (`a` -> `aa`, replace_all) reaches terabytes
        // long before a check on the result could reject it. The count is
        // exact, so this is the size the replacement would produce.
        let replaced = if e.replace_all { matches } else { 1 };
        let projected = new_content
            .len()
            .saturating_sub(replaced.saturating_mul(e.old_text.len()))
            .saturating_add(replaced.saturating_mul(e.new_text.len()));
        if projected > MAX_TEXT_BYTES {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("{at}resulting file would exceed {MAX_TEXT_BYTES} bytes"),
            ));
        }
        new_content = if e.replace_all {
            new_content.replace(&e.old_text, &e.new_text)
        } else {
            new_content.replacen(&e.old_text, &e.new_text, 1)
        };
    }
    // A single replacement from a value to itself is already refused as
    // nothing to change; replacements that undo each other are the same
    // request spelled across several entries, and refusing them keeps a
    // guarantee the WAL cannot otherwise express. A record whose before and
    // after hashes are equal is one recovery has to read as "the rename never
    // happened", so an edit that DID land would be dropped from history and
    // its request handed back for retry.
    if crate::hash::hash_bytes(new_content.as_bytes()) == current {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("the replacements leave {path} unchanged; nothing to do"),
        ));
    }
    // Staged before the marker is written, so a failure to even write the
    // replacement leaves nothing behind to interpret.
    let staged = stage_write(&abs, new_content.as_bytes())?;
    let (op_id, new_hash) = write_ahead_install(store, request_id, path, &abs, &current, staged)?;
    let old_hash = Some(current);
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash,
        new_hash,
    }))
}

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
    // A delete's "expected after" is the absent-file sentinel.
    let op_id = write_ahead_delete(store, request_id, path, &abs, &before_hash)?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: before_hash,
        new_hash: FILE_ABSENT_HASH.into(),
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
    let kind = if meta.file_type().is_symlink() {
        ListKind::Symlink
    } else if meta.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    };
    FileEntry {
        path: client_path.to_string(),
        kind,
        size: meta.len(),
        hash: if meta.is_file() {
            hash_file(abs).ok().flatten()
        } else {
            None
        },
        mode: Some(file_mode(meta)),
    }
}

#[cfg(unix)]
fn file_mode(meta: &std::fs::Metadata) -> remote_workspace_protocol::FileMode {
    use std::os::unix::fs::PermissionsExt;

    let mode = meta.permissions().mode();
    remote_workspace_protocol::FileMode {
        readable: mode & 0o400 != 0,
        writable: mode & 0o200 != 0,
        executable: mode & 0o111 != 0,
    }
}

#[cfg(windows)]
fn file_mode(meta: &std::fs::Metadata) -> remote_workspace_protocol::FileMode {
    remote_workspace_protocol::FileMode {
        readable: true,
        writable: !meta.permissions().readonly(),
        executable: false,
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use remote_workspace_protocol::AnyOperationRecord;
    use tempfile::tempdir;

    // The guarantee `edit` rests on: between deciding what to overwrite and the
    // rename that does it, a write can land from `exec` or any host process.
    // Only a check taken with the replacement already staged can still refuse.
    // Reachable only from inside the crate -- from outside, the base_hash check
    // rejects a changed file long before this point.
    #[test]
    fn a_target_that_changed_under_a_staged_write_is_refused() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, "as the caller last saw it\n").unwrap();
        let expected = crate::hash::hash_file(&path).unwrap().unwrap();

        let staged = stage_write(&path, b"the edit\n").unwrap();
        // Someone else got there first, after the replacement was staged.
        std::fs::write(&path, "written by something else\n").unwrap();

        let err = verify_unchanged(&path, &expected, "f.txt")
            .expect_err("must refuse to overwrite a file that changed");
        assert_eq!(err.code, ErrorCode::StaleFile);
        // Refusing means not installing; the staged file is simply dropped.
        drop(staged);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "written by something else\n",
            "the concurrent write must survive"
        );
    }

    #[test]
    fn a_target_that_is_gone_is_refused_too() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, "here\n").unwrap();
        let expected = crate::hash::hash_file(&path).unwrap().unwrap();
        std::fs::remove_file(&path).unwrap();
        let err = verify_unchanged(&path, &expected, "f.txt").expect_err("must refuse");
        assert_eq!(err.code, ErrorCode::StaleFile);
    }

    #[test]
    fn an_unchanged_target_is_accepted_and_installed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, "before\n").unwrap();
        let expected = crate::hash::hash_file(&path).unwrap().unwrap();

        let staged = stage_write(&path, b"after\n").unwrap();
        verify_unchanged(&path, &expected, "f.txt").unwrap();
        let new_hash = staged.install(&path).unwrap();
        assert_eq!(new_hash, crate::hash::hash_bytes(b"after\n"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "after\n");
    }

    // Drives the real refusal path -- `write_ahead_install`, exactly as `edit`
    // calls it -- rather than re-enacting the sequence: the point is that the
    // marker is withdrawn by THAT code, so removing the withdrawal must break
    // this test.
    #[test]
    fn a_refused_install_withdraws_its_prepared_marker() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("ws");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("f.txt");
        std::fs::write(&path, "v1\n").unwrap();
        let before = crate::hash::hash_bytes(b"v1\n");
        let store = OperationStore::new(dir.path().join("state")).unwrap();

        let staged = stage_write(&path, b"v2\n").unwrap();
        // The other writer left exactly what this install intended, which is
        // the content recovery cannot tell apart from a completed rename.
        std::fs::write(&path, "v2\n").unwrap();

        let err = write_ahead_install(&store, "req-1", "f.txt", &path, &before, staged)
            .expect_err("must refuse to install over a changed file");
        assert_eq!(err.code, ErrorCode::StaleFile);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "v2\n",
            "the other writer's content must survive"
        );

        // Reopening replays the log: the marker must come back withdrawn, so
        // recovery has nothing to synthesize.
        drop(store);
        let ws = Workspace::new(root, dir.path().join("scratch")).unwrap();
        let reopened = OperationStore::new(dir.path().join("state")).unwrap();
        let actions = reopened.recover(&ws).unwrap();
        assert!(
            actions.is_empty(),
            "a withdrawn marker must give recovery nothing to do: {actions:?}"
        );
        assert!(
            reopened.history(None).is_empty(),
            "a refused install must not appear in history"
        );
    }

    /// A store and workspace over one temp dir, reopened by `recovered_after`.
    fn store_at(dir: &std::path::Path) -> OperationStore {
        OperationStore::new(dir.join("state")).unwrap()
    }

    /// Replay the log into a fresh store and run recovery, which is the only
    /// time markers are reloaded -- and the moment a marker left behind by a
    /// failed mutation would be turned into a phantom operation.
    fn recovered_after(dir: &std::path::Path, store: OperationStore) -> Vec<AnyOperationRecord> {
        drop(store);
        let ws = Workspace::new(dir.join("ws"), dir.join("scratch")).unwrap();
        let reopened = store_at(dir);
        reopened.recover(&ws).unwrap();
        reopened.history(None)
    }

    fn ws_dir() -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("ws")).unwrap();
        dir
    }

    // Losing the create race to another process that wrote the SAME bytes is
    // the case recovery cannot tell apart from this request's own completed
    // create. Called directly because through `create` the early existence
    // check wins first; the branch that matters is the one after the marker.
    #[test]
    fn a_create_that_lost_the_race_withdraws_its_marker() {
        let dir = ws_dir();
        let abs = dir.path().join("ws/f.txt");
        std::fs::write(&abs, "same bytes\n").unwrap();
        let store = store_at(dir.path());

        let err = write_ahead_create(&store, "req-1", "f.txt", &abs, b"same bytes\n")
            .expect_err("must lose to the existing file");
        assert_eq!(err.code, ErrorCode::AlreadyExists);

        assert!(
            recovered_after(dir.path(), store).is_empty(),
            "a create that never happened must not appear in history"
        );
    }

    // A delete that failed because the file was already gone leaves recovery
    // looking at an absent file -- which for a delete marker reads as success.
    #[test]
    fn a_delete_that_failed_withdraws_its_marker() {
        let dir = ws_dir();
        let abs = dir.path().join("ws/gone.txt");
        let store = store_at(dir.path());

        let err = write_ahead_delete(
            &store,
            "req-1",
            "gone.txt",
            &abs,
            &Some(crate::hash::hash_bytes(b"whatever\n")),
        )
        .expect_err("removing a file that is not there must fail");
        assert_eq!(err.code, ErrorCode::IoError);

        assert!(
            recovered_after(dir.path(), store).is_empty(),
            "a delete that never happened must not appear in history"
        );
    }

    // The companion: a delete that DID happen is still recovered, so the
    // withdrawal above cannot be blamed for losing real work.
    #[test]
    fn a_delete_interrupted_after_the_removal_is_still_recovered() {
        let dir = ws_dir();
        let abs = dir.path().join("ws/f.txt");
        std::fs::write(&abs, "doomed\n").unwrap();
        let store = store_at(dir.path());
        let op_id = store
            .prepare_fs_record(
                "req-1",
                OperationKind::Delete,
                "f.txt",
                Some(crate::hash::hash_bytes(b"doomed\n")),
                FILE_ABSENT_HASH.into(),
            )
            .unwrap();
        // The removal landed; the commit did not.
        std::fs::remove_file(&abs).unwrap();

        let history = recovered_after(dir.path(), store);
        assert!(
            history.iter().any(|r| r.operation_id() == op_id),
            "a crash between removal and commit must still be recovered"
        );
    }

    // A write must not silently strip an executable bit.
    #[test]
    #[cfg(unix)]
    fn installing_over_a_file_preserves_its_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.sh");
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        stage_write(&path, b"#!/bin/sh\necho hi\n")
            .unwrap()
            .install(&path)
            .unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755);
    }
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use tempfile::tempdir;

    fn ws_with(path: &str, content: &str) -> (tempfile::TempDir, Workspace) {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(path), content).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
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
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
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
