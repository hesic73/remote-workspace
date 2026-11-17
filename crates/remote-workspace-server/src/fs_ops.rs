use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use remote_workspace_protocol::{
    ErrorCode, FileEntry, FsOperationRecord, ListEntry, ListKind, MutationResult, OperationKind,
    ProtocolError, ReadOptions, ReadResult, ResultBody,
};
use tokio::sync::OwnedMutexGuard;

use crate::hash::hash_file;
use crate::store::OperationStore;
use crate::workspace::Workspace;

pub const LIST_DEFAULT_LIMIT: usize = 1000;
pub const LIST_MAX_LIMIT: usize = 1000;
/// Upper bound on create/edit text inputs, on the file being edited, and on
/// the resulting file. Larger or binary files go through upload_file.
pub const MAX_TEXT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_EDITS: usize = 100;
pub const READ_DEFAULT_LIMIT: u64 = 65536;
pub const READ_MAX_LIMIT: u64 = 64 * 1024;
pub const READ_MAX_LINES: u64 = 2000;
#[cfg(unix)]
pub(crate) const NEW_FILE_MODE: u32 = 0o644;

const UTF8_TAIL_SLACK: u64 = 3;
const FILE_ABSENT_HASH: &str = "sha256:";
const MUTATE_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

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
    let list_failed =
        |e: std::io::Error| ProtocolError::new(ErrorCode::IoError, format!("list failed: {e}"));
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&abs).map_err(list_failed)? {
        let entry = entry.map_err(list_failed)?;
        let name = entry.file_name().to_string_lossy().to_string();
        if abs == ws.root && name == ".remote-workspace" {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(list_failed(e)),
        };
        entries.push(ListEntry {
            name,
            kind: list_kind(meta.file_type(), false),
            size: Some(meta.len()),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    if offset > 0 && offset >= entries.len() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "offset {offset} is past the last entry ({} entries)",
                entries.len()
            ),
        ));
    }
    let end = offset.saturating_add(limit).min(entries.len());
    let page = entries[offset..end].to_vec();
    Ok(ResultBody::List(remote_workspace_protocol::ListResult {
        entries: page,
        next_offset: (end < entries.len()).then_some(end),
    }))
}

/// Follows a symlink, as every other operation does, so size and hash are its
/// target's; `kind` still says it is one, as `list` does.
pub fn stat(ws: &Workspace, path: &str) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let meta = std::fs::metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("stat failed: {e}"))
        }
    })?;
    let entry = entry_for(path, &abs, &meta, ws.is_symlink(path)?)?;
    Ok(ResultBody::Stat { stat: entry })
}

pub fn read(ws: &Workspace, path: &str, opts: ReadOptions) -> Result<ResultBody, ProtocolError> {
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
    if !meta.is_file() {
        return Err(not_a_regular_file(path));
    }

    let has_byte = opts.offset.is_some() || opts.limit.is_some();
    let has_line = opts.start_line.is_some() || opts.max_lines.is_some();
    let has_tail_lines = opts.tail_lines.is_some();
    let has_tail_bytes = opts.tail_bytes.is_some();
    if has_tail_lines && has_tail_bytes {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "tail_lines and tail_bytes are mutually exclusive",
        ));
    }
    let mode_count = usize::from(has_byte)
        + usize::from(has_line)
        + usize::from(has_tail_lines || has_tail_bytes);
    if mode_count > 1 {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "read modes are mutually exclusive: choose byte (offset/limit), line (start_line/max_lines), or tail (tail_lines or tail_bytes)",
        ));
    }

    let result = if has_line {
        read_line_mode(&abs, meta.len(), opts.start_line, opts.max_lines)?
    } else if let Some(tb) = opts.tail_bytes {
        read_tail_bytes_mode(&abs, meta.len(), tb)?
    } else if let Some(tl) = opts.tail_lines {
        read_tail_lines_mode(&abs, meta.len(), tl)?
    } else {
        read_byte_mode(&abs, meta.len(), opts.offset, opts.limit)?
    };
    Ok(ResultBody::Read(result))
}

fn check_offset(start: u64, file_len: u64) -> Result<(), ProtocolError> {
    if start > file_len {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("offset {start} is past end of file ({file_len} bytes)"),
        ));
    }
    Ok(())
}

fn read_byte_mode(
    abs: &Path,
    meta_len: u64,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<ReadResult, ProtocolError> {
    let limit = limit.unwrap_or(READ_DEFAULT_LIMIT);
    if limit == 0 || limit > READ_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("read limit must be between 1 and {READ_MAX_LIMIT} bytes"),
        ));
    }
    let start_offset = offset.unwrap_or(0);

    let mut file = std::fs::File::open(abs).map_err(io_read_error)?;
    let (total_bytes, mut window, hash) = if meta_len <= MAX_TEXT_BYTES as u64 {
        let mut whole = Vec::new();
        file.read_to_end(&mut whole).map_err(io_read_error)?;
        let total_bytes = whole.len() as u64;
        check_offset(start_offset, total_bytes)?;
        let end = whole.len().min(
            start_offset
                .saturating_add(limit)
                .saturating_add(UTF8_TAIL_SLACK) as usize,
        );
        let window = whole[start_offset as usize..end].to_vec();
        (total_bytes, window, Some(crate::hash::hash_bytes(&whole)))
    } else {
        let total_bytes = meta_len;
        check_offset(start_offset, total_bytes)?;
        file.seek(SeekFrom::Start(start_offset))
            .map_err(io_read_error)?;
        let mut window = Vec::new();
        file.take(limit + UTF8_TAIL_SLACK)
            .read_to_end(&mut window)
            .map_err(io_read_error)?;
        (total_bytes, window, None)
    };

    if window.first().is_some_and(|b| is_continuation(*b)) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("offset {start_offset} is not on a UTF-8 character boundary"),
        ));
    }
    let mut end = window.len().min(limit as usize);
    while end < window.len() && is_continuation(window[end]) {
        end += 1;
    }
    window.truncate(end);
    let content = String::from_utf8(window).map_err(|_| non_utf8_read_error())?;
    let end_offset = start_offset + content.len() as u64;
    let truncated = end_offset < total_bytes;
    let trailing_newline = content.ends_with('\n');
    Ok(ReadResult {
        content,
        total_bytes,
        start_offset,
        end_offset,
        trailing_newline,
        start_line: None,
        end_line: None,
        total_lines: None,
        next_line: None,
        hash,
        truncated,
        next_offset: truncated.then_some(end_offset),
    })
}

fn read_line_mode(
    abs: &Path,
    meta_len: u64,
    start_line: Option<u64>,
    max_lines: Option<u64>,
) -> Result<ReadResult, ProtocolError> {
    let start_line = start_line.unwrap_or(1);
    if start_line == 0 {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "start_line must be >= 1",
        ));
    }
    let raw_max_lines = max_lines.unwrap_or(READ_MAX_LINES);
    if raw_max_lines == 0 {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "max_lines must be >= 1",
        ));
    }
    let max_lines = raw_max_lines.min(READ_MAX_LINES);

    let too_large = || {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "file exceeds {MAX_TEXT_BYTES} bytes; use byte offset or tail_lines/tail_bytes for files > 4 MiB"
            ),
        )
    };
    if meta_len > MAX_TEXT_BYTES as u64 {
        return Err(too_large());
    }
    let whole = std::fs::read(abs).map_err(io_read_error)?;
    if whole.len() > MAX_TEXT_BYTES {
        return Err(too_large());
    }
    let total_bytes = whole.len() as u64;
    let hash = Some(crate::hash::hash_bytes(&whole));
    let text = String::from_utf8(whole).map_err(|_| non_utf8_read_error())?;

    if text.is_empty() {
        if start_line > 1 {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("start_line {start_line} is past end of file (0 lines)"),
            ));
        }
        return Ok(ReadResult {
            content: String::new(),
            total_bytes: 0,
            start_offset: 0,
            end_offset: 0,
            trailing_newline: false,
            start_line: None,
            end_line: None,
            total_lines: Some(0),
            next_line: None,
            hash,
            truncated: false,
            next_offset: None,
        });
    }

    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let total_lines = lines.len() as u64;
    if start_line > total_lines {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("start_line {start_line} is past end of file ({total_lines} lines)"),
        ));
    }

    let start_idx = (start_line - 1) as usize;
    let start_offset: u64 = lines[..start_idx].iter().map(|l| l.len() as u64).sum();
    let first = lines[start_idx];
    if first.len() > READ_MAX_LIMIT as usize {
        let mut cut = READ_MAX_LIMIT as usize;
        while !first.is_char_boundary(cut) {
            cut -= 1;
        }
        let content = first[..cut].to_string();
        let end_offset = start_offset + content.len() as u64;
        let trailing_newline = content.ends_with('\n');
        return Ok(ReadResult {
            content,
            total_bytes,
            start_offset,
            end_offset,
            trailing_newline,
            start_line: Some(start_line),
            end_line: Some(start_line),
            total_lines: Some(total_lines),
            next_line: None,
            hash,
            truncated: true,
            next_offset: Some(end_offset),
        });
    }

    let end_bound = start_idx
        .saturating_add(max_lines as usize)
        .min(lines.len());
    let mut byte_len = 0usize;
    let mut count = 0u64;
    for line in &lines[start_idx..end_bound] {
        if byte_len + line.len() > READ_MAX_LIMIT as usize {
            break;
        }
        byte_len += line.len();
        count += 1;
    }
    let content = text[start_offset as usize..start_offset as usize + byte_len].to_string();
    let end_offset = start_offset + byte_len as u64;
    let end_line = start_line + count - 1;
    let truncated = end_line < total_lines;
    let trailing_newline = content.ends_with('\n');
    Ok(ReadResult {
        content,
        total_bytes,
        start_offset,
        end_offset,
        trailing_newline,
        start_line: Some(start_line),
        end_line: Some(end_line),
        total_lines: Some(total_lines),
        next_line: truncated.then_some(end_line + 1),
        hash,
        truncated,
        next_offset: truncated.then_some(end_offset),
    })
}

fn read_tail_bytes_mode(
    abs: &Path,
    meta_len: u64,
    tail_bytes: u64,
) -> Result<ReadResult, ProtocolError> {
    if tail_bytes == 0 || tail_bytes > READ_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("tail_bytes must be between 1 and {READ_MAX_LIMIT} bytes"),
        ));
    }

    let (total_bytes, raw_start, buf, hash) = if meta_len <= MAX_TEXT_BYTES as u64 {
        let whole = std::fs::read(abs).map_err(io_read_error)?;
        let total_bytes = whole.len() as u64;
        let hash = Some(crate::hash::hash_bytes(&whole));
        let raw_start = total_bytes.saturating_sub(tail_bytes);
        (
            total_bytes,
            raw_start,
            whole[raw_start as usize..].to_vec(),
            hash,
        )
    } else {
        let mut file = std::fs::File::open(abs).map_err(io_read_error)?;
        let raw_start = meta_len.saturating_sub(tail_bytes);
        file.seek(SeekFrom::Start(raw_start))
            .map_err(io_read_error)?;
        let mut buf = Vec::new();
        file.take(tail_bytes)
            .read_to_end(&mut buf)
            .map_err(io_read_error)?;
        let total_bytes = raw_start + buf.len() as u64;
        (total_bytes, raw_start, buf, None)
    };

    let skip = if raw_start > 0 {
        leading_continuation_count(&buf)
    } else {
        0
    };
    let content = std::str::from_utf8(&buf[skip..])
        .map_err(|_| non_utf8_read_error())?
        .to_string();
    let start_offset = raw_start + skip as u64;
    let end_offset = start_offset + content.len() as u64;
    let truncated = start_offset > 0;
    let trailing_newline = content.ends_with('\n');
    Ok(ReadResult {
        content,
        total_bytes,
        start_offset,
        end_offset,
        trailing_newline,
        start_line: None,
        end_line: None,
        total_lines: None,
        next_line: None,
        hash,
        truncated,
        next_offset: None,
    })
}

fn read_tail_lines_mode(
    abs: &Path,
    meta_len: u64,
    tail_lines: u64,
) -> Result<ReadResult, ProtocolError> {
    if tail_lines == 0 || tail_lines > READ_MAX_LINES {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("tail_lines must be between 1 and {READ_MAX_LINES} lines"),
        ));
    }

    if meta_len <= MAX_TEXT_BYTES as u64 {
        let whole = std::fs::read(abs).map_err(io_read_error)?;
        if whole.len() <= MAX_TEXT_BYTES {
            let total_bytes = whole.len() as u64;
            let hash = Some(crate::hash::hash_bytes(&whole));
            let text = String::from_utf8(whole).map_err(|_| non_utf8_read_error())?;
            if text.is_empty() {
                return Ok(ReadResult {
                    content: String::new(),
                    total_bytes: 0,
                    start_offset: 0,
                    end_offset: 0,
                    trailing_newline: false,
                    start_line: None,
                    end_line: None,
                    total_lines: Some(0),
                    next_line: None,
                    hash,
                    truncated: false,
                    next_offset: None,
                });
            }
            let lines: Vec<&str> = text.split_inclusive('\n').collect();
            let total_lines = lines.len() as u64;
            let (content, first_idx) = slice_tail_lines(&text, &lines, tail_lines as usize);
            let start_offset = total_bytes - content.len() as u64;
            let trailing_newline = content.ends_with('\n');
            return Ok(ReadResult {
                content,
                total_bytes,
                start_offset,
                end_offset: total_bytes,
                trailing_newline,
                start_line: Some(first_idx as u64 + 1),
                end_line: Some(total_lines),
                total_lines: Some(total_lines),
                next_line: None,
                hash,
                truncated: start_offset > 0,
                next_offset: None,
            });
        }
    }

    // Read 64 KiB + 4 bytes so after skipping at most 3 UTF-8 continuation
    // bytes, `window_str.len() > READ_MAX_LIMIT`: `lines[0]` (any partial
    // leading line) can never fit when >= 2 lines exist in the window.
    let window_cap = READ_MAX_LIMIT + UTF8_TAIL_SLACK + 1;
    let mut file = std::fs::File::open(abs).map_err(io_read_error)?;
    let raw_start = meta_len.saturating_sub(window_cap);
    file.seek(SeekFrom::Start(raw_start))
        .map_err(io_read_error)?;
    let mut buf = Vec::new();
    file.take(window_cap)
        .read_to_end(&mut buf)
        .map_err(io_read_error)?;
    let total_bytes = raw_start + buf.len() as u64;
    let skip = if raw_start > 0 {
        leading_continuation_count(&buf)
    } else {
        0
    };
    let window_str = std::str::from_utf8(&buf[skip..]).map_err(|_| non_utf8_read_error())?;
    if window_str.is_empty() {
        return Ok(ReadResult {
            content: String::new(),
            total_bytes,
            start_offset: total_bytes,
            end_offset: total_bytes,
            trailing_newline: false,
            start_line: None,
            end_line: None,
            total_lines: None,
            next_line: None,
            hash: None,
            truncated: total_bytes > 0,
            next_offset: None,
        });
    }
    let lines: Vec<&str> = window_str.split_inclusive('\n').collect();
    let (content, _) = slice_tail_lines(window_str, &lines, tail_lines as usize);
    let start_offset = total_bytes - content.len() as u64;
    let trailing_newline = content.ends_with('\n');
    Ok(ReadResult {
        content,
        total_bytes,
        start_offset,
        end_offset: total_bytes,
        trailing_newline,
        start_line: None,
        end_line: None,
        total_lines: None,
        next_line: None,
        hash: None,
        truncated: start_offset > 0,
        next_offset: None,
    })
}

fn slice_tail_lines(text: &str, lines: &[&str], tail_lines: usize) -> (String, usize) {
    let last_idx = lines.len() - 1;
    if lines[last_idx].len() > READ_MAX_LIMIT as usize {
        let raw = &text.as_bytes()[text.len() - READ_MAX_LIMIT as usize..];
        let skip = leading_continuation_count(raw);
        let s = std::str::from_utf8(&raw[skip..]).expect("slice of valid UTF-8 after char snap");
        return (s.to_string(), last_idx);
    }
    let min_idx = lines.len().saturating_sub(tail_lines);
    let mut byte_len = 0usize;
    let mut first_idx = last_idx;
    for i in (min_idx..=last_idx).rev() {
        if byte_len + lines[i].len() > READ_MAX_LIMIT as usize {
            break;
        }
        byte_len += lines[i].len();
        first_idx = i;
    }
    (text[text.len() - byte_len..].to_string(), first_idx)
}

fn leading_continuation_count(buf: &[u8]) -> usize {
    let mut skip = 0usize;
    while skip < 3 && skip < buf.len() && is_continuation(buf[skip]) {
        skip += 1;
    }
    skip
}

fn is_continuation(byte: u8) -> bool {
    byte & 0xC0 == 0x80
}

fn non_utf8_read_error() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::InvalidRequest,
        "file is not valid UTF-8; binary reads are not supported",
    )
}

fn io_read_error(e: std::io::Error) -> ProtocolError {
    ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}"))
}

fn is_well_formed_hash(hash: &str) -> bool {
    match hash.strip_prefix("sha256:") {
        Some(hex) => hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
        None => false,
    }
}

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

struct StagedWrite {
    tmp: tempfile::NamedTempFile,
    new_hash: String,
}

fn write_temp(dir: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(crate::transfer::WRITE_STAGING_PREFIX)
        .suffix(crate::transfer::STAGING_SUFFIX)
        .tempfile_in(dir)
}

/// Sync file data (`fdatasync`) before atomic rename so a power loss never
/// leaves a zero-filled or torn target file.
fn write_synced(tmp: &tempfile::NamedTempFile, content: &[u8]) -> std::io::Result<()> {
    let mut file = tmp.as_file();
    file.write_all(content)?;
    file.sync_data()
}

fn stage_write(abs: &Path, content: &[u8]) -> Result<StagedWrite, ProtocolError> {
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mkdir failed: {e}")))?;
    let tmp = write_temp(parent).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("temp file create failed: {e}"))
    })?;
    write_synced(&tmp, content)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(abs)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("stat failed: {e}")))?
            .permissions()
            .mode();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).map_err(
            |e| ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}")),
        )?;
    }
    Ok(StagedWrite {
        tmp,
        new_hash: crate::hash::hash_bytes(content),
    })
}

impl StagedWrite {
    fn install(self, abs: &Path) -> Result<String, ProtocolError> {
        self.tmp.persist(abs).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("atomic persist failed: {e}"))
        })?;
        Ok(self.new_hash)
    }
}

/// Serialize the `verify_unchanged` + `persist` window across concurrent server
/// processes sharing `state_dir` so conflicting edits deterministically fail
/// with `STALE_FILE` instead of racing a clobber.
fn with_mutation_lock<T>(
    state_dir: &Path,
    f: impl FnOnce() -> Result<T, ProtocolError>,
) -> Result<T, ProtocolError> {
    let lock_path = state_dir.join("mutate.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("open mutate lock: {e}")))?;
    let deadline = Instant::now() + MUTATE_LOCK_TIMEOUT;
    loop {
        if crate::locking::try_lock_exclusive(&lock_file, 0)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mutate lock: {e}")))?
        {
            let result = f();
            crate::locking::unlock(&lock_file, 0);
            return result;
        }
        if Instant::now() >= deadline {
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                "timed out acquiring workspace mutation lock",
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn atomic_create_bytes(
    abs: &Path,
    content: &[u8],
    client_path: &str,
) -> Result<String, ProtocolError> {
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mkdir failed: {e}")))?;
    let tmp = write_temp(parent).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("temp file create failed: {e}"))
    })?;
    write_synced(&tmp, content)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(NEW_FILE_MODE))
            .map_err(|e| {
                ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}"))
            })?;
    }
    tmp.persist_noclobber(abs).map_err(|e| {
        let e = e.error;
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            ProtocolError::new(
                ErrorCode::AlreadyExists,
                format!("already exists: {client_path}; modify existing files with edit"),
            )
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("install failed: {e}"))
        }
    })?;
    Ok(crate::hash::hash_bytes(content))
}

pub fn create(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &OwnedMutexGuard<()>,
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
    let new_hash = atomic_create_bytes(&abs, content.as_bytes(), path)?;
    let op_id = store.next_operation_id();
    store.append_fs_record(FsOperationRecord {
        operation_id: op_id.clone(),
        request_id: request_id.to_string(),
        kind: OperationKind::Create,
        path: path.to_string(),
        before_hash: None,
        after_hash: new_hash.clone(),
        timestamp_ms: now_ms(),
    })?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: None,
        new_hash,
    }))
}

fn edit_prefix(index: usize, total: usize) -> String {
    if total == 1 {
        String::new()
    } else {
        format!("edit {} of {}: ", index + 1, total)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn edit(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &OwnedMutexGuard<()>,
    request_id: &str,
    path: &str,
    base_hash: &str,
    edits: &[remote_workspace_protocol::EditSpec],
    replace_content: Option<&str>,
) -> Result<ResultBody, ProtocolError> {
    if !edits.is_empty() && replace_content.is_some() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "edits and replace_content are mutually exclusive",
        ));
    }
    if edits.is_empty() && replace_content.is_none() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "edits must not be empty (or pass replace_content)",
        ));
    }

    if let Some(replacement) = replace_content {
        if replacement.len() > MAX_TEXT_BYTES {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!(
                    "replace_content exceeds {MAX_TEXT_BYTES} bytes; use upload_file for large files"
                ),
            ));
        }
    } else {
        if edits.len() > MAX_EDITS {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("edits must not exceed {MAX_EDITS} replacements"),
            ));
        }
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
    }

    let abs = ws.resolve(path)?;
    match std::fs::metadata(&abs) {
        Ok(meta) if !meta.is_file() => return Err(not_a_regular_file(path)),
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

    let staged = if let Some(replacement) = replace_content {
        if crate::hash::hash_bytes(replacement.as_bytes()) == current {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("replace_content leaves {path} unchanged; nothing to do"),
            ));
        }
        stage_write(&abs, replacement.as_bytes())?
    } else {
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
        if crate::hash::hash_bytes(new_content.as_bytes()) == current {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("the replacements leave {path} unchanged; nothing to do"),
            ));
        }
        stage_write(&abs, new_content.as_bytes())?
    };

    let new_hash = with_mutation_lock(store.state_dir(), || {
        verify_unchanged(&abs, &current, path)?;
        staged.install(&abs)
    })?;
    let op_id = store.next_operation_id();
    store.append_fs_record(FsOperationRecord {
        operation_id: op_id.clone(),
        request_id: request_id.to_string(),
        kind: OperationKind::Edit,
        path: path.to_string(),
        before_hash: Some(current.clone()),
        after_hash: new_hash.clone(),
        timestamp_ms: now_ms(),
    })?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: Some(current),
        new_hash,
    }))
}

pub fn delete(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &OwnedMutexGuard<()>,
    request_id: &str,
    path: &str,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    if ws.is_symlink(path)? {
        return Err(ProtocolError::new(
            ErrorCode::NotAFile,
            format!("{path} is a symlink; remove the link itself with run_command"),
        ));
    }
    match std::fs::metadata(&abs) {
        Ok(meta) if meta.is_dir() => {
            return Err(ProtocolError::new(
                ErrorCode::IsADirectory,
                format!("not a file: {path}"),
            ))
        }
        Ok(meta) if !meta.is_file() => return Err(not_a_regular_file(path)),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ProtocolError::new(
                ErrorCode::NotFound,
                format!("not found: {path}"),
            ))
        }
        Err(e) => return Err(e.into()),
    }
    let before_hash = hash_file(&abs)?;
    std::fs::remove_file(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("remove failed: {e}")))?;
    let op_id = store.next_operation_id();
    store.append_fs_record(FsOperationRecord {
        operation_id: op_id.clone(),
        request_id: request_id.to_string(),
        kind: OperationKind::Delete,
        path: path.to_string(),
        before_hash: before_hash.clone(),
        after_hash: FILE_ABSENT_HASH.into(),
        timestamp_ms: now_ms(),
    })?;
    Ok(ResultBody::Mutation(MutationResult {
        operation_id: op_id,
        old_hash: before_hash,
        new_hash: FILE_ABSENT_HASH.into(),
    }))
}

/// For FIFOs, sockets and devices: opening one to read can block forever.
fn not_a_regular_file(path: &str) -> ProtocolError {
    ProtocolError::new(ErrorCode::NotAFile, format!("not a regular file: {path}"))
}

fn list_kind(file_type: std::fs::FileType, symlink: bool) -> ListKind {
    if symlink || file_type.is_symlink() {
        ListKind::Symlink
    } else if file_type.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    }
}

fn entry_for(
    client_path: &str,
    abs: &Path,
    meta: &std::fs::Metadata,
    symlink: bool,
) -> Result<FileEntry, ProtocolError> {
    let hash = if meta.is_file() && meta.len() <= MAX_TEXT_BYTES as u64 {
        match hash_file(abs) {
            Ok(hash) => hash,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(e) => {
                return Err(ProtocolError::new(
                    ErrorCode::IoError,
                    format!("stat failed: {e}"),
                ))
            }
        }
    } else {
        None
    };
    Ok(FileEntry {
        path: client_path.to_string(),
        kind: list_kind(meta.file_type(), symlink),
        size: meta.len(),
        hash,
        mode: Some(file_mode(meta)),
    })
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .expect("system clock is set before 1970")
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn a_target_that_changed_under_a_staged_write_is_refused() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, "as the caller last saw it\n").unwrap();
        let expected = crate::hash::hash_file(&path).unwrap().unwrap();

        let staged = stage_write(&path, b"the edit\n").unwrap();
        std::fs::write(&path, "written by something else\n").unwrap();

        let err = verify_unchanged(&path, &expected, "f.txt")
            .expect_err("must refuse to overwrite a file that changed");
        assert_eq!(err.code, ErrorCode::StaleFile);
        drop(staged);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "written by something else\n"
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

    fn byte_opts(offset: Option<u64>, limit: Option<u64>) -> ReadOptions {
        ReadOptions {
            offset,
            limit,
            ..Default::default()
        }
    }

    fn unpack_read(body: ResultBody) -> ReadResult {
        match body {
            ResultBody::Read(r) => r,
            other => panic!("expected ReadResult, got {other:?}"),
        }
    }

    #[test]
    fn read_multibyte_offset_on_boundary_works() {
        let (_d, w) = ws_with("f.txt", "éx");
        let r = unpack_read(read(&w, "f.txt", byte_opts(Some(2), Some(1))).unwrap());
        assert_eq!(r.content, "x");
        assert_eq!(r.start_offset, 2);
        assert_eq!(r.end_offset, 3);
        assert_eq!(r.total_bytes, 3);
        assert!(!r.trailing_newline);
    }

    #[test]
    fn read_multibyte_offset_off_boundary_rejected_not_panic() {
        let (_d, w) = ws_with("f.txt", "éx");
        let res = read(&w, "f.txt", byte_opts(Some(1), Some(1)));
        assert_eq!(res.unwrap_err().code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn read_limit_mid_codepoint_snaps_up_to_boundary() {
        let (_d, w) = ws_with("f.txt", "aébc");
        let r = unpack_read(read(&w, "f.txt", byte_opts(Some(0), Some(2))).unwrap());
        assert_eq!(r.content, "aé");
        assert!(r.truncated);
        assert_eq!(r.start_offset, 0);
        assert_eq!(r.end_offset, 3);
        assert_eq!(r.next_offset, Some(3));
    }

    #[test]
    fn read_huge_offset_rejected_without_overflow() {
        let (_d, w) = ws_with("f.txt", "hi");
        let res = read(&w, "f.txt", byte_opts(Some(u64::MAX), Some(u64::MAX)));
        assert_eq!(res.unwrap_err().code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn read_pages_large_file_without_hashing_it() {
        let dir = tempdir().unwrap();
        let big = vec![b'x'; MAX_TEXT_BYTES + 1024];
        std::fs::write(dir.path().join("big.log"), &big).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let offset = MAX_TEXT_BYTES as u64;
        let r = unpack_read(read(&w, "big.log", byte_opts(Some(offset), Some(64))).unwrap());
        assert_eq!(r.content.len(), 64);
        assert_eq!(r.hash, None);
        assert!(r.truncated);
        assert_eq!(r.next_offset, Some(offset + 64));
    }

    #[test]
    fn read_at_end_of_file_returns_empty_final_page() {
        let (_d, w) = ws_with("f.txt", "hi");
        let r = unpack_read(read(&w, "f.txt", byte_opts(Some(2), Some(8))).unwrap());
        assert_eq!(r.content, "");
        assert!(!r.truncated);
        assert_eq!(r.next_offset, None);
    }

    #[test]
    fn read_rejects_limit_above_hard_maximum() {
        let (_d, w) = ws_with("f.txt", "hi");
        let result = read(&w, "f.txt", byte_opts(None, Some(READ_MAX_LIMIT + 1)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn read_line_mode_preserves_endings_and_paginates() {
        let (_d, w) = ws_with("f.txt", "l1\r\nl2\nl3");
        let r1 = unpack_read(
            read(
                &w,
                "f.txt",
                ReadOptions {
                    start_line: Some(1),
                    max_lines: Some(2),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(r1.content, "l1\r\nl2\n");
        assert_eq!(r1.start_line, Some(1));
        assert_eq!(r1.end_line, Some(2));
        assert_eq!(r1.total_lines, Some(3));
        assert_eq!(r1.next_line, Some(3));
        assert!(r1.truncated);
        assert!(r1.trailing_newline);
        assert_eq!(r1.start_offset, 0);
        assert_eq!(r1.end_offset, 7);
        assert_eq!(r1.next_offset, Some(7));

        let r2 = unpack_read(
            read(
                &w,
                "f.txt",
                ReadOptions {
                    start_line: Some(3),
                    max_lines: Some(2),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(r2.content, "l3");
        assert_eq!(r2.start_line, Some(3));
        assert_eq!(r2.end_line, Some(3));
        assert_eq!(r2.total_lines, Some(3));
        assert_eq!(r2.next_line, None);
        assert!(!r2.truncated);
        assert!(!r2.trailing_newline);
        assert_eq!(r2.start_offset, 7);
        assert_eq!(r2.end_offset, 9);

        let (_de, we) = ws_with("empty.txt", "");
        let re = unpack_read(
            read(
                &we,
                "empty.txt",
                ReadOptions {
                    start_line: Some(1),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(re.content, "");
        assert_eq!(re.total_lines, Some(0));
        assert_eq!(re.start_line, None);
        assert_eq!(re.end_line, None);
        assert!(!re.truncated);
    }

    #[test]
    fn read_line_mode_caps_bytes_at_64kib_and_rejects_above_4mib() {
        let mut many_lines = String::new();
        for i in 0..1000 {
            many_lines.push_str(&format!("line-{i:04}-{}\n", "x".repeat(100)));
        }
        let (_d, w) = ws_with("many.txt", &many_lines);
        let r = unpack_read(
            read(
                &w,
                "many.txt",
                ReadOptions {
                    start_line: Some(1),
                    max_lines: Some(2000),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert!(r.content.len() <= READ_MAX_LIMIT as usize);
        assert!(r.truncated);
        assert!(r.trailing_newline);
        assert_eq!(r.next_line, Some(r.end_line.unwrap() + 1));

        // Single huge line > 64 KiB is sliced on a UTF-8 char boundary.
        let single_huge = format!("{}\nsecond\n", "é".repeat(40_000));
        let (_d2, w2) = ws_with("single.txt", &single_huge);
        let rs = unpack_read(
            read(
                &w2,
                "single.txt",
                ReadOptions {
                    start_line: Some(1),
                    max_lines: Some(10),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert!(rs.content.len() <= READ_MAX_LIMIT as usize);
        assert!(rs.truncated);
        assert_eq!(rs.start_line, Some(1));
        assert_eq!(rs.end_line, Some(1));
        assert_eq!(rs.next_line, None);
        assert_eq!(rs.next_offset, Some(rs.end_offset));

        // > 4 MiB rejected in line mode.
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("big.log"), vec![b'a'; MAX_TEXT_BYTES + 1]).unwrap();
        let wb = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let err = read(
            &wb,
            "big.log",
            ReadOptions {
                start_line: Some(1),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn read_tail_modes_snap_utf8_and_work_above_4mib() {
        // '🦀' is 4 bytes; tail_bytes=3 lands on byte 1 of '🦀' and snaps to "ok\n".
        let (_d, w) = ws_with("crab.txt", "hi🦀ok\n");
        let rt = unpack_read(
            read(
                &w,
                "crab.txt",
                ReadOptions {
                    tail_bytes: Some(5),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(rt.content, "ok\n");
        assert!(rt.truncated);
        assert_eq!(rt.next_offset, None);
        assert!(rt.hash.is_some());

        // > 4 MiB tail_lines seeks near EOF without hashing.
        let dir = tempdir().unwrap();
        let mut big = vec![b'x'; MAX_TEXT_BYTES + 5000];
        big.extend_from_slice(b"\npenultimate\nfinal-line\n");
        std::fs::write(dir.path().join("big.log"), &big).unwrap();
        let wb = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let rl = unpack_read(
            read(
                &wb,
                "big.log",
                ReadOptions {
                    tail_lines: Some(2),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(rl.content, "penultimate\nfinal-line\n");
        assert_eq!(rl.hash, None);
        assert!(rl.truncated);
        assert_eq!(rl.end_offset, big.len() as u64);
        assert_eq!(rl.next_offset, None);
    }

    #[test]
    fn read_modes_are_mutually_exclusive() {
        let (_d, w) = ws_with("f.txt", "a\nb\n");
        for bad in [
            ReadOptions {
                offset: Some(0),
                start_line: Some(1),
                ..Default::default()
            },
            ReadOptions {
                limit: Some(10),
                tail_lines: Some(1),
                ..Default::default()
            },
            ReadOptions {
                start_line: Some(1),
                tail_bytes: Some(10),
                ..Default::default()
            },
            ReadOptions {
                tail_lines: Some(1),
                tail_bytes: Some(10),
                ..Default::default()
            },
        ] {
            assert_eq!(
                read(&w, "f.txt", bad).unwrap_err().code,
                ErrorCode::InvalidRequest
            );
        }
    }
}
