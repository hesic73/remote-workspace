use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use remote_workspace_protocol::{
    ErrorCode, ProtocolError, ResultBody, TransferDirection, TransferOperationRecord,
    TransferResult, UploadPrepareResult,
};
use sha2::{Digest, Sha256};
use tokio::sync::MutexGuard;

use crate::store::OperationStore;
use crate::workspace::Workspace;

pub const TRANSFER_BUF_SIZE: usize = 64 * 1024;

/// Staging filename convention: `.remote-workspace-upload.<name>.<random>.part`.
/// Deliberately unmistakable so stale-staging cleanup can match exactly this
/// pattern and never touch anyone else's `.part` files.
pub const STAGING_PREFIX: &str = ".remote-workspace-upload.";
pub const STAGING_SUFFIX: &str = ".part";
/// A staging file whose mtime is older than this is considered abandoned (an
/// in-progress upload keeps its mtime fresh with every written chunk).
pub const STALE_STAGING_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// A staged upload awaiting commit. Lives only in memory: the staging file and
/// this entry die with the server process, and a client whose connection
/// dropped mid-transfer simply re-uploads.
#[derive(Clone)]
pub struct PendingUpload {
    pub staging: PathBuf,
    pub target: PathBuf,
    pub logical_path: String,
    pub overwrite: bool,
}

pub type UploadRegistry = parking_lot::Mutex<HashMap<String, PendingUpload>>;

fn new_transfer_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("xfer-{ts:016x}-{n:04x}")
}

pub fn upload_prepare(
    ws: &Workspace,
    registry: &UploadRegistry,
    path: &str,
    overwrite: bool,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    match std::fs::metadata(parent) {
        Ok(m) if m.is_dir() => {}
        Ok(_) => {
            return Err(ProtocolError::new(
                ErrorCode::NotADirectory,
                format!("parent of {path} is not a directory"),
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ProtocolError::new(
                ErrorCode::NotFound,
                format!("parent directory of {path} does not exist; create it first"),
            ))
        }
        Err(e) => return Err(e.into()),
    }
    match std::fs::symlink_metadata(&abs) {
        Ok(m) if m.is_dir() => {
            return Err(ProtocolError::new(
                ErrorCode::IsADirectory,
                format!("is a directory: {path}"),
            ))
        }
        Ok(m) if !m.is_file() => {
            return Err(ProtocolError::new(
                ErrorCode::NotAFile,
                format!("target exists and is not a regular file: {path}"),
            ))
        }
        Ok(_) if !overwrite => {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("target already exists: {path}; pass overwrite=true to replace it"),
            ))
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let file_name = abs
        .file_name()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no file name"))?
        .to_string_lossy()
        .into_owned();
    // A retried upload lands in the same directory as any staging file a
    // crashed predecessor left behind, so this is the natural place to sweep.
    let removed =
        sweep_stale_staging_dir(parent, &in_flight_staging(registry), STALE_STAGING_MAX_AGE);
    if removed > 0 {
        tracing::info!(
            dir = %parent.display(),
            removed,
            "removed stale upload staging files"
        );
    }
    // O_EXCL random name in the target's directory, so commit can link/rename
    // within one filesystem. Persisted (not auto-deleted): cleanup is owned by
    // commit/abort; a hard-killed server may leave a .part file behind, which
    // the stale-staging sweep (here and in gc) eventually removes.
    let staging = tempfile::Builder::new()
        .prefix(&format!("{STAGING_PREFIX}{file_name}."))
        .suffix(STAGING_SUFFIX)
        .tempfile_in(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("create staging file: {e}")))?
        .into_temp_path()
        .keep()
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("keep staging file: {e}")))?;
    let transfer_id = new_transfer_id();
    registry.lock().insert(
        transfer_id.clone(),
        PendingUpload {
            staging: staging.clone(),
            target: abs,
            logical_path: path.to_string(),
            overwrite,
        },
    );
    Ok(ResultBody::UploadPrepare(UploadPrepareResult {
        transfer_id,
        staging_path: staging.to_string_lossy().into_owned(),
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn upload_commit(
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    registry: &UploadRegistry,
    transfer_id: &str,
    size: u64,
    sha256: &str,
    duration_ms: u64,
) -> Result<ResultBody, ProtocolError> {
    let entry = registry.lock().get(transfer_id).cloned().ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::OperationNotFound,
            format!("unknown transfer id: {transfer_id}"),
        )
    })?;
    let meta = std::fs::metadata(&entry.staging).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("staging file unreadable: {e}"))
    })?;
    if !meta.is_file() {
        return Err(ProtocolError::new(
            ErrorCode::NotAFile,
            "staging path is not a regular file",
        ));
    }
    if meta.len() != size {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "staging file has {} bytes but client declared {size}",
                meta.len()
            ),
        ));
    }
    if entry.overwrite {
        std::fs::rename(&entry.staging, &entry.target)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("rename failed: {e}")))?;
    } else {
        // Race-free no-replace install: hard_link fails if the target appeared
        // since prepare, and never clobbers it.
        std::fs::hard_link(&entry.staging, &entry.target).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    format!(
                        "target was created while the upload ran: {}; pass overwrite=true to replace it",
                        entry.logical_path
                    ),
                )
            } else {
                ProtocolError::new(ErrorCode::IoError, format!("link into place failed: {e}"))
            }
        })?;
        std::fs::remove_file(&entry.staging).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("remove staging failed: {e}"))
        })?;
    }
    crate::fsync::fsync_file_or_dir(&entry.target)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("fsync target: {e}")))?;
    let operation_id = store.next_operation_id();
    store.append_transfer_record(TransferOperationRecord {
        operation_id: operation_id.clone(),
        request_id: request_id.to_string(),
        direction: TransferDirection::Upload,
        path: entry.logical_path.clone(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
        timestamp_ms: now_ms(),
    })?;
    registry.lock().remove(transfer_id);
    Ok(ResultBody::Transfer(TransferResult {
        operation_id,
        direction: TransferDirection::Upload,
        path: entry.logical_path,
        size,
        sha256: sha256.to_string(),
        duration_ms,
    }))
}

pub fn upload_abort(
    registry: &UploadRegistry,
    transfer_id: &str,
) -> Result<ResultBody, ProtocolError> {
    let entry = registry.lock().remove(transfer_id).ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::OperationNotFound,
            format!("unknown transfer id: {transfer_id}"),
        )
    })?;
    match std::fs::remove_file(&entry.staging) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                format!("remove staging failed: {e}"),
            ))
        }
    }
    Ok(ResultBody::UploadAbort {
        transfer_id: transfer_id.to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn download_record(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    size: u64,
    sha256: &str,
    duration_ms: u64,
) -> Result<ResultBody, ProtocolError> {
    ws.resolve(path)?;
    let operation_id = store.next_operation_id();
    store.append_transfer_record(TransferOperationRecord {
        operation_id: operation_id.clone(),
        request_id: request_id.to_string(),
        direction: TransferDirection::Download,
        path: path.to_string(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
        timestamp_ms: now_ms(),
    })?;
    Ok(ResultBody::Transfer(TransferResult {
        operation_id,
        direction: TransferDirection::Download,
        path: path.to_string(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
    }))
}

/// Staging paths of uploads currently in flight; the sweeps must never touch
/// these.
pub fn in_flight_staging(registry: &UploadRegistry) -> std::collections::HashSet<PathBuf> {
    registry
        .lock()
        .values()
        .map(|p| p.staging.clone())
        .collect()
}

/// Best-effort removal of abandoned upload staging files in one directory.
/// Deletes only regular files matching the exact remote-workspace staging naming
/// convention, older than `max_age` by mtime, and not in `in_flight`. Never
/// touches other `.part` files. Returns the number of files removed.
pub fn sweep_stale_staging_dir(
    dir: &Path,
    in_flight: &std::collections::HashSet<PathBuf>,
    max_age: std::time::Duration,
) -> usize {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(STAGING_PREFIX) || !name.ends_with(STAGING_SUFFIX) {
            continue;
        }
        let path = entry.path();
        if in_flight.contains(&path) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let stale = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age >= max_age);
        if stale && std::fs::remove_file(&path).is_ok() {
            tracing::info!(path = %path.display(), "removed stale upload staging file");
            removed += 1;
        }
    }
    removed
}

/// Recursive sweep over a whole tree (workspace root or scratch root), used by
/// gc. Does not follow symlinks.
pub fn sweep_stale_staging_tree(
    root: &Path,
    in_flight: &std::collections::HashSet<PathBuf>,
    max_age: std::time::Duration,
) -> usize {
    let mut removed = sweep_stale_staging_dir(root, in_flight, max_age);
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return removed,
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            removed += sweep_stale_staging_tree(&entry.path(), in_flight, max_age);
        }
    }
    removed
}

// ---- raw data plane (hidden CLI modes; no OperationStore, no state lock) ----

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ReceiveMetadata {
    pub size: u64,
    pub sha256: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SendHeader {
    pub size: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SendTrailer {
    pub sha256: String,
}

/// `--transfer-receive`: stream stdin into an existing staging file, then
/// report `{size, sha256}` on stdout. Fails if the byte count differs from
/// the caller-declared size.
pub fn run_transfer_receive(staging: &Path, expect_size: u64) -> anyhow::Result<()> {
    // The staging file must already exist (created by upload_prepare); its
    // absence means the prepare/commit lifecycle is being bypassed or raced.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(staging)
        .map_err(|e| anyhow::anyhow!("open staging file {staging:?}: {e}"))?;
    let mut stdin = std::io::stdin().lock();
    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    loop {
        let n = stdin.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n])?;
        total += n as u64;
    }
    if total != expect_size {
        anyhow::bail!("received {total} bytes but caller declared {expect_size}");
    }
    file.sync_all()?;
    let meta = ReceiveMetadata {
        size: total,
        sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
    };
    println!("{}", serde_json::to_string(&meta)?);
    Ok(())
}

/// `--transfer-send`: re-validate the path against the workspace boundary,
/// then write to stdout: one JSON header line with the size, exactly that
/// many raw bytes, and one JSON trailer line with the SHA-256.
pub fn run_transfer_send(root: &Path, state_base: &Path, path: &str) -> anyhow::Result<()> {
    let state_dir = crate::state_dir_under(state_base, root)?;
    let ws = Workspace::new(root.to_path_buf(), state_dir.join("scratch"))?;
    let abs = ws.resolve(path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let meta =
        std::fs::symlink_metadata(&abs).map_err(|e| anyhow::anyhow!("cannot stat {path}: {e}"))?;
    if !meta.is_file() {
        anyhow::bail!("not a regular file: {path}");
    }
    let size = meta.len();
    let mut file = std::fs::File::open(&abs)?;
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut out, &SendHeader { size })?;
    out.write_all(b"\n")?;
    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut remaining = size;
    while remaining > 0 {
        let want = (remaining as usize).min(buf.len());
        let n = file.read(&mut buf[..want])?;
        if n == 0 {
            anyhow::bail!("file shrank during send: {path}");
        }
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    let trailer = SendTrailer {
        sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
    };
    serde_json::to_writer(&mut out, &trailer)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Measure scratch, and optionally delete files untouched for `max_age`.
///
/// Scratch is the one state directory with no retention of its own: operations,
/// blobs and the request table are all bounded by `--history-limit`, while
/// scratch only ever grows. Reporting is therefore unconditional, but deleting
/// is not -- scratch holds agent-produced artifacts (logs, but also weights and
/// recordings), so a caller must name an age rather than inherit a default that
/// could quietly discard them. Does not follow symlinks.
pub fn scratch_usage(
    root: &Path,
    max_age: Option<std::time::Duration>,
) -> remote_workspace_protocol::ScratchUsage {
    let mut usage = remote_workspace_protocol::ScratchUsage::default();
    let now = std::time::SystemTime::now();
    let mut oldest = std::time::Duration::ZERO;
    scan_scratch(root, max_age, now, &mut oldest, &mut usage);
    if usage.files > 0 {
        usage.oldest_days = Some((oldest.as_secs() / 86_400) as u32);
    }
    usage
}

fn scan_scratch(
    dir: &Path,
    max_age: Option<std::time::Duration>,
    now: std::time::SystemTime,
    oldest: &mut std::time::Duration,
    usage: &mut remote_workspace_protocol::ScratchUsage,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            scan_scratch(&entry.path(), max_age, now, oldest, usage);
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let age = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .unwrap_or_default();
        if max_age.is_some_and(|limit| age > limit) && std::fs::remove_file(entry.path()).is_ok() {
            usage.removed_files += 1;
            usage.removed_bytes += meta.len();
            continue;
        }
        usage.files += 1;
        usage.bytes += meta.len();
        *oldest = (*oldest).max(age);
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::Duration;

    #[test]
    fn sweep_matches_only_the_exact_staging_convention() {
        let dir = tempfile::tempdir().unwrap();
        let stale = dir
            .path()
            .join(".remote-workspace-upload.f.bin.abc123.part");
        let foreign_part = dir.path().join("download.part");
        let dotted_part = dir.path().join(".f.bin.xyz.part");
        let normal = dir.path().join("kept.txt");
        for p in [&stale, &foreign_part, &dotted_part, &normal] {
            std::fs::write(p, b"x").unwrap();
        }
        // max_age zero makes every matching file stale, isolating the
        // name-matching rule from the age rule.
        let removed = sweep_stale_staging_dir(dir.path(), &HashSet::new(), Duration::ZERO);
        assert_eq!(removed, 1);
        assert!(!stale.exists());
        assert!(foreign_part.exists());
        assert!(dotted_part.exists());
        assert!(normal.exists());
    }

    #[test]
    fn sweep_spares_fresh_and_in_flight_files() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join(".remote-workspace-upload.a.111.part");
        let in_flight = dir.path().join(".remote-workspace-upload.b.222.part");
        std::fs::write(&fresh, b"x").unwrap();
        std::fs::write(&in_flight, b"x").unwrap();

        // Fresh mtime, generous threshold: nothing is stale.
        let removed =
            sweep_stale_staging_dir(dir.path(), &HashSet::new(), Duration::from_secs(3600));
        assert_eq!(removed, 0);

        // Registered in-flight staging survives even a zero threshold.
        let mut registered = HashSet::new();
        registered.insert(in_flight.clone());
        let removed = sweep_stale_staging_dir(dir.path(), &registered, Duration::ZERO);
        assert_eq!(removed, 1);
        assert!(!fresh.exists());
        assert!(in_flight.exists());
    }

    #[test]
    fn tree_sweep_recurses_into_subdirectories() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        let top = dir.path().join(".remote-workspace-upload.t.1.part");
        let deep = dir.path().join("a/b/.remote-workspace-upload.d.2.part");
        std::fs::write(&top, b"x").unwrap();
        std::fs::write(&deep, b"x").unwrap();
        let removed = sweep_stale_staging_tree(dir.path(), &HashSet::new(), Duration::ZERO);
        assert_eq!(removed, 2);
        assert!(!top.exists());
        assert!(!deep.exists());
    }
}
