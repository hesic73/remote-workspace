use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use remote_workspace_protocol::{
    AnyOperationRecord, ErrorCode, ExecOperationRecord, FsOperationRecord, ProtocolError,
    ServerMessage, TransferOperationRecord,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Lock past any realistic shard EOF so Windows mandatory byte-range locking
/// never blocks concurrent readers scanning bytes `0..len`.
const SHARD_LOCK_OFFSET: u64 = 1 << 32;
const MAX_CACHED_REQUESTS: usize = 1024;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum StoredResult {
    Done(ServerMessage),
    Error(ProtocolError),
}

#[derive(Default)]
struct RequestCache {
    entries: HashMap<String, Option<StoredResult>>,
    finished_order: VecDeque<String>,
}

struct OpenShard {
    path: PathBuf,
    file: std::fs::File,
}

impl Drop for OpenShard {
    fn drop(&mut self) {
        crate::locking::unlock(&self.file, SHARD_LOCK_OFFSET);
    }
}

struct OpIdState {
    last_ts_ms: u64,
    seq: u16,
}

static OP_ID_STATE: Mutex<OpIdState> = Mutex::new(OpIdState {
    last_ts_ms: 0,
    seq: 0,
});

#[derive(Clone)]
pub struct OperationStore {
    log_dir: PathBuf,
    shard: Arc<Mutex<Option<OpenShard>>>,
    requests: Arc<Mutex<RequestCache>>,
    write_lock: Arc<AsyncMutex<()>>,
    log_torn: Arc<AtomicBool>,
}

pub struct PruneStats {
    pub removed_operations: usize,
    pub retained_operations: usize,
}

impl OperationStore {
    pub fn new(log_dir: PathBuf) -> Result<Self, ProtocolError> {
        std::fs::create_dir_all(&log_dir).map_err(io_to_protocol)?;
        Ok(Self {
            log_dir,
            shard: Arc::new(Mutex::new(None)),
            requests: Arc::new(Mutex::new(RequestCache::default())),
            write_lock: Arc::new(AsyncMutex::new(())),
            log_torn: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn state_dir(&self) -> &Path {
        &self.log_dir
    }

    pub async fn write_guard(&self) -> OwnedMutexGuard<()> {
        self.write_lock.clone().lock_owned().await
    }

    pub fn next_operation_id(&self) -> String {
        let now = now_ms();
        let (ts, seq) = {
            let mut g = OP_ID_STATE.lock();
            if now > g.last_ts_ms {
                g.last_ts_ms = now;
                g.seq = 0;
                (now, 0)
            } else if g.seq < u16::MAX {
                g.seq += 1;
                (g.last_ts_ms, g.seq)
            } else {
                g.last_ts_ms += 1;
                g.seq = 0;
                (g.last_ts_ms, 0)
            }
        };
        let ts_low = ts & 0xFFFF_FFFF_FFFF;
        let pid_low = std::process::id() & 0xFF_FFFF;
        format!("op-{ts_low:012x}-{pid_low:06x}-{seq:04x}")
    }

    /// Claim `request_id` within this session. Returns `None` if the caller won
    /// ownership, `Some(None)` if already in progress, or `Some(Some(res))` to
    /// replay a finished request.
    pub fn claim_request(&self, request_id: &str) -> Option<Option<StoredResult>> {
        let mut cache = self.requests.lock();
        if let Some(existing) = cache.entries.get(request_id) {
            return Some(existing.clone());
        }
        cache.entries.insert(request_id.to_string(), None);
        None
    }

    pub fn remember_result(&self, request_id: &str, msg: ServerMessage) {
        self.finish_request(request_id, StoredResult::Done(msg));
    }

    pub fn remember_error(&self, request_id: &str, err: ProtocolError) {
        self.finish_request(request_id, StoredResult::Error(err));
    }

    fn finish_request(&self, request_id: &str, result: StoredResult) {
        let mut cache = self.requests.lock();
        cache.entries.insert(request_id.to_string(), Some(result));
        cache.finished_order.push_back(request_id.to_string());
        while cache.finished_order.len() > MAX_CACHED_REQUESTS {
            if let Some(old_id) = cache.finished_order.pop_front() {
                if matches!(cache.entries.get(&old_id), Some(Some(_))) {
                    cache.entries.remove(&old_id);
                }
            }
        }
    }

    pub fn append_fs_record(&self, record: FsOperationRecord) -> Result<(), ProtocolError> {
        self.append_record(AnyOperationRecord::Fs(record))
    }

    pub fn append_exec_record(&self, record: ExecOperationRecord) -> Result<(), ProtocolError> {
        self.append_record(AnyOperationRecord::Exec(record))
    }

    pub fn append_transfer_record(
        &self,
        record: TransferOperationRecord,
    ) -> Result<(), ProtocolError> {
        self.append_record(AnyOperationRecord::Transfer(record))
    }

    pub fn append_record(&self, record: AnyOperationRecord) -> Result<(), ProtocolError> {
        let mut buf = serde_json::to_vec(&record).map_err(|e| {
            ProtocolError::new(
                ErrorCode::IoError,
                format!("failed to serialize operation record: {e}"),
            )
        })?;
        buf.push(b'\n');

        let mut guard = self.shard.lock();
        if self.log_torn.load(Ordering::SeqCst) {
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                "refusing to append to op log: an earlier partial line could not be undone; \
                 restart the server to open a fresh shard",
            ));
        }
        let shard = match guard.as_mut() {
            Some(s) => s,
            None => {
                let opened = open_session_shard(&self.log_dir.join("ops"))?;
                guard.insert(opened)
            }
        };
        let len_before = shard
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("seek op shard: {e}")))?;
        if let Err(e) = shard.file.write_all(&buf).and_then(|()| shard.file.flush()) {
            if let Err(t) = shard
                .file
                .set_len(len_before)
                .and_then(|()| shard.file.seek(SeekFrom::Start(len_before)).map(|_| ()))
            {
                self.log_torn.store(true, Ordering::SeqCst);
                return Err(ProtocolError::new(
                    ErrorCode::IoError,
                    format!("write op shard: {e}; rollback failed ({t}), refusing further appends"),
                ));
            }
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                format!("write op shard: {e}"),
            ));
        }
        Ok(())
    }

    pub fn shard_count_exceeds(&self, threshold: usize) -> bool {
        let entries = match std::fs::read_dir(self.log_dir.join("ops")) {
            Ok(e) => e,
            Err(_) => return false,
        };
        let mut count = 0usize;
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".jsonl"))
            {
                count += 1;
                if count > threshold {
                    return true;
                }
            }
        }
        false
    }

    pub fn find_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<AnyOperationRecord>, ProtocolError> {
        let records = self.collect_all_committed()?;
        Ok(records
            .into_iter()
            .find(|r| r.operation_id() == operation_id))
    }

    pub fn history(&self, limit: Option<usize>) -> Result<Vec<AnyOperationRecord>, ProtocolError> {
        let mut committed = self.collect_all_committed()?;
        if let Some(n) = limit {
            if committed.len() > n {
                let start = committed.len() - n;
                committed = committed.split_off(start);
            }
        }
        for record in &mut committed {
            if let AnyOperationRecord::Exec(exec) = record {
                exec.stdout.prefix.clear();
                exec.stdout.suffix.clear();
                exec.stdout.omitted_bytes = exec.stdout.total_bytes;
                exec.stderr.prefix.clear();
                exec.stderr.suffix.clear();
                exec.stderr.omitted_bytes = exec.stderr.total_bytes;
            }
        }
        Ok(committed)
    }

    fn collect_all_committed(&self) -> Result<Vec<AnyOperationRecord>, ProtocolError> {
        let mut by_id: BTreeMap<String, AnyOperationRecord> = BTreeMap::new();
        let legacy_ops = self.log_dir.join("operations.jsonl");
        read_committed_into(&legacy_ops, &mut by_id)?;

        let ops_dir = self.log_dir.join("ops");
        match std::fs::read_dir(&ops_dir) {
            Ok(entries) => {
                let mut paths = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(io_to_protocol)?;
                    if entry
                        .file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with(".jsonl"))
                    {
                        paths.push(entry.path());
                    }
                }
                paths.sort();
                for path in &paths {
                    read_committed_into(path, &mut by_id)?;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_to_protocol(e)),
        }

        Ok(sort_records(by_id.into_values()))
    }

    /// Compact closed shards + legacy `operations.jsonl` down to at most one
    /// closed shard retaining `keep` newest committed operations across live +
    /// closed shards. Performs zero writes when already compact.
    pub fn prune(&self, keep: usize) -> Result<PruneStats, ProtocolError> {
        cleanup_legacy_artifacts(&self.log_dir);

        let own_shard_path = self.shard.lock().as_ref().map(|s| s.path.clone());
        let mut live_by_id: BTreeMap<String, AnyOperationRecord> = BTreeMap::new();
        let mut closed_by_id: BTreeMap<String, AnyOperationRecord> = BTreeMap::new();
        let mut closed_shards: Vec<(PathBuf, std::fs::File)> = Vec::new();

        let ops_dir = self.log_dir.join("ops");
        match std::fs::read_dir(&ops_dir) {
            Ok(entries) => {
                let mut paths = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(io_to_protocol)?;
                    if entry
                        .file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with(".jsonl"))
                    {
                        paths.push(entry.path());
                    }
                }
                paths.sort();
                for path in paths {
                    if own_shard_path.as_ref() == Some(&path) {
                        read_committed_into(&path, &mut live_by_id)?;
                        continue;
                    }
                    let f = match std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                    {
                        Ok(f) => f,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(io_to_protocol(e)),
                    };
                    let closed = crate::locking::try_lock_exclusive(&f, SHARD_LOCK_OFFSET)
                        .map_err(io_to_protocol)?;
                    if closed {
                        if !is_still_linked(&f, &path) {
                            continue;
                        }
                        read_committed_into(&path, &mut closed_by_id)?;
                        closed_shards.push((path, f));
                    } else {
                        read_committed_into(&path, &mut live_by_id)?;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_to_protocol(e)),
        }

        let legacy_ops = self.log_dir.join("operations.jsonl");
        let has_legacy_ops = legacy_ops.exists();
        if has_legacy_ops {
            read_committed_into(&legacy_ops, &mut closed_by_id)?;
        }

        for live_id in live_by_id.keys() {
            closed_by_id.remove(live_id);
        }
        let live_count = live_by_id.len();
        let closed_records = sort_records(closed_by_id.into_values());
        let closed_keep = keep.saturating_sub(live_count);
        let removed_operations = closed_records.len().saturating_sub(closed_keep);
        let retained_closed = &closed_records[removed_operations..];
        let retained_operations = live_count + retained_closed.len();

        let already_compact = !has_legacy_ops
            && removed_operations == 0
            && (closed_shards.is_empty()
                || (closed_shards.len() == 1 && !retained_closed.is_empty()));
        if already_compact {
            return Ok(PruneStats {
                removed_operations: 0,
                retained_operations,
            });
        }

        if !retained_closed.is_empty() {
            std::fs::create_dir_all(&ops_dir).map_err(io_to_protocol)?;
            write_compacted_shard(&ops_dir, retained_closed)?;
        }

        if has_legacy_ops {
            match std::fs::remove_file(&legacy_ops) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_to_protocol(e)),
            }
        }
        for (old_path, lock_file) in closed_shards {
            #[cfg(windows)]
            {
                crate::locking::unlock(&lock_file, SHARD_LOCK_OFFSET);
                drop(lock_file);
            }
            match std::fs::remove_file(&old_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_to_protocol(e)),
            }
            #[cfg(not(windows))]
            {
                crate::locking::unlock(&lock_file, SHARD_LOCK_OFFSET);
                drop(lock_file);
            }
        }

        Ok(PruneStats {
            removed_operations,
            retained_operations,
        })
    }
}

fn cleanup_legacy_artifacts(state_dir: &Path) {
    let blobs = state_dir.join("blobs");
    if blobs.symlink_metadata().is_ok() {
        let _ = std::fs::remove_dir_all(&blobs);
    }
    for name in [
        "requests.jsonl",
        "op-counter",
        "lock",
        "pending_uploads.json",
    ] {
        let path = state_dir.join(name);
        if path.symlink_metadata().is_ok() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn sort_records(iter: impl IntoIterator<Item = AnyOperationRecord>) -> Vec<AnyOperationRecord> {
    let mut records: Vec<AnyOperationRecord> = iter.into_iter().collect();
    records.sort_by(|a, b| {
        a.timestamp_ms()
            .cmp(&b.timestamp_ms())
            .then_with(|| a.operation_id().cmp(b.operation_id()))
    });
    records
}

fn new_shard_filename() -> String {
    let ts_low = now_ms() & 0xFFFF_FFFF_FFFF;
    let pid_low = std::process::id() & 0xFF_FFFF;
    let rand = rand_u16();
    format!("{ts_low:012x}-{pid_low:06x}-{rand:04x}.jsonl")
}

fn rand_u16() -> u16 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    static SEQ: AtomicU16 = AtomicU16::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut h = RandomState::new().build_hasher();
    h.write_u16(n);
    h.finish() as u16
}

fn open_session_shard(ops_dir: &Path) -> Result<OpenShard, ProtocolError> {
    std::fs::create_dir_all(ops_dir).map_err(io_to_protocol)?;
    for _ in 0..32 {
        let path = ops_dir.join(new_shard_filename());
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(io_to_protocol(e)),
        };
        if !crate::locking::try_lock_exclusive(&file, SHARD_LOCK_OFFSET).map_err(io_to_protocol)? {
            continue;
        }
        if !is_still_linked(&file, &path) {
            continue;
        }
        return Ok(OpenShard { path, file });
    }
    Err(ProtocolError::new(
        ErrorCode::IoError,
        format!("could not create unique operation shard in {ops_dir:?}"),
    ))
}

#[cfg(unix)]
fn is_still_linked(file: &std::fs::File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    matches!(
        (file.metadata(), std::fs::metadata(path)),
        (Ok(a), Ok(b)) if a.dev() == b.dev() && a.ino() == b.ino()
    )
}

#[cfg(windows)]
fn is_still_linked(_file: &std::fs::File, path: &Path) -> bool {
    path.is_file()
}

fn write_compacted_shard(
    ops_dir: &Path,
    records: &[AnyOperationRecord],
) -> Result<PathBuf, ProtocolError> {
    let mut tmp = tempfile::NamedTempFile::new_in(ops_dir).map_err(io_to_protocol)?;
    for item in records {
        let line = serde_json::to_string(item)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("serialize: {e}")))?;
        writeln!(tmp, "{line}").map_err(io_to_protocol)?;
    }
    tmp.as_file().sync_data().map_err(io_to_protocol)?;
    for _ in 0..32 {
        let dest = ops_dir.join(new_shard_filename());
        match tmp.persist_noclobber(&dest) {
            Ok(_) => return Ok(dest),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                tmp = e.file;
            }
            Err(e) => {
                return Err(ProtocolError::new(
                    ErrorCode::IoError,
                    format!("persist compacted shard {dest:?}: {}", e.error),
                ))
            }
        }
    }
    Err(ProtocolError::new(
        ErrorCode::IoError,
        format!("could not persist unique compacted shard in {ops_dir:?}"),
    ))
}

struct LineEntry {
    text: String,
    has_newline: bool,
    utf8_ok: bool,
}

fn read_lines(path: &Path) -> std::io::Result<Vec<LineEntry>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < bytes.len() {
        match bytes[start..].iter().position(|&b| b == b'\n') {
            Some(i) => {
                let nl = start + i;
                let (text, utf8_ok) = bytes_to_text(&bytes[start..nl]);
                out.push(LineEntry {
                    text,
                    has_newline: true,
                    utf8_ok,
                });
                start = nl + 1;
            }
            None => {
                let (text, utf8_ok) = bytes_to_text(&bytes[start..]);
                out.push(LineEntry {
                    text,
                    has_newline: false,
                    utf8_ok,
                });
                break;
            }
        }
    }
    Ok(out)
}

fn bytes_to_text(bytes: &[u8]) -> (String, bool) {
    match std::str::from_utf8(bytes) {
        Ok(s) => (s.to_string(), true),
        Err(_) => (String::new(), false),
    }
}

#[allow(clippy::large_enum_variant)]
enum ParseResult<T> {
    Parsed(T),
    SkippedCrashTruncated,
    SkippedBlank,
}

fn parse_log_line_strict<T: serde::de::DeserializeOwned>(
    entry: &LineEntry,
    path: &Path,
    idx: usize,
) -> Result<ParseResult<T>, ProtocolError> {
    if !entry.utf8_ok {
        if !entry.has_newline {
            return Ok(ParseResult::SkippedCrashTruncated);
        }
        return Err(ProtocolError::new(
            ErrorCode::IoError,
            format!(
                "corrupted log {:?}: line {} is not valid UTF-8",
                path.display(),
                idx + 1
            ),
        ));
    }
    let trimmed = entry.text.trim();
    if trimmed.is_empty() {
        if !entry.has_newline {
            return Ok(ParseResult::SkippedBlank);
        }
        return Err(ProtocolError::new(
            ErrorCode::IoError,
            format!(
                "corrupted log {:?}: line {} is empty (jsonl records must not be blank)",
                path.display(),
                idx + 1
            ),
        ));
    }
    match serde_json::from_str::<T>(trimmed) {
        Ok(v) => Ok(ParseResult::Parsed(v)),
        Err(e) => {
            if !entry.has_newline {
                return Ok(ParseResult::SkippedCrashTruncated);
            }
            Err(ProtocolError::new(
                ErrorCode::IoError,
                format!(
                    "corrupted log {:?}: line {} is not valid JSON ({e})",
                    path.display(),
                    idx + 1
                ),
            ))
        }
    }
}

fn read_committed_into(
    path: &Path,
    by_id: &mut BTreeMap<String, AnyOperationRecord>,
) -> Result<(), ProtocolError> {
    let lines = read_lines(path).map_err(io_to_protocol)?;
    for (idx, line) in lines.iter().enumerate() {
        let rec: AnyOperationRecord = match parse_log_line_strict(line, path, idx)? {
            ParseResult::Parsed(r) => r,
            ParseResult::SkippedCrashTruncated | ParseResult::SkippedBlank => continue,
        };
        if rec.is_committed() {
            by_id.insert(rec.operation_id().to_string(), rec);
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .expect("system clock is set before 1970")
}

fn io_to_protocol(e: std::io::Error) -> ProtocolError {
    ProtocolError::new(ErrorCode::IoError, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_workspace_protocol::OperationKind;

    fn sample_fs(op_id: &str, ts_ms: u64) -> FsOperationRecord {
        FsOperationRecord {
            operation_id: op_id.to_string(),
            request_id: format!("req-{op_id}"),
            kind: OperationKind::Create,
            path: "a.txt".into(),
            before_hash: None,
            after_hash: format!("sha256:{}", "11".repeat(32)),
            timestamp_ms: ts_ms,
        }
    }

    #[test]
    fn startup_touches_no_shards_or_lock_files() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let _store = OperationStore::new(state.clone()).unwrap();
        assert!(state.is_dir());
        assert!(!state.join("ops").exists());
        assert!(!state.join("lock").exists());
        assert!(!state.join("requests.jsonl").exists());
        assert!(!state.join("op-counter").exists());
    }

    #[test]
    fn operation_ids_are_monotonic_and_unique_within_process() {
        let dir = tempfile::tempdir().unwrap();
        let s1 = OperationStore::new(dir.path().join("s1")).unwrap();
        let s2 = OperationStore::new(dir.path().join("s2")).unwrap();
        let mut prev = String::new();
        for i in 0..200 {
            let id = if i % 2 == 0 {
                s1.next_operation_id()
            } else {
                s2.next_operation_id()
            };
            assert!(id.starts_with("op-"));
            assert_eq!(id.len(), 3 + 12 + 1 + 6 + 1 + 4, "bad id shape: {id}");
            assert!(id > prev, "{id} was not strictly greater than {prev}");
            prev = id;
        }
    }

    #[test]
    fn two_stores_share_state_dir_and_prune_spares_live_shard() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let closed_a = OperationStore::new(state.clone()).unwrap();
        let closed_b = OperationStore::new(state.clone()).unwrap();
        let live = OperationStore::new(state.clone()).unwrap();

        let id1 = closed_a.next_operation_id();
        closed_a.append_fs_record(sample_fs(&id1, 10)).unwrap();
        drop(closed_a);

        let id2 = closed_b.next_operation_id();
        closed_b.append_fs_record(sample_fs(&id2, 20)).unwrap();
        drop(closed_b);

        let id3 = live.next_operation_id();
        live.append_fs_record(sample_fs(&id3, 30)).unwrap();

        let pruner = OperationStore::new(state.clone()).unwrap();
        let stats = pruner.prune(2).unwrap();
        assert_eq!(stats.removed_operations, 1);
        assert_eq!(stats.retained_operations, 2);

        let history = live.history(None).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].operation_id(), id2);
        assert_eq!(history[1].operation_id(), id3);

        // Now closed shards are already compacted to 1 file with 0 removals:
        // a second prune must be a zero-write no-op (mtime/name unchanged).
        let shards_before: Vec<PathBuf> = std::fs::read_dir(state.join("ops"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        let stats2 = pruner.prune(2).unwrap();
        assert_eq!(stats2.removed_operations, 0);
        assert_eq!(stats2.retained_operations, 2);
        let shards_after: Vec<PathBuf> = std::fs::read_dir(state.join("ops"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(shards_before, shards_after);
    }
}
