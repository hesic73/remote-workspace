#![cfg(unix)]

use std::path::PathBuf;

use remote_workspace_protocol::{FsOperationRecord, OperationKind};
use remote_workspace_server::store::OperationStore;

struct FileSizeLimit(libc::rlimit);

impl FileSizeLimit {
    /// Cap file writes at `bytes`, keeping SIGXFSZ from killing the test.
    fn set(bytes: u64) -> Self {
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        }
        let mut previous = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_FSIZE, &mut previous) },
            0
        );
        let limited = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: previous.rlim_max,
        };
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &limited) }, 0);
        Self(previous)
    }
}

impl Drop for FileSizeLimit {
    fn drop(&mut self) {
        unsafe {
            libc::setrlimit(libc::RLIMIT_FSIZE, &self.0);
        }
    }
}

fn single_shard(ops_dir: &std::path::Path) -> PathBuf {
    let mut shards: Vec<PathBuf> = std::fs::read_dir(ops_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("jsonl"))
        .collect();
    assert_eq!(shards.len(), 1, "expected 1 shard, got {shards:?}");
    shards.pop().unwrap()
}

fn sample_fs(op_id: &str, req_id: &str, path: &str, fill: &str, ts: u64) -> FsOperationRecord {
    FsOperationRecord {
        operation_id: op_id.into(),
        request_id: req_id.into(),
        kind: OperationKind::Edit,
        path: path.into(),
        before_hash: None,
        after_hash: format!("sha256:{}", fill.repeat(32)),
        timestamp_ms: ts,
    }
}

#[test]
fn a_write_the_filesystem_cuts_short_leaves_the_shard_loadable() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");

    let store = OperationStore::new(state.clone()).unwrap();
    let first = store.next_operation_id();
    store
        .append_fs_record(sample_fs(&first, "req-1", "a.txt", "11", 1))
        .unwrap();

    let shard = single_shard(&state.join("ops"));
    let good_len = std::fs::metadata(&shard).unwrap().len();
    assert!(good_len > 0);

    {
        let _limit = FileSizeLimit::set(good_len + 16);
        let second = store.next_operation_id();
        let err = store
            .append_fs_record(sample_fs(&second, "req-2", "b.txt", "22", 2))
            .expect_err("the capped write must fail");
        assert_eq!(err.code, remote_workspace_protocol::ErrorCode::IoError);
    }

    assert_eq!(
        std::fs::metadata(&shard).unwrap().len(),
        good_len,
        "the partial line was left behind instead of being undone"
    );

    let history = store.history(None).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation_id(), first);

    let third = store.next_operation_id();
    store
        .append_fs_record(sample_fs(&third, "req-3", "c.txt", "33", 3))
        .unwrap();

    drop(store);
    let reopened = OperationStore::new(state).unwrap();
    let history = reopened.history(None).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].operation_id(), first);
    assert_eq!(history[1].operation_id(), third);
}
