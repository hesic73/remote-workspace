// The only test in this binary on purpose: it lowers RLIMIT_FSIZE and changes
// the disposition of SIGXFSZ, both of which are process-wide.
//
// What it forces is the one failure the append path cannot otherwise be shown
// to survive: the filesystem accepting the first bytes of a log line and then
// refusing the rest. A partial line must be undone, because the loader repairs
// an unterminated tail but must refuse a fragment that a later append has
// welded onto and terminated.

use remote_workspace_protocol::OperationKind;
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

#[test]
fn a_write_the_filesystem_cuts_short_leaves_the_log_loadable() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let ops = state.join("operations.jsonl");

    let store = OperationStore::new(state.clone()).unwrap();
    // One good record, so there is something to lose.
    let first = store
        .prepare_fs_record(
            "req-1",
            OperationKind::Edit,
            "a.txt",
            None,
            format!("sha256:{}", "11".repeat(32)),
        )
        .unwrap();
    let good_len = std::fs::metadata(&ops).unwrap().len();
    assert!(good_len > 0);

    {
        // Room for a few more bytes only: the next record is a few hundred, so
        // the write takes a prefix and then fails.
        let _limit = FileSizeLimit::set(good_len + 16);
        let err = store
            .prepare_fs_record(
                "req-2",
                OperationKind::Edit,
                "b.txt",
                None,
                format!("sha256:{}", "22".repeat(32)),
            )
            .expect_err("the capped write must fail");
        assert_eq!(err.code, remote_workspace_protocol::ErrorCode::IoError);
    }

    assert_eq!(
        std::fs::metadata(&ops).unwrap().len(),
        good_len,
        "the partial line was left behind instead of being undone"
    );

    // The log still loads, and still holds exactly what was durably written.
    drop(store);
    let reopened = OperationStore::new(state).expect("log must still be loadable");
    let text = std::fs::read_to_string(&ops).unwrap();
    assert_eq!(text.lines().count(), 1);
    assert!(text.contains(&first));
    assert!(!text.contains("b.txt"));

    // And appending after the failure still produces a loadable log.
    reopened
        .prepare_fs_record(
            "req-3",
            OperationKind::Edit,
            "c.txt",
            None,
            format!("sha256:{}", "33".repeat(32)),
        )
        .unwrap();
    drop(reopened);
    OperationStore::new(dir.path().join("state")).expect("log must still be loadable");
}
