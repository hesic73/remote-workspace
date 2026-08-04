use remote_workspace_protocol::{EditSpec, ErrorCode, OperationKind, ProtocolError, ResultBody};
use remote_workspace_server::fs_ops::{self, MAX_EDITS, MAX_TEXT_BYTES};
use remote_workspace_server::store::OperationStore;
use remote_workspace_server::workspace::Workspace;
use sha2::{Digest, Sha256};

struct Fixture {
    _dir: tempfile::TempDir,
    ws: Workspace,
    store: OperationStore,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
    let store = OperationStore::new(dir.path().join("state")).unwrap();
    Fixture {
        _dir: dir,
        ws,
        store,
    }
}

async fn create(f: &Fixture, path: &str, content: &str) -> Result<ResultBody, ProtocolError> {
    let guard = f.store.write_guard().await;
    fs_ops::create(&f.ws, &f.store, &guard, "req-create", path, content)
}

async fn edit(
    f: &Fixture,
    path: &str,
    base_hash: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
) -> Result<ResultBody, ProtocolError> {
    edit_many(
        f,
        path,
        base_hash,
        vec![spec(old_text, new_text, replace_all)],
    )
    .await
}

async fn edit_many(
    f: &Fixture,
    path: &str,
    base_hash: &str,
    edits: Vec<EditSpec>,
) -> Result<ResultBody, ProtocolError> {
    let guard = f.store.write_guard().await;
    fs_ops::edit(&f.ws, &f.store, &guard, "req-edit", path, base_hash, &edits)
}

fn spec(old_text: &str, new_text: &str, replace_all: bool) -> EditSpec {
    EditSpec {
        old_text: old_text.into(),
        new_text: new_text.into(),
        replace_all,
    }
}

fn new_hash(r: ResultBody) -> String {
    match r {
        ResultBody::Mutation(m) => m.new_hash,
        other => panic!("expected mutation result, got {other:?}"),
    }
}

fn hash_of(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    format!("sha256:{}", hex::encode(h.finalize()))
}

fn code(r: Result<ResultBody, ProtocolError>) -> ErrorCode {
    r.expect_err("expected an error").code
}

#[tokio::test]
async fn create_refuses_existing_path_and_sets_conventional_mode() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture();
    create(&f, "a.txt", "one").await.unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("a.txt")).unwrap(),
        "one"
    );
    let mode = std::fs::metadata(f.ws.root.join("a.txt"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o644);
    assert_eq!(
        code(create(&f, "a.txt", "two").await),
        ErrorCode::AlreadyExists
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("a.txt")).unwrap(),
        "one",
        "a refused create must leave the file untouched"
    );
}

#[tokio::test]
async fn edit_replaces_multiline_text_and_supports_deletion() {
    let f = fixture();
    let hash = new_hash(
        create(&f, "f.txt", "fn a() {}\nfn b() {}\nfn c() {}\n")
            .await
            .unwrap(),
    );
    let hash = new_hash(
        edit(
            &f,
            "f.txt",
            &hash,
            "fn b() {}\nfn c() {}\n",
            "fn b2() {}\n",
            false,
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "fn a() {}\nfn b2() {}\n"
    );
    // Empty new_text deletes the matched text.
    edit(&f, "f.txt", &hash, "fn b2() {}\n", "", false)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "fn a() {}\n"
    );
}

#[tokio::test]
async fn ambiguous_match_requires_replace_all() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "x = 1\nx = 1\n").await.unwrap());
    assert_eq!(
        code(edit(&f, "f.txt", &hash, "x = 1\n", "x = 2\n", false).await),
        ErrorCode::AmbiguousMatch
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "x = 1\nx = 1\n",
        "ambiguous edit must not partially apply"
    );
    edit(&f, "f.txt", &hash, "x = 1\n", "x = 2\n", true)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "x = 2\nx = 2\n"
    );
}

#[tokio::test]
async fn edit_input_validation() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "abc").await.unwrap());
    assert_eq!(
        code(edit(&f, "f.txt", &hash, "zzz", "y", false).await),
        ErrorCode::NoMatch
    );
    assert_eq!(
        code(edit(&f, "f.txt", &hash, "", "y", false).await),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        code(edit(&f, "f.txt", &hash, "abc", "abc", false).await),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        code(edit(&f, "missing.txt", "sha256:abc", "a", "b", false).await),
        ErrorCode::NotFound
    );
    let huge = "x".repeat(MAX_TEXT_BYTES + 1);
    assert_eq!(
        code(edit(&f, "f.txt", &hash, &huge, "y", false).await),
        ErrorCode::InvalidRequest
    );
    let guard = f.store.write_guard().await;
    let r = fs_ops::create(&f.ws, &f.store, &guard, "req-huge", "huge.txt", &huge);
    drop(guard);
    assert_eq!(r.unwrap_err().code, ErrorCode::InvalidRequest);
}

// ---- several replacements in one edit ----

// The defining property: each replacement sees what the previous ones
// produced, so a later one may match text an earlier one introduced.
#[tokio::test]
async fn edits_apply_in_order_each_to_the_previous_result() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "alpha\nbeta\ngamma\n").await.unwrap());
    edit_many(
        &f,
        "f.txt",
        &hash,
        vec![
            spec("alpha", "ALPHA", false),
            spec("gamma", "gamma2", false),
            // Matches only because the first replacement produced it.
            spec("ALPHA", "one", false),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "one\nbeta\ngamma2\n"
    );
}

// A failure anywhere in the list leaves the file byte-for-byte unchanged --
// including a failure in the last replacement, after earlier ones already
// "succeeded" against the in-memory content.
#[tokio::test]
async fn a_failure_anywhere_applies_none_of_the_edits() {
    let f = fixture();
    let original = "a\nb\nc\n";
    let hash = new_hash(create(&f, "f.txt", original).await.unwrap());

    let cases: Vec<(Vec<EditSpec>, ErrorCode)> = vec![
        // Last one matches nothing.
        (
            vec![
                spec("a\n", "A\n", false),
                spec("b\n", "B\n", false),
                spec("zzz", "x", false),
            ],
            ErrorCode::NoMatch,
        ),
        // Last one is ambiguous in the content the earlier ones produced.
        (
            vec![
                spec("b\n", "a\n", false),
                spec("c\n", "a\n", false),
                spec("a\n", "q\n", false),
            ],
            ErrorCode::AmbiguousMatch,
        ),
        // Last one is malformed: rejected before the file is even read.
        (
            vec![spec("a\n", "A\n", false), spec("", "x", false)],
            ErrorCode::InvalidRequest,
        ),
    ];
    for (edits, want) in cases {
        assert_eq!(code(edit_many(&f, "f.txt", &hash, edits).await), want);
        assert_eq!(
            std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
            original,
            "a rejected edit list must not partially apply"
        );
    }
}

// Several replacements are ONE operation: one record, one before/after pair.
// Splitting them into several would make the list interruptible, which is the
// property this exists to remove.
#[tokio::test]
async fn several_replacements_are_a_single_operation() {
    let f = fixture();
    let created = create(&f, "f.txt", "1\n2\n3\n").await.unwrap();
    let hash = new_hash(created);
    let before = f.store.history(None).len();
    let result = edit_many(
        &f,
        "f.txt",
        &hash,
        vec![
            spec("1\n", "one\n", false),
            spec("2\n", "two\n", false),
            spec("3\n", "three\n", false),
        ],
    )
    .await
    .unwrap();
    match result {
        ResultBody::Mutation(m) => {
            assert_eq!(m.old_hash.as_deref(), Some(hash.as_str()));
            assert_ne!(m.new_hash, hash);
        }
        other => panic!("expected mutation, got {other:?}"),
    }
    assert_eq!(
        f.store.history(None).len() - before,
        1,
        "one edit request must append exactly one operation record"
    );
}

// Error text names the failing replacement when there is more than one, and
// stays index-free for a lone one.
#[tokio::test]
async fn errors_name_which_replacement_failed() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a\nb\n").await.unwrap());
    let err = edit_many(
        &f,
        "f.txt",
        &hash,
        vec![
            spec("a\n", "A\n", false),
            spec("b\n", "B\n", false),
            spec("zzz", "x", false),
        ],
    )
    .await
    .expect_err("must fail");
    assert!(
        err.message.contains("edit 3 of 3"),
        "message must locate the failure: {}",
        err.message
    );

    let err = edit(&f, "f.txt", &hash, "zzz", "x", false)
        .await
        .expect_err("must fail");
    assert!(
        !err.message.contains("edit 1 of 1"),
        "a lone replacement needs no index: {}",
        err.message
    );
}

#[tokio::test]
async fn edit_list_bounds() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a\n").await.unwrap());
    assert_eq!(
        code(edit_many(&f, "f.txt", &hash, vec![]).await),
        ErrorCode::InvalidRequest,
        "an empty list would report success having changed nothing"
    );
    let too_many = (0..MAX_EDITS + 1)
        .map(|i| spec(&format!("x{i}"), &format!("y{i}"), false))
        .collect();
    assert_eq!(
        code(edit_many(&f, "f.txt", &hash, too_many).await),
        ErrorCode::InvalidRequest
    );
}

// Replacements compound, so a growing one applied repeatedly reaches terabytes
// long before a check on the final result could reject it. The bound has to be
// enforced before each allocation, not after the last one.
#[tokio::test]
async fn a_compounding_replacement_is_rejected_before_it_allocates() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a").await.unwrap());
    // Each doubles the content; unchecked, 40 of them ask for about a TiB.
    let doubling: Vec<EditSpec> = (0..40).map(|_| spec("a", "aa", true)).collect();
    assert_eq!(
        code(edit_many(&f, "f.txt", &hash, doubling).await),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "a",
        "a rejected edit must leave the file alone"
    );
}

// The result bound holds across the list as a whole: each replacement fits on
// its own, and together they do not.
#[tokio::test]
async fn a_result_over_the_cap_is_rejected() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a\nb\n").await.unwrap());
    let half = "x".repeat(MAX_TEXT_BYTES / 2 + 100);
    assert_eq!(
        code(
            edit_many(
                &f,
                "f.txt",
                &hash,
                vec![spec("a\n", &half, false), spec("b\n", &half, false)],
            )
            .await
        ),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "a\nb\n"
    );
}

// A file already changed when the edit arrives is caught by base_hash, before
// any work is done. The narrower case -- a write landing AFTER that check, while
// the replacement is being built -- is caught under the rename itself and is
// covered by fs_ops' own `write_tests`, since from out here it cannot be
// reached: this check fires first.
#[tokio::test]
async fn an_edit_against_an_already_changed_file_is_refused() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "original\n").await.unwrap());
    // Stand in for the concurrent writer: the content changes after the hash
    // the edit was stated against was taken.
    std::fs::write(f.ws.root.join("f.txt"), "written by something else\n").unwrap();

    let err = edit_many(
        &f,
        "f.txt",
        &hash,
        vec![spec("original\n", "edited\n", false)],
    )
    .await
    .expect_err("must not overwrite a newer file");
    assert_eq!(err.code, ErrorCode::StaleFile);
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "written by something else\n",
        "the concurrent write must survive"
    );
}

// Recovery synthesizes a commit for a marker whose file already holds the
// intended result -- right for a real crash between rename and commit, and the
// reason a mutation that was REFUSED after its marker was written must withdraw
// it. The withdrawal itself is driven through the real code path by fs_ops'
// `write_tests`, which can reach the branch this test cannot.
#[tokio::test]
async fn a_marker_left_by_a_crash_is_recovered_as_a_commit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let state = root.join("state");
    std::fs::write(root.join("f.txt"), "v1\n").unwrap();

    let op_id = {
        let store = OperationStore::new(state.clone()).unwrap();
        store
            .prepare_fs_record(
                "req-1",
                OperationKind::Edit,
                "f.txt",
                Some(hash_of("v1\n")),
                hash_of("v2\n"),
            )
            .unwrap()
    };
    // The rename landed; the commit did not.
    std::fs::write(root.join("f.txt"), "v2\n").unwrap();

    // Markers are reloaded only when a store opens, so recovery runs on a
    // reopened one, exactly as the next server would.
    let ws = Workspace::new(root, dir.path().join("scratch")).unwrap();
    let store = OperationStore::new(state).unwrap();
    store.recover(&ws).unwrap();
    assert!(
        store
            .history(None)
            .iter()
            .any(|r| r.operation_id() == op_id),
        "a genuine crash between rename and commit must still be recovered"
    );
}

// `gc` rewrites the operation log from the in-memory table, so anything missing
// there is erased. A prepared marker is not history but pending state -- the
// only thing that lets the next start work out what became of a mutation whose
// outcome is unknown -- so no `keep`, zero included, may take it.
#[tokio::test]
async fn pruning_never_removes_a_pending_marker() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let state = root.join("state");
    std::fs::write(root.join("f.txt"), "v1\n").unwrap();

    let pending = {
        let store = OperationStore::new(state.clone()).unwrap();
        // Something committed, for pruning to have work to do.
        let done = store
            .prepare_fs_record(
                "req-done",
                OperationKind::Edit,
                "old.txt",
                None,
                hash_of("x"),
            )
            .unwrap();
        store
            .commit_fs_record(
                &done,
                "req-done",
                OperationKind::Edit,
                "old.txt",
                None,
                hash_of("x"),
            )
            .unwrap();
        // And a mutation whose outcome is still unknown.
        let pending = store
            .prepare_fs_record(
                "req-pending",
                OperationKind::Edit,
                "f.txt",
                Some(hash_of("v1\n")),
                hash_of("v2\n"),
            )
            .unwrap();

        store.prune(0).unwrap();
        assert!(
            store.history(None).is_empty(),
            "history should have been pruned away"
        );
        pending
    };

    // The rename had landed; only the commit was missing. Recovery can still
    // tell, because the marker survived the pruning and the reopen.
    std::fs::write(root.join("f.txt"), "v2\n").unwrap();
    let ws = Workspace::new(root, dir.path().join("scratch")).unwrap();
    let reopened = OperationStore::new(state).unwrap();
    reopened.recover(&ws).unwrap();
    assert!(
        reopened
            .history(None)
            .iter()
            .any(|r| r.operation_id() == pending),
        "gc erased the marker recovery needed"
    );
}

// `keep` counts what a caller can go on to read. WAL bookkeeping is not that,
// so an internal marker must never take a retained slot from an operation the
// caller asked to keep.
#[tokio::test]
async fn pruning_counts_history_not_wal_bookkeeping() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let state = root.join("state");

    {
        let store = OperationStore::new(state.clone()).unwrap();
        let done = store
            .prepare_fs_record("req-done", OperationKind::Edit, "a.txt", None, hash_of("x"))
            .unwrap();
        store
            .commit_fs_record(
                &done,
                "req-done",
                OperationKind::Edit,
                "a.txt",
                None,
                hash_of("x"),
            )
            .unwrap();
        // A mutation that was decided against after its marker was written.
        let refused = store
            .prepare_fs_record(
                "req-refused",
                OperationKind::Edit,
                "b.txt",
                None,
                hash_of("y"),
            )
            .unwrap();
        store.abort_prepared(&refused).unwrap();
    }

    // Reopening is where the abort collapses onto the marker it supersedes.
    let store = OperationStore::new(state).unwrap();
    let stats = store.prune(1).unwrap();
    assert_eq!(stats.retained_operations, 1);
    let history = store.history(None);
    assert_eq!(
        history.len(),
        1,
        "the one operation worth keeping was pruned in favour of a marker"
    );
    assert_eq!(history[0].operation_id(), "op-1");
}

// Replacements that undo each other are a request for nothing, spelled across
// several entries -- refused like a single replacement from a value to itself.
// The WAL is the reason it cannot merely be allowed through: a record whose
// before and after hashes are equal is one recovery must read as "the rename
// never happened", so an edit that DID land would vanish from history and its
// request would be handed back for retry.
#[tokio::test]
async fn replacements_that_cancel_out_are_refused() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a\n").await.unwrap());
    let err = edit_many(
        &f,
        "f.txt",
        &hash,
        vec![spec("a\n", "b\n", false), spec("b\n", "a\n", false)],
    )
    .await
    .expect_err("a net-zero edit must be refused");
    assert_eq!(err.code, ErrorCode::InvalidRequest);
    assert!(
        err.message.contains("unchanged"),
        "unhelpful message: {}",
        err.message
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "a\n"
    );

    // A list that reaches a different result is still fine, including one that
    // passes through the original content on the way.
    edit_many(
        &f,
        "f.txt",
        &hash,
        vec![
            spec("a\n", "b\n", false),
            spec("b\n", "a\n", false),
            spec("a\n", "c\n", false),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "c\n"
    );
}

// Handler tasks append to the logs concurrently -- request-table and exec
// records go in without the mutation guard -- so the append path has to be safe
// against itself, not merely against another server process.
#[tokio::test]
async fn concurrent_appends_all_survive() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store = OperationStore::new(state.clone()).unwrap();

    let writers: Vec<_> = (0..32)
        .map(|i| {
            let store = store.clone();
            std::thread::spawn(move || {
                store
                    .prepare_fs_record(
                        &format!("req-{i}"),
                        OperationKind::Edit,
                        &format!("f{i}.txt"),
                        None,
                        hash_of(&format!("v{i}")),
                    )
                    .unwrap()
            })
        })
        .collect();
    let ids: Vec<String> = writers.into_iter().map(|w| w.join().unwrap()).collect();

    let text = std::fs::read_to_string(state.join("operations.jsonl")).unwrap();
    assert_eq!(text.lines().count(), ids.len(), "a line was lost");
    for line in text.lines() {
        serde_json::from_str::<serde_json::Value>(line)
            .unwrap_or_else(|e| panic!("line does not parse: {e}: {line}"));
    }
    let unique: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "operation ids collided");

    // And the whole thing still loads.
    drop(store);
    OperationStore::new(state).expect("log must be loadable");
}

// Pruning replaces the logs wholesale, which is an append's opposite number: a
// record appended while the replacement is in flight would be thrown away after
// its handler had already been told the operation succeeded. A smoke test, not
// a proof -- it cannot pin the two threads to the interleaving that matters
// without a hook in the write path -- so what it guards is that the two are
// safe to run together at all, and that nothing deadlocks now they share a
// lock.
#[tokio::test]
async fn a_concurrent_gc_does_not_erase_appended_records() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store = OperationStore::new(state.clone()).unwrap();

    // Enough history that pruning has real work to do.
    for i in 0..200 {
        let id = store
            .prepare_fs_record(
                &format!("req-old-{i}"),
                OperationKind::Edit,
                "old.txt",
                None,
                hash_of(&format!("v{i}")),
            )
            .unwrap();
        store
            .commit_fs_record(
                &id,
                &format!("req-old-{i}"),
                OperationKind::Edit,
                "old.txt",
                None,
                hash_of(&format!("v{i}")),
            )
            .unwrap();
    }

    let pruner = {
        let store = store.clone();
        std::thread::spawn(move || store.prune(50).unwrap())
    };
    let appender = {
        let store = store.clone();
        std::thread::spawn(move || {
            let id = store
                .prepare_fs_record(
                    "req-new",
                    OperationKind::Edit,
                    "new.txt",
                    None,
                    hash_of("new"),
                )
                .unwrap();
            store
                .commit_fs_record(
                    &id,
                    "req-new",
                    OperationKind::Edit,
                    "new.txt",
                    None,
                    hash_of("new"),
                )
                .unwrap();
            id
        })
    };
    pruner.join().unwrap();
    let appended = appender.join().unwrap();

    // In whichever order they happened to run, a record reported as committed
    // must be in the log the next server reads.
    drop(store);
    let reopened = OperationStore::new(state).unwrap();
    assert!(
        reopened
            .history(None)
            .iter()
            .any(|r| r.operation_id() == appended),
        "gc replaced the log under a record it had already accepted"
    );
}

// Two callers racing on one request_id: exactly one may claim it, and only one
// InProgress line may reach the log. A losing claimant that wrote its own line
// would have it land after the winner's terminal one, and that is the line a
// restart reads -- reviving a finished request as unfinished and letting it
// execute a second time.
#[tokio::test]
async fn racing_claims_write_one_request_line() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store = OperationStore::new(state.clone()).unwrap();

    let claimants: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || store.claim_request("same-id", "edit").unwrap())
        })
        .collect();
    let outcomes: Vec<_> = claimants.into_iter().map(|c| c.join().unwrap()).collect();
    assert_eq!(
        outcomes.iter().filter(|o| o.is_none()).count(),
        1,
        "exactly one caller may win the claim"
    );

    store
        .remember_error(
            "same-id",
            remote_workspace_protocol::ProtocolError::new(ErrorCode::NoMatch, "no match"),
        )
        .unwrap();

    // The terminal line has to be the last word on this request.
    let text = std::fs::read_to_string(state.join("requests.jsonl")).unwrap();
    assert_eq!(
        text.lines().filter(|l| l.contains("inprogress")).count(),
        1,
        "a losing claimant wrote an InProgress line: {text}"
    );
    drop(store);
    let reopened = OperationStore::new(state).unwrap();
    let status = reopened.status_for_request("same-id");
    assert_eq!(
        status.status,
        remote_workspace_protocol::RequestStatus::Error
    );
}
