use remote_workspace_protocol::{
    EditSpec, ErrorCode, FsOperationRecord, OperationKind, ProtocolError, ResultBody,
};
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
    fs_ops::edit(
        &f.ws, &f.store, &guard, "req-edit", path, base_hash, &edits, None,
    )
}

async fn replace(
    f: &Fixture,
    path: &str,
    base_hash: &str,
    content: &str,
) -> Result<ResultBody, ProtocolError> {
    let guard = f.store.write_guard().await;
    fs_ops::edit(
        &f.ws,
        &f.store,
        &guard,
        "req-replace",
        path,
        base_hash,
        &[],
        Some(content),
    )
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
    let f = fixture();
    create(&f, "a.txt", "one").await.unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("a.txt")).unwrap(),
        "one"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(f.ws.root.join("a.txt"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o644);
    }
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
    edit(&f, "f.txt", &hash, "fn b2() {}\n", "", false)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "fn a() {}\n"
    );
}

#[tokio::test]
async fn edit_replace_content_atomic_stale_unchanged_and_exclusivity() {
    let f = fixture();
    let h1 = new_hash(create(&f, "f.txt", "first\n").await.unwrap());

    // Mutual exclusivity: neither provided or both provided.
    let guard = f.store.write_guard().await;
    assert_eq!(
        code(fs_ops::edit(
            &f.ws,
            &f.store,
            &guard,
            "r0",
            "f.txt",
            &h1,
            &[],
            None
        )),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        code(fs_ops::edit(
            &f.ws,
            &f.store,
            &guard,
            "r1",
            "f.txt",
            &h1,
            &[spec("first\n", "second\n", false)],
            Some("second\n"),
        )),
        ErrorCode::InvalidRequest
    );
    drop(guard);

    // Unchanged content rejected.
    let unchanged = replace(&f, "f.txt", &h1, "first\n")
        .await
        .expect_err("unchanged replace_content must be refused");
    assert_eq!(unchanged.code, ErrorCode::InvalidRequest);
    assert!(unchanged.message.contains("unchanged"));

    // Valid atomic whole-file replace.
    let h2 = match replace(&f, "f.txt", &h1, "replaced completely\n")
        .await
        .unwrap()
    {
        ResultBody::Mutation(m) => {
            assert_eq!(m.old_hash.as_deref(), Some(h1.as_str()));
            assert_eq!(m.new_hash, hash_of("replaced completely\n"));
            m.new_hash
        }
        other => panic!("expected mutation, got {other:?}"),
    };
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "replaced completely\n"
    );

    // Stale hash rejected with expected + actual hashes.
    let stale = replace(&f, "f.txt", &h1, "third\n")
        .await
        .expect_err("stale base_hash must fail");
    assert_eq!(stale.code, ErrorCode::StaleFile);
    assert_eq!(stale.expected_hash.as_deref(), Some(h1.as_str()));
    assert_eq!(stale.actual_hash.as_deref(), Some(h2.as_str()));
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "replaced completely\n"
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
    assert_eq!(
        code(replace(&f, "f.txt", &hash, &huge).await),
        ErrorCode::InvalidRequest
    );
    let guard = f.store.write_guard().await;
    let r = fs_ops::create(&f.ws, &f.store, &guard, "req-huge", "huge.txt", &huge);
    drop(guard);
    assert_eq!(r.unwrap_err().code, ErrorCode::InvalidRequest);
}

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

#[tokio::test]
async fn a_failure_anywhere_applies_none_of_the_edits() {
    let f = fixture();
    let original = "a\nb\nc\n";
    let hash = new_hash(create(&f, "f.txt", original).await.unwrap());

    let cases: Vec<(Vec<EditSpec>, ErrorCode)> = vec![
        (
            vec![
                spec("a\n", "A\n", false),
                spec("b\n", "B\n", false),
                spec("zzz", "x", false),
            ],
            ErrorCode::NoMatch,
        ),
        (
            vec![
                spec("b\n", "a\n", false),
                spec("c\n", "a\n", false),
                spec("a\n", "q\n", false),
            ],
            ErrorCode::AmbiguousMatch,
        ),
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

#[tokio::test]
async fn several_replacements_are_a_single_operation() {
    let f = fixture();
    let created = create(&f, "f.txt", "1\n2\n3\n").await.unwrap();
    let hash = new_hash(created);
    let before = f.store.history(None).unwrap().len();
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
        f.store.history(None).unwrap().len() - before,
        1,
        "one edit request must append exactly one operation record"
    );
}

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
        ErrorCode::InvalidRequest
    );
    let too_many = (0..MAX_EDITS + 1)
        .map(|i| spec(&format!("x{i}"), &format!("y{i}"), false))
        .collect();
    assert_eq!(
        code(edit_many(&f, "f.txt", &hash, too_many).await),
        ErrorCode::InvalidRequest
    );
}

#[tokio::test]
async fn a_compounding_replacement_is_rejected_before_it_allocates() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "a").await.unwrap());
    let doubling: Vec<EditSpec> = (0..40).map(|_| spec("a", "aa", true)).collect();
    assert_eq!(
        code(edit_many(&f, "f.txt", &hash, doubling).await),
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "a"
    );
}

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

#[tokio::test]
async fn an_edit_against_an_already_changed_file_is_refused() {
    let f = fixture();
    let hash = new_hash(create(&f, "f.txt", "original\n").await.unwrap());
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
        "written by something else\n"
    );
}

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
    assert!(err.message.contains("unchanged"));
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("f.txt")).unwrap(),
        "a\n"
    );

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

#[tokio::test]
async fn concurrent_appends_across_threads_and_sessions_all_survive() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store_a = OperationStore::new(state.clone()).unwrap();
    let store_b = OperationStore::new(state.clone()).unwrap();

    let writers: Vec<_> = (0..32)
        .map(|i| {
            let store = if i % 2 == 0 {
                store_a.clone()
            } else {
                store_b.clone()
            };
            std::thread::spawn(move || {
                let id = store.next_operation_id();
                store
                    .append_fs_record(FsOperationRecord {
                        operation_id: id.clone(),
                        request_id: format!("req-{i}"),
                        kind: OperationKind::Edit,
                        path: format!("f{i}.txt"),
                        before_hash: None,
                        after_hash: hash_of(&format!("v{i}")),
                        timestamp_ms: 100 + i as u64,
                    })
                    .unwrap();
                id
            })
        })
        .collect();
    let ids: Vec<String> = writers.into_iter().map(|w| w.join().unwrap()).collect();

    let unique: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "operation ids collided");

    let history = store_a.history(None).unwrap();
    assert_eq!(history.len(), ids.len());

    drop(store_a);
    drop(store_b);
    let reopened = OperationStore::new(state).unwrap();
    assert_eq!(reopened.history(None).unwrap().len(), ids.len());
}

#[tokio::test]
async fn a_concurrent_gc_does_not_erase_live_appended_records() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");

    // Populate a closed shard so prune has real work to do.
    {
        let closed = OperationStore::new(state.clone()).unwrap();
        for i in 0..100 {
            let id = closed.next_operation_id();
            closed
                .append_fs_record(FsOperationRecord {
                    operation_id: id,
                    request_id: format!("req-old-{i}"),
                    kind: OperationKind::Edit,
                    path: "old.txt".into(),
                    before_hash: None,
                    after_hash: hash_of(&format!("v{i}")),
                    timestamp_ms: i as u64,
                })
                .unwrap();
        }
    }

    let live = OperationStore::new(state.clone()).unwrap();
    let pruner = {
        let store = live.clone();
        std::thread::spawn(move || store.prune(50).unwrap())
    };
    let appender = {
        let store = live.clone();
        std::thread::spawn(move || {
            let id = store.next_operation_id();
            store
                .append_fs_record(FsOperationRecord {
                    operation_id: id.clone(),
                    request_id: "req-new".into(),
                    kind: OperationKind::Edit,
                    path: "new.txt".into(),
                    before_hash: None,
                    after_hash: hash_of("new"),
                    timestamp_ms: 10_000,
                })
                .unwrap();
            id
        })
    };
    pruner.join().unwrap();
    let appended = appender.join().unwrap();

    drop(live);
    let reopened = OperationStore::new(state).unwrap();
    assert!(
        reopened
            .history(None)
            .unwrap()
            .iter()
            .any(|r| r.operation_id() == appended),
        "gc erased a record appended to a live shard"
    );
}

#[tokio::test]
async fn racing_in_session_claims_allow_single_winner() {
    let dir = tempfile::tempdir().unwrap();
    let store = OperationStore::new(dir.path().join("state")).unwrap();

    let claimants: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || store.claim_request("same-id").is_none())
        })
        .collect();
    let wins = claimants
        .into_iter()
        .map(|c| c.join().unwrap())
        .filter(|&won| won)
        .count();
    assert_eq!(wins, 1, "exactly one caller may win the claim");
}

#[cfg(unix)]
async fn delete(f: &Fixture, path: &str) -> Result<ResultBody, ProtocolError> {
    let guard = f.store.write_guard().await;
    fs_ops::delete(&f.ws, &f.store, &guard, "req-delete", path)
}

fn stat_hash(f: &Fixture, path: &str) -> Option<String> {
    match fs_ops::stat(&f.ws, path).unwrap() {
        ResultBody::Stat { stat } => stat.hash,
        other => panic!("expected stat result, got {other:?}"),
    }
}

#[tokio::test]
#[cfg(unix)]
async fn deleting_a_symlink_is_refused_and_leaves_its_target() {
    let f = fixture();
    create(&f, "real.txt", "keep me\n").await.unwrap();
    std::os::unix::fs::symlink("real.txt", f.ws.root.join("link.txt")).unwrap();

    assert_eq!(code(delete(&f, "link.txt").await), ErrorCode::NotAFile);
    assert_eq!(
        std::fs::read_to_string(f.ws.root.join("real.txt")).unwrap(),
        "keep me\n"
    );
    assert!(std::fs::symlink_metadata(f.ws.root.join("link.txt"))
        .unwrap()
        .file_type()
        .is_symlink());
    delete(&f, "real.txt").await.unwrap();
}

#[tokio::test]
#[cfg(unix)]
async fn non_regular_files_are_refused_before_they_are_opened() {
    let f = fixture();
    let fifo = std::ffi::CString::new(f.ws.root.join("pipe").to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o644) }, 0);

    assert_eq!(
        code(fs_ops::read(
            &f.ws,
            "pipe",
            remote_workspace_protocol::ReadOptions::default()
        )),
        ErrorCode::NotAFile
    );
    assert_eq!(
        code(edit(&f, "pipe", &hash_of(""), "a", "b", false).await),
        ErrorCode::NotAFile
    );
    assert_eq!(code(delete(&f, "pipe").await), ErrorCode::NotAFile);
    create(&f, "after.txt", "the guard is free\n")
        .await
        .unwrap();
}

#[tokio::test]
async fn stat_hashes_only_files_edit_could_accept() {
    let f = fixture();
    std::fs::write(f.ws.root.join("small.txt"), "hi").unwrap();
    std::fs::write(f.ws.root.join("big.log"), vec![b'x'; MAX_TEXT_BYTES + 1]).unwrap();
    assert_eq!(stat_hash(&f, "small.txt"), Some(hash_of("hi")));
    assert_eq!(stat_hash(&f, "big.log"), None);
}

#[tokio::test]
#[cfg(unix)]
async fn stat_reports_an_unreadable_file_instead_of_failing() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture();
    let path = f.ws.root.join("secret.txt");
    std::fs::write(&path, "hi").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    match fs_ops::stat(&f.ws, "secret.txt").unwrap() {
        ResultBody::Stat { stat } => {
            assert_eq!(stat.hash, None);
            assert!(!stat.mode.unwrap().readable);
        }
        other => panic!("expected stat result, got {other:?}"),
    }
}
