use remote_workspace_protocol::{ErrorCode, ProtocolError, ResultBody};
use remote_workspace_server::fs_ops::{self, MAX_TEXT_BYTES};
use remote_workspace_server::store::OperationStore;
use remote_workspace_server::workspace::Workspace;

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

#[allow(clippy::too_many_arguments)]
async fn edit(
    f: &Fixture,
    path: &str,
    base_hash: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
) -> Result<ResultBody, ProtocolError> {
    let guard = f.store.write_guard().await;
    fs_ops::edit(
        &f.ws,
        &f.store,
        &guard,
        "req-edit",
        path,
        base_hash,
        old_text,
        new_text,
        replace_all,
    )
}

fn new_hash(r: ResultBody) -> String {
    match r {
        ResultBody::Mutation(m) => m.new_hash,
        other => panic!("expected mutation result, got {other:?}"),
    }
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
