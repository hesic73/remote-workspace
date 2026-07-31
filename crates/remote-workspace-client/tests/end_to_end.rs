use std::path::{Path, PathBuf};

use remote_workspace_client::{Client, Transport};
use remote_workspace_protocol::{ExecTermination, ListKind};

/// Path to the built remote-workspace-server binary.
fn server_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // workspace target dir is two levels up from the crate
    p.push("../../target/debug/remote-workspace-server");
    p.canonicalize().unwrap_or(p)
}

struct LocalServerTransport {
    argv: Vec<String>,
}

impl Transport for LocalServerTransport {
    fn spawn(
        &mut self,
    ) -> std::io::Result<(
        tokio::process::Child,
        tokio::process::ChildStdin,
        tokio::process::ChildStdout,
    )> {
        use std::process::Stdio;
        use tokio::process::Command;
        let mut cmd = Command::new(&self.argv[0]);
        cmd.args(&self.argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok((child, stdin, stdout))
    }
}

async fn make_client(root: &Path) -> Client {
    // Ensure the server binary exists; build it if missing.
    let bin = server_bin();
    if !bin.exists() {
        panic!(
            "server binary not found at {:?}; run `cargo build -p remote-workspace-server` first",
            bin
        );
    }
    let argv = vec![
        bin.to_string_lossy().into_owned(),
        "--root".into(),
        root.to_string_lossy().into_owned(),
        // Keep server state inside the test tempdir instead of the real HOME.
        "--state-base".into(),
        root.join(".remote-workspace-test")
            .to_string_lossy()
            .into_owned(),
    ];
    Client::connect(LocalServerTransport { argv }, None)
        .await
        .expect("connect")
}

#[tokio::test]
async fn end_to_end_create_read_list_stat() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;

    let w = client.create("src/main.py", "print('hi')\n").await.unwrap();
    assert!(w.operation_id.starts_with("op-"));

    let r = client.read("src/main.py", None, None).await.unwrap();
    assert_eq!(r.content, "print('hi')\n");
    assert!(!r.truncated);

    let result = client.list("src", None, None).await.unwrap();
    assert!(result
        .entries
        .iter()
        .any(|e| e.name == "main.py" && e.kind == ListKind::File));

    let s = client.stat("src/main.py").await.unwrap();
    assert!(s.size > 0);
}

#[tokio::test]
async fn end_to_end_edit_with_base_hash() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;

    let w = client.create("f.txt", "a\nb\nc\n").await.unwrap();
    let edited = client
        .edit("f.txt", &w.new_hash, "b\n", "BEE\n", false)
        .await
        .unwrap();
    assert_ne!(edited.new_hash, w.new_hash);

    let r = client.read("f.txt", None, None).await.unwrap();
    assert_eq!(r.content, "a\nBEE\nc\n");
}

#[tokio::test]
async fn end_to_end_stale_hash_errors() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;
    let _ = client.create("f.txt", "v1").await.unwrap();
    // Wrong base hash.
    let err = client
        .edit("f.txt", "sha256:deadbeef", "v1", "v2", false)
        .await
        .unwrap_err();
    match err {
        remote_workspace_client::ClientError::Server(e) => {
            assert_eq!(e.code, remote_workspace_protocol::ErrorCode::StaleFile);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn end_to_end_exec_returns_bounded_result() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;
    let result = client
        .exec(
            vec![
                "sh".into(),
                "-c".into(),
                "echo hello; echo err >&2; exit 3".into(),
            ],
            None,
            None,
            Some(10000),
        )
        .await
        .unwrap();
    assert_eq!(result.termination, ExecTermination::Exited { code: 3 });
    assert!(result.stdout.prefix.contains("hello"));
    assert!(result.stderr.prefix.contains("err"));
}

#[tokio::test]
async fn end_to_end_history_records_each_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;
    let w = client.create("u.txt", "first\n").await.unwrap();
    let edit = client
        .edit("u.txt", &w.new_hash, "first\n", "second\n", false)
        .await
        .unwrap();

    let history = client.history(None).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].operation_id(), edit.operation_id);
    let r = client.read("u.txt", None, None).await.unwrap();
    assert_eq!(r.content, "second\n");
}

#[tokio::test]
async fn end_to_end_status_query() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;
    // Unknown.
    let s = client.request_status("never-existed").await.unwrap();
    assert_eq!(s.status, remote_workspace_protocol::RequestStatus::Unknown);

    // Execute then query.
    let w = client.create("q.txt", "q").await.unwrap();
    let s = client.request_status("__noop__").await.unwrap();
    let _ = s;
    // We don't know the request_id the client generated internally, but we can
    // still check operation_get works.
    let d = client.operation_get(&w.operation_id).await.unwrap();
    assert_eq!(d.record.operation_id(), w.operation_id);
}

// F3: client must not hang when the connection closes mid-request.
#[tokio::test]
async fn client_returns_closed_when_server_dies() {
    // A transport whose process exits immediately (stdout closes -> EOF). The
    // client must surface an error, not block forever on its reply channel.
    struct DeadTransport;
    impl Transport for DeadTransport {
        fn spawn(
            &mut self,
        ) -> std::io::Result<(
            tokio::process::Child,
            tokio::process::ChildStdin,
            tokio::process::ChildStdout,
        )> {
            use std::process::Stdio;
            use tokio::process::Command;
            let mut cmd = Command::new("false");
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true);
            let mut child = cmd.spawn()?;
            let stdin = child.stdin.take().expect("piped stdin");
            let stdout = child.stdout.take().expect("piped stdout");
            Ok((child, stdin, stdout))
        }
    }

    let client = Client::connect(DeadTransport, None).await.unwrap();
    // Give the dead process a moment to exit so the reader observes EOF.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // This request must NOT hang; it should return an error quickly.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.read("any.txt", None, None),
    )
    .await;
    match result {
        Ok(Err(_)) => { /* good: surfaced an error */ }
        Ok(Ok(_)) => panic!("should not have succeeded against a dead server"),
        Err(_) => panic!("client hung instead of returning Closed"),
    }
}

// F3: exec must not hang when the connection closes mid-stream.
#[tokio::test]
async fn client_exec_returns_closed_when_server_dies() {
    struct DeadTransport;
    impl Transport for DeadTransport {
        fn spawn(
            &mut self,
        ) -> std::io::Result<(
            tokio::process::Child,
            tokio::process::ChildStdin,
            tokio::process::ChildStdout,
        )> {
            use std::process::Stdio;
            use tokio::process::Command;
            let mut cmd = Command::new("false");
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true);
            let mut child = cmd.spawn()?;
            let stdin = child.stdin.take().expect("piped stdin");
            let stdout = child.stdout.take().expect("piped stdout");
            Ok((child, stdin, stdout))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let client = Client::connect(DeadTransport, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.exec(vec!["sleep".into(), "10".into()], None, None, None),
    )
    .await;
    match result {
        Ok(Err(_)) => { /* good */ }
        Ok(Ok(_)) => panic!("exec should not have succeeded against a dead server"),
        Err(_) => panic!("exec hung instead of returning Closed"),
    }
    let _ = dir; // suppress unused
}

#[tokio::test]
async fn end_to_end_gc_prunes_history() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;

    let mut hash = client.create("a.txt", "1").await.unwrap().new_hash;
    for (old, new) in [("1", "2"), ("2", "3")] {
        hash = client
            .edit("a.txt", &hash, old, new, false)
            .await
            .unwrap()
            .new_hash;
    }
    let g = client.gc(Some(1)).await.unwrap();
    assert_eq!(g.removed_operations, 2);
    assert_eq!(g.retained_operations, 1);

    let ops = client.history(None).await.unwrap();
    assert_eq!(ops.len(), 1);
}

#[tokio::test]
async fn end_to_end_delete_is_permanent() {
    let dir = tempfile::tempdir().unwrap();
    let client = make_client(dir.path()).await;

    client.create("d.txt", "precious").await.unwrap();
    let del = client.delete("d.txt").await.unwrap();
    assert!(!dir.path().join("d.txt").exists());
    assert_eq!(del.new_hash, "sha256:");

    let ops = client.history(None).await.unwrap();
    assert_eq!(ops.last().unwrap().operation_id(), del.operation_id);
    assert!(!dir.path().join("d.txt").exists());
}

// Drive the REAL `remote-workspace` CLI binary through a stub `ssh` on PATH. The
// stub mimics real ssh (joins trailing args with spaces, re-parses through a
// shell), so this exercises the quoted remote-command assembly end to end --
// with spaces in both the workspace root and the state dir.
#[tokio::test]
async fn cli_over_fake_ssh_quotes_paths_with_spaces() {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;

    let stub = tempfile::tempdir().unwrap();
    let ssh_path = stub.path().join("ssh");
    // Skip `-o opt` pairs and the host, then run the remaining args joined
    // with spaces through a shell -- the same thing real ssh does remotely.
    std::fs::write(
        &ssh_path,
        "#!/bin/sh\nwhile [ \"$1\" = \"-o\" ]; do shift 2; done\nshift\nexec sh -c \"$*\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&ssh_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path_env = format!(
        "{}:{}",
        stub.path().display(),
        std::env::var("PATH").unwrap()
    );

    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("my project");
    std::fs::create_dir(&root).unwrap();
    let state = base.path().join("st ate");

    let cli = env!("CARGO_BIN_EXE_remote-workspace");
    let srv = server_bin();
    let common = [
        "--host".to_string(),
        "fakehost".to_string(),
        "--remote-bin".to_string(),
        srv.to_string_lossy().into_owned(),
        "--root".to_string(),
        root.to_string_lossy().into_owned(),
        "--state-base".to_string(),
        state.to_string_lossy().into_owned(),
    ];

    let mut child = std::process::Command::new(cli)
        .args(&common)
        .args(["create", "f.txt"])
        .env("PATH", &path_env)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"hello over ssh")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "write over fake ssh failed: {out:?}");
    assert_eq!(
        std::fs::read_to_string(root.join("f.txt")).unwrap(),
        "hello over ssh"
    );

    let out = std::process::Command::new(cli)
        .args(&common)
        .args(["cat", "f.txt"])
        .env("PATH", &path_env)
        .output()
        .unwrap();
    assert!(out.status.success(), "cat over fake ssh failed: {out:?}");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "hello over ssh");
    // State landed under the (space-containing) base dir, keyed per root, not
    // in the workspace.
    let keyed: Vec<_> = std::fs::read_dir(state.join("state")).unwrap().collect();
    assert_eq!(keyed.len(), 1);
    assert!(keyed[0]
        .as_ref()
        .unwrap()
        .path()
        .join("operations.jsonl")
        .exists());
    assert!(!root.join(".remote-workspace").exists());
}

// --state-base redirects the state base while keeping per-root keying.
#[tokio::test]
async fn cli_state_base_redirects_state_location() {
    let base = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let state_base = base.path().join("alt-state");
    let cli = env!("CARGO_BIN_EXE_remote-workspace");
    let srv = server_bin();

    let out = std::process::Command::new(cli)
        .args([
            "--local",
            "--remote-bin",
            &srv.to_string_lossy(),
            "--root",
            &root.path().to_string_lossy(),
            "--state-base",
            &state_base.to_string_lossy(),
            "exec",
            "--",
            "true",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "exec failed: {out:?}");

    // State landed under <base>/state/<name>-<hash>, workspace untouched.
    let state_root = state_base.join("state");
    let entries: Vec<_> = std::fs::read_dir(&state_root).unwrap().collect();
    assert_eq!(entries.len(), 1, "exactly one per-root state dir");
    let keyed = entries[0].as_ref().unwrap().path();
    let root_name = root.path().file_name().unwrap().to_string_lossy();
    assert!(keyed
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with(&*root_name));
    assert!(keyed.join("operations.jsonl").exists());
    assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
}
