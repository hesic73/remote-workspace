#![cfg(unix)]

use std::path::{Path, PathBuf};

use remote_workspace_client::{Client, EditSpec, Transport};
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
    fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
        spawn_piped(&self.argv)
    }
}

/// The argv spawned the way `ArgvTransport` does it, stderr included, so the
/// tests exercise the same pipe setup the real client uses.
fn spawn_piped(argv: &[String]) -> std::io::Result<remote_workspace_client::Spawned> {
    use std::process::Stdio;
    use tokio::process::Command;
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take();
    Ok(remote_workspace_client::Spawned {
        child,
        stdin,
        stdout,
        stderr,
    })
}

/// A single replacement, which is what most of these tests exercise.
fn spec(old_text: &str, new_text: &str) -> EditSpec {
    EditSpec {
        old_text: old_text.into(),
        new_text: new_text.into(),
        replace_all: false,
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
        .edit("f.txt", &w.new_hash, vec![spec("b\n", "BEE\n")])
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
    // A real hash of some other content: the file is genuinely not what the
    // caller last saw.
    let stale = format!("sha256:{}", "de".repeat(32));
    let err = client
        .edit("f.txt", &stale, vec![spec("v1", "v2")])
        .await
        .unwrap_err();
    match err {
        remote_workspace_client::ClientError::Server(e) => {
            assert_eq!(e.code, remote_workspace_protocol::ErrorCode::StaleFile);
            // Both hashes must reach the caller, not just the structured
            // fields: it is the only way to tell an outdated base_hash of
            // one's own from a file someone else changed.
            let text = e.to_string();
            assert!(text.contains(&stale), "stale error lost base_hash: {text}");
            assert!(
                text.contains("current sha256:"),
                "stale error lost the file's current hash: {text}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    // A value that was never a hash is a bad request, not a stale file: the
    // caller must fix the argument, not re-read and retry.
    for bogus in ["auto", "sha256:REPLACE", "sha256:deadbeef"] {
        let err = client
            .edit("f.txt", bogus, vec![spec("v1", "v2")])
            .await
            .unwrap_err();
        match err {
            remote_workspace_client::ClientError::Server(e) => {
                assert_eq!(
                    e.code,
                    remote_workspace_protocol::ErrorCode::InvalidRequest,
                    "base_hash {bogus:?}"
                );
            }
            other => panic!("unexpected for {bogus:?}: {other:?}"),
        }
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
        .edit("u.txt", &w.new_hash, vec![spec("first\n", "second\n")])
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
        fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
            spawn_piped(&["false".to_string()])
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
        fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
            spawn_piped(&["false".to_string()])
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
            .edit("a.txt", &hash, vec![spec(old, new)])
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

// A server that refuses to start explains itself on stderr and exits. Without
// that text the client can only report the symptom ("server closed
// connection"), which is what made a locked workspace undiagnosable.
#[tokio::test]
async fn closed_error_carries_what_the_remote_printed() {
    struct RefusingTransport;
    impl Transport for RefusingTransport {
        fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
            spawn_piped(&[
                "sh".to_string(),
                "-c".to_string(),
                "echo 'noise from a login shell' >&2; \
                 echo 'Error: state directory is locked by another remote-workspace-server (held by pid 4242)' >&2; \
                 exit 1"
                    .to_string(),
            ])
        }
    }

    let client = Client::connect(RefusingTransport, None).await.unwrap();
    let err = client.stat(".").await.unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("locked by another remote-workspace-server") && text.contains("4242"),
        "close error lost the remote's reason: {text}"
    );
}

// The same, with the far end given time to be thoroughly dead first, so the
// request fails on the write rather than on the missing reply. Both routes out
// have to carry the reason; only one of them is a "closed connection" by name.
#[tokio::test]
async fn a_request_written_to_a_dead_transport_still_reports_why() {
    struct RefusingTransport;
    impl Transport for RefusingTransport {
        fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
            spawn_piped(&[
                "sh".to_string(),
                "-c".to_string(),
                "echo 'Error: state directory is locked (held by pid 4242)' >&2; exit 1"
                    .to_string(),
            ])
        }
    }

    let client = Client::connect(RefusingTransport, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let text = client.stat(".").await.unwrap_err().to_string();
    assert!(
        text.contains("state directory is locked") && text.contains("4242"),
        "error lost the remote's reason: {text}"
    );
}

// Nothing printed, nothing invented: the message stays clean when the far end
// dies silently.
#[tokio::test]
async fn closed_error_stays_bare_when_the_remote_said_nothing() {
    struct SilentTransport;
    impl Transport for SilentTransport {
        fn spawn(&mut self) -> std::io::Result<remote_workspace_client::Spawned> {
            spawn_piped(&["false".to_string()])
        }
    }

    let client = Client::connect(SilentTransport, None).await.unwrap();
    let err = client.stat(".").await.unwrap_err();
    assert_eq!(err.to_string(), "server closed connection");
}
