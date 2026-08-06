use std::path::PathBuf;

use remote_workspace_protocol::*;
use remote_workspace_server::{Server, ServerOptions};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct Harness {
    req_tx: tokio::sync::mpsc::UnboundedSender<String>,
    msg_rx: tokio::sync::mpsc::UnboundedReceiver<ServerMessage>,
    server_task: tokio::task::JoinHandle<()>,
    /// Owned only when this harness created the tempdir. Restart tests pass an
    /// externally-owned root and leave this None so the dir survives drop.
    _root: Option<tempfile::TempDir>,
    root_path: PathBuf,
}

fn hash_of(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    format!("sha256:{}", hex::encode(h.finalize()))
}

async fn harness() -> Harness {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let log_dir = root_path.join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    harness_at_with_owned(Some(root), root_path, log_dir, None).await
}

async fn harness_with_config(config_text: Option<&str>) -> Harness {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let log_dir = root_path.join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    let config_path = match config_text {
        Some(text) => {
            let p = root_path.join("config.toml");
            std::fs::write(&p, text).unwrap();
            Some(p)
        }
        None => None,
    };
    harness_at_with_owned(Some(root), root_path, log_dir, config_path).await
}

/// Harness over an externally-owned root directory (used by restart tests).
async fn harness_at(root: &std::path::Path) -> Harness {
    let log_dir = root.join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    harness_at_with_owned(None, root.to_path_buf(), log_dir, None).await
}

/// Harness over an externally-owned root + explicit log dir.
async fn harness_at_with(
    root: &std::path::Path,
    log_dir: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
) -> Harness {
    std::fs::create_dir_all(&log_dir).unwrap();
    harness_at_with_owned(None, root.to_path_buf(), log_dir, config_path).await
}

async fn harness_at_with_owned(
    owned: Option<tempfile::TempDir>,
    root_path: PathBuf,
    log_dir: PathBuf,
    config_path: Option<PathBuf>,
) -> Harness {
    let server = Server::new(ServerOptions {
        root: root_path.clone(),
        state_dir: log_dir,
        config_path,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();

    let (client_tx, client_rx) = tokio::io::duplex(1 << 20);
    let (server_tx, server_rx) = tokio::io::duplex(1 << 20);

    let server_task = tokio::spawn(async move {
        let _ = server.run(client_rx, server_tx).await;
    });

    let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
    let mut reader = BufReader::new(server_rx);
    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(m) = serde_json::from_str::<ServerMessage>(trimmed) {
                let _ = msg_tx.send(m);
            }
        }
    });

    let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut write = client_tx;
    tokio::spawn(async move {
        while let Some(line) = req_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = write.flush().await;
        }
    });

    Harness {
        req_tx,
        msg_rx,
        server_task,
        _root: owned,
        root_path,
    }
}

impl Harness {
    /// Close the client side and wait for the server task to exit, releasing
    /// the state-directory lock. Restart tests must call this before opening a
    /// second harness on the same log dir.
    async fn shutdown(self) {
        let Harness {
            req_tx,
            server_task,
            ..
        } = self;
        drop(req_tx);
        let _ = server_task.await;
    }

    fn send(&self, req: &Request) {
        let mut line = serde_json::to_string(req).unwrap();
        line.push('\n');
        self.req_tx.send(line).unwrap();
    }

    async fn recv(&mut self) -> ServerMessage {
        self.msg_rx
            .recv()
            .await
            .expect("server closed before responding")
    }

    /// Collect all messages for a given request_id until a terminal Result or
    /// Error arrives.
    async fn recv_all_for(&mut self, request_id: &str) -> Vec<ServerMessage> {
        let mut out = Vec::new();
        loop {
            let m = self.recv().await;
            let terminal = matches!(
                &m,
                ServerMessage::Result { request_id: rid, .. }
                    | ServerMessage::Error { request_id: rid, .. }
                if rid == request_id
            );
            let belongs = match &m {
                ServerMessage::Result {
                    request_id: rid, ..
                }
                | ServerMessage::Error {
                    request_id: rid, ..
                } => rid == request_id,
            };
            if belongs {
                out.push(m);
            }
            if terminal {
                break;
            }
        }
        out
    }
}

fn req(id: &str, body: RequestBody) -> Request {
    Request {
        request_id: id.into(),
        body,
    }
}

#[tokio::test]
async fn create_then_read_roundtrip() {
    let mut h = harness().await;
    h.send(&req(
        "r1",
        RequestBody::Create {
            path: "hello.txt".into(),
            content: "hello world\n".into(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert_eq!(w.old_hash, None);
            assert_eq!(w.new_hash, hash_of("hello world\n"));
            assert!(w.operation_id.starts_with("op-"));
        }
        other => panic!("unexpected: {other:?}"),
    }

    h.send(&req(
        "r2",
        RequestBody::Read {
            path: "hello.txt".into(),
            offset: None,
            limit: None,
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
            assert_eq!(r.content, "hello world\n");
            assert_eq!(r.hash.as_deref(), Some(hash_of("hello world\n").as_str()));
            assert!(!r.truncated);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn list_and_stat() {
    let mut h = harness().await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "a.txt".into(),
            content: "aaa".into(),
        },
    ));
    let _ = h.recv().await;
    std::fs::create_dir_all(h.root_path.join("sub")).unwrap();
    std::fs::write(h.root_path.join("sub/b.txt"), "bbb").unwrap();

    h.send(&req(
        "l",
        RequestBody::List {
            path: ".".into(),
            offset: None,
            limit: None,
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::List(ListResult { entries, .. }),
            ..
        } => {
            let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"a.txt"));
            assert!(names.contains(&"sub"));
            // .remote-workspace must be hidden.
            assert!(!names.contains(&".remote-workspace"));
        }
        other => panic!("unexpected: {other:?}"),
    }

    h.send(&req(
        "s",
        RequestBody::Stat {
            path: "a.txt".into(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Stat { stat },
            ..
        } => {
            assert_eq!(stat.size, 3);
            assert_eq!(stat.kind, ListKind::File);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn list_is_paginated_and_bounded() {
    let mut h = harness().await;
    for name in ["a", "b", "c"] {
        std::fs::write(h.root_path.join(name), name).unwrap();
    }
    h.send(&req(
        "l1",
        RequestBody::List {
            path: ".".into(),
            offset: None,
            limit: Some(2),
        },
    ));
    let next = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::List(result),
            ..
        } => {
            assert_eq!(result.entries.len(), 2);
            result.next_offset.expect("second page")
        }
        other => panic!("unexpected: {other:?}"),
    };
    h.send(&req(
        "l2",
        RequestBody::List {
            path: ".".into(),
            offset: Some(next),
            limit: Some(2),
        },
    ));
    assert!(matches!(
        h.recv().await,
        ServerMessage::Result {
            result: ResultBody::List(ListResult {
                entries,
                next_offset: None,
            }),
            ..
        } if entries.len() == 1
    ));
}

#[tokio::test]
async fn stale_hash_rejected() {
    let mut h = harness().await;
    h.send(&req(
        "w1",
        RequestBody::Create {
            path: "f.txt".into(),
            content: "v1".into(),
        },
    ));
    let hash = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => w.new_hash,
        other => panic!("unexpected: {other:?}"),
    };

    // Edit with correct base_hash should succeed.
    h.send(&req(
        "w2",
        RequestBody::Edit {
            path: "f.txt".into(),
            base_hash: hash.clone(),
            edits: vec![EditSpec {
                old_text: "v1".into(),
                new_text: "v2".into(),
                replace_all: false,
            }],
        },
    ));
    let _ = h.recv().await;

    // Now edit with the stale v1 base_hash should be rejected.
    h.send(&req(
        "w3",
        RequestBody::Edit {
            path: "f.txt".into(),
            base_hash: hash,
            edits: vec![EditSpec {
                old_text: "v2".into(),
                new_text: "v3".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Error {
            error:
                ProtocolError {
                    code: ErrorCode::StaleFile,
                    expected_hash,
                    actual_hash,
                    ..
                },
            ..
        } => {
            // expected was the stale v1 hash; actual is the v2 hash.
            assert!(expected_hash.is_some());
            assert!(actual_hash.is_some());
            assert_ne!(expected_hash, actual_hash);
        }
        other => panic!("expected StaleFile, got {other:?}"),
    }

    // File must be unchanged: still v2.
    let content = std::fs::read_to_string(h.root_path.join("f.txt")).unwrap();
    assert_eq!(content, "v2");
}

#[tokio::test]
async fn edit_atomic_all_or_nothing() {
    let mut h = harness().await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "p.txt".into(),
            content: "a\nb\nc\n".into(),
        },
    ));
    let hash = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => w.new_hash,
        other => panic!("unexpected: {other:?}"),
    };

    // Valid edit.
    h.send(&req(
        "pa",
        RequestBody::Edit {
            path: "p.txt".into(),
            base_hash: hash.clone(),
            edits: vec![EditSpec {
                old_text: "b".into(),
                new_text: "BEE".into(),
                replace_all: false,
            }],
        },
    ));
    let _ = h.recv().await;
    assert_eq!(
        std::fs::read_to_string(h.root_path.join("p.txt")).unwrap(),
        "a\nBEE\nc\n"
    );

    // Failing edit (old_text absent): file must remain unchanged.
    let before = std::fs::read_to_string(h.root_path.join("p.txt")).unwrap();
    let current_hash = hash_of(&before);
    h.send(&req(
        "pb",
        RequestBody::Edit {
            path: "p.txt".into(),
            base_hash: current_hash,
            edits: vec![EditSpec {
                old_text: "not-present".into(),
                new_text: "X".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        m,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::NoMatch,
                ..
            },
            ..
        }
    ));
    let after = std::fs::read_to_string(h.root_path.join("p.txt")).unwrap();
    assert_eq!(before, after, "edit failure must not mutate file");
    // And the failed edit must not have been recorded: only create + valid edit.
    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => assert_eq!(operations.len(), 2, "failed edit must not be recorded"),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn path_boundary_rejects_escape() {
    let mut h = harness().await;
    h.send(&req(
        "r",
        RequestBody::Read {
            path: "../etc/passwd".into(),
            offset: None,
            limit: None,
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        m,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::PathOutsideRoot,
                ..
            },
            ..
        }
    ));
}

#[tokio::test]
async fn exec_returns_stdout_and_exit() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["echo".into(), "hello-stdout".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    assert!(matches!(
        &msgs[0],
        ServerMessage::Result {
            result: ResultBody::Exec(ExecResult {
                termination: ExecTermination::Exited { code: 0 },
                stdout,
                ..
            }),
            ..
        } if stdout.prefix.contains("hello-stdout")
    ));
}

#[tokio::test]
async fn exec_nonzero_exit_and_stderr() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["sh".into(), "-c".into(), "echo err >&2; exit 7".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    assert!(matches!(
        &msgs[0],
        ServerMessage::Result {
            result: ResultBody::Exec(ExecResult {
                termination: ExecTermination::Exited { code: 7 },
                stderr,
                ..
            }),
            ..
        } if stderr.prefix.contains("err")
    ));
}

#[tokio::test]
#[cfg(unix)]
async fn exec_output_is_bounded_with_prefix_and_suffix() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "python3".into(),
                "-c".into(),
                "import sys; sys.stdout.write('A' * 5000 + 'B' * 13000)".into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(result),
            ..
        } => {
            assert_eq!(result.stdout.prefix.len(), 4 * 1024);
            assert_eq!(result.stdout.suffix.len(), 12 * 1024);
            assert!(result.stdout.prefix.bytes().all(|b| b == b'A'));
            assert!(result.stdout.suffix.bytes().all(|b| b == b'B'));
            assert_eq!(result.stdout.total_bytes, 18_000);
            assert_eq!(result.stdout.omitted_bytes, 18_000 - 16 * 1024);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn scratch_is_shared_by_exec_and_file_tools() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "sh".into(),
                "-c".into(),
                "printf scratch-data > \"$REMOTE_WORKSPACE_SCRATCH/job.log\"".into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let _ = h.recv_all_for("e").await;

    h.send(&req(
        "r",
        RequestBody::Read {
            path: "@scratch/job.log".into(),
            offset: None,
            limit: None,
        },
    ));
    assert!(matches!(
        h.recv().await,
        ServerMessage::Result {
            result: ResultBody::Read(ReadResult { content, .. }),
            ..
        } if content == "scratch-data"
    ));
}

#[tokio::test]
async fn exec_rejects_timeout_above_hard_limit() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["true".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(60 * 60 * 1000 + 1),
        },
    ));
    assert!(matches!(
        h.recv().await,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            },
            ..
        }
    ));
}

#[tokio::test]
async fn history_rejects_limit_above_hard_maximum() {
    let mut h = harness().await;
    h.send(&req("h", RequestBody::History { limit: Some(101) }));
    assert!(matches!(
        h.recv().await,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            },
            ..
        }
    ));
}

#[tokio::test]
async fn idempotent_replay_returns_same_result() {
    let mut h = harness().await;
    let body = RequestBody::Create {
        path: "idem.txt".into(),
        content: "x".into(),
    };
    h.send(&req("dup", body.clone()));
    let m1 = h.recv().await;
    // Replay the same request_id.
    h.send(&req("dup", body));
    let m2 = h.recv().await;
    // Both should be identical Results (same operation_id, same hashes).
    assert_eq!(
        serde_json::to_string(&m1).unwrap(),
        serde_json::to_string(&m2).unwrap(),
        "replay must return the stored result"
    );
}

#[tokio::test]
async fn request_status_unknown_and_done() {
    let mut h = harness().await;
    // Unknown request.
    h.send(&req(
        "s0",
        RequestBody::RequestStatus {
            target: "never".into(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::RequestStatus(r),
            ..
        } => {
            assert_eq!(r.status, RequestStatus::Unknown);
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Execute a request, then query its status.
    h.send(&req(
        "real",
        RequestBody::Create {
            path: "q.txt".into(),
            content: "q".into(),
        },
    ));
    let _ = h.recv().await;
    h.send(&req(
        "s1",
        RequestBody::RequestStatus {
            target: "real".into(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::RequestStatus(r),
            ..
        } => {
            assert_eq!(r.target, "real");
            assert_eq!(r.status, RequestStatus::Done);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn history_and_operation_get() {
    let mut h = harness().await;
    h.send(&req(
        "w1",
        RequestBody::Create {
            path: "h.txt".into(),
            content: "1".into(),
        },
    ));
    let op1 = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => w.operation_id,
        other => panic!("unexpected: {other:?}"),
    };
    h.send(&req(
        "w2",
        RequestBody::Edit {
            path: "h.txt".into(),
            base_hash: hash_of("1"),
            edits: vec![EditSpec {
                old_text: "1".into(),
                new_text: "2".into(),
                replace_all: false,
            }],
        },
    ));
    let op2 = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => w.operation_id,
        other => panic!("unexpected: {other:?}"),
    };

    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 2);
            assert_eq!(operations[0].operation_id(), op1);
            assert_eq!(operations[1].operation_id(), op2);
        }
        other => panic!("unexpected: {other:?}"),
    }

    h.send(&req(
        "og",
        RequestBody::OperationGet {
            operation_id: op2.clone(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Operation(OperationDetails { record }),
            ..
        } => match record {
            AnyOperationRecord::Fs(fs) => {
                assert_eq!(fs.operation_id, op2);
                assert_eq!(fs.kind, OperationKind::Edit);
            }
            other => panic!("expected fs record, got {other:?}"),
        },
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
#[cfg(unix)]
async fn profile_setup_runs_before_command() {
    let cfg = r#"
[profiles.greet]
setup = 'export GREETING=hi'
"#;
    let mut h = harness_with_config(Some(cfg)).await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["sh".into(), "-c".into(), "echo $GREETING".into()],
            cwd: None,
            profile: Some("greet".into()),
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    assert!(matches!(
        &msgs[0],
        ServerMessage::Result {
            result: ResultBody::Exec(ExecResult { stdout, .. }),
            ..
        } if stdout.prefix.contains("hi")
    ));
}

#[tokio::test]
async fn profile_unknown_rejected() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["true".into()],
            cwd: None,
            profile: Some("nope".into()),
            timeout_ms: Some(10000),
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        m,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            },
            ..
        }
    ));
}

// symlinked ancestor + nonexistent leaf (the critical escape).
#[tokio::test]
#[cfg(unix)]
async fn symlinked_ancestor_nonexistent_leaf_blocked() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
    let mut h = harness_at(root.path()).await;

    h.send(&req(
        "w",
        RequestBody::Create {
            path: "escape/new.txt".into(),
            content: "PWNED".into(),
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        m,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::PathOutsideRoot,
                ..
            },
            ..
        }
    ));
    assert!(!outside.path().join("new.txt").exists());
    // And nothing created under the symlink either.
    assert!(std::fs::read_dir(root.path().join("escape"))
        .map(|mut d| d.next().is_none())
        .unwrap_or(true));
}

// idempotency survives restart.
#[tokio::test]
async fn replay_after_restart_returns_stored_result() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();

    // First session: do a write with request_id "stable".
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "stable",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "v1".into(),
            },
        ));
        let _ = h.recv().await;
        h.shutdown().await;
    }
    // The workspace + log dir live on disk under root; shutdown released the
    // state-directory lock so a second server can take it.

    // Second session over the SAME log dir: replay "stable".
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req(
            "stable",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "v2".into(), // different content; must be ignored
            },
        ));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::Mutation(w),
                ..
            } => {
                // The replayed result must reflect the ORIGINAL create (v1), and
                // no new operation id should have been allocated.
                assert_eq!(w.new_hash, hash_of("v1"));
            }
            other => panic!("replay should return stored result, got {other:?}"),
        }
        // File content must still be v1.
        assert_eq!(
            std::fs::read_to_string(root.path().join("f.txt")).unwrap(),
            "v1"
        );
        // History (which reconciles the WAL) must report exactly ONE operation:
        // the replay did not execute and did not append a committed record.
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                assert_eq!(
                    operations.len(),
                    1,
                    "replay must not create a second record"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// request.status recovers prior status after restart.
#[tokio::test]
async fn request_status_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();

    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "real",
            RequestBody::Create {
                path: "q.txt".into(),
                content: "q".into(),
            },
        ));
        let _ = h.recv().await;
        h.shutdown().await;
    }
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req(
            "s",
            RequestBody::RequestStatus {
                target: "real".into(),
            },
        ));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::RequestStatus(r),
                ..
            } => {
                assert_eq!(r.target, "real");
                assert_eq!(r.status, RequestStatus::Done);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// concurrent duplicate request ids execute once.
#[tokio::test]
async fn concurrent_duplicate_request_runs_once() {
    let mut h = harness().await;
    // Fire two requests with the SAME id back to back, before the first
    // resolves. A create is fast, but sending both first guarantees they are
    // both in flight.
    let body = RequestBody::Create {
        path: "dup.txt".into(),
        content: "x".into(),
    };
    h.send(&req("same", body.clone()));
    h.send(&req("same", body));
    let m1 = h.recv().await;
    let m2 = h.recv().await;
    // Both responses must be identical, and only one operation recorded.
    assert_eq!(
        serde_json::to_string(&m1).unwrap(),
        serde_json::to_string(&m2).unwrap(),
    );
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(
                operations.len(),
                1,
                "duplicate concurrent request must run once"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// exec is recorded and retrievable via history and operation.get.
#[tokio::test]
async fn exec_recorded_in_history_and_operation_get() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["echo".into(), "recorded".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let op_id = {
        let msgs = h.recv_all_for("e").await;
        let mut id = None;
        for m in &msgs {
            if let ServerMessage::Result {
                result: ResultBody::Exec(ExecResult { operation_id, .. }),
                ..
            } = m
            {
                id = Some(operation_id.clone());
            }
        }
        id.expect("exit event")
    };

    // history must include the exec record.
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AnyOperationRecord::Exec(e) => {
                    assert_eq!(e.operation_id, op_id);
                    assert_eq!(e.argv, vec!["echo".to_string(), "recorded".to_string()]);
                    assert_eq!(e.termination, Some(ExecTermination::Exited { code: 0 }));
                    assert!(e.stdout.prefix.is_empty());
                    assert_eq!(e.stdout.omitted_bytes, e.stdout.total_bytes);
                }
                other => panic!("expected exec record, got {other:?}"),
            }
        }
        other => panic!("unexpected: {other:?}"),
    }

    // operation.get must find the exec.
    h.send(&req(
        "og",
        RequestBody::OperationGet {
            operation_id: op_id.clone(),
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        &m,
        ServerMessage::Result {
            result: ResultBody::Operation(OperationDetails {
                record: AnyOperationRecord::Exec(exec),
            }),
            ..
        } if exec.stdout.prefix.contains("recorded")
    ));
}

// rejected exec also consumes an id and is recorded.
#[tokio::test]
async fn rejected_exec_recorded_with_disposition() {
    let mut h = harness().await;
    h.send(&req(
        "bad",
        RequestBody::Exec {
            argv: vec!["true".into()],
            cwd: None,
            profile: Some("nonexistent".into()),
            timeout_ms: Some(10000),
        },
    ));
    let _ = h.recv().await; // error

    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AnyOperationRecord::Exec(e) => {
                    assert_eq!(e.disposition, ExecDisposition::Rejected);
                    assert_eq!(e.termination, None);
                }
                other => panic!("expected rejected exec record, got {other:?}"),
            }
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// edit of an existing file preserves executable permissions.
#[tokio::test]
#[cfg(unix)]
async fn edit_preserves_executable_bit() {
    use std::os::unix::fs::PermissionsExt;
    let mut h = harness().await;
    // Create an executable script directly.
    std::fs::write(h.root_path.join("run.sh"), "#!/bin/sh\necho hi\n").unwrap();
    let perms = std::fs::metadata(h.root_path.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    std::fs::set_permissions(
        h.root_path.join("run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();

    h.send(&req(
        "w",
        RequestBody::Edit {
            path: "run.sh".into(),
            base_hash: hash_of("#!/bin/sh\necho hi\n"),
            edits: vec![EditSpec {
                old_text: "echo hi".into(),
                new_text: "echo bye".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    assert!(
        matches!(m, ServerMessage::Result { .. }),
        "edit must succeed: {m:?}"
    );

    let after = std::fs::metadata(h.root_path.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(
        after & 0o777,
        0o755,
        "executable bit must be preserved (was {perms:o}, now {after:o})"
    );
}

// The hash read returns is accepted as base_hash by the next mutation.
#[tokio::test]
async fn read_hash_consistent_with_base_hash() {
    let mut h = harness().await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "c.txt".into(),
            content: "alpha\n".into(),
        },
    ));
    let _ = h.recv().await;

    h.send(&req(
        "r",
        RequestBody::Read {
            path: "c.txt".into(),
            offset: None,
            limit: None,
        },
    ));
    let hash = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => r.hash.unwrap(),
        other => panic!("unexpected: {other:?}"),
    };

    // Using the read-returned hash as base_hash for an edit must succeed
    // (i.e. read and mutation agree on the hash).
    h.send(&req(
        "w2",
        RequestBody::Edit {
            path: "c.txt".into(),
            base_hash: hash,
            edits: vec![EditSpec {
                old_text: "alpha".into(),
                new_text: "beta".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    assert!(
        matches!(m, ServerMessage::Result { .. }),
        "base_hash from read must be accepted"
    );
}

// non-UTF-8 read is rejected, not lossy-converted.
#[tokio::test]
async fn read_rejects_non_utf8() {
    let mut h = harness().await;
    std::fs::write(h.root_path.join("bin.dat"), [0xFF, 0xFE, 0x00, 0x01]).unwrap();
    h.send(&req(
        "r",
        RequestBody::Read {
            path: "bin.dat".into(),
            offset: None,
            limit: None,
        },
    ));
    let m = h.recv().await;
    assert!(matches!(
        m,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            },
            ..
        }
    ));
}

// write+read over binary-ish but valid UTF-8 round-trips with a consistent
// hash (hash is over raw bytes).
#[tokio::test]
async fn binary_safe_hash_for_multibyte_utf8() {
    let mut h = harness().await;
    let content = "héllo, 世界 🦀\n";
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "u.txt".into(),
            content: content.into(),
        },
    ));
    let _ = h.recv().await;
    h.send(&req(
        "r",
        RequestBody::Read {
            path: "u.txt".into(),
            offset: None,
            limit: None,
        },
    ));
    let r = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => r,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(r.content, content);
    assert_eq!(r.hash.unwrap(), hash_of(content));
}

// the crash window — prepared written, file already renamed (so its
// hash == expected_after), but commit and result never written. After restart,
// recovery must synthesize the commit so the change is recorded and the
// request reports Done (not "in progress").
#[tokio::test]
async fn recovery_synthesizes_commit_when_rename_done() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let before = "old\n";
    let after = "new\n";
    let before_hash = hash_of(before);
    let after_hash = hash_of(after);

    // Pre-existing "before" file (the workspace state prior to the mutation).
    std::fs::write(root.path().join("f.txt"), before).unwrap();
    // The rename already happened: file now holds the after-content.
    std::fs::write(root.path().join("f.txt"), after).unwrap();

    // Hand-write ONLY the prepared marker (crash before commit).
    let prepared = serde_json::json!({
        "record_kind": "prepared",
        "operation_id": "op-7",
        "request_id": "crashed-req",
        "kind": "edit",
        "path": "f.txt",
        "before_hash": before_hash,
        "expected_after_hash": after_hash,
        "timestamp_ms": 1,
    });
    std::fs::write(&ops_path, format!("{prepared}\n")).unwrap();
    // And the in-progress request marker (claim had succeeded before crash).
    let in_progress = serde_json::json!({
        "request_id": "crashed-req",
        "status": "inprogress",
        "op": "edit",
    });
    std::fs::write(&req_path, format!("{in_progress}\n")).unwrap();

    // Restart: recovery runs in Server::new.
    let mut h = harness_at_with(root.path(), log_dir, None).await;

    // The change must now be recorded in history (synthesized commit).
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 1, "synthesized commit must appear");
            match &operations[0] {
                AnyOperationRecord::Fs(fs) => {
                    assert_eq!(fs.operation_id, "op-7");
                    assert_eq!(fs.before_hash.as_deref(), Some(before_hash.as_str()));
                    assert_eq!(fs.after_hash, after_hash);
                }
                other => panic!("expected fs record, got {other:?}"),
            }
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Replaying the same request id must return Done (not "in progress"),
    // with the synthesized result.
    h.send(&req(
        "crashed-req",
        RequestBody::Edit {
            path: "f.txt".into(),
            base_hash: after_hash.clone(),
            edits: vec![EditSpec {
                old_text: "new".into(),
                new_text: "DIFFERENT".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert_eq!(w.operation_id, "op-7");
            assert_eq!(
                w.new_hash, after_hash,
                "replay must return synthesized result"
            );
        }
        other => panic!("replay should return synthesized Done, got {other:?}"),
    }
}

// when the rename did NOT take effect (file still == before), recovery must
// drop the orphaned prepared marker and make the request retryable, so the
// change is neither lost nor stuck.
#[tokio::test]
async fn recovery_drops_when_rename_not_done() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let before = "old\n";
    let after = "new\n";
    let before_hash = hash_of(before);
    let after_hash = hash_of(after);

    // File is still in the BEFORE state (rename never happened).
    std::fs::write(root.path().join("f.txt"), before).unwrap();

    let prepared = serde_json::json!({
        "record_kind": "prepared",
        "operation_id": "op-7",
        "request_id": "crashed-req",
        "kind": "edit",
        "path": "f.txt",
        "before_hash": before_hash,
        "expected_after_hash": after_hash,
        "timestamp_ms": 1,
    });
    std::fs::write(&ops_path, format!("{prepared}\n")).unwrap();
    std::fs::write(
        &req_path,
        format!(
            "{}\n",
            serde_json::json!({"request_id": "crashed-req", "status": "inprogress", "op": "edit"})
        ),
    )
    .unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;

    // History must be empty (orphan dropped, no phantom operation).
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => assert!(
            operations.is_empty(),
            "orphan must be dropped: {operations:?}"
        ),
        other => panic!("unexpected: {other:?}"),
    }

    // The stuck request must now be retryable (status Unknown), so replaying it
    // executes the edit for real.
    h.send(&req(
        "crashed-req",
        RequestBody::Edit {
            path: "f.txt".into(),
            base_hash: before_hash.clone(),
            edits: vec![EditSpec {
                old_text: "old".into(),
                new_text: "new".into(),
                replace_all: false,
            }],
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert_eq!(w.new_hash, after_hash);
            assert_ne!(w.operation_id, "op-7", "retry must allocate a new op id");
        }
        other => panic!("retry must succeed, got {other:?}"),
    }
}

// when the request log is unwritable, the server must surface the error to
// the client rather than silently reporting success with no durable state.
#[tokio::test]
#[cfg(unix)]
async fn read_only_request_log_surfaces_error() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    // Seed requests.jsonl so the file exists, then make it read-only so appends fail.
    std::fs::write(log_dir.join("requests.jsonl"), "").unwrap();
    std::fs::set_permissions(
        log_dir.join("requests.jsonl"),
        std::fs::Permissions::from_mode(0o444),
    )
    .unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;
    h.send(&req(
        "r1",
        RequestBody::Create {
            path: "f.txt".into(),
            content: "x".into(),
        },
    ));
    let m = h.recv().await;
    // Must be an error (logging failed), NOT a silent success.
    assert!(
        matches!(m, ServerMessage::Error { .. }),
        "expected error when request log is unwritable, got {m:?}"
    );
    // Restore perms so the tempdir cleans up.
    std::fs::set_permissions(
        root.path().join(".remote-workspace/requests.jsonl"),
        std::fs::Permissions::from_mode(0o644),
    )
    .ok();
}

// a normal create durably appends BOTH a prepared marker and a committed
// fs record (the WAL), and history reconciles them to a single entry.
#[tokio::test]
async fn create_appends_prepared_then_committed() {
    let root = tempfile::tempdir().unwrap();
    let mut h = harness_at(root.path()).await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "f.txt".into(),
            content: "hi".into(),
        },
    ));
    let _ = h.recv().await;
    let ops_path = root.path().join(".remote-workspace/operations.jsonl");
    let raw = std::fs::read_to_string(&ops_path).unwrap();
    let lines: Vec<&str> = raw.lines().filter(|l| !l.is_empty()).collect();
    // Exactly two durable lines: prepared then committed.
    assert_eq!(lines.len(), 2, "expected prepared+committed, got: {raw}");
    assert!(
        lines[0].contains("\"record_kind\":\"prepared\""),
        "first line should be prepared: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"record_kind\":\"fs\""),
        "second line should be committed fs: {}",
        lines[1]
    );
    // And history exposes exactly ONE (reconciled) operation.
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => assert_eq!(operations.len(), 1),
        other => panic!("unexpected: {other:?}"),
    }
}

// Regression: zombie prepared record does not resurrect after drop+retry+restart.
#[tokio::test]
async fn aborted_marker_prevents_zombie_prepared() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();
    let before_hash = hash_of("before");
    let after_hash = hash_of("after");

    // 1) Hand-write ONLY a prepared marker (file is still "before").
    std::fs::write(root.path().join("f.txt"), "before").unwrap();
    let prepared = serde_json::json!({
        "record_kind": "prepared",
        "operation_id": "op-7",
        "request_id": "zombie-req",
        "kind": "edit",
        "path": "f.txt",
        "before_hash": before_hash,
        "expected_after_hash": after_hash,
        "timestamp_ms": 1,
    });
    std::fs::write(&ops_path, format!("{prepared}\n")).unwrap();
    std::fs::write(
        log_dir.join("requests.jsonl"),
        format!(
            "{}\n",
            serde_json::json!({"request_id":"zombie-req","status":"inprogress","op":"edit"})
        ),
    )
    .unwrap();

    // 2) First restart: recovery drops op-7 (file == before) and writes Aborted.
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        // Retry with a NEW request id so we don't replay cleanup.
        h.send(&req(
            "new-req",
            RequestBody::Edit {
                path: "f.txt".into(),
                base_hash: before_hash,
                edits: vec![EditSpec {
                    old_text: "before".into(),
                    new_text: "after".into(),
                    replace_all: false,
                }],
            },
        ));
        let _ = h.recv().await;
        assert_eq!(
            std::fs::read_to_string(root.path().join("f.txt")).unwrap(),
            "after"
        );
        h.shutdown().await;
    }

    // 3) Second restart: the Aborted marker must supersede the prepared
    // marker, so op-7 does NOT reappear in history even though the file now
    // happens to match the expected after_hash (after a legitimate retry).
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                // Must be exactly ONE record: the successful retry (op-8 or
                // similar). op-7 must not have been resurrected.
                assert_eq!(
                    operations.len(),
                    1,
                    "zombie op-7 had been aborted and must not reappear"
                );
                match &operations[0] {
                    AnyOperationRecord::Fs(fs) => {
                        assert_ne!(fs.operation_id, "op-7", "op-7 must not resurrect");
                    }
                    _ => panic!("expected fs record"),
                }
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// Regression: exec must not auto-retry when replayed after disconnection.
#[tokio::test]
async fn exec_replay_after_disconnect_rejected() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    let marker = root.path().join("marker");

    // First: run the exec normally and wait for it, so the side effect is real.
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "exec-1",
            RequestBody::Exec {
                argv: vec!["sh".into(), "-c".into(), "echo x >> marker".into()],
                cwd: None,
                profile: None,
                timeout_ms: Some(10000),
            },
        ));
        let _ = h.recv_all_for("exec-1").await;
        h.shutdown().await;
    }
    assert!(
        std::fs::read_to_string(&marker)
            .unwrap_or_default()
            .contains("x"),
        "first exec must have written to marker"
    );

    // Now simulate a crash AFTER the exec side effects but BEFORE the server
    // recorded the terminal result: rewrite the request log to make "exec-1"
    // look stuck InProgress without a result, so recovery treats it as
    // an interrupted exec. Also wipe the exec record so the operation log
    // has nothing for this id.
    let req_path = log_dir.join("requests.jsonl");
    let fake_stuck = serde_json::json!({
        "request_id": "exec-1",
        "status": "inprogress",
        "op": "exec",
    });
    std::fs::write(&req_path, format!("{fake_stuck}\n")).unwrap();
    let ops_path = log_dir.join("operations.jsonl");
    std::fs::write(&ops_path, "").unwrap();

    // Restart: replay "exec-1" must be PERMANENTLY rejected.
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req(
            "exec-1",
            RequestBody::Exec {
                argv: vec!["sh".into(), "-c".into(), "echo y >> marker".into()],
                cwd: None,
                profile: None,
                timeout_ms: Some(10000),
            },
        ));
        let m = h.recv_all_for("exec-1").await;
        let is_error = m
            .iter()
            .any(|msg| matches!(msg, ServerMessage::Error { .. }));
        assert!(is_error, "exec replay must be rejected, got {m:?}");
    }

    // Marker must still only have "x" — the replay did NOT execute.
    let content = std::fs::read_to_string(&marker).unwrap_or_default();
    assert!(
        content.contains("x") && !content.contains("y"),
        "replayed exec must not execute again: {content:?}"
    );
}

// Regression: when the committed operation record is on disk but the terminal
// result was lost in the crash, recovery must reconstruct the result from the
// committed record rather than clearing the request and allowing replay.
#[tokio::test]
async fn recovery_reconstructs_result_from_committed_record() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let after_hash = hash_of("after");

    // Simulate: a create ran to completion (prepared → rename → committed),
    // but the terminal result was never written to requests.jsonl.
    // File is already "after".
    std::fs::write(root.path().join("f.txt"), "after").unwrap();

    let committed = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-7",
        "request_id": "committed-no-result",
        "kind": "create",
        "path": "f.txt",
        "before_hash": null,
        "after_hash": after_hash,
        "timestamp_ms": 1,
    });
    std::fs::write(&ops_path, format!("{committed}\n")).unwrap();

    // Request is still InProgress — result never written.
    std::fs::write(
        &req_path,
        format!(
            "{}\n",
            serde_json::json!({
                "request_id": "committed-no-result",
                "status": "inprogress",
                "op": "create",
            })
        ),
    )
    .unwrap();

    // Restart: recovery must see the committed op-7 for "committed-no-result",
    // reconstruct the result, and mark the request Done.
    let mut h = harness_at_with(root.path(), log_dir, None).await;

    // Replaying the same request_id must return the stored (reconstructed)
    // result, NOT re-execute.
    h.send(&req(
        "committed-no-result",
        RequestBody::Create {
            path: "f.txt".into(),
            content: "should-not-run".into(),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert_eq!(w.operation_id, "op-7");
            assert_eq!(w.new_hash, after_hash, "must return original result");
        }
        other => panic!("replay must return reconstructed Done, got {other:?}"),
    }

    // History must have exactly ONE record, not two (no duplicate execution).
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 1, "no duplicate operation must exist");
            assert_eq!(operations[0].operation_id(), "op-7");
        }
        other => panic!("unexpected: {other:?}"),
    }

    // File must still be "after", not "should-not-run".
    assert_eq!(
        std::fs::read_to_string(root.path().join("f.txt")).unwrap(),
        "after"
    );
}

// Regression: a rejected exec committed on disk must reconstruct as Error
// (not Exit) so a replayed request_id returns the same wire-level type.
#[tokio::test]
async fn rejected_exec_replay_returns_error_not_exit() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Hand-write a rejected exec record (it ran, was rejected, consumed op-4).
    let rejected = serde_json::json!({
        "record_kind": "exec",
        "operation_id": "op-4",
        "request_id": "bad-exec",
        "argv": ["true"],
        "disposition": "rejected",
        "duration_ms": 0,
        "timestamp_ms": 1,
        "error": "unknown profile: nope",
        "error_code": "INVALID_REQUEST",
        "stdout": {"prefix": "", "suffix": "", "total_bytes": 0, "omitted_bytes": 0},
        "stderr": {"prefix": "", "suffix": "", "total_bytes": 0, "omitted_bytes": 0},
    });
    std::fs::write(&ops_path, format!("{rejected}\n")).unwrap();
    // Request is still InProgress (terminal result was lost in the crash).
    std::fs::write(
        &req_path,
        format!(
            "{}\n",
            serde_json::json!({
                "request_id": "bad-exec",
                "status": "inprogress",
                "op": "exec",
            })
        ),
    )
    .unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;

    // Replay: must reconstruct as Error, matching the original invocation's
    // wire-level type. NOT a successful Exit.
    h.send(&req(
        "bad-exec",
        RequestBody::Exec {
            argv: vec!["true".into()],
            cwd: None,
            profile: Some("nope".into()),
            timeout_ms: Some(10000),
        },
    ));
    let m = h.recv_all_for("bad-exec").await;
    let is_error = m
        .iter()
        .any(|msg| matches!(msg, ServerMessage::Error { .. }));
    let is_exit = m.iter().any(|msg| {
        matches!(
            msg,
            ServerMessage::Result {
                result: ResultBody::Exec(_),
                ..
            }
        )
    });
    assert!(
        is_error,
        "rejected exec replay must return Error, got {m:?}"
    );
    assert!(
        !is_exit,
        "rejected exec replay must NOT return Exit (would violate idempotency)"
    );
}

// Regression: UTF-8 pagination must always make progress when limit > 0 and
// data remains, even when the first codepoint is multi-byte.
#[tokio::test]
async fn utf8_pagination_always_makes_progress() {
    let mut h = harness().await;
    // "é" is 2 bytes; a 1-byte limit from offset 0 must NOT return an empty
    // page (which would loop forever if the caller advances by returned bytes).
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "u.txt".into(),
            content: "é".into(),
        },
    ));
    let _ = h.recv().await;

    h.send(&req(
        "r1",
        RequestBody::Read {
            path: "u.txt".into(),
            offset: Some(0),
            limit: Some(1),
        },
    ));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
            assert!(
                !r.content.is_empty(),
                "page must contain at least one codepoint, got empty"
            );
            assert_eq!(r.content, "é");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// Regression: a command that produces continuous output must STILL be killed at
// the deadline. The old code recreated the timer each loop iteration, so steady
// output reset it forever and the timeout was bypassed.
#[tokio::test]
async fn continuous_output_command_is_killed_at_deadline() {
    let mut h = harness().await;
    // Emit a line in a tight loop for a long time; timeout at 200ms. With the
    // old bug the timer was reset each iteration and this ran to completion
    // (~seconds); with the fix it must be killed near 200ms.
    let start = std::time::Instant::now();
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "sh".into(),
                "-c".into(),
                "i=0; while [ $i -lt 1000000 ]; do echo x; i=$((i+1)); done".into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(200),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "continuous-output command was not killed at deadline (elapsed {elapsed:?})"
    );
    assert!(matches!(
        &msgs[0],
        ServerMessage::Result {
            result: ResultBody::Exec(ExecResult {
                termination: ExecTermination::TimedOut,
                ..
            }),
            ..
        }
    ));
}

// Regression: stdout split across two pipe reads at a UTF-8 codepoint boundary
// must be reassembled into the correct character, not two replacement chars.
#[tokio::test]
#[cfg(unix)]
async fn cross_chunk_utf8_stdout_reassembled() {
    let mut h = harness().await;
    // Use python to write the raw bytes of "é" (0xC3 0xA9) with a flush and
    // delay between them, so the pipe read splits them across two reads.
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "python3".into(),
                "-c".into(),
                "import sys,time; sys.stdout.buffer.write(b'\\xc3'); \
             sys.stdout.buffer.flush(); time.sleep(0.1); \
             sys.stdout.buffer.write(b'\\xa9\\n')"
                    .into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    let combined = match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(result),
            ..
        } => format!("{}{}", result.stdout.prefix, result.stdout.suffix),
        other => panic!("unexpected: {other:?}"),
    };
    assert!(
        combined.contains('é'),
        "expected reassembled é in {combined:?}"
    );
    assert!(
        !combined.contains('\u{FFFD}'),
        "no replacement char should appear in {combined:?}"
    );
}

// Regression: a corrupted MIDDLE line in operations.jsonl must fail startup,
// not be silently skipped (which would lose records / enable id reuse).
#[tokio::test]
async fn corrupted_middle_log_line_fails_startup() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();
    // Two valid records with a garbage line between them.
    let valid1 = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-1",
        "request_id": "r1",
        "kind": "create",
        "path": "a.txt",
        "after_hash": "sha256:x",
        "timestamp_ms": 1,
    });
    let valid2 = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-2",
        "request_id": "r2",
        "kind": "create",
        "path": "b.txt",
        "after_hash": "sha256:y",
        "timestamp_ms": 2,
    });
    std::fs::write(&ops_path, format!("{valid1}\nNOT JSON\n{valid2}\n")).unwrap();
    std::fs::write(log_dir.join("requests.jsonl"), "").unwrap();

    // Server::new must surface an error, not silently start with a partial log.
    let result = remote_workspace_server::Server::new(remote_workspace_server::ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: log_dir,
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    assert!(
        result.is_err(),
        "startup must fail on a corrupted middle log line, got {result:?}"
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("corrupted") || msg.contains("not valid JSON"),
        "error should mention corruption: {msg}"
    );
}

// Regression: a corrupted TRAILING line (crash mid-write) is tolerated.
#[tokio::test]
async fn corrupted_trailing_log_line_tolerated() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();
    let valid = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-1",
        "request_id": "r1",
        "kind": "create",
        "path": "a.txt",
        "after_hash": "sha256:x",
        "timestamp_ms": 1,
    });
    // A truncated trailing record (crash mid-write).
    std::fs::write(
        &ops_path,
        format!("{valid}\n{{\"record_kind\":\"fs\",\"operation_id\":\"op-2\",\"req"),
    )
    .unwrap();
    std::fs::write(log_dir.join("requests.jsonl"), "").unwrap();

    let server = remote_workspace_server::Server::new(remote_workspace_server::ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: log_dir,
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    assert!(
        server.is_ok(),
        "trailing truncated line should be tolerated"
    );
}

// Regression: a crash-truncated trailing log line is physically removed on
// startup. Subsequent writes append cleanly, and the log remains valid across
// restarts — the truncation must not be a one-time pass but a durable fix.
#[tokio::test]
async fn truncated_log_fixed_then_append_then_restart() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Write a valid op-1 and a crash-truncated op-2 (NO trailing newline).
    let valid1 = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-1",
        "request_id": "r1",
        "kind": "create",
        "path": "a.txt",
        "after_hash": "sha256:x",
        "timestamp_ms": 1,
    });
    // Truncated: no closing brace, no newline.
    std::fs::write(
        &ops_path,
        format!("{valid1}\n{{\"record_kind\":\"fs\",\"operation_id\":\"op-2\",\"re"),
    )
    .unwrap();
    std::fs::write(&req_path, "").unwrap();

    // First startup: must succeed (truncation tolerated).
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        // Do a normal write — this appends to the log. If the truncated bytes
        // were still in the file, the append would concatenate onto them.
        h.send(&req(
            "new",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "hi".into(),
            },
        ));
        let _ = h.recv().await;
        h.shutdown().await;
    }

    // Second startup: must succeed again. If the previous append was poisoned
    // by concatenating onto the truncated bytes, startup fails here.
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                // Must see op-1 plus the new write (at least 2 records).
                assert!(
                    operations.len() >= 2,
                    "log must be writable and readable after trunc + append, got {} records",
                    operations.len()
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// Regression: a normal-exit command whose last output byte is an incomplete
// UTF-8 leader (e.g. 0xC3 with no continuation byte) must emit a replacement
// char on the wire via the flush path, not lose the byte silently.
#[tokio::test]
#[cfg(unix)]
async fn incomplete_trailing_utf8_flushed_on_clean_exit() {
    let mut h = harness().await;
    // Output a single incomplete byte 0xC3 and exit immediately.
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "python3".into(),
                "-c".into(),
                "import sys; sys.stdout.buffer.write(b'\\xc3')".into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    let combined = match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(result),
            ..
        } => format!("{}{}", result.stdout.prefix, result.stdout.suffix),
        other => panic!("unexpected: {other:?}"),
    };
    // Must contain the replacement char (U+FFFD) from the flush path — not be
    // empty (which would silently lose the byte).
    assert!(
        combined.contains('\u{FFFD}'),
        "incomplete trailing byte must be flushed as U+FFFD, got empty/stdout: {combined:?}"
    );
}

// Regression: invalid UTF-8 byte 0xFF must be emitted as U+FFFD, not silently
// dropped.
#[tokio::test]
#[cfg(unix)]
async fn invalid_utf8_byte_emitted_as_replacement() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec![
                "python3".into(),
                "-c".into(),
                "import sys; sys.stdout.buffer.write(b'\\xffok')".into(),
            ],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    let combined = match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(result),
            ..
        } => format!("{}{}", result.stdout.prefix, result.stdout.suffix),
        other => panic!("unexpected: {other:?}"),
    };
    assert!(
        combined.contains('\u{FFFD}'),
        "invalid byte 0xFF must emit U+FFFD, got: {combined:?}"
    );
    assert!(
        combined.contains("ok"),
        "valid bytes after invalid must be preserved"
    );
}

// Regression: a complete, valid JSON record without a trailing newline
// (crash between write(record) and write(\n)) must be PRESERVED, not deleted.
// The server should append the missing newline and keep the record intact.
#[tokio::test]
async fn valid_record_without_trailing_newline_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    // A complete, well-formed JSON record but WITHOUT the trailing \n.
    let valid = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-7",
        "request_id": "r7",
        "kind": "create",
        "path": "x.txt",
        "after_hash": "sha256:abc",
        "timestamp_ms": 1,
    });
    let line = serde_json::to_string(&valid).unwrap();
    // NO trailing newline: simulate a crash between the write of the JSON
    // and the write of the newline.
    std::fs::write(&ops_path, &line).unwrap();
    std::fs::write(&req_path, "").unwrap();

    // First startup: must see op-7 in history.
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                assert_eq!(
                    operations.len(),
                    1,
                    "valid record without newline must be preserved"
                );
                assert_eq!(operations[0].operation_id(), "op-7");
            }
            other => panic!("unexpected: {other:?}"),
        }
        h.shutdown().await;
    }

    // The file must now have a trailing newline appended (the repair step).
    let after = std::fs::read_to_string(&ops_path).unwrap();
    assert!(after.ends_with('\n'), "repair must add trailing newline");

    // Second startup: history must still see op-7 (no truncation).
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                assert_eq!(operations.len(), 1, "op-7 must survive second restart");
                assert_eq!(operations[0].operation_id(), "op-7");
            }
            other => panic!("unexpected: {other:?}"),
        }
        h.shutdown().await;
    }

    // And a new write must cleanly append (not concatenate).
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "w",
            RequestBody::Create {
                path: "y.txt".into(),
                content: "z".into(),
            },
        ));
        let _ = h.recv().await;
        h.shutdown().await;
    }
    // Third restart: must still work.
    {
        let _h = harness_at_with(root.path(), log_dir.clone(), None).await;
        // If we got here without error, the log is clean.
    }
}

#[tokio::test]
async fn crash_truncated_partial_record_still_removed() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let valid1 = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-1",
        "request_id": "r1",
        "kind": "create",
        "path": "a.txt",
        "after_hash": "sha256:x",
        "timestamp_ms": 1,
    });
    // Clearly truncated: partial, invalid JSON, no newline.
    std::fs::write(
        &ops_path,
        format!("{valid1}\n{{\"record_kind\":\"fs\",\"operation_id\":\"op-2\",\"re"),
    )
    .unwrap();
    std::fs::write(&req_path, "").unwrap();

    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        // Do a create. If truncation did NOT happen, this append would poison the log.
        h.send(&req(
            "w",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "hi".into(),
            },
        ));
        let _ = h.recv().await;
        h.shutdown().await;
    }
    // Restart must succeed, and history must contain exactly 2 records:
    // op-1 (from the original log) and the new write. The truncated op-2
    // must not have survived as a phantom record.
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req("hist", RequestBody::History { limit: None }));
        let m = h.recv().await;
        match m {
            ServerMessage::Result {
                result: ResultBody::History { operations },
                ..
            } => {
                assert_eq!(operations.len(), 2, "must be exactly op-1 + new write");
                // The first must be op-1 (from the original log).
                assert_eq!(operations[0].operation_id(), "op-1");
                // The second is the freshly written record (which may reuse
                // op-2, since the truncated line never consumed that id).
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// Regression: a crash that truncates the log file mid-codepoint of a multi-byte
// UTF-8 character must be recoverable. The raw bytes should be treated as a
// crash-truncated trailing record and physically removed, not crash the parser.
#[tokio::test]
async fn crash_mid_utf8_codepoint_tail_recovered() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let valid1 = r#"{"record_kind":"fs","operation_id":"op-1","request_id":"r1","kind":"create","path":"a.txt","after_hash":"sha256:x","timestamp_ms":1}"#;
    let mut raw = valid1.as_bytes().to_vec();
    raw.push(b'\n');
    raw.extend_from_slice(&[0xE6]); // partial byte of multi-byte character
    std::fs::write(&ops_path, &raw).unwrap();
    std::fs::write(log_dir.join("requests.jsonl"), "").unwrap();

    // Server::new must NOT panic or error — the trailing byte is treated as
    // crash-truncated and the file is repaired to valid JSONL.
    let server = remote_workspace_server::Server::new(remote_workspace_server::ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: log_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    assert!(
        server.is_ok(),
        "server must start despite mid-UTF8 crash tail: {:?}",
        server.err()
    );

    // The file must now be clean UTF-8 and end with a newline.
    let after = std::fs::read_to_string(&ops_path).unwrap();
    assert!(
        after.ends_with('\n'),
        "repaired file must end with newline, got: {after:?}"
    );
    assert!(
        after.contains("op-1"),
        "op-1 must still be present in the log"
    );
    // The trailing 0xE6 must be gone.
    let bytes = std::fs::read(&ops_path).unwrap();
    assert!(
        !bytes.contains(&0xE6),
        "truncated 0xE6 byte must have been removed"
    );
}

// A committed record following a prepared one for the same operation_id
// collapses to a single entry, so the three-line log below reconciles to two
// operations: op-1 and the committed op-2.
#[tokio::test]
async fn prepared_and_committed_same_id_reconcile_to_one() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_path = log_dir.join("operations.jsonl");
    let req_path = log_dir.join("requests.jsonl");
    std::fs::create_dir_all(&log_dir).unwrap();

    let op1 = r#"{"record_kind":"fs","operation_id":"op-1","request_id":"r1","kind":"create","path":"a.txt","after_hash":"sha256:x","timestamp_ms":1}"#;
    let prepared = serde_json::json!({
        "record_kind": "prepared",
        "operation_id": "op-2",
        "request_id": "w",
        "kind": "create",
        "path": "f.txt",
        "expected_after_hash": "sha256:8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4",
        "timestamp_ms": 2,
    });
    let committed = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-2",
        "request_id": "w",
        "kind": "create",
        "path": "f.txt",
        "after_hash": "sha256:8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4",
        "timestamp_ms": 3,
    });
    std::fs::write(&ops_path, format!("{op1}\n{prepared}\n{committed}\n")).unwrap();
    std::fs::write(&req_path, "").unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;
    h.send(&req("hist", RequestBody::History { limit: None }));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(
                operations.len(),
                2,
                "must reconcile to 2 records (op-1 and committed op-2)"
            );
            assert_eq!(operations[0].operation_id(), "op-1");
            assert_eq!(operations[1].operation_id(), "op-2");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// Gc drops old operations and stale request entries, and pruned ids are never
// resolvable again.
#[tokio::test]
async fn gc_prunes_operations_and_requests() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "w0",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "v1".into(),
            },
        ));
        let _ = h.recv().await;
        for (rid, old_c, new_c) in [("w1", "v1", "v2"), ("w2", "v2", "v3")] {
            h.send(&req(
                rid,
                RequestBody::Edit {
                    path: "f.txt".into(),
                    base_hash: hash_of(old_c),
                    edits: vec![EditSpec {
                        old_text: old_c.into(),
                        new_text: new_c.into(),
                        replace_all: false,
                    }],
                },
            ));
            let _ = h.recv().await;
        }
        h.send(&req("gc", RequestBody::Gc { keep: Some(1) }));
        match h.recv().await {
            ServerMessage::Result {
                result: ResultBody::Gc(g),
                ..
            } => {
                assert_eq!(g.removed_operations, 2);
                assert_eq!(g.retained_operations, 1);
                assert_eq!(g.removed_requests, 2, "w0 and w1 must be dropped");
            }
            other => panic!("unexpected: {other:?}"),
        }
        // A pruned operation is gone for good, not resolvable to a new one.
        h.send(&req(
            "op2",
            RequestBody::OperationGet {
                operation_id: "op-2".into(),
            },
        ));
        match h.recv().await {
            ServerMessage::Error { error, .. } => {
                assert_eq!(error.code, ErrorCode::OperationNotFound)
            }
            other => panic!("pruned op must not resolve: {other:?}"),
        }
        h.shutdown().await;
    }
    // Restart: pruned state loads, and ids continue past the pruned range
    // (no reuse of op-1/op-2 even though they left the log).
    {
        let mut h = harness_at_with(root.path(), log_dir, None).await;
        h.send(&req(
            "w-after",
            RequestBody::Edit {
                path: "f.txt".into(),
                base_hash: hash_of("v3"),
                edits: vec![EditSpec {
                    old_text: "v3".into(),
                    new_text: "v4".into(),
                    replace_all: false,
                }],
            },
        ));
        match h.recv().await {
            ServerMessage::Result {
                result: ResultBody::Mutation(w),
                ..
            } => assert_eq!(w.operation_id, "op-4"),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// Startup prune honors ServerOptions::history_limit.
#[tokio::test]
async fn startup_prune_respects_history_limit() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    std::fs::create_dir_all(&log_dir).unwrap();
    {
        let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
        h.send(&req(
            "w0",
            RequestBody::Create {
                path: "f.txt".into(),
                content: "v0".into(),
            },
        ));
        let _ = h.recv().await;
        for (rid, old_c, new_c) in [("w1", "v0", "v1"), ("w2", "v1", "v2")] {
            h.send(&req(
                rid,
                RequestBody::Edit {
                    path: "f.txt".into(),
                    base_hash: hash_of(old_c),
                    edits: vec![EditSpec {
                        old_text: old_c.into(),
                        new_text: new_c.into(),
                        replace_all: false,
                    }],
                },
            ));
            let _ = h.recv().await;
        }
        h.shutdown().await;
    }
    let server = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: log_dir,
        config_path: None,
        history_limit: Some(1),
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();
    let ops = server.store.history(None);
    assert_eq!(ops.len(), 1, "startup prune must keep only the newest op");
    assert_eq!(ops[0].operation_id(), "op-3");
}

// Gc with no explicit keep and no server-side limit is an explicit error.
#[tokio::test]
async fn gc_without_keep_or_limit_rejected() {
    let mut h = harness().await;
    h.send(&req("gc", RequestBody::Gc { keep: None }));
    match h.recv().await {
        ServerMessage::Error { error, .. } => assert_eq!(error.code, ErrorCode::InvalidRequest),
        other => panic!("unexpected: {other:?}"),
    }
}

// Delete: result hashes, history record, and error cases.
#[tokio::test]
async fn delete_roundtrip_and_errors() {
    let mut h = harness().await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "d.txt".into(),
            content: "keep me".into(),
        },
    ));
    let _ = h.recv().await;

    h.send(&req(
        "del",
        RequestBody::Delete {
            path: "d.txt".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert_eq!(w.operation_id, "op-2");
            assert_eq!(w.old_hash, Some(hash_of("keep me")));
            assert_eq!(w.new_hash, "sha256:");
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(!h.root_path.join("d.txt").exists());

    // Deleting a directory is IsADirectory, not NotFound.
    std::fs::create_dir(h.root_path.join("somedir")).unwrap();
    h.send(&req(
        "del-dir",
        RequestBody::Delete {
            path: "somedir".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Error { error, .. } => assert_eq!(error.code, ErrorCode::IsADirectory),
        other => panic!("unexpected: {other:?}"),
    }
    // Deleting a missing file is NotFound.
    h.send(&req(
        "del-missing",
        RequestBody::Delete {
            path: "nope.txt".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Error { error, .. } => assert_eq!(error.code, ErrorCode::NotFound),
        other => panic!("unexpected: {other:?}"),
    }

    // The delete is a first-class history record.
    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            let has_delete = operations.iter().any(|r| {
                matches!(r, AnyOperationRecord::Fs(f)
                    if f.operation_id == "op-2" && matches!(f.kind, OperationKind::Delete))
            });
            assert!(has_delete, "delete must appear in history: {operations:?}");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// Without a profile the argv is spawned directly: no shell means no word
// splitting, expansion, or quoting hazards.
#[tokio::test]
async fn exec_without_profile_spawns_argv_directly() {
    let mut h = harness().await;
    let hostile = "a b'c\"d$HOME`id`\nnewline";
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["printf".into(), "%s".into(), hostile.into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(r),
            ..
        } => {
            assert_eq!(r.termination, ExecTermination::Exited { code: 0 });
            assert_eq!(r.stdout.prefix, hostile, "argv must pass through verbatim");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// A nonexistent command without a profile is a rejected spawn, not a shell's
// exit 127.
#[tokio::test]
async fn exec_without_profile_missing_command_rejected() {
    let mut h = harness().await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["definitely-not-a-command-xyz".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    assert!(matches!(
        h.recv().await,
        ServerMessage::Error {
            error: ProtocolError {
                code: ErrorCode::ExecFailed,
                ..
            },
            ..
        }
    ));
}

// A profile with an empty setup must still run through the profile's shell:
// the shell choice itself is the point. Using `echo` as the "shell" makes the
// generated script observable as output.
#[tokio::test]
async fn profile_with_empty_setup_still_uses_its_shell() {
    let cfg = r#"
[profiles.echoer]
shell = ["echo"]
setup = ""
"#;
    let mut h = harness_with_config(Some(cfg)).await;
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["hi".into()],
            cwd: None,
            profile: Some("echoer".into()),
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(r),
            ..
        } => {
            assert_eq!(r.termination, ExecTermination::Exited { code: 0 });
            #[cfg(unix)]
            assert_eq!(r.stdout.prefix.trim(), "exec 'hi'");
            #[cfg(windows)]
            assert!(r
                .stdout
                .prefix
                .contains("[System.Diagnostics.ProcessStartInfo]::new"));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// default_profile applies when the request names no profile; an explicit
// profile overrides it.
#[tokio::test]
async fn default_profile_applies_and_explicit_overrides() {
    #[cfg(unix)]
    let cfg = r#"
default_profile = "main"

[profiles.main]
setup = "export MARKER=via-default"

[profiles.other]
shell = ["sh", "-c"]
setup = "export MARKER=via-override"
"#;
    #[cfg(windows)]
    let cfg = r#"
default_profile = "main"

[profiles.main]
setup = "$env:MARKER = 'via-default'"

[profiles.other]
setup = "$env:MARKER = 'via-override'"
"#;
    let mut h = harness_with_config(Some(cfg)).await;
    for (rid, profile, expected) in [
        ("d", None, "via-default"),
        ("o", Some("other".to_string()), "via-override"),
    ] {
        h.send(&req(
            rid,
            RequestBody::Exec {
                #[cfg(unix)]
                argv: vec!["printenv".into(), "MARKER".into()],
                #[cfg(windows)]
                argv: vec![
                    "cmd.exe".into(),
                    "/C".into(),
                    "echo".into(),
                    "%MARKER%".into(),
                ],
                cwd: None,
                profile,
                timeout_ms: Some(10000),
            },
        ));
        let msgs = h.recv_all_for(rid).await;
        match &msgs[0] {
            ServerMessage::Result {
                result: ResultBody::Exec(r),
                ..
            } => assert_eq!(r.stdout.prefix.trim(), expected),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

// A config the server cannot honour -- an undeclared default_profile, an
// unknown field, an empty shell -- must fail startup loudly instead of
// silently running commands in the wrong environment.
#[tokio::test]
async fn server_startup_rejects_config_with_unknown_fields() {
    for bad in [
        "default_profile = \"ghost\"\n",
        "[profiles.p]\nsetup = \"\"\nfuture_field = 1\n",
        "[profiles.p]\nshell = []\nsetup = \"\"\n",
    ] {
        let root = tempfile::tempdir().unwrap();
        let config_path = root.path().join("config.toml");
        std::fs::write(&config_path, bad).unwrap();
        let result = Server::new(ServerOptions {
            root: root.path().to_path_buf(),
            state_dir: root.path().join(".remote-workspace"),
            config_path: Some(config_path),
            history_limit: None,
            scratch_max_age: None,
            idle_timeout: None,
        });
        assert!(result.is_err(), "config must be rejected at startup: {bad}");
    }
}

// A state directory written before undo was removed must still load: its log
// carries `kind: "undo"` records, and its blobs directory is dead weight the
// server reclaims on first start.
#[tokio::test]
async fn legacy_undo_state_still_loads_and_sheds_its_blobs() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let blobs_dir = log_dir.join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    std::fs::write(root.path().join("f.txt"), "restored\n").unwrap();
    std::fs::write(blobs_dir.join("op-1.before"), "restored\n").unwrap();

    let undo_record = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-2",
        "request_id": "old-undo",
        "kind": "undo",
        "path": "f.txt",
        "before_hash": hash_of("edited\n"),
        "after_hash": hash_of("restored\n"),
        "timestamp_ms": 1,
    });
    std::fs::write(log_dir.join("operations.jsonl"), format!("{undo_record}\n")).unwrap();
    // A stored terminal result of the removed operation must still deserialize.
    let old_result = serde_json::json!({
        "request_id": "old-undo",
        "status": "done",
        "result_done": {
            "request_id": "old-undo",
            "type": "undo",
            "operation_id": "op-2",
            "restored_hash": hash_of("restored\n"),
            "new_hash": hash_of("restored\n"),
        },
        "op": "undo",
    });
    std::fs::write(log_dir.join("requests.jsonl"), format!("{old_result}\n")).unwrap();

    let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 1);
            assert_eq!(operations[0].operation_id(), "op-2");
        }
        other => panic!("legacy undo record must load: {other:?}"),
    }
    assert!(!blobs_dir.exists(), "legacy blobs must be reclaimed");

    // New mutations continue past the legacy id.
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "new.txt".into(),
            content: "x".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => assert_eq!(w.operation_id, "op-3"),
        other => panic!("unexpected: {other:?}"),
    }
}

// ---- idle timeout ----

/// A server on `state_dir` with an idle timeout, wired to raw duplex pipes.
/// Returns the write half (holding it open is what makes the connection look
/// alive but silent, exactly as a dead SSH session does), a reader over its
/// replies, and the task running it.
fn spawn_idle_server(
    root: &std::path::Path,
    state_dir: PathBuf,
    idle: std::time::Duration,
) -> (
    tokio::io::DuplexStream,
    BufReader<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<()>,
) {
    let server = Server::new(ServerOptions {
        root: root.to_path_buf(),
        state_dir,
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: Some(idle),
    })
    .unwrap();
    let (client_tx, client_rx) = tokio::io::duplex(1 << 20);
    let (server_tx, server_rx) = tokio::io::duplex(1 << 20);
    let task = tokio::spawn(async move {
        server.run(client_rx, server_tx).await.unwrap();
    });
    (client_tx, BufReader::new(server_rx), task)
}

fn session_events(state_dir: &std::path::Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(state_dir.join("server.jsonl")).unwrap();
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

// The whole point of the timeout: a connection that is open but silent -- the
// remote end of an SSH session whose client vanished -- must not hold the
// state lock forever.
#[tokio::test]
async fn idle_timeout_exits_and_releases_the_state_lock() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (_writer, _reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_millis(200),
    );

    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("server must exit on its own while the connection is still open")
        .unwrap();

    // The lock is gone with it: a fresh server takes the same state directory.
    let reopened = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: state_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    assert!(
        reopened.is_ok(),
        "state lock still held after an idle exit: {:?}",
        reopened.err()
    );
    drop(reopened);

    let events = session_events(&state_dir);
    assert_eq!(events[0]["event"], "started");
    let exit = events.iter().find(|e| e["event"] == "exit").unwrap();
    assert_eq!(exit["reason"], "idle_timeout");
    assert_eq!(exit["requests"], 0);
}

// A single exec may legitimately run for an hour with the client silent. The
// timeout measures idleness, not silence, so it must not kill the request it is
// waiting on -- and the clock must restart FROM THAT REQUEST'S COMPLETION, not
// from the last read. Checked by behaviour rather than by timing: the second
// request is sent at a point where a read-anchored clock would already have
// closed the connection, and it has to be served.
#[tokio::test]
async fn idle_timeout_spares_a_request_and_restarts_from_its_completion() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (mut writer, mut reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_millis(300),
    );

    let mut line = serde_json::to_string(&req(
        "slow",
        RequestBody::Exec {
            argv: vec!["sleep".into(), "0.8".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ))
    .unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    // The command outlives several idle windows without being killed, and its
    // reply still arrives.
    let mut reply = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut reply),
    )
    .await
    .expect("server exited while its own exec was still running")
    .unwrap();
    match serde_json::from_str::<ServerMessage>(reply.trim()).unwrap() {
        ServerMessage::Result {
            result: ResultBody::Exec(r),
            ..
        } => assert_eq!(r.termination, ExecTermination::Exited { code: 0 }),
        other => panic!("unexpected: {other:?}"),
    }

    // Sent 150ms after the reply -- past the point a clock anchored to the last
    // read would have closed the connection (its windows ended at 300/600/900ms,
    // the last of them once the command was done), and well inside the window a
    // clock restarted at completion still has to run.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut line = serde_json::to_string(&req(
        "after",
        RequestBody::List {
            path: ".".into(),
            offset: None,
            limit: None,
        },
    ))
    .unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    let mut reply = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut reply),
    )
    .await
    .expect("server closed: the idle clock did not restart from the completion")
    .unwrap();
    assert!(
        reply.contains("\"after\""),
        "second request not served: {reply}"
    );

    // And it still exits on its own once genuinely idle.
    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("server must still exit when nothing is happening")
        .unwrap();
    let exit = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "exit")
        .unwrap();
    assert_eq!(exit["reason"], "idle_timeout");
    assert_eq!(exit["requests"], 2);
}

// EOF must still be the ordinary way out, and must be distinguishable in the
// record from an idle exit -- they mean different things about the client.
#[tokio::test]
async fn stdin_eof_is_recorded_as_its_own_exit_reason() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (writer, _reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_secs(3600),
    );
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("server must exit on EOF")
        .unwrap();
    let exit = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "exit")
        .unwrap();
    assert_eq!(exit["reason"], "stdin_eof");
}

// A refused start is the moment a workspace looks occupied, so it has to leave
// a trace on the remote naming who held the lock.
#[tokio::test]
async fn a_refused_start_is_recorded_with_the_holder() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let holder = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: state_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();

    let refused = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: state_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    let message = refused
        .expect_err("second server must be refused")
        .to_string();
    assert!(message.contains("locked"), "unexpected error: {message}");

    let denied = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "lock_denied")
        .expect("refusal must be recorded");
    assert_eq!(
        denied["holder_pid"].as_str().unwrap(),
        std::process::id().to_string(),
        "the recorded holder must be the process actually holding the lock"
    );
    drop(holder);
}

// A client closing its end of the pipe does not mean it stopped listening --
// over SSH, stdin EOF reaches the server while stdout is still open. Returning
// the moment stdin ends would drop a handler mid-flight, so a mutation already
// applied to the workspace would never report its result and the caller could
// not tell whether it landed.
#[tokio::test]
async fn a_request_still_running_at_stdin_eof_still_gets_its_reply() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (mut writer, mut reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_secs(3600),
    );

    let mut line = serde_json::to_string(&req(
        "slow",
        RequestBody::Exec {
            argv: vec!["sh".into(), "-c".into(), "sleep 0.4; echo done".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ))
    .unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    // EOF arrives while the command is still running.
    drop(writer);

    let mut reply = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut reply),
    )
    .await
    .expect("server exited without replying")
    .unwrap();
    match serde_json::from_str::<ServerMessage>(reply.trim()).unwrap() {
        ServerMessage::Result {
            result: ResultBody::Exec(r),
            ..
        } => {
            assert_eq!(r.termination, ExecTermination::Exited { code: 0 });
            assert!(r.stdout.prefix.contains("done"));
        }
        other => panic!("unexpected: {other:?}"),
    }

    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("server must still exit")
        .unwrap();
    let exit = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "exit")
        .unwrap();
    assert_eq!(exit["reason"], "stdin_eof");
    assert_eq!(
        exit["drained"], true,
        "the record must say whether anything was abandoned"
    );
}

// The wait is bounded: a command far longer than the drain window is abandoned
// rather than held onto, because a server on its way out must not keep the
// state lock from its successor. Recovery resolves the abandoned request.
#[tokio::test]
async fn shutdown_does_not_wait_for_a_long_running_command() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (mut writer, _reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_secs(3600),
    );

    let mut line = serde_json::to_string(&req(
        "forever",
        RequestBody::Exec {
            argv: vec!["sleep".into(), "60".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(120_000),
        },
    ))
    .unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    drop(writer);

    let started = std::time::Instant::now();
    tokio::time::timeout(std::time::Duration::from_secs(30), task)
        .await
        .expect("server must not wait for the command to finish")
        .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "shutdown waited far too long: {:?}",
        started.elapsed()
    );
    let exit = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "exit")
        .unwrap();
    assert_eq!(exit["drained"], false);
}

// Input that breaks rather than ends is still a shutdown: it has to drain and
// record its exit like any other, or a mutation that landed just before the
// connection broke reports nothing and leaves no trace of why.
#[tokio::test]
async fn a_broken_input_stream_still_drains_and_records_its_exit() {
    struct BrokenAfterFirstRead(Option<Vec<u8>>);
    impl tokio::io::AsyncRead for BrokenAfterFirstRead {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            match self.0.take() {
                Some(bytes) => {
                    buf.put_slice(&bytes);
                    std::task::Poll::Ready(Ok(()))
                }
                None => std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset",
                ))),
            }
        }
    }

    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let server = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: state_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();

    let mut line = serde_json::to_string(&req(
        "slow",
        RequestBody::Exec {
            argv: vec!["sh".into(), "-c".into(), "sleep 0.4; echo done".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ))
    .unwrap();
    line.push('\n');
    let (server_tx, server_rx) = tokio::io::duplex(1 << 20);
    let task = tokio::spawn(async move {
        server
            .run(BrokenAfterFirstRead(Some(line.into_bytes())), server_tx)
            .await
    });

    // The reply is written despite the read side having failed.
    let mut reader = BufReader::new(server_rx);
    let mut reply = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut reply),
    )
    .await
    .expect("server abandoned the handler on a read error")
    .unwrap();
    assert!(reply.contains("\"slow\""), "unexpected reply: {reply}");

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("server must exit")
        .unwrap();
    assert!(
        result.is_err(),
        "a read error must still surface as an error"
    );

    let exit = session_events(&state_dir)
        .into_iter()
        .find(|e| e["event"] == "exit")
        .expect("a broken stream must still record its exit");
    assert_eq!(exit["reason"], "read_error");
    assert_eq!(exit["drained"], true);
}

// A handler that outlives the drain must be let go of, not carried: it holds an
// Arc<Server>, so keeping it would keep the state directory locked against the
// next server long after this one reported itself gone.
#[tokio::test]
async fn an_abandoned_handler_does_not_keep_the_state_lock() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (mut writer, _reader, task) = spawn_idle_server(
        root.path(),
        state_dir.clone(),
        std::time::Duration::from_secs(3600),
    );

    let mut line = serde_json::to_string(&req(
        "forever",
        RequestBody::Exec {
            argv: vec!["sleep".into(), "60".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(120_000),
        },
    ))
    .unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    drop(writer);

    tokio::time::timeout(std::time::Duration::from_secs(30), task)
        .await
        .expect("server must exit without waiting for the command")
        .unwrap();

    // The successor takes the lock immediately -- it must not have to burn its
    // grace period waiting for a task the previous server walked away from.
    let successor = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir,
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    });
    assert!(
        successor.is_ok(),
        "state lock still held by an abandoned handler: {:?}",
        successor.err()
    );
}

// A request that arrives in pieces spanning idle-timeout expiries must still
// be executed whole. `read_line` loses bytes it has already consumed when its
// future is dropped, so letting the timeout interrupt it silently truncated a
// request into something that parses as garbage -- or, worse, as a different
// valid request. A handler runs throughout so the expiries fire without the
// server exiting, which is the situation that made the loss reachable.
#[tokio::test]
async fn a_request_split_across_idle_expiries_is_not_truncated() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("f.txt"), "hello\n").unwrap();
    let state_dir = root.path().join(".remote-workspace");
    let (mut writer, mut reader, task) = spawn_idle_server(
        root.path(),
        state_dir,
        std::time::Duration::from_millis(150),
    );

    // Keeps the server alive across the expiries below.
    let mut busy = serde_json::to_string(&req(
        "busy",
        RequestBody::Exec {
            argv: vec!["sleep".into(), "0.9".into()],
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ))
    .unwrap();
    busy.push('\n');
    writer.write_all(busy.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    let line = serde_json::to_string(&req(
        "split",
        RequestBody::Read {
            path: "f.txt".into(),
            offset: None,
            limit: None,
        },
    ))
    .unwrap();
    let (head, tail) = line.split_at(line.len() / 2);
    writer.write_all(head.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    // Several idle windows expire with half a request consumed.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    writer
        .write_all(format!("{tail}\n").as_bytes())
        .await
        .unwrap();
    writer.flush().await.unwrap();

    // Both replies must arrive, and the split one must be the request that was
    // actually sent.
    let mut saw_read = false;
    for _ in 0..2 {
        let mut reply = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            reader.read_line(&mut reply),
        )
        .await
        .expect("no reply: the split request was lost")
        .unwrap();
        match serde_json::from_str::<ServerMessage>(reply.trim()).unwrap() {
            ServerMessage::Result {
                request_id,
                result: ResultBody::Read(r),
            } => {
                assert_eq!(request_id, "split");
                assert_eq!(r.content, "hello\n");
                saw_read = true;
            }
            ServerMessage::Result { .. } => {}
            other => panic!("request was truncated or mangled: {other:?}"),
        }
    }
    assert!(saw_read, "the split request never produced its result");
    drop(writer);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), task).await;
}
