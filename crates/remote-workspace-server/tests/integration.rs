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

fn read_body(path: &str, offset: Option<u64>, limit: Option<u64>) -> RequestBody {
    ReadOptions {
        offset,
        limit,
        ..Default::default()
    }
    .into_request_body(path.into())
}

fn edit_body(path: &str, base_hash: String, edits: Vec<EditSpec>) -> RequestBody {
    RequestBody::Edit {
        path: path.into(),
        base_hash,
        edits,
        replace_content: None,
    }
}

#[cfg(unix)]
fn platform_script(unix_script: &str, _windows_script: &str) -> Vec<String> {
    vec!["sh".into(), "-c".into(), unix_script.into()]
}

#[cfg(windows)]
fn platform_script(_unix_script: &str, windows_script: &str) -> Vec<String> {
    vec![
        "powershell.exe".into(),
        "-NoLogo".into(),
        "-NoProfile".into(),
        "-NonInteractive".into(),
        "-Command".into(),
        windows_script.into(),
    ]
}

fn sleep_argv(milliseconds: u64) -> Vec<String> {
    platform_script(
        &format!("sleep {}", milliseconds as f64 / 1000.0),
        &format!("Start-Sleep -Milliseconds {milliseconds}"),
    )
}

#[cfg(unix)]
fn verbatim_argv(_dir: &std::path::Path, value: &str) -> Vec<String> {
    vec!["printf".into(), "%s".into(), value.into()]
}

#[cfg(windows)]
fn verbatim_argv(dir: &std::path::Path, value: &str) -> Vec<String> {
    let mut argv = echo_last_arg_argv(dir);
    argv.push(value.into());
    argv
}

/// PowerShell that prints its last command-line argument verbatim. Only a
/// script file can take an argument: `-Command` treats the rest as code.
#[cfg(windows)]
fn echo_last_arg_argv(dir: &std::path::Path) -> Vec<String> {
    let script = dir.join("echo-last-arg.ps1");
    std::fs::write(
        &script,
        "[Console]::Out.Write([Environment]::GetCommandLineArgs()[-1])",
    )
    .unwrap();
    vec![
        "powershell.exe".into(),
        "-NoLogo".into(),
        "-NoProfile".into(),
        "-NonInteractive".into(),
        "-ExecutionPolicy".into(),
        "Bypass".into(),
        "-File".into(),
        script.display().to_string(),
    ]
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

    h.send(&req("r2", read_body("hello.txt", None, None)));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
            assert_eq!(r.content, "hello world\n");
            assert_eq!(r.total_bytes, 12);
            assert_eq!(r.start_offset, 0);
            assert_eq!(r.end_offset, 12);
            assert!(r.trailing_newline);
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
        edit_body(
            "f.txt",
            hash.clone(),
            vec![EditSpec {
                old_text: "v1".into(),
                new_text: "v2".into(),
                replace_all: false,
            }],
        ),
    ));
    let _ = h.recv().await;

    // Now edit with the stale v1 base_hash should be rejected.
    h.send(&req(
        "w3",
        edit_body(
            "f.txt",
            hash,
            vec![EditSpec {
                old_text: "v2".into(),
                new_text: "v3".into(),
                replace_all: false,
            }],
        ),
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
        edit_body(
            "p.txt",
            hash.clone(),
            vec![EditSpec {
                old_text: "b".into(),
                new_text: "BEE".into(),
                replace_all: false,
            }],
        ),
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
        edit_body(
            "p.txt",
            current_hash,
            vec![EditSpec {
                old_text: "not-present".into(),
                new_text: "X".into(),
                replace_all: false,
            }],
        ),
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
    h.send(&req("r", read_body("../etc/passwd", None, None)));
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
            argv: platform_script(
                "printf hello-stdout",
                "[Console]::Out.Write('hello-stdout')",
            ),
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
            argv: platform_script(
                "echo err >&2; exit 7",
                "[Console]::Error.WriteLine('err'); exit 7",
            ),
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
            argv: platform_script(
                "printf scratch-data > \"$REMOTE_WORKSPACE_SCRATCH/job.log\"",
                // PowerShell 5.1 cmdlets, like cmd and git, reject `\\?\` paths.
                "Set-Content -LiteralPath \"$env:REMOTE_WORKSPACE_SCRATCH\\job.log\" -Value scratch-data -NoNewline",
            ),
            cwd: None,
            profile: None,
            timeout_ms: Some(10000),
        },
    ));
    let exec_msgs = h.recv_all_for("e").await;
    assert!(
        matches!(
            &exec_msgs[0],
            ServerMessage::Result {
                result: ResultBody::Exec(ExecResult {
                    termination: ExecTermination::Exited { code: 0 },
                    ..
                }),
                ..
            }
        ),
        "scratch writer failed: {exec_msgs:?}"
    );

    h.send(&req("r", read_body("@scratch/job.log", None, None)));
    let read_msg = h.recv().await;
    assert!(
        matches!(
            &read_msg,
            ServerMessage::Result {
                result: ResultBody::Read(ReadResult { content, .. }),
                ..
            } if content == "scratch-data"
        ),
        "unexpected scratch read: {read_msg:?}"
    );
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
        edit_body(
            "h.txt",
            hash_of("1"),
            vec![EditSpec {
                old_text: "1".into(),
                new_text: "2".into(),
                replace_all: false,
            }],
        ),
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
async fn profile_unknown_rejected_with_available_profiles() {
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
    match m {
        ServerMessage::Error { error, .. } => {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            assert!(
                error.message.contains("no profiles are configured"),
                "unexpected message: {}",
                error.message
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
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
    // If the first completed before the second claimed, both replay the same
    // result; if genuinely concurrent in flight, the second is refused with
    // InvalidRequest ("request already in progress"). Either way it runs once.
    let results = [&m1, &m2]
        .iter()
        .filter(|m| matches!(m, ServerMessage::Result { .. }))
        .count();
    assert!(results >= 1, "at least one must succeed: {m1:?} / {m2:?}");
    if results == 2 {
        assert_eq!(
            serde_json::to_string(&m1).unwrap(),
            serde_json::to_string(&m2).unwrap(),
        );
    } else {
        let err = [&m1, &m2]
            .into_iter()
            .find_map(|m| match m {
                ServerMessage::Error { error, .. } => Some(error),
                _ => None,
            })
            .unwrap();
        assert_eq!(err.code, ErrorCode::InvalidRequest);
        assert!(err.message.contains("in progress"), "unexpected: {err}");
    }
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
    let argv = platform_script("printf recorded", "[Console]::Out.Write('recorded')");
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: argv.clone(),
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
                    assert_eq!(e.argv, argv);
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
        edit_body(
            "run.sh",
            hash_of("#!/bin/sh\necho hi\n"),
            vec![EditSpec {
                old_text: "echo hi".into(),
                new_text: "echo bye".into(),
                replace_all: false,
            }],
        ),
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

    h.send(&req("r", read_body("c.txt", None, None)));
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
        edit_body(
            "c.txt",
            hash,
            vec![EditSpec {
                old_text: "alpha".into(),
                new_text: "beta".into(),
                replace_all: false,
            }],
        ),
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
    h.send(&req("r", read_body("bin.dat", None, None)));
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
    h.send(&req("r", read_body("u.txt", None, None)));
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

#[tokio::test]
#[cfg(unix)]
async fn read_only_ops_dir_surfaces_error() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: running as root");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_dir = log_dir.join("ops");
    std::fs::create_dir_all(&ops_dir).unwrap();
    std::fs::set_permissions(&ops_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;
    h.send(&req(
        "r1",
        RequestBody::Create {
            path: "f.txt".into(),
            content: "x".into(),
        },
    ));
    let m = h.recv().await;
    assert!(
        matches!(m, ServerMessage::Error { .. }),
        "expected error when ops dir is unwritable, got {m:?}"
    );
    std::fs::set_permissions(&ops_dir, std::fs::Permissions::from_mode(0o755)).ok();
}

#[tokio::test]
async fn create_appends_single_committed_record_to_session_shard() {
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

    let ops_dir = root.path().join(".remote-workspace/ops");
    let shards: Vec<_> = std::fs::read_dir(&ops_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(shards.len(), 1);
    let raw = std::fs::read_to_string(&shards[0]).unwrap();
    let lines: Vec<&str> = raw.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("\"record_kind\":\"fs\""));

    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => assert_eq!(operations.len(), 1),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn rejected_exec_replay_in_session_returns_error_not_exit() {
    let mut h = harness().await;
    let body = RequestBody::Exec {
        argv: vec!["true".into()],
        cwd: None,
        profile: Some("nope".into()),
        timeout_ms: Some(10000),
    };
    h.send(&req("bad-exec", body.clone()));
    let first = h.recv_all_for("bad-exec").await;
    assert!(first
        .iter()
        .any(|m| matches!(m, ServerMessage::Error { .. })));

    h.send(&req("bad-exec", body));
    let replay = h.recv_all_for("bad-exec").await;
    assert!(replay
        .iter()
        .any(|m| matches!(m, ServerMessage::Error { .. })));
    assert!(!replay.iter().any(|m| matches!(
        m,
        ServerMessage::Result {
            result: ResultBody::Exec(_),
            ..
        }
    )));
}

#[tokio::test]
async fn utf8_pagination_always_makes_progress() {
    let mut h = harness().await;
    h.send(&req(
        "w",
        RequestBody::Create {
            path: "u.txt".into(),
            content: "é".into(),
        },
    ));
    let _ = h.recv().await;

    h.send(&req("r1", read_body("u.txt", Some(0), Some(1))));
    let m = h.recv().await;
    match m {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
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
            argv: platform_script(
                "i=0; while [ $i -lt 1000000 ]; do echo x; i=$((i+1)); done",
                "while ($true) { [Console]::Out.WriteLine('x') }",
            ),
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

#[tokio::test]
async fn corrupted_middle_log_line_surfaces_io_error_on_history() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_dir = log_dir.join("ops");
    std::fs::create_dir_all(&ops_dir).unwrap();
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
    std::fs::write(
        ops_dir.join("000000000001-000001-0001.jsonl"),
        format!("{valid1}\nNOT JSON\n{valid2}\n"),
    )
    .unwrap();

    // Startup does zero JSONL reads; reading history surfaces the corruption.
    let mut h = harness_at_with(root.path(), log_dir, None).await;
    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Error { error, .. } => {
            assert_eq!(error.code, ErrorCode::IoError);
            assert!(
                error.message.contains("corrupted") || error.message.contains("not valid JSON"),
                "unexpected error: {}",
                error.message
            );
        }
        other => panic!("expected IoError on corrupted middle line, got {other:?}"),
    }
}

#[tokio::test]
async fn crash_truncated_and_mid_utf8_trailing_shard_lines_tolerated() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let ops_dir = log_dir.join("ops");
    std::fs::create_dir_all(&ops_dir).unwrap();

    let valid1 = serde_json::json!({
        "record_kind": "fs",
        "operation_id": "op-1",
        "request_id": "r1",
        "kind": "create",
        "path": "a.txt",
        "after_hash": "sha256:x",
        "timestamp_ms": 1,
    });
    std::fs::write(
        ops_dir.join("000000000001-000001-0001.jsonl"),
        format!("{valid1}\n{{\"record_kind\":\"fs\",\"operation_id\":\"op-2\",\"re"),
    )
    .unwrap();

    let valid2 = r#"{"record_kind":"fs","operation_id":"op-3","request_id":"r3","kind":"create","path":"b.txt","after_hash":"sha256:y","timestamp_ms":2}"#;
    let mut raw = valid2.as_bytes().to_vec();
    raw.push(b'\n');
    raw.extend_from_slice(&[0xE6]);
    std::fs::write(ops_dir.join("000000000002-000001-0002.jsonl"), &raw).unwrap();

    let mut h = harness_at_with(root.path(), log_dir, None).await;
    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 2);
            assert_eq!(operations[0].operation_id(), "op-1");
            assert_eq!(operations[1].operation_id(), "op-3");
        }
        other => panic!("unexpected: {other:?}"),
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

#[tokio::test]
async fn legacy_operations_jsonl_and_prepared_markers_merge_and_compact_on_gc() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
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
    let aborted = serde_json::json!({
        "record_kind": "aborted",
        "operation_id": "op-99",
        "timestamp_ms": 4,
    });
    // Include no trailing newline on the last line to verify legacy tail tolerance.
    std::fs::write(
        log_dir.join("operations.jsonl"),
        format!("{op1}\n{prepared}\n{committed}\n{aborted}"),
    )
    .unwrap();

    let mut h = harness_at_with(root.path(), log_dir.clone(), None).await;
    h.send(&req(
        "new-w",
        RequestBody::Create {
            path: "b.txt".into(),
            content: "hello".into(),
        },
    ));
    let new_op = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(m),
            ..
        } => m.operation_id,
        other => panic!("unexpected: {other:?}"),
    };

    h.send(&req("hist", RequestBody::History { limit: None }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => {
            assert_eq!(operations.len(), 3);
            assert_eq!(operations[0].operation_id(), "op-1");
            assert_eq!(operations[1].operation_id(), "op-2");
            assert_eq!(operations[2].operation_id(), new_op);
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Gc compacts legacy operations.jsonl into ops/ while keeping h's live shard untouched.
    h.send(&req("gc", RequestBody::Gc { keep: Some(2) }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Gc(g),
            ..
        } => {
            assert_eq!(g.removed_operations, 1);
            assert_eq!(g.retained_operations, 2);
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(!log_dir.join("operations.jsonl").exists());

    h.send(&req(
        "gone",
        RequestBody::OperationGet {
            operation_id: "op-1".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Error { error, .. } => assert_eq!(error.code, ErrorCode::OperationNotFound),
        other => panic!("pruned op-1 must be gone: {other:?}"),
    }
}

#[tokio::test]
async fn gc_uses_configured_history_limit_without_startup_prune() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
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
                edit_body(
                    "f.txt",
                    hash_of(old_c),
                    vec![EditSpec {
                        old_text: old_c.into(),
                        new_text: new_c.into(),
                        replace_all: false,
                    }],
                ),
            ));
            let _ = h.recv().await;
        }
        h.shutdown().await;
    }

    // Startup with <= 64 shards does 0 history I/O (all 3 remain until gc).
    let server = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: log_dir,
        config_path: None,
        history_limit: Some(1),
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();
    assert_eq!(server.store.history(None).unwrap().len(), 3);

    let (mut cw, sr) = tokio::io::duplex(1 << 16);
    let (sw, cr) = tokio::io::duplex(1 << 16);
    let task = tokio::spawn(server.run(sr, sw));
    let mut line = serde_json::to_string(&req("gc", RequestBody::Gc { keep: None })).unwrap();
    line.push('\n');
    cw.write_all(line.as_bytes()).await.unwrap();
    drop(cw);

    let mut reader = BufReader::new(cr).lines();
    let reply: ServerMessage =
        serde_json::from_str(&reader.next_line().await.unwrap().unwrap()).unwrap();
    match reply {
        ServerMessage::Result {
            result: ResultBody::Gc(g),
            ..
        } => {
            assert_eq!(g.removed_operations, 2);
            assert_eq!(g.retained_operations, 1);
        }
        other => panic!("unexpected: {other:?}"),
    }
    task.await.unwrap().unwrap();
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
    let del_op = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(w),
            ..
        } => {
            assert!(w.operation_id.starts_with("op-"));
            assert_eq!(w.old_hash, Some(hash_of("keep me")));
            assert_eq!(w.new_hash, "sha256:");
            w.operation_id
        }
        other => panic!("unexpected: {other:?}"),
    };
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
                    if f.operation_id == del_op && matches!(f.kind, OperationKind::Delete))
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
            argv: verbatim_argv(&h.root_path, hostile),
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
    #[cfg(unix)]
    let cfg = r#"
[profiles.echoer]
shell = ["echo"]
setup = ""
"#;
    #[cfg(windows)]
    let script_dir = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    let cfg_text = format!(
        "[profiles.echoer]\nshell = {}\nsetup = \"\"\n",
        toml::Value::from(echo_last_arg_argv(script_dir.path()))
    );
    #[cfg(windows)]
    let cfg = cfg_text.as_str();
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

// The command a Windows profile runs is found the way PowerShell finds it --
// `.cmd` shims included, on the PATH the setup left -- and starts where the
// setup's Set-Location left it, as `cd` in a Unix setup would.
#[cfg(windows)]
#[tokio::test]
async fn windows_profile_finds_cmd_shims_and_follows_set_location() {
    let tools = tempfile::tempdir().unwrap();
    std::fs::write(tools.path().join("shim.cmd"), "@echo shim ran in %CD%\r\n").unwrap();
    let setup = format!(
        "$env:PATH = '{};' + $env:PATH; Set-Location sub",
        tools.path().display()
    );
    let cfg = format!(
        "[profiles.p]\nsetup = {}\n",
        toml::Value::from(setup.as_str())
    );
    let mut h = harness_with_config(Some(&cfg)).await;
    std::fs::create_dir(h.root_path.join("sub")).unwrap();
    h.send(&req(
        "e",
        RequestBody::Exec {
            argv: vec!["shim".into()],
            cwd: None,
            profile: Some("p".into()),
            timeout_ms: Some(10000),
        },
    ));
    let msgs = h.recv_all_for("e").await;
    match &msgs[0] {
        ServerMessage::Result {
            result: ResultBody::Exec(r),
            ..
        } => {
            assert_eq!(r.termination, ExecTermination::Exited { code: 0 }, "{r:?}");
            let line = r.stdout.prefix.trim();
            assert!(line.starts_with("shim ran in "), "{line:?}");
            assert!(line.ends_with(r"\sub"), "{line:?}");
        }
        other => panic!("unexpected: {other:?}"),
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

#[tokio::test]
async fn legacy_undo_state_still_loads_and_gc_reclaims_legacy_files() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join(".remote-workspace");
    let blobs_dir = log_dir.join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    std::fs::write(root.path().join("f.txt"), "restored\n").unwrap();
    std::fs::write(blobs_dir.join("op-1.before"), "restored\n").unwrap();
    std::fs::write(log_dir.join("requests.jsonl"), "legacy\n").unwrap();
    std::fs::write(log_dir.join("op-counter"), "2\n").unwrap();
    std::fs::write(log_dir.join("lock"), "1234\n").unwrap();
    std::fs::write(log_dir.join("pending_uploads.json"), "{}\n").unwrap();

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

    h.send(&req("gc", RequestBody::Gc { keep: Some(10) }));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Gc(g),
            ..
        } => {
            assert_eq!(g.retained_operations, 1);
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(!blobs_dir.exists(), "legacy blobs must be reclaimed");
    assert!(!log_dir.join("requests.jsonl").exists());
    assert!(!log_dir.join("op-counter").exists());
    assert!(!log_dir.join("lock").exists());
    assert!(!log_dir.join("pending_uploads.json").exists());
    assert!(!log_dir.join("operations.jsonl").exists());
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
            argv: sleep_argv(800),
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

#[tokio::test]
async fn multi_session_servers_share_root_and_state_dir_concurrently() {
    let root = tempfile::tempdir().unwrap();
    let state_dir = root.path().join(".remote-workspace");

    // Zero startup writes to ops/ or pending_uploads/.
    let s0 = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: state_dir.clone(),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();
    assert!(!state_dir.join("ops").exists());
    assert!(!state_dir.join("pending_uploads").exists());
    assert!(!state_dir.join("lock").exists());
    drop(s0);

    // Two live servers running simultaneously on the exact same root + state_dir.
    let mut h1 = harness_at_with(root.path(), state_dir.clone(), None).await;
    let mut h2 = harness_at_with(root.path(), state_dir.clone(), None).await;

    h1.send(&req(
        "c1",
        RequestBody::Create {
            path: "shared.txt".into(),
            content: "v1\n".into(),
        },
    ));
    let base_hash = match h1.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(m),
            ..
        } => m.new_hash,
        other => panic!("h1 create failed: {other:?}"),
    };

    h2.send(&req(
        "c2",
        RequestBody::Create {
            path: "peer.txt".into(),
            content: "from-h2\n".into(),
        },
    ));
    assert!(matches!(h2.recv().await, ServerMessage::Result { .. }));

    // Concurrent exec on h2 while h1 reads history.
    h2.send(&req(
        "e2",
        RequestBody::Exec {
            argv: platform_script("printf ok", "[Console]::Out.Write('ok')"),
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ));
    let exec_msgs = h2.recv_all_for("e2").await;
    assert!(matches!(
        &exec_msgs[0],
        ServerMessage::Result {
            result: ResultBody::Exec(_),
            ..
        }
    ));

    // Both servers see all 3 operations across both live shards.
    h1.send(&req("hist1", RequestBody::History { limit: None }));
    match h1.recv().await {
        ServerMessage::Result {
            result: ResultBody::History { operations },
            ..
        } => assert_eq!(operations.len(), 3),
        other => panic!("unexpected: {other:?}"),
    }

    // Concurrent conflicting edits on shared.txt with the same base_hash:
    // exactly one succeeds and the other returns STALE_FILE.
    h1.send(&req(
        "race1",
        RequestBody::Edit {
            path: "shared.txt".into(),
            base_hash: base_hash.clone(),
            edits: Vec::new(),
            replace_content: Some("from-h1\n".into()),
        },
    ));
    h2.send(&req(
        "race2",
        RequestBody::Edit {
            path: "shared.txt".into(),
            base_hash: base_hash.clone(),
            edits: Vec::new(),
            replace_content: Some("from-h2-conflict\n".into()),
        },
    ));
    let r1 = h1.recv().await;
    let r2 = h2.recv().await;
    let mut ok_count = 0;
    let mut stale_count = 0;
    for msg in [r1, r2] {
        match msg {
            ServerMessage::Result {
                result: ResultBody::Mutation(_),
                ..
            } => ok_count += 1,
            ServerMessage::Error { error, .. } if error.code == ErrorCode::StaleFile => {
                stale_count += 1;
            }
            other => panic!("unexpected race outcome: {other:?}"),
        }
    }
    assert_eq!(ok_count, 1, "exactly one conflicting edit must win");
    assert_eq!(
        stale_count, 1,
        "losing conflicting edit must get STALE_FILE"
    );

    // Gc on h1 while h2 is still live must NOT delete h2's live shard.
    h1.send(&req("gc1", RequestBody::Gc { keep: Some(10) }));
    assert!(matches!(
        h1.recv().await,
        ServerMessage::Result {
            result: ResultBody::Gc(GcResult {
                retained_operations: 4,
                ..
            }),
            ..
        }
    ));

    h1.shutdown().await;
    h2.shutdown().await;
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
            argv: platform_script(
                "sleep 0.4; echo done",
                "Start-Sleep -Milliseconds 400; [Console]::Out.WriteLine('done')",
            ),
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
            argv: sleep_argv(60_000),
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
            argv: platform_script(
                "sleep 0.4; echo done",
                "Start-Sleep -Milliseconds 400; [Console]::Out.WriteLine('done')",
            ),
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
            argv: sleep_argv(60_000),
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
            argv: sleep_argv(900),
            cwd: None,
            profile: None,
            timeout_ms: Some(10_000),
        },
    ))
    .unwrap();
    busy.push('\n');
    writer.write_all(busy.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    let line = serde_json::to_string(&req("split", read_body("f.txt", None, None))).unwrap();
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

// A request this server cannot parse still gets a reply its sender can match.
// An op from a newer client used to be answered under "(parse-error)", leaving
// the caller waiting on its own id until it timed out.
#[tokio::test]
async fn unreadable_requests_are_answered_under_their_own_id() {
    let mut h = harness().await;
    h.req_tx
        .send("{\"request_id\":\"future-1\",\"op\":\"teleport\"}\n".into())
        .unwrap();
    match h.recv().await {
        ServerMessage::Error { request_id, error } => {
            assert_eq!(request_id, "future-1");
            assert_eq!(error.code, ErrorCode::InvalidRequest);
        }
        other => panic!("expected an error, got {other:?}"),
    }
    // Windows PowerShell can prefix what it writes with a byte order mark.
    h.req_tx
        .send("\u{feff}{\"request_id\":\"bom-1\",\"op\":\"stat\",\"path\":\".\"}\n".into())
        .unwrap();
    assert!(matches!(
        h.recv().await,
        ServerMessage::Result { request_id, .. } if request_id == "bom-1"
    ));
}

#[tokio::test]
async fn list_offset_past_the_end_is_refused() {
    let mut h = harness().await;
    std::fs::write(h.root_path.join("a"), "a").unwrap();
    h.send(&req(
        "l",
        RequestBody::List {
            path: ".".into(),
            offset: Some(50),
            limit: None,
        },
    ));
    match h.recv().await {
        ServerMessage::Error { error, .. } => assert_eq!(error.code, ErrorCode::InvalidRequest),
        other => panic!("a page past the end must not look like an empty directory: {other:?}"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn stat_reports_a_symlink_as_one() {
    let mut h = harness().await;
    std::fs::write(h.root_path.join("target.txt"), "abc").unwrap();
    std::os::unix::fs::symlink("target.txt", h.root_path.join("link")).unwrap();
    h.send(&req(
        "s",
        RequestBody::Stat {
            path: "link".into(),
        },
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Stat { stat },
            ..
        } => {
            assert_eq!(stat.kind, ListKind::Symlink);
            assert_eq!(stat.size, 3);
            assert_eq!(stat.hash, Some(hash_of("abc")));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// One request that is not UTF-8 is a bad request, answered like any other --
// not a broken stream that takes the server and everything in flight with it.
#[tokio::test]
async fn a_request_line_that_is_not_utf8_is_refused_without_stopping_the_server() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::new(ServerOptions {
        root: root.path().to_path_buf(),
        state_dir: root.path().join(".remote-workspace"),
        config_path: None,
        history_limit: None,
        scratch_max_age: None,
        idle_timeout: None,
    })
    .unwrap();
    let (client, server_io) = tokio::io::duplex(1 << 16);
    let (server_read, server_write) = tokio::io::split(server_io);
    tokio::spawn(server.run(server_read, server_write));
    let (client_read, mut client_write) = tokio::io::split(client);

    let mut bad = serde_json::to_vec(&req(
        "bad",
        RequestBody::Stat {
            path: "cafX".into(),
        },
    ))
    .unwrap();
    let x = bad.iter().position(|b| *b == b'X').unwrap();
    bad[x] = 0xE9;
    bad.push(b'\n');
    client_write.write_all(&bad).await.unwrap();
    let mut good =
        serde_json::to_vec(&req("good", RequestBody::Stat { path: ".".into() })).unwrap();
    good.push(b'\n');
    client_write.write_all(&good).await.unwrap();

    let mut lines = BufReader::new(client_read).lines();
    let first: ServerMessage =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    match first {
        ServerMessage::Error { request_id, error } => {
            assert_eq!(request_id, "bad");
            assert_eq!(error.code, ErrorCode::InvalidRequest);
        }
        other => panic!("unexpected: {other:?}"),
    }
    let second: ServerMessage =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(
        matches!(&second, ServerMessage::Result { request_id, .. } if request_id == "good"),
        "{second:?}"
    );
}

#[tokio::test]
async fn server_info_reports_metadata_sorted_profiles_and_unknown_profile_hint() {
    let cfg = r#"
default_profile = "gpu"

[profiles.zeta]
setup = ""

[profiles.alpha]
setup = ""

[profiles.gpu]
setup = ""
"#;
    let mut h = harness_with_config(Some(cfg)).await;
    h.send(&req("info", RequestBody::ServerInfo {}));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::ServerInfo(info),
            ..
        } => {
            assert_eq!(info.protocol_version, PROTOCOL_VERSION);
            assert_eq!(info.software_version, env!("CARGO_PKG_VERSION"));
            assert_eq!(
                info.platform,
                format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
            );
            assert_eq!(info.default_profile.as_deref(), Some("gpu"));
            assert_eq!(info.profiles, vec!["alpha", "gpu", "zeta"]);
        }
        other => panic!("unexpected: {other:?}"),
    }

    h.send(&req(
        "bad-prof",
        RequestBody::Exec {
            argv: vec!["true".into()],
            cwd: None,
            profile: Some("missing".into()),
            timeout_ms: Some(10_000),
        },
    ));
    match h.recv().await {
        ServerMessage::Error { error, .. } => {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            assert_eq!(
                error.message,
                "unknown profile 'missing'; available profiles: alpha, gpu, zeta (default: gpu)"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn wire_read_line_tail_and_edit_replace_content() {
    let mut h = harness().await;
    h.send(&req(
        "c",
        RequestBody::Create {
            path: "notes.txt".into(),
            content: "one\ntwo\nthree\nfour\n".into(),
        },
    ));
    let h1 = match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(m),
            ..
        } => m.new_hash,
        other => panic!("unexpected: {other:?}"),
    };

    // Line mode via wire.
    h.send(&req(
        "rl",
        ReadOptions {
            start_line: Some(2),
            max_lines: Some(2),
            ..Default::default()
        }
        .into_request_body("notes.txt".into()),
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
            assert_eq!(r.content, "two\nthree\n");
            assert_eq!(r.start_line, Some(2));
            assert_eq!(r.end_line, Some(3));
            assert_eq!(r.total_lines, Some(4));
            assert_eq!(r.next_line, Some(4));
            assert!(r.truncated);
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Tail mode via wire.
    h.send(&req(
        "rt",
        ReadOptions {
            tail_lines: Some(2),
            ..Default::default()
        }
        .into_request_body("notes.txt".into()),
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Read(r),
            ..
        } => {
            assert_eq!(r.content, "three\nfour\n");
            assert!(r.truncated);
            assert_eq!(r.next_offset, None);
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Whole-file replace_content via wire.
    h.send(&req(
        "rep",
        RequestBody::Edit {
            path: "notes.txt".into(),
            base_hash: h1,
            edits: Vec::new(),
            replace_content: Some("brand new\n".into()),
        },
    ));
    match h.recv().await {
        ServerMessage::Result {
            result: ResultBody::Mutation(m),
            ..
        } => {
            assert_eq!(m.new_hash, hash_of("brand new\n"));
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(
        std::fs::read_to_string(h.root_path.join("notes.txt")).unwrap(),
        "brand new\n"
    );
}
