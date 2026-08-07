use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
#[cfg(windows)]
fn idle_timeout_exits_even_while_stdin_pipe_remains_open() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_remote-workspace-server"))
        .args([
            "--root",
            workspace.path().to_str().unwrap(),
            "--state-base",
            state.path().to_str().unwrap(),
            "--idle-timeout-secs",
            "1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let stderr = child.wait_with_output().unwrap().stderr;
            panic!(
                "server did not exit after idle timeout: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn transfer_receiver_finishes_at_declared_size_without_stdin_eof() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("upload.part");
    std::fs::write(&staging, []).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_remote-workspace-server"))
        .args([
            "--transfer-receive",
            staging.to_str().unwrap(),
            "--expect-size",
            "4",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"data").unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("receiver did not finish after its declared byte count");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut output = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    assert!(output.contains("\"size\":4"));
    assert_eq!(std::fs::read(staging).unwrap(), b"data");
    drop(stdin);
}

#[test]
fn base64_transfer_receiver_finishes_without_stdin_eof() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("upload.part");
    std::fs::write(&staging, []).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_remote-workspace-server"))
        .args([
            "--transfer-receive",
            staging.to_str().unwrap(),
            "--expect-size",
            "4",
            "--transfer-base64",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    // Two short, independently framed chunks exercise partial sender reads.
    stdin.write_all(b"ZGE=\ndGE=\n").unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("Base64 receiver did not finish after its declared byte count");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(std::fs::read(staging).unwrap(), b"data");
    drop(stdin);
}
