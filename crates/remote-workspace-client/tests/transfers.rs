use std::path::{Path, PathBuf};

use remote_workspace_client::{
    download_file, upload_file, ArgvTransport, Client, ClientError, Endpoint,
};
use remote_workspace_protocol::{AnyOperationRecord, ErrorCode, TransferDirection};

fn server_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/remote-workspace-server");
    p.canonicalize().unwrap_or(p)
}

fn endpoint(root: &Path) -> Endpoint {
    Endpoint::Local {
        server_bin: server_bin().to_string_lossy().into_owned(),
        root: root.to_string_lossy().into_owned(),
        state_base: Some(
            root.join(".remote-workspace-test")
                .to_string_lossy()
                .into_owned(),
        ),
        config: None,
    }
}

async fn connect(ep: &Endpoint) -> Client {
    assert!(server_bin().exists(), "build remote-workspace-server first");
    Client::connect(
        ArgvTransport {
            argv: ep.control_argv(),
        },
        None,
    )
    .await
    .expect("connect")
}

fn part_files(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "part"))
        .collect()
}

fn sha256_of(bytes: &[u8]) -> String {
    use sha2::Digest;
    format!("sha256:{}", hex::encode(sha2::Sha256::digest(bytes)))
}

/// The stall window is read from a process-wide environment variable, so the
/// tests that shorten it must not overlap: one clearing the variable while the
/// other is mid-transfer would silently restore the two-minute default and
/// leave that test waiting on it.
static STALL_ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Shorten the stall window for the duration of the guard. The lock is held,
/// never read: it exists to keep these tests from overlapping.
struct ShortStallWindow {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl ShortStallWindow {
    fn ms(ms: u64) -> Self {
        let lock = STALL_ENV.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("REMOTE_WORKSPACE_STALL_TIMEOUT_MS", ms.to_string());
        Self { _lock: lock }
    }
}

impl Drop for ShortStallWindow {
    fn drop(&mut self) {
        std::env::remove_var("REMOTE_WORKSPACE_STALL_TIMEOUT_MS");
    }
}

#[tokio::test]
async fn upload_download_small_text_roundtrip() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let content = b"hello transfer\n";
    let src = local.path().join("src.txt");
    std::fs::write(&src, content).unwrap();

    let up = upload_file(&client, &ep, &src, "data.txt", false)
        .await
        .unwrap();
    assert!(up.operation_id.starts_with("op-"));
    assert!(matches!(up.direction, TransferDirection::Upload));
    assert_eq!(up.path, "data.txt");
    assert_eq!(up.size, content.len() as u64);
    assert_eq!(up.sha256, sha256_of(content));
    assert_eq!(
        std::fs::read(remote.path().join("data.txt")).unwrap(),
        content
    );

    let dest = local.path().join("back.txt");
    let down = download_file(&client, &ep, "data.txt", &dest, false)
        .await
        .unwrap();
    assert!(matches!(down.direction, TransferDirection::Download));
    assert_eq!(down.sha256, up.sha256);
    assert_eq!(std::fs::read(&dest).unwrap(), content);
    assert!(part_files(local.path()).is_empty());
    assert!(part_files(remote.path()).is_empty());
}

#[tokio::test]
async fn binary_larger_than_buffer_roundtrips_via_scratch() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // > 64 KiB streaming buffer, includes NUL bytes and invalid UTF-8.
    let content: Vec<u8> = (0..300_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    assert!(content.contains(&0u8) && content.contains(&0xFFu8));
    assert!(String::from_utf8(content.clone()).is_err());

    let src = local.path().join("blob.bin");
    std::fs::write(&src, &content).unwrap();

    let up = upload_file(&client, &ep, &src, "@scratch/blob.bin", false)
        .await
        .unwrap();
    assert_eq!(up.path, "@scratch/blob.bin");
    assert_eq!(up.size, content.len() as u64);
    assert_eq!(up.sha256, sha256_of(&content));

    let dest = local.path().join("blob-back.bin");
    let down = download_file(&client, &ep, "@scratch/blob.bin", &dest, false)
        .await
        .unwrap();
    assert_eq!(down.size, up.size);
    assert_eq!(down.sha256, up.sha256);
    assert_eq!(std::fs::read(&dest).unwrap(), content);
}

#[tokio::test]
async fn upload_refuses_then_overwrites_existing_target() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let src = local.path().join("v.txt");
    std::fs::write(&src, "v1").unwrap();
    upload_file(&client, &ep, &src, "v.txt", false)
        .await
        .unwrap();

    std::fs::write(&src, "v2").unwrap();
    let err = upload_file(&client, &ep, &src, "v.txt", false)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => {
            assert_eq!(e.code, ErrorCode::InvalidRequest);
            assert!(e.message.contains("overwrite"), "message: {}", e.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        std::fs::read_to_string(remote.path().join("v.txt")).unwrap(),
        "v1"
    );

    upload_file(&client, &ep, &src, "v.txt", true)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(remote.path().join("v.txt")).unwrap(),
        "v2"
    );
    assert!(part_files(remote.path()).is_empty());
}

#[tokio::test]
async fn download_refuses_then_overwrites_existing_target() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    std::fs::write(remote.path().join("r.txt"), "remote").unwrap();
    let dest = local.path().join("d.txt");
    std::fs::write(&dest, "precious local").unwrap();

    let err = download_file(&client, &ep, "r.txt", &dest, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ClientError::Transfer(m) if m.contains("overwrite")),
        "unexpected error: {err:?}"
    );
    assert_eq!(std::fs::read_to_string(&dest).unwrap(), "precious local");

    download_file(&client, &ep, "r.txt", &dest, true)
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&dest).unwrap(), "remote");
}

#[tokio::test]
async fn upload_missing_parents_and_sources_fail() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let src = local.path().join("s.txt");
    std::fs::write(&src, "x").unwrap();

    // Remote parent directory missing: no implicit mkdir.
    let err = upload_file(&client, &ep, &src, "no_such_dir/f.txt", false)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => assert_eq!(e.code, ErrorCode::NotFound),
        other => panic!("unexpected error: {other:?}"),
    }

    // Local source missing.
    let err = upload_file(&client, &ep, &local.path().join("nope.txt"), "f.txt", false)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transfer(_)));

    // Local source is a directory.
    let err = upload_file(&client, &ep, local.path(), "f.txt", false)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ClientError::Transfer(m) if m.contains("regular file")),
        "unexpected error: {err:?}"
    );
    assert!(!remote.path().join("f.txt").exists());
}

#[tokio::test]
async fn upload_rejects_symlink_ancestor_escape() {
    let remote = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), remote.path().join("escape")).unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let src = local.path().join("s.txt");
    std::fs::write(&src, "x").unwrap();
    let err = upload_file(&client, &ep, &src, "escape/f.txt", false)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => assert_eq!(e.code, ErrorCode::PathOutsideRoot),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn download_missing_source_dir_source_and_missing_parent_fail() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // Missing remote source: the sender exits nonzero before any framing.
    let err = download_file(
        &client,
        &ep,
        "missing.bin",
        &local.path().join("d.bin"),
        false,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ClientError::Transfer(_)));

    // Remote source is a directory.
    std::fs::create_dir(remote.path().join("adir")).unwrap();
    let err = download_file(&client, &ep, "adir", &local.path().join("d2.bin"), false)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transfer(_)));

    // Local parent directory missing.
    std::fs::write(remote.path().join("ok.bin"), "x").unwrap();
    let err = download_file(
        &client,
        &ep,
        "ok.bin",
        &local.path().join("no_dir/d.bin"),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, ClientError::Transfer(m) if m.contains("parent")),
        "unexpected error: {err:?}"
    );
    assert!(part_files(local.path()).is_empty());
}

#[tokio::test]
async fn commit_size_mismatch_does_not_install_and_abort_cleans_staging() {
    let remote = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let prep = client.upload_prepare("m.bin", false).await.unwrap();
    let staging = PathBuf::from(&prep.staging_path);
    assert!(staging.exists());
    std::fs::write(&staging, b"12345").unwrap();

    // Declared size disagrees with the staged bytes: commit must refuse.
    let err = client
        .upload_commit(&prep.transfer_id, 999, "sha256:bogus", 1)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => {
            assert_eq!(e.code, ErrorCode::InvalidRequest);
            assert!(e.message.contains("declared"), "message: {}", e.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(!remote.path().join("m.bin").exists());
    assert!(
        staging.exists(),
        "failed commit must keep staging for abort"
    );

    client.upload_abort(&prep.transfer_id).await.unwrap();
    assert!(!staging.exists());
    assert!(part_files(remote.path()).is_empty());

    // The transfer id is gone after abort.
    let err = client
        .upload_commit(&prep.transfer_id, 5, "sha256:x", 1)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => assert_eq!(e.code, ErrorCode::OperationNotFound),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn no_overwrite_commit_loses_race_to_concurrent_creation() {
    let remote = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let prep = client.upload_prepare("raced.bin", false).await.unwrap();
    std::fs::write(&prep.staging_path, b"staged").unwrap();
    // Someone else creates the target between prepare and commit.
    std::fs::write(remote.path().join("raced.bin"), b"winner").unwrap();

    let err = client
        .upload_commit(&prep.transfer_id, 6, &sha256_of(b"staged"), 1)
        .await
        .unwrap_err();
    match err {
        ClientError::Server(e) => assert_eq!(e.code, ErrorCode::InvalidRequest),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        std::fs::read(remote.path().join("raced.bin")).unwrap(),
        b"winner"
    );
    client.upload_abort(&prep.transfer_id).await.unwrap();
    assert!(part_files(remote.path()).is_empty());
}

// A data-plane child that dies mid-transfer must not install anything and
// must not leak temp files. The stub binary stands in for a crashed
// receiver/sender (e.g. ssh dropping); the control plane stays real.
#[tokio::test]
async fn child_death_mid_transfer_leaves_no_target_or_temp_files() {
    use std::os::unix::fs::PermissionsExt;

    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // Stub sender: announces 100 bytes, delivers 10, dies.
    let stub = local.path().join("stub.sh");
    std::fs::write(
        &stub,
        "#!/bin/sh\necho '{\"size\":100}'\nprintf 'aaaaaaaaaa'\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    let broken_ep = Endpoint::Local {
        server_bin: stub.to_string_lossy().into_owned(),
        root: remote.path().to_string_lossy().into_owned(),
        state_base: None,
        config: None,
    };

    std::fs::write(remote.path().join("r.bin"), vec![7u8; 100]).unwrap();
    let dest = local.path().join("d.bin");
    let err = download_file(&client, &broken_ep, "r.bin", &dest, false)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transfer(_)));
    // A broken transfer must say how far it got, so "died immediately" and
    // "died near the end" are distinguishable without a second connection.
    let text = err.to_string();
    assert!(
        text.contains("ended early") && text.contains("MiB"),
        "a truncated download must report its byte position: {text}"
    );
    assert!(!dest.exists(), "target must not appear after a dead sender");
    assert!(part_files(local.path()).is_empty());

    // Stub receiver: consumes nothing and dies. The upload must fail and the
    // remote staging file must be cleaned via upload_abort.
    let src = local.path().join("s.bin");
    std::fs::write(&src, vec![9u8; 200_000]).unwrap();
    let err = upload_file(&client, &broken_ep, &src, "u.bin", false)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transfer(_)));
    let text = err.to_string();
    assert!(
        text.contains("MiB") && text.contains("KiB/s"),
        "a failed upload must report position and rate: {text}"
    );
    assert!(!remote.path().join("u.bin").exists());
    assert!(part_files(remote.path()).is_empty());
}

// Drive the raw receiver binary directly: a byte count that differs from
// --expect-size must fail, and a missing staging file (lifecycle bypass) must
// be rejected.
#[test]
fn raw_receiver_rejects_size_mismatch_and_missing_staging() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("s.part");
    std::fs::write(&staging, b"").unwrap();

    let mut child = Command::new(server_bin())
        .args([
            "--transfer-receive",
            staging.to_str().unwrap(),
            "--expect-size",
            "10",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"1234").unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success(), "short stream must fail");
    assert!(out.stdout.is_empty(), "no metadata on failure");

    let out = Command::new(server_bin())
        .args([
            "--transfer-receive",
            dir.path().join("absent.part").to_str().unwrap(),
            "--expect-size",
            "0",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success(), "missing staging file must fail");
}

#[tokio::test]
async fn transfers_appear_in_history_without_undo_or_leaked_paths() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    let marker = "SECRET-CONTENT-MARKER-0451";
    let src = local.path().join("secret-local-name.txt");
    std::fs::write(&src, marker).unwrap();

    let up = upload_file(&client, &ep, &src, "pub.txt", false)
        .await
        .unwrap();
    let dest = local.path().join("secret-dest-name.txt");
    let down = download_file(&client, &ep, "pub.txt", &dest, false)
        .await
        .unwrap();

    // Both transfers are in history and resolvable via operation_get.
    let history = client.history(None).await.unwrap();
    let transfers: Vec<_> = history
        .iter()
        .filter_map(|r| match r {
            AnyOperationRecord::Transfer(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(transfers.len(), 2);
    assert!(matches!(transfers[0].direction, TransferDirection::Upload));
    assert!(matches!(
        transfers[1].direction,
        TransferDirection::Download
    ));
    let d = client.operation_get(&up.operation_id).await.unwrap();
    assert_eq!(d.record.operation_id(), up.operation_id);

    // Transfers cannot be undone.
    for op in [&up.operation_id, &down.operation_id] {
        let err = client.undo(op).await.unwrap_err();
        match err {
            ClientError::Server(e) => {
                assert!(e.message.contains("undo"), "message: {}", e.message)
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // The server-side logs contain neither local paths nor file content nor
    // staging paths.
    let state_root = remote.path().join(".remote-workspace-test").join("state");
    let state_dir = std::fs::read_dir(&state_root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    for log in ["operations.jsonl", "requests.jsonl"] {
        let text = std::fs::read_to_string(state_dir.join(log)).unwrap();
        assert!(!text.contains(marker), "{log} leaked file content");
        assert!(
            !text.contains("secret-local-name") && !text.contains("secret-dest-name"),
            "{log} leaked a local path"
        );
        assert!(!text.contains(".part"), "{log} leaked a staging path");
    }
}

fn vm_hwm_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap()
}

// Manual check that memory stays flat for large files:
//   cargo test -p remote-workspace-client --test transfers -- --ignored
#[tokio::test]
#[ignore]
async fn large_file_roundtrip_uses_bounded_memory() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // 256 MiB of non-constant data, written in 1 MiB chunks.
    let src = local.path().join("big.bin");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&src).unwrap();
        let mut chunk = vec![0u8; 1 << 20];
        for i in 0..256u32 {
            chunk
                .iter_mut()
                .enumerate()
                .for_each(|(j, b)| *b = (j as u32).wrapping_mul(i + 1) as u8);
            f.write_all(&chunk).unwrap();
        }
    }
    let before_kib = vm_hwm_kib();

    let up = upload_file(&client, &ep, &src, "@scratch/big.bin", false)
        .await
        .unwrap();
    assert_eq!(up.size, 256 << 20);

    let dest = local.path().join("big-back.bin");
    let down = download_file(&client, &ep, "@scratch/big.bin", &dest, false)
        .await
        .unwrap();
    assert_eq!(down.sha256, up.sha256);
    assert_eq!(
        std::fs::metadata(&dest).unwrap().len(),
        std::fs::metadata(&src).unwrap().len()
    );

    let grown_kib = vm_hwm_kib().saturating_sub(before_kib);
    assert!(
        grown_kib < 64 * 1024,
        "client peak RSS grew by {grown_kib} KiB during a 256 MiB roundtrip"
    );
}

// A sender that announces bytes, delivers some, then goes silent without
// closing the stream. Before stall detection this hung forever and was
// indistinguishable from a slow link; it must now fail with a stalled-transfer
// error that reports how far it got.
#[tokio::test]
async fn a_silent_sender_is_reported_as_stalled_not_hung() {
    use std::os::unix::fs::PermissionsExt;

    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // 1 MiB announced, 64 KiB delivered, then silence past the stall window.
    // `exec` matters: the sleep must REPLACE the shell rather than be its
    // child, so killing the transfer child actually releases the pipe instead
    // of leaving an orphan holding stdout open.
    let stub = local.path().join("silent.sh");
    std::fs::write(
        &stub,
        "#!/bin/sh\necho '{\"size\":1048576}'\nhead -c 65536 /dev/zero\nexec sleep 600\n",
    )
    .unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    let silent_ep = Endpoint::Local {
        server_bin: stub.to_string_lossy().into_owned(),
        root: remote.path().to_string_lossy().into_owned(),
        state_base: None,
        config: None,
    };

    let _window = ShortStallWindow::ms(1500);
    let dest = local.path().join("stalled.bin");
    let started = std::time::Instant::now();
    let err = download_file(&client, &silent_ep, "whatever.bin", &dest, false)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    let text = err.to_string();
    assert!(
        text.contains("transfer_stalled"),
        "must carry the stable code: {text}"
    );
    // The whole point: the message says it *was* moving, and how far it got.
    assert!(
        text.contains("0.1/1.0 MiB"),
        "must report progress so a stall is distinguishable from a dead link: {text}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "must give up on the stall window, not hang: took {elapsed:?}"
    );
    assert!(!dest.exists(), "no partial target may be left behind");
    assert!(part_files(local.path()).is_empty());
}

// The upload direction needs the same guarantee: a receiver that stops reading
// wedges the write once the pipe buffer fills. Without a stall window this
// blocks forever with the staging file still reserved on the remote.
#[tokio::test]
async fn a_receiver_that_stops_reading_is_reported_as_stalled() {
    use std::os::unix::fs::PermissionsExt;

    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let ep = endpoint(remote.path());
    let client = connect(&ep).await;

    // Never reads stdin and never exits; `exec` so killing the child really
    // releases the pipe instead of orphaning a sleep that still holds it.
    let stub = local.path().join("deaf.sh");
    std::fs::write(&stub, "#!/bin/sh\nexec sleep 600\n").unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    let deaf_ep = Endpoint::Local {
        server_bin: stub.to_string_lossy().into_owned(),
        root: remote.path().to_string_lossy().into_owned(),
        state_base: None,
        config: None,
    };

    // Comfortably larger than a pipe buffer, so the write blocks part-way.
    let src = local.path().join("big.bin");
    std::fs::write(&src, vec![3u8; 4 * 1024 * 1024]).unwrap();

    let _window = ShortStallWindow::ms(1500);
    let started = std::time::Instant::now();
    let err = upload_file(&client, &deaf_ep, &src, "u.bin", false)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    let text = err.to_string();
    assert!(
        text.contains("transfer_stalled"),
        "must carry the stable code: {text}"
    );
    assert!(
        text.contains("MiB") && text.contains("KiB/s"),
        "must report how far it got: {text}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "must give up on the stall window, not hang: took {elapsed:?}"
    );
    // The reserved staging file must still be cleaned up via upload_abort.
    assert!(!remote.path().join("u.bin").exists());
    assert!(part_files(remote.path()).is_empty());
}
