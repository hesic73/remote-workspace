use std::path::Path;
use std::process::Stdio;

use agent_remote_protocol::TransferResult;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::{shell_quote, Client, ClientError};

const TRANSFER_BUF_SIZE: usize = 64 * 1024;

/// Where the server runs. The single source of every argv this client spawns:
/// the resident JSONL control plane and the per-transfer raw data plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Local {
        server_bin: String,
        root: String,
        state_base: Option<String>,
        config: Option<String>,
    },
    Ssh {
        host: String,
        remote_bin: String,
        root: String,
        state_base: Option<String>,
        config: Option<String>,
    },
}

impl Endpoint {
    /// Argv for the resident JSONL control-plane server.
    pub fn control_argv(&self) -> Vec<String> {
        match self {
            Endpoint::Local {
                server_bin,
                root,
                state_base,
                config,
            } => {
                let mut argv = vec![server_bin.clone(), "--root".into(), root.clone()];
                if let Some(c) = config {
                    argv.push("--config".into());
                    argv.push(c.clone());
                }
                if let Some(b) = state_base {
                    argv.push("--state-base".into());
                    argv.push(b.clone());
                }
                argv
            }
            Endpoint::Ssh {
                host,
                remote_bin,
                root,
                state_base,
                config,
            } => {
                let mut remote = vec![remote_bin.clone(), "--root".into(), root.clone()];
                if let Some(c) = config {
                    remote.push("--config".into());
                    remote.push(c.clone());
                }
                if let Some(b) = state_base {
                    remote.push("--state-base".into());
                    remote.push(b.clone());
                }
                ssh_argv(host, &remote)
            }
        }
    }

    /// Argv for the raw upload receiver (stdin -> staging file).
    pub fn transfer_receive_argv(&self, staging_path: &str, expect_size: u64) -> Vec<String> {
        let tail = |bin: &str| {
            vec![
                bin.to_string(),
                "--transfer-receive".into(),
                staging_path.to_string(),
                "--expect-size".into(),
                expect_size.to_string(),
            ]
        };
        match self {
            Endpoint::Local { server_bin, .. } => tail(server_bin),
            Endpoint::Ssh {
                host, remote_bin, ..
            } => ssh_argv(host, &tail(remote_bin)),
        }
    }

    /// Argv for the raw download sender (workspace file -> stdout framing).
    pub fn transfer_send_argv(&self, remote_path: &str) -> Vec<String> {
        let tail = |bin: &str, root: &str, state_base: &Option<String>| {
            let mut argv = vec![
                bin.to_string(),
                "--transfer-send".into(),
                remote_path.to_string(),
                "--root".into(),
                root.to_string(),
            ];
            if let Some(b) = state_base {
                argv.push("--state-base".into());
                argv.push(b.clone());
            }
            argv
        };
        match self {
            Endpoint::Local {
                server_bin,
                root,
                state_base,
                ..
            } => tail(server_bin, root, state_base),
            Endpoint::Ssh {
                host,
                remote_bin,
                root,
                state_base,
                ..
            } => ssh_argv(host, &tail(remote_bin, root, state_base)),
        }
    }
}

/// `ssh` plus this client's connection policy, up to and including the host:
/// the single place those options are chosen. BatchMode fails fast instead of
/// hanging on an auth prompt; ServerAlive keeps NAT'd / idle-pruning
/// connections open across long sessions.
pub(crate) fn ssh_prefix(host: &str) -> Vec<String> {
    vec![
        "ssh".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ServerAliveInterval=30".into(),
        "-o".into(),
        "ServerAliveCountMax=4".into(),
        host.into(),
    ]
}

/// Wrap a remote argv for ssh: every remote-side argument is shell-quoted into
/// one command string, because ssh joins trailing arguments with spaces and
/// hands the result to the remote shell.
fn ssh_argv(host: &str, remote: &[String]) -> Vec<String> {
    let cmd = remote
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    let mut argv = ssh_prefix(host);
    argv.push(cmd);
    argv
}

#[derive(serde::Deserialize)]
struct ReceiveMetadata {
    size: u64,
    sha256: String,
}

#[derive(serde::Deserialize)]
struct SendHeader {
    size: u64,
}

#[derive(serde::Deserialize)]
struct SendTrailer {
    sha256: String,
}

fn transfer_err(msg: impl Into<String>) -> ClientError {
    ClientError::Transfer(msg.into())
}

fn spawn_transfer_child(argv: &[String]) -> std::io::Result<tokio::process::Child> {
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    // Die with the parent, like the control-plane transport: a killed consumer
    // must not leave an orphaned ssh streaming bytes.
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    cmd.spawn()
}

/// Upload a local file to `remote_path` (workspace-relative or `@scratch/...`)
/// by streaming raw bytes through a dedicated receiver process, then
/// atomically installing on the remote.
pub async fn upload_file(
    client: &Client,
    endpoint: &Endpoint,
    local_path: &Path,
    remote_path: &str,
    overwrite: bool,
) -> Result<TransferResult, ClientError> {
    let start = std::time::Instant::now();
    let meta = tokio::fs::metadata(local_path)
        .await
        .map_err(|e| transfer_err(format!("cannot stat local source {local_path:?}: {e}")))?;
    if !meta.is_file() {
        return Err(transfer_err(format!(
            "local source is not a regular file: {local_path:?}"
        )));
    }
    let size = meta.len();

    let prep = client.upload_prepare(remote_path, overwrite).await?;

    // From here on every failure must abort the prepared upload so the remote
    // staging file is cleaned up.
    let streamed = stream_to_receiver(endpoint, local_path, size, &prep.staging_path).await;
    let sha256 = match streamed {
        Ok(sha256) => sha256,
        Err(e) => return Err(abort_after(client, &prep.transfer_id, e).await),
    };

    let duration_ms = start.elapsed().as_millis() as u64;
    match client
        .upload_commit(&prep.transfer_id, size, &sha256, duration_ms)
        .await
    {
        Ok(result) => Ok(result),
        Err(e) => Err(abort_after(client, &prep.transfer_id, e).await),
    }
}

/// Stream the local file into the raw receiver and cross-check both sides'
/// size and SHA-256. Returns the verified hash.
async fn stream_to_receiver(
    endpoint: &Endpoint,
    local_path: &Path,
    size: u64,
    staging_path: &str,
) -> Result<String, ClientError> {
    let argv = endpoint.transfer_receive_argv(staging_path, size);
    let mut child = spawn_transfer_child(&argv)
        .map_err(|e| transfer_err(format!("spawn transfer receiver: {e}")))?;
    let mut child_stdin = child.stdin.take().expect("piped stdin");
    let child_stdout = child.stdout.take().expect("piped stdout");

    let mut file = tokio::fs::File::open(local_path)
        .await
        .map_err(|e| transfer_err(format!("open local source {local_path:?}: {e}")))?;
    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut sent: u64 = 0;
    let stream_result: Result<(), ClientError> = async {
        loop {
            let n = file
                .read(&mut buf)
                .await
                .map_err(|e| transfer_err(format!("read local source: {e}")))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            child_stdin
                .write_all(&buf[..n])
                .await
                .map_err(|e| transfer_err(format!("write to receiver: {e}")))?;
            sent += n as u64;
        }
        if sent != size {
            return Err(transfer_err(format!(
                "local file changed size during upload: sent {sent} bytes, expected {size}"
            )));
        }
        child_stdin
            .shutdown()
            .await
            .map_err(|e| transfer_err(format!("close receiver stdin: {e}")))?;
        Ok(())
    }
    .await;
    drop(child_stdin);
    if let Err(e) = stream_result {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(e);
    }

    let mut out = String::new();
    let mut reader = BufReader::new(child_stdout);
    reader
        .read_to_string(&mut out)
        .await
        .map_err(|e| transfer_err(format!("read receiver metadata: {e}")))?;
    let status = child
        .wait()
        .await
        .map_err(|e| transfer_err(format!("wait for receiver: {e}")))?;
    if !status.success() {
        return Err(transfer_err(format!(
            "transfer receiver failed with {status}"
        )));
    }
    let remote: ReceiveMetadata = serde_json::from_str(out.trim())
        .map_err(|e| transfer_err(format!("invalid receiver metadata {out:?}: {e}")))?;
    let local_sha = format!("sha256:{}", hex::encode(hasher.finalize()));
    if remote.size != size || remote.sha256 != local_sha {
        return Err(transfer_err(format!(
            "upload verification failed: local {size} bytes {local_sha}, remote {} bytes {}",
            remote.size, remote.sha256
        )));
    }
    Ok(local_sha)
}

/// Abort the prepared upload after `err`. The original error stays primary; a
/// failed abort is appended rather than swallowed.
async fn abort_after(client: &Client, transfer_id: &str, err: ClientError) -> ClientError {
    match client.upload_abort(transfer_id).await {
        Ok(()) => err,
        Err(abort_err) => transfer_err(format!(
            "{err}; additionally, cleaning up the staged upload failed: {abort_err}"
        )),
    }
}

/// Download `remote_path` (workspace-relative or `@scratch/...`) into a local
/// file by streaming raw bytes from a dedicated sender process, verifying
/// size and SHA-256, then atomically installing at `local_path`.
pub async fn download_file(
    client: &Client,
    endpoint: &Endpoint,
    remote_path: &str,
    local_path: &Path,
    overwrite: bool,
) -> Result<TransferResult, ClientError> {
    let start = std::time::Instant::now();
    let parent = local_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| transfer_err(format!("local target has no parent: {local_path:?}")))?;
    if !parent.is_dir() {
        return Err(transfer_err(format!(
            "local parent directory does not exist: {parent:?}"
        )));
    }
    match std::fs::symlink_metadata(local_path) {
        Ok(m) if m.is_dir() => {
            return Err(transfer_err(format!(
                "local target is a directory: {local_path:?}"
            )))
        }
        Ok(_) if !overwrite => {
            return Err(transfer_err(format!(
                "local target already exists: {local_path:?}; pass overwrite=true to replace it"
            )))
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(transfer_err(format!("stat local target: {e}"))),
    }

    // Temp file in the target's directory; dropped (deleted) on any error path
    // before the final persist.
    let tmp = tempfile::Builder::new()
        .suffix(".part")
        .tempfile_in(parent)
        .map_err(|e| transfer_err(format!("create local temp file: {e}")))?;

    let argv = endpoint.transfer_send_argv(remote_path);
    let mut child = spawn_transfer_child(&argv)
        .map_err(|e| transfer_err(format!("spawn transfer sender: {e}")))?;
    drop(child.stdin.take());
    let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));

    let received = receive_stream(&mut reader, tmp.as_file()).await;
    let status = child
        .wait()
        .await
        .map_err(|e| transfer_err(format!("wait for sender: {e}")))?;
    let (size, sha256) = match received {
        Ok(v) => v,
        Err(e) => {
            return Err(if status.success() {
                e
            } else {
                transfer_err(format!("transfer sender failed with {status}: {e}"))
            })
        }
    };
    if !status.success() {
        return Err(transfer_err(format!(
            "transfer sender failed with {status}"
        )));
    }

    tmp.as_file()
        .sync_all()
        .map_err(|e| transfer_err(format!("sync local temp file: {e}")))?;
    if overwrite {
        tmp.persist(local_path)
            .map_err(|e| transfer_err(format!("install local target: {e}")))?;
    } else {
        tmp.persist_noclobber(local_path)
            .map_err(|e| transfer_err(format!("install local target: {e}")))?;
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    client
        .download_record(remote_path, size, &sha256, duration_ms)
        .await
        .map_err(|e| {
            transfer_err(format!(
                "download completed and {local_path:?} was installed, \
                 but recording the operation on the server failed: {e}"
            ))
        })
}

/// Read the sender framing (header line, exactly `size` raw bytes, trailer
/// line) into `out`, verifying the trailer hash. Returns (size, sha256).
async fn receive_stream(
    reader: &mut BufReader<tokio::process::ChildStdout>,
    out: &std::fs::File,
) -> Result<(u64, String), ClientError> {
    use std::io::Write;

    let mut header = String::new();
    reader
        .read_line(&mut header)
        .await
        .map_err(|e| transfer_err(format!("read sender header: {e}")))?;
    if header.trim().is_empty() {
        return Err(transfer_err("sender produced no header"));
    }
    let header: SendHeader = serde_json::from_str(header.trim())
        .map_err(|e| transfer_err(format!("invalid sender header {header:?}: {e}")))?;

    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut remaining = header.size;
    let mut out = out;
    while remaining > 0 {
        let want = (remaining as usize).min(buf.len());
        let n = reader
            .read(&mut buf[..want])
            .await
            .map_err(|e| transfer_err(format!("read file bytes: {e}")))?;
        if n == 0 {
            return Err(transfer_err(format!(
                "sender stream ended early: {remaining} of {} bytes missing",
                header.size
            )));
        }
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])
            .map_err(|e| transfer_err(format!("write local temp file: {e}")))?;
        remaining -= n as u64;
    }

    let mut trailer = String::new();
    reader
        .read_line(&mut trailer)
        .await
        .map_err(|e| transfer_err(format!("read sender trailer: {e}")))?;
    let trailer: SendTrailer = serde_json::from_str(trailer.trim())
        .map_err(|e| transfer_err(format!("invalid sender trailer {trailer:?}: {e}")))?;
    let local_sha = format!("sha256:{}", hex::encode(hasher.finalize()));
    if trailer.sha256 != local_sha {
        return Err(transfer_err(format!(
            "download verification failed: received hash {local_sha}, sender reported {}",
            trailer.sha256
        )));
    }
    Ok((header.size, local_sha))
}
