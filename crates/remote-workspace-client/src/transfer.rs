use std::path::Path;
use std::process::Stdio;

use base64::Engine as _;
use remote_workspace_protocol::TransferResult;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::{platform, remote_argv_command, Client, ClientError, RemoteShell};

const TRANSFER_BUF_SIZE: usize = 64 * 1024;
const POWERSHELL_TRANSFER_BUF_SIZE: usize = 3 * 1024;

/// How long a transfer may move zero bytes before it is called stalled. This is
/// deliberately NOT a total timeout: a large file over a slow link legitimately
/// takes hours, and capping the total would kill healthy transfers. What can be
/// judged is whether anything is still flowing.
const TRANSFER_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const STALL_TIMEOUT_ENV: &str = "REMOTE_WORKSPACE_STALL_TIMEOUT_MS";

/// The stall window, overridable for tests and for links so slow that a single
/// 64 KiB chunk cannot cross in two minutes. A malformed value is rejected
/// rather than silently ignored.
fn stall_timeout() -> std::time::Duration {
    match std::env::var(STALL_TIMEOUT_ENV) {
        Err(_) => TRANSFER_STALL_TIMEOUT,
        Ok(raw) => match raw.parse::<u64>() {
            Ok(ms) if ms > 0 => std::time::Duration::from_millis(ms),
            _ => {
                panic!("{STALL_TIMEOUT_ENV} must be a positive number of milliseconds, got {raw:?}")
            }
        },
    }
}

/// Progress as an error-message fragment, so a failure says whether the
/// transfer was moving and how fast rather than only that it broke.
fn progress_note(done: u64, total: u64, elapsed: std::time::Duration) -> String {
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let rate_kib = done as f64 / elapsed.as_secs_f64().max(0.001) / 1024.0;
    format!(
        "{:.1}/{:.1} MiB after {:.0}s, averaging {:.0} KiB/s",
        mib(done),
        mib(total),
        elapsed.as_secs_f64(),
        rate_kib
    )
}

/// Await one step of a transfer, distinguishing three outcomes that all used to
/// look the same from outside: it worked, it failed, or nothing moved at all.
async fn transfer_step<T, E, F>(
    fut: F,
    what: &str,
    done: u64,
    total: u64,
    started: std::time::Instant,
) -> Result<T, ClientError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let window = stall_timeout();
    match tokio::time::timeout(window, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(transfer_err(format!(
            "{what} failed at {}: {e}",
            progress_note(done, total, started.elapsed())
        ))),
        Err(_) => Err(transfer_err(format!(
            "transfer_stalled: nothing moved for {:.0}s while {what}, at {}. \
             The connection is open but no bytes are flowing; retry, or check the remote host.",
            window.as_secs_f64(),
            progress_note(done, total, started.elapsed())
        ))),
    }
}

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
        remote_shell: RemoteShell,
        remote_bin: String,
        root: String,
        state_base: Option<String>,
        config: Option<String>,
    },
}

impl Endpoint {
    fn uses_base64_transfer(&self) -> bool {
        matches!(
            self,
            Endpoint::Ssh {
                remote_shell: RemoteShell::Powershell,
                ..
            }
        )
    }

    /// Argv for the resident JSONL control-plane server.
    pub fn control_argv(&self) -> Vec<String> {
        self.control_argv_inner(None)
    }

    pub fn one_shot_control_argv(&self) -> Vec<String> {
        self.control_argv_inner(Some(1))
    }

    pub fn control_argv_with_idle_timeout(&self, seconds: u64) -> Vec<String> {
        self.control_argv_inner(Some(seconds))
    }

    fn control_argv_inner(&self, idle_timeout_secs: Option<u64>) -> Vec<String> {
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
                if let Some(seconds) = idle_timeout_secs {
                    argv.extend(["--idle-timeout-secs".into(), seconds.to_string()]);
                }
                argv
            }
            Endpoint::Ssh {
                host,
                remote_shell,
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
                match remote_shell {
                    RemoteShell::Posix => {
                        if let Some(seconds) = idle_timeout_secs {
                            remote.extend(["--idle-timeout-secs".into(), seconds.to_string()]);
                        }
                        ssh_argv(host, *remote_shell, &remote)
                    }
                    RemoteShell::Powershell => {
                        remote.extend(["--idle-timeout-secs".into(), "1".into()]);
                        powershell_ssh_proxy_argv(host, &remote)
                    }
                }
            }
        }
    }

    /// Argv for the raw upload receiver (stdin -> staging file).
    pub fn transfer_receive_argv(&self, staging_path: &str, expect_size: u64) -> Vec<String> {
        let tail = |bin: &str, base64: bool| {
            let mut argv = vec![
                bin.to_string(),
                "--transfer-receive".into(),
                staging_path.to_string(),
                "--expect-size".into(),
                expect_size.to_string(),
            ];
            if base64 {
                argv.push("--transfer-base64".into());
            }
            argv
        };
        match self {
            Endpoint::Local { server_bin, .. } => tail(server_bin, false),
            Endpoint::Ssh {
                host,
                remote_shell,
                remote_bin,
                ..
            } => ssh_argv(
                host,
                *remote_shell,
                &tail(remote_bin, *remote_shell == RemoteShell::Powershell),
            ),
        }
    }

    /// Argv for the raw download sender (workspace file -> stdout framing).
    pub fn transfer_send_argv(&self, remote_path: &str) -> Vec<String> {
        let tail = |bin: &str, root: &str, state_base: &Option<String>, base64: bool| {
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
            if base64 {
                argv.push("--transfer-base64".into());
            }
            argv
        };
        match self {
            Endpoint::Local {
                server_bin,
                root,
                state_base,
                ..
            } => tail(server_bin, root, state_base, false),
            Endpoint::Ssh {
                host,
                remote_shell,
                remote_bin,
                root,
                state_base,
                ..
            } => ssh_argv(
                host,
                *remote_shell,
                &tail(
                    remote_bin,
                    root,
                    state_base,
                    *remote_shell == RemoteShell::Powershell,
                ),
            ),
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
fn ssh_argv(host: &str, shell: RemoteShell, remote: &[String]) -> Vec<String> {
    let cmd = remote_argv_command(shell, remote);
    let mut argv = ssh_prefix(host);
    argv.push(cmd);
    argv
}

fn windows_command_line_arg(arg: &str) -> String {
    if !arg.is_empty() && !arg.chars().any(|c| c.is_whitespace() || c == '"') {
        return arg.to_string();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0;
    for c in arg.chars() {
        if c == '\\' {
            backslashes += 1;
        } else if c == '"' {
            quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
            quoted.push('"');
            backslashes = 0;
        } else {
            quoted.push_str(&"\\".repeat(backslashes));
            backslashes = 0;
            quoted.push(c);
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

fn powershell_encoded_command(script: &str) -> String {
    let bytes: Vec<u8> = script
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn powershell_ssh_proxy_argv(host: &str, remote: &[String]) -> Vec<String> {
    let remote_command = remote_argv_command(RemoteShell::Powershell, remote);
    let mut ssh = ssh_prefix(host);
    ssh.push(remote_command);
    let arguments = ssh[1..]
        .iter()
        .map(|arg| windows_command_line_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let ssh_bin = crate::powershell_quote(&ssh[0]);
    let arguments = crate::powershell_quote(&arguments);
    let script = format!(
        "$ErrorActionPreference = 'Stop'; \
         $utf8 = [System.Text.UTF8Encoding]::new($false); \
         $reader = [System.IO.StreamReader]::new([Console]::OpenStandardInput(), $utf8, $false); \
         $output = [System.IO.StreamWriter]::new([Console]::OpenStandardOutput(), $utf8); \
         $output.AutoFlush = $true; \
         while (($line = $reader.ReadLine()) -ne $null) {{ \
         $psi = [System.Diagnostics.ProcessStartInfo]::new(); \
         $psi.FileName = {ssh_bin}; $psi.Arguments = {arguments}; \
         $psi.UseShellExecute = $false; $psi.CreateNoWindow = $true; \
         $psi.RedirectStandardInput = $true; $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true; \
         $session = [System.Diagnostics.Process]::Start($psi); \
         $requestBytes = $utf8.GetBytes($line + [char]10); \
         $session.StandardInput.BaseStream.Write($requestBytes, 0, $requestBytes.Length); $session.StandardInput.Close(); \
         $sessionOut = [System.IO.StreamReader]::new($session.StandardOutput.BaseStream, $utf8, $false); \
         $sessionErr = [System.IO.StreamReader]::new($session.StandardError.BaseStream, $utf8, $false); \
         $response = $sessionOut.ReadLine(); \
         $exited = $session.WaitForExit(2500); \
         if (-not $exited) {{ $session.Kill(); $session.WaitForExit() }}; \
         $errorText = $sessionErr.ReadToEnd(); \
         if ($null -eq $response -or ($exited -and $session.ExitCode -ne 0)) {{ \
         [Console]::Error.WriteLine($errorText.Trim()); exit 1 \
         }}; $output.WriteLine($response) \
         }}"
    );
    vec![
        "powershell.exe".into(),
        "-NoLogo".into(),
        "-NoProfile".into(),
        "-NonInteractive".into(),
        "-EncodedCommand".into(),
        powershell_encoded_command(&script),
    ]
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
    platform::configure_parent_death(&mut cmd);
    let mut child = cmd.spawn()?;
    if let Err(error) = platform::attach_parent_death(&mut child) {
        let _ = child.start_kill();
        return Err(error);
    }
    Ok(child)
}

#[cfg(windows)]
fn close_transfer_stdin(stdin: tokio::process::ChildStdin) -> std::io::Result<()> {
    drop(stdin.into_owned_handle()?);
    Ok(())
}

async fn finish_transfer_child(
    child: &mut tokio::process::Child,
    powershell_ssh: bool,
) -> Result<Option<std::process::ExitStatus>, ClientError> {
    if powershell_ssh {
        match tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await {
            Ok(status) => status
                .map(Some)
                .map_err(|e| transfer_err(format!("wait for transfer process: {e}"))),
            Err(_) => {
                child
                    .kill()
                    .await
                    .map_err(|e| transfer_err(format!("stop completed transfer process: {e}")))?;
                child
                    .wait()
                    .await
                    .map_err(|e| transfer_err(format!("reap completed transfer process: {e}")))?;
                Ok(None)
            }
        }
    } else {
        child
            .wait()
            .await
            .map(Some)
            .map_err(|e| transfer_err(format!("wait for transfer process: {e}")))
    }
}

#[cfg(not(windows))]
fn close_transfer_stdin(stdin: tokio::process::ChildStdin) -> std::io::Result<()> {
    drop(stdin);
    Ok(())
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
    let buf_size = if endpoint.uses_base64_transfer() {
        POWERSHELL_TRANSFER_BUF_SIZE
    } else {
        TRANSFER_BUF_SIZE
    };
    let mut buf = vec![0u8; buf_size];
    let mut hasher = Sha256::new();
    let mut sent: u64 = 0;
    let started = std::time::Instant::now();
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
            let encoded;
            let bytes = if endpoint.uses_base64_transfer() {
                encoded = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                encoded.as_bytes()
            } else {
                &buf[..n]
            };
            transfer_step(
                child_stdin.write_all(bytes),
                "sending bytes to the receiver",
                sent,
                size,
                started,
            )
            .await?;
            sent += n as u64;
        }
        if sent != size {
            return Err(transfer_err(format!(
                "local file changed size during upload: sent {sent} bytes, expected {size}"
            )));
        }
        Ok(())
    }
    .await;
    let close_result = close_transfer_stdin(child_stdin)
        .map_err(|e| transfer_err(format!("close receiver stream: {e}")));
    if let Err(e) = stream_result {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(e);
    }
    close_result?;

    // The receiver reports {size, sha256} once it has the whole file. Guard it
    // too: a remote that accepted every byte and then wedged would otherwise
    // hang here forever, with all the bytes already sent.
    let mut out = String::new();
    let mut reader = BufReader::new(child_stdout);
    transfer_step(
        reader.read_to_string(&mut out),
        "waiting for the receiver to confirm the upload",
        sent,
        size,
        started,
    )
    .await?;
    let remote: ReceiveMetadata = serde_json::from_str(out.trim())
        .map_err(|e| transfer_err(format!("invalid receiver metadata {out:?}: {e}")))?;
    let local_sha = format!("sha256:{}", hex::encode(hasher.finalize()));
    if remote.size != size || remote.sha256 != local_sha {
        return Err(transfer_err(format!(
            "upload verification failed: local {size} bytes {local_sha}, remote {} bytes {}",
            remote.size, remote.sha256
        )));
    }
    if let Some(status) = finish_transfer_child(&mut child, endpoint.uses_base64_transfer()).await?
    {
        if !status.success() {
            return Err(transfer_err(format!(
                "transfer receiver failed with {status}"
            )));
        }
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

    let received =
        receive_stream(&mut reader, tmp.as_file(), endpoint.uses_base64_transfer()).await;
    if received.is_err() {
        // A sender that stalled will never exit on its own, so waiting for it
        // would hang exactly where the stall was supposed to be caught. Killing
        // it first is what turns a detected stall into a returned error.
        let _ = child.kill().await;
    }
    let status = finish_transfer_child(&mut child, endpoint.uses_base64_transfer()).await?;
    let (size, sha256) = match received {
        Ok(v) => v,
        Err(e) => {
            return Err(if status.as_ref().is_none_or(|status| status.success()) {
                e
            } else {
                transfer_err(format!(
                    "transfer sender failed with {}: {e}",
                    status.unwrap()
                ))
            })
        }
    };
    if let Some(status) = status {
        if !status.success() {
            return Err(transfer_err(format!(
                "transfer sender failed with {status}"
            )));
        }
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
    base64_chunks: bool,
) -> Result<(u64, String), ClientError> {
    use std::io::Write;

    let started = std::time::Instant::now();
    let mut header = String::new();
    transfer_step(
        reader.read_line(&mut header),
        "waiting for the sender's header",
        0,
        0,
        started,
    )
    .await?;
    if header.trim().is_empty() {
        return Err(transfer_err("sender produced no header"));
    }
    let header: SendHeader = serde_json::from_str(header.trim())
        .map_err(|e| transfer_err(format!("invalid sender header {header:?}: {e}")))?;

    let mut hasher = Sha256::new();
    let mut remaining = header.size;
    let mut out = out;
    if base64_chunks {
        let mut line = String::new();
        while remaining > 0 {
            line.clear();
            let received = header.size - remaining;
            let n = transfer_step(
                reader.read_line(&mut line),
                "receiving a Base64 chunk from the sender",
                received,
                header.size,
                started,
            )
            .await?;
            if n == 0 {
                return Err(transfer_err(format!(
                    "sender stream ended early at {}: {remaining} of {} bytes missing",
                    progress_note(received, header.size, started.elapsed()),
                    header.size
                )));
            }
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(line.trim_end_matches(['\r', '\n']))
                .map_err(|e| transfer_err(format!("invalid Base64 transfer chunk: {e}")))?;
            if decoded.is_empty() || decoded.len() as u64 > remaining {
                return Err(transfer_err(format!(
                    "invalid Base64 chunk size {} with {remaining} bytes remaining",
                    decoded.len()
                )));
            }
            hasher.update(&decoded);
            out.write_all(&decoded)
                .map_err(|e| transfer_err(format!("write local temp file: {e}")))?;
            remaining -= decoded.len() as u64;
        }
    } else {
        let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
        while remaining > 0 {
            let want = (remaining as usize).min(buf.len());
            let received = header.size - remaining;
            let n = transfer_step(
                reader.read(&mut buf[..want]),
                "receiving bytes from the sender",
                received,
                header.size,
                started,
            )
            .await?;
            if n == 0 {
                return Err(transfer_err(format!(
                    "sender stream ended early at {}: {remaining} of {} bytes missing",
                    progress_note(received, header.size, started.elapsed()),
                    header.size
                )));
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n])
                .map_err(|e| transfer_err(format!("write local temp file: {e}")))?;
            remaining -= n as u64;
        }
    }

    let mut trailer = String::new();
    transfer_step(
        reader.read_line(&mut trailer),
        "waiting for the sender's trailer",
        header.size,
        header.size,
        started,
    )
    .await?;
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
