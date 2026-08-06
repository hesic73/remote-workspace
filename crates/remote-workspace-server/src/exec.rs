use std::collections::VecDeque;
use std::time::Duration;

use remote_workspace_protocol::{
    ErrorCode, ExecOutput, ExecTermination, OperationId, ProtocolError,
};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::config::ServerConfig;
use crate::workspace::Workspace;

pub const OUTPUT_PREFIX_LIMIT: usize = 4 * 1024;
pub const OUTPUT_SUFFIX_LIMIT: usize = 12 * 1024;
pub const DEFAULT_TIMEOUT_MS: u64 = 5 * 60 * 1000;
pub const MAX_TIMEOUT_MS: u64 = 60 * 60 * 1000;
/// After the direct child terminates, output collection waits at most this
/// long for the pipes to reach EOF. A descendant that inherited stdout/stderr
/// (e.g. `some-server &`) would otherwise hold the drain open indefinitely;
/// at the deadline the whole process group is SIGKILLed and the readers are
/// abandoned. Detached workloads are not a supported property of exec.
pub const DRAIN_GRACE_MS: u64 = 2_000;

pub struct ExecOutcome {
    pub operation_id: OperationId,
    pub termination: ExecTermination,
    pub stdout: ExecOutput,
    pub stderr: ExecOutput,
    pub duration_ms: u64,
    pub drain_timed_out: bool,
}

enum StreamEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

pub async fn exec(
    ws: &Workspace,
    config: &ServerConfig,
    cwd: Option<&str>,
    profile: Option<&str>,
    argv: &[String],
    timeout_ms: Option<u64>,
    operation_id: OperationId,
) -> Result<ExecOutcome, ProtocolError> {
    if argv.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "argv must not be empty",
        ));
    }
    let timeout_ms = timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"),
        ));
    }
    let resolved_profile = config.profile_for(profile)?;
    let working_dir = match cwd {
        Some(c) => ws.resolve(c)?,
        None => ws.root.clone(),
    };
    if !working_dir.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::NotFound,
            format!("cwd not found: {}", working_dir.display()),
        ));
    }

    // Without a profile the argv is spawned directly, no shell involved. A
    // profile (explicit or default) always runs through ITS shell -- even
    // with an empty setup, since the shell choice itself (e.g. `zsh -lic`
    // loading the user's real environment) is the point -- and ends in exec
    // so signals reach the real command.
    let mut cmd = match resolved_profile {
        None => {
            let mut c = Command::new(&argv[0]);
            c.args(&argv[1..]);
            c
        }
        Some(p) => {
            let script = profile_script(&p.setup, argv);
            let mut c = Command::new(&p.shell[0]);
            c.args(&p.shell[1..]).arg(script);
            c
        }
    };
    cmd.current_dir(&working_dir)
        .env("REMOTE_WORKSPACE_SCRATCH", &ws.scratch_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    crate::process_group::ProcessGroup::configure(&mut cmd);
    let start = std::time::Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| ProtocolError::new(ErrorCode::ExecFailed, format!("spawn failed: {e}")))?;
    let process_group = crate::process_group::ProcessGroup::attach(&mut child).map_err(|e| {
        let _ = child.start_kill();
        ProtocolError::new(
            ErrorCode::ExecFailed,
            format!("create process group failed: {e}"),
        )
    })?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    spawn_reader(stdout, tx.clone(), StreamEvent::Stdout);
    spawn_reader(stderr, tx.clone(), StreamEvent::Stderr);
    drop(tx);

    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut captured_stdout = OutputCapture::default();
    let mut captured_stderr = OutputCapture::default();

    let termination = loop {
        if tokio::time::Instant::now() >= deadline {
            process_group.kill();
            let _ = child.start_kill();
            break ExecTermination::TimedOut;
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => {
                process_group.kill();
                let _ = child.start_kill();
                break ExecTermination::TimedOut;
            }
            Some(event) = rx.recv() => capture(event, &mut captured_stdout, &mut captured_stderr),
            status = child.wait() => {
                let status = status.map_err(|e| {
                    ProtocolError::new(ErrorCode::ExecFailed, format!("wait failed: {e}"))
                })?;
                break exit_termination(status);
            }
        }
    };

    if matches!(termination, ExecTermination::TimedOut) {
        let _ = child.wait().await;
    }
    // Bounded final drain. The direct child has exited (or been killed), but a
    // descendant that inherited stdout/stderr can keep the pipes open past the
    // child's lifetime. Waiting for channel close here without a deadline
    // would hang until that descendant exits. Policy: give the pipes a short
    // grace period to reach EOF; if they do not, SIGKILL the process group and
    // abandon the readers, reporting drain_timed_out so the caller knows
    // collection stopped before pipe EOF.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(DRAIN_GRACE_MS);
    let mut drain_timed_out = false;
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(drain_deadline) => {
                process_group.kill();
                drain_timed_out = true;
                break;
            }
            event = rx.recv() => match event {
                Some(event) => capture(event, &mut captured_stdout, &mut captured_stderr),
                None => break,
            }
        }
    }

    Ok(ExecOutcome {
        operation_id,
        termination,
        stdout: captured_stdout.finish(),
        stderr: captured_stderr.finish(),
        duration_ms: start.elapsed().as_millis() as u64,
        drain_timed_out,
    })
}

fn spawn_reader<R, F>(mut reader: R, tx: tokio::sync::mpsc::Sender<StreamEvent>, wrap: F)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    F: Fn(Vec<u8>) -> StreamEvent + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send(wrap(buf[..n].to_vec())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
}

fn capture(event: StreamEvent, stdout: &mut OutputCapture, stderr: &mut OutputCapture) {
    match event {
        StreamEvent::Stdout(bytes) => stdout.push(&bytes),
        StreamEvent::Stderr(bytes) => stderr.push(&bytes),
    }
}

#[derive(Default)]
struct OutputCapture {
    prefix: Vec<u8>,
    suffix: VecDeque<u8>,
    total_bytes: u64,
}

impl OutputCapture {
    fn push(&mut self, mut bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len() as u64);
        let prefix_remaining = OUTPUT_PREFIX_LIMIT - self.prefix.len();
        let prefix_len = prefix_remaining.min(bytes.len());
        self.prefix.extend_from_slice(&bytes[..prefix_len]);
        bytes = &bytes[prefix_len..];

        for byte in bytes {
            if self.suffix.len() == OUTPUT_SUFFIX_LIMIT {
                self.suffix.pop_front();
            }
            self.suffix.push_back(*byte);
        }
    }

    fn finish(mut self) -> ExecOutput {
        let kept_bytes = self.prefix.len() + self.suffix.len();
        ExecOutput {
            prefix: bounded_lossy(&self.prefix, OUTPUT_PREFIX_LIMIT),
            suffix: bounded_lossy(self.suffix.make_contiguous(), OUTPUT_SUFFIX_LIMIT),
            total_bytes: self.total_bytes,
            omitted_bytes: self.total_bytes.saturating_sub(kept_bytes as u64),
        }
    }
}

fn bounded_lossy(bytes: &[u8], limit: usize) -> String {
    let mut text = String::from_utf8_lossy(bytes).into_owned();
    if text.len() > limit {
        let mut end = limit;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

#[cfg(unix)]
fn exit_termination(status: std::process::ExitStatus) -> ExecTermination {
    use std::os::unix::process::ExitStatusExt;

    match status.code() {
        Some(code) => ExecTermination::Exited { code },
        None => ExecTermination::Signaled {
            signal: status.signal().unwrap_or(0),
        },
    }
}

#[cfg(windows)]
fn exit_termination(status: std::process::ExitStatus) -> ExecTermination {
    ExecTermination::Exited {
        code: status.code().unwrap_or(1),
    }
}

#[cfg(unix)]
fn profile_script(setup: &str, argv: &[String]) -> String {
    let quoted: Vec<String> = argv.iter().map(|a| shell_quote(a)).collect();
    if setup.is_empty() {
        format!("exec {}", quoted.join(" "))
    } else {
        format!("{setup}\nexec {}", quoted.join(" "))
    }
}

#[cfg(unix)]
fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(windows)]
fn profile_script(setup: &str, argv: &[String]) -> String {
    let executable = argv.first().map(String::as_str).unwrap_or_default();
    let arguments = argv
        .iter()
        .skip(1)
        .map(|arg| windows_command_line_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let invoke = format!(
        "try {{ $psi = [System.Diagnostics.ProcessStartInfo]::new(); $psi.FileName = {}; $psi.Arguments = {}; $psi.UseShellExecute = $false; $p = [System.Diagnostics.Process]::Start($psi); $p.WaitForExit(); exit $p.ExitCode }} catch {{ [Console]::Error.WriteLine($_.Exception.Message); exit 1 }}",
        powershell_quote(executable),
        powershell_quote(&arguments),
    );
    format!("{setup}\n{invoke}")
}

#[cfg(windows)]
fn powershell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "''"))
}

#[cfg(windows)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_capture_keeps_whole_small_output() {
        let mut capture = OutputCapture::default();
        capture.push("hello".as_bytes());
        let output = capture.finish();
        assert_eq!(output.prefix, "hello");
        assert_eq!(output.suffix, "");
        assert_eq!(output.total_bytes, 5);
        assert_eq!(output.omitted_bytes, 0);
    }

    #[test]
    fn output_capture_keeps_fixed_prefix_and_suffix() {
        let bytes = vec![b'x'; OUTPUT_PREFIX_LIMIT + OUTPUT_SUFFIX_LIMIT + 37];
        let mut capture = OutputCapture::default();
        for chunk in bytes.chunks(997) {
            capture.push(chunk);
        }
        let output = capture.finish();
        assert_eq!(output.prefix.len(), OUTPUT_PREFIX_LIMIT);
        assert_eq!(output.suffix.len(), OUTPUT_SUFFIX_LIMIT);
        assert_eq!(output.total_bytes, bytes.len() as u64);
        assert_eq!(output.omitted_bytes, 37);
    }

    #[test]
    fn output_capture_is_utf8_safe_at_preview_boundaries() {
        let mut bytes = vec![b'a'; OUTPUT_PREFIX_LIMIT - 1];
        bytes.extend_from_slice("é".as_bytes());
        bytes.extend(vec![b'b'; OUTPUT_SUFFIX_LIMIT + 1]);
        let mut capture = OutputCapture::default();
        capture.push(&bytes);
        let output = capture.finish();
        assert!(output.prefix.is_char_boundary(output.prefix.len()));
        assert!(output.suffix.is_char_boundary(output.suffix.len()));
        assert!(output.prefix.len() <= OUTPUT_PREFIX_LIMIT);
        assert!(output.suffix.len() <= OUTPUT_SUFFIX_LIMIT);
    }
}
