#![cfg(windows)]

use std::time::{Duration, Instant};

use remote_workspace_protocol::ExecTermination;
use remote_workspace_server::config::ServerConfig;
use remote_workspace_server::exec::{self, ExecOutcome, DRAIN_GRACE_MS};
use remote_workspace_server::workspace::Workspace;
use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
use windows_sys::Win32::System::Threading::{
    OpenProcess, TerminateProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
};

fn ws() -> (tempfile::TempDir, Workspace) {
    let dir = tempfile::tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    let workspace = Workspace::new(dir.path().to_path_buf(), scratch).unwrap();
    (dir, workspace)
}

async fn run_bounded(workspace: &Workspace, script: &str, timeout_ms: u64) -> ExecOutcome {
    let argv = vec![
        "powershell.exe".to_string(),
        "-NoProfile".to_string(),
        "-NonInteractive".to_string(),
        "-Command".to_string(),
        script.to_string(),
    ];
    tokio::time::timeout(
        Duration::from_secs(20),
        exec::exec(
            workspace,
            &ServerConfig::default(),
            None,
            None,
            &argv,
            Some(timeout_ms),
            "op-test".into(),
        ),
    )
    .await
    .expect("exec must reach a terminal response")
    .expect("exec must succeed")
}

fn first_line_pid(stdout: &str) -> u32 {
    stdout
        .lines()
        .next()
        .expect("script must print the descendant pid")
        .trim()
        .parse()
        .expect("first stdout line must be a pid")
}

fn process_gone(pid: u32) -> bool {
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if process.is_null() {
        return true;
    }
    let wait = unsafe { WaitForSingleObject(process, 0) };
    unsafe { CloseHandle(process) };
    wait == WAIT_OBJECT_0
}

fn assert_process_gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !process_gone(pid) {
        assert!(Instant::now() < deadline, "descendant {pid} survived");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn terminate_process(pid: u32) {
    let process = unsafe { OpenProcess(PROCESS_TERMINATE | PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(!process.is_null(), "open descendant {pid}");
    unsafe {
        TerminateProcess(process, 1);
        WaitForSingleObject(process, 5_000);
        CloseHandle(process);
    }
}

const CHILD_ARGS: &str = "-NoProfile -NonInteractive -Command \"Start-Sleep -Seconds 30\"";

#[tokio::test]
async fn descendant_holding_pipes_is_bounded_and_killed() {
    let (_dir, workspace) = ws();
    let script = format!(
        "$p = Start-Process powershell.exe -ArgumentList '{CHILD_ARGS}' -NoNewWindow -PassThru; \
         [Console]::Out.WriteLine($p.Id); [Console]::Out.WriteLine('started')"
    );
    let start = Instant::now();
    let outcome = run_bounded(&workspace, &script, 60_000).await;
    assert!(start.elapsed() < Duration::from_secs(15));
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(outcome.drain_timed_out);
    assert!(outcome.stdout.prefix.contains("started"));
    assert_process_gone(first_line_pid(&outcome.stdout.prefix));
}

#[tokio::test]
async fn detached_descendant_with_closed_pipes_survives() {
    let (_dir, workspace) = ws();
    let script = format!(
        "$p = Start-Process powershell.exe -ArgumentList '{CHILD_ARGS}' \
         -WindowStyle Hidden -PassThru; [Console]::Out.WriteLine($p.Id)"
    );
    let outcome = run_bounded(&workspace, &script, 60_000).await;
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(!outcome.drain_timed_out);
    assert!(
        !outcome.stdout.prefix.trim().is_empty(),
        "missing descendant pid; stderr={:?}",
        outcome.stderr.prefix
    );
    let pid = first_line_pid(&outcome.stdout.prefix);
    assert!(!process_gone(pid), "detached descendant must survive");
    terminate_process(pid);
}

#[tokio::test]
async fn timeout_kills_whole_job() {
    let (_dir, workspace) = ws();
    let script = format!(
        "$p = Start-Process powershell.exe -ArgumentList '{CHILD_ARGS}' -NoNewWindow -PassThru; \
         [Console]::Out.WriteLine($p.Id); Start-Sleep -Seconds 30"
    );
    let outcome = run_bounded(&workspace, &script, 3_000).await;
    assert_eq!(outcome.termination, ExecTermination::TimedOut);
    assert_process_gone(first_line_pid(&outcome.stdout.prefix));
}

#[tokio::test]
async fn clean_exit_does_not_pay_drain_grace() {
    let (_dir, workspace) = ws();
    let start = Instant::now();
    let outcome = run_bounded(&workspace, "Write-Output 'done'", 30_000).await;
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(!outcome.drain_timed_out);
    assert!(start.elapsed() < Duration::from_millis(DRAIN_GRACE_MS));
}
