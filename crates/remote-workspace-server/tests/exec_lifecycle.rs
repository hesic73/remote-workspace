#![cfg(unix)]

use std::time::{Duration, Instant};

use remote_workspace_protocol::{ExecTermination, WaitOutcome, WaitResult};
use remote_workspace_server::config::ServerConfig;
use remote_workspace_server::exec::{self, ExecOutcome, DRAIN_GRACE_MS};
use remote_workspace_server::workspace::Workspace;

fn ws() -> (tempfile::TempDir, Workspace) {
    let dir = tempfile::tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    let w = Workspace::new(dir.path().to_path_buf(), scratch).unwrap();
    (dir, w)
}

/// Run a bash script through exec with a hard test-side deadline, so a
/// regression can never hang the test suite itself.
async fn run_bounded(w: &Workspace, script: &str, timeout_ms: u64) -> ExecOutcome {
    let argv = vec!["bash".to_string(), "-c".to_string(), script.to_string()];
    tokio::time::timeout(
        Duration::from_secs(20),
        exec::exec(
            w,
            &ServerConfig::default(),
            None,
            None,
            &argv,
            Some(timeout_ms),
        ),
    )
    .await
    .expect("exec must reach a terminal response within a bounded period")
    .expect("exec must succeed")
}

fn first_line_pid(stdout_prefix: &str) -> i32 {
    stdout_prefix
        .lines()
        .next()
        .expect("script must print the descendant pid")
        .trim()
        .parse()
        .expect("first stdout line must be a pid")
}

/// True once the pid no longer refers to a running process. A killed
/// descendant may linger briefly as a zombie until it is reaped, which counts
/// as gone for cleanup purposes.
fn process_gone(pid: i32) -> bool {
    if unsafe { libc::kill(pid, 0) } != 0 {
        return true;
    }
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => {
            stat.rsplit(')')
                .next()
                .and_then(|rest| rest.trim().chars().next())
                == Some('Z')
        }
        Err(_) => true,
    }
}

fn assert_process_gone(pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !process_gone(pid) {
        assert!(
            Instant::now() < deadline,
            "descendant {pid} survived exec cleanup"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// The core post-exit hang: the direct child exits immediately while a
// descendant keeps the inherited stdout/stderr open. Exec must return within
// the drain grace period (not wait for the descendant), flag the truncated
// drain, and kill the descendant.
#[tokio::test]
async fn descendant_holding_pipes_is_bounded_and_killed() {
    let (_d, w) = ws();
    let start = Instant::now();
    let outcome = run_bounded(&w, "sleep 30 & echo $!; echo started", 60_000).await;
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "exec must not wait for the pipe-holding descendant"
    );
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(
        outcome.drain_timed_out,
        "collection ended before pipe EOF and must say so"
    );
    assert!(outcome.stdout.prefix.contains("started"));
    let pid = first_line_pid(&outcome.stdout.prefix);
    assert_process_gone(pid);
}

// A properly detached descendant (pipes redirected away) does not trigger the
// drain deadline and is NOT killed: only pipe-holders are terminated. This is
// what keeps the documented tmux/nohup-with-redirect workflow working.
#[tokio::test]
async fn detached_descendant_with_closed_pipes_survives() {
    let (_d, w) = ws();
    let outcome = run_bounded(&w, "sleep 30 >/dev/null 2>&1 & echo $!", 60_000).await;
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(
        !outcome.drain_timed_out,
        "pipes reached EOF; no drain timeout"
    );
    let pid = first_line_pid(&outcome.stdout.prefix);
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        0,
        "detached descendant must survive a clean drain"
    );
    unsafe { libc::kill(pid, libc::SIGKILL) };
}

// Timeout must terminate the whole process group, including a descendant that
// would otherwise keep running (and keep the pipes open) after the direct
// child is killed.
#[tokio::test]
async fn timeout_kills_whole_process_group() {
    let (_d, w) = ws();
    let start = Instant::now();
    let outcome = run_bounded(&w, "sleep 30 & echo $!; sleep 30", 500).await;
    assert_eq!(outcome.termination, ExecTermination::TimedOut);
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "timeout cleanup must be bounded"
    );
    let pid = first_line_pid(&outcome.stdout.prefix);
    assert_process_gone(pid);
}

// Both streams produced concurrently are fully collected when the command
// exits normally and closes its pipes.
#[tokio::test]
async fn concurrent_stdout_and_stderr_are_both_collected() {
    let (_d, w) = ws();
    let outcome = run_bounded(
        &w,
        "for i in $(seq 1 200); do echo o$i; echo e$i 1>&2; done",
        30_000,
    )
    .await;
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(!outcome.drain_timed_out);
    let stdout = format!("{}{}", outcome.stdout.prefix, outcome.stdout.suffix);
    let stderr = format!("{}{}", outcome.stderr.prefix, outcome.stderr.suffix);
    assert!(stdout.contains("o1\n") && stdout.contains("o200"));
    assert!(stderr.contains("e1\n") && stderr.contains("e200"));
    assert_eq!(outcome.stdout.omitted_bytes, 0);
    assert_eq!(outcome.stderr.omitted_bytes, 0);
}

// The drain deadline is a grace period, not a fixed cost: a command whose
// pipes close at exit must return well before DRAIN_GRACE_MS elapses.
#[tokio::test]
async fn clean_exit_does_not_pay_the_drain_grace() {
    let (_d, w) = ws();
    let start = Instant::now();
    let outcome = run_bounded(&w, "echo done", 30_000).await;
    assert_eq!(outcome.termination, ExecTermination::Exited { code: 0 });
    assert!(!outcome.drain_timed_out);
    assert!(
        start.elapsed() < Duration::from_millis(DRAIN_GRACE_MS),
        "clean exit must not wait out the drain grace period"
    );
}

// An exec abandoned mid-run -- its task aborted at server shutdown -- must
// take its descendants with it, not only the direct child.
#[tokio::test]
async fn dropping_a_running_exec_kills_its_process_tree() {
    let (d, w) = ws();
    let pid_file = d.path().join("pid");
    let script = format!(
        "sleep 30 >/dev/null 2>&1 & echo $! > {}; sleep 30",
        pid_file.display()
    );
    let argv = vec!["bash".to_string(), "-c".to_string(), script];
    let config = ServerConfig::default();
    let running = exec::exec(&w, &config, None, None, &argv, Some(60_000));
    let deadline = Instant::now() + Duration::from_secs(10);
    let pid = tokio::select! {
        _ = running => panic!("exec finished before it was dropped"),
        pid = async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&pid_file)
                    .ok()
                    .and_then(|text| text.trim().parse::<i32>().ok())
                {
                    return pid;
                }
                assert!(Instant::now() < deadline, "descendant never started");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } => pid,
    };
    assert_process_gone(pid);
}

async fn wait_bounded(
    w: &Workspace,
    argv: &[&str],
    timeout_ms: u64,
) -> Result<WaitResult, remote_workspace_protocol::ProtocolError> {
    let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
    tokio::time::timeout(
        Duration::from_secs(20),
        exec::wait(
            w,
            &ServerConfig::default(),
            None,
            None,
            &argv,
            Some(timeout_ms),
        ),
    )
    .await
    .expect("wait must return within a bounded period")
}

#[tokio::test]
async fn wait_on_something_already_gone_returns_at_the_first_check() {
    let (_d, w) = ws();
    let r = wait_bounded(&w, &["sh", "-c", "echo none; exit 1"], 60_000)
        .await
        .unwrap();
    assert_eq!(r.outcome, WaitOutcome::Gone);
    assert!(r.first_check);
    assert_eq!(r.checks, 1);
    assert_eq!(r.last_check.stdout.prefix, "none\n");
}

#[tokio::test]
async fn wait_returns_once_the_check_says_gone() {
    let (d, w) = ws();
    let flag = d.path().join("alive");
    std::fs::write(&flag, "").unwrap();
    let remover = {
        let flag = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            std::fs::remove_file(flag).unwrap();
        })
    };
    let start = Instant::now();
    let r = wait_bounded(&w, &["test", "-e", flag.to_str().unwrap()], 60_000)
        .await
        .unwrap();
    remover.await.unwrap();
    assert_eq!(r.outcome, WaitOutcome::Gone);
    assert!(!r.first_check);
    assert!(r.checks >= 2);
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn wait_times_out_with_the_last_check_output() {
    let (_d, w) = ws();
    let start = Instant::now();
    let r = wait_bounded(&w, &["sh", "-c", "echo PENDING; exit 0"], 1_500)
        .await
        .unwrap();
    assert_eq!(r.outcome, WaitOutcome::TimedOut);
    assert!(start.elapsed() >= Duration::from_millis(1_500));
    assert!(r.checks >= 2);
    assert_eq!(r.last_check.stdout.prefix, "PENDING\n");
}

#[tokio::test]
async fn a_check_that_neither_succeeds_nor_exits_1_fails_the_wait() {
    let (_d, w) = ws();
    let r = wait_bounded(&w, &["sh", "-c", "echo broken >&2; exit 2"], 60_000)
        .await
        .unwrap();
    assert_eq!(r.outcome, WaitOutcome::CheckFailed);
    assert_eq!(
        r.last_check.termination,
        ExecTermination::Exited { code: 2 }
    );
    assert_eq!(r.last_check.stderr.prefix, "broken\n");

    let missing = wait_bounded(&w, &["sh", "-c", "no-such-command-xyz"], 60_000)
        .await
        .unwrap();
    assert_eq!(missing.outcome, WaitOutcome::CheckFailed);
    assert_eq!(
        missing.last_check.termination,
        ExecTermination::Exited { code: 127 }
    );
}

#[tokio::test]
async fn wait_rejects_bad_requests_instead_of_waiting() {
    let (_d, w) = ws();
    assert!(wait_bounded(&w, &["true"], 0).await.is_err());
    assert!(wait_bounded(&w, &["true"], 3_600_001).await.is_err());
    assert!(wait_bounded(&w, &[], 1_000).await.is_err());
    assert!(wait_bounded(&w, &["definitely-not-a-program-xyz"], 1_000)
        .await
        .is_err());
}
