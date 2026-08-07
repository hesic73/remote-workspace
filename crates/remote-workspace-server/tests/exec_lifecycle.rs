#![cfg(unix)]

use std::time::{Duration, Instant};

use remote_workspace_protocol::ExecTermination;
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
            "op-test".into(),
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
