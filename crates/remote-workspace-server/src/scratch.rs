use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use remote_workspace_protocol::ScratchUsage;

/// Default retention. Scratch is a workspace's staging area, not storage: what
/// lives there is the agent's own throwaway scripts and small intermediates,
/// and anything worth keeping belongs in the workspace or on the local machine.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(7 * 86_400);
/// Sweeping runs at most this often per workspace, because a server starts on
/// every reconnect and walking the tree each time would tax the workspace.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(86_400);

const SWEEP_MARKER: &str = "scratch-swept";

struct Entry {
    path: PathBuf,
    bytes: u64,
    /// Time since the file was last written *or read*. Reading matters: under
    /// `relatime` an agent paging through a large log refreshes atime while
    /// mtime stays put, and evicting on mtime alone would delete the file out
    /// from under it. Where atime is unavailable (a `noatime` mount) this
    /// degrades to mtime, which is still correct, only less generous.
    idle: Duration,
}

/// Measure scratch, and with `max_age` also evict everything idle beyond it.
///
/// Eviction is by age alone. A size ceiling was considered and rejected: it
/// punishes the wrong file (a perfectly good one is deleted because something
/// else was just written), and a per-file cap cannot express the distinction
/// that matters -- in real usage here the legitimate logs are *larger* than the
/// checkpoints that do not belong. Keeping artifacts out is a job for the agent
/// guidance, which can talk about kind rather than bytes.
pub fn enforce(root: &Path, max_age: Option<Duration>) -> ScratchUsage {
    let now = SystemTime::now();
    let mut entries = Vec::new();
    collect(root, now, &mut entries);

    let mut usage = ScratchUsage::default();
    for e in &entries {
        if max_age.is_some_and(|limit| e.idle > limit) && std::fs::remove_file(&e.path).is_ok() {
            usage.removed_files += 1;
            usage.removed_bytes += e.bytes;
            continue;
        }
        usage.files += 1;
        usage.bytes += e.bytes;
        let days = (e.idle.as_secs() / 86_400) as u32;
        usage.oldest_days = Some(usage.oldest_days.unwrap_or(0).max(days));
    }
    usage
}

fn collect(dir: &Path, now: SystemTime, out: &mut Vec<Entry>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            collect(&entry.path(), now, out);
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let since = |t: Option<SystemTime>| {
            t.and_then(|t| now.duration_since(t).ok())
                .unwrap_or(Duration::MAX)
        };
        out.push(Entry {
            path: entry.path(),
            bytes: meta.len(),
            idle: since(meta.modified().ok()).min(since(meta.accessed().ok())),
        });
    }
}

/// Sweep at most once per `SWEEP_INTERVAL`, tracked by a marker file in the
/// state directory. There is no daemon: sweeping rides on server startup, which
/// happens on every reconnect, so the common path must cost one stat and
/// nothing more. Growth and cleanup are therefore driven by the same event --
/// use -- which is why a workspace nobody touches is never swept and never
/// needs to be.
pub fn sweep_if_due(
    state_dir: &Path,
    scratch_root: &Path,
    max_age: Duration,
) -> Option<ScratchUsage> {
    let marker = state_dir.join(SWEEP_MARKER);
    let due = match std::fs::metadata(&marker).and_then(|m| m.modified()) {
        Ok(t) => SystemTime::now()
            .duration_since(t)
            .map(|age| age >= SWEEP_INTERVAL)
            .unwrap_or(true),
        Err(_) => true,
    };
    if !due {
        return None;
    }
    // Stamp first: a sweep that fails should not retry on every reconnect.
    let _ = std::fs::write(&marker, b"");
    Some(enforce(scratch_root, Some(max_age)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aged(dir: &Path, name: &str, bytes: usize, idle_days: u64) -> PathBuf {
        let p = dir.join(name);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&p, vec![b'x'; bytes]).unwrap();
        let t = SystemTime::now() - Duration::from_secs(idle_days * 86_400 + 60);
        std::fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_accessed(t).set_modified(t))
            .unwrap();
        p
    }

    fn days(n: u64) -> Duration {
        Duration::from_secs(n * 86_400)
    }

    #[test]
    fn reports_without_a_limit_and_deletes_nothing() {
        let d = tempfile::tempdir().unwrap();
        aged(d.path(), "old.log", 10, 30);
        aged(d.path(), "nested/new.log", 5, 0);

        let u = enforce(d.path(), None);
        assert_eq!(u.files, 2);
        assert_eq!(u.bytes, 15);
        assert!(u.oldest_days.unwrap() >= 29);
        assert_eq!(u.removed_files, 0);
        assert!(d.path().join("old.log").exists());
    }

    #[test]
    fn evicts_by_age_including_nested() {
        let d = tempfile::tempdir().unwrap();
        aged(d.path(), "stale.log", 100, 30);
        aged(d.path(), "deep/also-stale.log", 50, 8);
        aged(d.path(), "fresh.py", 7, 1);

        let u = enforce(d.path(), Some(days(7)));
        assert_eq!(u.removed_files, 2);
        assert_eq!(u.removed_bytes, 150);
        assert_eq!(u.files, 1);
        assert_eq!(u.bytes, 7);
        assert!(!d.path().join("stale.log").exists());
        assert!(!d.path().join("deep/also-stale.log").exists());
        assert!(d.path().join("fresh.py").exists());
    }

    // A log an agent is still paging through keeps a fresh atime while its
    // mtime stays old. Evicting on mtime alone would delete it mid-read.
    #[test]
    fn a_recently_read_file_survives_an_old_mtime() {
        let d = tempfile::tempdir().unwrap();
        let p = aged(d.path(), "being-read.log", 10, 30);
        std::fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_accessed(SystemTime::now()))
            .unwrap();

        let u = enforce(d.path(), Some(days(7)));
        assert_eq!(u.removed_files, 0, "a file being read must not be evicted");
        assert!(p.exists());
    }

    // Size is deliberately not a criterion: a big file that is still in use
    // stays, however large.
    #[test]
    fn size_alone_never_evicts() {
        let d = tempfile::tempdir().unwrap();
        aged(d.path(), "huge-but-fresh.jsonl", 4_000_000, 0);

        let u = enforce(d.path(), Some(days(7)));
        assert_eq!(u.removed_files, 0);
        assert_eq!(u.bytes, 4_000_000);
        assert!(d.path().join("huge-but-fresh.jsonl").exists());
    }

    #[test]
    fn sweep_is_rate_limited_by_the_marker() {
        let state = tempfile::tempdir().unwrap();
        let scratch = state.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        aged(&scratch, "stale.log", 10, 30);

        assert_eq!(
            sweep_if_due(state.path(), &scratch, days(7))
                .unwrap()
                .removed_files,
            1
        );

        aged(&scratch, "another-stale.log", 10, 30);
        assert!(
            sweep_if_due(state.path(), &scratch, days(7)).is_none(),
            "a second sweep within the interval must not run"
        );
        assert!(scratch.join("another-stale.log").exists());
    }
}
