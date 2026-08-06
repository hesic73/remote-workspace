use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::deploy::DeployError;
use crate::{ArgvTransport, Client, Endpoint, RemoteShell};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetFile {
    workspaces: BTreeMap<String, WorkspaceEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceEntry {
    /// SSH host (resolvable via ~/.ssh/config); omit to run the server on the
    /// local machine.
    host: Option<String>,
    remote_shell: Option<RemoteShell>,
    root: String,
    /// Server binary path on that machine. Defaults to `remote-workspace-server`
    /// on PATH. `workspace add` records the absolute managed path here.
    bin: Option<String>,
    config: Option<String>,
    state_base: Option<String>,
    /// Human-readable description shown by list_workspaces.
    label: Option<String>,
}

/// A configured workspace: where its server runs, plus display metadata.
pub struct Workspace {
    pub endpoint: Endpoint,
    pub label: Option<String>,
}

/// Parse and validate a fleet config. Rejects an empty fleet and two
/// workspaces addressing the same (host, root): they would contend for the
/// same server-side state lock and one of them would always fail.
pub fn parse_fleet(text: &str) -> anyhow::Result<BTreeMap<String, Workspace>> {
    let file: FleetFile = toml::from_str(text)?;
    if file.workspaces.is_empty() {
        anyhow::bail!("fleet config declares no workspaces");
    }
    let mut seen: BTreeMap<(Option<String>, String), String> = BTreeMap::new();
    let mut host_shells: BTreeMap<String, RemoteShell> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (name, entry) in file.workspaces {
        if let Some(prev) = seen.insert((entry.host.clone(), entry.root.clone()), name.clone()) {
            anyhow::bail!(
                "workspaces '{prev}' and '{name}' address the same host and root; \
                 they would contend for the same server state lock"
            );
        }
        let bin = entry
            .bin
            .unwrap_or_else(|| "remote-workspace-server".into());
        let endpoint = match entry.host {
            Some(host) => {
                let remote_shell = entry.remote_shell.unwrap_or_default();
                if let Some(previous) = host_shells.insert(host.clone(), remote_shell) {
                    if previous != remote_shell {
                        anyhow::bail!(
                            "SSH host '{host}' has conflicting remote_shell values: {previous} and {remote_shell}"
                        );
                    }
                }
                Endpoint::Ssh {
                    host,
                    remote_shell,
                    remote_bin: bin,
                    root: entry.root,
                    state_base: entry.state_base,
                    config: entry.config,
                }
            }
            None => {
                if entry.remote_shell.is_some() {
                    anyhow::bail!("workspace '{name}' sets remote_shell without an SSH host");
                }
                Endpoint::Local {
                    server_bin: bin,
                    root: entry.root,
                    state_base: entry.state_base,
                    config: entry.config,
                }
            }
        };
        out.insert(
            name,
            Workspace {
                endpoint,
                label: entry.label,
            },
        );
    }
    Ok(out)
}

/// One-shot health probe of a workspace: spawn its server and do a real
/// round-trip. Single attempt, no retries -- this is a diagnostic, not the
/// resilient tool-call path. The error text starts with a stable code:
/// `connect_failed` (transport/spawn) or `probe_failed` (server reached but the
/// round-trip failed, e.g. bad root or a locked state directory).
pub async fn check_workspace(endpoint: &Endpoint) -> Result<(), String> {
    let transport = ArgvTransport {
        argv: endpoint.one_shot_control_argv(),
    };
    match Client::connect(transport, None).await {
        Err(e) => Err(format!("connect_failed: {e}")),
        Ok(c) => {
            let result = match c.stat(".").await {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("probe_failed: {e}")),
            };
            c.close().await;
            result
        }
    }
}

/// Default fleet file: `~/.remote-workspace/workspaces.toml`.
pub fn default_fleet_path() -> anyhow::Result<PathBuf> {
    Ok(crate::platform::home_dir("--fleet")?.join(".remote-workspace/workspaces.toml"))
}

/// A workspace entry to append to the fleet file.
pub struct NewEntry {
    pub name: String,
    pub host: String,
    pub remote_shell: RemoteShell,
    pub root: String,
    /// Absolute server path to record. Present for both managed installs (the
    /// managed path) and user-managed servers (`--remote-bin`).
    pub bin: Option<String>,
    pub label: Option<String>,
    pub config: Option<String>,
    pub state_base: Option<String>,
}

/// Check that `entry` can be added without colliding with an existing one.
/// Returns the stable-coded error the CLI surfaces (`workspace_already_exists`
/// / `duplicate_workspace_target`). `text` is the current fleet file contents
/// (empty string if none yet).
pub fn check_addable(text: &str, entry: &NewEntry) -> Result<(), DeployError> {
    let doc = parse_document(text)?;
    let Some(workspaces) = doc.get("workspaces").and_then(|w| w.as_table_like()) else {
        return Ok(());
    };
    if workspaces.get(&entry.name).is_some() {
        return Err(DeployError::new(
            "workspace_already_exists",
            format!("workspace '{}' already exists in the fleet", entry.name),
        ));
    }
    for (existing_name, item) in workspaces.iter() {
        let Some(tbl) = item.as_table_like() else {
            continue;
        };
        let host = tbl.get("host").and_then(|v| v.as_str());
        let root = tbl.get("root").and_then(|v| v.as_str());
        if host == Some(entry.host.as_str()) && root == Some(entry.root.as_str()) {
            return Err(DeployError::new(
                "duplicate_workspace_target",
                format!(
                    "workspace '{existing_name}' already targets {}:{}; \
                     they would contend for the same server state lock",
                    entry.host, entry.root
                ),
            ));
        }
    }
    Ok(())
}

/// Atomically add a workspace entry to the fleet file, preserving existing
/// entries, comments, and formatting. Serializes concurrent writers with a
/// lock, re-checks for collisions under the lock, validates the whole result
/// with `parse_fleet`, and installs via a temp file + fsync + rename so the
/// fleet is never left partially written.
pub fn add_workspace_entry(fleet_path: &Path, entry: &NewEntry) -> Result<(), DeployError> {
    use toml_edit::{value, Item, Table};

    if let Some(parent) = fleet_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            DeployError::new("fleet_write_failed", format!("create {parent:?}: {e}"))
        })?;
    }

    let _lock = acquire_fleet_lock(fleet_path)?;

    let text = match std::fs::read_to_string(fleet_path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return Err(DeployError::new(
                "fleet_write_failed",
                format!("read {fleet_path:?}: {e}"),
            ))
        }
    };

    // Authoritative re-check under the lock.
    check_addable(&text, entry)?;

    let mut doc = parse_document(&text)?;
    let workspaces = doc
        .entry("workspaces")
        .or_insert_with(|| Item::Table(Table::new()));
    let workspaces = workspaces
        .as_table_mut()
        .ok_or_else(|| DeployError::new("fleet_write_failed", "`workspaces` is not a table"))?;
    // Render as [workspaces.<name>] rather than an inline [workspaces] blob.
    workspaces.set_implicit(true);

    let mut tbl = Table::new();
    tbl.insert("host", value(&entry.host));
    tbl.insert("remote_shell", value(entry.remote_shell.to_string()));
    tbl.insert("root", value(&entry.root));
    if let Some(bin) = &entry.bin {
        tbl.insert("bin", value(bin));
    }
    if let Some(config) = &entry.config {
        tbl.insert("config", value(config));
    }
    if let Some(state_base) = &entry.state_base {
        tbl.insert("state_base", value(state_base));
    }
    if let Some(label) = &entry.label {
        tbl.insert("label", value(label));
    }
    workspaces.insert(&entry.name, Item::Table(tbl));

    let new_text = doc.to_string();
    // Full-schema validation before anything touches disk.
    parse_fleet(&new_text).map_err(|e| {
        DeployError::new(
            "fleet_write_failed",
            format!("refusing to write an invalid fleet config: {e}"),
        )
    })?;

    write_atomic(fleet_path, &new_text)
}

fn parse_document(text: &str) -> Result<toml_edit::DocumentMut, DeployError> {
    text.parse::<toml_edit::DocumentMut>()
        .map_err(|e| DeployError::new("fleet_write_failed", format!("parse fleet config: {e}")))
}

fn write_atomic(path: &Path, contents: &str) -> Result<(), DeployError> {
    use std::io::Write;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(".workspaces.toml.tmp-{}", std::process::id()));
    let write = || -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        // fsync the directory so the rename itself is durable.
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    };
    write().map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        DeployError::new("fleet_write_failed", format!("write {path:?}: {e}"))
    })
}

fn acquire_fleet_lock(fleet_path: &Path) -> Result<std::fs::File, DeployError> {
    let dir = fleet_path.parent().unwrap_or_else(|| Path::new("."));
    let path = dir.join(".workspaces.toml.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| DeployError::new("fleet_write_failed", format!("open {path:?}: {e}")))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if try_lock_exclusive(&f)
            .map_err(|e| DeployError::new("fleet_write_failed", format!("lock {path:?}: {e}")))?
        {
            return Ok(f);
        }
        if Instant::now() >= deadline {
            return Err(DeployError::new(
                "fleet_write_failed",
                format!("fleet config {path:?} is locked by another process"),
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;

    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(false),
        _ => Err(error),
    }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::mem::zeroed;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };

    let mut overlapped = unsafe { zeroed() };
    let locked = unsafe {
        LockFileEx(
            file.as_raw_handle() as HANDLE,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if locked != 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == ERROR_LOCK_VIOLATION as i32 => Ok(false),
        _ => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, host: &str, root: &str) -> NewEntry {
        NewEntry {
            name: name.into(),
            host: host.into(),
            remote_shell: RemoteShell::Posix,
            root: root.into(),
            bin: Some("/home/u/.local/lib/remote-workspace/remote-workspace-server".into()),
            label: Some("Lab".into()),
            config: None,
            state_base: None,
        }
    }

    #[test]
    fn add_creates_and_preserves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workspaces.toml");
        std::fs::write(
            &path,
            "# my fleet\n[workspaces.a]\nhost = \"h1\"\nroot = \"/r1\"\n",
        )
        .unwrap();

        add_workspace_entry(&path, &entry("b", "h2", "/r2")).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("# my fleet"), "comment preserved: {text}");
        assert!(text.contains("[workspaces.a]"));
        assert!(text.contains("[workspaces.b]"));
        assert!(text.contains("/home/u/.local/lib/remote-workspace/remote-workspace-server"));
        // The whole file must still validate.
        let parsed = parse_fleet(&text).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn add_first_workspace_to_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("workspaces.toml");
        add_workspace_entry(&path, &entry("only", "h", "/r")).unwrap();
        let parsed = parse_fleet(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn rejects_duplicate_name_and_target() {
        let text = "[workspaces.a]\nhost = \"h1\"\nroot = \"/r1\"\n";
        let dup_name = check_addable(text, &entry("a", "h9", "/r9")).unwrap_err();
        assert_eq!(dup_name.code, "workspace_already_exists");
        let dup_target = check_addable(text, &entry("b", "h1", "/r1")).unwrap_err();
        assert_eq!(dup_target.code, "duplicate_workspace_target");
        // A genuinely new entry is fine.
        check_addable(text, &entry("c", "h3", "/r3")).unwrap();
    }

    #[test]
    fn legacy_ssh_entries_default_to_posix_and_new_entries_persist_shell() {
        let legacy = "[workspaces.old]\nhost = \"linux\"\nroot = \"/work\"\n";
        let parsed = parse_fleet(legacy).unwrap();
        assert!(matches!(
            &parsed["old"].endpoint,
            Endpoint::Ssh {
                remote_shell: RemoteShell::Posix,
                ..
            }
        ));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workspaces.toml");
        let mut powershell = entry("windows", "win", r"C:\work");
        powershell.remote_shell = RemoteShell::Powershell;
        add_workspace_entry(&path, &powershell).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("remote_shell = \"powershell\""));
        assert!(matches!(
            &parse_fleet(&text).unwrap()["windows"].endpoint,
            Endpoint::Ssh {
                remote_shell: RemoteShell::Powershell,
                ..
            }
        ));
    }

    #[test]
    fn local_entry_rejects_remote_shell() {
        let text = "[workspaces.local]\nroot = \"C:\\\\work\"\nremote_shell = \"powershell\"\n";
        let error = parse_fleet(text).err().unwrap();
        assert!(error.to_string().contains("without an SSH host"));
    }

    #[test]
    fn one_ssh_host_cannot_declare_two_shells() {
        let text = "[workspaces.a]\nhost = \"same\"\nroot = \"/a\"\nremote_shell = \"posix\"\n\
                    [workspaces.b]\nhost = \"same\"\nroot = \"C:\\\\b\"\nremote_shell = \"powershell\"\n";
        let error = parse_fleet(text).err().unwrap();
        assert!(error.to_string().contains("conflicting remote_shell"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_lock_is_exclusive_and_released_on_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let first = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let second = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        assert!(try_lock_exclusive(&first).unwrap());
        assert!(!try_lock_exclusive(&second).unwrap());
        drop(first);
        assert!(try_lock_exclusive(&second).unwrap());
    }
}
