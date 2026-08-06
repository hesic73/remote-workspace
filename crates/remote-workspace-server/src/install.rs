use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use remote_workspace_protocol::{should_replace, InstallOutcome, VersionInfo, PROTOCOL_VERSION};
use sha2::{Digest, Sha256};

use crate::fsync::fsync_dir;

const LOCK_GRACE: Duration = Duration::from_secs(30);

/// Self-install entrypoint: this process is the freshly-uploaded binary running
/// from a temporary path. Under the per-libdir install lock it re-probes the
/// currently installed server and atomically replaces it only if strictly older
/// (the monotonic rule), then reports the outcome as JSON on stdout. Running as
/// the desired binary means the version comparison and the swap happen in one
/// locked process, so concurrent installers can never race a downgrade in.
pub fn run_install_to(managed: &Path, expect_sha256: Option<&str>) -> Result<()> {
    let self_path = std::env::current_exe().context("resolve current executable")?;

    if let Some(expect) = expect_sha256 {
        let bytes = std::fs::read(&self_path).context("read own binary for checksum")?;
        let got = hex::encode(Sha256::digest(&bytes));
        if !got.eq_ignore_ascii_case(expect) {
            bail!("uploaded binary checksum mismatch: expected {expect}, got {got}");
        }
    }

    let desired = VersionInfo {
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
    };

    let libdir = managed
        .parent()
        .ok_or_else(|| anyhow!("managed path {managed:?} has no parent directory"))?;
    std::fs::create_dir_all(libdir)
        .with_context(|| format!("create install directory {libdir:?}"))?;

    let _lock = acquire_install_lock(libdir)?;

    // The check must happen inside the lock: a version observed before
    // acquiring it is not authoritative.
    let previous = probe_installed(managed);
    let installed = should_replace(previous.as_ref(), &desired);
    if installed {
        // Rename of a running executable is safe on Linux: existing processes
        // keep the old inode, new spawns get the new file.
        std::fs::rename(&self_path, managed)
            .with_context(|| format!("install {self_path:?} -> {managed:?}"))?;
        let _ = fsync_dir(libdir);
    } else {
        // Keep the newer/equal server; drop our now-unused upload.
        let _ = std::fs::remove_file(&self_path);
    }

    let current = probe_installed(managed).ok_or_else(|| {
        anyhow!("managed server {managed:?} did not report a version after install")
    })?;
    let outcome = InstallOutcome {
        installed,
        previous,
        current,
    };
    println!("{}", serde_json::to_string(&outcome)?);
    Ok(())
}

/// Run `<path> --version-json` and parse it. `None` means "no version to
/// compare against": a missing path, a legacy server predating `--version-json`,
/// or a binary too broken to answer. All three are deliberately replaceable --
/// unlike the client-side probe, this runs the binary directly, so its output
/// is not exposed to remote shell banners and unparseable really does mean
/// broken.
fn probe_installed(path: &Path) -> Option<VersionInfo> {
    let out = std::process::Command::new(path)
        .arg("--version-json")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

/// Exclusive advisory lock on `<libdir>/install.lock`, retried until the grace
/// elapses. Dropped (released by the kernel) when this process exits.
fn acquire_install_lock(libdir: &Path) -> Result<std::fs::File> {
    let path: PathBuf = libdir.join("install.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("open install lock {path:?}"))?;
    let deadline = Instant::now() + LOCK_GRACE;
    loop {
        if crate::locking::try_lock_exclusive(&f, 0)
            .with_context(|| format!("lock install file {path:?}"))?
        {
            return Ok(f);
        }
        if Instant::now() >= deadline {
            bail!("install lock {path:?} is held by another installer; try again");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
