use std::io::{Read as _, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_remote_protocol::{preflight, InstallOutcome, Preflight, VersionInfo, PROTOCOL_VERSION};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::shell_quote;

const DEFAULT_RELEASE_REPO: &str = "https://github.com/hesic73/agent-remote/releases/download";
const RELEASE_BASE_ENV: &str = "AGENT_REMOTE_RELEASE_BASE";

/// A failure during onboarding, tagged with a stable machine-readable code (see
/// the onboarding design doc, section 18) so the CLI can report which layer
/// failed with an actionable message.
#[derive(Debug)]
pub struct DeployError {
    pub code: &'static str,
    pub message: String,
}

impl DeployError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DeployError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DeployError {}

type Result<T> = std::result::Result<T, DeployError>;

/// Remote platform, as probed via `uname` plus the remote `$HOME`.
#[derive(Debug, Clone)]
pub struct Platform {
    pub os: String,
    pub arch: String,
    pub home: String,
}

impl Platform {
    /// Human label, e.g. `linux-x86_64`.
    pub fn label(&self) -> String {
        format!("{}-{}", self.os, self.arch)
    }

    /// Default managed server path for this identity: one active binary per SSH
    /// identity, shared by every workspace on it.
    pub fn managed_bin(&self) -> String {
        format!("{}/.local/lib/agent-remote/agent-remote-server", self.home)
    }
}

/// Map a probed platform to the CI artifact target label (e.g.
/// `linux-x86_64-musl`). Only the currently supported targets resolve; anything
/// else is a hard error rather than an approximate match.
pub fn artifact_target(os: &str, arch: &str) -> Result<String> {
    match (os, arch) {
        ("linux", "x86_64") => Ok("linux-x86_64-musl".into()),
        _ => Err(DeployError::new(
            "unsupported_remote_platform",
            format!("no prebuilt server artifact for {os}-{arch} (supported: linux-x86_64)"),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct ReleaseManifest {
    software_version: String,
    protocol_version: u32,
    artifacts: Vec<ManifestArtifact>,
}

#[derive(Debug, Deserialize)]
struct ManifestArtifact {
    os: String,
    arch: String,
    file: String,
    sha256: String,
}

/// Where release artifacts (manifest, binaries, checksums) are fetched from.
/// Defaults to this client's own GitHub release; overridable via
/// `AGENT_REMOTE_RELEASE_BASE` (an `https://` base, a local directory, or a
/// `file://` URL) for air-gapped mirrors and local testing.
struct ReleaseSource {
    base: String,
}

impl ReleaseSource {
    fn resolve(client_version: &str) -> Self {
        let base = std::env::var(RELEASE_BASE_ENV).unwrap_or_else(|_| {
            format!("{DEFAULT_RELEASE_REPO}/v{client_version}")
        });
        Self { base }
    }

    fn fetch(&self, name: &str) -> Result<Vec<u8>> {
        let base = &self.base;
        if base.starts_with("http://") || base.starts_with("https://") {
            let url = format!("{}/{name}", base.trim_end_matches('/'));
            let resp = ureq::get(&url).call().map_err(|e| {
                DeployError::new("release_manifest_unavailable", format!("GET {url}: {e}"))
            })?;
            let mut buf = Vec::new();
            resp.into_reader()
                .read_to_end(&mut buf)
                .map_err(|e| DeployError::new("release_manifest_unavailable", format!("read {url}: {e}")))?;
            Ok(buf)
        } else {
            let dir = base.strip_prefix("file://").unwrap_or(base);
            let path = PathBuf::from(dir).join(name);
            std::fs::read(&path).map_err(|e| {
                DeployError::new(
                    "release_manifest_unavailable",
                    format!("read {path:?}: {e}"),
                )
            })
        }
    }
}

/// A verified server artifact in the local cache, ready to upload.
pub struct CachedArtifact {
    pub path: PathBuf,
    pub sha256: String,
    pub desired: VersionInfo,
}

/// Resolve, download (if not already cached), and verify the server artifact
/// for `target`, returning its local path. Reuses a cached artifact whose
/// checksum already matches the manifest.
pub fn resolve_artifact(client_version: &str, os: &str, arch: &str, target: &str) -> Result<CachedArtifact> {
    let source = ReleaseSource::resolve(client_version);
    let manifest_bytes = source.fetch("release-manifest.json")?;
    let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
        DeployError::new("release_manifest_unavailable", format!("parse manifest: {e}"))
    })?;

    let artifact = manifest
        .artifacts
        .iter()
        .find(|a| a.os == os && a.arch == arch)
        .ok_or_else(|| {
            DeployError::new(
                "artifact_not_found",
                format!("release manifest has no artifact for {os}-{arch}"),
            )
        })?;

    let desired = VersionInfo {
        software_version: manifest.software_version.clone(),
        protocol_version: manifest.protocol_version,
    };

    let cache_dir = cache_dir()?.join(target);
    std::fs::create_dir_all(&cache_dir).map_err(|e| {
        DeployError::new("artifact_not_found", format!("create cache dir {cache_dir:?}: {e}"))
    })?;
    let cached = cache_dir.join("agent-remote-server");

    // Reuse a cached artifact whose checksum already matches the manifest.
    if let Ok(bytes) = std::fs::read(&cached) {
        if hex::encode(Sha256::digest(&bytes)).eq_ignore_ascii_case(&artifact.sha256) {
            return Ok(CachedArtifact {
                path: cached,
                sha256: artifact.sha256.to_lowercase(),
                desired,
            });
        }
    }

    let bytes = source.fetch(&artifact.file)?;
    let got = hex::encode(Sha256::digest(&bytes));
    if !got.eq_ignore_ascii_case(&artifact.sha256) {
        return Err(DeployError::new(
            "artifact_checksum_mismatch",
            format!(
                "downloaded {} has sha256 {got}, manifest expects {}",
                artifact.file, artifact.sha256
            ),
        ));
    }

    // Atomic install into the cache: write a temp file next to the target and
    // rename, so a concurrent reader never sees a half-written artifact.
    let tmp = cache_dir.join(format!("agent-remote-server.download-{}", unique_suffix()));
    std::fs::write(&tmp, &bytes)
        .and_then(|_| std::fs::rename(&tmp, &cached))
        .map_err(|e| DeployError::new("artifact_not_found", format!("cache artifact: {e}")))?;

    Ok(CachedArtifact {
        path: cached,
        sha256: got,
        desired,
    })
}

fn cache_dir() -> Result<PathBuf> {
    if let Some(x) = std::env::var_os("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(x).join("agent-remote").join("server"));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| DeployError::new("artifact_not_found", "HOME is not set"))?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join("agent-remote")
        .join("server"))
}

// ---- SSH helpers ----

fn ssh_prefix(host: &str) -> Vec<String> {
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

/// Run a `sh` script on the remote host (piped via stdin, so the script itself
/// needs no shell-quoting), returning captured stdout on success.
fn ssh_script(host: &str, script: &str) -> Result<String> {
    let mut argv = ssh_prefix(host);
    argv.push("sh".into());
    argv.push("-s".into());
    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| DeployError::new("ssh_connect_failed", format!("spawn ssh: {e}")))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .map_err(|e| DeployError::new("ssh_connect_failed", format!("write remote script: {e}")))?;
    let out = child
        .wait_with_output()
        .map_err(|e| DeployError::new("ssh_connect_failed", format!("ssh wait: {e}")))?;
    if !out.status.success() {
        return Err(DeployError::new(
            "ssh_connect_failed",
            format!(
                "remote command failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Probe the remote OS, architecture, and `$HOME` in one round trip.
pub fn probe_platform(host: &str) -> Result<Platform> {
    let out = ssh_script(
        host,
        "printf 'os=%s\\narch=%s\\nhome=%s\\n' \"$(uname -s)\" \"$(uname -m)\" \"$HOME\"\n",
    )
    .map_err(|e| DeployError::new("remote_probe_failed", e.message))?;
    let mut os = None;
    let mut arch = None;
    let mut home = None;
    for line in out.lines() {
        if let Some(v) = line.strip_prefix("os=") {
            os = Some(normalize_os(v));
        } else if let Some(v) = line.strip_prefix("arch=") {
            arch = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("home=") {
            home = Some(v.trim().to_string());
        }
    }
    match (os, arch, home) {
        (Some(os), Some(arch), Some(home)) if !home.is_empty() => Ok(Platform { os, arch, home }),
        _ => Err(DeployError::new(
            "remote_probe_failed",
            format!("could not parse remote platform from: {out:?}"),
        )),
    }
}

fn normalize_os(uname_s: &str) -> String {
    match uname_s.trim() {
        "Linux" => "linux".to_string(),
        "Darwin" => "darwin".to_string(),
        other => other.to_lowercase(),
    }
}

/// Validate the workspace root on the remote host: it must exist, be a
/// directory, and be accessible. Returns its canonical path. Never creates it.
pub fn validate_root(host: &str, root: &str) -> Result<String> {
    let script = format!(
        "r={q}\n\
         if [ ! -e \"$r\" ]; then echo NOROOT; \
         elif [ ! -d \"$r\" ]; then echo NOTDIR; \
         elif cd \"$r\" 2>/dev/null; then printf 'OK=%s\\n' \"$(pwd -P)\"; \
         else echo NOACCESS; fi\n",
        q = shell_quote(root)
    );
    let out = ssh_script(host, &script)?;
    let line = out.lines().next().unwrap_or("").trim();
    if let Some(canon) = line.strip_prefix("OK=") {
        Ok(canon.to_string())
    } else if line == "NOROOT" {
        Err(DeployError::new(
            "workspace_root_not_found",
            format!("remote workspace root {root:?} does not exist"),
        ))
    } else {
        Err(DeployError::new(
            "workspace_root_invalid",
            format!("remote workspace root {root:?} is not an accessible directory ({line})"),
        ))
    }
}

/// Probe an installed server's version. `None` means no such binary or one that
/// predates `--version-json` (a legacy server), which callers treat as
/// "needs install".
pub fn probe_installed(host: &str, bin: &str) -> Result<Option<VersionInfo>> {
    let script = format!(
        "b={q}\nif [ -x \"$b\" ]; then \"$b\" --version-json 2>/dev/null || true; fi\n",
        q = shell_quote(bin)
    );
    let out = ssh_script(host, &script)?;
    let trimmed = out.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(serde_json::from_str(trimmed).ok())
}

/// Upload the artifact to a unique temp path and run its self-install under the
/// remote lock. Returns the outcome reported by the installed binary.
pub fn upload_and_install(
    host: &str,
    artifact: &CachedArtifact,
    managed_bin: &str,
) -> Result<InstallOutcome> {
    let libdir = managed_bin
        .rsplit_once('/')
        .map(|(d, _)| d.to_string())
        .unwrap_or_else(|| ".".into());
    let tmp = format!("{libdir}/.agent-remote-server.upload-{}", unique_suffix());

    // Stream the artifact bytes to the remote temp path.
    let upload_cmd = format!(
        "mkdir -p {qlib} && cat > {qtmp} && chmod +x {qtmp}",
        qlib = shell_quote(&libdir),
        qtmp = shell_quote(&tmp),
    );
    let mut argv = ssh_prefix(host);
    argv.push(upload_cmd);
    let bytes = std::fs::read(&artifact.path)
        .map_err(|e| DeployError::new("remote_install_failed", format!("read cached artifact: {e}")))?;
    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| DeployError::new("ssh_connect_failed", format!("spawn ssh upload: {e}")))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&bytes)
        .map_err(|e| DeployError::new("remote_install_failed", format!("stream artifact: {e}")))?;
    let up = child
        .wait_with_output()
        .map_err(|e| DeployError::new("remote_install_failed", format!("upload wait: {e}")))?;
    if !up.status.success() {
        return Err(DeployError::new(
            "remote_install_failed",
            format!("upload failed: {}", String::from_utf8_lossy(&up.stderr).trim()),
        ));
    }

    // Confirm the uploaded binary runs on the remote arch and reports the
    // expected version before we let it install itself.
    let probe = probe_installed(host, &tmp)?;
    match &probe {
        Some(v) if *v == artifact.desired => {}
        other => {
            let _ = ssh_script(host, &format!("rm -f {}\n", shell_quote(&tmp)));
            return Err(DeployError::new(
                "remote_install_failed",
                format!(
                    "uploaded binary reported {other:?}, expected {:?} (wrong artifact or arch)",
                    artifact.desired
                ),
            ));
        }
    }

    // Locked compare-and-swap, performed by the uploaded binary itself.
    let install_cmd = format!(
        "{qtmp} --install-to {qmanaged} --expect-sha256 {qsha}\n",
        qtmp = shell_quote(&tmp),
        qmanaged = shell_quote(managed_bin),
        qsha = shell_quote(&artifact.sha256),
    );
    let out = ssh_script(host, &install_cmd)
        .map_err(|e| DeployError::new("remote_install_failed", e.message))?;
    let outcome: InstallOutcome = serde_json::from_str(out.trim()).map_err(|e| {
        DeployError::new(
            "remote_install_failed",
            format!("could not parse install outcome from {out:?}: {e}"),
        )
    })?;
    Ok(outcome)
}

/// What onboarding did with the managed server on the target.
pub enum ServerStep {
    /// The managed binary was installed or upgraded. Carries the outcome.
    Installed(InstallOutcome),
    /// A compatible server was already present; nothing was changed.
    AlreadyCurrent(VersionInfo),
}

/// Full managed-deploy step for a default (non-custom) server path: probe,
/// preflight, and install/upgrade only when needed. `managed_bin` is the fixed
/// path from `Platform::managed_bin`.
pub fn deploy_managed(
    host: &str,
    os: &str,
    arch: &str,
    managed_bin: &str,
) -> Result<ServerStep> {
    let client_version = env!("CARGO_PKG_VERSION");
    let installed = probe_installed(host, managed_bin)?;
    let desired = VersionInfo {
        software_version: client_version.to_string(),
        protocol_version: PROTOCOL_VERSION,
    };

    match preflight(installed.as_ref(), &desired) {
        Preflight::Connect => Ok(ServerStep::AlreadyCurrent(installed.unwrap())),
        Preflight::ClientTooOld => Err(client_too_old(installed.as_ref(), &desired)),
        Preflight::NeedInstall => {
            let target = artifact_target(os, arch)?;
            let artifact = resolve_artifact(client_version, os, arch, &target)?;
            let outcome = upload_and_install(host, &artifact, managed_bin)?;
            Ok(ServerStep::Installed(outcome))
        }
    }
}

/// Compatibility check for a user-managed (`--remote-bin`) server, which is
/// never installed or overwritten: it must already exist and speak the client's
/// protocol.
pub fn check_custom_bin(host: &str, bin: &str) -> Result<VersionInfo> {
    let desired = VersionInfo {
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
    };
    let installed = probe_installed(host, bin)?.ok_or_else(|| {
        DeployError::new(
            "server_probe_failed",
            format!("custom server {bin:?} is missing or does not support --version-json; it is user-managed and will not be installed"),
        )
    })?;
    match preflight(Some(&installed), &desired) {
        Preflight::Connect => Ok(installed),
        Preflight::ClientTooOld => Err(client_too_old(Some(&installed), &desired)),
        Preflight::NeedInstall => Err(DeployError::new(
            "server_probe_failed",
            format!(
                "custom server {bin:?} is {} (protocol {}), older than this client {} (protocol {}); \
                 update your user-managed binary (it will not be overwritten)",
                installed.software_version,
                installed.protocol_version,
                desired.software_version,
                desired.protocol_version
            ),
        )),
    }
}

fn client_too_old(installed: Option<&VersionInfo>, desired: &VersionInfo) -> DeployError {
    let (sw, proto) = installed
        .map(|v| (v.software_version.clone(), v.protocol_version))
        .unwrap_or_else(|| ("?".into(), 0));
    DeployError::new(
        "client_too_old",
        format!(
            "the remote server is {sw} (protocol {proto}), newer than this client {} (protocol {}); \
             update the local agent-remote client and retry (the remote server was not modified)",
            desired.software_version, desired.protocol_version
        ),
    )
}

fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{ts:016x}-{:x}-{n:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_target_supported_and_not() {
        assert_eq!(artifact_target("linux", "x86_64").unwrap(), "linux-x86_64-musl");
        let e = artifact_target("linux", "aarch64").unwrap_err();
        assert_eq!(e.code, "unsupported_remote_platform");
        let e = artifact_target("darwin", "arm64").unwrap_err();
        assert_eq!(e.code, "unsupported_remote_platform");
    }

    #[test]
    fn normalize_os_maps_uname() {
        assert_eq!(normalize_os("Linux"), "linux");
        assert_eq!(normalize_os("Darwin"), "darwin");
    }

    #[test]
    fn managed_bin_path() {
        let p = Platform {
            os: "linux".into(),
            arch: "x86_64".into(),
            home: "/home/robot".into(),
        };
        assert_eq!(
            p.managed_bin(),
            "/home/robot/.local/lib/agent-remote/agent-remote-server"
        );
        assert_eq!(p.label(), "linux-x86_64");
    }
}
