#![cfg(unix)]

use std::path::PathBuf;

use remote_workspace_client::{
    deploy::{self, ServerStep},
    RemoteShell,
};

fn server_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/remote-workspace-server");
    p
}

/// Fake `ssh` that runs the "remote" command locally, exactly as ssh would
/// (skip `-o` option pairs and the host; join the rest for a shell). Lets the
/// real deploy code drive a loopback "remote" without ssh key setup.
///
/// It deliberately prints a banner to stdout first, the way a remote
/// `~/.bashrc` or `~/.zshenv` that echoes does on a non-interactive ssh
/// command. Every probe must locate its result despite that noise.
const FAKE_SSH: &str = r#"#!/usr/bin/env bash
echo "Welcome to the lab! Please read /etc/motd"
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    -*) shift ;;
    *) args+=("$1"); shift ;;
  esac
done
rest=("${args[@]:1}")
if [ "${#rest[@]}" -eq 2 ] && [ "${rest[0]}" = "sh" ] && [ "${rest[1]}" = "-s" ]; then
  exec sh -s
else
  exec sh -c "${rest[*]}"
fi
"#;

fn version_json(bin: &PathBuf) -> (String, u32) {
    let out = std::process::Command::new(bin)
        .arg("--version-json")
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    (
        v["software_version"].as_str().unwrap().to_string(),
        v["protocol_version"].as_u64().unwrap() as u32,
    )
}

#[test]
fn managed_deploy_installs_then_reuses() {
    let bin = server_bin();
    if !bin.exists() {
        eprintln!("server binary missing; run `cargo build -p remote-workspace-server` first");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().to_path_buf();

    // Fake ssh on PATH.
    let fakebin = home.join("fakebin");
    std::fs::create_dir_all(&fakebin).unwrap();
    let ssh = fakebin.join("ssh");
    std::fs::write(&ssh, FAKE_SSH).unwrap();
    let mut perm = std::fs::metadata(&ssh).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
    std::fs::set_permissions(&ssh, perm).unwrap();

    // Local release base: the built server as the x86_64 artifact + manifest.
    let (sw, proto) = version_json(&bin);
    let release = home.join("release");
    std::fs::create_dir_all(&release).unwrap();
    let artifact_name = "remote-workspace-server-linux-x86_64-musl";
    let bytes = std::fs::read(&bin).unwrap();
    std::fs::write(release.join(artifact_name), &bytes).unwrap();
    let sha = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(&bytes))
    };
    let manifest = serde_json::json!({
        "software_version": sw,
        "protocol_version": proto,
        "artifacts": [{"os":"linux","arch":"x86_64","file":artifact_name,"sha256":sha}],
    });
    std::fs::write(
        release.join("release-manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    // Point the deploy code at the fake ssh, temp HOME, and local release base.
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", fakebin.display(), old_path));
    std::env::set_var("HOME", &home);
    std::env::remove_var("XDG_CACHE_HOME");
    std::env::set_var("REMOTE_WORKSPACE_RELEASE_BASE", &release);

    let host = "loopback";

    // Platform probe reads the (fake) remote uname + $HOME.
    let platform = deploy::probe_platform(host, RemoteShell::Posix).unwrap();
    assert_eq!(platform.os, "linux");
    assert_eq!(platform.home, home.to_string_lossy());
    let managed = platform.managed_bin();

    // Root validation: existing dir passes, missing dir fails with a code.
    let canon = deploy::validate_root(host, RemoteShell::Posix, &home.to_string_lossy()).unwrap();
    assert!(!canon.is_empty());
    let missing = home.join("does-not-exist");
    let err =
        deploy::validate_root(host, RemoteShell::Posix, &missing.to_string_lossy()).unwrap_err();
    assert_eq!(err.code, "workspace_root_not_found");

    // First deploy installs the managed binary.
    match deploy::deploy_managed(
        host,
        RemoteShell::Posix,
        &platform.os,
        &platform.arch,
        &managed,
    )
    .unwrap()
    {
        ServerStep::Installed(o) => {
            assert!(o.installed);
            assert!(o.previous.is_none());
            assert_eq!(o.current.protocol_version, proto);
        }
        ServerStep::AlreadyCurrent(_) => panic!("expected a fresh install"),
    }
    assert!(PathBuf::from(&managed).exists());

    // Second deploy sees an equal version and reuses it (no downgrade, no swap).
    match deploy::deploy_managed(
        host,
        RemoteShell::Posix,
        &platform.os,
        &platform.arch,
        &managed,
    )
    .unwrap()
    {
        ServerStep::AlreadyCurrent(v) => assert_eq!(v.protocol_version, proto),
        ServerStep::Installed(_) => panic!("expected reuse, not reinstall"),
    }
}
