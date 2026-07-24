use std::path::PathBuf;
use std::process::Command;

fn server_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/agent-remote-server");
    p
}

/// Fake ssh, with a banner on stdout like a remote rc file that echoes.
const FAKE_SSH: &str = r#"#!/usr/bin/env bash
echo "MOTD: reboot scheduled Sunday"
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

struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    fleet: PathBuf,
    path: String,
    release: PathBuf,
}

fn setup() -> Option<Env> {
    let srv = server_bin();
    if !srv.exists() {
        eprintln!("server binary missing; run `cargo build -p agent-remote-server` first");
        return None;
    }
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();

    let fakebin = tmp.path().join("fakebin");
    std::fs::create_dir_all(&fakebin).unwrap();
    let ssh = fakebin.join("ssh");
    std::fs::write(&ssh, FAKE_SSH).unwrap();
    let mut perm = std::fs::metadata(&ssh).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
    std::fs::set_permissions(&ssh, perm).unwrap();

    let release = tmp.path().join("release");
    std::fs::create_dir_all(&release).unwrap();
    let artifact = "agent-remote-server-linux-x86_64-musl";
    let bytes = std::fs::read(&srv).unwrap();
    std::fs::write(release.join(artifact), &bytes).unwrap();
    let sha = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(&bytes))
    };
    let vj = Command::new(&srv).arg("--version-json").output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&vj.stdout).unwrap();
    let manifest = serde_json::json!({
        "software_version": v["software_version"],
        "protocol_version": v["protocol_version"],
        "artifacts": [{"os":"linux","arch":"x86_64","file":artifact,"sha256":sha}],
    });
    std::fs::write(
        release.join("release-manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    let path = format!(
        "{}:{}",
        fakebin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let fleet = home.join("workspaces.toml");
    Some(Env {
        _tmp: tmp,
        home,
        fleet,
        path,
        release,
    })
}

impl Env {
    fn add(&self, name: &str, root: &str) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_agent-remote"))
            .args([
                "workspace",
                "add",
                name,
                "--host",
                "robot@ws",
                "--root",
                root,
                "--fleet",
            ])
            .arg(&self.fleet)
            .env("HOME", &self.home)
            .env("PATH", &self.path)
            .env("AGENT_REMOTE_RELEASE_BASE", &self.release)
            .env_remove("XDG_CACHE_HOME")
            .output()
            .unwrap()
    }

    fn upgrade(&self, args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_agent-remote"))
            .args(["workspace", "upgrade"])
            .args(args)
            .args(["--fleet"])
            .arg(&self.fleet)
            .env("HOME", &self.home)
            .env("PATH", &self.path)
            .env("AGENT_REMOTE_RELEASE_BASE", &self.release)
            .env_remove("XDG_CACHE_HOME")
            .output()
            .unwrap()
    }
}

// `add` refuses an existing workspace, so `upgrade` is the path for picking up
// a new release on hosts already in the fleet.
#[test]
fn upgrade_covers_registered_workspaces() {
    let Some(env) = setup() else { return };
    for dir in ["p1", "p2"] {
        std::fs::create_dir_all(env.home.join(dir)).unwrap();
    }
    let p1 = env.home.join("p1").to_string_lossy().to_string();
    let p2 = env.home.join("p2").to_string_lossy().to_string();
    assert!(env.add("a", &p1).status.success());
    assert!(env.add("b", &p2).status.success());

    let before = std::fs::read_to_string(&env.fleet).unwrap();

    // Both workspaces share one SSH identity, so the binary is handled once.
    let out = env.upgrade(&[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "upgrade failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("up to date"), "{stdout}");
    assert!(
        stdout.contains("already handled"),
        "second workspace on the same host must not reinstall: {stdout}"
    );
    // Upgrading never rewrites the fleet.
    assert_eq!(std::fs::read_to_string(&env.fleet).unwrap(), before);

    // A single workspace can be targeted by name.
    let one = env.upgrade(&["a"]);
    assert!(one.status.success());
    let one_out = String::from_utf8_lossy(&one.stdout);
    assert!(one_out.contains("a ["), "{one_out}");
    assert!(
        !one_out.contains("b ["),
        "only 'a' was requested: {one_out}"
    );

    // An unknown name is a clear error, not a silent no-op.
    let bad = env.upgrade(&["nope"]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("unknown_workspace"));
}

#[test]
fn add_installs_probes_and_records() {
    let Some(env) = setup() else { return };
    let proj = env.home.join("proj");
    std::fs::create_dir_all(&proj).unwrap();
    let proj = proj.to_string_lossy().to_string();

    let out = env.add("robot", &proj);
    assert!(
        out.status.success(),
        "add failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Server                 installed"),
        "{stdout}"
    );
    assert!(stdout.contains("Workspace probe        passed"), "{stdout}");
    assert!(stdout.contains("is ready"), "{stdout}");

    let fleet = std::fs::read_to_string(&env.fleet).unwrap();
    assert!(fleet.contains("[workspaces.robot]"));
    assert!(fleet.contains(".local/lib/agent-remote/agent-remote-server"));
    // The managed binary was actually installed on the (loopback) remote.
    assert!(env
        .home
        .join(".local/lib/agent-remote/agent-remote-server")
        .exists());

    // Duplicate name is rejected before any remote work, leaving the fleet
    // unchanged.
    let dup = env.add("robot", &proj);
    assert!(!dup.status.success());
    assert!(String::from_utf8_lossy(&dup.stderr).contains("workspace_already_exists"));

    // Duplicate (host, root) under a new name is rejected too.
    let dup_target = env.add("other", &proj);
    assert!(!dup_target.status.success());
    assert!(String::from_utf8_lossy(&dup_target.stderr).contains("duplicate_workspace_target"));

    // The same directory named differently (trailing slash, `/.`) must also be
    // caught: entries are recorded canonically, so path aliases cannot smuggle
    // in a second workspace contending for one server state lock.
    for alias in [format!("{proj}/"), format!("{proj}/."), format!("{proj}//")] {
        let dup_alias = env.add("aliased", &alias);
        assert!(
            !dup_alias.status.success(),
            "alias {alias} must not be addable"
        );
        assert!(
            String::from_utf8_lossy(&dup_alias.stderr).contains("duplicate_workspace_target"),
            "alias {alias} stderr: {}",
            String::from_utf8_lossy(&dup_alias.stderr)
        );
    }

    // Missing root fails after the platform probe.
    let missing = env.home.join("nope").to_string_lossy().to_string();
    let bad = env.add("bad", &missing);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("workspace_root_not_found"));

    // A genuinely new workspace appends without disturbing the first.
    let proj2 = env.home.join("proj2");
    std::fs::create_dir_all(&proj2).unwrap();
    let ok2 = env.add("lab", &proj2.to_string_lossy());
    assert!(ok2.status.success());
    let fleet = std::fs::read_to_string(&env.fleet).unwrap();
    assert!(fleet.contains("[workspaces.robot]") && fleet.contains("[workspaces.lab]"));
}
