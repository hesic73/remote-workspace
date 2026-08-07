use std::path::PathBuf;

use clap::Parser;
use remote_workspace_server::{Server, ServerOptions};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "remote-workspace-server",
    version,
    about = "Remote workspace endpoint for remote-workspace"
)]
struct Args {
    /// Workspace root that all paths are resolved relative to.
    #[arg(long)]
    root: Option<PathBuf>,

    /// Base directory for server state (history, request table).
    /// State lives at `<base>/state/<name>-<hash>`, keyed by the canonical
    /// root path, so the workspace itself stays untouched. Defaults to
    /// `~/.remote-workspace`.
    #[arg(long)]
    state_base: Option<PathBuf>,

    /// Print `{software_version, protocol_version}` as JSON and exit, without
    /// touching the workspace, config, or state directory. Stable across
    /// releases; used by the client to decide whether to install/upgrade the
    /// managed server binary.
    #[arg(long)]
    version_json: bool,

    /// Internal self-install: this binary, uploaded to a temporary path, takes
    /// the install lock next to <PATH> and atomically replaces the server there
    /// only if strictly older, printing the outcome as JSON.
    #[arg(long, hide = true, value_name = "MANAGED_PATH")]
    install_to: Option<PathBuf>,

    /// Expected SHA-256 (hex) of this uploaded binary, verified before install.
    #[arg(long, hide = true, requires = "install_to")]
    expect_sha256: Option<String>,

    /// Path to a TOML config file with profile setup scripts.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Keep only this many recent operations (older ones are pruned at startup
    /// and on gc). 0 disables pruning.
    #[arg(long, default_value_t = 1000)]
    history_limit: usize,

    /// Evict scratch files idle (neither written nor read) for this many days,
    /// on gc and at most once a day at startup. 0 disables sweeping. Scratch is
    /// a staging area, not storage: anything worth keeping belongs in the
    /// workspace or on the local machine.
    #[arg(long, default_value_t = 7)]
    scratch_max_age_days: u64,

    /// Exit after this many seconds with no request arriving and none running,
    /// releasing the workspace's state lock. Clients reconnect on demand, so
    /// the cost of timing out is one SSH connect on the next call; the cost of
    /// not timing out is a workspace that stays locked for as long as a dead
    /// SSH session goes unnoticed on this host. 0 disables it.
    #[arg(long, default_value_t = 900)]
    idle_timeout_secs: u64,

    /// Internal raw data plane: stream stdin into this staging file (created
    /// by upload_prepare on the resident server). Does not open the state
    /// directory, so it cannot conflict with the resident server's lock.
    #[arg(long, hide = true, value_name = "STAGING_PATH")]
    transfer_receive: Option<PathBuf>,

    /// Internal: declared byte count for --transfer-receive.
    #[arg(long, hide = true, requires = "transfer_receive")]
    expect_size: Option<u64>,

    /// Internal raw data plane: stream this workspace/@scratch file to stdout
    /// (JSON size header, raw bytes, JSON sha256 trailer). Requires --root.
    #[arg(
        long,
        hide = true,
        value_name = "PATH",
        conflicts_with = "transfer_receive"
    )]
    transfer_send: Option<String>,

    /// Internal: encode data-plane chunks as Base64 lines for text-only
    /// transports such as Windows PowerShell over OpenSSH.
    #[arg(long, hide = true)]
    transfer_base64: bool,
}

fn resolve_state_base(state_base: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    match state_base {
        Some(b) => Ok(b),
        None => {
            #[cfg(unix)]
            const HOME_ENV: &str = "HOME";
            #[cfg(windows)]
            const HOME_ENV: &str = "USERPROFILE";
            let home = std::env::var_os(HOME_ENV)
                .ok_or_else(|| anyhow::anyhow!("{HOME_ENV} is not set; pass --state-base"))?;
            Ok(PathBuf::from(home).join(".remote-workspace"))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Lifecycle events are the whole record of a remote server's life, and
    // RUST_LOG cannot be set through `ssh host '<binary> ...'` anyway, so the
    // useful level is the default rather than something to opt into.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    if args.version_json {
        let info = remote_workspace_protocol::VersionInfo {
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: remote_workspace_protocol::PROTOCOL_VERSION,
        };
        println!("{}", serde_json::to_string(&info)?);
        return Ok(());
    }

    if let Some(managed) = args.install_to {
        return remote_workspace_server::install::run_install_to(
            &managed,
            args.expect_sha256.as_deref(),
        );
    }

    if let Some(staging) = args.transfer_receive {
        let expect_size = args
            .expect_size
            .ok_or_else(|| anyhow::anyhow!("--transfer-receive requires --expect-size"))?;
        return finish_transfer(remote_workspace_server::transfer::run_transfer_receive(
            &staging,
            expect_size,
            args.transfer_base64,
        ));
    }

    let base = resolve_state_base(args.state_base)?;

    if let Some(path) = args.transfer_send {
        let root = args
            .root
            .ok_or_else(|| anyhow::anyhow!("--transfer-send requires --root"))?;
        return finish_transfer(remote_workspace_server::transfer::run_transfer_send(
            &root,
            &base,
            &path,
            args.transfer_base64,
        ));
    }

    let root = args
        .root
        .ok_or_else(|| anyhow::anyhow!("--root is required"))?;
    let state_dir = remote_workspace_server::state_dir_under(&base, &root)?;

    let opts = ServerOptions {
        root,
        state_dir,
        config_path: args.config,
        history_limit: (args.history_limit > 0).then_some(args.history_limit),
        scratch_max_age: (args.scratch_max_age_days > 0)
            .then(|| std::time::Duration::from_secs(args.scratch_max_age_days * 86_400)),
        idle_timeout: (args.idle_timeout_secs > 0)
            .then(|| std::time::Duration::from_secs(args.idle_timeout_secs)),
    };

    let result = Server::new(opts)?.run_stdio().await;
    #[cfg(windows)]
    // Tokio runtime teardown can retain Windows pipe/runtime worker state after
    // stdio has completed; framing is flushed before this explicit process exit.
    match result {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(1);
        }
    }
    #[cfg(not(windows))]
    {
        result?;
        Ok(())
    }
}

fn finish_transfer(result: anyhow::Result<()>) -> anyhow::Result<()> {
    #[cfg(windows)]
    // Transfer functions flush their stdout before returning; avoid the same
    // Windows runtime teardown hang as the control path.
    match result {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(1);
        }
    }
    #[cfg(not(windows))]
    result
}
