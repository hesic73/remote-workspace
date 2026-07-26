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

    /// Base directory for server state (history, undo blobs, request table).
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

    /// Keep only this many recent operations (older ones and their blobs are
    /// pruned at startup and on gc). 0 disables pruning.
    #[arg(long, default_value_t = 1000)]
    history_limit: usize,

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
}

fn resolve_state_base(state_base: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    match state_base {
        Some(b) => Ok(b),
        None => {
            let home = std::env::var_os("HOME")
                .ok_or_else(|| anyhow::anyhow!("HOME is not set; pass --state-base"))?;
            Ok(PathBuf::from(home).join(".remote-workspace"))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
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
        return remote_workspace_server::transfer::run_transfer_receive(&staging, expect_size);
    }

    let base = resolve_state_base(args.state_base)?;

    if let Some(path) = args.transfer_send {
        let root = args
            .root
            .ok_or_else(|| anyhow::anyhow!("--transfer-send requires --root"))?;
        return remote_workspace_server::transfer::run_transfer_send(&root, &base, &path);
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
    };

    Server::new(opts)?.run_stdio().await?;
    Ok(())
}
