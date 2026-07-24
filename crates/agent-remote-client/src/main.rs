use std::path::PathBuf;

use agent_remote_client::{ArgvTransport, Client, ClientLog};
use agent_remote_protocol::{ExecOutput, ExecTermination, ListKind};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote",
    version,
    about = "Client for agent-remote remote workspaces"
)]
struct Cli {
    /// SSH host (as resolvable via ~/.ssh/config). Required unless --local is
    /// given.
    #[arg(long)]
    host: Option<String>,

    /// Path to the remote `agent-remote-server` binary. Defaults to expecting
    /// it on the remote PATH.
    #[arg(long, default_value = "agent-remote-server")]
    remote_bin: String,

    /// Workspace root on the remote host. Required for the connect-based
    /// commands; not used by `workspace add`, which takes its own --root.
    #[arg(long)]
    root: Option<String>,

    /// Optional remote config TOML path passed to the server.
    #[arg(long)]
    config: Option<String>,

    /// Optional base directory for server state instead of ~/.agent-remote on
    /// the remote (state still keyed per workspace under <base>/state/).
    #[arg(long)]
    state_base: Option<String>,

    /// Run the server locally as a subprocess instead of over SSH. The
    /// --remote-bin must be an executable path available locally.
    #[arg(long)]
    local: bool,

    /// Path to a client interaction log file (JSONL).
    #[arg(long)]
    log: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List a directory.
    Ls {
        path: String,
        #[arg(long)]
        offset: Option<usize>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Stat a file or directory.
    Stat { path: String },
    /// Read a file.
    Cat {
        path: String,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Create a new file with content from --file or stdin. Fails if the
    /// target already exists; modify existing files with `edit`.
    Create {
        path: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Replace an exact occurrence of --old-text with --new-text in an
    /// existing file.
    Edit {
        path: String,
        #[arg(long)]
        base_hash: String,
        #[arg(long)]
        old_text: String,
        #[arg(long)]
        new_text: String,
        #[arg(long)]
        replace_all: bool,
    },
    /// Execute a command remotely.
    Exec {
        /// Working directory relative to root.
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        timeout_ms: Option<u64>,
        /// Command argv (first element is the program).
        argv: Vec<String>,
    },
    /// Delete a file.
    Rm { path: String },
    /// Undo a recorded file operation.
    Undo { operation_id: String },
    /// Show operation history.
    History {
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show details of one operation.
    Op { operation_id: String },
    /// Query the status of a previously-issued request.
    Status { request_id: String },
    /// Prune stored history down to the most recent operations.
    Gc {
        /// How many operations to keep. Defaults to the server's
        /// --history-limit.
        #[arg(long)]
        keep: Option<usize>,
    },
    /// Manage the local workspace fleet. Trusted local admin operations, not
    /// exposed to the agent through MCP.
    Workspace {
        #[command(subcommand)]
        cmd: WorkspaceCmd,
    },
}

#[derive(Debug, Subcommand)]
enum WorkspaceCmd {
    /// Onboard a remote workspace: probe it, install or upgrade the managed
    /// server binary, run a real protocol probe, and record it in the fleet.
    Add {
        /// Name the agent will use to select this workspace.
        name: String,
        /// SSH host, resolvable via ~/.ssh/config (e.g. robot@workstation).
        #[arg(long)]
        host: String,
        /// Existing workspace root directory on the remote host.
        #[arg(long)]
        root: String,
        /// Human-readable description shown by list_workspaces.
        #[arg(long)]
        label: Option<String>,
        /// Remote server config TOML path (profiles).
        #[arg(long)]
        config: Option<String>,
        /// Base directory for remote server state.
        #[arg(long)]
        state_base: Option<String>,
        /// Use this server binary as-is (user-managed): verify compatibility
        /// but never install or overwrite it. Omit to use the managed binary.
        #[arg(long)]
        remote_bin: Option<String>,
        /// Fleet config file to update. Defaults to ~/.agent-remote/workspaces.toml.
        #[arg(long)]
        fleet: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async_main_real())
}

async fn async_main_real() -> Result<()> {
    let cli = Cli::parse();

    // Admin commands do not connect to a server; handle them before the
    // connect-based dispatch that requires --host/--root.
    if let Command::Workspace { cmd } = &cli.command {
        return handle_workspace(cmd).await;
    }

    let log = match &cli.log {
        Some(p) => Some(
            ClientLog::open(p.clone())
                .await
                .context("open client log")?,
        ),
        None => None,
    };

    let root = cli
        .root
        .clone()
        .ok_or_else(|| anyhow!("--root is required"))?;
    let endpoint = if cli.local {
        agent_remote_client::Endpoint::Local {
            server_bin: cli.remote_bin.clone(),
            root,
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        }
    } else {
        let host = cli.host.clone().ok_or_else(|| {
            anyhow!("--host is required (or use --local to run the server locally)")
        })?;
        agent_remote_client::Endpoint::Ssh {
            host,
            remote_bin: cli.remote_bin.clone(),
            root,
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        }
    };

    let transport = ArgvTransport {
        argv: endpoint.control_argv(),
    };
    let client = Client::connect(transport, log)
        .await
        .context("connect to server")?;

    match cli.command {
        Command::Ls {
            path,
            offset,
            limit,
        } => {
            let result = client.list(&path, offset, limit).await?;
            for e in result.entries {
                let kind = match e.kind {
                    ListKind::File => 'f',
                    ListKind::Dir => 'd',
                    ListKind::Symlink => 'l',
                };
                match e.size {
                    Some(s) => println!("{kind} {:>10} {}", s, e.name),
                    None => println!("{kind} {:>10} {}", '-', e.name),
                }
            }
            if let Some(next) = result.next_offset {
                eprintln!("[more entries: use --offset {next}]");
            }
        }
        Command::Stat { path } => {
            let s = client.stat(&path).await?;
            println!("{}", serde_json::to_string_pretty(&s)?);
        }
        Command::Cat {
            path,
            offset,
            limit,
        } => {
            let r = client.read(&path, offset, limit).await?;
            print!("{}", r.content);
            if let Some(next) = r.next_offset {
                eprintln!("\n[truncated: use --offset {next}]");
            }
        }
        Command::Create { path, file } => {
            let content = read_input(file)?;
            let res = client.create(&path, &content).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Edit {
            path,
            base_hash,
            old_text,
            new_text,
            replace_all,
        } => {
            let res = client
                .edit(&path, &base_hash, &old_text, &new_text, replace_all)
                .await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Exec {
            cwd,
            profile,
            timeout_ms,
            argv,
        } => {
            if argv.is_empty() {
                return Err(anyhow!("exec requires at least one argv element"));
            }
            let result = client.exec(argv, cwd, profile, timeout_ms).await?;
            print_exec_output(&result.stdout, false);
            print_exec_output(&result.stderr, true);
            eprintln!(
                "[{:?}] operation_id={} duration_ms={}",
                result.termination, result.operation_id, result.duration_ms
            );
            let code = match result.termination {
                ExecTermination::Exited { code } => code,
                ExecTermination::TimedOut => 124,
                ExecTermination::Signaled { signal } => 128 + signal,
            };
            std::process::exit(code);
        }
        Command::Rm { path } => {
            let res = client.delete(&path).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Undo { operation_id } => {
            let res = client.undo(&operation_id).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::History { limit } => {
            let ops = client.history(limit).await?;
            println!("{}", serde_json::to_string_pretty(&ops)?);
        }
        Command::Op { operation_id } => {
            let d = client.operation_get(&operation_id).await?;
            println!("{}", serde_json::to_string_pretty(&d)?);
        }
        Command::Status { request_id } => {
            let r = client.request_status(&request_id).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        Command::Gc { keep } => {
            let r = client.gc(keep).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        // Handled before the connect path.
        Command::Workspace { .. } => unreachable!(),
    }
    Ok(())
}

async fn handle_workspace(cmd: &WorkspaceCmd) -> Result<()> {
    match cmd {
        WorkspaceCmd::Add {
            name,
            host,
            root,
            label,
            config,
            state_base,
            remote_bin,
            fleet,
        } => {
            workspace_add(WorkspaceAddParams {
                name,
                host,
                root,
                label: label.as_deref(),
                config: config.as_deref(),
                state_base: state_base.as_deref(),
                remote_bin: remote_bin.as_deref(),
                fleet: fleet.clone(),
            })
            .await
        }
    }
}

struct WorkspaceAddParams<'a> {
    name: &'a str,
    host: &'a str,
    root: &'a str,
    label: Option<&'a str>,
    config: Option<&'a str>,
    state_base: Option<&'a str>,
    remote_bin: Option<&'a str>,
    fleet: Option<PathBuf>,
}

async fn workspace_add(p: WorkspaceAddParams<'_>) -> Result<()> {
    // Run the transaction, tagging any failure with the workspace/host context.
    match workspace_add_inner(&p).await {
        Ok(()) => Ok(()),
        Err(e) => Err(anyhow!(
            "Cannot add workspace '{}' ({}): {}",
            p.name,
            p.host,
            e
        )),
    }
}

async fn workspace_add_inner(p: &WorkspaceAddParams<'_>) -> Result<()> {
    use agent_remote_client::deploy::{self, ServerStep};
    use agent_remote_client::fleet::{self, NewEntry};

    let fleet_path = match &p.fleet {
        Some(path) => path.clone(),
        None => fleet::default_fleet_path()?,
    };

    println!("Adding workspace '{}'", p.name);

    // 1-2. Reject collisions up front (fast, before touching the remote).
    let preview = NewEntry {
        name: p.name.into(),
        host: p.host.into(),
        root: p.root.into(),
        bin: None,
        label: None,
        config: None,
        state_base: None,
    };
    let existing = std::fs::read_to_string(&fleet_path).unwrap_or_default();
    fleet::check_addable(&existing, &preview).map_err(anyhow::Error::msg)?;

    // 3-5. Probe SSH, remote platform, and the workspace root.
    let platform = deploy::probe_platform(p.host).map_err(anyhow::Error::msg)?;
    println!("  {:<22} connected", "SSH");
    println!("  {:<22} {}", "Remote platform", platform.label());
    deploy::validate_root(p.host, p.root).map_err(anyhow::Error::msg)?;
    println!("  {:<22} valid", "Workspace root");

    // 6-10. Resolve the server binary: user-managed (never installed) or the
    // managed path (installed/upgraded as needed).
    let (bin, protocol) = if let Some(custom) = p.remote_bin {
        let v = deploy::check_custom_bin(p.host, custom).map_err(anyhow::Error::msg)?;
        println!(
            "  {:<22} user-managed {} (not modified)",
            "Server", v.software_version
        );
        (custom.to_string(), v.protocol_version)
    } else {
        let managed = platform.managed_bin();
        match deploy::deploy_managed(p.host, &platform.os, &platform.arch, &managed)
            .map_err(anyhow::Error::msg)?
        {
            ServerStep::Installed(o) => {
                let msg = match &o.previous {
                    _ if !o.installed => format!("up to date {}", o.current.software_version),
                    None => format!("installed {}", o.current.software_version),
                    Some(prev) => format!(
                        "upgraded {} -> {}",
                        prev.software_version, o.current.software_version
                    ),
                };
                println!("  {:<22} {}", "Server", msg);
                (managed, o.current.protocol_version)
            }
            ServerStep::AlreadyCurrent(v) => {
                println!(
                    "  {:<22} up to date {}",
                    "Server", v.software_version
                );
                (managed, v.protocol_version)
            }
        }
    };
    println!("  {:<22} {}", "Protocol", protocol);

    // 11. Real protocol round-trip against the target root before committing.
    let endpoint = agent_remote_client::Endpoint::Ssh {
        host: p.host.into(),
        remote_bin: bin.clone(),
        root: p.root.into(),
        state_base: p.state_base.map(str::to_string),
        config: p.config.map(str::to_string),
    };
    if let Err(e) = fleet::check_workspace(&endpoint).await {
        return Err(anyhow!("server_probe_failed: {e}"));
    }
    println!("  {:<22} passed", "Workspace probe");

    // 12. Commit the fleet entry atomically.
    let entry = NewEntry {
        name: p.name.into(),
        host: p.host.into(),
        root: p.root.into(),
        bin: Some(bin),
        label: p.label.map(str::to_string),
        config: p.config.map(str::to_string),
        state_base: p.state_base.map(str::to_string),
    };
    fleet::add_workspace_entry(&fleet_path, &entry).map_err(anyhow::Error::msg)?;
    println!("  {:<22} updated", "Fleet configuration");
    println!("Workspace '{}' is ready.", p.name);
    Ok(())
}

fn print_exec_output(output: &ExecOutput, stderr: bool) {
    use std::io::Write;

    let mut text = output.prefix.clone();
    if output.omitted_bytes > 0 {
        text.push_str(&format!("\n[{} bytes omitted]\n", output.omitted_bytes));
    }
    text.push_str(&output.suffix);
    if stderr {
        let _ = std::io::stderr().write_all(text.as_bytes());
    } else {
        let _ = std::io::stdout().write_all(text.as_bytes());
    }
}

fn read_input(file: Option<PathBuf>) -> Result<String> {
    match file {
        Some(p) => Ok(std::fs::read_to_string(&p).with_context(|| format!("read {p:?}"))?),
        None => {
            use std::io::Read;
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            Ok(s)
        }
    }
}
