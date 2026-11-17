use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use remote_workspace_client::{ArgvTransport, Client, ClientLog, Endpoint, RemoteShell};
use remote_workspace_protocol::{ExecOutput, ExecTermination, ListKind, ReadOptions};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "remote-workspace",
    version,
    about = "Client for remote-workspace remote workspaces"
)]
struct Cli {
    /// Configured workspace name from the fleet file. Mutually exclusive with
    /// explicit target flags (--host, --root, --local, --config, --state-base,
    /// --remote-bin, --remote-shell).
    #[arg(short = 'w', long)]
    workspace: Option<String>,

    /// Fleet config file (TOML). Defaults to ~/.remote-workspace/workspaces.toml.
    #[arg(long, global = true)]
    fleet: Option<PathBuf>,

    /// SSH host (as resolvable via ~/.ssh/config). Required unless --local or
    /// --workspace is given.
    #[arg(long)]
    host: Option<String>,

    /// Path to the remote `remote-workspace-server` binary. Defaults to
    /// `remote-workspace-server` on the remote PATH.
    #[arg(long)]
    remote_bin: Option<String>,

    /// Shell used by the SSH server to interpret remote commands (default: posix).
    #[arg(long, value_enum)]
    remote_shell: Option<RemoteShell>,

    /// Workspace root on the remote host. Required unless --workspace is given;
    /// not used by `workspace` subcommands.
    #[arg(long)]
    root: Option<String>,

    /// Optional remote config TOML path passed to the server.
    #[arg(long)]
    config: Option<String>,

    /// Optional base directory for server state instead of ~/.remote-workspace on
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
    /// Read a file (byte paging, line range, or tail).
    Cat {
        path: String,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
        #[arg(long)]
        start_line: Option<u64>,
        #[arg(long)]
        max_lines: Option<u64>,
        #[arg(long)]
        tail_lines: Option<u64>,
        #[arg(long)]
        tail_bytes: Option<u64>,
    },
    /// Create a new file with content from --file or stdin. Fails if the
    /// target already exists; modify existing files with `edit`.
    Create {
        path: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Modify an existing file via exact text replacement (--old-text and
    /// --new-text) or full-file replacement (--replace-file).
    Edit {
        path: String,
        #[arg(long)]
        base_hash: String,
        #[arg(long)]
        old_text: Option<String>,
        #[arg(long)]
        new_text: Option<String>,
        #[arg(long)]
        replace_all: bool,
        #[arg(long)]
        replace_file: Option<PathBuf>,
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
    /// Show operation history.
    History {
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show details of one operation.
    Op { operation_id: String },
    /// Prune stored history down to the most recent operations.
    Gc {
        /// How many operations to keep. Defaults to the server's
        /// --history-limit.
        #[arg(long)]
        keep: Option<usize>,
    },
    /// Summarize how often each tool was called, from the logs written by
    /// `remote-workspace-mcp --log-dir`. Local only; connects to nothing.
    Stats {
        /// Log directory to read. Defaults to ~/.remote-workspace/log.
        #[arg(long)]
        log_dir: Option<PathBuf>,
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
        /// Override remote-shell detection.
        #[arg(long, value_enum)]
        remote_shell: Option<RemoteShell>,
    },
    /// List configured workspaces in the fleet.
    List,
    /// Remove a workspace entry from the fleet file (does not touch the remote
    /// host or its server state).
    #[command(visible_alias = "remove")]
    Rm {
        /// Workspace name to remove.
        name: String,
    },
    /// Install or upgrade the managed server for workspaces already in the
    /// fleet. One install per SSH identity, however many workspaces share it.
    /// Never touches a user-managed binary and never edits the fleet file.
    Upgrade {
        /// Workspace to upgrade. Omit to upgrade every workspace in the fleet.
        name: Option<String>,
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

fn default_log_dir() -> Result<PathBuf> {
    Ok(remote_workspace_client::home_dir("--log-dir")?.join(".remote-workspace/log"))
}

enum PreparedEdit {
    Edits(Vec<remote_workspace_protocol::EditSpec>),
    Replace(String),
}

fn prepare_edit(
    old_text: Option<String>,
    new_text: Option<String>,
    replace_all: bool,
    replace_file: Option<PathBuf>,
) -> Result<PreparedEdit> {
    if let Some(p) = replace_file {
        if old_text.is_some() || new_text.is_some() || replace_all {
            anyhow::bail!(
                "--replace-file cannot be combined with --old-text, --new-text, or --replace-all"
            );
        }
        let content = std::fs::read_to_string(&p).with_context(|| format!("read {p:?}"))?;
        return Ok(PreparedEdit::Replace(content));
    }
    let (Some(old_text), Some(new_text)) = (old_text, new_text) else {
        anyhow::bail!("edit requires either --old-text and --new-text, or --replace-file");
    };
    Ok(PreparedEdit::Edits(vec![
        remote_workspace_protocol::EditSpec {
            old_text,
            new_text,
            replace_all,
        },
    ]))
}

fn resolve_endpoint(cli: &Cli) -> Result<Endpoint> {
    if let Some(ws_name) = &cli.workspace {
        let mut conflicting = Vec::new();
        if cli.host.is_some() {
            conflicting.push("--host");
        }
        if cli.root.is_some() {
            conflicting.push("--root");
        }
        if cli.local {
            conflicting.push("--local");
        }
        if cli.config.is_some() {
            conflicting.push("--config");
        }
        if cli.state_base.is_some() {
            conflicting.push("--state-base");
        }
        if cli.remote_bin.is_some() {
            conflicting.push("--remote-bin");
        }
        if cli.remote_shell.is_some() {
            conflicting.push("--remote-shell");
        }
        if !conflicting.is_empty() {
            anyhow::bail!(
                "--workspace cannot be combined with {}; workspace connection settings come from the fleet config",
                conflicting.join(", ")
            );
        }

        let fleet_path = match &cli.fleet {
            Some(p) => p.clone(),
            None => remote_workspace_client::fleet::default_fleet_path()?,
        };
        let workspaces = remote_workspace_client::fleet::load_fleet(&fleet_path)?;
        let ws = workspaces.get(ws_name).ok_or_else(|| {
            let available = if workspaces.is_empty() {
                "(none)".to_string()
            } else {
                workspaces.keys().cloned().collect::<Vec<_>>().join(", ")
            };
            anyhow!(
                "unknown_workspace: '{ws_name}' is not in {fleet_path:?}; available: {available}"
            )
        })?;
        return Ok(ws.endpoint.clone());
    }

    if cli.fleet.is_some() {
        anyhow::bail!("--fleet requires --workspace (or a `workspace` subcommand)");
    }

    let remote_bin = cli
        .remote_bin
        .clone()
        .unwrap_or_else(|| "remote-workspace-server".into());
    let remote_shell = cli.remote_shell.unwrap_or_default();
    let root = cli
        .root
        .clone()
        .ok_or_else(|| anyhow!("--root is required (or pass -w/--workspace <NAME>)"))?;
    if cli.local {
        Ok(Endpoint::Local {
            server_bin: remote_bin,
            root,
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        })
    } else {
        let host = cli.host.clone().ok_or_else(|| {
            anyhow!("--host is required (or use --local or -w/--workspace <NAME>)")
        })?;
        Ok(Endpoint::Ssh {
            host,
            remote_shell,
            remote_bin,
            root,
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        })
    }
}

async fn async_main_real() -> Result<()> {
    let cli = Cli::parse();

    // Admin commands do not connect to a server; handle them before the
    // connect-based dispatch.
    if let Command::Workspace { cmd } = &cli.command {
        if cli.workspace.is_some() {
            anyhow::bail!("--workspace cannot be used with `workspace` subcommands");
        }
        return handle_workspace(cmd, cli.fleet).await;
    }
    if let Command::Stats { log_dir } = &cli.command {
        if cli.workspace.is_some() {
            anyhow::bail!("--workspace cannot be used with `stats`");
        }
        let dir = match log_dir {
            Some(d) => d.clone(),
            None => default_log_dir()?,
        };
        if !dir.is_dir() {
            anyhow::bail!(
                "no log directory at {}; run remote-workspace-mcp with --log-dir {} to record calls",
                dir.display(),
                dir.display()
            );
        }
        print!(
            "{}",
            remote_workspace_client::stats::collect(&dir)?.render()
        );
        return Ok(());
    }

    let endpoint = resolve_endpoint(&cli)?;

    let log = match &cli.log {
        Some(p) => Some(
            ClientLog::open(p.clone())
                .await
                .context("open client log")?,
        ),
        None => None,
    };

    // Read local file/stdin inputs before connecting: the one-shot server exits
    // after a second without a request, so slow stdin would outlive it.
    let create_content = match &cli.command {
        Command::Create { file, .. } => Some(read_input(file.clone())?),
        _ => None,
    };
    let prepared_edit = match &cli.command {
        Command::Edit {
            old_text,
            new_text,
            replace_all,
            replace_file,
            ..
        } => Some(prepare_edit(
            old_text.clone(),
            new_text.clone(),
            *replace_all,
            replace_file.clone(),
        )?),
        _ => None,
    };

    let transport = ArgvTransport {
        argv: endpoint.one_shot_control_argv(),
        serial: endpoint.is_powershell_ssh(),
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
            start_line,
            max_lines,
            tail_lines,
            tail_bytes,
        } => {
            let r = client
                .read_with(
                    &path,
                    ReadOptions {
                        offset,
                        limit,
                        start_line,
                        max_lines,
                        tail_lines,
                        tail_bytes,
                    },
                )
                .await?;
            print!("{}", r.content);
            if let Some(next_line) = r.next_line {
                eprintln!("\n[truncated: use --start-line {next_line}]");
            } else if let Some(next) = r.next_offset {
                eprintln!("\n[truncated: use --offset {next}]");
            }
        }
        Command::Create { path, .. } => {
            let content = create_content.expect("read before connecting");
            let res = client.create(&path, &content).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Edit {
            path, base_hash, ..
        } => {
            let res = match prepared_edit.expect("prepared before connecting") {
                PreparedEdit::Edits(edits) => client.edit(&path, &base_hash, edits).await?,
                PreparedEdit::Replace(content) => {
                    client.replace(&path, &base_hash, &content).await?
                }
            };
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
        Command::History { limit } => {
            let ops = client.history(limit).await?;
            println!("{}", serde_json::to_string_pretty(&ops)?);
        }
        Command::Op { operation_id } => {
            let d = client.operation_get(&operation_id).await?;
            println!("{}", serde_json::to_string_pretty(&d)?);
        }
        Command::Gc { keep } => {
            let r = client.gc(keep).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        // Handled before the connect path.
        Command::Stats { .. } | Command::Workspace { .. } => unreachable!(),
    }
    Ok(())
}

async fn handle_workspace(cmd: &WorkspaceCmd, fleet: Option<PathBuf>) -> Result<()> {
    match cmd {
        WorkspaceCmd::Add {
            name,
            host,
            root,
            label,
            config,
            state_base,
            remote_bin,
            remote_shell,
        } => {
            workspace_add(WorkspaceAddParams {
                name,
                host,
                root,
                label: label.as_deref(),
                config: config.as_deref(),
                state_base: state_base.as_deref(),
                remote_bin: remote_bin.as_deref(),
                remote_shell: *remote_shell,
                fleet,
            })
            .await
        }
        WorkspaceCmd::List => workspace_list(fleet),
        WorkspaceCmd::Rm { name } => workspace_rm(name, fleet),
        WorkspaceCmd::Upgrade { name } => workspace_upgrade(name.as_deref(), fleet).await,
    }
}

fn workspace_list(fleet: Option<PathBuf>) -> Result<()> {
    use remote_workspace_client::fleet;

    let fleet_path = match fleet {
        Some(path) => path,
        None => fleet::default_fleet_path()?,
    };
    let workspaces = fleet::load_fleet(&fleet_path)?;
    if workspaces.is_empty() {
        println!("No workspaces configured.");
        return Ok(());
    }
    let rows: Vec<(&str, &str, &str, Option<&str>)> = workspaces
        .iter()
        .map(|(name, ws)| {
            let (host, root) = match &ws.endpoint {
                Endpoint::Ssh { host, root, .. } => (host.as_str(), root.as_str()),
                Endpoint::Local { root, .. } => ("(local)", root.as_str()),
            };
            (name.as_str(), host, root, ws.label.as_deref())
        })
        .collect();
    let name_w = rows.iter().map(|(n, ..)| n.len()).max().unwrap_or(0);
    let host_w = rows.iter().map(|(_, h, ..)| h.len()).max().unwrap_or(0);
    for (name, host, root, label) in rows {
        match label {
            Some(l) => println!("{name:<name_w$}  {host:<host_w$}  {root}  {l}"),
            None => println!("{name:<name_w$}  {host:<host_w$}  {root}"),
        }
    }
    Ok(())
}

fn workspace_rm(name: &str, fleet: Option<PathBuf>) -> Result<()> {
    use remote_workspace_client::fleet;

    let fleet_path = match fleet {
        Some(path) => path,
        None => fleet::default_fleet_path()?,
    };
    fleet::remove_workspace_entry(&fleet_path, name)?;
    println!("Removed workspace '{name}'.");
    Ok(())
}

/// Upgrade the managed server behind already-registered workspaces. `add`
/// deliberately refuses a workspace that already exists, so this is the path
/// for picking up a new release. The fleet file is never modified.
async fn workspace_upgrade(name: Option<&str>, fleet: Option<PathBuf>) -> Result<()> {
    use remote_workspace_client::fleet;

    let fleet_path = match fleet {
        Some(path) => path,
        None => fleet::default_fleet_path()?,
    };
    let text = std::fs::read_to_string(&fleet_path)
        .with_context(|| format!("read fleet config {fleet_path:?}"))?;
    let workspaces = fleet::parse_fleet(&text)
        .with_context(|| format!("invalid fleet config {fleet_path:?}"))?;

    let selected: Vec<(&String, &fleet::Workspace)> = match name {
        Some(n) => {
            let ws = workspaces.get(n).ok_or_else(|| {
                anyhow!(
                    "unknown_workspace: '{n}' is not in {fleet_path:?}; available: {}",
                    workspaces.keys().cloned().collect::<Vec<_>>().join(", ")
                )
            })?;
            vec![(workspaces.get_key_value(n).unwrap().0, ws)]
        }
        None => workspaces.iter().collect(),
    };

    // Installation belongs to an SSH identity, not a workspace: several
    // workspaces on one host share a single binary, so each (host, binary) pair
    // is done once. Keyed by host alone, a workspace with a user-managed binary
    // would mark its host done and the managed server there would be skipped.
    let mut done: BTreeSet<(String, String)> = BTreeSet::new();
    let mut failures = 0;
    for (ws_name, ws) in selected {
        let (host, shell, bin) = match &ws.endpoint {
            remote_workspace_client::Endpoint::Ssh {
                host,
                remote_shell,
                remote_bin,
                ..
            } => (host, *remote_shell, remote_bin),
            remote_workspace_client::Endpoint::Local { .. } => {
                println!("{ws_name}: local workspace, nothing to install");
                continue;
            }
        };
        if !done.insert((host.clone(), bin.clone())) {
            println!("{ws_name}: shares {bin} on {host}, already handled");
            continue;
        }
        match upgrade_host(host, shell, bin).await {
            Ok(msg) => println!("{ws_name} [{host}]: {msg}"),
            Err(e) => {
                failures += 1;
                println!("{ws_name} [{host}]: {e}");
            }
        }
    }
    if failures > 0 {
        anyhow::bail!("{failures} workspace(s) failed to upgrade");
    }
    Ok(())
}

/// Upgrade one SSH identity's managed server. A binary at any path other than
/// the managed one is user-managed and is only reported, never replaced.
async fn upgrade_host(host: &str, shell: RemoteShell, bin: &str) -> Result<String> {
    use remote_workspace_client::deploy::{self, ServerStep};

    let platform = deploy::probe_platform(host, shell)?;
    let managed = platform.managed_bin();
    if bin != managed {
        let v = deploy::check_custom_bin(host, shell, bin)?;
        return Ok(format!(
            "user-managed {bin} at {} (not modified)",
            v.software_version
        ));
    }
    match deploy::deploy_managed(host, shell, &platform.os, &platform.arch, &managed)? {
        ServerStep::AlreadyCurrent(v) => Ok(format!("up to date {}", v.software_version)),
        ServerStep::Installed(o) => Ok(match (&o.previous, o.installed) {
            (_, false) => format!("up to date {}", o.current.software_version),
            (None, _) => format!("installed {}", o.current.software_version),
            (Some(prev), _) => format!(
                "upgraded {} -> {}",
                prev.software_version, o.current.software_version
            ),
        }),
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
    remote_shell: Option<RemoteShell>,
    fleet: Option<PathBuf>,
}

async fn workspace_add(p: WorkspaceAddParams<'_>) -> Result<()> {
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
    use remote_workspace_client::deploy::{self, ServerStep};
    use remote_workspace_client::fleet::{self, NewEntry};

    let fleet_path = match &p.fleet {
        Some(path) => path.clone(),
        None => fleet::default_fleet_path()?,
    };

    println!("Adding workspace '{}'", p.name);

    // Reject collisions before touching the remote. This is the fast path; the
    // authoritative re-check happens under the config lock at commit time.
    let preview = NewEntry {
        name: p.name.into(),
        host: p.host.into(),
        remote_shell: p.remote_shell.unwrap_or_default(),
        root: p.root.into(),
        bin: None,
        label: None,
        config: None,
        state_base: None,
    };
    let existing = match std::fs::read_to_string(&fleet_path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(anyhow!("fleet_write_failed: read {fleet_path:?}: {e}")),
    };
    fleet::check_addable(&existing, &preview)?;

    let (remote_shell, platform) = deploy::detect_platform(p.host, p.remote_shell)?;
    println!("  {:<22} connected", "SSH");
    println!("  {:<22} {}", "Remote shell", remote_shell);
    println!("  {:<22} {}", "Remote platform", platform.label());
    // Record the canonical root, so later additions naming the same directory
    // through a symlink or a trailing slash are caught as duplicates.
    let root = deploy::validate_root(p.host, remote_shell, p.root)?;
    println!("  {:<22} valid", "Workspace root");

    // A user-managed binary is only checked; the managed one is installed or
    // upgraded as needed.
    let (bin, protocol) = if let Some(custom) = p.remote_bin {
        let v = deploy::check_custom_bin(p.host, remote_shell, custom)?;
        println!(
            "  {:<22} user-managed {} (not modified)",
            "Server", v.software_version
        );
        (custom.to_string(), v.protocol_version)
    } else {
        let managed = platform.managed_bin();
        match deploy::deploy_managed(p.host, remote_shell, &platform.os, &platform.arch, &managed)?
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
                println!("  {:<22} up to date {}", "Server", v.software_version);
                (managed, v.protocol_version)
            }
        }
    };
    println!("  {:<22} {}", "Protocol", protocol);

    // Prove the workspace really works before it becomes authorized. The error
    // already carries check_workspace's own stable code.
    let endpoint = remote_workspace_client::Endpoint::Ssh {
        host: p.host.into(),
        remote_shell,
        remote_bin: bin.clone(),
        root: root.clone(),
        state_base: p.state_base.map(str::to_string),
        config: p.config.map(str::to_string),
    };
    if let Err(e) = fleet::check_workspace(&endpoint).await {
        return Err(anyhow!(e));
    }
    println!("  {:<22} passed", "Workspace probe");

    let entry = NewEntry {
        name: p.name.into(),
        host: p.host.into(),
        remote_shell,
        root,
        bin: Some(bin),
        label: p.label.map(str::to_string),
        config: p.config.map(str::to_string),
        state_base: p.state_base.map(str::to_string),
    };
    fleet::add_workspace_entry(&fleet_path, &entry)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn workspace_flag_rejects_explicit_connection_flags() {
        for extra in [
            vec!["--host", "h"],
            vec!["--root", "/r"],
            vec!["--local"],
            vec!["--config", "/c.toml"],
            vec!["--state-base", "/s"],
            vec!["--remote-bin", "/bin/srv"],
            vec!["--remote-shell", "posix"],
        ] {
            let mut argv = vec!["remote-workspace", "-w", "ws"];
            argv.extend(extra.iter().copied());
            argv.extend(["ls", "."]);
            let cli = Cli::try_parse_from(&argv).unwrap();
            let err = resolve_endpoint(&cli).unwrap_err().to_string();
            assert!(
                err.contains("--workspace cannot be combined with"),
                "{argv:?} -> {err}"
            );
        }
    }
}
