use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use remote_workspace_client::{Client, Endpoint};
use remote_workspace_protocol::ListKind;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;

// Fleet configuration lives in the client crate (shared with the `workspace
// add` CLI); re-exported here so existing callers keep working.
pub use remote_workspace_client::fleet::{check_workspace, parse_fleet, Workspace};

const SERVER_NAME: &str = "remote-workspace-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const AGENT_GUIDANCE: &str = include_str!("../AGENT_GUIDANCE.md");

// ---- Helpers ----

fn ok(text: impl Into<String>) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

fn err(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(text)])
}

/// Serialize a result object with the workspace name injected, so the agent
/// never has to guess which workspace an operation_id or path belongs to.
fn ok_json_in_workspace<T: serde::Serialize>(workspace: &str, value: &T) -> CallToolResult {
    let mut v = match serde_json::to_value(value) {
        Ok(v) => v,
        Err(e) => return err(format!("result serialize error: {e}")),
    };
    if let Some(obj) = v.as_object_mut() {
        obj.insert("workspace".into(), workspace.into());
    }
    match serde_json::to_string_pretty(&v) {
        Ok(text) => ok(text),
        Err(e) => err(format!("result serialize error: {e}")),
    }
}

/// Integer parameters that accept a numeric string as well as a number. The
/// schema advertises `integer`, but some MCP hosts stringify every scalar; the
/// agent then gets a deserialize error it cannot act on, because the schema it
/// was given already said `integer`. Only unambiguous numeric strings are
/// accepted -- anything else still fails.
mod lenient_int {
    use serde::{Deserialize, Deserializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }

    fn parse<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        match Option::<NumOrStr>::deserialize(d)? {
            None => Ok(None),
            Some(NumOrStr::Num(n)) => Ok(Some(n)),
            Some(NumOrStr::Str(s)) => s.trim().parse::<u64>().map(Some).map_err(|_| {
                serde::de::Error::custom(format!("expected an integer, got the string {s:?}"))
            }),
        }
    }

    pub fn opt_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        parse(d)
    }

    pub fn opt_usize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
        Ok(parse(d)?.map(|n| n as usize))
    }
}

// ---- Input structs ----

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDirectoryInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "Directory path in the workspace, or @scratch/...")]
    pub path: String,
    #[serde(default, deserialize_with = "lenient_int::opt_usize")]
    #[schemars(with = "usize", description = "Entry offset to start at (default: 0)")]
    pub offset: Option<usize>,
    #[serde(default, deserialize_with = "lenient_int::opt_usize")]
    #[schemars(
        with = "usize",
        description = "Maximum entries to return (default and maximum: 1000)"
    )]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "File path in the workspace, or @scratch/...")]
    pub path: String,
    #[serde(default, deserialize_with = "lenient_int::opt_u64")]
    #[schemars(
        with = "u64",
        description = "Byte offset to start reading from (default 0)"
    )]
    pub offset: Option<u64>,
    #[serde(default, deserialize_with = "lenient_int::opt_u64")]
    #[schemars(
        with = "u64",
        description = "Maximum bytes to read (default and hard maximum: 65536)"
    )]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateFileInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "File path in the workspace, or @scratch/...")]
    pub path: String,
    #[schemars(description = "Full content of the new file")]
    pub content: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EditInput {
    #[schemars(description = "Exact text to replace, including whitespace")]
    pub old_text: String,
    #[schemars(description = "Replacement text; empty string deletes old_text")]
    pub new_text: String,
    #[serde(default)]
    #[schemars(
        with = "bool",
        description = "Replace every occurrence instead of failing when old_text is not unique (default: false)"
    )]
    pub replace_all: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EditFileInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "File path in the workspace, or @scratch/...")]
    pub path: String,
    #[schemars(description = "Current file hash, as returned by read_file")]
    pub base_hash: String,
    #[schemars(
        description = "Replacements to apply in order, each to the result of the one before it. All succeed or the file is left untouched."
    )]
    pub edits: Vec<EditInput>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteFileInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "File path in the workspace, or @scratch/...")]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandInput {
    #[schemars(description = "Workspace name")]
    pub workspace: String,
    #[schemars(description = "Command and arguments, e.g. [\"pytest\", \"-q\"]")]
    pub argv: Vec<String>,
    #[serde(default)]
    #[schemars(
        with = "String",
        description = "Working directory in the workspace, or @scratch/... (default: root)"
    )]
    pub cwd: Option<String>,
    #[serde(default)]
    #[schemars(
        with = "String",
        description = "Environment profile name (configured server-side)"
    )]
    pub profile: Option<String>,
    #[serde(default, deserialize_with = "lenient_int::opt_u64")]
    #[schemars(
        with = "u64",
        description = "Timeout in milliseconds (default: 300000; maximum: 3600000)"
    )]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadFileInput {
    #[schemars(description = "Destination workspace name")]
    pub workspace: String,
    #[schemars(description = "Absolute or relative path of the local source file")]
    pub local_path: String,
    #[schemars(description = "Destination path in the workspace, or @scratch/...")]
    pub remote_path: String,
    #[serde(default)]
    #[schemars(
        with = "bool",
        description = "Replace an existing destination file (default: false)"
    )]
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DownloadFileInput {
    #[schemars(description = "Source workspace name")]
    pub workspace: String,
    #[schemars(description = "Source path in the workspace, or @scratch/...")]
    pub remote_path: String,
    #[schemars(description = "Absolute or relative path of the local destination file")]
    pub local_path: String,
    #[serde(default)]
    #[schemars(
        with = "bool",
        description = "Replace an existing destination file (default: false)"
    )]
    pub overwrite: Option<bool>,
}

// ---- MCP server ----

/// One configured workspace's connection state: its endpoint plus an
/// independent connection slot, so an unreachable machine's connect retries
/// never block calls to the other workspaces. Shared behind an `Arc` and reused
/// across fleet reloads while its endpoint is unchanged, so an open SSH
/// connection survives edits to unrelated workspaces.
struct WorkspaceHandle {
    endpoint: Endpoint,
    /// Current connection. A Client never recovers once its transport dies
    /// (e.g. sshd resetting the connection), so tool calls fetch it through
    /// `client()`, which reconnects on demand.
    slot: tokio::sync::Mutex<Option<Arc<Client>>>,
}

impl WorkspaceHandle {
    /// What this process last observed about the connection, without touching
    /// the network. Deliberately not a health check: probing every workspace
    /// would open an SSH session and take each server's state lock, which is
    /// itself a way to break a workspace someone else is using. A real call
    /// still reports `connect_failed` / `probe_failed` at the moment it
    /// matters; this only says which workspaces are already warm.
    fn connection(&self) -> &'static str {
        match self.slot.try_lock() {
            // Held means a call is connecting or in flight right now.
            Err(_) => "in_use",
            Ok(slot) => match slot.as_deref() {
                None => "not_connected",
                Some(c) if c.is_closed() => "disconnected",
                Some(_) => "connected",
            },
        }
    }
}

/// A workspace in the current fleet snapshot: its connection handle plus the
/// display label (which, unlike the endpoint, can change without dropping the
/// connection).
struct Entry {
    handle: Arc<WorkspaceHandle>,
    label: Option<String>,
}

type Snapshot = BTreeMap<String, Entry>;

/// File change stamp used to skip re-parsing an unchanged fleet file.
#[derive(Clone, PartialEq, Eq)]
struct FleetStamp {
    modified: std::time::SystemTime,
    len: u64,
}

pub struct RemoteWorkspaceServer {
    fleet_path: std::path::PathBuf,
    /// When set, every connection writes its request/response log to
    /// `<dir>/<workspace>.jsonl`. This is the only record of the read-only
    /// calls (`list_directory`, `read_file`), which the server's operation log
    /// deliberately does not record; `remote-workspace stats` reads it.
    log_dir: Option<std::path::PathBuf>,
    /// Current fleet, swapped atomically when the fleet file changes on disk.
    /// Reads clone the `Arc` and never block on I/O.
    snapshot: std::sync::RwLock<Arc<Snapshot>>,
    /// Serializes reload attempts and remembers the last-seen file stamp
    /// (`None` = the file was absent or unreadable when last checked).
    last_seen: std::sync::Mutex<Option<FleetStamp>>,
}

const CONNECT_ATTEMPTS: u32 = 4;
const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

fn stamp_of(path: &std::path::Path) -> Option<FleetStamp> {
    let meta = std::fs::metadata(path).ok()?;
    Some(FleetStamp {
        modified: meta.modified().ok()?,
        len: meta.len(),
    })
}

/// Build a new snapshot from a parsed fleet, reusing the previous handle (and
/// its live connection) for any workspace whose endpoint is unchanged.
fn build_snapshot(old: &Snapshot, fleet: BTreeMap<String, Workspace>) -> Snapshot {
    fleet
        .into_iter()
        .map(|(name, ws)| {
            let handle = match old.get(&name) {
                Some(e) if e.handle.endpoint == ws.endpoint => e.handle.clone(),
                _ => Arc::new(WorkspaceHandle {
                    endpoint: ws.endpoint,
                    slot: tokio::sync::Mutex::new(None),
                }),
            };
            (
                name,
                Entry {
                    handle,
                    label: ws.label,
                },
            )
        })
        .collect()
}

impl RemoteWorkspaceServer {
    /// Reads and validates the fleet file itself, so the change stamp is taken
    /// *before* the read it describes. Stamping after would miss a write landing
    /// in between and serve a stale snapshot that looks current.
    pub fn load(fleet_path: std::path::PathBuf) -> anyhow::Result<Self> {
        let stamp = stamp_of(&fleet_path);
        let text = std::fs::read_to_string(&fleet_path)
            .with_context(|| format!("read fleet config {fleet_path:?}"))?;
        let fleet =
            parse_fleet(&text).with_context(|| format!("invalid fleet config {fleet_path:?}"))?;
        Ok(Self {
            fleet_path,
            log_dir: None,
            snapshot: std::sync::RwLock::new(Arc::new(build_snapshot(&BTreeMap::new(), fleet))),
            last_seen: std::sync::Mutex::new(stamp),
        })
    }

    pub fn with_log_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.log_dir = Some(dir);
        self
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.read().unwrap().clone()
    }

    /// Reload the fleet if the file changed since last checked; a no-op when the
    /// stamp is unchanged. A file that is absent, unreadable, or invalid is
    /// never partially applied: the last known-good snapshot keeps serving and
    /// the triggering operation gets `fleet_reload_failed`. The bad state is not
    /// recorded as seen, so the failure keeps being reported (rather than
    /// silently serving a stale fleet) until the file is valid again.
    fn refresh_fleet_if_changed(&self) -> Result<(), String> {
        let mut last = self.last_seen.lock().unwrap();
        let now = stamp_of(&self.fleet_path);
        if now == *last {
            return Ok(());
        }
        let text = std::fs::read_to_string(&self.fleet_path)
            .map_err(|e| format!("fleet_reload_failed: read fleet config: {e}"))?;
        let fleet = parse_fleet(&text).map_err(|e| format!("fleet_reload_failed: {e}"))?;
        let old = self.snapshot();
        *self.snapshot.write().unwrap() = Arc::new(build_snapshot(&old, fleet));
        *last = now;
        Ok(())
    }

    fn handle(
        &self,
        snapshot: &Arc<Snapshot>,
        workspace: &str,
    ) -> Result<Arc<WorkspaceHandle>, String> {
        snapshot.get(workspace).map(|e| e.handle.clone()).ok_or_else(|| {
            let names = snapshot
                .keys()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown_workspace: '{workspace}' is not a configured workspace; available workspaces: {names}")
        })
    }

    /// Returns a live client for the workspace, (re)connecting with retries if
    /// there is none or the previous connection died. A fresh connection is
    /// probed with a real round-trip, because a transport can spawn fine and
    /// die immediately (e.g. sshd resetting rapid successive connections).
    /// Reloads the fleet first, so a workspace added by `workspace add` becomes
    /// reachable without restarting the MCP process.
    async fn client(&self, workspace: &str) -> Result<(Arc<Client>, Arc<WorkspaceHandle>), String> {
        self.refresh_fleet_if_changed()?;
        let snapshot = self.snapshot();
        let handle = self.handle(&snapshot, workspace)?;
        let mut slot = handle.slot.lock().await;
        if let Some(c) = slot.as_ref() {
            if !c.is_closed() {
                return Ok((c.clone(), handle.clone()));
            }
        }
        // `code` is a stable keyword so agents can tell transport failures
        // (connect_failed) apart from a reachable-but-unhealthy workspace
        // (probe_failed: server spawned, the round-trip did not survive).
        let log = match &self.log_dir {
            None => None,
            Some(dir) => {
                let path = dir.join(format!("{workspace}.jsonl"));
                match remote_workspace_client::ClientLog::open(path.clone()).await {
                    Ok(l) => Some(l),
                    Err(e) => return Err(format!("log_open_failed: {}: {e}", path.display())),
                }
            }
        };
        let mut code = "connect_failed";
        let mut last = String::new();
        for attempt in 1..=CONNECT_ATTEMPTS {
            if attempt > 1 {
                tokio::time::sleep(CONNECT_BACKOFF).await;
            }
            let transport = remote_workspace_client::ArgvTransport {
                argv: handle.endpoint.control_argv(),
            };
            match Client::connect(transport, log.clone()).await {
                Ok(c) => match c.stat(".").await {
                    Ok(_) => {
                        let c = Arc::new(c);
                        *slot = Some(c.clone());
                        return Ok((c, handle.clone()));
                    }
                    Err(e) => {
                        code = "probe_failed";
                        last = format!("attempt {attempt}: {e}");
                    }
                },
                Err(e) => {
                    code = "connect_failed";
                    last = format!("attempt {attempt}: {e}");
                }
            }
        }
        Err(format!(
            "{code}: cannot reach workspace '{workspace}' after {CONNECT_ATTEMPTS} attempts ({last})"
        ))
    }
}

#[tool_router]
impl RemoteWorkspaceServer {
    #[tool(description = "List the configured workspaces: name, host, and root directory.")]
    async fn list_workspaces(&self) -> CallToolResult {
        if let Err(e) = self.refresh_fleet_if_changed() {
            return err(e);
        }
        let snapshot = self.snapshot();
        let rows: Vec<serde_json::Value> = snapshot
            .iter()
            .map(|(name, entry)| {
                let (host, root) = match &entry.handle.endpoint {
                    Endpoint::Ssh { host, root, .. } => (host.as_str(), root.as_str()),
                    Endpoint::Local { root, .. } => ("(local)", root.as_str()),
                };
                let mut row = serde_json::json!({
                    "name": name,
                    "host": host,
                    "root": root,
                    "connection": entry.handle.connection(),
                });
                if let Some(label) = &entry.label {
                    row["label"] = serde_json::Value::String(label.clone());
                }
                row
            })
            .collect();
        match serde_json::to_string_pretty(&rows) {
            Ok(text) => ok(text),
            Err(e) => err(format!("serialize error: {e}")),
        }
    }

    #[tool(description = "List the contents of a directory in a workspace.")]
    async fn list_directory(
        &self,
        Parameters(ListDirectoryInput {
            workspace,
            path,
            offset,
            limit,
        }): Parameters<ListDirectoryInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.list(&path, offset, limit).await {
            Ok(result) => {
                if result.entries.is_empty() {
                    return ok("(empty directory)");
                }
                let mut out = result
                    .entries
                    .iter()
                    .map(|e| match e.kind {
                        ListKind::Dir => format!("  {}/", e.name),
                        ListKind::File => match e.size {
                            Some(s) => format!("  {} ({} bytes)", e.name, s),
                            None => format!("  {}", e.name),
                        },
                        ListKind::Symlink => format!("  {} ->", e.name),
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if let Some(next) = result.next_offset {
                    out.push_str(&format!("\n[more entries: use offset={next}]"));
                }
                ok(out)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Read a text file, paging with the returned next offset. Also returns the hash to pass to edit_file as base_hash, omitted for files too large to edit. Refuses non-UTF-8 files."
    )]
    async fn read_file(
        &self,
        Parameters(ReadFileInput {
            workspace,
            path,
            offset,
            limit,
        }): Parameters<ReadFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.read(&path, offset, limit).await {
            Ok(r) => {
                let mut out = r.content;
                if let Some(next) = r.next_offset {
                    out.push_str(&format!(
                        "\n\n[output truncated; use offset={next} to read more]"
                    ));
                }
                if let Some(hash) = &r.hash {
                    out.push_str(&format!("\n\n[hash: {hash}]"));
                }
                ok(out)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Create a new text file atomically. Fails with ALREADY_EXISTS if the path exists; modify existing files with edit_file."
    )]
    async fn create_file(
        &self,
        Parameters(CreateFileInput {
            workspace,
            path,
            content,
        }): Parameters<CreateFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.create(&path, &content).await {
            Ok(w) => ok(format!(
                "Created {path} in workspace '{workspace}'. operation_id={}, new_hash={}",
                w.operation_id, w.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Modify an existing text file by exact text replacement, atomically. Pass one or more edits: they are applied in order, each to the result of the one before it, and either all of them land or the file is left untouched. Each old_text must match the content it is applied to exactly: zero occurrences fail with NO_MATCH, several with AMBIGUOUS_MATCH unless replace_all is set. One base_hash covers the whole call; a stale one fails with STALE_FILE, so re-read and retry."
    )]
    async fn edit_file(
        &self,
        Parameters(EditFileInput {
            workspace,
            path,
            base_hash,
            edits,
        }): Parameters<EditFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        let count = edits.len();
        let edits = edits
            .into_iter()
            .map(|e| remote_workspace_client::EditSpec {
                old_text: e.old_text,
                new_text: e.new_text,
                replace_all: e.replace_all.unwrap_or(false),
            })
            .collect();
        match client.edit(&path, &base_hash, edits).await {
            Ok(w) => ok(format!(
                "Edited {path} in workspace '{workspace}' ({count} replacement{}). operation_id={}, new_hash={}",
                if count == 1 { "" } else { "s" },
                w.operation_id,
                w.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Delete a file. Permanent: nothing here restores it.")]
    async fn delete_file(
        &self,
        Parameters(DeleteFileInput { workspace, path }): Parameters<DeleteFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.delete(&path).await {
            Ok(w) => ok(format!(
                "Deleted {path} in workspace '{workspace}'. operation_id={}",
                w.operation_id
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Run a command synchronously. Returns termination, duration, and a bounded preview of each output stream (first 4 KiB, last 12 KiB)."
    )]
    async fn run_command(
        &self,
        Parameters(RunCommandInput {
            workspace,
            argv,
            cwd,
            profile,
            timeout_ms,
        }): Parameters<RunCommandInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.exec(argv, cwd, profile, timeout_ms).await {
            Ok(result) => ok_json_in_workspace(&workspace, &result),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Upload one local file to a workspace as raw streamed bytes; the content never enters the model context. Use this, not create_file, for binary or large files. A long-running call is normal for a big file."
    )]
    async fn upload_file(
        &self,
        Parameters(UploadFileInput {
            workspace,
            local_path,
            remote_path,
            overwrite,
        }): Parameters<UploadFileInput>,
    ) -> CallToolResult {
        let (client, handle) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match remote_workspace_client::upload_file(
            &client,
            &handle.endpoint,
            std::path::Path::new(&local_path),
            &remote_path,
            overwrite.unwrap_or(false),
        )
        .await
        {
            Ok(r) => ok_json_in_workspace(&workspace, &r),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Download one file from a workspace to the local machine as raw streamed bytes; the content never enters the model context. Use this, not read_file, for binary or large files. A long-running call is normal for a big file."
    )]
    async fn download_file(
        &self,
        Parameters(DownloadFileInput {
            workspace,
            remote_path,
            local_path,
            overwrite,
        }): Parameters<DownloadFileInput>,
    ) -> CallToolResult {
        let (client, handle) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match remote_workspace_client::download_file(
            &client,
            &handle.endpoint,
            &remote_path,
            std::path::Path::new(&local_path),
            overwrite.unwrap_or(false),
        )
        .await
        {
            Ok(mut r) => {
                // For a download the useful path is the local destination; the
                // server-side record keeps the remote logical path.
                r.path = local_path;
                ok_json_in_workspace(&workspace, &r)
            }
            Err(e) => err(format!("{e}")),
        }
    }
}

#[tool_handler]
impl ServerHandler for RemoteWorkspaceServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.server_info.name = SERVER_NAME.into();
        info.server_info.version = SERVER_VERSION.into();
        info.instructions = Some(AGENT_GUIDANCE.into());
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}
