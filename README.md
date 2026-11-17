# remote-workspace

**Remote workspaces for coding agents, over plain SSH.**

The agent runs on your machine; the code, toolchain, and GPUs stay on the
remote host, which runs one small binary exposing atomic file operations,
bounded command execution, and a durable operation log.

```
coding agent  ->  remote-workspace (CLI) or remote-workspace-mcp (MCP)  ->  ssh stdio  ->  remote-workspace-server  ->  workspace
```

The transport is JSON Lines over an SSH process's stdin/stdout: no daemon, no
open port, no workspace sync. If `ssh <host>` works, this works --
`~/.ssh/config`, ProxyJump, Tailscale, and ControlMaster all apply.

Design rationale and protocol details: [`docs/design.md`](docs/design.md).

## Install

Linux x86_64:

```bash
BASE=https://github.com/hesic73/remote-workspace/releases/latest/download
mkdir -p ~/.local/bin
for b in remote-workspace remote-workspace-mcp; do
  curl -fsSL "$BASE/$b-linux-x86_64-musl" -o ~/.local/bin/$b
  chmod +x ~/.local/bin/$b
done
```

Windows x64 (PowerShell):

```powershell
$releaseBase = "https://github.com/hesic73/remote-workspace/releases/latest/download"
$installDir = "$env:LOCALAPPDATA\remote-workspace\bin"
New-Item -ItemType Directory -Force $installDir | Out-Null
foreach ($name in @("remote-workspace", "remote-workspace-mcp")) {
    Invoke-WebRequest "$releaseBase/$name-windows-x86_64.exe" -OutFile "$installDir\$name.exe"
}
$env:Path = "$installDir;$env:Path"   # this shell only; add it to your user PATH
```

Checksums: `SHA256SUMS` (Linux), `SHA256SUMS-windows-x86_64` (Windows). Or build
from source:

```bash
cargo build --release --locked
# target/release/remote-workspace         client + CLI
# target/release/remote-workspace-server  server (runs on the remote host)
# target/release/remote-workspace-mcp     MCP server for coding agents
```

### Platform support

| Local client / MCP | Remote host | Server installation |
| --- | --- | --- |
| Linux x86_64 or Windows x64 | Linux x86_64, POSIX shell | Automatic (static musl) |
| Windows x64 | Windows, PowerShell as OpenSSH `DefaultShell` | Manual, with `--remote-bin` |

Linux clients cannot connect to PowerShell endpoints. Other POSIX targets need a
server built and installed manually. For a Windows remote, build
`cargo build --release --locked -p remote-workspace-server` from the same release
on that host, place the `.exe` at a persistent path, and from a Windows client:

```powershell
remote-workspace workspace add windows --host user@windows-host --root C:/projects/app --remote-bin C:/tools/remote-workspace-server.exe --remote-shell powershell
```

PowerShell endpoints use one short SSH/server process per request, so requests
to one workspace are serialized and pay SSH startup each time.

## Quick start

Onboard a workspace once. This probes the host, installs or upgrades the
server, runs a real protocol round-trip, and records the workspace:

```bash
remote-workspace workspace add robot --host robot@workstation --root /home/robot/project
```

```text
Adding workspace 'robot'
  SSH                    connected
  Remote shell           posix
  Remote platform        linux-x86_64
  Workspace root         valid
  Server                 installed 0.8.0
  Protocol               5
  Workspace probe        passed
  Fleet configuration    updated
Workspace 'robot' is ready.
```

Run CLI commands against any configured workspace with `-w` / `--workspace`:

```bash
remote-workspace -w robot ls .
remote-workspace -w robot exec -- pytest -q
```

Or connect directly without a fleet entry via `--host` / `--root` (or `--local`
to run the server as a local subprocess).

## Use from a coding agent (MCP)

```bash
claude mcp add --scope user remote-workspace -- remote-workspace-mcp
codex mcp add remote-workspace -- remote-workspace-mcp
grok mcp add --scope user remote-workspace -- remote-workspace-mcp
```

For Antigravity (`~/.gemini/antigravity/mcp_config.json`) or Claude Desktop
(`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "remote-workspace": {
      "command": "/home/you/.local/bin/remote-workspace-mcp"
    }
  }
}
```

One MCP serves the whole fleet. Tools: `list_workspaces`, `list_directory`,
`read_file`, `create_file`, `edit_file`, `delete_file`, `run_command`,
`wait_while`, `upload_file`, `download_file`. Search, Git, builds, and tests all
go through `run_command`; long work runs under tmux or slurm, and `wait_while`
waits for it to be gone. Every tool except `list_workspaces` takes a **required**
`workspace` argument, so a call never lands on a machine by default. Inspecting
history (`history`, `op`) is a CLI job, not an agent tool.

The MCP can be registered before any workspace exists: an empty or missing fleet
is valid, and workspaces added later are picked up on the next call. Agent
conventions live in
[`AGENT_GUIDANCE.md`](crates/remote-workspace-mcp/AGENT_GUIDANCE.md), embedded in
the MCP's initialization instructions.

Diagnose the fleet without starting the MCP:

```bash
remote-workspace-mcp --check
```

It probes every workspace once and exits nonzero if any is unhealthy. Errors
start with a stable code: `connect_failed` (SSH), `probe_failed` (server reached,
round-trip failed), `unknown_workspace`, `fleet_reload_failed`, and, from tool
calls, `server_too_old` / `client_too_old` when the two sides speak different
protocols.

Register with `--log-dir DIR` to record every request and response (one JSONL
file per workspace), then run `remote-workspace stats` for per-tool call and
error counts. It is off by default because logs carry full file contents.

## Configuration

### Fleet

Workspaces live in `~/.remote-workspace/workspaces.toml` (override with
`--fleet`). A workspace is a `(machine, root)` pair.

```toml
[workspaces.robot]
host = "robot@workstation"   # omit to run on the local machine
remote_shell = "posix"       # "powershell" for Windows OpenSSH
root = "/home/robot/project"
bin = "/home/robot/.local/lib/remote-workspace/remote-workspace-server"
label = "ROS workspace"      # optional, shown by list_workspaces
# config / state_base optional, same meaning as the server flags
```

Prefer `workspace add` over hand edits: it validates the target and rewrites the
file atomically, keeping comments and other entries. A running MCP picks up
changes on its next call, keeping connections to unchanged workspaces. A file
that does not parse is never partially applied.

### Execution profiles

Every `exec` spawns a fresh process, so `conda activate` never leaks between
commands. Re-apply environment setup per command with profiles in a TOML file on
the remote host (pass it with `--config`, or the fleet's `config` field):

```toml
default_profile = "robot"

[profiles.robot]
shell = ["bash", "-lc"]
setup = """
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

With no profile, the argv is spawned directly with no shell. Parsing is strict:
unknown fields, an empty `shell`, or an undeclared `default_profile` fail server
startup. On a Windows server the default `shell` is Windows PowerShell; variables
and `Set-Location` from `setup` carry over to the command, functions and aliases
do not.

### Server state

Operation shards, pending upload metadata, and scratch live **outside the
workspace**, on the remote host:

```
~/.remote-workspace/state/<rootname>-<hash>/
```

Nothing shows up in `git status`, and a command inside the workspace cannot
delete the log. `--state-base` relocates it; deleting it is always safe. History
is compacted to `--history-limit` (default 1000) via `gc` (and opportunistically
at startup when shard count grows); scratch files idle for 7 days are swept.

Multiple agent and CLI sessions can share the same workspace root concurrently:
each session appends to its own operation shard under `ops/`, and conflicting
concurrent `edit` calls fail cleanly with `STALE_FILE`. An idle server exits
after `--idle-timeout-secs` (default 900) with nothing arriving or running.
`<state>/server.jsonl` records each session's start and exit.

## CLI

| Command | Purpose |
|---------|---------|
| `workspace add <name> --host H --root R` | Onboard a new workspace: install the server, record it in the fleet |
| `workspace list` | List configured workspaces in the fleet |
| `workspace rm <name>` | Remove a workspace entry from the fleet (alias: `remove`) |
| `workspace upgrade [<name>]` | Install/upgrade the managed server for workspaces in the fleet |
| `ls <path>` | List a directory |
| `stat <path>` | Stat a file or directory |
| `cat <path>` | Read a file (`--offset`/`--limit`, `--start-line`/`--max-lines`, `--tail-lines`/`--tail-bytes`) |
| `create <path> [--file F]` | Create a new file (stdin by default); fails if it exists |
| `edit <path> --base-hash H ...` | Edit an existing file (`--old-text S --new-text S` or `--replace-file F`) |
| `rm <path>` | Delete a file |
| `exec [--cwd D] [--profile P] -- argv...` | Run a command |
| `history [--limit N]` | List recorded operations |
| `op <operation_id>` | Details of one operation |
| `gc [--keep N]` | Compact closed history shards, sweep expired scratch, report scratch usage |
| `stats [--log-dir D]` | Per-tool call and error counts from MCP logs (local only) |

Connection flags: `-w` / `--workspace <name>`, `--fleet <file>`, or explicit
`--host`, `--root`, `--remote-bin`, `--remote-shell`, `--config`,
`--state-base`, `--local`, plus `--log <file>`.

## Upgrades

The client installs only the server release matching **its own** version, so
upgrade in this order:

1. Your own binaries: rerun the [install](#install) commands, then restart the
   MCP host.
2. The remote servers: `remote-workspace workspace upgrade` (or
   `workspace upgrade robot` for one).

`upgrade` installs once per SSH identity, transfers nothing when a host is
current, never downgrades a newer server, and never edits the fleet file. Each
SSH identity holds one managed server at
`~/.local/lib/remote-workspace/remote-workspace-server`. A server given with
`--remote-bin` is user-managed: checked for compatibility, never overwritten.

`remote-workspace-server --version-json` reports `software_version` and
`protocol_version` (bumped only on incompatible changes).

## Development

```
crates/
  remote-workspace-protocol/  # pure serde types: messages, errors, records
  remote-workspace-server/    # workspace, fs ops, exec, operation store (binary)
  remote-workspace-client/    # transport, typed API, deploy, fleet, CLI (binary `remote-workspace`)
  remote-workspace-mcp/       # MCP stdio server on top of the client (binary)
```

```bash
cargo build --workspace --locked                # binaries used by integration tests
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets -- -D warnings
```

CI runs tests, formatting, and Clippy on Linux and Windows, and builds the
release artifacts. Releases are cut by pushing a tag
(`git tag -a vX.Y.Z && git push origin vX.Y.Z`); publishing requires every job to
pass and the tag to match the built version.

The Windows-client SSH smoke test is opt-in. From Windows, set
`REMOTE_WORKSPACE_TEST_HOST`, `REMOTE_WORKSPACE_TEST_ROOT`,
`REMOTE_WORKSPACE_TEST_BIN`, and `REMOTE_WORKSPACE_TEST_SHELL` (`posix` or
`powershell`), then:

```powershell
cargo test -p remote-workspace-client --test windows_remote_smoke -- --ignored
```
