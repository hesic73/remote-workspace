# agent-remote

**Remote workspaces for coding agents, over plain SSH.**

The agent runs on your machine; the code, toolchain, and GPUs stay on the
remote host. Instead of installing an agent on every server, the remote side
runs one small binary exposing atomic file operations, bounded command
execution, undo, and a durable operation log.

```
coding agent  ->  agent-remote (CLI) or agent-remote-mcp (MCP)  ->  ssh stdio  ->  agent-remote-server  ->  workspace
```

The transport is JSON Lines over an SSH process's stdin/stdout: no daemon, no
open port, no public IP, and no workspace sync. If `ssh <host>` works, this
works -- `~/.ssh/config`, ProxyJump, Tailscale, SSH agent, and ControlMaster
all apply unchanged.

Design rationale and protocol details: [`docs/design.md`](docs/design.md).

## Install

Download the server artifact from a
[release](https://github.com/hesic73/agent-remote/releases) or build locally:

```bash
cargo build --release
# target/release/agent-remote         client + CLI
# target/release/agent-remote-server  server (runs on the remote host)
# target/release/agent-remote-mcp     MCP server for coding agents
```

You do not need to copy the server to the remote host yourself -- `workspace
add` does that. Released servers are static musl binaries, so they run on any
Linux of the same architecture regardless of glibc version.

## Quick start

Onboard a workspace once. This probes the host, installs or upgrades the
server, runs a real protocol round-trip, and records the workspace:

```bash
agent-remote workspace add robot --host robot@workstation --root /home/robot/project
```

```text
Adding workspace 'robot'
  SSH                    connected
  Remote platform        linux-x86_64
  Workspace root         valid
  Server                 installed 0.2.0
  Protocol               1
  Workspace probe        passed
  Fleet configuration    updated
Workspace 'robot' is ready.
```

Then use it, from an agent through MCP or directly from the CLI:

```bash
agent-remote --host robot@workstation --root /home/robot/project ls .
agent-remote --host robot@workstation --root /home/robot/project exec -- pytest -q
```

`--local` runs the server as a subprocess instead of over SSH, which is handy
for trying things out on one machine.

## Use from a coding agent (MCP)

```bash
claude mcp add agent-remote -- agent-remote-mcp     # one entry serves every workspace
```

`agent-remote-mcp` multiplexes the whole fleet over stdio. Tools:
`list_workspaces`, `list_directory`, `read_file`, `create_file`, `edit_file`,
`delete_file`, `run_command`, `upload_file`, `download_file`, `undo`,
`history`, `operation_get`, `request_status`.

There is exactly one canonical tool per intent -- search, file discovery, Git,
builds, and tests all go through `run_command`, with no wrapper tools. Every
tool except `list_workspaces` takes a **required** `workspace` argument, so a
call can never land on the wrong machine because a default filled itself in.

Conventions for the agent itself live in one canonical place,
[`AGENT_GUIDANCE.md`](crates/agent-remote-mcp/AGENT_GUIDANCE.md), which the MCP
server embeds verbatim in its initialization instructions.

Diagnose the fleet without starting the MCP:

```bash
agent-remote-mcp --check
```

It validates the config and probes every workspace once, printing per-workspace
status and exiting nonzero if anything is unhealthy. Connection-class errors
carry stable codes (`unknown_workspace`, `connect_failed`, `probe_failed`) so a
failure says which layer broke.

## Configuration

### Fleet

Workspaces live in `~/.agent-remote/workspaces.toml` (override with
`--fleet`). A workspace is a `(machine, root)` pair; two roots on one machine
and one root each on two machines are the same concept.

```toml
[workspaces.robot]
host = "robot@workstation"   # omit to run on the local machine
root = "/home/robot/project"
bin = "/home/robot/.local/lib/agent-remote/agent-remote-server"
label = "ROS workspace"      # optional, shown by list_workspaces
# config / state_base optional, same meaning as the server flags
```

Prefer `workspace add` over editing this by hand: it validates the target and
rewrites the file atomically, preserving your comments and other entries. A
running MCP picks up changes on its next call -- no restart, and workspaces
whose entry did not change keep their open connections. A file that does not
parse is never partially applied; calls report `fleet_reload_failed` until it
is valid again.

### Execution profiles

Every `exec` spawns a fresh process, so `conda activate` never leaks between
commands. Re-apply environment setup per command with server-side profiles, in
a TOML file on the remote host (point the server at it with `--config`, or the
fleet's `config` field):

```toml
default_profile = "user-zsh"

[profiles.user-zsh]
shell = ["zsh", "-lic"]      # reuse the user's real login environment
setup = ""

[profiles.robot]
setup = """
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

A profile decides only which shell to start and what to run before the command.
With no profile at all, the argv is spawned directly with no shell. Parsing is
strict: unknown fields, an empty `shell`, or a `default_profile` naming no
declared profile fail server startup rather than silently running commands in
the wrong environment.

### Server state

Server state (history, undo blobs, idempotency table, scratch) lives **outside
the workspace**, on the remote host, keyed by canonical root path:

```
~/.agent-remote/state/<rootname>-<hash>/
```

So the workspace has no dotdir, nothing shows up in `git status`, and a
destructive command inside the workspace cannot take the undo data with it.
`--state-base /data/$USER` relocates the base when home is tight; deleting the
state directory is always safe. Growth is bounded by `--history-limit`
(default 1000) and `gc`.

## CLI

| Command | Purpose |
|---------|---------|
| `workspace add <name> --host H --root R` | Onboard a workspace: install/upgrade the server, record it in the fleet |
| `ls <path>` | List a directory |
| `stat <path>` | Stat a file or directory |
| `cat <path>` | Read a file |
| `create <path> [--file F]` | Create a new file; fails if it exists |
| `edit <path> --base-hash H --old-text S --new-text S` | Exact text replacement in an existing file |
| `rm <path>` | Delete a file |
| `exec [--cwd D] [--profile P] -- argv...` | Run a command |
| `undo <operation_id>` | Undo a recorded file change |
| `history [--limit N]` | List recorded operations |
| `op <operation_id>` | Details of one operation |
| `status <request_id>` | Status of a previously-issued request |
| `gc [--keep N]` | Prune stored history |

Connection flags: `--host`, `--root`, `--remote-bin`, `--config`,
`--state-base`, `--local`, `--log <file>`. `workspace add` is an
administrative command with its own `--host`/`--root` and does not use them.

## Upgrades

`agent-remote-server --version-json` reports two fields: `software_version`
(the release) and `protocol_version` (bumped only on incompatible changes).
A newer client upgrades an older server; an older client never downgrades a
newer one. Re-run `workspace add` to upgrade a host; an already-current host
reports `up to date` and transfers nothing.

Each SSH identity holds one managed server at
`~/.local/lib/agent-remote/agent-remote-server`, shared by every workspace on
that host. Passing `--remote-bin` marks a server user-managed: checked for
compatibility, never installed or overwritten. Releases are cut by pushing a
tag (`git tag -a vX.Y.Z && git push origin vX.Y.Z`), the only event that runs
CI: it builds the static artifacts and publishes them with a manifest and
checksums.

## Development

```
crates/
  agent-remote-protocol/  # pure serde types: messages, errors, records
  agent-remote-server/    # workspace, fs ops, exec, operation store (binary)
  agent-remote-client/    # transport, typed API, deploy, fleet, CLI (binary `agent-remote`)
  agent-remote-mcp/       # MCP stdio server on top of the client (binary)
```

```bash
cargo test --workspace --all-targets   # includes end-to-end tests against the
                                       # real server and MCP binaries
cargo clippy --workspace --all-targets -- -D warnings
```
