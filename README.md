# agent-remote

A lightweight remote-workspace protocol for coding agents. The agent runs
locally; code and the execution environment live on a remote host running
`agent-remote-server`. The client talks to it over plain SSH stdio -- no
daemon, port, or public IP.

```
coding agent  ->  agent-remote (CLI) or agent-remote-mcp (MCP)  ->  ssh stdio  ->  agent-remote-server  ->  workspace
```

The transport is JSON Lines over the SSH process's stdin/stdout, so
`~/.ssh/config`, ProxyJump, Tailscale, SSH agent, and ControlMaster all work
unchanged. See `DESIGN.md` for the rationale and protocol details.

## Build

```bash
cargo build --release
# produces:
#   target/release/agent-remote        (client + CLI)
#   target/release/agent-remote-server (server)
#   target/release/agent-remote-mcp    (MCP server for coding agents)
```

You do not normally copy the server yourself: `agent-remote workspace add`
installs it (see [Adding a workspace](#adding-a-workspace)). Manual placement
is still supported — anywhere on the remote `PATH`, or point at its full path
(the CLI's `--remote-bin` flag; the fleet config's `bin` field for the MCP).

If the remote's glibc is older than your build machine's, build the server as
a fully static musl binary instead — it runs anywhere on the same
architecture. This is what CI publishes, so released artifacts have no glibc
constraint:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl -p agent-remote-server
# -> target/x86_64-unknown-linux-musl/release/agent-remote-server
```

## Adding a workspace

One command onboards a remote workspace: it probes the host, installs or
upgrades the server binary, runs a real protocol round-trip, and records the
workspace in the fleet config.

```bash
agent-remote workspace add robot \
  --host robot@workstation \
  --root /home/robot/project
```

```text
Adding workspace 'robot'
  SSH                    connected
  Remote platform        linux-x86_64
  Workspace root         valid
  Server                 upgraded 0.1.0 -> 0.2.0
  Protocol               1
  Workspace probe        passed
  Fleet configuration    updated
Workspace 'robot' is ready.
```

Optional flags mirror the fleet fields: `--label`, `--config`, `--state-base`,
and `--fleet` (which file to update). A running MCP server picks the new
workspace up on its next call — no restart.

`workspace add` is deliberately a **local CLI operation only**, never an MCP
tool: it expands the set of machines an agent may reach, so it stays on the
trusted side of the boundary. The agent sees `list_workspaces` and can use
what is already configured; it cannot add hosts.

### Managed vs user-managed servers

Omitting `--remote-bin` selects the **managed** binary: one active server per
SSH identity at `~/.local/lib/agent-remote/agent-remote-server`, shared by
every workspace on that host. The client downloads the artifact matching its
own release from GitHub Releases, verifies its SHA-256 locally, uploads it to
a temporary path, and lets the uploaded binary install itself under a remote
`flock` — replacing the old one only if strictly older, then atomically
renaming into place. Running sessions keep their old inode and are unaffected.

Passing `--remote-bin /path/to/server` marks the server **user-managed**: it
is checked for compatibility but never installed, upgraded, or overwritten.
Use this for development builds.

Two version fields drive the decision. `software_version` is the release
(`0.2.0`), `protocol_version` an integer bumped only on incompatible wire or
state changes. A newer client upgrades an older server; an older client never
downgrades a newer one; a server with a newer protocol is refused with a clear
"update your client" error and left untouched. Both are reported by:

```bash
agent-remote-server --version-json
# {"software_version":"0.2.0","protocol_version":1}
```

Air-gapped or testing setups can point the client at a different artifact
source (an `https://` base, a local directory, or a `file://` URL):

```bash
AGENT_REMOTE_RELEASE_BASE=/path/to/dist agent-remote workspace add ...
```

## Quick start

Local mode (`--local`) runs the server as a subprocess -- handy for a single
host or for trying things out:

```bash
WS=$(mktemp -d)
SRV=$(pwd)/target/release/agent-remote-server

agent-remote --local --remote-bin "$SRV" --root "$WS" create hello.txt <<< "hi there"
agent-remote --local --remote-bin "$SRV" --root "$WS" cat hello.txt
agent-remote --local --remote-bin "$SRV" --root "$WS" exec -- make test
```

Over SSH:

```bash
scp target/release/agent-remote-server robot@workstation:~/.local/bin/

agent-remote --host robot@workstation --root /home/robot/project ls .
agent-remote --host robot@workstation --root /home/robot/project exec -- pytest -q
```

Environment profiles (conda, ROS, ...) live in a TOML file on the remote side
and are re-applied for every `exec`, so commands stay stateless:

```toml
# on the remote host; point the server at it with --config
default_profile = "user-zsh"      # applied when a request names no profile

[profiles.user-zsh]
shell = ["zsh", "-lic"]           # load the user's real shell environment
setup = ""

[profiles.robot]
setup = """
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""

[profiles.raw]
setup = ""                        # escape hatch from default_profile
```

Without a profile (and with no `default_profile`), the argv is spawned
directly -- no shell involved. With a profile, the command runs through that
profile's `shell` (default `["bash", "-c"]`): the server appends a script of
the profile's `setup` followed by `exec <argv>` as the shell's final
argument, so the profile picks the environment while signals still reach the
real command. `shell = ["zsh", "-lic"]` loads your actual `.zprofile`/
`.zshrc` (they must behave without a tty), which is usually all a "get my
conda/proxy/PATH right" profile needs. Config parsing is strict: unknown
fields, an empty `shell`, or a `default_profile` naming no declared profile
all fail server startup instead of silently running commands in the wrong
environment.

## CLI subcommands

| Command | Purpose |
|---------|---------|
| `ls <path> [--offset N] [--limit N]` | List a directory |
| `stat <path>` | Stat a file or directory |
| `cat <path> [--offset N] [--limit N]` | Read a file |
| `create <path> [--file F]` | Create a new file (content from file or stdin); fails if it exists |
| `edit <path> --base-hash H --old-text S --new-text S [--replace-all]` | Exact text replacement in an existing file |
| `rm <path>` | Delete a file |
| `exec [--cwd DIR] [--profile P] [--timeout-ms N] -- argv...` | Run a command |
| `undo <operation_id>` | Undo a recorded file change |
| `history [--limit N]` | List recorded operations |
| `op <operation_id>` | Details of one operation |
| `status <request_id>` | Status of a previously-issued request |
| `gc [--keep N]` | Prune stored history down to the newest N operations |
| `workspace add <name> --host H --root R` | Onboard a workspace: install/upgrade the server and record it in the fleet |

Shared flags: `--host`, `--remote-bin` (default `agent-remote-server`),
`--root`, `--config`, `--local`, `--log <file>` (client interaction log),
`--state-base` (relocate server state, see below). `workspace add` takes its
own `--host`/`--root` and does not connect through these.

## Server state

The server keeps its history, undo blobs, and idempotency table **outside the
workspace**, on the remote host:

```
~/.agent-remote/state/<rootname>-<hash>/   # keyed by canonical root path
  operations.jsonl  requests.jsonl  blobs/  lock  op-counter
```

The workspace itself is never touched (no dotdir, nothing for `git status`),
and a destructive command inside the workspace cannot take the undo data with
it. To relocate state -- e.g. when home is nearly full -- pass
`--state-base /data/$USER`: the base changes, the automatic per-workspace
keying stays.

Growth is bounded: at startup the server keeps only the newest
`--history-limit` operations (default 1000; 0 disables) and drops older
records, their blobs, and stale request entries. `gc --keep N` does the same
on demand; deleting the state directory itself is always safe. Undoing a
pruned operation returns `OPERATION_NOT_FOUND`, and pruned operation ids are
never reused.

One server per workspace root: the state directory holds an exclusive lock,
so a second concurrent session fails fast instead of corrupting the logs
(reconnects get a short grace period while the predecessor exits).

### Editing model

One intent, one operation: new text files are made with `create` (refuses an
existing path with `ALREADY_EXISTS`), and existing text files are changed only
with `edit`. `edit` is exact text replacement: `old_text` must match the
current content exactly; zero occurrences fail with `NO_MATCH`, several with
`AMBIGUOUS_MATCH` unless `replace_all` is set. An empty `new_text` deletes the
matched text; a full-file rewrite is expressed by passing the whole current
content as `old_text`. Inputs, the edited file, and the result are bounded at
4 MiB -- larger or binary files go through the transfer tools. A failed edit
leaves the file byte-for-byte untouched.

## MCP server (use from a coding agent)

Agent-facing conventions have a single canonical source in
[`AGENT_GUIDANCE.md`](AGENT_GUIDANCE.md). The MCP server embeds that file
verbatim in its initialization instructions.

`agent-remote-mcp` exposes the same operations as MCP tools over stdio. It
serves a **fleet** of named workspaces declared in
`~/.agent-remote/workspaces.toml` (override with `--fleet <path>`); each
workspace is a `(machine, root)` pair, on any mix of SSH hosts and the local
machine:

```toml
# ~/.agent-remote/workspaces.toml
[workspaces.robot]
host = "robot@workstation"        # omit host to run on the local machine
root = "/home/robot/project"
bin  = "/home/robot/.local/bin/agent-remote-server"  # optional, default: on PATH
label = "ROS workspace"           # optional, shown by list_workspaces
# config / state_base optional, same meaning as the server flags

[workspaces.lab-gpu]
host = "lab-gpu-1"
root = "/data/experiments"
```

Write these entries with `agent-remote workspace add` rather than by hand; it
validates the target, installs the server, and updates the file atomically
while preserving your comments and other entries.

The fleet file is reloadable configuration: before listing workspaces or
resolving one for a tool call, the MCP re-reads it if it changed on disk, so a
workspace added mid-session is visible immediately without restarting the MCP
host. Unchanged workspaces keep their open connections across a reload. An
invalid edit is never partially applied: the last known-good fleet keeps
serving and the triggering call returns `fleet_reload_failed`. Administrative
operations are deliberately absent from the tool surface -- there is no
`add_workspace`, `install_server`, or reload tool.

`agent-remote-mcp --check` diagnoses the fleet without starting the MCP:
it validates the config, probes every workspace once (spawns its server,
one real round-trip), prints per-workspace status, and exits nonzero if
anything is unhealthy. Connection-class tool errors carry stable codes --
`unknown_workspace`, `connect_failed` (transport/spawn), `probe_failed`
(server spawned but the round-trip died, e.g. missing root or a locked
state directory) -- so a failing call tells you which layer broke.

```bash
# Claude Code example: one MCP entry serves every workspace
claude mcp add agent-remote -- agent-remote-mcp
```

Tools: `list_workspaces`, `list_directory`, `read_file`, `create_file`,
`edit_file`, `delete_file`, `run_command`, `upload_file`, `download_file`,
`undo`, `history`, `operation_get`, `request_status`. Each common intent has
exactly one canonical tool; search, file discovery, Git, builds, and tests all
go through `run_command`. Every tool except
`list_workspaces` takes a required `workspace` argument naming which
workspace to act on. Workspaces are fully isolated: state, operation IDs,
history, and undo are scoped per workspace (server-side, keyed by root), and
connections are opened per workspace on demand -- an unreachable machine
fails only its own calls. Two entries must not address the same host and
root (they would contend for the same server state lock; the config is
rejected at startup).

`upload_file` / `download_file` move single regular files between the local
machine and the remote workspace (or `@scratch/...`). They are synchronous and
stream raw bytes over a dedicated SSH process with a fixed-size buffer -- file
content never passes through the JSONL protocol or the model context, and
memory use does not grow with file size. Both ends compute SHA-256 while
streaming. For uploads, the receiving process reports the remote size and
hash and the client verifies them against the local file before committing;
the commit step itself re-checks the staged size but does not rehash the
staging file (single-pass by design -- the client-side hash comparison is the
integrity check, under the same-user trust model). Downloads verify the
sender's declared size and hash locally before installing. Installation is
atomic (rename/link on the remote, temp-file rename locally). Existing
destinations are refused unless `overwrite=true`; parent directories are
never created implicitly. The result and the operation log carry metadata
only: direction, remote path, size, `sha256:...`, duration. Transfers cannot
be undone, and there is no resume -- a failed or disconnected transfer leaves
the destination absent (or untouched), and you simply call the tool again.

A hard-killed upload can leave its staging file
(`.agent-remote-upload.<name>.<random>.part`) in the target directory. These
are swept conservatively: only files matching that exact naming convention,
older than 24 hours, and not part of an in-flight upload are deleted -- on the
next upload into the same directory and during `gc` (which walks the workspace
and scratch roots and reports the count as `removed_stale_staging`).

Tool failures come back as MCP `isError` results. `run_command` is synchronous
and returns a server-bounded preview for each stream: the first 4 KiB and last
12 KiB, together with byte counts and termination details. Every invocation
reaches a terminal response within a bounded period: after the command exits,
output collection waits a short grace period for the pipes to close, then
kills any descendants still holding them and sets `drain_timed_out` in the
result (detached workloads belong in a remote supervisor such as tmux, with
pipes redirected away). Full logs belong in
the server-managed `@scratch/...` namespace and can be paged with `read_file`.
Reads are limited to 64 KiB per call. Directory listings return at most 1,000
entries with `next_offset`. History defaults to 50 records, has a hard maximum
of 100, and omits stored exec preview text.

The connection is resilient: if the SSH link dies (network blip, sshd
resetting the connection), the next tool call reconnects automatically with
retries and a liveness probe. A call that fails mid-flight is reported as an
error, never silently re-executed. The spawned `ssh` uses BatchMode (no tty
prompts) and keepalives, and dies with the MCP process, so a killed session
cannot leave an orphaned connection holding the remote state lock.

## Guarantees

- **File boundary.** Normal paths resolve inside `--root`; `@scratch/...`
  resolves inside a separate server-managed scratch root. `..`, absolute
  paths, and symlinks escaping either root are rejected. Guards against
  accidents, not adversaries -- `exec` can still touch anything the user can.
- **Atomic mutations.** `create`/`edit` build the full result, then atomically
  install it (`edit` preserves file mode). A failed edit changes nothing.
- **Optimistic concurrency.** `edit` requires `base_hash` and is rejected
  with `STALE_FILE` if the file changed; `read` returns a hash usable
  directly as the next `base_hash`.
- **Durable, recoverable log.** Every operation is recorded in fsync'd JSONL
  with before/after hashes or bounded exec output previews. Mutations are
  write-ahead (`prepared`/`committed`), and startup recovery reconciles a
  crash between rename and commit.
- **Safe undo.** Only applies while the file is still in the recorded
  `after` state; otherwise `UNDO_CONFLICT`. Undoing a creation removes the
  file.
- **Idempotency.** Request results persist across restarts; replaying a
  `request_id` returns the stored result without re-executing, and
  `status <request_id>` answers "did that ever run?" after a reconnect.
  The replay window equals the retention window: entries older than the
  newest `--history-limit` operations are pruned.
- **Non-invasive state.** All server state lives outside the workspace,
  keyed by root path, with a single-writer lock and bounded growth (see
  "Server state" above).

## Layout and testing

```
crates/
  agent-remote-protocol/  # pure serde types: messages, errors, records
  agent-remote-server/    # workspace, fs ops, exec, operation store (binary)
  agent-remote-client/    # transport, typed API, CLI (binary `agent-remote`)
  agent-remote-mcp/       # MCP stdio server on top of the client (binary)
```

```bash
cargo test --workspace --all-targets   # includes end-to-end tests against
                                       # the real server and MCP binaries
cargo clippy --workspace --all-targets -- -D warnings
```
