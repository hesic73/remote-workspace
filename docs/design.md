# Design

Why remote-workspace works the way it does, and what the protocol guarantees.
For installation and usage see the [README](../README.md); this document is
the rationale and the reference for behavior that callers depend on.

## Motivation

Coding agents are practical to run on only a few machines, while code and
execution environments are spread across servers, workstations, and
containers. Installing a full agent everywhere does not scale, especially for
short-lived containers.

So: decouple the agent's intelligence from the execution environment. The
agent runs on the client side and plans changes; the remote side runs a
lightweight endpoint exposing file reads, file mutations, command execution,
and status queries. The client never clones or syncs the workspace, and one
agent can reach many heterogeneous environments through one interface.

## Architecture

```text
coding agent
    |
    v
remote-workspace (CLI)  or  remote-workspace-mcp (MCP server)
    |
    v
remote-workspace client library
    |
    | persistent SSH stdio connection
    v
remote-workspace-server  --  fs ops, exec, operation log, workspace root
```

The client starts the remote process itself:

```bash
ssh <host> remote-workspace-server --root /path/to/project
```

SSH stdin/stdout is the transport: no public IP, no extra port, no daemon. If
`ssh <host>` works, the connection works, inheriting `~/.ssh/config`,
ProxyJump, ControlMaster, Tailscale, and the rest.

The protocol is JSON Lines: one request and one terminal response per line,
correlated by `request_id`.

## Session semantics

Persistent connection, stateless execution. The SSH connection, server
process, and workspace root persist; every `exec` spawns a fresh child
process, so `conda activate` in one command does not leak into the next.
Environment setup (conda, ROS, ...) is re-applied per command via server-side
profiles:

```toml
default_profile = "user-zsh"

[profiles.user-zsh]
shell = ["zsh", "-lic"]
setup = ""

[profiles.robot]
setup = """
source /mnt/data/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

A profile owns two things and nothing more: which shell to start (`shell`,
default `["bash", "-c"]`; the server appends `setup` + `exec <argv>` as the
final argument) and what to run before the command (`setup`). Choosing
`["zsh", "-lic"]` reuses the user's own login/interactive configuration
instead of teaching the server about conda or ROS -- the server never
understands toolchains, it only picks a shell and execs through it. Without
any profile (explicit or `default_profile`), the argv is spawned directly
with no shell at all. Config parsing is strict (`deny_unknown_fields`, empty
shells and undeclared defaults rejected at startup): an older server reading
a newer config must fail loudly, never silently run commands in the wrong
environment.

There is no server-side `cd`; every request carries explicit paths or `cwd`.
Interactive sessions (PTY, REPL, persistent shell) are out of scope.

## Operations

```text
list  stat  read  create  edit  delete  exec
history  operation.get  request.status  gc
upload_prepare  upload_commit  upload_abort  download_record   (transfer control plane)
```

### Reads and hashes

`read` returns content plus a hash over the file's raw bytes. Only the
requested window is read, so paging through a multi-gigabyte exec log costs the
page, not the file. The hash exists to be handed back as `edit`'s `base_hash`,
and `edit` refuses files above 4 MiB, so above that size it is omitted rather
than re-derived per page from content nothing can edit:

```json
{"request_id":"r1","op":"read","path":"src/main.py","offset":0,"limit":65536}
{"request_id":"r1","type":"read","content":"...","hash":"sha256:abc","truncated":true,"next_offset":65536}
```

### Mutations: one intent, one operation

Creation and modification are deliberately separate, so there is exactly one
canonical way to perform each:

* `create` makes a NEW text file and fails with `ALREADY_EXISTS` if the path
  exists (installed atomically via an exclusive link, so even a concurrent
  creator cannot be clobbered).
* `edit` modifies an EXISTING text file by exact text replacement, the
  editing semantics coding models already know: `old_text` must match the
  current content exactly; zero occurrences fail with `NO_MATCH`, several
  with `AMBIGUOUS_MATCH` unless `replace_all` is set; an empty `new_text`
  deletes the matched text. A full-file rewrite passes the entire current
  content as `old_text`, keeping destructive replacement explicit.

  One `edit` carries a **list** of replacements, applied in order, each to the
  result of the one before it -- so a replacement may match text an earlier one
  produced, and its match count is taken against that content rather than the
  original. The list is one operation: the content is built and fully validated
  in memory, then installed by a single rename, so a failure at any position
  leaves the file byte-for-byte unchanged and the log gets one record with one
  before/after pair. This is why the list exists at all -- three separate
  `edit` calls are three separately interruptible mutations, and a failure in
  the third leaves the file in a state no caller asked for. Errors name the
  failing position (`edit 2 of 3`) when there is more than one. Bounded at 100
  replacements per call, each separately bounded like any other text input.

  Creating a file is still `create` alone: an empty `old_text` is a bad
  argument here, not a shorthand for "write this file". One intent, one
  operation applies to the list form too.

```json
{"request_id":"r2","op":"edit","path":"src/main.py","base_hash":"sha256:abc","edits":[{"old_text":"x = 1","new_text":"x = 2"},{"old_text":"def f(","new_text":"def g("}]}
{"request_id":"r2","type":"write","operation_id":"op-7","old_hash":"sha256:abc","new_hash":"sha256:def"}
```

`edit` requires a `base_hash`. The server checks the current hash first and
rejects with `STALE_FILE` (carrying `expected_hash`/`actual_hash`) if the
file changed under you. Mutations build the complete new content, then
atomically rename into place (preserving file mode) -- a failed edit leaves
the file byte-for-byte unchanged. Success returns an `operation_id` plus
`old_hash`/`new_hash`. Inputs, the edited file, and the result are bounded at
4 MiB; larger or binary files use the transfer path.

The earlier line-based `patch` operation was removed rather than kept as a
second editing mechanism; logs recorded by older servers still load.

### Exec

```json
{"request_id":"r3","op":"exec","argv":["pytest","-q"],"cwd":".","profile":"robot","timeout_ms":300000}
{"request_id":"r3","type":"exec","operation_id":"op-43","termination":{"kind":"exited","code":0},"duration_ms":842,"stdout":{"prefix":"...","suffix":"","total_bytes":3,"omitted_bytes":0},"stderr":{"prefix":"","suffix":"","total_bytes":0,"omitted_bytes":0}}
```

The result is synchronous and bounded: each stream retains its first 4 KiB and
last 12 KiB. `exec` promises no transactionality -- it can do anything the
remote user can.

`exec` owns the command's process tree (the child runs in its own session via
`setsid`, whose failure aborts the spawn). The central invariant: every
invocation reaches a terminal response within a bounded period, including
subprocess cleanup. After the direct child exits, output collection waits a
short grace period (2 s) for stdout/stderr to reach EOF; a descendant that
inherited the pipes and still holds them at the deadline is SIGKILLed along
with the rest of the process group, the readers are abandoned, and the result
carries `drain_timed_out: true` to say collection stopped before pipe EOF. A
descendant that redirected its output away (the tmux/nohup pattern) closes
the pipes at exit and survives. Timeout kills the whole process group
immediately. Detached workloads are not a supported property of `exec`.

### File transfer

`upload_file`/`download_file` (exposed as MCP tools and client-library
functions) move single regular files without pushing content through the JSONL
protocol. The control plane stays on the resident connection; the data plane is
a separate short-lived process per transfer, spawned over the same SSH
configuration:

```text
remote-workspace-server --transfer-receive <staging> --expect-size N   # stdin -> staging file
remote-workspace-server --transfer-send <path> --root R [--state-base B]  # header JSON, raw bytes, trailer JSON -> stdout
```

These raw modes never open the operation store, so they cannot conflict with
the resident server's state lock. `--transfer-send` re-validates the path with
the same workspace/`@scratch` boundary rules as every other operation.

Uploads are three-phase on the control plane: `upload_prepare` validates the
target (parent must exist; existing targets refused unless `overwrite`) and
creates a staging file named `.remote-workspace-upload.<name>.<random>.part` in
the target's directory; `upload_commit` verifies the staged size, installs
atomically (rename for overwrite, hard-link-then-unlink for race-free
no-replace), fsyncs, and appends the operation record; `upload_abort` deletes
the staging file after a failure. The staging path travels only between the
resident server and the client; it is never persisted or shown to the agent.
Downloads verify size and SHA-256 against the sender's framing, install
locally via temp file + (no-clobber) rename, then append a `download_record`.

Upload integrity is verified once, in a single pass: the receive process
hashes the bytes as they stream into the staging file and reports
`{size, sha256}`, which the client checks against the local file before
committing. `upload_commit` re-checks the staged byte count but does NOT
rehash the staging file -- rehashing would read large files twice, and the
workspace is not defended against the same OS user anyway (that is the
documented trust model).

A hard-killed or interrupted upload can leave its staging file behind.
Cleanup is conservative and best-effort: only files matching the exact
`.remote-workspace-upload.*.part` convention, older than 24 hours by mtime (an
active upload keeps its mtime fresh), and not registered as in-flight are
deleted. The sweep runs where staging files accumulate -- the target
directory on each `upload_prepare` -- and over the whole workspace and
scratch trees on `gc`, which reports the count as `removed_stale_staging`.
Startup deliberately does not walk the tree: a server starts on every
reconnect, and an unbounded scan there would tax large workspaces.

Both directions stream through fixed 64 KiB buffers; memory does not grow
with file size. Operation records are metadata-only (direction, remote
logical path, size, hash, duration) -- no local paths, no content. Transfers
are synchronous and have no resume/job machinery: a
dropped connection fails the call, the destination is never left
half-written, and the caller just retries.

A stalled transfer is not the same as a slow one, and the difference is what a
caller actually needs to know. There is deliberately **no total timeout**: a
large file over a slow link legitimately takes hours, and any ceiling would
kill healthy transfers. Instead each step is bounded by a stall window
(`REMOTE_WORKSPACE_STALL_TIMEOUT_MS`, default 120 s) measuring only whether
*anything* is still moving; crossing it fails with `transfer_stalled` and the
byte position, so a transfer that died at 42 of 100 MiB says so. Every other
transfer error carries the same position and average rate. Detecting the stall
is not sufficient on its own: a sender that has stopped producing will also
never exit, so the child is killed before it is reaped -- otherwise the call
hangs at the reap, precisely where the stall was supposed to surface.

## No reversal

Nothing here undoes a file operation. An `undo` operation existed through
protocol 1, restoring a `create`/`edit`/`delete` from a stored before-blob
while the file was untouched since. It was removed in protocol 2, because the
cases it covered are the ones a caller can already redo -- an agent that just
wrote an edit still holds the old text, and a tracked file is a `git checkout`
away -- while everything genuinely destructive (`rm -rf`, `mv`, `sed -i`, a
bad `git` invocation) runs through `exec` and was never reversible anyway. A
safety net that catches only what does not fall is worth less than the cost of
carrying it: a before-blob written and fsync'd on every mutation, plus its own
recovery and pruning paths.

What remains is the record, not the rescue: `history` and `operation.get` say
exactly what happened, and durability against mistakes belongs to version
control. A state directory written by an older server hands its now-unreadable
`blobs/` back as disk on first start.

## Server state and logging

All server state lives **outside the workspace**, on the remote host, keyed by
the canonical root path:

```text
~/.remote-workspace/state/<rootname>-<hash>/
|-- operations.jsonl   one record per operation (fs + exec)
|-- requests.jsonl     request idempotency table
|-- scratch/           agent-visible runtime artifacts (`@scratch/...`)
|-- lock               single-writer flock
|-- server.jsonl       server lifecycle: start, lock refusal, exit reason
`-- op-counter         id high-water mark (prevents reuse after pruning)
```

The workspace stays untouched -- nothing for `git status`, nothing a
destructive command inside the workspace can destroy along with itself.
`--state-base` swaps the base directory while keeping per-root keying (for
hosts where home is nearly full). State is per-workspace, not per-session:
sessions are just connections, and cross-session features (history, replay
after reconnect) are exactly the reason the state must outlive them.

* **Server log = execution truth.** Every operation is recorded with hashes,
  argv, and exit codes. Appends are fsync'd. Mutations are
  write-ahead: `prepared` before the rename, `committed` after, so a crash in
  between is reconciled on restart instead of leaving a phantom operation.
* **Client log = interaction truth.** Optional JSONL log of every request
  sent and every response/event received (including truncation flags), i.e.
  what the agent actually saw. `remote-workspace-mcp --log-dir DIR` writes one
  per workspace and `remote-workspace stats` aggregates them into per-tool
  call and error counts. It is the only record of the read-only calls, which
  mutate nothing and are deliberately absent from the operation log, so
  "which tools does the agent actually use" is a question only this log
  answers. Off by default: a request line carries the full content of a
  `create_file` or `edit_file`.
* **Session log = lifecycle truth.** `server.jsonl` records one line per
  server start, refused start (with the pid holding the lock), and exit (with
  the reason). The server's stderr goes to whoever is attached over SSH, and
  the case worth diagnosing -- a server still running after its client
  vanished -- is precisely the case where nobody is. This file is what remains
  on the remote to answer it. Trimmed to its newest lines at startup once past
  256 KiB. The server logs at `info` by default rather than requiring
  `RUST_LOG`, which cannot be set through `ssh host '<binary> ...'` anyway.
* **Bounded growth.** At startup the server prunes to the newest
  `--history-limit` operations (default 1000; 0 disables), dropping older
  records and request entries no longer referenced. The `gc` operation does the
  same on demand. A pruned operation id returns `OPERATION_NOT_FOUND`; pruned
  ids are never reallocated.
* **Scratch expires.** Files idle -- neither written nor read -- beyond
  `--scratch-max-age-days` (default 7; 0 disables) are deleted, and `gc`
  reports what scratch holds either way. *Idle* counts reads deliberately:
  under `relatime` an agent paging through a large log leaves mtime untouched,
  so evicting on mtime alone would delete the file mid-read. There is no
  daemon; the sweep rides on server startup and a marker file limits it to
  once a day per workspace, so a reconnect normally costs one stat. Growth and
  cleanup are therefore driven by the same event -- use -- and a workspace
  nobody touches is never swept because it is not growing either.

  Age is the only criterion. A size ceiling was considered and rejected twice
  over: a total cap deletes the wrong file (something perfectly good goes
  because something else was just written, which makes the same workflow
  succeed or fail depending on unrelated activity), and a per-file cap cannot
  express the distinction that matters -- in real usage here the legitimate
  logs are *larger* than the checkpoints that do not belong. What scratch is
  for is a question of kind, not bytes, so it is stated in the agent guidance
  instead: working material yes, results no.
* **Single writer.** The state directory is protected by an exclusive flock
  held for the server's lifetime (auto-released by the kernel on death). A
  second server on the same root fails fast with a clear error; reconnects
  get a short grace period while the predecessor shuts down.
* **The server outlives no one.** Stdin EOF is the ordinary way a server
  exits, but it depends on the far end of the SSH session noticing that its
  peer is gone -- which a suspended laptop, a switched network, or a dropped
  VPN can delay for as long as TCP keepalives take (hours). The client's own
  detection is aggressive by comparison (`ServerAliveInterval=30`,
  `ServerAliveCountMax=4`), so without a deadline of its own the server spends
  that entire gap holding a lock for a client that is already gone and
  reconnecting -- which is what makes a workspace look occupied when nobody is
  using it. So the server also exits after `--idle-timeout-secs` (default 900;
  0 disables) with no request arriving *and none running*: a single hour-long
  `exec` is silence, not idleness, and must not be killed by the timer waiting
  on it. Timing out is cheap because connections are rebuilt on demand -- the
  next tool call pays one SSH connect.

## Idempotency and reconnect

Every request has a globally unique `request_id`. The server persists results
in `requests.jsonl` and reloads them on restart, so resending the same
`request_id` returns the stored result without re-executing, and
`request.status` reports what became of one.

No shipped client draws on this yet. The MCP layer generates a fresh id per
send and never replays: a dropped connection surfaces as an error and the
next call reconnects. The table is still worth its one append per request --
it is what makes replay possible at all -- but the recovery it enables is a
capability, not a behavior. `exec` is never auto-retried in any case, since
re-running a command may not be safe.

The replay window equals the retention window: request entries older than the
newest `--history-limit` operations are pruned along with them. Reconnect
recovery happens minutes after a drop, far inside any reasonable window.

## Workspace boundary

All file paths resolve inside `--root`; `..`, absolute paths, and symlinks
escaping the root are rejected (including a non-existent leaf under a
symlinked parent). This guards against accidents, not adversaries -- `exec`
can still reach anything the remote user can. Real isolation belongs to
containers or user permissions.

## Deployment and onboarding

Adding a workspace is one local command, which probes the target, installs or
upgrades the server, proves the workspace works, and records it:

```bash
remote-workspace workspace add robot --host robot@workstation --root /home/robot/project
```

This is a **local CLI operation and never an MCP tool**. It expands the set of
machines and directories an agent may reach, so it belongs on the trusted side
of the boundary: the CLI adds and removes workspaces and installs binaries;
the MCP exposes only already-configured workspaces; the agent chooses among
them. There is no `add_workspace`, `install_server`, or reload tool.

`add` is onboarding only and refuses a name already in the fleet, so picking up
a new release is a separate verb:

```bash
remote-workspace workspace upgrade [<name>]
```

It installs once per SSH identity however many workspaces share it, leaves
user-managed binaries alone, and never edits the fleet file -- upgrading a
server changes no authorization, so it needs none of `add`'s transaction.

Two version fields are reported by `remote-workspace-server --version-json`, a
stable probe that mutates nothing, needs no config, and never starts the JSONL
server:

```json
{"software_version": "0.5.0", "protocol_version": 3}
```

`software_version` identifies the release and its CI artifact;
`protocol_version` is an integer bumped only when client/server compatibility
changes, so a bug-fix release does not force a redeployment. The rule is
one-directional: **a newer client may upgrade an older server; an older client
never downgrades a newer one.** A missing or legacy server (one that does not
understand `--version-json`) is installed; an older protocol or, at equal
protocol, older software is upgraded; equal or newer software is left alone; a
newer protocol is refused with `client_too_old` and the remote is untouched.

Each SSH identity has exactly one active managed binary at
`~/.local/lib/remote-workspace/remote-workspace-server`, shared by every workspace on
that host -- installation is a property of the identity, not the workspace.
There is no multi-version layout: GitHub Releases is the historical archive.
The client never builds the server; CI publishes static musl artifacts plus a
machine-readable `release-manifest.json` and `SHA256SUMS`, and the client
downloads the artifact pinned to *its own* release (never an unpinned
`latest`), verifies its SHA-256, and caches it under
`~/.cache/remote-workspace/server/`. The remote host needs no internet access.
Passing an explicit `--remote-bin` marks the server user-managed: checked for
compatibility, never installed or overwritten.

Installation is atomic and downgrade-proof. The artifact is uploaded to a
unique temporary path, and the *uploaded binary installs itself*: it verifies
its own SHA-256, takes an advisory `flock` on
`~/.local/lib/remote-workspace/install.lock`, re-probes what is installed **inside
the lock** (a version observed before acquiring it is not authoritative), and
renames itself into place only if the installed server is strictly older.
Doing the compare-and-swap inside one locked process is what makes concurrent
installers safe; an equal or newer server is kept and the upload deleted.
Replacing an executable path does not disturb processes already running from
the old inode, so live sessions continue unaffected and no session registry is
needed -- the workspace state lock already prevents two servers from sharing a
root.

The fleet file is only written after remote installation and a real protocol
round-trip against the target root both succeed (reusing the `--check` probe
rather than adding a second health path). The write preserves comments and
unrelated entries (`toml_edit`), holds a local lock, re-checks for duplicate
names and duplicate `(host, root)` pairs under it, validates the complete
result, and installs it via temp file + fsync + rename. Failure at any earlier
step leaves the fleet unchanged: a workspace becomes authorized only when that
final write lands. Errors carry stable codes naming the failing layer --
`ssh_connect_failed`, `remote_probe_failed`, `unsupported_remote_platform`,
`workspace_root_not_found`, `workspace_root_invalid`,
`release_manifest_unavailable`, `artifact_not_found`,
`artifact_checksum_mismatch`, `artifact_cache_failed`, `remote_install_failed`,
`server_probe_failed`, `client_too_old`, `workspace_already_exists`,
`duplicate_workspace_target`, `fleet_write_failed`.

## MCP integration

Operational conventions for agents live only in
[`AGENT_GUIDANCE.md`](../crates/remote-workspace-mcp/AGENT_GUIDANCE.md), which the
MCP server embeds verbatim in `ServerInfo.instructions` -- it is a shipped
asset of that crate, not prose about it. This section documents protocol
behavior rather than duplicating those instructions.

`remote-workspace-mcp` wraps the client library in an MCP stdio server that
multiplexes a fleet of named workspaces, declared in a single TOML file
(`~/.remote-workspace/workspaces.toml` by convention). A workspace is a `(machine,
root)` pair; "two roots on one machine" and "one root each on two machines"
are the same concept, because all server-side state is already keyed per
root. The agent sees tools: `list_workspaces`, `list_directory`,
`read_file`, `create_file`, `edit_file`, `delete_file`, `run_command`,
`upload_file`, `download_file` -- one canonical tool per intent (search, file
discovery, Git, builds, and tests all go through `run_command`; no wrapper
tools), and each (except `list_workspaces`) with a required
`workspace` argument. Making it required, with no default, is deliberate: a
call can never land on the wrong machine because a default silently filled
in. Results echo the workspace name, since operation and request IDs are
only unique within one workspace.

`history`, `operation.get` and `request.status` are protocol operations
without MCP tools. Across ~3,000 logged calls, `history` was used three times
(all while testing this server) and the other two never. `request.status`
never could be: request ids are generated per send and appear in no result or
error, so the agent cannot obtain the argument. `operation.get` could be, but
an agent that just made an edit already knows what it did. Both stay on the
CLI, where the reader is a person rather than the agent that wrote the
record.

`list_directory` survives a similar test for the opposite reason. Agents
rarely call it either, preferring `ls` for its flags and composition, but it
is the only file discovery that enforces the workspace boundary, and a
surface that can read a path but not find one is incoherent. An unpopular
tool is not a dead one.

The MCP process keeps one independent, lazily-opened connection per
workspace, so a dead machine costs only its own calls, and the fleet needs
no server-side coordination at all -- there is no cross-workspace operation
(file movement between workspaces goes through a local file via
`download_file` + `upload_file`).

* Protocol errors map to MCP `isError` results, so failures are visible to
  the agent.
* `run_command` returns one synchronous terminal result. The server drains both
  pipes but retains only the first 4 KiB and last 12 KiB of each stream, with
  total and omitted byte counts. No streaming output path exists.
* `read_file` returns at most 64 KiB per call; directory listings return at
  most 1,000 entries with `next_offset`. History defaults to 50 records,
  rejects limits above 100, and omits exec preview text.
* Integer parameters are published as `integer` but also accept an
  unambiguous numeric string, because some hosts stringify every scalar and
  the agent cannot diagnose the resulting rejection from a schema that told
  it `integer`. Nothing else is coerced.
* In SSH mode the remote command line is shell-quoted per argument, because
  `ssh` re-parses its trailing arguments through the remote shell.
* The fleet file is reloadable configuration, not startup-only state. Before
  listing workspaces or resolving one for a tool call, the MCP compares a
  cheap file stamp and, if it changed, parses and validates the whole file and
  swaps the in-memory snapshot atomically. Workspaces whose endpoint is
  unchanged keep their existing connection slot, so an edit elsewhere in the
  file never drops a live SSH session; new entries get slots, removed or
  materially changed ones lose theirs. An invalid file is never partially
  applied: the last known-good snapshot is retained and every operation returns
  `fleet_reload_failed` until the file parses again. Reporting the breakage once
  and then quietly serving a stale fleet would turn a real misconfiguration into
  a silent one, so the failure is deliberately persistent rather than a
  one-shot warning. Because `workspace add` writes only fully validated
  configuration atomically, a new workspace simply appears on the next call --
  without restarting Claude Code, Codex, or the MCP process.
* Connections are rebuilt on demand: a dead link is replaced on the next
  tool call (retries with backoff, probed with a real round-trip), while a
  call that dies mid-flight surfaces as an error and is never auto-retried.
  `initialize` never blocks on connecting, and the transport child carries
  PDEATHSIG so a killed MCP cannot orphan its ssh (which would keep the
  remote server -- and the state lock -- alive). PDEATHSIG covers a dead
  *process*; a dead *link* is covered from the far end by the server's idle
  timeout.
* The transport child's stderr is captured, not inherited, and its tail is
  attached to `server closed connection`. A server that refuses to start says
  why and exits; inheriting that text scatters it into the MCP host's log,
  where the agent -- the one being told the workspace is unreachable -- cannot
  see it. Kept as the *last* few lines, since a login shell on the far end
  prints its own noise before the part that matters.

## Technology

Rust workspace; both ends ship as single near-static binaries (no runtime to
install remotely). Tokio for stdio/process/timeout concurrency; serde for the
protocol; the system `ssh` binary as transport (no SSH library). The protocol
crate has no I/O dependencies, so other transports can be added without
touching operation semantics.

Deliberately absent: databases (JSONL suffices), custom daemons,
embedded shells (commands run from explicit `argv`; only profile setup goes
through a shell), and RPC frameworks.

```text
crates/
  remote-workspace-protocol/  # pure serde types: messages, errors, records
  remote-workspace-server/    # workspace boundary, fs ops, exec, operation store
  remote-workspace-client/    # transport, typed API, client log, CLI
  remote-workspace-mcp/       # MCP stdio server on top of the client
```

Tests live inside each crate: protocol round-trips, in-process server tests,
end-to-end tests that spawn the real server binary over stdio, and MCP tests
that drive the real `remote-workspace-mcp` binary.

## Non-goals

Deliberately out of scope, so the primitives stay small and predictable:
workspace sync or cloning, resident daemons, reversal of any operation,
multi-file transactions, multi-agent merging, job scheduling, and interactive
PTYs (REPLs, persistent shells).

**Concurrent sessions on one workspace.** The state directory's flock admits
exactly one server per root, so a second agent session against the same
workspace fails to connect while the first holds it (after a short grace for a
predecessor still shutting down). This is a real limitation, not an oversight:
the alternative is a multi-process operation log -- per-append locking, id
allocation through the counter file, and in-memory tables demoted to caches
with invalidation -- which puts the WAL and idempotency core at risk to serve a
case with a simple workaround. Point the second session at a different root, or
wait for the first to exit -- bounded by the idle timeout, so "wait for it" is
minutes rather than however long a dead session goes unnoticed. Which server
holds the lock, and whether it was ever knocked on, is in the holder's
`server.jsonl`.
