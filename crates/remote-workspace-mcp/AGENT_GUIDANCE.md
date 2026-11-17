# Agent guidance

Every tool except `list_workspaces` requires `workspace`. Workspaces are isolated: paths and operation IDs apply to one workspace only. If `list_workspaces` returns `[]`, ask the user for a target and onboard it locally with `remote-workspace workspace add <name> --host <host> --root <path>`; never invent a workspace. Fleet edits reload on the next call. In `list_workspaces`, `connection` (`connected`, `connecting`, `not_connected`, `disconnected`) is a local hint, not a health check -- act directly. Warm rows also report `platform`, `profiles`, and `default_profile`.

## Files and scratch

- `read_file`: returns `[workspace=... path=... bytes=...]` above `---`, then raw content. Modes (mutually exclusive): byte (`offset`/`limit`, <= 64 KiB; `rg -b` gives byte offsets on >4 MiB files), line (`start_line`/`max_lines`, <= 2000 lines, <= 4 MiB), or tail (`tail_lines` or `tail_bytes`, any size). Files <= 4 MiB include `hash` in the header.
- `create_file`: new text files only (`ALREADY_EXISTS` if present).
- `edit_file`: modifies existing text files (<= 4 MiB) gated by `base_hash`. Pass batched ordered `edits` (1..=100 exact replacements in one atomic call) OR `replace_content` (whole-file rewrite), never both. On `STALE_FILE`, retry with the reported hash only if the change since your read was your own edit; otherwise re-read first.
- `delete_file`: permanent; rely on git.
- `@scratch/...` maps to transient workspace storage, passed to commands in `REMOTE_WORKSPACE_SCRATCH` (`$REMOTE_WORKSPACE_SCRATCH` / `$env:REMOTE_WORKSPACE_SCRATCH`). Shared across sessions with 7-day idle eviction: keep scripts, logs, and intermediate files there, and put durable results in the workspace or fetch via `download_file`. File tools enforce workspace/scratch roots; `run_command` is not sandboxed.

## Commands and long-running work

`run_command` is synchronous, owns its process tree, and NEVER shell-splits `argv` (use `["bash", "-c", "..."]` for pipes, globs, or redirection). Previews keep the first 4 KiB and last 12 KiB per stream; redirect large output to `$REMOTE_WORKSPACE_SCRATCH/...` and read `@scratch/...`. Descendants holding pipes past exit are killed (`drain_timed_out: true`). Never auto-retry after an uncertain transport drop.

Run work outliving one call under `tmux` or `slurm` with output in `$REMOTE_WORKSPACE_SCRATCH`. Use a dedicated socket (`tmux -L rw`) or expand `$REMOTE_WORKSPACE_SCRATCH` in the outer shell so an existing tmux server does not drop profile env vars:

`tmux -L rw new-session -d -s NAME "CMD > \"$REMOTE_WORKSPACE_SCRATCH/NAME.log\" 2>&1; echo \$? > \"$REMOTE_WORKSPACE_SCRATCH/NAME.exit\""`

Await the session, job, or process with `wait_while` (`0` = running, `1` = gone, other = `check_failed`), never its output file (`gone` with `first_check: true` catches work that never started). Use bounded `timeout_ms` (60000-120000) and repeat on `timed_out` after checking progress:

- tmux: `["bash", "-c", "tmux -L rw has-session -t =NAME 2>/dev/null && { tail -n 5 \"$REMOTE_WORKSPACE_SCRATCH/NAME.log\"; exit 0; }; cat \"$REMOTE_WORKSPACE_SCRATCH/NAME.exit\" 2>/dev/null; exit 1"]`
- slurm: `["bash", "-c", "s=$(squeue -h -j ID -o '%T %r' 2>&1); rc=$?; printf '%s\n' \"$s\"; case \"$s\" in *'Invalid job id'*) exit 1;; esac; [ $rc -eq 0 ] || exit 2; [ -n \"$s\" ]"]`
- process (same OS user; `EPERM` also exits 1): `["kill", "-0", "PID"]`

## Transfers

`upload_file` and `download_file` stream single regular files outside model context; never transfer via `create_file`, base64, shell quoting, or `read_file` pages. Parent directories must exist; pass `overwrite=true` to replace. Genuine stalls fail with `transfer_stalled` and byte position -- do not cancel healthy transfers. After a disconnect, inspect the destination directory before retrying. Use remote `curl -o` via `run_command` for URLs.
