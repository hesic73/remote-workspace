# Agent guidance

This server manages zero or more named workspaces, each a directory on a configured machine. Every tool except `list_workspaces` requires a `workspace` argument naming which one to act on. Workspaces are fully isolated from each other: paths and operation IDs are scoped to a single workspace and mean nothing in another.

An empty `list_workspaces` result means no workspaces are configured. Ask the user for a target and add it locally with `remote-workspace workspace add <name> --host <host> --root <path>`. Do not invent a workspace or register the current directory automatically. The running MCP picks up the first workspace on its next call.

The normal workflow:

1. Call `list_workspaces` when the workspace name is unknown. Its `connection` field is only what this process last observed (`connected`, `not_connected`, `disconnected`, `in_use`) -- a hint about which workspaces are already warm, never a promise that one is reachable now. Do not consult it before acting; act, and read the error if there is one.
2. Inspect with `list_directory` and `read_file`; follow the returned offsets to page through large results.
3. Use `create_file` only for new text files; it refuses existing paths.
4. Use `edit_file` for every modification to an existing text file: pass the hash from `read_file` as `base_hash` and copy each `old_text` exactly from the current content. Any other valid hash fails with `STALE_FILE`, whose message shows the current hash: retry with it only when the change since your read is your own edit, and otherwise read again first. Pass every change you have for one file as a list of `edits` in one call: sending them separately is slower and leaves the file half-changed if a later one fails.
5. Use `delete_file` for deletion. Nothing here reverses a file operation; anything that must survive a mistake belongs in version control.
6. Use `run_command` for search (`rg`), file discovery (`find`), Git, builds, tests, and running programs.
7. Use `upload_file`/`download_file` for large or binary files and whenever a file must land on the other machine; their content never enters the model context. Use `read_file` only to reason about content. Never move a file through `create_file`, base64, shell quoting, or pasted-together `read_file` pages.
8. Never automatically retry `run_command` after an uncertain transport failure, because the command may already have produced side effects.
9. Wait for long-running work with `wait_while`, never by polling a log or output file (see below).

Normal relative paths address the workspace. Paths beginning with `@scratch/` address the workspace's server-managed scratch area; commands receive its physical path in the `REMOTE_WORKSPACE_SCRATCH` environment variable (`$REMOTE_WORKSPACE_SCRATCH` in POSIX shells, `$env:REMOTE_WORKSPACE_SCRATCH` in PowerShell, `%REMOTE_WORKSPACE_SCRATCH%` in cmd).

Scratch is transient and shared by every session using that workspace: the place for your own working material (scripts you wrote, intermediate output, command logs) that would otherwise clutter the user's project. It is not storage: files idle for a week are deleted without warning. Results (trained weights, recordings, datasets) belong in the workspace, or on the local machine via `download_file`.

`run_command` is synchronous and owns its process tree. The result contains the termination reason, duration, and a bounded preview of each stream (first 4 KiB, last 12 KiB); redirect full output to `$REMOTE_WORKSPACE_SCRATCH` and read the `@scratch/...` file incrementally. After the command exits, output collection waits briefly for the pipes to close; descendants still holding them are killed and the result sets `drain_timed_out`.

Work that must outlive one call runs under a remote supervisor (tmux, slurm, or a backgrounded process) with its output redirected to a file. A launcher that exited 0 has not shown that the work started. Wait with `wait_while` on what does the work -- the session, the job, the process -- and never on its log or result file: a result that never appears waits forever, while work that never started is `gone` at the first check (`first_check: true`). Checks that exit 0 while the work exists and 1 once it is gone:

- tmux session: `["tmux", "has-session", "-t", "=NAME"]`. The `=` matches the name exactly; the session closes when its command exits.
- slurm job: `["bash", "-c", "squeue -h -j ID -o '%T %r' | grep ."]`. It prints the state and pending reason while the job is queued or running.
- process: `["kill", "-0", "PID"]`.

After `gone`, read the outcome where the work left it: its log, an exit-code file it wrote, or `sacct -j ID`. `timed_out` means the work still exists; read the last check's output and the log before waiting again. Work that exists but makes no progress (stuck in a queue, hung) is never `gone`, so between waits check that it advances.

Transfers move exactly one regular file between the local machine and one workspace; the destination's parent directory must already exist, and an existing destination is only replaced with `overwrite=true`. They are synchronous: a long-running call means bytes are still flowing, and a transfer that genuinely stops moving fails by itself with `transfer_stalled` and the byte position reached. Do not cancel and re-issue a transfer merely because it is taking a while. After a disconnected transfer, check the destination (e.g. `list_directory` its parent) before deciding whether to retry. To import a file from a URL, run a downloader (`curl -o`) on the remote side via `run_command` instead of transferring through the local machine.

File tools are confined to the workspace and scratch roots. Command execution is not a sandbox and can access anything permitted to the remote server user.
