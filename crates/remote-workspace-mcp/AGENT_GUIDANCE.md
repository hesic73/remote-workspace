# Agent guidance

This server manages one or more named workspaces, each a directory on a configured machine. Every tool except `list_workspaces` requires a `workspace` argument naming which one to act on. Workspaces are fully isolated from each other: paths and operation IDs are scoped to a single workspace and mean nothing in another.

The normal workflow:

1. Call `list_workspaces` when the workspace name is unknown. Its `connection` field is only what this process last observed (`connected`, `not_connected`, `disconnected`, `in_use`) -- a hint about which workspaces are already warm, never a promise that one is reachable now. Do not consult it before acting; act, and read the error if there is one.
2. Inspect with `list_directory` and `read_file`; follow the returned offsets to page through large results.
3. Use `create_file` only for new text files; it refuses existing paths.
4. Use `edit_file` for every modification to an existing text file: pass the hash from `read_file` as `base_hash` and copy each `old_text` exactly from the current content. There is no way to skip the read; an invented `base_hash` is rejected as a bad argument. Pass every change you have for one file as a list of `edits` in a single call rather than calling once per change: they apply in order, each to the result of the one before, and either all of them land or the file is left untouched. Sending them separately is slower and leaves the file half-changed if a later one fails.
5. Use `delete_file` for deletion. Nothing here reverses a file operation: the log records what happened, it does not restore. Anything that must survive a mistake belongs in version control.
6. Use `run_command` for search (`rg`), file discovery (`find`), Git, builds, tests, and running programs.
7. Use `upload_file`/`download_file` for large or binary files; their content never enters the model context. Never move binary data through `create_file`, base64, shell quoting, or paginated `read_file`.
8. Never automatically retry `run_command` after an uncertain transport failure, because the command may already have produced side effects.

Normal relative paths address the workspace. Paths beginning with `@scratch/` address the workspace's server-managed scratch area; commands receive its physical path in `$REMOTE_WORKSPACE_SCRATCH`.

Scratch is transient and shared by every session using that workspace. It is the right place for your own working material -- analysis scripts you wrote, intermediate output, command logs -- which would otherwise clutter the user's project. It is not storage: files idle for a week are deleted without warning. Anything meant to last belongs in the workspace, or on the local machine via `download_file`. Do not park results there -- trained weights, recordings, datasets -- and do not treat a file you left in scratch last week as still being there.

`run_command` is synchronous and owns its process tree. The result contains the termination reason, duration, and a bounded preview of each stream (first 4 KiB, last 12 KiB); redirect full output to `$REMOTE_WORKSPACE_SCRATCH` and read the `@scratch/...` file incrementally. After the command exits, output collection waits briefly for the pipes to close, then kills leftover descendants and sets `drain_timed_out` in the result. For work that must outlive one call, use a remote-native supervisor such as tmux and write its logs to scratch; a successful launcher exit does not confirm the detached workload succeeded.

Choose between reading and transferring by where the bytes need to end up. To reason about file contents, use `read_file` and page through it with the returned offsets; it is capped per call on purpose, so paging is the intended way to read something large. To place a file on the other machine, use `upload_file`/`download_file`; never reconstruct a file by pasting together `read_file` pages, which corrupts anything non-textual and wastes the context window.

Transfers move exactly one regular file between the local machine and one workspace; the destination's parent directory must already exist, and an existing destination is only replaced with `overwrite=true`. They are synchronous: a long-running call means bytes are still flowing, and a transfer that genuinely stops moving fails by itself with `transfer_stalled` and the byte position reached, so a slow link never needs to be guessed at. Do not cancel and re-issue a transfer merely because it is taking a while. After a disconnected transfer, check the destination (e.g. `list_directory` its parent) before deciding whether to retry. To import a file from a URL, run a downloader (`curl -o`) on the remote side via `run_command` instead of transferring through the local machine.

File tools are confined to the workspace and scratch roots. Command execution is not a sandbox and can access anything permitted to the remote server user.
