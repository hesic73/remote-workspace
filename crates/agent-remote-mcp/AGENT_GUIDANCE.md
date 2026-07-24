# Agent guidance

This server manages one or more named workspaces, each a directory on a configured machine. Every tool except `list_workspaces` requires a `workspace` argument naming which one to act on. Workspaces are fully isolated from each other: paths, operation IDs, history, undo, and request IDs are scoped to a single workspace and mean nothing in another.

The normal workflow:

1. Call `list_workspaces` when the workspace name is unknown.
2. Inspect with `list_directory` and `read_file`; follow the returned offsets to page through large results.
3. Use `create_file` only for new text files; it refuses existing paths.
4. Use `edit_file` for every modification to an existing text file: pass the hash from `read_file` as `base_hash` and copy `old_text` exactly from the current content.
5. Use `delete_file` for deletion.
6. Use `run_command` for search (`rg`), file discovery (`find`), Git, builds, tests, and running programs.
7. Use `upload_file`/`download_file` for large or binary files; their content never enters the model context. Never move binary data through `create_file`, base64, shell quoting, or paginated `read_file`.
8. Use `undo` only for recorded file mutations (create/edit/delete), and only while the file is unchanged since.
9. Never automatically retry `run_command` after an uncertain transport failure, because the command may already have produced side effects.

Normal relative paths address the workspace. Paths beginning with `@scratch/` address the workspace's server-managed scratch area; commands receive its physical path in `$AGENT_REMOTE_SCRATCH`. Use scratch for logs and other runtime artifacts.

`run_command` is synchronous and owns its process tree. The result contains the termination reason, duration, and a bounded preview of each stream (first 4 KiB, last 12 KiB); redirect full output to `$AGENT_REMOTE_SCRATCH` and read the `@scratch/...` file incrementally. After the command exits, output collection waits briefly for the pipes to close, then kills leftover descendants and sets `drain_timed_out` in the result. For work that must outlive one call, use a remote-native supervisor such as tmux and write its logs to scratch; a successful launcher exit does not confirm the detached workload succeeded.

Transfers move exactly one regular file between the local machine and one workspace; the destination's parent directory must already exist, and an existing destination is only replaced with `overwrite=true`. They are synchronous: a long-running call means bytes are still flowing. After a disconnected transfer, check the destination (e.g. `list_directory` its parent) before deciding whether to retry. To import a file from a URL, run a downloader (`curl -o`) on the remote side via `run_command` instead of transferring through the local machine.

File tools are confined to the workspace and scratch roots. Command execution is not a sandbox and can access anything permitted to the remote server user.
