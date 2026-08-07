#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

fn mcp_bin() -> String {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/remote-workspace-mcp");
    p.to_string_lossy().into_owned()
}

fn server_bin() -> String {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/remote-workspace-server");
    p.to_string_lossy().into_owned()
}

/// A local fleet entry named `name` for `root`, with server state kept inside
/// the root instead of the real HOME.
fn fleet_entry(name: &str, root: &str) -> String {
    format!(
        "[workspaces.{name}]\nroot = {root:?}\nbin = {srv:?}\nstate_base = {state:?}\n",
        srv = server_bin(),
        state = format!("{root}/.remote-workspace-test"),
    )
}

struct McpSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    id: u64,
}

impl McpSession {
    /// Single-workspace session: workspace "test" serving `root`.
    fn spawn(root: &str) -> Self {
        let fleet = std::path::Path::new(root).join(".fleet.toml");
        std::fs::write(&fleet, fleet_entry("test", root)).unwrap();
        Self::spawn_fleet(&fleet)
    }

    fn spawn_fleet(fleet_path: &std::path::Path) -> Self {
        let srv = server_bin();
        assert!(
            std::path::Path::new(&srv).exists(),
            "server binary not found at {srv}"
        );
        let mcp = mcp_bin();
        assert!(
            std::path::Path::new(&mcp).exists(),
            "mcp binary not found at {mcp}"
        );
        let mut child = Command::new(&mcp)
            .args(["--fleet", &fleet_path.to_string_lossy()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
            id: 0,
        }
    }

    fn initialize(&mut self) {
        self.call(
            "initialize",
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1"},
            }),
        );
        self.notify("notifications/initialized", serde_json::json!({}));
    }

    fn call(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.id += 1;
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        self.read_response()
    }

    fn notify(&mut self, method: &str, params: serde_json::Value) {
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
    }

    fn read_response(&mut self) -> serde_json::Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read response");
        // Skip notifications (no "id" field).
        let v: serde_json::Value = serde_json::from_str(line.trim()).expect("parse response");
        if v.get("id").is_none() {
            return self.read_response();
        }
        v
    }

    fn tool(&mut self, name: &str, args: serde_json::Value) -> (bool, String) {
        let resp = self.call(
            "tools/call",
            serde_json::json!({"name": name, "arguments": args}),
        );
        let is_err = resp["result"]["isError"].as_bool().unwrap();
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        (is_err, text)
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mcp_initialize_and_server_info() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    let resp = s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    assert_eq!(resp["result"]["serverInfo"]["name"], "remote-workspace-mcp");
    assert_eq!(
        resp["result"]["instructions"],
        include_str!("../AGENT_GUIDANCE.md")
    );
    // Agent-facing conventions have one canonical source; the docs must point
    // at it instead of restating (and drifting from) it.
    for doc in [
        include_str!("../../../README.md"),
        include_str!("../../../docs/design.md"),
    ] {
        assert!(doc.contains("AGENT_GUIDANCE.md"));
    }
    s.notify("notifications/initialized", serde_json::json!({}));
}

#[test]
fn mcp_rejects_invalid_fleet_configs() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        ("empty", "".to_string()),
        (
            "duplicate",
            format!(
                "{}{}",
                fleet_entry("a", dir.path().to_str().unwrap()),
                fleet_entry("b", dir.path().to_str().unwrap())
            ),
        ),
        (
            "unknown-field",
            "[workspaces.x]\nroot = \"/tmp\"\nhostt = \"typo\"\n".into(),
        ),
    ];
    for (label, content) in cases {
        let fleet = dir.path().join(format!("{label}.toml"));
        std::fs::write(&fleet, content).unwrap();
        let out = Command::new(mcp_bin())
            .args(["--fleet", &fleet.to_string_lossy()])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "{label} fleet config must be rejected at startup"
        );
    }
}

// --check probes each workspace once and reports per-workspace health with
// stable error codes, exiting nonzero when any workspace is unhealthy.
#[test]
fn mcp_check_reports_per_workspace_health() {
    let good = tempfile::tempdir().unwrap();
    let fleet = good.path().join("check.toml");

    // All healthy: exit 0, every workspace reported ok.
    std::fs::write(&fleet, fleet_entry("good", good.path().to_str().unwrap())).unwrap();
    let out = Command::new(mcp_bin())
        .args(["--fleet", &fleet.to_string_lossy(), "--check"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "healthy fleet must pass: {stdout}");
    assert!(
        stdout.contains("good [") && stdout.contains(": ok"),
        "{stdout}"
    );

    // One workspace with a nonexistent root: the check still runs the healthy
    // one, reports the broken one with a stable code, and exits nonzero.
    std::fs::write(
        &fleet,
        format!(
            "{}{}",
            fleet_entry("good", good.path().to_str().unwrap()),
            fleet_entry("broken", "/nonexistent-remote-workspace-root")
        ),
    )
    .unwrap();
    let out = Command::new(mcp_bin())
        .args(["--fleet", &fleet.to_string_lossy(), "--check"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !out.status.success(),
        "broken workspace must fail the check"
    );
    assert!(
        stdout.contains("good [") && stdout.contains(": ok"),
        "{stdout}"
    );
    assert!(
        stdout.contains("broken [")
            && (stdout.contains("probe_failed") || stdout.contains("connect_failed")),
        "broken workspace must be reported with a stable code: {stdout}"
    );
}

#[test]
fn mcp_tools_list_has_expected_tools() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let resp = s.call("tools/list", serde_json::json!({}));
    let tools = resp["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    let expected = [
        "list_workspaces",
        "list_directory",
        "read_file",
        "create_file",
        "edit_file",
        "delete_file",
        "run_command",
        "upload_file",
        "download_file",
    ];
    for tool in expected {
        assert!(names.contains(&tool), "missing tool {tool}; have {names:?}");
    }
    assert_eq!(names.len(), expected.len(), "unexpected tools: {names:?}");

    // Every tool except list_workspaces requires the workspace argument.
    for t in tools {
        let name = t["name"].as_str().unwrap();
        let required: Vec<&str> = t["inputSchema"]["required"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        if name == "list_workspaces" {
            assert!(!required.contains(&"workspace"), "{name}");
        } else {
            assert!(
                required.contains(&"workspace"),
                "{name} must require workspace, requires {required:?}"
            );
        }

        // Every parameter must advertise one concrete type and a description.
        // An optional field left as `Option<T>` publishes `["integer","null"]`,
        // which some MCP hosts collapse to an untyped schema and then send the
        // value as a string -- the server rightly rejects it, and the tool
        // becomes unusable from that host. Optional params are omitted rather
        // than sent as null, so the non-nullable type is also the honest one.
        let props = t["inputSchema"]["properties"].as_object().unwrap();
        for (param, spec) in props {
            let ty = &spec["type"];
            assert!(
                ty.is_string(),
                "{name}.{param} must publish a single concrete type, got {ty}"
            );
            assert!(
                spec["description"].is_string(),
                "{name}.{param} has no description"
            );
        }
        // ... and an optional one must not have become required in the process.
        for opt in [
            "offset",
            "limit",
            "cwd",
            "profile",
            "timeout_ms",
            "replace_all",
            "overwrite",
        ] {
            assert!(
                !required.contains(&opt),
                "{name}.{opt} is optional but was marked required"
            );
        }
    }
}

// Optional parameters must work both ways round: omitted entirely, and passed
// as their declared JSON type.
#[test]
fn mcp_optional_parameters_are_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, _) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "a.txt", "content": "x\ny\n"}),
    );
    assert!(!e);

    // Omitted.
    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "a.txt"}),
    );
    assert!(!e, "read_file without offset/limit: {text}");
    let (e, text) = s.tool(
        "run_command",
        serde_json::json!({"workspace": "test", "argv": ["true"]}),
    );
    assert!(!e, "run_command without optionals: {text}");

    let (e, text) = s.tool(
        "run_command",
        serde_json::json!({"workspace": "test", "argv": ["pwd"], "cwd": ".", "timeout_ms": 30000}),
    );
    assert!(!e, "run_command with cwd/timeout_ms: {text}");

    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "a.txt", "offset": 2, "limit": 2}),
    );
    assert!(!e, "read_file with offset/limit: {text}");
    assert!(text.starts_with('y'), "offset/limit not applied: {text}");

    let (e, text) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "test", "path": ".", "offset": 0, "limit": 1}),
    );
    assert!(!e, "list_directory with offset/limit: {text}");
}

// Hosts that stringify scalars must not make the integer parameters unusable:
// the schema says `integer`, so "2" is unambiguous and is accepted. Anything
// that is not a number still fails.
#[test]
fn mcp_integer_parameters_accept_numeric_strings() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, _) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "a.txt", "content": "x\ny\n"}),
    );
    assert!(!e);

    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "a.txt", "offset": "2", "limit": "2"}),
    );
    assert!(!e, "read_file with stringified offset/limit: {text}");
    assert!(text.starts_with('y'), "offset/limit not applied: {text}");

    let (e, text) = s.tool(
        "run_command",
        serde_json::json!({"workspace": "test", "argv": ["true"], "timeout_ms": "30000"}),
    );
    assert!(!e, "run_command with stringified timeout_ms: {text}");

    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "a.txt", "limit": "soon"}),
    );
    assert!(e, "a non-numeric string must still be rejected: {text}");
}

#[test]
fn mcp_tool_call_success_is_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, text) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "test.txt", "content": "hello\n"}),
    );
    assert!(!e, "create should not be an error: {text}");
    assert!(text.contains("Created test.txt"), "unexpected text: {text}");
    assert!(
        text.contains("workspace 'test'"),
        "result must echo the workspace: {text}"
    );
}

#[test]
fn mcp_tool_call_failure_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "missing.txt"}),
    );
    assert!(e, "reading a missing file must be isError=true");
    assert!(
        text.contains("NotFound"),
        "error text should mention the error: {text}"
    );
}

#[test]
fn mcp_unknown_workspace_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, text) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "nope", "path": "."}),
    );
    assert!(e, "unknown workspace must be isError=true");
    assert!(
        text.contains("unknown_workspace") && text.contains("'nope'") && text.contains("test"),
        "error must carry the stable code, the bad name, and the available names: {text}"
    );
}

#[test]
fn mcp_run_command_returns_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, text) = s.tool(
        "run_command",
        serde_json::json!({"workspace": "test", "argv": ["echo", "hello-from-mcp"]}),
    );
    assert!(!e);
    assert!(text.contains("hello-from-mcp"), "stdout missing: {text}");
    let result: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(result["termination"]["kind"], "exited");
    assert_eq!(result["termination"]["code"], 0);
    assert_eq!(result["workspace"], "test");
}

// Two workspaces in one fleet are fully isolated: a path resolves, and is
// visible, only in the workspace it belongs to. (Operation ids and the log are
// isolated by construction: each root gets its own server and state directory.)
#[test]
fn mcp_workspaces_are_isolated() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let fleet = dir_a.path().join(".fleet.toml");
    std::fs::write(
        &fleet,
        format!(
            "{}label = \"Workspace A\"\n{}",
            fleet_entry("a", dir_a.path().to_str().unwrap()),
            fleet_entry("b", dir_b.path().to_str().unwrap())
        ),
    )
    .unwrap();
    let mut s = McpSession::spawn_fleet(&fleet);
    s.initialize();

    let (e, text) = s.tool("list_workspaces", serde_json::json!({}));
    assert!(!e);
    let rows: serde_json::Value = serde_json::from_str(&text).unwrap();
    let names: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
    assert_eq!(rows[0]["host"], "(local)");
    assert_eq!(rows[0]["root"], dir_a.path().to_str().unwrap());
    assert_eq!(rows[0]["label"], "Workspace A");
    assert!(
        rows[1].get("label").is_none(),
        "label must be omitted when unset: {}",
        rows[1]
    );

    let (e, _) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "a", "path": "only-in-a.txt", "content": "x"}),
    );
    assert!(!e);
    assert!(dir_a.path().join("only-in-a.txt").exists());
    assert!(!dir_b.path().join("only-in-a.txt").exists());

    let (e, _) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "b", "path": "only-in-a.txt"}),
    );
    assert!(e, "workspace b must not see a's file");

    let (e, text) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "b", "path": "."}),
    );
    assert!(!e);
    assert!(
        !text.contains("only-in-a.txt"),
        "b must not see a's file: {text}"
    );
    let (e, text) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "a", "path": "."}),
    );
    assert!(!e);
    assert!(text.contains("only-in-a.txt"), "a's listing: {text}");
}

// Drive edit_file and delete_file over real MCP stdio, plus the error paths an
// agent actually hits. Transfers have their own test.
#[test]
fn mcp_full_tool_surface() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.initialize();

    let (e, _) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "content": "l1\nl2\n"}),
    );
    assert!(!e);

    // Creating the same path again must be refused: existing files are only
    // modified through edit_file.
    let (e, text) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "content": "other"}),
    );
    assert!(e, "re-creating an existing file must be isError=true");
    assert!(text.contains("AlreadyExists"), "unexpected text: {text}");

    // read_file returns the hash edit_file needs as base_hash.
    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(!e, "read failed: {text}");
    let hash = text
        .split("[hash: ")
        .nth(1)
        .expect("read result must carry the hash")
        .trim_end_matches(']')
        .trim()
        .to_string();

    // A non-matching old_text is a structured NO_MATCH error.
    let (e, text) = s.tool(
        "edit_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "base_hash": hash,
            "edits": [{"old_text": "nope", "new_text": "x"}]}),
    );
    assert!(e, "no-match edit must be isError=true");
    assert!(text.contains("NoMatch"), "unexpected text: {text}");

    let (e, text) = s.tool(
        "edit_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "base_hash": hash,
            "edits": [{"old_text": "l2\n", "new_text": "L2\n"}]}),
    );
    assert!(!e, "edit failed: {text}");
    assert!(
        text.contains("operation_id="),
        "edit result must carry the operation id: {text}"
    );

    let (e, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(!e);
    assert!(text.starts_with("l1\nL2\n"), "edit not applied: {text}");

    // Several replacements in one call, applied in order and landing together.
    let hash_of = |s: &mut McpSession| -> String {
        s.tool(
            "read_file",
            serde_json::json!({"workspace": "test", "path": "f.txt"}),
        )
        .1
        .rsplit_once("[hash: ")
        .expect("read result must carry the hash")
        .1
        .trim_end_matches(']')
        .trim()
        .to_string()
    };
    let hash = hash_of(&mut s);
    let (e, text) = s.tool(
        "edit_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "base_hash": hash,
        "edits": [
            {"old_text": "l1\n", "new_text": "L1\n"},
            {"old_text": "L2\n", "new_text": "L2!\n"}
        ]}),
    );
    assert!(!e, "multi-edit failed: {text}");
    assert!(
        text.contains("2 replacements"),
        "result must say how many landed: {text}"
    );
    let (_, text) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(text.starts_with("L1\nL2!\n"), "edits not applied: {text}");

    // One bad replacement rejects the whole call, leaving the file as it was.
    // The base_hash is current, so this fails on the replacement itself rather
    // than on staleness.
    let hash = hash_of(&mut s);
    let (e, text) = s.tool(
        "edit_file",
        serde_json::json!({"workspace": "test", "path": "f.txt", "base_hash": hash,
        "edits": [
            {"old_text": "L1\n", "new_text": "x\n"},
            {"old_text": "absent", "new_text": "y"}
        ]}),
    );
    assert!(e, "a list with a bad replacement must fail");
    assert!(
        text.contains("edit 2 of 2"),
        "error must locate the failing replacement: {text}"
    );
    let (_, after) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(
        after.starts_with("L1\nL2!\n"),
        "rejected list must not partially apply: {after}"
    );

    // Delete, then verify it is gone.
    let (e, text) = s.tool(
        "delete_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(!e, "delete failed: {text}");
    assert!(!dir.path().join("f.txt").exists());
    let (e, _) = s.tool(
        "read_file",
        serde_json::json!({"workspace": "test", "path": "f.txt"}),
    );
    assert!(e, "reading a deleted file must be an error");

    // An invented base_hash must not be reported as a stale file: that would
    // send the agent to re-read and retry a call that fails identically.
    let (e, _) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "test", "path": "g.txt", "content": "x\n"}),
    );
    assert!(!e);
    let (e, text) = s.tool(
        "edit_file",
        serde_json::json!({"workspace": "test", "path": "g.txt", "base_hash": "auto",
            "edits": [{"old_text": "x", "new_text": "y"}]}),
    );
    assert!(e, "an invented base_hash must be an error");
    assert!(
        text.contains("not a hash") && !text.contains("Stale"),
        "unhelpful error for an invented base_hash: {text}"
    );
}

// If the connection to the server dies mid-session, the next tool call must
// transparently reconnect instead of failing forever with "server closed
// connection" (regression: flaky sshd resetting the connection).
#[test]
fn mcp_reconnects_after_server_death() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut s = McpSession::spawn(root);
    s.initialize();

    let (e, _) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "test", "path": "."}),
    );
    assert!(!e, "first call must succeed");

    // Kill the underlying server process. Anchored so it matches only the
    // server itself, not the MCP process whose argv also contains this text.
    let killed = std::process::Command::new("pkill")
        .args(["-f", &format!("^{} --root {root}", server_bin())])
        .status()
        .unwrap();
    assert!(killed.success(), "must find and kill the server process");
    // Give the MCP's reader a moment to observe the EOF.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let (e, text) = s.tool(
        "list_directory",
        serde_json::json!({"workspace": "test", "path": "."}),
    );
    assert!(!e, "call after server death must reconnect, got: {text}");
}

// A workspace added to the fleet file while the MCP is running becomes visible
// and usable on the next call, without restarting the process (the behaviour
// `remote-workspace workspace add` relies on).
#[test]
fn mcp_reloads_fleet_on_change() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let fleet = dir_a.path().join(".fleet.toml");
    std::fs::write(&fleet, fleet_entry("a", dir_a.path().to_str().unwrap())).unwrap();
    let mut s = McpSession::spawn_fleet(&fleet);
    s.initialize();

    let (e, text) = s.tool("list_workspaces", serde_json::json!({}));
    assert!(!e);
    let names = |t: &str| -> Vec<String> {
        serde_json::from_str::<serde_json::Value>(t)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["name"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(names(&text), vec!["a"]);

    // Append a second workspace to the fleet file (length change guarantees the
    // stamp differs even within one second).
    std::fs::write(
        &fleet,
        format!(
            "{}{}",
            fleet_entry("a", dir_a.path().to_str().unwrap()),
            fleet_entry("b", dir_b.path().to_str().unwrap())
        ),
    )
    .unwrap();

    let (e, text) = s.tool("list_workspaces", serde_json::json!({}));
    assert!(!e, "reload failed: {text}");
    assert_eq!(names(&text), vec!["a", "b"], "new workspace not visible");

    // The newly added workspace is immediately usable.
    let (e, text) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "b", "path": "in-b.txt", "content": "x"}),
    );
    assert!(!e, "new workspace unusable: {text}");
    assert!(dir_b.path().join("in-b.txt").exists());
}

// An invalid fleet edit is never partially applied. Every operation reports
// fleet_reload_failed while the file is broken -- reporting it only once and
// then quietly serving a stale fleet would hide a real misconfiguration -- and
// fixing the file recovers without restarting the process.
#[test]
fn mcp_invalid_fleet_reload_is_reported_until_fixed() {
    let dir_a = tempfile::tempdir().unwrap();
    let fleet = dir_a.path().join(".fleet.toml");
    let good = fleet_entry("a", dir_a.path().to_str().unwrap());
    std::fs::write(&fleet, &good).unwrap();
    let mut s = McpSession::spawn_fleet(&fleet);
    s.initialize();

    let (e, _) = s.tool("list_workspaces", serde_json::json!({}));
    assert!(!e);

    // Replace with an invalid fleet (declares no workspaces).
    std::fs::write(&fleet, "# broken\n").unwrap();

    // Every operation keeps reporting it, not just the first one.
    for attempt in 1..=2 {
        let (e, text) = s.tool("list_workspaces", serde_json::json!({}));
        assert!(e, "invalid reload must be an error (attempt {attempt})");
        assert!(
            text.contains("fleet_reload_failed"),
            "must carry the stable code: {text}"
        );
    }
    let (e, text) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "a", "path": "nope.txt", "content": "x"}),
    );
    assert!(e, "tool calls must not run against a broken fleet: {text}");
    assert!(!dir_a.path().join("nope.txt").exists());

    // Restoring a valid file recovers in place.
    std::fs::write(&fleet, format!("{good}label = \"restored\"\n")).unwrap();
    let (e, text) = s.tool("list_workspaces", serde_json::json!({}));
    assert!(!e, "valid fleet must load again: {text}");
    assert!(text.contains("restored"), "reloaded content: {text}");
    let (e, text) = s.tool(
        "create_file",
        serde_json::json!({"workspace": "a", "path": "works-again.txt", "content": "x"}),
    );
    assert!(!e, "workspace usable after recovery: {text}");
    assert!(dir_a.path().join("works-again.txt").exists());
}

// upload_file/download_file over real MCP stdio: success round-trip, default
// no-overwrite failure, and a failing upload all with correct isError.
#[test]
fn mcp_transfer_tools_roundtrip_and_errors() {
    let remote = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(remote.path().to_str().unwrap());
    s.initialize();

    // Binary content that read_file/create_file could not carry.
    let content: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x00, 0x42];
    let src = local.path().join("payload.bin");
    std::fs::write(&src, &content).unwrap();

    let (e, text) = s.tool(
        "upload_file",
        serde_json::json!({
            "workspace": "test",
            "local_path": src.to_str().unwrap(),
            "remote_path": "payload.bin",
        }),
    );
    assert!(!e, "upload failed: {text}");
    let up: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(up["direction"], "upload");
    assert_eq!(up["path"], "payload.bin");
    assert_eq!(up["size"], content.len());
    assert_eq!(up["workspace"], "test");
    assert!(up["operation_id"].as_str().unwrap().starts_with("op-"));
    assert!(up["sha256"].as_str().unwrap().starts_with("sha256:"));
    assert!(up["duration_ms"].is_u64());
    assert!(
        up.get("staging_path").is_none() && !text.contains(".part"),
        "staging path leaked into the tool result: {text}"
    );
    assert_eq!(
        std::fs::read(remote.path().join("payload.bin")).unwrap(),
        content
    );

    // Default no-overwrite refuses the existing remote target.
    let (e, text) = s.tool(
        "upload_file",
        serde_json::json!({
            "workspace": "test",
            "local_path": src.to_str().unwrap(),
            "remote_path": "payload.bin",
        }),
    );
    assert!(e, "re-upload without overwrite must be isError=true");
    assert!(text.contains("overwrite"), "unexpected text: {text}");

    let dest = local.path().join("payload-back.bin");
    let (e, text) = s.tool(
        "download_file",
        serde_json::json!({
            "workspace": "test",
            "remote_path": "payload.bin",
            "local_path": dest.to_str().unwrap(),
        }),
    );
    assert!(!e, "download failed: {text}");
    let down: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(down["direction"], "download");
    assert_eq!(down["path"], dest.to_str().unwrap());
    assert_eq!(down["sha256"], up["sha256"]);
    assert_eq!(down["workspace"], "test");
    assert_eq!(std::fs::read(&dest).unwrap(), content);

    // Missing local source is a tool error, not a crash.
    let (e, text) = s.tool(
        "upload_file",
        serde_json::json!({
            "workspace": "test",
            "local_path": local.path().join("missing.bin").to_str().unwrap(),
            "remote_path": "x.bin",
        }),
    );
    assert!(e, "missing local source must be isError=true, got: {text}");
}
