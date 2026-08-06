#![cfg(windows)]

use remote_workspace_client::{
    download_file, upload_file, ArgvTransport, Client, Endpoint, RemoteShell,
};

#[tokio::test]
#[ignore = "requires a reachable SSH host"]
async fn windows_client_round_trips_binary_over_ssh() {
    let host = std::env::var("REMOTE_WORKSPACE_TEST_HOST").unwrap();
    let root = std::env::var("REMOTE_WORKSPACE_TEST_ROOT").unwrap();
    let remote_bin = std::env::var("REMOTE_WORKSPACE_TEST_BIN").unwrap();
    let remote_shell = match std::env::var("REMOTE_WORKSPACE_TEST_SHELL")
        .unwrap_or_else(|_| "posix".into())
        .as_str()
    {
        "posix" => RemoteShell::Posix,
        "powershell" => RemoteShell::Powershell,
        other => panic!("REMOTE_WORKSPACE_TEST_SHELL must be posix or powershell, got {other:?}"),
    };
    let remote_path = format!("windows-roundtrip-{}.bin", std::process::id());
    let endpoint = Endpoint::Ssh {
        host,
        remote_shell,
        remote_bin,
        root,
        state_base: None,
        config: None,
    };
    let client = Client::connect(
        ArgvTransport {
            argv: endpoint.control_argv_with_idle_timeout(20),
        },
        None,
    )
    .await
    .unwrap();

    let local_dir = tempfile::tempdir().unwrap();
    let source = local_dir.path().join("source.bin");
    let destination = local_dir.path().join("roundtrip.bin");
    let source_bytes: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
    tokio::fs::write(&source, &source_bytes).await.unwrap();

    let upload = upload_file(&client, &endpoint, &source, &remote_path, false)
        .await
        .unwrap();
    let download = download_file(&client, &endpoint, &remote_path, &destination, false)
        .await
        .unwrap();

    let destination_bytes = tokio::fs::read(&destination).await.unwrap();
    assert_eq!(source_bytes, destination_bytes);
    assert_eq!(upload.sha256, download.sha256);
    assert_eq!(upload.size, download.size);

    client.delete(&remote_path).await.unwrap();
    client
        .close_with_grace(std::time::Duration::from_secs(2))
        .await;
}
