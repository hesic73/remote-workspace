use std::future::Future;
use std::path::PathBuf;

use agent_remote_mcp::RemoteWorkspaceServer;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rmcp::model::{ClientJsonRpcMessage, ClientRequest, ErrorCode, ErrorData, JsonRpcMessage};
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::async_rw::AsyncRwTransport;
use rmcp::transport::Transport;
use rmcp::{RoleServer, ServiceExt};
use tracing_subscriber::EnvFilter;

/// Answers requests with unknown methods with JSON-RPC -32601 instead of
/// letting them reach rmcp, whose pre-initialize handshake drops the
/// connection on anything but ping/initialize. Some hosts (Antigravity CLI)
/// probe with a nonstandard request such as `server/discover` before
/// `initialize` and fall back to standard MCP only if it is answered.
struct RejectUnknownMethods<T>(T);

impl<T: Transport<RoleServer>> Transport<RoleServer> for RejectUnknownMethods<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.0.send(item)
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        loop {
            let msg = self.0.receive().await?;
            let ClientJsonRpcMessage::Request(req) = &msg else {
                return Some(msg);
            };
            let ClientRequest::CustomRequest(custom) = &req.request else {
                return Some(msg);
            };
            let error = ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                format!("method not found: {}", custom.method),
                None,
            );
            let reply = JsonRpcMessage::error(error, Some(req.id.clone()));
            self.0.send(reply).await.ok()?;
        }
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.0.close()
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote-mcp",
    version,
    about = "MCP server exposing named agent-remote workspaces"
)]
struct Cli {
    /// Path to the fleet config (TOML) declaring the workspaces. Defaults to
    /// ~/.agent-remote/workspaces.toml.
    #[arg(long)]
    fleet: Option<PathBuf>,

    /// Diagnostic mode: validate the fleet config, probe every workspace once
    /// (spawn its server, one real round-trip), report per-workspace status,
    /// and exit nonzero if any workspace is unhealthy.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    let fleet_path = match cli.fleet {
        Some(p) => p,
        None => {
            let home =
                std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set; pass --fleet"))?;
            PathBuf::from(home).join(".agent-remote/workspaces.toml")
        }
    };
    if cli.check {
        let text = std::fs::read_to_string(&fleet_path).with_context(|| {
            format!("read fleet config {fleet_path:?}; create it or pass --fleet")
        })?;
        let fleet = agent_remote_mcp::parse_fleet(&text)
            .with_context(|| format!("invalid fleet config {fleet_path:?}"))?;
        println!(
            "fleet config {} ok: {} workspace(s)",
            fleet_path.display(),
            fleet.len()
        );
        let mut unhealthy = 0;
        for (name, ws) in &fleet {
            let location = match &ws.endpoint {
                agent_remote_client::Endpoint::Ssh { host, root, .. } => format!("{host}:{root}"),
                agent_remote_client::Endpoint::Local { root, .. } => format!("(local):{root}"),
            };
            match agent_remote_mcp::check_workspace(&ws.endpoint).await {
                Ok(()) => println!("{name} [{location}]: ok"),
                Err(e) => {
                    unhealthy += 1;
                    println!("{name} [{location}]: {e}");
                }
            }
        }
        if unhealthy > 0 {
            anyhow::bail!("{unhealthy} workspace(s) unhealthy");
        }
        return Ok(());
    }

    // No eager connect: initialize must answer immediately (a blocking retry
    // loop here makes the MCP host time the server out, e.g. when a session
    // being resumed briefly overlaps its predecessor on the same state lock).
    // The first tool call to each workspace connects on demand.
    let server = RemoteWorkspaceServer::load(fleet_path)
        .context("fleet config unusable; create it or pass --fleet")?;
    let service = server
        .serve(RejectUnknownMethods(
            AsyncRwTransport::<RoleServer, _, _>::new(tokio::io::stdin(), tokio::io::stdout()),
        ))
        .await
        .context("failed to start MCP server")?;
    service.waiting().await?;
    Ok(())
}
