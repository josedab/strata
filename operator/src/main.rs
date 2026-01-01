//! Strata Kubernetes Operator
//!
//! Manages Strata distributed filesystem clusters on Kubernetes.

use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod controller;
mod crd;
mod error;
mod metrics;
mod reconciler;

use controller::Controller;

#[derive(Parser, Debug)]
#[command(name = "strata-operator")]
#[command(about = "Kubernetes Operator for Strata distributed filesystem")]
struct Args {
    /// Namespace to watch (empty for all namespaces)
    #[arg(short, long, default_value = "")]
    namespace: String,

    /// Metrics server port
    #[arg(short, long, default_value = "8080")]
    metrics_port: u16,

    /// Health check port
    #[arg(short = 'H', long, default_value = "8081")]
    health_port: u16,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Enable leader election
    #[arg(long, default_value = "true")]
    leader_election: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    let level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(true)
        .json()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Strata Kubernetes Operator");
    info!(namespace = %args.namespace, "Watching namespace");

    // Start metrics server
    let metrics_handle = tokio::spawn(metrics::run_metrics_server(args.metrics_port));

    // Start health server
    let health_handle = tokio::spawn(run_health_server(args.health_port));

    // Create and run controller
    let controller = Controller::new(args.namespace, args.leader_election).await?;
    controller.run().await?;

    // Wait for background tasks
    let _ = metrics_handle.await;
    let _ = health_handle.await;

    Ok(())
}

async fn run_health_server(port: u16) {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    info!(port = %port, "Health server started");

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;

                let response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nOK";
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    }
}
