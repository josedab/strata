//! Strata server binary.

use strata::config::StrataConfig;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "strata-server")]
#[command(about = "Strata distributed file system server")]
struct Args {
    /// Node ID
    #[arg(short, long, env = "STRATA_NODE_ID", default_value = "1")]
    node_id: u64,

    /// Configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Data directory
    #[arg(long, default_value = "/var/lib/strata")]
    data_dir: PathBuf,

    /// Metadata bind address
    #[arg(long, default_value = "0.0.0.0:9000")]
    metadata_addr: String,

    /// Data bind address
    #[arg(long, default_value = "0.0.0.0:9001")]
    data_addr: String,

    /// S3 gateway bind address
    #[arg(long, default_value = "0.0.0.0:9002")]
    s3_addr: String,

    /// Raft peers (format: id=addr,id=addr)
    #[arg(long)]
    peers: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load or create configuration
    let mut config = if let Some(config_path) = args.config {
        StrataConfig::from_file(&config_path)?
    } else {
        StrataConfig::development()
    };

    // Override with CLI args
    config.node.id = args.node_id;
    config.metadata.bind_addr = args.metadata_addr.parse()?;
    config.data.bind_addr = args.data_addr.parse()?;
    config.s3.bind_addr = args.s3_addr.parse()?;
    config.storage.data_dir = args.data_dir.join("data");
    config.storage.metadata_dir = args.data_dir.join("metadata");

    if let Some(peers_str) = args.peers {
        config.metadata.raft_peers = peers_str
            .split(',')
            .map(|s| s.to_string())
            .collect();
    }

    // Run the server
    strata::run(config).await?;

    Ok(())
}
