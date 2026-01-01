//! Cluster management commands

use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;
use tabled::Tabled;

use crate::client::{ClusterHealth, ClusterStats, NodeInfo, StrataClient};
use crate::output::{self, OutputFormat};

#[derive(Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    command: ClusterCommands,
}

#[derive(Subcommand)]
enum ClusterCommands {
    /// Show cluster health status
    Health,

    /// Show cluster statistics
    Stats,

    /// List cluster nodes
    Nodes,

    /// Show node details
    Node {
        /// Node ID
        id: String,
    },

    /// Drain a node
    Drain {
        /// Node ID
        id: String,

        /// Skip confirmation
        #[arg(short, long)]
        force: bool,
    },

    /// Resume a drained node
    Resume {
        /// Node ID
        id: String,
    },

    /// Show cluster configuration
    Config,

    /// Show Raft status
    Raft,
}

#[derive(Debug, Serialize, Tabled)]
struct NodeRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Role")]
    role: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "CPU")]
    cpu: String,
    #[tabled(rename = "Memory")]
    memory: String,
    #[tabled(rename = "Disk")]
    disk: String,
    #[tabled(rename = "Uptime")]
    uptime: String,
}

impl From<NodeInfo> for NodeRow {
    fn from(n: NodeInfo) -> Self {
        Self {
            id: n.id[..8.min(n.id.len())].to_string(),
            name: n.name,
            role: n.role,
            status: output::status_indicator(&n.status),
            cpu: output::format_percent(n.cpu_usage),
            memory: output::format_percent(n.memory_usage),
            disk: output::format_percent(n.disk_usage),
            uptime: output::format_duration(n.uptime),
        }
    }
}

pub async fn execute(args: ClusterArgs, client: &StrataClient, format: OutputFormat) -> Result<()> {
    match args.command {
        ClusterCommands::Health => {
            let health: ClusterHealth = client.get("/cluster/health").await?;

            output::print_header("Cluster Health");
            output::print_kv("Status", output::status_indicator(&health.status));
            output::print_kv("Nodes", format!("{}/{} healthy", health.healthy_nodes, health.nodes));
            output::print_kv("Uptime", output::format_duration(health.uptime));
            output::print_kv("Version", &health.version);
        }

        ClusterCommands::Stats => {
            let stats: ClusterStats = client.get("/cluster/stats").await?;

            output::print_header("Cluster Statistics");
            output::print_kv("Total Buckets", stats.total_buckets.to_string());
            output::print_kv("Total Objects", format_number(stats.total_objects));
            output::print_kv("Total Size", output::format_bytes(stats.total_size_bytes));
            output::print_kv(
                "Capacity",
                format!(
                    "{} / {} ({:.1}%)",
                    output::format_bytes(stats.used_capacity_bytes),
                    output::format_bytes(stats.total_capacity_bytes),
                    (stats.used_capacity_bytes as f64 / stats.total_capacity_bytes as f64) * 100.0
                ),
            );
            output::print_kv("Requests/sec", format!("{:.1}", stats.requests_per_second));
        }

        ClusterCommands::Nodes => {
            let nodes: Vec<NodeInfo> = client.get("/nodes").await?;
            let rows: Vec<NodeRow> = nodes.into_iter().map(Into::into).collect();
            output::print_output(&rows, format);
        }

        ClusterCommands::Node { id } => {
            let node: NodeInfo = client.get(&format!("/nodes/{}", id)).await?;

            output::print_header(&format!("Node: {}", node.name));
            output::print_kv("ID", &node.id);
            output::print_kv("Address", &node.address);
            output::print_kv("Role", &node.role);
            output::print_kv("Status", output::status_indicator(&node.status));
            output::print_kv("Version", &node.version);
            output::print_kv("Uptime", output::format_duration(node.uptime));

            output::print_header("Resources");
            output::print_kv("CPU Usage", output::format_percent(node.cpu_usage));
            output::print_kv("Memory Usage", output::format_percent(node.memory_usage));
            output::print_kv("Disk Usage", output::format_percent(node.disk_usage));
        }

        ClusterCommands::Drain { id, force } => {
            if !force && !output::confirm(&format!("Drain node '{}'? This will migrate all data.", id)) {
                output::info("Cancelled");
                return Ok(());
            }

            let spinner = output::create_spinner(format!("Draining node {}...", id));
            client.post_empty(&format!("/nodes/{}/drain", id), &()).await?;
            spinner.finish_and_clear();

            output::success(format!("Node {} is now draining", id));
        }

        ClusterCommands::Resume { id } => {
            client.post_empty(&format!("/nodes/{}/resume", id), &()).await?;
            output::success(format!("Node {} resumed", id));
        }

        ClusterCommands::Config => {
            #[derive(Debug, serde::Deserialize, Serialize)]
            struct ClusterConfig {
                replication_factor: u32,
                erasure_coding: bool,
                chunk_size_bytes: u64,
                encryption_enabled: bool,
                compression_enabled: bool,
            }

            let config: ClusterConfig = client.get("/cluster/config").await?;

            output::print_header("Cluster Configuration");
            output::print_kv("Replication Factor", config.replication_factor.to_string());
            output::print_kv("Erasure Coding", if config.erasure_coding { "Enabled" } else { "Disabled" });
            output::print_kv("Chunk Size", output::format_bytes(config.chunk_size_bytes));
            output::print_kv("Encryption", if config.encryption_enabled { "Enabled" } else { "Disabled" });
            output::print_kv("Compression", if config.compression_enabled { "Enabled" } else { "Disabled" });
        }

        ClusterCommands::Raft => {
            #[derive(Debug, serde::Deserialize, Serialize)]
            struct RaftStatus {
                state: String,
                term: u64,
                leader_id: Option<String>,
                commit_index: u64,
                last_applied: u64,
                peers: Vec<String>,
            }

            let status: RaftStatus = client.get("/cluster/raft").await?;

            output::print_header("Raft Consensus Status");
            output::print_kv("State", output::status_indicator(&status.state));
            output::print_kv("Term", status.term.to_string());
            output::print_kv("Leader", status.leader_id.as_deref().unwrap_or("(none)"));
            output::print_kv("Commit Index", status.commit_index.to_string());
            output::print_kv("Last Applied", status.last_applied.to_string());
            output::print_kv("Peers", status.peers.join(", "));
        }
    }

    Ok(())
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
