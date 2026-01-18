//! Cluster setup example for Strata.
//!
//! This example demonstrates how to configure and initialize
//! a multi-node Strata cluster with Raft consensus.
//!
//! # Running
//!
//! ```bash
//! cargo run --example cluster_setup
//! ```

fn main() {
    println!("Strata Cluster Setup Example");
    println!("=============================\n");

    // Example: Define cluster nodes
    println!("1. Define cluster nodes:");
    println!("   let mut nodes = HashMap::new();");
    println!("   nodes.insert(1, \"192.168.1.10:8080\".parse().unwrap());");
    println!("   nodes.insert(2, \"192.168.1.11:8080\".parse().unwrap());");
    println!("   nodes.insert(3, \"192.168.1.12:8080\".parse().unwrap());");
    println!();

    // Example: Configure Raft consensus
    println!("2. Configure Raft consensus:");
    println!("   let raft_config = RaftConfig {{");
    println!("       election_timeout_min: Duration::from_millis(150),");
    println!("       election_timeout_max: Duration::from_millis(300),");
    println!("       heartbeat_interval: Duration::from_millis(50),");
    println!("       max_entries_per_request: 100,");
    println!("   }};");
    println!();

    // Example: Configure erasure coding
    println!("3. Configure erasure coding:");
    println!("   let erasure_config = ErasureConfig {{");
    println!("       data_shards: 4,      // Number of data shards");
    println!("       parity_shards: 2,    // Number of parity shards");
    println!("       // Can tolerate loss of up to 2 shards");
    println!("   }};");
    println!();

    // Example: Configure placement strategy
    println!("4. Configure placement strategy:");
    println!("   let placement = PlacementStrategy::RackAware {{");
    println!("       min_racks: 2,        // Spread across at least 2 racks");
    println!("       replication_factor: 3,");
    println!("   }};");
    println!();

    // Example: Initialize cluster
    println!("5. Initialize cluster:");
    println!("   let cluster = ClusterBuilder::new()");
    println!("       .nodes(nodes)");
    println!("       .raft_config(raft_config)");
    println!("       .erasure_config(erasure_config)");
    println!("       .placement_strategy(placement)");
    println!("       .build()");
    println!("       .await?;");
    println!();

    // Example: Wait for leader election
    println!("6. Wait for leader election:");
    println!("   let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;");
    println!("   println!(\"Leader elected: node {{}}\", leader);");
    println!();

    // Example: Health check all nodes
    println!("7. Health check all nodes:");
    println!("   for (node_id, status) in cluster.health_check().await? {{");
    println!("       println!(\"Node {{}}: {{}}\", node_id, status);");
    println!("   }}");
    println!();

    println!("For production deployments, see docs/deployment.md for");
    println!("detailed configuration options and best practices.");
}

// Uncomment for actual usage:
/*
use strata::cluster::{ClusterBuilder, PlacementStrategy};
use strata::raft::RaftConfig;
use strata::erasure::ErasureConfig;
use strata::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mut nodes = HashMap::new();
    nodes.insert(1, "192.168.1.10:8080".parse().unwrap());
    nodes.insert(2, "192.168.1.11:8080".parse().unwrap());
    nodes.insert(3, "192.168.1.12:8080".parse().unwrap());

    let raft_config = RaftConfig {
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        max_entries_per_request: 100,
    };

    let erasure_config = ErasureConfig {
        data_shards: 4,
        parity_shards: 2,
    };

    let cluster = ClusterBuilder::new()
        .nodes(nodes)
        .raft_config(raft_config)
        .erasure_config(erasure_config)
        .placement_strategy(PlacementStrategy::RackAware {
            min_racks: 2,
            replication_factor: 3,
        })
        .build()
        .await?;

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;
    println!("Leader elected: node {}", leader);

    for (node_id, status) in cluster.health_check().await? {
        println!("Node {}: {}", node_id, status);
    }

    Ok(())
}
*/
