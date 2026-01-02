//! Cluster integration tests
//!
//! Tests cluster management, failure scenarios, and recovery.

#[allow(dead_code)]
mod common;

use common::cluster_sim::{ClusterBuilder, FailureScenario, NodeRole, SimulatedCluster};
use std::time::Duration;

// =============================================================================
// Cluster Setup Tests
// =============================================================================

#[tokio::test]
async fn test_cluster_creation() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(5)
        .build()
        .await;

    let metadata = cluster.nodes_by_role(NodeRole::Metadata).await;
    let data = cluster.nodes_by_role(NodeRole::Data).await;

    assert_eq!(metadata.len(), 3);
    assert_eq!(data.len(), 5);
}

#[tokio::test]
async fn test_cluster_startup() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    assert_eq!(cluster.healthy_node_count().await, 0);

    cluster.start_all().await.unwrap();

    assert_eq!(cluster.healthy_node_count().await, 4);
}

#[tokio::test]
async fn test_cluster_shutdown() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    assert_eq!(cluster.healthy_node_count().await, 4);

    cluster.stop_all().await.unwrap();
    assert_eq!(cluster.healthy_node_count().await, 0);
}

// =============================================================================
// Single Node Failure Tests
// =============================================================================

#[tokio::test]
async fn test_single_data_node_failure() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let data_nodes = cluster.nodes_by_role(NodeRole::Data).await;

    // Fail one data node
    cluster.fail_node(data_nodes[0]).await;

    // Cluster should still be operational with 2 data nodes
    assert_eq!(cluster.healthy_node_count().await, 3);

    // With 2 remaining data nodes, should still support read/write
    let remaining_data = cluster.nodes_by_role(NodeRole::Data).await;
    let healthy_data: Vec<_> = remaining_data
        .iter()
        .filter(|id| futures::executor::block_on(async {
            cluster.node_addr(**id).await.is_some()
        }))
        .collect();

    assert!(healthy_data.len() >= 2);
}

#[tokio::test]
async fn test_single_metadata_node_failure() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let metadata_nodes = cluster.nodes_by_role(NodeRole::Metadata).await;

    // Fail one metadata node
    cluster.fail_node(metadata_nodes[0]).await;

    // Should still have quorum with 2/3 metadata nodes
    assert_eq!(cluster.healthy_node_count().await, 5);
}

// =============================================================================
// Multiple Node Failure Tests
// =============================================================================

#[tokio::test]
async fn test_minority_metadata_failure() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(5)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let metadata_nodes = cluster.nodes_by_role(NodeRole::Metadata).await;

    // Fail 2 of 5 metadata nodes (minority)
    cluster.fail_node(metadata_nodes[0]).await;
    cluster.fail_node(metadata_nodes[1]).await;

    // Should still have quorum with 3/5 metadata nodes
    assert_eq!(cluster.healthy_node_count().await, 6);
}

#[tokio::test]
async fn test_majority_metadata_failure() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let metadata_nodes = cluster.nodes_by_role(NodeRole::Metadata).await;

    // Fail 2 of 3 metadata nodes (majority)
    cluster.fail_node(metadata_nodes[0]).await;
    cluster.fail_node(metadata_nodes[1]).await;

    // No quorum - only 4 healthy nodes (1 metadata + 3 data)
    assert_eq!(cluster.healthy_node_count().await, 4);
}

// =============================================================================
// Network Partition Tests
// =============================================================================

#[tokio::test]
async fn test_network_partition_data_nodes() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(4)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let data_nodes = cluster.nodes_by_role(NodeRole::Data).await;

    // Partition data nodes into two groups
    let group_a = vec![data_nodes[0], data_nodes[1]];
    let group_b = vec![data_nodes[2], data_nodes[3]];

    cluster.partition(group_a, group_b).await;

    // All nodes still healthy (network partition doesn't affect health)
    assert_eq!(cluster.healthy_node_count().await, 5);

    // Heal partition
    cluster.heal_partition().await;
}

#[tokio::test]
async fn test_split_brain_scenario() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(4)
        .build()
        .await;

    cluster.start_all().await.unwrap();

    let metadata_nodes = cluster.nodes_by_role(NodeRole::Metadata).await;
    let data_nodes = cluster.nodes_by_role(NodeRole::Data).await;

    // Create split brain: separate metadata nodes
    let group_a = vec![metadata_nodes[0], data_nodes[0], data_nodes[1]];
    let group_b = vec![metadata_nodes[1], metadata_nodes[2], data_nodes[2], data_nodes[3]];

    cluster.partition(group_a, group_b).await;

    // All nodes still running
    assert_eq!(cluster.healthy_node_count().await, 7);

    // In real scenario, only group_b would have quorum (2/3 metadata)
    // group_a would enter read-only mode

    cluster.heal_partition().await;
}

// =============================================================================
// Recovery Tests
// =============================================================================

#[tokio::test]
async fn test_node_restart() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let data_nodes = cluster.nodes_by_role(NodeRole::Data).await;

    // Stop and restart a node
    cluster.stop_node(data_nodes[0]).await.unwrap();
    assert_eq!(cluster.healthy_node_count().await, 3);

    cluster.start_node(data_nodes[0]).await.unwrap();
    assert_eq!(cluster.healthy_node_count().await, 4);
}

#[tokio::test]
async fn test_rolling_restart() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(5)
        .build()
        .await;

    cluster.start_all().await.unwrap();

    let all_nodes: Vec<u64> = {
        let metadata = cluster.nodes_by_role(NodeRole::Metadata).await;
        let data = cluster.nodes_by_role(NodeRole::Data).await;
        metadata.into_iter().chain(data.into_iter()).collect()
    };

    // Rolling restart all nodes
    for node_id in all_nodes {
        cluster.stop_node(node_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        cluster.start_node(node_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cluster should remain partially operational
        assert!(cluster.healthy_node_count().await > 0);
    }

    // All nodes back
    assert_eq!(cluster.healthy_node_count().await, 8);
}

// =============================================================================
// Failure Scenario Tests
// =============================================================================

#[tokio::test]
async fn test_cascading_failure_scenario() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(5)
        .build()
        .await;

    cluster.start_all().await.unwrap();
    let data_nodes = cluster.nodes_by_role(NodeRole::Data).await;

    // Define cascading failure scenario
    let scenario = FailureScenario::new(
        "cascading_failure",
        "Sequential node failures with delays",
    )
    .fail_node(data_nodes[0])
    .wait(Duration::from_millis(100))
    .fail_node(data_nodes[1])
    .wait(Duration::from_millis(100))
    .fail_node(data_nodes[2]);

    scenario.execute(&cluster).await.unwrap();

    // 3 data nodes failed, 2 remaining + 1 metadata
    assert_eq!(cluster.healthy_node_count().await, 3);
}

#[tokio::test]
async fn test_partition_then_heal() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(4)
        .build()
        .await;

    cluster.start_all().await.unwrap();

    let metadata = cluster.nodes_by_role(NodeRole::Metadata).await;
    let data = cluster.nodes_by_role(NodeRole::Data).await;

    let scenario = FailureScenario::new("partition_heal", "Network partition and recovery")
        .partition(vec![metadata[0]], vec![metadata[1], metadata[2]])
        .wait(Duration::from_millis(500))
        .heal()
        .wait(Duration::from_millis(100));

    scenario.execute(&cluster).await.unwrap();

    // All nodes should be healthy after healing
    assert_eq!(cluster.healthy_node_count().await, 7);
}

// =============================================================================
// Scaling Tests
// =============================================================================

#[tokio::test]
async fn test_add_node_to_running_cluster() {
    let mut cluster = SimulatedCluster::new("scaling-test");

    // Start with minimal cluster
    let meta_id = cluster.add_node(NodeRole::Metadata).await;
    let data_id1 = cluster.add_node(NodeRole::Data).await;
    let data_id2 = cluster.add_node(NodeRole::Data).await;

    cluster.start_node(meta_id).await.unwrap();
    cluster.start_node(data_id1).await.unwrap();
    cluster.start_node(data_id2).await.unwrap();

    assert_eq!(cluster.healthy_node_count().await, 3);

    // Add new data node to running cluster
    let data_id3 = cluster.add_node(NodeRole::Data).await;
    cluster.start_node(data_id3).await.unwrap();

    assert_eq!(cluster.healthy_node_count().await, 4);
}

#[tokio::test]
async fn test_scale_up_data_nodes() {
    let mut cluster = SimulatedCluster::new("scale-up-test");

    let meta_id = cluster.add_node(NodeRole::Metadata).await;
    cluster.start_node(meta_id).await.unwrap();

    // Scale up from 0 to 5 data nodes
    for i in 0..5 {
        let data_id = cluster.add_node(NodeRole::Data).await;
        cluster.start_node(data_id).await.unwrap();
        assert_eq!(cluster.healthy_node_count().await, 2 + i);
    }
}

// =============================================================================
// Wait and Timeout Tests
// =============================================================================

#[tokio::test]
async fn test_wait_for_healthy() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    // Spawn task to start nodes after delay
    let cluster_clone = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = cluster_clone.start_all().await;
    });

    // Wait for cluster to become healthy
    let healthy = cluster
        .wait_for_healthy(4, Duration::from_secs(2))
        .await;

    assert!(healthy);
    assert_eq!(cluster.healthy_node_count().await, 4);
}

#[tokio::test]
async fn test_wait_timeout() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(1)
        .data_nodes(3)
        .build()
        .await;

    // Don't start any nodes - wait should timeout
    let healthy = cluster
        .wait_for_healthy(4, Duration::from_millis(100))
        .await;

    assert!(!healthy);
}

// =============================================================================
// Node Address Tests
// =============================================================================

#[tokio::test]
async fn test_node_addresses_unique() {
    let cluster = ClusterBuilder::new("test-cluster")
        .metadata_nodes(3)
        .data_nodes(5)
        .build()
        .await;

    let metadata = cluster.nodes_by_role(NodeRole::Metadata).await;
    let data = cluster.nodes_by_role(NodeRole::Data).await;

    let all_nodes: Vec<_> = metadata.iter().chain(data.iter()).collect();
    let mut addresses = std::collections::HashSet::new();

    for node_id in all_nodes {
        let addr = cluster.node_addr(*node_id).await;
        assert!(addr.is_some());
        addresses.insert(addr.unwrap());
    }

    // All addresses should be unique
    assert_eq!(addresses.len(), 8);
}

// Clone impl for SimulatedCluster (minimal version for tests)
impl Clone for SimulatedCluster {
    fn clone(&self) -> Self {
        // This is a simplified clone that shares the underlying data
        // In real implementation, this would be more sophisticated
        Self {
            name: self.name.clone(),
            temp_dir: tempfile::TempDir::new().unwrap(),
            nodes: self.nodes.clone(),
            network: self.network.clone(),
            next_node_id: 100, // Different starting point
            next_port: 20000,
        }
    }
}
