// Cluster simulation for integration tests
// Simulates multi-node cluster scenarios for testing

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{broadcast, RwLock};

/// Simulated node state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Starting,
    Running,
    Degraded,
    Stopped,
    Failed,
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Metadata,
    Data,
    Combined,
}

/// Simulated cluster node
pub struct SimulatedNode {
    pub id: u64,
    pub role: NodeRole,
    pub state: NodeState,
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl SimulatedNode {
    pub fn new(id: u64, role: NodeRole, addr: SocketAddr, data_dir: PathBuf) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            id,
            role,
            state: NodeState::Stopped,
            addr,
            data_dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub async fn start(&mut self) -> crate::common::Result<()> {
        self.state = NodeState::Starting;
        // Simulate startup delay
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.state = NodeState::Running;
        Ok(())
    }

    pub async fn stop(&mut self) -> crate::common::Result<()> {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
        self.state = NodeState::Stopped;
        Ok(())
    }

    pub async fn fail(&mut self) {
        self.state = NodeState::Failed;
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
    }

    pub fn shutdown_receiver(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.as_ref().unwrap().subscribe()
    }

    pub fn is_healthy(&self) -> bool {
        self.state == NodeState::Running
    }
}

/// Network partition simulation
pub struct NetworkPartition {
    /// Nodes that cannot communicate with each other
    partitioned: HashMap<u64, Vec<u64>>,
    /// Simulated latency between nodes (node_id -> (target_id, latency_ms))
    latencies: HashMap<u64, HashMap<u64, Duration>>,
    /// Packet loss rates between nodes (0.0 - 1.0)
    packet_loss: HashMap<u64, HashMap<u64, f64>>,
}

impl NetworkPartition {
    pub fn new() -> Self {
        Self {
            partitioned: HashMap::new(),
            latencies: HashMap::new(),
            packet_loss: HashMap::new(),
        }
    }

    /// Creates a partition between two groups of nodes
    pub fn partition(&mut self, group_a: Vec<u64>, group_b: Vec<u64>) {
        for &a in &group_a {
            self.partitioned
                .entry(a)
                .or_insert_with(Vec::new)
                .extend(group_b.iter());
        }
        for &b in &group_b {
            self.partitioned
                .entry(b)
                .or_insert_with(Vec::new)
                .extend(group_a.iter());
        }
    }

    /// Heals a network partition
    pub fn heal(&mut self) {
        self.partitioned.clear();
    }

    /// Sets latency between two nodes
    pub fn set_latency(&mut self, from: u64, to: u64, latency: Duration) {
        self.latencies
            .entry(from)
            .or_insert_with(HashMap::new)
            .insert(to, latency);
    }

    /// Sets packet loss rate between two nodes
    pub fn set_packet_loss(&mut self, from: u64, to: u64, rate: f64) {
        self.packet_loss
            .entry(from)
            .or_insert_with(HashMap::new)
            .insert(to, rate.clamp(0.0, 1.0));
    }

    /// Checks if two nodes can communicate
    pub fn can_communicate(&self, from: u64, to: u64) -> bool {
        if let Some(blocked) = self.partitioned.get(&from) {
            if blocked.contains(&to) {
                return false;
            }
        }
        true
    }

    /// Gets the latency between two nodes
    pub fn get_latency(&self, from: u64, to: u64) -> Duration {
        self.latencies
            .get(&from)
            .and_then(|m| m.get(&to))
            .copied()
            .unwrap_or(Duration::from_millis(1))
    }
}

impl Default for NetworkPartition {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulated cluster for testing
pub struct SimulatedCluster {
    pub name: String,
    pub temp_dir: TempDir,
    pub nodes: Arc<RwLock<HashMap<u64, SimulatedNode>>>,
    pub network: Arc<RwLock<NetworkPartition>>,
    pub next_node_id: u64,
    pub next_port: u16,
}

impl SimulatedCluster {
    pub fn new(name: impl Into<String>) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        Self {
            name: name.into(),
            temp_dir,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            network: Arc::new(RwLock::new(NetworkPartition::new())),
            next_node_id: 1,
            next_port: 10000,
        }
    }

    /// Adds a new node to the cluster
    pub async fn add_node(&mut self, role: NodeRole) -> u64 {
        let id = self.next_node_id;
        self.next_node_id += 1;

        let port = self.next_port;
        self.next_port += 1;

        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let data_dir = self.temp_dir.path().join(format!("node_{}", id));
        std::fs::create_dir_all(&data_dir).expect("Failed to create node dir");

        let node = SimulatedNode::new(id, role, addr, data_dir);
        self.nodes.write().await.insert(id, node);

        id
    }

    /// Starts a specific node
    pub async fn start_node(&self, node_id: u64) -> crate::common::Result<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.start().await?;
        }
        Ok(())
    }

    /// Stops a specific node
    pub async fn stop_node(&self, node_id: u64) -> crate::common::Result<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.stop().await?;
        }
        Ok(())
    }

    /// Fails a node (simulates crash)
    pub async fn fail_node(&self, node_id: u64) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.fail().await;
        }
    }

    /// Starts all nodes in the cluster
    pub async fn start_all(&self) -> crate::common::Result<()> {
        let mut nodes = self.nodes.write().await;
        for node in nodes.values_mut() {
            node.start().await?;
        }
        Ok(())
    }

    /// Stops all nodes in the cluster
    pub async fn stop_all(&self) -> crate::common::Result<()> {
        let mut nodes = self.nodes.write().await;
        for node in nodes.values_mut() {
            node.stop().await?;
        }
        Ok(())
    }

    /// Creates a network partition
    pub async fn partition(&self, group_a: Vec<u64>, group_b: Vec<u64>) {
        let mut network = self.network.write().await;
        network.partition(group_a, group_b);
    }

    /// Heals all network partitions
    pub async fn heal_partition(&self) {
        let mut network = self.network.write().await;
        network.heal();
    }

    /// Gets the number of healthy nodes
    pub async fn healthy_node_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.values().filter(|n| n.is_healthy()).count()
    }

    /// Gets nodes by role
    pub async fn nodes_by_role(&self, role: NodeRole) -> Vec<u64> {
        let nodes = self.nodes.read().await;
        nodes
            .iter()
            .filter(|(_, n)| n.role == role)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Gets node address
    pub async fn node_addr(&self, node_id: u64) -> Option<SocketAddr> {
        let nodes = self.nodes.read().await;
        nodes.get(&node_id).map(|n| n.addr)
    }

    /// Waits for cluster to reach desired healthy node count
    pub async fn wait_for_healthy(&self, count: usize, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.healthy_node_count().await >= count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

/// Cluster configuration builder
pub struct ClusterBuilder {
    name: String,
    metadata_nodes: usize,
    data_nodes: usize,
}

impl ClusterBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metadata_nodes: 1,
            data_nodes: 3,
        }
    }

    pub fn metadata_nodes(mut self, count: usize) -> Self {
        self.metadata_nodes = count;
        self
    }

    pub fn data_nodes(mut self, count: usize) -> Self {
        self.data_nodes = count;
        self
    }

    pub async fn build(self) -> SimulatedCluster {
        let mut cluster = SimulatedCluster::new(self.name);

        for _ in 0..self.metadata_nodes {
            cluster.add_node(NodeRole::Metadata).await;
        }

        for _ in 0..self.data_nodes {
            cluster.add_node(NodeRole::Data).await;
        }

        cluster
    }
}

/// Failure injection scenarios
pub struct FailureScenario {
    pub name: String,
    pub description: String,
    actions: Vec<FailureAction>,
}

#[derive(Clone)]
pub enum FailureAction {
    FailNode(u64),
    StopNode(u64),
    RestartNode(u64),
    Partition(Vec<u64>, Vec<u64>),
    HealPartition,
    Wait(Duration),
}

impl FailureScenario {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            actions: Vec::new(),
        }
    }

    pub fn fail_node(mut self, node_id: u64) -> Self {
        self.actions.push(FailureAction::FailNode(node_id));
        self
    }

    pub fn stop_node(mut self, node_id: u64) -> Self {
        self.actions.push(FailureAction::StopNode(node_id));
        self
    }

    pub fn restart_node(mut self, node_id: u64) -> Self {
        self.actions.push(FailureAction::RestartNode(node_id));
        self
    }

    pub fn partition(mut self, group_a: Vec<u64>, group_b: Vec<u64>) -> Self {
        self.actions.push(FailureAction::Partition(group_a, group_b));
        self
    }

    pub fn heal(mut self) -> Self {
        self.actions.push(FailureAction::HealPartition);
        self
    }

    pub fn wait(mut self, duration: Duration) -> Self {
        self.actions.push(FailureAction::Wait(duration));
        self
    }

    pub async fn execute(&self, cluster: &SimulatedCluster) -> crate::common::Result<()> {
        for action in &self.actions {
            match action {
                FailureAction::FailNode(id) => {
                    cluster.fail_node(*id).await;
                }
                FailureAction::StopNode(id) => {
                    cluster.stop_node(*id).await?;
                }
                FailureAction::RestartNode(id) => {
                    cluster.stop_node(*id).await?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    cluster.start_node(*id).await?;
                }
                FailureAction::Partition(a, b) => {
                    cluster.partition(a.clone(), b.clone()).await;
                }
                FailureAction::HealPartition => {
                    cluster.heal_partition().await;
                }
                FailureAction::Wait(d) => {
                    tokio::time::sleep(*d).await;
                }
            }
        }
        Ok(())
    }
}

/// Pre-built failure scenarios
pub mod scenarios {
    use super::*;

    /// Single node failure
    pub fn single_node_failure(node_id: u64) -> FailureScenario {
        FailureScenario::new(
            "single_node_failure",
            "Simulates a single node crash",
        )
        .fail_node(node_id)
    }

    /// Rolling restart of all nodes
    pub fn rolling_restart(node_ids: Vec<u64>) -> FailureScenario {
        let mut scenario = FailureScenario::new(
            "rolling_restart",
            "Restarts all nodes one by one",
        );
        for id in node_ids {
            scenario = scenario
                .stop_node(id)
                .wait(Duration::from_millis(500))
                .restart_node(id)
                .wait(Duration::from_millis(500));
        }
        scenario
    }

    /// Network partition (split brain)
    pub fn split_brain(group_a: Vec<u64>, group_b: Vec<u64>) -> FailureScenario {
        FailureScenario::new(
            "split_brain",
            "Creates a network partition between two groups",
        )
        .partition(group_a, group_b)
    }

    /// Metadata leader failure
    pub fn metadata_leader_failure(leader_id: u64) -> FailureScenario {
        FailureScenario::new(
            "metadata_leader_failure",
            "Simulates metadata leader crash",
        )
        .fail_node(leader_id)
    }

    /// Cascading failure
    pub fn cascading_failure(node_ids: Vec<u64>, delay: Duration) -> FailureScenario {
        let mut scenario = FailureScenario::new(
            "cascading_failure",
            "Fails multiple nodes in sequence",
        );
        for id in node_ids {
            scenario = scenario.fail_node(id).wait(delay);
        }
        scenario
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_creation() {
        let cluster = ClusterBuilder::new("test")
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
    async fn test_node_lifecycle() {
        let mut cluster = SimulatedCluster::new("test");
        let node_id = cluster.add_node(NodeRole::Data).await;

        assert_eq!(cluster.healthy_node_count().await, 0);

        cluster.start_node(node_id).await.unwrap();
        assert_eq!(cluster.healthy_node_count().await, 1);

        cluster.fail_node(node_id).await;
        assert_eq!(cluster.healthy_node_count().await, 0);
    }

    #[tokio::test]
    async fn test_network_partition() {
        let mut partition = NetworkPartition::new();
        partition.partition(vec![1, 2], vec![3, 4]);

        assert!(!partition.can_communicate(1, 3));
        assert!(!partition.can_communicate(3, 1));
        assert!(partition.can_communicate(1, 2));

        partition.heal();
        assert!(partition.can_communicate(1, 3));
    }
}
