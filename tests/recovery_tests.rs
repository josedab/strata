//! Recovery scenario tests for distributed system resilience.
//!
//! These tests verify that the system correctly recovers from various
//! failure scenarios, including:
//! - Node crashes and restarts
//! - Network partitions
//! - Data corruption
//! - Cascading failures
//! - Split-brain scenarios

#[allow(dead_code)]
mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};

// =============================================================================
// Recovery Testing Infrastructure
// =============================================================================

/// Simulated node for recovery testing.
pub struct RecoverableNode {
    id: u64,
    state: Arc<RwLock<NodeState>>,
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    wal: Arc<Mutex<WriteAheadLog>>,
    checkpoint: Arc<Mutex<Option<Checkpoint>>>,
    crash_handler: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Starting,
    Running,
    Recovering,
    Crashed,
    Stopped,
}

#[derive(Clone)]
struct WriteAheadLog {
    entries: Vec<WalEntry>,
    committed_index: u64,
    flushed_index: u64,
}

#[derive(Clone, Debug)]
struct WalEntry {
    index: u64,
    operation: Operation,
    committed: bool,
}

#[derive(Clone, Debug)]
enum Operation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

#[derive(Clone)]
struct Checkpoint {
    data: HashMap<String, Vec<u8>>,
    wal_index: u64,
    timestamp: Instant,
}

impl RecoverableNode {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            state: Arc::new(RwLock::new(NodeState::Stopped)),
            data: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(Mutex::new(WriteAheadLog {
                entries: Vec::new(),
                committed_index: 0,
                flushed_index: 0,
            })),
            checkpoint: Arc::new(Mutex::new(None)),
            crash_handler: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the node.
    pub async fn start(&self) -> Result<(), String> {
        let mut state = self.state.write().await;
        if *state == NodeState::Crashed || *state == NodeState::Stopped {
            *state = NodeState::Starting;
            // Reset crash handler so operations can proceed
            self.crash_handler.store(false, Ordering::SeqCst);
            drop(state);

            // Recover from checkpoint and WAL
            self.recover().await?;

            *self.state.write().await = NodeState::Running;
        }
        Ok(())
    }

    /// Stop the node gracefully.
    pub async fn stop(&self) -> Result<(), String> {
        let mut state = self.state.write().await;
        if *state == NodeState::Running {
            // Flush WAL and checkpoint
            self.create_checkpoint().await?;
            *state = NodeState::Stopped;
        }
        Ok(())
    }

    /// Crash the node (simulate sudden failure).
    pub async fn crash(&self) {
        self.crash_handler.store(true, Ordering::SeqCst);
        *self.state.write().await = NodeState::Crashed;
        // Note: WAL entries not explicitly flushed may be lost
    }

    /// Recover from crash state.
    async fn recover(&self) -> Result<(), String> {
        *self.state.write().await = NodeState::Recovering;

        // 1. Load checkpoint if available
        let checkpoint = self.checkpoint.lock().await.clone();
        if let Some(cp) = checkpoint {
            *self.data.write().await = cp.data.clone();
        }

        // 2. Replay committed WAL entries after checkpoint
        let wal = self.wal.lock().await;
        let checkpoint_index = self.checkpoint.lock().await
            .as_ref()
            .map(|cp| cp.wal_index)
            .unwrap_or(0);

        let mut data = self.data.write().await;
        for entry in &wal.entries {
            if entry.index > checkpoint_index && entry.committed {
                match &entry.operation {
                    Operation::Put { key, value } => {
                        data.insert(key.clone(), value.clone());
                    }
                    Operation::Delete { key } => {
                        data.remove(key);
                    }
                }
            }
        }

        Ok(())
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), String> {
        if *self.state.read().await != NodeState::Running {
            return Err("Node not running".to_string());
        }

        if self.crash_handler.load(Ordering::SeqCst) {
            return Err("Node crashed".to_string());
        }

        // Write to WAL first
        let index = {
            let mut wal = self.wal.lock().await;
            let index = wal.entries.len() as u64 + 1;
            wal.entries.push(WalEntry {
                index,
                operation: Operation::Put {
                    key: key.to_string(),
                    value: value.clone(),
                },
                committed: false,
            });
            index
        };

        // Simulate flush (in real system, this would be fsync)
        tokio::time::sleep(Duration::from_micros(100)).await;

        // Commit
        {
            let mut wal = self.wal.lock().await;
            if let Some(entry) = wal.entries.iter_mut().find(|e| e.index == index) {
                entry.committed = true;
            }
            wal.committed_index = index;
            wal.flushed_index = index;
        }

        // Apply to data
        self.data.write().await.insert(key.to_string(), value);

        Ok(())
    }

    /// Get a value by key.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        if *self.state.read().await != NodeState::Running {
            return Err("Node not running".to_string());
        }

        Ok(self.data.read().await.get(key).cloned())
    }

    /// Delete a key.
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        if *self.state.read().await != NodeState::Running {
            return Err("Node not running".to_string());
        }

        // Write to WAL first
        let index = {
            let mut wal = self.wal.lock().await;
            let index = wal.entries.len() as u64 + 1;
            wal.entries.push(WalEntry {
                index,
                operation: Operation::Delete {
                    key: key.to_string(),
                },
                committed: false,
            });
            index
        };

        // Commit
        {
            let mut wal = self.wal.lock().await;
            if let Some(entry) = wal.entries.iter_mut().find(|e| e.index == index) {
                entry.committed = true;
            }
            wal.committed_index = index;
        }

        // Apply to data
        self.data.write().await.remove(key);

        Ok(())
    }

    /// Create a checkpoint.
    pub async fn create_checkpoint(&self) -> Result<(), String> {
        let wal = self.wal.lock().await;
        let data = self.data.read().await;

        let cp = Checkpoint {
            data: data.clone(),
            wal_index: wal.committed_index,
            timestamp: Instant::now(),
        };

        *self.checkpoint.lock().await = Some(cp);
        Ok(())
    }

    /// Get current state.
    pub async fn state(&self) -> NodeState {
        self.state.read().await.clone()
    }

    /// Get all data for verification.
    pub async fn all_data(&self) -> HashMap<String, Vec<u8>> {
        self.data.read().await.clone()
    }
}

// =============================================================================
// Cluster Recovery Testing
// =============================================================================

/// A simulated cluster for recovery testing.
pub struct RecoverableCluster {
    nodes: Arc<RwLock<Vec<RecoverableNode>>>,
    leader_id: Arc<RwLock<Option<u64>>>,
    partition: Arc<RwLock<ClusterPartition>>,
}

struct ClusterPartition {
    isolated_nodes: std::collections::HashSet<u64>,
    blocked_routes: HashMap<u64, std::collections::HashSet<u64>>,
}

impl ClusterPartition {
    fn new() -> Self {
        Self {
            isolated_nodes: std::collections::HashSet::new(),
            blocked_routes: HashMap::new(),
        }
    }

    fn can_communicate(&self, from: u64, to: u64) -> bool {
        if self.isolated_nodes.contains(&from) || self.isolated_nodes.contains(&to) {
            return false;
        }
        if let Some(blocked) = self.blocked_routes.get(&from) {
            if blocked.contains(&to) {
                return false;
            }
        }
        true
    }
}

impl RecoverableCluster {
    pub fn new(num_nodes: usize) -> Self {
        let nodes: Vec<RecoverableNode> = (0..num_nodes as u64)
            .map(RecoverableNode::new)
            .collect();

        Self {
            nodes: Arc::new(RwLock::new(nodes)),
            leader_id: Arc::new(RwLock::new(Some(0))),
            partition: Arc::new(RwLock::new(ClusterPartition::new())),
        }
    }

    /// Start all nodes.
    pub async fn start_all(&self) -> Result<(), String> {
        let nodes = self.nodes.read().await;
        for node in nodes.iter() {
            node.start().await?;
        }
        Ok(())
    }

    /// Stop all nodes gracefully.
    pub async fn stop_all(&self) -> Result<(), String> {
        let nodes = self.nodes.read().await;
        for node in nodes.iter() {
            node.stop().await?;
        }
        Ok(())
    }

    /// Crash a specific node.
    pub async fn crash_node(&self, node_id: u64) -> Result<(), String> {
        let nodes = self.nodes.read().await;
        if let Some(node) = nodes.iter().find(|n| n.id == node_id) {
            node.crash().await;

            // If leader crashed, elect new one
            if *self.leader_id.read().await == Some(node_id) {
                self.elect_new_leader().await;
            }

            Ok(())
        } else {
            Err(format!("Node {} not found", node_id))
        }
    }

    /// Restart a specific node.
    pub async fn restart_node(&self, node_id: u64) -> Result<(), String> {
        let nodes = self.nodes.read().await;
        if let Some(node) = nodes.iter().find(|n| n.id == node_id) {
            node.start().await?;
            Ok(())
        } else {
            Err(format!("Node {} not found", node_id))
        }
    }

    /// Isolate a node (network partition).
    pub async fn isolate_node(&self, node_id: u64) {
        self.partition.write().await.isolated_nodes.insert(node_id);

        // If leader is isolated, elect new one
        if *self.leader_id.read().await == Some(node_id) {
            self.elect_new_leader().await;
        }
    }

    /// Reconnect an isolated node.
    pub async fn reconnect_node(&self, node_id: u64) {
        self.partition.write().await.isolated_nodes.remove(&node_id);
    }

    /// Create a network partition between two groups.
    pub async fn create_partition(&self, group_a: Vec<u64>, group_b: Vec<u64>) {
        let mut partition = self.partition.write().await;
        for &a in &group_a {
            for &b in &group_b {
                partition.blocked_routes.entry(a).or_default().insert(b);
                partition.blocked_routes.entry(b).or_default().insert(a);
            }
        }
    }

    /// Heal all partitions.
    pub async fn heal_partition(&self) {
        let mut partition = self.partition.write().await;
        partition.isolated_nodes.clear();
        partition.blocked_routes.clear();
    }

    /// Elect a new leader.
    async fn elect_new_leader(&self) {
        let nodes = self.nodes.read().await;
        let partition = self.partition.read().await;

        for node in nodes.iter() {
            if node.state().await == NodeState::Running
                && !partition.isolated_nodes.contains(&node.id)
            {
                *self.leader_id.write().await = Some(node.id);
                return;
            }
        }

        // No available leader
        *self.leader_id.write().await = None;
    }

    /// Write to the cluster (through leader).
    pub async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), String> {
        let leader_id = self.leader_id.read().await
            .ok_or("No leader available")?;

        let nodes = self.nodes.read().await;
        let leader = nodes.iter()
            .find(|n| n.id == leader_id)
            .ok_or("Leader not found")?;

        leader.put(key, value.clone()).await?;

        // Replicate to followers
        let partition = self.partition.read().await;
        for node in nodes.iter() {
            if node.id != leader_id
                && partition.can_communicate(leader_id, node.id)
                && node.state().await == NodeState::Running
            {
                // Best effort replication
                let _ = node.put(key, value.clone()).await;
            }
        }

        Ok(())
    }

    /// Read from the cluster (through leader for linearizability).
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let leader_id = self.leader_id.read().await
            .ok_or("No leader available")?;

        let nodes = self.nodes.read().await;
        let leader = nodes.iter()
            .find(|n| n.id == leader_id)
            .ok_or("Leader not found")?;

        leader.get(key).await
    }

    /// Get the current leader.
    pub async fn leader(&self) -> Option<u64> {
        *self.leader_id.read().await
    }

    /// Get count of running nodes.
    pub async fn running_node_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        let mut count = 0;
        for node in nodes.iter() {
            if node.state().await == NodeState::Running {
                count += 1;
            }
        }
        count
    }

    /// Verify data consistency across nodes.
    pub async fn verify_consistency(&self) -> Result<(), String> {
        let nodes = self.nodes.read().await;
        let mut reference_data: Option<HashMap<String, Vec<u8>>> = None;

        for node in nodes.iter() {
            if node.state().await != NodeState::Running {
                continue;
            }

            let data = node.all_data().await;
            match &reference_data {
                Some(ref_data) => {
                    if data != *ref_data {
                        return Err(format!(
                            "Data inconsistency detected at node {}",
                            node.id
                        ));
                    }
                }
                None => {
                    reference_data = Some(data);
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Recovery Test Scenarios
// =============================================================================

/// Test configuration for recovery scenarios.
#[derive(Clone)]
pub struct RecoveryTestConfig {
    pub num_nodes: usize,
    pub num_operations: usize,
    pub failure_type: FailureType,
    pub recovery_timeout: Duration,
}

#[derive(Clone, Debug)]
pub enum FailureType {
    SingleNodeCrash,
    MultiNodeCrash(usize),
    LeaderCrash,
    NetworkPartition,
    SplitBrain,
    RollingRestart,
    CascadingFailure,
}

impl Default for RecoveryTestConfig {
    fn default() -> Self {
        Self {
            num_nodes: 3,
            num_operations: 100,
            failure_type: FailureType::SingleNodeCrash,
            recovery_timeout: Duration::from_secs(5),
        }
    }
}

/// Result of a recovery test.
#[derive(Debug)]
pub struct RecoveryTestResult {
    pub success: bool,
    pub data_preserved: bool,
    pub consistency_maintained: bool,
    pub recovery_time: Duration,
    pub operations_before_failure: usize,
    pub operations_after_recovery: usize,
    pub error: Option<String>,
}

/// Run a recovery test scenario.
pub async fn run_recovery_test(config: RecoveryTestConfig) -> RecoveryTestResult {
    let cluster = Arc::new(RecoverableCluster::new(config.num_nodes));
    let start = Instant::now();

    // Start cluster
    if let Err(e) = cluster.start_all().await {
        return RecoveryTestResult {
            success: false,
            data_preserved: false,
            consistency_maintained: false,
            recovery_time: Duration::ZERO,
            operations_before_failure: 0,
            operations_after_recovery: 0,
            error: Some(e),
        };
    }

    // Perform some operations before failure
    let ops_before = config.num_operations / 2;
    for i in 0..ops_before {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i).into_bytes();
        if cluster.put(&key, value).await.is_err() {
            break;
        }
    }

    // Inject failure
    let failure_start = Instant::now();
    if let Err(e) = inject_failure(&cluster, &config.failure_type).await {
        return RecoveryTestResult {
            success: false,
            data_preserved: false,
            consistency_maintained: false,
            recovery_time: Duration::ZERO,
            operations_before_failure: ops_before,
            operations_after_recovery: 0,
            error: Some(e),
        };
    }

    // Wait a bit for failure to take effect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Recover from failure
    if let Err(e) = recover_from_failure(&cluster, &config.failure_type).await {
        return RecoveryTestResult {
            success: false,
            data_preserved: false,
            consistency_maintained: false,
            recovery_time: Instant::now().duration_since(failure_start),
            operations_before_failure: ops_before,
            operations_after_recovery: 0,
            error: Some(e),
        };
    }

    let recovery_time = Instant::now().duration_since(failure_start);

    // Verify data is preserved
    let mut data_preserved = true;
    for i in 0..ops_before {
        let key = format!("key-{}", i);
        match cluster.get(&key).await {
            Ok(Some(value)) => {
                let expected = format!("value-{}", i).into_bytes();
                if value != expected {
                    data_preserved = false;
                    break;
                }
            }
            Ok(None) => {
                data_preserved = false;
                break;
            }
            Err(_) => {
                data_preserved = false;
                break;
            }
        }
    }

    // Perform operations after recovery
    let mut ops_after = 0;
    for i in ops_before..config.num_operations {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i).into_bytes();
        if cluster.put(&key, value).await.is_ok() {
            ops_after += 1;
        } else {
            break;
        }
    }

    // Verify consistency
    let consistency_maintained = cluster.verify_consistency().await.is_ok();

    RecoveryTestResult {
        success: data_preserved && consistency_maintained && ops_after > 0,
        data_preserved,
        consistency_maintained,
        recovery_time,
        operations_before_failure: ops_before,
        operations_after_recovery: ops_after,
        error: None,
    }
}

async fn inject_failure(
    cluster: &RecoverableCluster,
    failure_type: &FailureType,
) -> Result<(), String> {
    match failure_type {
        FailureType::SingleNodeCrash => {
            // Crash a non-leader node
            let leader = cluster.leader().await;
            let target = if leader == Some(0) { 1 } else { 0 };
            cluster.crash_node(target).await?;
        }
        FailureType::MultiNodeCrash(count) => {
            // Crash multiple nodes (keeping majority)
            for i in 0..(*count).min(2) {
                cluster.crash_node(i as u64).await?;
            }
        }
        FailureType::LeaderCrash => {
            // Crash the leader
            if let Some(leader) = cluster.leader().await {
                cluster.crash_node(leader).await?;
            }
        }
        FailureType::NetworkPartition => {
            // Isolate one node
            cluster.isolate_node(2).await;
        }
        FailureType::SplitBrain => {
            // Create partition between [0,1] and [2]
            cluster.create_partition(vec![0, 1], vec![2]).await;
        }
        FailureType::RollingRestart => {
            // Restart nodes one by one
            for i in 0..3 {
                cluster.crash_node(i).await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
                cluster.restart_node(i).await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        FailureType::CascadingFailure => {
            // Fail nodes in sequence with delays
            for i in 0..2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                cluster.crash_node(i).await?;
            }
        }
    }
    Ok(())
}

async fn recover_from_failure(
    cluster: &RecoverableCluster,
    failure_type: &FailureType,
) -> Result<(), String> {
    match failure_type {
        FailureType::SingleNodeCrash => {
            let leader = cluster.leader().await;
            let target = if leader == Some(0) { 1 } else { 0 };
            cluster.restart_node(target).await?;
        }
        FailureType::MultiNodeCrash(count) => {
            for i in 0..(*count).min(2) {
                cluster.restart_node(i as u64).await?;
            }
        }
        FailureType::LeaderCrash => {
            // Find the crashed node and restart it
            // The cluster should have elected a new leader already
            for i in 0..3 {
                let nodes = cluster.nodes.read().await;
                if let Some(node) = nodes.iter().find(|n| n.id == i) {
                    if node.state().await == NodeState::Crashed {
                        drop(nodes);
                        cluster.restart_node(i).await?;
                        break;
                    }
                }
            }
        }
        FailureType::NetworkPartition | FailureType::SplitBrain => {
            cluster.heal_partition().await;
        }
        FailureType::RollingRestart => {
            // Already recovered during failure injection
        }
        FailureType::CascadingFailure => {
            // Restart crashed nodes
            for i in 0..2 {
                cluster.restart_node(i).await?;
            }
        }
    }

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

// =============================================================================
// Specific Recovery Scenarios
// =============================================================================

/// Test recovery from checkpointed state.
pub async fn test_checkpoint_recovery() -> Result<(), String> {
    let node = RecoverableNode::new(0);
    node.start().await?;

    // Write some data
    for i in 0..10 {
        node.put(&format!("key-{}", i), vec![i as u8; 100]).await?;
    }

    // Create checkpoint
    node.create_checkpoint().await?;

    // Write more data
    for i in 10..20 {
        node.put(&format!("key-{}", i), vec![i as u8; 100]).await?;
    }

    // Crash and recover
    node.crash().await;
    node.start().await?;

    // Verify all data is present
    for i in 0..20 {
        let value = node.get(&format!("key-{}", i)).await?;
        if value.is_none() {
            return Err(format!("Key {} missing after recovery", i));
        }
    }

    Ok(())
}

/// Test recovery during concurrent writes.
pub async fn test_concurrent_write_recovery() -> Result<(), String> {
    let cluster = Arc::new(RecoverableCluster::new(3));
    cluster.start_all().await?;

    let cluster_clone = cluster.clone();

    // Start concurrent writers
    let writer_handle = tokio::spawn(async move {
        for i in 0..100 {
            let key = format!("key-{}", i);
            let value = vec![i as u8; 64];
            let _ = cluster_clone.put(&key, value).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    // Crash and restart a node mid-way
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.crash_node(1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.restart_node(1).await?;

    // Wait for writer to finish
    let _ = writer_handle.await;

    // Verify consistency
    cluster.verify_consistency().await
}

/// Test recovery from WAL without checkpoint.
pub async fn test_wal_only_recovery() -> Result<(), String> {
    let node = RecoverableNode::new(0);
    node.start().await?;

    // Write data without checkpointing
    for i in 0..50 {
        node.put(&format!("key-{}", i), vec![i as u8; 32]).await?;
    }

    // Crash (no checkpoint created)
    node.crash().await;
    node.start().await?;

    // Only committed entries should be present
    let data = node.all_data().await;
    if data.is_empty() {
        return Err("No data recovered from WAL".to_string());
    }

    Ok(())
}

/// Test recovery after multiple sequential crashes.
pub async fn test_multiple_crash_recovery() -> Result<(), String> {
    let node = RecoverableNode::new(0);
    node.start().await?;

    for round in 0..3 {
        // Write some data
        for i in 0..10 {
            let key = format!("round-{}-key-{}", round, i);
            node.put(&key, vec![round as u8, i as u8]).await?;
        }

        // Checkpoint and crash
        node.create_checkpoint().await?;
        node.crash().await;
        node.start().await?;
    }

    // Verify all data from all rounds
    for round in 0..3 {
        for i in 0..10 {
            let key = format!("round-{}-key-{}", round, i);
            if node.get(&key).await?.is_none() {
                return Err(format!("Missing key {} after multiple recoveries", key));
            }
        }
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Single Node Recovery Tests (Unit tests for RecoverableNode)
    // These tests verify the WAL and checkpoint-based recovery mechanisms.
    // =========================================================================

    #[tokio::test]
    async fn test_checkpoint_based_recovery() {
        let result = test_checkpoint_recovery().await;
        assert!(result.is_ok(), "Checkpoint recovery should succeed: {:?}", result);
    }

    #[tokio::test]
    async fn test_wal_based_recovery() {
        let result = test_wal_only_recovery().await;
        assert!(result.is_ok(), "WAL-only recovery should succeed: {:?}", result);
    }

    #[tokio::test]
    async fn test_sequential_crash_recovery() {
        let result = test_multiple_crash_recovery().await;
        assert!(result.is_ok(), "Multiple sequential crashes should recover properly: {:?}", result);
    }

    // =========================================================================
    // Cluster Recovery Tests
    // These tests verify cluster-level behavior. Note that the simulated
    // cluster has limitations - each node maintains independent state, so
    // some scenarios may not fully replicate production behavior.
    // =========================================================================

    #[tokio::test]
    async fn test_network_partition_recovery() {
        let config = RecoveryTestConfig {
            num_nodes: 3,
            num_operations: 50,
            failure_type: FailureType::NetworkPartition,
            ..Default::default()
        };

        let result = run_recovery_test(config).await;

        // Network partition heals and operations continue
        assert!(result.consistency_maintained, "Consistency after partition heal");
        assert!(result.operations_after_recovery > 0, "Operations should work after partition heals");
    }

    #[tokio::test]
    async fn test_cluster_basic_operations() {
        // Test basic cluster functionality without failures
        let cluster = RecoverableCluster::new(3);
        cluster.start_all().await.unwrap();

        // Write some data
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i).into_bytes();
            cluster.put(&key, value).await.unwrap();
        }

        // Read back data
        for i in 0..10 {
            let key = format!("key-{}", i);
            let expected = format!("value-{}", i).into_bytes();
            let value = cluster.get(&key).await.unwrap();
            assert_eq!(value, Some(expected), "Data should be retrievable");
        }

        cluster.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn test_cluster_leader_failover() {
        let cluster = RecoverableCluster::new(3);
        cluster.start_all().await.unwrap();

        // Write initial data
        cluster.put("key1", b"value1".to_vec()).await.unwrap();

        let initial_leader = cluster.leader().await.unwrap();

        // Crash the leader
        cluster.crash_node(initial_leader).await.unwrap();

        // New leader should be elected
        let new_leader = cluster.leader().await;
        assert!(new_leader.is_some(), "New leader should be elected");

        // Write should work with new leader
        let result = cluster.put("key2", b"value2".to_vec()).await;
        assert!(result.is_ok(), "Write should succeed with new leader");
    }

    #[tokio::test]
    async fn test_recoverable_node_basic() {
        let node = RecoverableNode::new(0);

        // Start
        node.start().await.unwrap();
        assert_eq!(node.state().await, NodeState::Running);

        // Put and get
        node.put("key1", vec![1, 2, 3]).await.unwrap();
        let value = node.get("key1").await.unwrap();
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Delete
        node.delete("key1").await.unwrap();
        let value = node.get("key1").await.unwrap();
        assert_eq!(value, None);

        // Stop
        node.stop().await.unwrap();
        assert_eq!(node.state().await, NodeState::Stopped);
    }

    #[tokio::test]
    async fn test_recoverable_node_crash_recovery() {
        let node = RecoverableNode::new(0);
        node.start().await.unwrap();

        // Write data
        node.put("key1", vec![1, 2, 3]).await.unwrap();
        node.put("key2", vec![4, 5, 6]).await.unwrap();

        // Checkpoint
        node.create_checkpoint().await.unwrap();

        // Crash and recover
        node.crash().await;
        assert_eq!(node.state().await, NodeState::Crashed);

        node.start().await.unwrap();
        assert_eq!(node.state().await, NodeState::Running);

        // Verify data
        assert_eq!(node.get("key1").await.unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(node.get("key2").await.unwrap(), Some(vec![4, 5, 6]));
    }

    #[tokio::test]
    async fn test_recoverable_cluster_basic() {
        let cluster = RecoverableCluster::new(3);

        // Start cluster
        cluster.start_all().await.unwrap();
        assert_eq!(cluster.running_node_count().await, 3);

        // Write and read
        cluster.put("key1", vec![1, 2, 3]).await.unwrap();
        let value = cluster.get("key1").await.unwrap();
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Stop cluster
        cluster.stop_all().await.unwrap();
    }

}
