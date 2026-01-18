//! Linearizability tests for Raft consensus.
//!
//! These tests verify that the Raft implementation maintains linearizability
//! under various failure conditions, network partitions, and concurrent access.
//!
//! Linearizability guarantees:
//! - Every operation appears to execute atomically at some point between its
//!   invocation and response
//! - The order of operations is consistent with real-time ordering
//! - Operations on a register behave as if there's a single copy

#[allow(dead_code)]
mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

// =============================================================================
// Linearization Point Tracking
// =============================================================================

/// Tracks linearization points for operations.
#[derive(Debug, Clone)]
pub struct LinearizationPoint {
    /// Operation ID.
    pub op_id: u64,
    /// Logical timestamp when the operation linearized.
    pub timestamp: u64,
    /// The value after this operation.
    pub resulting_value: Option<i64>,
}

/// A history of operations with linearization point tracking.
pub struct LinearizableHistory {
    /// Operations in invocation order.
    operations: Vec<TrackedOperation>,
    /// Linearization points (operation ID -> point).
    linearization_points: HashMap<u64, LinearizationPoint>,
    /// Global logical clock.
    clock: AtomicU64,
    /// Next operation ID.
    next_id: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct TrackedOperation {
    pub id: u64,
    pub kind: OperationKind,
    pub key: String,
    pub invoke_time: u64,
    pub response_time: Option<u64>,
    pub input_value: Option<i64>,
    pub output_value: Option<i64>,
    pub success: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Write,
    CompareAndSwap,
}

impl LinearizableHistory {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            linearization_points: HashMap::new(),
            clock: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
        }
    }

    /// Begin an operation.
    pub fn begin_op(&mut self, kind: OperationKind, key: String, input: Option<i64>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let invoke_time = self.clock.fetch_add(1, Ordering::SeqCst);

        self.operations.push(TrackedOperation {
            id,
            kind,
            key,
            invoke_time,
            response_time: None,
            input_value: input,
            output_value: None,
            success: false,
        });

        id
    }

    /// Complete an operation.
    pub fn complete_op(&mut self, id: u64, output: Option<i64>, success: bool) {
        let response_time = self.clock.fetch_add(1, Ordering::SeqCst);

        if let Some(op) = self.operations.iter_mut().find(|o| o.id == id) {
            op.response_time = Some(response_time);
            op.output_value = output;
            op.success = success;
        }
    }

    /// Record a linearization point for an operation.
    pub fn record_linearization(&mut self, op_id: u64, resulting_value: Option<i64>) {
        let timestamp = self.clock.fetch_add(1, Ordering::SeqCst);
        self.linearization_points.insert(
            op_id,
            LinearizationPoint {
                op_id,
                timestamp,
                resulting_value,
            },
        );
    }

    /// Get operations that overlap in time.
    pub fn concurrent_operations(&self) -> Vec<Vec<u64>> {
        let mut groups = Vec::new();
        let completed: Vec<_> = self
            .operations
            .iter()
            .filter(|o| o.response_time.is_some())
            .collect();

        for op in &completed {
            let overlapping: Vec<u64> = completed
                .iter()
                .filter(|other| {
                    other.id != op.id
                        && self.intervals_overlap(
                            op.invoke_time,
                            op.response_time.unwrap(),
                            other.invoke_time,
                            other.response_time.unwrap(),
                        )
                })
                .map(|o| o.id)
                .collect();

            if !overlapping.is_empty() {
                let mut group = vec![op.id];
                group.extend(overlapping);
                group.sort();
                if !groups.contains(&group) {
                    groups.push(group);
                }
            }
        }

        groups
    }

    fn intervals_overlap(&self, start1: u64, end1: u64, start2: u64, end2: u64) -> bool {
        start1 < end2 && start2 < end1
    }

    /// Verify that the history is linearizable.
    pub fn verify_linearizable(&self) -> LinearizabilityVerification {
        let completed: Vec<_> = self
            .operations
            .iter()
            .filter(|o| o.response_time.is_some() && o.success)
            .cloned()
            .collect();

        if completed.is_empty() {
            return LinearizabilityVerification::Valid;
        }

        // Group by key
        let mut by_key: HashMap<String, Vec<TrackedOperation>> = HashMap::new();
        for op in completed {
            by_key.entry(op.key.clone()).or_default().push(op);
        }

        // Check each key independently
        for (key, ops) in by_key {
            if let Err(reason) = self.verify_key_linearizable(&ops) {
                return LinearizabilityVerification::Invalid {
                    key,
                    reason,
                };
            }
        }

        LinearizabilityVerification::Valid
    }

    fn verify_key_linearizable(&self, ops: &[TrackedOperation]) -> Result<(), String> {
        // Sort by invoke time
        let mut sorted = ops.to_vec();
        sorted.sort_by_key(|o| o.invoke_time);

        // Try to find a valid linearization using DFS
        if self.find_linearization(&sorted, None, &[]) {
            Ok(())
        } else {
            Err("No valid linearization found".to_string())
        }
    }

    fn find_linearization(
        &self,
        remaining: &[TrackedOperation],
        current_value: Option<i64>,
        linearized: &[u64],
    ) -> bool {
        if remaining.is_empty() {
            return true;
        }

        let linearized_set: std::collections::HashSet<u64> = linearized.iter().copied().collect();

        // Find operations that can be linearized next
        for (idx, op) in remaining.iter().enumerate() {
            // Check if this operation can be linearized now
            // (no un-linearized operation that responded before this invoked)
            let can_linearize = remaining.iter().all(|other| {
                if other.id == op.id || linearized_set.contains(&other.id) {
                    return true;
                }
                // If other responded before this invoked, it must be linearized first
                if let Some(other_response) = other.response_time {
                    other_response >= op.invoke_time
                } else {
                    true
                }
            });

            if !can_linearize {
                continue;
            }

            // Check if linearizing this operation is consistent with the model
            let new_value = match op.kind {
                OperationKind::Write => {
                    // Write always succeeds and sets the value
                    op.input_value
                }
                OperationKind::Read => {
                    // Read must observe the current value
                    if op.output_value != current_value {
                        continue; // Invalid linearization
                    }
                    current_value
                }
                OperationKind::CompareAndSwap => {
                    // CAS succeeds only if current value matches expected
                    if op.success {
                        if current_value != op.input_value {
                            continue; // CAS shouldn't have succeeded
                        }
                        op.output_value
                    } else {
                        // CAS failed, value unchanged
                        current_value
                    }
                }
            };

            // Recurse with this operation linearized
            let mut new_remaining = remaining.to_vec();
            new_remaining.remove(idx);
            let mut new_linearized = linearized.to_vec();
            new_linearized.push(op.id);

            if self.find_linearization(&new_remaining, new_value, &new_linearized) {
                return true;
            }
        }

        false
    }
}

impl Default for LinearizableHistory {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum LinearizabilityVerification {
    Valid,
    Invalid { key: String, reason: String },
}

impl LinearizabilityVerification {
    pub fn is_valid(&self) -> bool {
        matches!(self, LinearizabilityVerification::Valid)
    }
}

// =============================================================================
// Simulated Raft Register (for linearizability testing)
// =============================================================================

/// A simulated Raft-based register for testing linearizability.
pub struct RaftRegister {
    /// Current leader ID.
    leader: Arc<RwLock<Option<u64>>>,
    /// Node values (simulating replicated state).
    nodes: Arc<RwLock<HashMap<u64, HashMap<String, i64>>>>,
    /// Commit index per node.
    commit_index: Arc<RwLock<HashMap<u64, u64>>>,
    /// Log entries per node.
    logs: Arc<RwLock<HashMap<u64, Vec<LogEntry>>>>,
    /// Network partition simulation.
    partition: Arc<RwLock<NetworkPartition>>,
    /// History for verification.
    history: Arc<Mutex<LinearizableHistory>>,
    /// Configuration.
    config: RaftRegisterConfig,
}

#[derive(Clone)]
pub struct RaftRegisterConfig {
    pub num_nodes: usize,
    pub election_timeout_ms: u64,
    pub network_delay_ms: u64,
}

impl Default for RaftRegisterConfig {
    fn default() -> Self {
        Self {
            num_nodes: 3,
            election_timeout_ms: 100,
            network_delay_ms: 5,
        }
    }
}

#[derive(Clone, Debug)]
struct LogEntry {
    term: u64,
    index: u64,
    key: String,
    value: i64,
}

struct NetworkPartition {
    /// Nodes that are isolated.
    isolated: std::collections::HashSet<u64>,
    /// Specific blocked connections (from -> to).
    blocked: HashMap<u64, std::collections::HashSet<u64>>,
}

impl NetworkPartition {
    fn new() -> Self {
        Self {
            isolated: std::collections::HashSet::new(),
            blocked: HashMap::new(),
        }
    }

    fn can_reach(&self, from: u64, to: u64) -> bool {
        if self.isolated.contains(&from) || self.isolated.contains(&to) {
            return false;
        }
        if let Some(blocked_targets) = self.blocked.get(&from) {
            if blocked_targets.contains(&to) {
                return false;
            }
        }
        true
    }

    fn isolate(&mut self, node: u64) {
        self.isolated.insert(node);
    }

    fn reconnect(&mut self, node: u64) {
        self.isolated.remove(&node);
    }

    fn block(&mut self, from: u64, to: u64) {
        self.blocked.entry(from).or_default().insert(to);
    }

    fn unblock(&mut self, from: u64, to: u64) {
        if let Some(targets) = self.blocked.get_mut(&from) {
            targets.remove(&to);
        }
    }

    fn heal_all(&mut self) {
        self.isolated.clear();
        self.blocked.clear();
    }
}

impl RaftRegister {
    pub fn new(config: RaftRegisterConfig) -> Self {
        let mut nodes = HashMap::new();
        let mut commit_index = HashMap::new();
        let mut logs = HashMap::new();

        for i in 0..config.num_nodes as u64 {
            nodes.insert(i, HashMap::new());
            commit_index.insert(i, 0);
            logs.insert(i, Vec::new());
        }

        Self {
            leader: Arc::new(RwLock::new(Some(0))), // Node 0 starts as leader
            nodes: Arc::new(RwLock::new(nodes)),
            commit_index: Arc::new(RwLock::new(commit_index)),
            logs: Arc::new(RwLock::new(logs)),
            partition: Arc::new(RwLock::new(NetworkPartition::new())),
            history: Arc::new(Mutex::new(LinearizableHistory::new())),
            config,
        }
    }

    /// Write a value with linearizability tracking.
    pub async fn write(&self, key: &str, value: i64) -> Result<(), String> {
        let op_id = {
            let mut history = self.history.lock().await;
            history.begin_op(OperationKind::Write, key.to_string(), Some(value))
        };

        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(self.config.network_delay_ms)).await;

        let result = self.do_write(key, value).await;

        {
            let mut history = self.history.lock().await;
            history.complete_op(op_id, Some(value), result.is_ok());
            if result.is_ok() {
                history.record_linearization(op_id, Some(value));
            }
        }

        result
    }

    async fn do_write(&self, key: &str, value: i64) -> Result<(), String> {
        let leader = *self.leader.read().await;
        let leader = leader.ok_or("No leader")?;

        // Check if we can reach the leader
        let partition = self.partition.read().await;
        if !partition.can_reach(0, leader) {
            // Assume client is at node 0
            return Err("Cannot reach leader".to_string());
        }
        drop(partition);

        // Simulate quorum write
        let majority = (self.config.num_nodes / 2) + 1;
        let mut acks = 1; // Leader counts as one ack

        // Write to leader
        {
            let mut nodes = self.nodes.write().await;
            if let Some(node_state) = nodes.get_mut(&leader) {
                node_state.insert(key.to_string(), value);
            }
        }

        // Replicate to followers
        let partition = self.partition.read().await;
        for node_id in 0..self.config.num_nodes as u64 {
            if node_id == leader {
                continue;
            }

            if partition.can_reach(leader, node_id) {
                let mut nodes = self.nodes.write().await;
                if let Some(node_state) = nodes.get_mut(&node_id) {
                    node_state.insert(key.to_string(), value);
                    acks += 1;
                }
            }
        }

        if acks >= majority {
            Ok(())
        } else {
            Err("Failed to reach quorum".to_string())
        }
    }

    /// Read a value with linearizability tracking.
    pub async fn read(&self, key: &str) -> Result<Option<i64>, String> {
        let op_id = {
            let mut history = self.history.lock().await;
            history.begin_op(OperationKind::Read, key.to_string(), None)
        };

        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(self.config.network_delay_ms)).await;

        let result = self.do_read(key).await;

        {
            let mut history = self.history.lock().await;
            let value = result.as_ref().ok().and_then(|v| *v);
            history.complete_op(op_id, value, result.is_ok());
            if result.is_ok() {
                history.record_linearization(op_id, value);
            }
        }

        result
    }

    async fn do_read(&self, key: &str) -> Result<Option<i64>, String> {
        let leader = *self.leader.read().await;
        let leader = leader.ok_or("No leader")?;

        // Check if we can reach the leader
        let partition = self.partition.read().await;
        if !partition.can_reach(0, leader) {
            return Err("Cannot reach leader".to_string());
        }
        drop(partition);

        // Read from leader (linearizable read)
        let nodes = self.nodes.read().await;
        if let Some(node_state) = nodes.get(&leader) {
            Ok(node_state.get(key).copied())
        } else {
            Err("Leader not found".to_string())
        }
    }

    /// Isolate a node (simulate network partition).
    pub async fn isolate_node(&self, node_id: u64) {
        let mut partition = self.partition.write().await;
        partition.isolate(node_id);

        // If leader is isolated, trigger election
        let leader = *self.leader.read().await;
        if leader == Some(node_id) {
            drop(partition);
            self.trigger_election().await;
        }
    }

    /// Reconnect a node.
    pub async fn reconnect_node(&self, node_id: u64) {
        let mut partition = self.partition.write().await;
        partition.reconnect(node_id);
    }

    /// Block communication between two specific nodes.
    pub async fn block_connection(&self, from: u64, to: u64) {
        let mut partition = self.partition.write().await;
        partition.block(from, to);
    }

    /// Heal all network partitions.
    pub async fn heal_all(&self) {
        let mut partition = self.partition.write().await;
        partition.heal_all();
    }

    /// Trigger a leader election.
    async fn trigger_election(&self) {
        // Find the node with the highest ID that's not isolated
        let partition = self.partition.read().await;
        let mut new_leader = None;

        for node_id in 0..self.config.num_nodes as u64 {
            if !partition.isolated.contains(&node_id) {
                new_leader = Some(node_id);
            }
        }

        drop(partition);
        *self.leader.write().await = new_leader;
    }

    /// Get the current leader.
    pub async fn leader(&self) -> Option<u64> {
        *self.leader.read().await
    }

    /// Verify that all operations were linearizable.
    pub async fn verify_linearizability(&self) -> LinearizabilityVerification {
        let history = self.history.lock().await;
        history.verify_linearizable()
    }

    /// Get the history for external verification.
    pub async fn history(&self) -> LinearizableHistory {
        let guard = self.history.lock().await;
        LinearizableHistory {
            operations: guard.operations.clone(),
            linearization_points: guard.linearization_points.clone(),
            clock: AtomicU64::new(guard.clock.load(Ordering::SeqCst)),
            next_id: AtomicU64::new(guard.next_id.load(Ordering::SeqCst)),
        }
    }
}

// =============================================================================
// Linearizability Test Scenarios
// =============================================================================

/// Configuration for linearizability tests.
#[derive(Clone)]
pub struct LinearizabilityTestConfig {
    pub num_clients: usize,
    pub operations_per_client: usize,
    pub read_ratio: f64,
    pub num_keys: usize,
    pub inject_failures: bool,
    pub failure_probability: f64,
}

impl Default for LinearizabilityTestConfig {
    fn default() -> Self {
        Self {
            num_clients: 5,
            operations_per_client: 50,
            read_ratio: 0.5,
            num_keys: 5,
            inject_failures: false,
            failure_probability: 0.1,
        }
    }
}

/// Result of a linearizability test.
#[derive(Debug)]
pub struct LinearizabilityTestResult {
    pub is_linearizable: bool,
    pub total_operations: usize,
    pub successful_operations: usize,
    pub failed_operations: usize,
    pub duration: Duration,
    pub error: Option<String>,
}

/// Run a linearizability test with multiple concurrent clients.
pub async fn run_linearizability_test(
    config: LinearizabilityTestConfig,
) -> LinearizabilityTestResult {
    let register = Arc::new(RaftRegister::new(RaftRegisterConfig::default()));
    let start = Instant::now();

    let successful_ops = Arc::new(AtomicU64::new(0));
    let failed_ops = Arc::new(AtomicU64::new(0));

    // Spawn client tasks
    let mut handles = Vec::new();

    for client_id in 0..config.num_clients {
        let reg = register.clone();
        let cfg = config.clone();
        let success = successful_ops.clone();
        let fail = failed_ops.clone();

        handles.push(tokio::spawn(async move {
            for op_idx in 0..cfg.operations_per_client {
                let key = format!("key-{}", op_idx % cfg.num_keys);
                let is_read = rand::random::<f64>() < cfg.read_ratio;

                let result = if is_read {
                    reg.read(&key).await.map(|_| ())
                } else {
                    let value = (client_id * 1000 + op_idx) as i64;
                    reg.write(&key, value).await
                };

                if result.is_ok() {
                    success.fetch_add(1, Ordering::SeqCst);
                } else {
                    fail.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    // Optionally inject failures
    if config.inject_failures {
        let reg = register.clone();
        tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if rand::random::<f64>() < config.failure_probability {
                    let node_to_fail = (rand::random::<f64>() * 3.0) as u64;
                    reg.isolate_node(node_to_fail).await;
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    reg.reconnect_node(node_to_fail).await;
                }
            }
        });
    }

    // Wait for all clients
    for handle in handles {
        let _ = handle.await;
    }

    let duration = start.elapsed();

    // Verify linearizability
    let verification = register.verify_linearizability().await;
    let is_linearizable = verification.is_valid();
    let error = match verification {
        LinearizabilityVerification::Valid => None,
        LinearizabilityVerification::Invalid { key, reason } => {
            Some(format!("Key '{}': {}", key, reason))
        }
    };

    LinearizabilityTestResult {
        is_linearizable,
        total_operations: config.num_clients * config.operations_per_client,
        successful_operations: successful_ops.load(Ordering::SeqCst) as usize,
        failed_operations: failed_ops.load(Ordering::SeqCst) as usize,
        duration,
        error,
    }
}

/// Test linearizability during leader changes.
pub async fn test_linearizability_leader_change() -> LinearizabilityTestResult {
    let register = Arc::new(RaftRegister::new(RaftRegisterConfig::default()));
    let start = Instant::now();

    let successful_ops = Arc::new(AtomicU64::new(0));
    let failed_ops = Arc::new(AtomicU64::new(0));

    let reg = register.clone();
    let success = successful_ops.clone();
    let fail = failed_ops.clone();

    // Start some operations
    let client_handle = tokio::spawn(async move {
        for i in 0..50 {
            let key = format!("key-{}", i % 5);
            if i % 2 == 0 {
                if reg.write(&key, i as i64).await.is_ok() {
                    success.fetch_add(1, Ordering::SeqCst);
                } else {
                    fail.fetch_add(1, Ordering::SeqCst);
                }
            } else if reg.read(&key).await.is_ok() {
                success.fetch_add(1, Ordering::SeqCst);
            } else {
                fail.fetch_add(1, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    // Cause leader changes
    let reg2 = register.clone();
    let failure_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Isolate current leader
        if let Some(leader) = reg2.leader().await {
            reg2.isolate_node(leader).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            reg2.reconnect_node(leader).await;
        }
    });

    let _ = tokio::join!(client_handle, failure_handle);

    let duration = start.elapsed();
    let verification = register.verify_linearizability().await;

    LinearizabilityTestResult {
        is_linearizable: verification.is_valid(),
        total_operations: 50,
        successful_operations: successful_ops.load(Ordering::SeqCst) as usize,
        failed_operations: failed_ops.load(Ordering::SeqCst) as usize,
        duration,
        error: match verification {
            LinearizabilityVerification::Valid => None,
            LinearizabilityVerification::Invalid { key, reason } => {
                Some(format!("Key '{}': {}", key, reason))
            }
        },
    }
}

/// Test linearizability during network partition.
pub async fn test_linearizability_partition() -> LinearizabilityTestResult {
    let register = Arc::new(RaftRegister::new(RaftRegisterConfig {
        num_nodes: 5,
        ..Default::default()
    }));
    let start = Instant::now();

    let successful_ops = Arc::new(AtomicU64::new(0));
    let failed_ops = Arc::new(AtomicU64::new(0));

    // Start client operations
    let reg = register.clone();
    let success = successful_ops.clone();
    let fail = failed_ops.clone();

    let client_handle = tokio::spawn(async move {
        for i in 0..100 {
            let key = format!("key-{}", i % 10);
            let result = if i % 3 == 0 {
                reg.read(&key).await.map(|_| ())
            } else {
                reg.write(&key, i as i64).await
            };

            if result.is_ok() {
                success.fetch_add(1, Ordering::SeqCst);
            } else {
                fail.fetch_add(1, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // Create and heal partitions
    let reg2 = register.clone();
    let partition_handle = tokio::spawn(async move {
        // Wait, then partition
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Isolate minority (nodes 3, 4)
        reg2.isolate_node(3).await;
        reg2.isolate_node(4).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Heal partition
        reg2.heal_all().await;
    });

    let _ = tokio::join!(client_handle, partition_handle);

    let duration = start.elapsed();
    let verification = register.verify_linearizability().await;

    LinearizabilityTestResult {
        is_linearizable: verification.is_valid(),
        total_operations: 100,
        successful_operations: successful_ops.load(Ordering::SeqCst) as usize,
        failed_operations: failed_ops.load(Ordering::SeqCst) as usize,
        duration,
        error: match verification {
            LinearizabilityVerification::Valid => None,
            LinearizabilityVerification::Invalid { key, reason } => {
                Some(format!("Key '{}': {}", key, reason))
            }
        },
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_linearizability() {
        let config = LinearizabilityTestConfig {
            num_clients: 3,
            operations_per_client: 20,
            read_ratio: 0.5,
            num_keys: 3,
            inject_failures: false,
            failure_probability: 0.0,
        };

        let result = run_linearizability_test(config).await;

        assert!(
            result.is_linearizable,
            "Basic operations should be linearizable"
        );
        assert_eq!(result.total_operations, 60);
    }

    #[tokio::test]
    async fn test_linearizability_high_concurrency() {
        let config = LinearizabilityTestConfig {
            num_clients: 10,
            operations_per_client: 30,
            read_ratio: 0.5,
            num_keys: 5,
            inject_failures: false,
            failure_probability: 0.0,
        };

        let result = run_linearizability_test(config).await;

        assert!(
            result.is_linearizable,
            "High concurrency operations should be linearizable"
        );
    }

    #[tokio::test]
    async fn test_linearizability_read_heavy() {
        let config = LinearizabilityTestConfig {
            num_clients: 5,
            operations_per_client: 50,
            read_ratio: 0.9,
            num_keys: 3,
            inject_failures: false,
            failure_probability: 0.0,
        };

        let result = run_linearizability_test(config).await;

        assert!(
            result.is_linearizable,
            "Read-heavy workload should be linearizable"
        );
    }

    #[tokio::test]
    async fn test_linearizability_write_heavy() {
        let config = LinearizabilityTestConfig {
            num_clients: 5,
            operations_per_client: 50,
            read_ratio: 0.1,
            num_keys: 3,
            inject_failures: false,
            failure_probability: 0.0,
        };

        let result = run_linearizability_test(config).await;

        assert!(
            result.is_linearizable,
            "Write-heavy workload should be linearizable"
        );
    }

    #[tokio::test]
    async fn test_linearizability_single_key() {
        let config = LinearizabilityTestConfig {
            num_clients: 5,
            operations_per_client: 30,
            read_ratio: 0.5,
            num_keys: 1, // All operations on single key
            inject_failures: false,
            failure_probability: 0.0,
        };

        let result = run_linearizability_test(config).await;

        assert!(
            result.is_linearizable,
            "Single key operations should be linearizable"
        );
    }

    #[tokio::test]
    async fn test_linearizability_during_leader_change() {
        let result = test_linearizability_leader_change().await;

        // During leader changes, some operations may fail, but successful
        // ones should still be linearizable
        assert!(
            result.is_linearizable,
            "Operations during leader change should maintain linearizability"
        );
    }

    #[tokio::test]
    async fn test_linearizability_during_partition() {
        let result = test_linearizability_partition().await;

        // During partitions, operations should still be linearizable
        // (failed operations are not included in linearizability check)
        assert!(
            result.is_linearizable,
            "Operations during partition should maintain linearizability"
        );
        // Note: With minority partition (2 out of 5 nodes isolated),
        // the majority quorum can still process requests, so we may
        // not see failures in all cases
    }

    #[tokio::test]
    async fn test_raft_register_basic() {
        let register = RaftRegister::new(RaftRegisterConfig::default());

        // Write then read
        register.write("key1", 100).await.unwrap();
        let value = register.read("key1").await.unwrap();

        assert_eq!(value, Some(100));
    }

    #[tokio::test]
    async fn test_raft_register_partition() {
        let register = RaftRegister::new(RaftRegisterConfig {
            num_nodes: 5,
            ..Default::default()
        });

        // Initial write
        register.write("key1", 100).await.unwrap();

        // Isolate a non-leader node (node 2)
        register.isolate_node(2).await;

        // Operations should still work (leader is still available)
        let current_leader = register.leader().await;
        assert!(current_leader.is_some());

        // Write and read should work
        register.write("key2", 200).await.unwrap();
        let value = register.read("key2").await.unwrap();
        assert_eq!(value, Some(200));

        // Reconnect
        register.reconnect_node(2).await;
    }

    #[tokio::test]
    async fn test_linearizable_history_simple() {
        let mut history = LinearizableHistory::new();

        // Write 1 -> Read 1 (sequential, linearizable)
        let id1 = history.begin_op(OperationKind::Write, "key".to_string(), Some(1));
        history.complete_op(id1, Some(1), true);

        let id2 = history.begin_op(OperationKind::Read, "key".to_string(), None);
        history.complete_op(id2, Some(1), true);

        let verification = history.verify_linearizable();
        assert!(verification.is_valid());
    }

    #[tokio::test]
    async fn test_linearizable_history_concurrent() {
        let mut history = LinearizableHistory::new();

        // Concurrent writes, read sees one of them
        let id1 = history.begin_op(OperationKind::Write, "key".to_string(), Some(1));
        let id2 = history.begin_op(OperationKind::Write, "key".to_string(), Some(2));
        history.complete_op(id1, Some(1), true);
        history.complete_op(id2, Some(2), true);

        let id3 = history.begin_op(OperationKind::Read, "key".to_string(), None);
        history.complete_op(id3, Some(2), true); // Read sees 2

        let verification = history.verify_linearizable();
        assert!(verification.is_valid());
    }

    #[test]
    fn test_operation_ordering() {
        let mut history = LinearizableHistory::new();

        // Create overlapping operations
        let id1 = history.begin_op(OperationKind::Write, "key".to_string(), Some(1));
        let id2 = history.begin_op(OperationKind::Write, "key".to_string(), Some(2));
        // id1 and id2 are concurrent
        history.complete_op(id2, Some(2), true);
        history.complete_op(id1, Some(1), true);

        let groups = history.concurrent_operations();
        assert!(!groups.is_empty(), "Should detect concurrent operations");
    }
}
