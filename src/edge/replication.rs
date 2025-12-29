// Edge Cache Replication

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Replication factor
    pub factor: u32,
    /// Consistency level for reads
    pub read_consistency: ConsistencyLevel,
    /// Consistency level for writes
    pub write_consistency: ConsistencyLevel,
    /// Sync interval in milliseconds
    pub sync_interval_ms: u64,
    /// Enable proactive push
    pub proactive_push: bool,
    /// Maximum replication lag in milliseconds
    pub max_lag_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            factor: 2,
            read_consistency: ConsistencyLevel::One,
            write_consistency: ConsistencyLevel::One,
            sync_interval_ms: 1000,
            proactive_push: true,
            max_lag_ms: 5000,
        }
    }
}

/// Consistency level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyLevel {
    /// No consistency (best effort)
    None,
    /// Read/write from one node
    One,
    /// Read/write from quorum
    Quorum,
    /// Read/write from all nodes
    All,
    /// Local datacenter consistency
    LocalQuorum,
    /// Each datacenter quorum
    EachQuorum,
}

/// Node info for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub id: String,
    /// Node endpoint
    pub endpoint: String,
    /// Region
    pub region: String,
    /// Is healthy
    pub healthy: bool,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Replication lag in milliseconds
    pub lag_ms: u64,
}

/// Replication manager
pub struct ReplicationManager {
    /// Configuration
    config: ReplicationConfig,
    /// Local node ID
    local_node_id: String,
    /// Known nodes
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    /// Replication log
    replication_log: Arc<RwLock<Vec<ReplicationEntry>>>,
    /// Statistics
    stats: Arc<ReplicationStats>,
}

/// Replication log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    /// Entry ID
    pub id: u64,
    /// Operation type
    pub operation: ReplicationOp,
    /// Object key
    pub key: String,
    /// Source node
    pub source_node: String,
    /// Target nodes
    pub target_nodes: Vec<String>,
    /// Timestamp
    pub timestamp: u64,
    /// Status
    pub status: ReplicationStatus,
}

/// Replication operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationOp {
    Put,
    Delete,
    Invalidate,
}

/// Replication status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    PartialSuccess,
}

/// Replication statistics
pub struct ReplicationStats {
    /// Replications initiated
    pub initiated: AtomicU64,
    /// Replications completed
    pub completed: AtomicU64,
    /// Replications failed
    pub failed: AtomicU64,
    /// Bytes replicated
    pub bytes_replicated: AtomicU64,
    /// Average latency
    pub total_latency_ms: AtomicU64,
}

impl Default for ReplicationStats {
    fn default() -> Self {
        Self {
            initiated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            bytes_replicated: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
        }
    }
}

impl ReplicationManager {
    /// Creates a new replication manager
    pub fn new(local_node_id: String, config: ReplicationConfig) -> Self {
        Self {
            config,
            local_node_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            replication_log: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(ReplicationStats::default()),
        }
    }

    /// Registers a peer node
    pub async fn register_node(&self, info: NodeInfo) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(info.id.clone(), info);
    }

    /// Removes a peer node
    pub async fn unregister_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
    }

    /// Gets healthy peer nodes
    pub async fn healthy_peers(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|n| n.healthy && n.id != self.local_node_id)
            .cloned()
            .collect()
    }

    /// Selects nodes for replication
    pub async fn select_targets(&self, _key: &str) -> Vec<String> {
        let peers = self.healthy_peers().await;

        // Simple selection: first N peers
        peers.iter()
            .take((self.config.factor - 1) as usize)
            .map(|n| n.id.clone())
            .collect()
    }

    /// Replicates an object to peer nodes
    pub async fn replicate(&self, key: &str, data: &[u8]) -> Result<ReplicationResult> {
        let targets = self.select_targets(key).await;

        if targets.is_empty() {
            return Ok(ReplicationResult {
                success_count: 0,
                failure_count: 0,
                targets: Vec::new(),
            });
        }

        let entry_id = self.stats.initiated.fetch_add(1, Ordering::SeqCst);

        let entry = ReplicationEntry {
            id: entry_id,
            operation: ReplicationOp::Put,
            key: key.to_string(),
            source_node: self.local_node_id.clone(),
            target_nodes: targets.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            status: ReplicationStatus::InProgress,
        };

        // Log the replication
        {
            let mut log = self.replication_log.write().await;
            log.push(entry);
        }

        // Simulate replication to each target
        let mut successes = Vec::new();
        let failures: Vec<String> = Vec::new();

        for target in &targets {
            // In real implementation, would send to peer
            // Simulating success
            successes.push(target.clone());
            self.stats.bytes_replicated.fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        self.stats.completed.fetch_add(successes.len() as u64, Ordering::Relaxed);
        self.stats.failed.fetch_add(failures.len() as u64, Ordering::Relaxed);

        Ok(ReplicationResult {
            success_count: successes.len(),
            failure_count: failures.len(),
            targets: successes,
        })
    }

    /// Replicates a delete
    pub async fn replicate_delete(&self, key: &str) -> Result<ReplicationResult> {
        let targets = self.select_targets(key).await;

        if targets.is_empty() {
            return Ok(ReplicationResult {
                success_count: 0,
                failure_count: 0,
                targets: Vec::new(),
            });
        }

        // Simulate delete replication
        let successes = targets.clone();

        self.stats.initiated.fetch_add(1, Ordering::Relaxed);
        self.stats.completed.fetch_add(successes.len() as u64, Ordering::Relaxed);

        Ok(ReplicationResult {
            success_count: successes.len(),
            failure_count: 0,
            targets: successes,
        })
    }

    /// Checks consistency level satisfaction
    pub fn check_consistency(&self, success_count: usize, level: ConsistencyLevel) -> bool {
        let total_nodes = self.config.factor as usize;
        let quorum = (total_nodes / 2) + 1;

        match level {
            ConsistencyLevel::None => true,
            ConsistencyLevel::One => success_count >= 1,
            ConsistencyLevel::Quorum => success_count >= quorum,
            ConsistencyLevel::All => success_count >= total_nodes,
            ConsistencyLevel::LocalQuorum => success_count >= quorum,
            ConsistencyLevel::EachQuorum => success_count >= quorum,
        }
    }

    /// Gets replication statistics
    pub fn stats(&self) -> ReplicationStatsSnapshot {
        let initiated = self.stats.initiated.load(Ordering::Relaxed);
        let completed = self.stats.completed.load(Ordering::Relaxed);

        ReplicationStatsSnapshot {
            initiated,
            completed,
            failed: self.stats.failed.load(Ordering::Relaxed),
            bytes_replicated: self.stats.bytes_replicated.load(Ordering::Relaxed),
            avg_latency_ms: if completed > 0 {
                self.stats.total_latency_ms.load(Ordering::Relaxed) / completed
            } else {
                0
            },
        }
    }

    /// Gets replication log
    pub async fn get_log(&self, limit: usize) -> Vec<ReplicationEntry> {
        let log = self.replication_log.read().await;
        log.iter().rev().take(limit).cloned().collect()
    }
}

/// Replication result
#[derive(Debug, Clone)]
pub struct ReplicationResult {
    /// Number of successful replications
    pub success_count: usize,
    /// Number of failed replications
    pub failure_count: usize,
    /// Nodes that received the replication
    pub targets: Vec<String>,
}

impl ReplicationResult {
    /// Checks if replication was fully successful
    pub fn is_success(&self) -> bool {
        self.failure_count == 0 && self.success_count > 0
    }
}

/// Replication statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatsSnapshot {
    pub initiated: u64,
    pub completed: u64,
    pub failed: u64,
    pub bytes_replicated: u64,
    pub avg_latency_ms: u64,
}

impl ReplicationStatsSnapshot {
    /// Gets success rate
    pub fn success_rate(&self) -> f64 {
        if self.initiated == 0 {
            0.0
        } else {
            self.completed as f64 / self.initiated as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_manager() {
        let config = ReplicationConfig {
            factor: 3,
            ..Default::default()
        };
        let manager = ReplicationManager::new("node-1".to_string(), config);

        // Register peers
        manager.register_node(NodeInfo {
            id: "node-2".to_string(),
            endpoint: "http://node-2:8080".to_string(),
            region: "us-east-1".to_string(),
            healthy: true,
            last_seen: 0,
            lag_ms: 0,
        }).await;

        manager.register_node(NodeInfo {
            id: "node-3".to_string(),
            endpoint: "http://node-3:8080".to_string(),
            region: "us-east-1".to_string(),
            healthy: true,
            last_seen: 0,
            lag_ms: 0,
        }).await;

        let peers = manager.healthy_peers().await;
        assert_eq!(peers.len(), 2);

        // Test replication
        let result = manager.replicate("key1", b"data").await.unwrap();
        assert!(result.success_count >= 1);
    }

    #[test]
    fn test_consistency_check() {
        let config = ReplicationConfig {
            factor: 3,
            ..Default::default()
        };
        let manager = ReplicationManager::new("node-1".to_string(), config);

        assert!(manager.check_consistency(1, ConsistencyLevel::One));
        assert!(manager.check_consistency(2, ConsistencyLevel::Quorum));
        assert!(manager.check_consistency(3, ConsistencyLevel::All));
        assert!(!manager.check_consistency(1, ConsistencyLevel::Quorum));
    }
}
