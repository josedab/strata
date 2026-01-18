//! Distributed coordinator for cluster-wide operations.
//!
//! The coordinator provides distributed locking and coordination
//! to ensure safe concurrent recovery, rebalancing, and other
//! cluster-wide operations.

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, DataServerId, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Configuration for the distributed coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// How long locks are valid before expiring.
    pub lock_timeout: Duration,
    /// How often to check for expired locks.
    pub lock_cleanup_interval: Duration,
    /// Maximum concurrent operations per type.
    pub max_concurrent_recoveries: usize,
    /// Maximum concurrent rebalancing operations.
    pub max_concurrent_rebalances: usize,
    /// Enable distributed coordination (vs local-only).
    pub distributed: bool,
    /// This node's ID for leader election.
    pub node_id: NodeId,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(300),
            lock_cleanup_interval: Duration::from_secs(30),
            max_concurrent_recoveries: 10,
            max_concurrent_rebalances: 5,
            distributed: false,
            node_id: 0,
        }
    }
}

/// Types of distributed locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LockType {
    /// Lock for chunk recovery operations.
    ChunkRecovery(ChunkId),
    /// Lock for server-level operations.
    ServerOperation(DataServerId),
    /// Global cluster lock for major operations.
    ClusterOperation,
    /// Lock for rebalancing operations.
    Rebalancing,
}

/// Information about a held lock.
#[derive(Debug, Clone)]
struct LockInfo {
    /// Who holds this lock.
    holder: NodeId,
    /// When the lock was acquired (for debugging/auditing).
    #[allow(dead_code)]
    acquired_at: Instant,
    /// When the lock expires.
    expires_at: Instant,
    /// Operation description (for debugging/auditing).
    #[allow(dead_code)]
    operation: String,
}

/// Distributed coordinator for cluster operations.
pub struct ClusterCoordinator {
    config: CoordinatorConfig,
    /// Currently held locks.
    locks: Arc<RwLock<HashMap<LockType, LockInfo>>>,
    /// Pending lock requests.
    pending_requests: Arc<RwLock<HashMap<LockType, Vec<LockRequest>>>>,
    /// Current cluster leader (for distributed mode).
    leader: Arc<RwLock<Option<NodeId>>>,
    /// Channel for lock release notifications.
    release_tx: mpsc::Sender<LockType>,
    /// Statistics.
    stats: Arc<RwLock<CoordinatorStats>>,
    /// Degraded mode status.
    degraded_mode: Arc<RwLock<DegradedModeStatus>>,
}

/// A pending lock request.
#[derive(Debug)]
struct LockRequest {
    /// Who requested the lock (for debugging/auditing).
    #[allow(dead_code)]
    requester: NodeId,
    /// Operation description (for debugging/auditing).
    #[allow(dead_code)]
    operation: String,
    response_tx: mpsc::Sender<LockResult>,
}

/// Result of a lock acquisition attempt.
#[derive(Debug, Clone)]
pub enum LockResult {
    /// Lock was acquired.
    Acquired,
    /// Lock is held by another node.
    Held { holder: NodeId, expires_in: Duration },
    /// Lock acquisition timed out.
    Timeout,
    /// Coordinator is in degraded mode.
    DegradedMode,
}

/// Statistics for coordinator operations.
#[derive(Debug, Default, Clone)]
pub struct CoordinatorStats {
    /// Total lock acquisitions.
    pub locks_acquired: u64,
    /// Lock acquisitions that waited.
    pub locks_waited: u64,
    /// Lock acquisitions that timed out.
    pub locks_timed_out: u64,
    /// Lock releases.
    pub locks_released: u64,
    /// Locks that expired.
    pub locks_expired: u64,
    /// Current active locks.
    pub active_locks: usize,
}

/// Status when cluster is in degraded mode.
#[derive(Debug, Clone)]
pub struct DegradedModeStatus {
    /// Whether degraded mode is active.
    pub active: bool,
    /// Reason for degraded mode.
    pub reason: Option<String>,
    /// When degraded mode started.
    pub started_at: Option<Instant>,
    /// Operations that are disabled.
    pub disabled_operations: HashSet<String>,
    /// Minimum healthy nodes required to exit degraded mode.
    pub min_healthy_nodes: usize,
}

impl Default for DegradedModeStatus {
    fn default() -> Self {
        Self {
            active: false,
            reason: None,
            started_at: None,
            disabled_operations: HashSet::new(),
            min_healthy_nodes: 2,
        }
    }
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator.
    pub fn new(config: CoordinatorConfig) -> Self {
        let (release_tx, release_rx) = mpsc::channel(1000);

        let coordinator = Self {
            config: config.clone(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            leader: Arc::new(RwLock::new(None)),
            release_tx,
            stats: Arc::new(RwLock::new(CoordinatorStats::default())),
            degraded_mode: Arc::new(RwLock::new(DegradedModeStatus::default())),
        };

        // Start background cleanup task
        let locks = Arc::clone(&coordinator.locks);
        let stats = Arc::clone(&coordinator.stats);
        let cleanup_interval = config.lock_cleanup_interval;
        tokio::spawn(Self::cleanup_expired_locks(locks, stats, cleanup_interval));

        // Start lock release handler
        let pending = Arc::clone(&coordinator.pending_requests);
        tokio::spawn(Self::handle_lock_releases(release_rx, pending));

        coordinator
    }

    /// Try to acquire a lock for an operation.
    pub async fn try_acquire_lock(
        &self,
        lock_type: LockType,
        operation: &str,
        wait_timeout: Option<Duration>,
    ) -> Result<LockGuard> {
        // Check degraded mode
        {
            let degraded = self.degraded_mode.read().await;
            if degraded.active && self.is_operation_disabled(&degraded, &lock_type) {
                warn!(
                    lock_type = ?lock_type,
                    reason = ?degraded.reason,
                    "Operation blocked by degraded mode"
                );
                return Err(StrataError::Internal(
                    "Cluster is in degraded mode".to_string(),
                ));
            }
        }

        let now = Instant::now();
        let expires_at = now + self.config.lock_timeout;

        // Try immediate acquisition
        {
            let mut locks = self.locks.write().await;

            // Check if already held
            if let Some(info) = locks.get(&lock_type) {
                if info.expires_at > now {
                    // Lock is held and not expired
                    if wait_timeout.is_none() {
                        return Err(StrataError::Internal(format!(
                            "Lock held by node {}",
                            info.holder
                        )));
                    }
                } else {
                    // Lock expired, remove it
                    locks.remove(&lock_type);
                    let mut stats = self.stats.write().await;
                    stats.locks_expired += 1;
                }
            }

            // Try to acquire
            if !locks.contains_key(&lock_type) {
                let info = LockInfo {
                    holder: self.config.node_id,
                    acquired_at: now,
                    expires_at,
                    operation: operation.to_string(),
                };
                locks.insert(lock_type, info);

                let mut stats = self.stats.write().await;
                stats.locks_acquired += 1;
                stats.active_locks = locks.len();

                debug!(
                    lock_type = ?lock_type,
                    operation,
                    "Lock acquired"
                );

                return Ok(LockGuard {
                    lock_type,
                    release_tx: self.release_tx.clone(),
                });
            }
        }

        // Wait for lock if timeout specified
        if let Some(timeout) = wait_timeout {
            self.wait_for_lock(lock_type, operation, timeout).await
        } else {
            Err(StrataError::Internal("Lock not available".to_string()))
        }
    }

    /// Wait for a lock to become available.
    async fn wait_for_lock(
        &self,
        lock_type: LockType,
        operation: &str,
        timeout: Duration,
    ) -> Result<LockGuard> {
        let (response_tx, mut response_rx) = mpsc::channel(1);

        // Register pending request
        {
            let mut pending = self.pending_requests.write().await;
            pending.entry(lock_type).or_default().push(LockRequest {
                requester: self.config.node_id,
                operation: operation.to_string(),
                response_tx,
            });

            let mut stats = self.stats.write().await;
            stats.locks_waited += 1;
        }

        // Wait for response or timeout
        match tokio::time::timeout(timeout, response_rx.recv()).await {
            Ok(Some(LockResult::Acquired)) => {
                debug!(
                    lock_type = ?lock_type,
                    operation,
                    "Lock acquired after waiting"
                );
                Ok(LockGuard {
                    lock_type,
                    release_tx: self.release_tx.clone(),
                })
            }
            Ok(Some(LockResult::DegradedMode)) => {
                Err(StrataError::Internal("Cluster in degraded mode".to_string()))
            }
            _ => {
                let mut stats = self.stats.write().await;
                stats.locks_timed_out += 1;
                Err(StrataError::Internal("Lock acquisition timeout".to_string()))
            }
        }
    }

    /// Release a lock.
    pub async fn release_lock(&self, lock_type: LockType) {
        let mut locks = self.locks.write().await;
        if locks.remove(&lock_type).is_some() {
            let mut stats = self.stats.write().await;
            stats.locks_released += 1;
            stats.active_locks = locks.len();

            debug!(lock_type = ?lock_type, "Lock released");

            // Notify release handler
            drop(locks);
            let _ = self.release_tx.send(lock_type).await;
        }
    }

    /// Enter degraded mode.
    pub async fn enter_degraded_mode(&self, reason: &str, disabled_ops: Vec<String>) {
        let mut degraded = self.degraded_mode.write().await;
        degraded.active = true;
        degraded.reason = Some(reason.to_string());
        degraded.started_at = Some(Instant::now());
        degraded.disabled_operations = disabled_ops.into_iter().collect();

        warn!(
            reason,
            disabled = ?degraded.disabled_operations,
            "Entering degraded mode"
        );
    }

    /// Exit degraded mode.
    pub async fn exit_degraded_mode(&self) {
        let mut degraded = self.degraded_mode.write().await;
        if degraded.active {
            info!(
                duration = ?degraded.started_at.map(|s| s.elapsed()),
                "Exiting degraded mode"
            );
        }
        degraded.active = false;
        degraded.reason = None;
        degraded.started_at = None;
        degraded.disabled_operations.clear();
    }

    /// Check if cluster is in degraded mode.
    pub async fn is_degraded(&self) -> bool {
        self.degraded_mode.read().await.active
    }

    /// Get degraded mode status.
    pub async fn degraded_status(&self) -> DegradedModeStatus {
        self.degraded_mode.read().await.clone()
    }

    /// Get coordinator statistics.
    pub async fn stats(&self) -> CoordinatorStats {
        self.stats.read().await.clone()
    }

    /// Check if an operation is disabled in degraded mode.
    fn is_operation_disabled(&self, status: &DegradedModeStatus, lock_type: &LockType) -> bool {
        let op_name = match lock_type {
            LockType::ChunkRecovery(_) => "recovery",
            LockType::ServerOperation(_) => "server_ops",
            LockType::ClusterOperation => "cluster_ops",
            LockType::Rebalancing => "rebalancing",
        };
        status.disabled_operations.contains(op_name)
    }

    /// Background task to clean up expired locks.
    async fn cleanup_expired_locks(
        locks: Arc<RwLock<HashMap<LockType, LockInfo>>>,
        stats: Arc<RwLock<CoordinatorStats>>,
        interval: Duration,
    ) {
        let mut timer = tokio::time::interval(interval);

        loop {
            timer.tick().await;

            let now = Instant::now();
            let mut locks = locks.write().await;
            let mut expired_count = 0;

            locks.retain(|_, info| {
                if info.expires_at <= now {
                    expired_count += 1;
                    false
                } else {
                    true
                }
            });

            if expired_count > 0 {
                let mut stats = stats.write().await;
                stats.locks_expired += expired_count;
                stats.active_locks = locks.len();

                debug!(count = expired_count, "Cleaned up expired locks");
            }
        }
    }

    /// Handle lock release notifications.
    async fn handle_lock_releases(
        mut release_rx: mpsc::Receiver<LockType>,
        pending: Arc<RwLock<HashMap<LockType, Vec<LockRequest>>>>,
    ) {
        while let Some(lock_type) = release_rx.recv().await {
            let mut pending = pending.write().await;

            if let Some(requests) = pending.get_mut(&lock_type) {
                if let Some(request) = requests.pop() {
                    // Grant lock to next waiter
                    let _ = request.response_tx.send(LockResult::Acquired).await;
                }

                if requests.is_empty() {
                    pending.remove(&lock_type);
                }
            }
        }
    }

    /// Set the cluster leader.
    pub async fn set_leader(&self, leader_id: Option<NodeId>) {
        let mut leader = self.leader.write().await;
        *leader = leader_id;
        if let Some(id) = leader_id {
            info!(leader_id = id, "Cluster leader updated");
        }
    }

    /// Check if this node is the leader.
    pub async fn is_leader(&self) -> bool {
        let leader = self.leader.read().await;
        *leader == Some(self.config.node_id)
    }

    /// Get the current leader.
    pub async fn get_leader(&self) -> Option<NodeId> {
        *self.leader.read().await
    }
}

/// RAII guard for a held lock.
pub struct LockGuard {
    lock_type: LockType,
    release_tx: mpsc::Sender<LockType>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Best-effort release notification
        let _ = self.release_tx.try_send(self.lock_type);
    }
}

/// Graceful degradation policy.
#[derive(Debug, Clone)]
pub struct DegradationPolicy {
    /// Minimum nodes before entering degraded mode.
    pub min_healthy_nodes: usize,
    /// Whether to allow reads in degraded mode.
    pub allow_degraded_reads: bool,
    /// Whether to allow writes in degraded mode.
    pub allow_degraded_writes: bool,
    /// Operations to disable in degraded mode.
    pub disabled_operations: Vec<String>,
}

impl Default for DegradationPolicy {
    fn default() -> Self {
        Self {
            min_healthy_nodes: 2,
            allow_degraded_reads: true,
            allow_degraded_writes: false,
            disabled_operations: vec!["rebalancing".to_string()],
        }
    }
}

/// Cluster health evaluator for graceful degradation.
pub struct HealthEvaluator {
    policy: DegradationPolicy,
}

impl HealthEvaluator {
    pub fn new(policy: DegradationPolicy) -> Self {
        Self { policy }
    }

    /// Evaluate cluster health and recommend degradation level.
    pub fn evaluate(
        &self,
        healthy_nodes: usize,
        total_nodes: usize,
        under_replicated_chunks: usize,
    ) -> DegradationLevel {
        if healthy_nodes < self.policy.min_healthy_nodes {
            return DegradationLevel::Critical;
        }

        let health_ratio = healthy_nodes as f64 / total_nodes as f64;

        if health_ratio < 0.5 {
            DegradationLevel::Major
        } else if health_ratio < 0.8 || under_replicated_chunks > 100 {
            DegradationLevel::Minor
        } else {
            DegradationLevel::None
        }
    }

    /// Get operations that should be disabled for a degradation level.
    pub fn disabled_operations(&self, level: DegradationLevel) -> Vec<String> {
        match level {
            DegradationLevel::None => vec![],
            DegradationLevel::Minor => vec!["rebalancing".to_string()],
            DegradationLevel::Major => {
                vec!["rebalancing".to_string(), "background_scrub".to_string()]
            }
            DegradationLevel::Critical => vec![
                "rebalancing".to_string(),
                "background_scrub".to_string(),
                "non_critical_recovery".to_string(),
            ],
        }
    }
}

/// Level of cluster degradation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationLevel {
    /// Cluster is fully healthy.
    None,
    /// Minor issues, some operations may be delayed.
    Minor,
    /// Major issues, non-essential operations disabled.
    Major,
    /// Critical issues, only essential operations allowed.
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_release_lock() {
        let config = CoordinatorConfig::default();
        let coordinator = ClusterCoordinator::new(config);

        let chunk_id = ChunkId::new();
        let lock_type = LockType::ChunkRecovery(chunk_id);

        // Acquire lock
        let guard = coordinator
            .try_acquire_lock(lock_type, "test recovery", None)
            .await
            .unwrap();

        // Verify stats
        let stats = coordinator.stats().await;
        assert_eq!(stats.locks_acquired, 1);
        assert_eq!(stats.active_locks, 1);

        // Release lock
        drop(guard);

        // Allow async cleanup
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_degraded_mode() {
        let config = CoordinatorConfig::default();
        let coordinator = ClusterCoordinator::new(config);

        assert!(!coordinator.is_degraded().await);

        coordinator
            .enter_degraded_mode("test", vec!["recovery".to_string()])
            .await;

        assert!(coordinator.is_degraded().await);

        let status = coordinator.degraded_status().await;
        assert!(status.disabled_operations.contains("recovery"));

        coordinator.exit_degraded_mode().await;
        assert!(!coordinator.is_degraded().await);
    }

    #[test]
    fn test_health_evaluator() {
        let evaluator = HealthEvaluator::new(DegradationPolicy::default());

        // Healthy cluster
        let level = evaluator.evaluate(5, 5, 0);
        assert_eq!(level, DegradationLevel::None);

        // Minor degradation
        let level = evaluator.evaluate(4, 5, 50);
        assert_eq!(level, DegradationLevel::None);

        // Major degradation (less than 50% healthy)
        let level = evaluator.evaluate(2, 5, 0);
        assert_eq!(level, DegradationLevel::Major);

        // Critical (below minimum)
        let level = evaluator.evaluate(1, 5, 0);
        assert_eq!(level, DegradationLevel::Critical);
    }
}
