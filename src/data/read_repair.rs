//! Read repair for data consistency.
//!
//! Read repair is a mechanism to fix data inconsistencies discovered during read operations.
//! When reading from multiple replicas, if inconsistencies are detected, the system
//! automatically repairs the stale replicas with the latest data.

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, DataServerId, VectorClock, VectorClockOrdering, VersionedValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Configuration for read repair operations.
#[derive(Debug, Clone)]
pub struct ReadRepairConfig {
    /// Enable read repair.
    pub enabled: bool,
    /// Maximum concurrent repairs.
    pub max_concurrent_repairs: usize,
    /// Timeout for repair operations.
    pub repair_timeout: Duration,
    /// Whether to repair synchronously (blocking) or asynchronously.
    pub sync_repair: bool,
    /// Minimum replicas to read from for verification.
    pub read_replicas: usize,
}

impl Default for ReadRepairConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_repairs: 100,
            repair_timeout: Duration::from_secs(30),
            sync_repair: false,
            read_replicas: 2,
        }
    }
}

/// Read result with version information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult<T> {
    /// The value read.
    pub value: T,
    /// The version (vector clock) of the value.
    pub version: VectorClock,
    /// Which server the value was read from.
    pub source: DataServerId,
    /// Whether repair was needed.
    pub repair_needed: bool,
}

/// Repair task for asynchronous repair.
#[derive(Debug)]
pub struct RepairTask {
    /// Chunk to repair.
    pub chunk_id: ChunkId,
    /// Shard index.
    pub shard_index: usize,
    /// Target servers to repair.
    pub targets: Vec<DataServerId>,
    /// Source server with correct data.
    pub source: DataServerId,
    /// Version of the correct data.
    pub version: VectorClock,
}

/// Read repair coordinator.
pub struct ReadRepairCoordinator {
    config: ReadRepairConfig,
    /// Pending repairs queue.
    repair_queue: mpsc::Sender<RepairTask>,
    /// Active repairs (to avoid duplicates).
    active_repairs: Arc<RwLock<HashMap<(ChunkId, usize), RepairTask>>>,
    /// Statistics.
    stats: Arc<RwLock<ReadRepairStats>>,
}

/// Statistics for read repair operations.
#[derive(Debug, Default, Clone)]
pub struct ReadRepairStats {
    /// Total repairs initiated.
    pub repairs_initiated: u64,
    /// Successful repairs.
    pub repairs_succeeded: u64,
    /// Failed repairs.
    pub repairs_failed: u64,
    /// Repairs skipped (already in progress).
    pub repairs_skipped: u64,
    /// Total inconsistencies detected.
    pub inconsistencies_detected: u64,
}

impl ReadRepairCoordinator {
    /// Create a new read repair coordinator.
    pub fn new(config: ReadRepairConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.max_concurrent_repairs);

        let coordinator = Self {
            config: config.clone(),
            repair_queue: tx,
            active_repairs: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ReadRepairStats::default())),
        };

        // Start repair worker
        let active = Arc::clone(&coordinator.active_repairs);
        let stats = Arc::clone(&coordinator.stats);
        tokio::spawn(Self::repair_worker(rx, active, stats, config));

        coordinator
    }

    /// Compare values from multiple replicas and determine if repair is needed.
    pub fn compare_replicas<T: Clone + PartialEq>(
        &self,
        values: &[(DataServerId, VersionedValue<T>)],
    ) -> Option<RepairDecision<T>> {
        if values.is_empty() {
            return None;
        }

        if values.len() == 1 {
            return Some(RepairDecision {
                winner: values[0].clone(),
                losers: Vec::new(),
                needs_repair: false,
            });
        }

        // Find the most recent version
        let mut winner = &values[0];
        let mut conflicts = Vec::new();

        for item in values.iter().skip(1) {
            match winner.1.clock.compare(&item.1.clock) {
                VectorClockOrdering::Before => {
                    // Current winner is older, this item is newer
                    conflicts.push(winner.clone());
                    winner = item;
                }
                VectorClockOrdering::After => {
                    // This item is older
                    conflicts.push(item.clone());
                }
                VectorClockOrdering::Equal => {
                    // Same version, no conflict
                }
                VectorClockOrdering::Concurrent => {
                    // Concurrent updates - use timestamp as tiebreaker
                    if item.1.timestamp > winner.1.timestamp {
                        conflicts.push(winner.clone());
                        winner = item;
                    } else if item.1.timestamp < winner.1.timestamp {
                        conflicts.push(item.clone());
                    } else {
                        // Same timestamp, use origin node ID as final tiebreaker
                        if item.1.origin > winner.1.origin {
                            conflicts.push(winner.clone());
                            winner = item;
                        } else {
                            conflicts.push(item.clone());
                        }
                    }
                }
            }
        }

        let needs_repair = !conflicts.is_empty();
        if needs_repair {
            let _ = self.stats.try_write().map(|mut s| {
                s.inconsistencies_detected += 1;
            });
        }

        Some(RepairDecision {
            winner: winner.clone(),
            losers: conflicts,
            needs_repair,
        })
    }

    /// Schedule a repair task.
    pub async fn schedule_repair(&self, task: RepairTask) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let key = (task.chunk_id, task.shard_index);

        // Check if already in progress
        {
            let active = self.active_repairs.read().await;
            if active.contains_key(&key) {
                let mut stats = self.stats.write().await;
                stats.repairs_skipped += 1;
                debug!(
                    chunk_id = %task.chunk_id,
                    shard_index = task.shard_index,
                    "Repair already in progress, skipping"
                );
                return Ok(());
            }
        }

        // Add to active repairs
        {
            let mut active = self.active_repairs.write().await;
            active.insert(key, RepairTask {
                chunk_id: task.chunk_id,
                shard_index: task.shard_index,
                targets: task.targets.clone(),
                source: task.source,
                version: task.version.clone(),
            });
        }

        // Queue the repair
        self.repair_queue.send(task).await.map_err(|_| {
            StrataError::Internal("Repair queue full".to_string())
        })?;

        let mut stats = self.stats.write().await;
        stats.repairs_initiated += 1;

        Ok(())
    }

    /// Get repair statistics.
    pub async fn stats(&self) -> ReadRepairStats {
        self.stats.read().await.clone()
    }

    /// Background worker that processes repair tasks.
    async fn repair_worker(
        mut rx: mpsc::Receiver<RepairTask>,
        active: Arc<RwLock<HashMap<(ChunkId, usize), RepairTask>>>,
        stats: Arc<RwLock<ReadRepairStats>>,
        config: ReadRepairConfig,
    ) {
        while let Some(task) = rx.recv().await {
            let key = (task.chunk_id, task.shard_index);

            info!(
                chunk_id = %task.chunk_id,
                shard_index = task.shard_index,
                targets = ?task.targets,
                source = task.source,
                "Starting read repair"
            );

            // Execute repair with timeout
            let result = tokio::time::timeout(
                config.repair_timeout,
                Self::execute_repair(&task),
            )
            .await;

            // Update stats and remove from active
            {
                let mut stats = stats.write().await;
                match result {
                    Ok(Ok(())) => {
                        stats.repairs_succeeded += 1;
                        info!(
                            chunk_id = %task.chunk_id,
                            shard_index = task.shard_index,
                            "Read repair completed successfully"
                        );
                    }
                    Ok(Err(e)) => {
                        stats.repairs_failed += 1;
                        error!(
                            chunk_id = %task.chunk_id,
                            shard_index = task.shard_index,
                            error = %e,
                            "Read repair failed"
                        );
                    }
                    Err(_) => {
                        stats.repairs_failed += 1;
                        warn!(
                            chunk_id = %task.chunk_id,
                            shard_index = task.shard_index,
                            "Read repair timed out"
                        );
                    }
                }
            }

            active.write().await.remove(&key);
        }
    }

    /// Execute a repair task.
    async fn execute_repair(task: &RepairTask) -> Result<()> {
        // In a real implementation, this would:
        // 1. Read the correct data from the source server
        // 2. Write it to each target server
        // 3. Update the version on each target

        // For now, this is a placeholder that logs the repair
        debug!(
            chunk_id = %task.chunk_id,
            shard_index = task.shard_index,
            source = task.source,
            targets = ?task.targets,
            "Executing repair (placeholder)"
        );

        // Simulate repair time
        tokio::time::sleep(Duration::from_millis(10)).await;

        Ok(())
    }
}

/// Result of comparing replicas for repair decision.
#[derive(Debug, Clone)]
pub struct RepairDecision<T> {
    /// The winning (most recent) value and its source.
    pub winner: (DataServerId, VersionedValue<T>),
    /// Servers with stale data that need repair.
    pub losers: Vec<(DataServerId, VersionedValue<T>)>,
    /// Whether repair is needed.
    pub needs_repair: bool,
}

impl<T> RepairDecision<T> {
    /// Get the winning value.
    pub fn value(&self) -> &T {
        &self.winner.1.value
    }

    /// Get the winning version.
    pub fn version(&self) -> &VectorClock {
        &self.winner.1.clock
    }

    /// Get servers that need repair.
    pub fn repair_targets(&self) -> Vec<DataServerId> {
        self.losers.iter().map(|(id, _)| *id).collect()
    }
}

/// Quorum read implementation with read repair.
pub async fn quorum_read_with_repair<T, F>(
    servers: &[DataServerId],
    read_fn: F,
    coordinator: &ReadRepairCoordinator,
    chunk_id: ChunkId,
    shard_index: usize,
) -> Result<T>
where
    T: Clone + PartialEq + Send + 'static,
    F: Fn(DataServerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<VersionedValue<T>>> + Send>>,
{
    if servers.is_empty() {
        return Err(StrataError::Internal("No servers available".to_string()));
    }

    // Read from all servers in parallel
    let futures: Vec<_> = servers
        .iter()
        .map(|&server| {
            let future = read_fn(server);
            async move { (server, future.await) }
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(futures).await;

    // Collect successful reads
    let successful: Vec<_> = results
        .into_iter()
        .filter_map(|(server, result)| result.ok().map(|v| (server, v)))
        .collect();

    if successful.is_empty() {
        return Err(StrataError::Internal("All reads failed".to_string()));
    }

    // Compare replicas
    let decision = coordinator
        .compare_replicas(&successful)
        .ok_or_else(|| StrataError::Internal("No values to compare".to_string()))?;

    // Schedule repair if needed
    if decision.needs_repair {
        let task = RepairTask {
            chunk_id,
            shard_index,
            targets: decision.repair_targets(),
            source: decision.winner.0,
            version: decision.version().clone(),
        };
        coordinator.schedule_repair(task).await?;
    }

    Ok(decision.winner.1.value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compare_replicas_single() {
        let config = ReadRepairConfig::default();
        let coordinator = ReadRepairCoordinator::new(config);

        let value = VersionedValue::new("test".to_string(), 1, 100);
        let values = vec![(1, value)];

        let decision = coordinator.compare_replicas(&values).unwrap();
        assert!(!decision.needs_repair);
        assert_eq!(decision.value(), "test");
    }

    #[tokio::test]
    async fn test_compare_replicas_consistent() {
        let config = ReadRepairConfig::default();
        let coordinator = ReadRepairCoordinator::new(config);

        let mut clock = VectorClock::new();
        clock.increment(1);

        let value1 = VersionedValue {
            value: "test".to_string(),
            clock: clock.clone(),
            timestamp: 100,
            origin: 1,
        };
        let value2 = VersionedValue {
            value: "test".to_string(),
            clock: clock.clone(),
            timestamp: 100,
            origin: 1,
        };

        let values = vec![(1, value1), (2, value2)];
        let decision = coordinator.compare_replicas(&values).unwrap();
        assert!(!decision.needs_repair);
    }

    #[tokio::test]
    async fn test_compare_replicas_inconsistent() {
        let config = ReadRepairConfig::default();
        let coordinator = ReadRepairCoordinator::new(config);

        let mut clock1 = VectorClock::new();
        clock1.increment(1);

        let mut clock2 = clock1.clone();
        clock2.increment(1);

        let value1 = VersionedValue {
            value: "old".to_string(),
            clock: clock1,
            timestamp: 100,
            origin: 1,
        };
        let value2 = VersionedValue {
            value: "new".to_string(),
            clock: clock2,
            timestamp: 200,
            origin: 1,
        };

        let values = vec![(1, value1), (2, value2)];
        let decision = coordinator.compare_replicas(&values).unwrap();

        assert!(decision.needs_repair);
        assert_eq!(decision.value(), "new");
        assert_eq!(decision.repair_targets(), vec![1]);
    }

    #[tokio::test]
    async fn test_compare_replicas_concurrent() {
        let config = ReadRepairConfig::default();
        let coordinator = ReadRepairCoordinator::new(config);

        let mut clock1 = VectorClock::new();
        clock1.increment(1);

        let mut clock2 = VectorClock::new();
        clock2.increment(2);

        // Concurrent updates from different nodes
        let value1 = VersionedValue {
            value: "from_node1".to_string(),
            clock: clock1,
            timestamp: 100,
            origin: 1,
        };
        let value2 = VersionedValue {
            value: "from_node2".to_string(),
            clock: clock2,
            timestamp: 200, // Later timestamp wins
            origin: 2,
        };

        let values = vec![(1, value1), (2, value2)];
        let decision = coordinator.compare_replicas(&values).unwrap();

        assert!(decision.needs_repair);
        assert_eq!(decision.value(), "from_node2"); // Later timestamp wins
    }
}
