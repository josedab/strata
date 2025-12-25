//! Recovery manager for self-healing chunk replication.

use super::PlacementEngine;
use crate::types::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::warn;

/// Recovery manager for maintaining chunk redundancy.
pub struct RecoveryManager {
    /// Placement engine for choosing recovery targets.
    placement: PlacementEngine,
    /// Minimum shards per chunk for recovery to be needed.
    min_shards: usize,
    /// Recovery check interval.
    check_interval: Duration,
    /// Maximum concurrent recoveries.
    max_concurrent: usize,
}

impl RecoveryManager {
    pub fn new(placement: PlacementEngine, erasure_config: ErasureCodingConfig) -> Self {
        Self {
            placement,
            min_shards: erasure_config.data_shards,
            check_interval: Duration::from_secs(60),
            max_concurrent: 10,
        }
    }

    /// Find chunks that are under-replicated.
    pub fn find_under_replicated(
        &self,
        chunks: &HashMap<ChunkId, ChunkMeta>,
        servers: &HashMap<DataServerId, DataServerInfo>,
    ) -> Vec<ChunkId> {
        let online_servers: HashSet<_> = servers
            .values()
            .filter(|s| s.status == ServerStatus::Online)
            .map(|s| s.id)
            .collect();

        chunks
            .iter()
            .filter(|(_, meta)| {
                let available = meta
                    .locations
                    .iter()
                    .filter(|id| online_servers.contains(id))
                    .count();
                available < self.min_shards
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Create a recovery plan for under-replicated chunks.
    pub fn create_recovery_plan(
        &self,
        chunks: &[ChunkId],
        chunk_meta: &HashMap<ChunkId, ChunkMeta>,
        servers: &[DataServerInfo],
    ) -> Vec<RecoveryTask> {
        let mut tasks = Vec::new();

        for &chunk_id in chunks {
            if let Some(meta) = chunk_meta.get(&chunk_id) {
                // Find missing shards
                let _existing: HashSet<_> = meta.locations.iter().copied().collect();

                // Choose recovery targets
                match self.placement.choose_recovery_target(servers, &meta.locations) {
                    Ok(target) => {
                        tasks.push(RecoveryTask {
                            chunk_id,
                            source_locations: meta.locations.clone(),
                            target_server: target,
                            priority: self.calculate_priority(meta, servers),
                        });
                    }
                    Err(e) => {
                        warn!(
                            chunk_id = %chunk_id,
                            error = %e,
                            "Cannot create recovery task"
                        );
                    }
                }
            }
        }

        // Sort by priority (higher priority first)
        tasks.sort_by(|a, b| b.priority.cmp(&a.priority));

        tasks
    }

    /// Calculate recovery priority for a chunk.
    fn calculate_priority(
        &self,
        meta: &ChunkMeta,
        servers: &[DataServerInfo],
    ) -> u32 {
        let online_servers: HashSet<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online)
            .map(|s| s.id)
            .collect();

        let available = meta
            .locations
            .iter()
            .filter(|id| online_servers.contains(id))
            .count();

        // Higher priority for fewer available copies
        if available == 0 {
            1000 // Critical - no copies available
        } else if available == 1 {
            100 // Urgent - only one copy
        } else if available < self.min_shards {
            10 // Important - under minimum
        } else {
            1 // Normal
        }
    }
}

/// A recovery task for a single chunk.
#[derive(Debug, Clone)]
pub struct RecoveryTask {
    /// The chunk to recover.
    pub chunk_id: ChunkId,
    /// Current locations of shards.
    pub source_locations: Vec<DataServerId>,
    /// Target server for new shard.
    pub target_server: DataServerId,
    /// Priority (higher = more urgent).
    pub priority: u32,
}

impl RecoveryTask {
    pub fn is_critical(&self) -> bool {
        self.priority >= 100
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn make_servers(n: usize, offline: &[usize]) -> HashMap<DataServerId, DataServerInfo> {
        (0..n as u64)
            .map(|i| {
                let status = if offline.contains(&(i as usize)) {
                    ServerStatus::Offline
                } else {
                    ServerStatus::Online
                };
                (
                    i + 1,
                    DataServerInfo {
                        id: i + 1,
                        address: format!("127.0.0.1:{}", 9000 + i),
                        capacity: 1_000_000_000,
                        used: 0,
                        status,
                        last_heartbeat: SystemTime::now(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_find_under_replicated() {
        let placement = PlacementEngine::default();
        let config = ErasureCodingConfig::SMALL_CLUSTER; // 2+1
        let recovery = RecoveryManager::new(placement, config);

        let servers = make_servers(4, &[2]); // Server 3 is offline

        let chunk_id = ChunkId::new();
        let mut chunks = HashMap::new();
        chunks.insert(chunk_id, ChunkMeta {
            id: chunk_id,
            size: 1000,
            checksum: 0,
            locations: vec![2, 3], // One server offline
            version: 1,
        });

        let under_rep = recovery.find_under_replicated(&chunks, &servers);
        assert_eq!(under_rep.len(), 1);
        assert_eq!(under_rep[0], chunk_id);
    }

    #[test]
    fn test_recovery_priority() {
        let placement = PlacementEngine::default();
        let config = ErasureCodingConfig::SMALL_CLUSTER;
        let recovery = RecoveryManager::new(placement, config);

        let servers: Vec<_> = make_servers(4, &[]).values().cloned().collect();

        // Chunk with only 1 available copy - should be high priority
        let chunk_id = ChunkId::new();
        let meta = ChunkMeta {
            id: chunk_id,
            size: 1000,
            checksum: 0,
            locations: vec![1], // Only one copy
            version: 1,
        };

        let priority = recovery.calculate_priority(&meta, &servers);
        assert_eq!(priority, 100);
    }
}
