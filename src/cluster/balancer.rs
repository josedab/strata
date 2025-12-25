//! Data balancer for distributing load across servers.

use super::PlacementEngine;
use crate::types::*;
use std::collections::HashMap;
use tracing::debug;

/// Balancer for redistributing data across servers.
pub struct Balancer {
    /// Placement engine for choosing move targets.
    placement: PlacementEngine,
    /// Threshold for considering a server overloaded (usage percentage).
    high_threshold: f64,
    /// Threshold for considering a server underloaded.
    low_threshold: f64,
    /// Maximum moves per rebalance cycle.
    max_moves: usize,
}

impl Balancer {
    pub fn new(placement: PlacementEngine) -> Self {
        Self {
            placement,
            high_threshold: 80.0,
            low_threshold: 20.0,
            max_moves: 100,
        }
    }

    /// Check if the cluster needs rebalancing.
    pub fn needs_rebalance(&self, servers: &[DataServerInfo]) -> bool {
        if servers.is_empty() {
            return false;
        }

        let usages: Vec<f64> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online)
            .map(|s| s.usage_percent())
            .collect();

        if usages.is_empty() {
            return false;
        }

        let max_usage = usages.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min_usage = usages.iter().cloned().fold(f64::INFINITY, f64::min);

        // Rebalance if difference is too large or any server is critically full
        (max_usage - min_usage) > 30.0 || max_usage > self.high_threshold
    }

    /// Create a rebalance plan.
    pub fn create_rebalance_plan(
        &self,
        chunk_distribution: &HashMap<DataServerId, Vec<ChunkId>>,
        servers: &[DataServerInfo],
    ) -> Vec<MoveOperation> {
        let mut moves = Vec::new();

        // Find overloaded and underloaded servers
        let mut overloaded: Vec<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online && s.usage_percent() > self.high_threshold)
            .collect();

        let mut underloaded: Vec<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online && s.usage_percent() < self.low_threshold)
            .collect();

        // Sort: most overloaded first, least loaded first for targets
        overloaded.sort_by(|a, b| {
            b.usage_percent()
                .partial_cmp(&a.usage_percent())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        underloaded.sort_by(|a, b| {
            a.usage_percent()
                .partial_cmp(&b.usage_percent())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Create moves from overloaded to underloaded
        for source in &overloaded {
            if moves.len() >= self.max_moves {
                break;
            }

            if let Some(chunks) = chunk_distribution.get(&source.id) {
                for chunk_id in chunks.iter().take(10) {
                    if moves.len() >= self.max_moves {
                        break;
                    }

                    // Find a suitable target
                    for target in &underloaded {
                        if target.id != source.id {
                            moves.push(MoveOperation {
                                chunk_id: *chunk_id,
                                source_server: source.id,
                                target_server: target.id,
                            });
                            break;
                        }
                    }
                }
            }
        }

        debug!(num_moves = moves.len(), "Created rebalance plan");
        moves
    }

    /// Calculate cluster balance score (0-100, higher = more balanced).
    pub fn calculate_balance_score(&self, servers: &[DataServerInfo]) -> f64 {
        if servers.is_empty() {
            return 100.0;
        }

        let online: Vec<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online)
            .collect();

        if online.is_empty() {
            return 0.0;
        }

        let usages: Vec<f64> = online.iter().map(|s| s.usage_percent()).collect();

        let mean = usages.iter().sum::<f64>() / usages.len() as f64;
        let variance = usages.iter().map(|u| (u - mean).powi(2)).sum::<f64>() / usages.len() as f64;
        let std_dev = variance.sqrt();

        // Score is 100 - std_dev (capped)
        (100.0 - std_dev).max(0.0).min(100.0)
    }

    /// Get rebalance statistics.
    pub fn get_stats(&self, servers: &[DataServerInfo]) -> BalanceStats {
        let online: Vec<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online)
            .collect();

        if online.is_empty() {
            return BalanceStats::default();
        }

        let usages: Vec<f64> = online.iter().map(|s| s.usage_percent()).collect();
        let total_capacity: u64 = online.iter().map(|s| s.capacity).sum();
        let total_used: u64 = online.iter().map(|s| s.used).sum();

        BalanceStats {
            num_servers: online.len(),
            min_usage: usages.iter().cloned().fold(f64::INFINITY, f64::min),
            max_usage: usages.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            avg_usage: usages.iter().sum::<f64>() / usages.len() as f64,
            total_capacity,
            total_used,
            balance_score: self.calculate_balance_score(servers),
        }
    }
}

/// A chunk move operation.
#[derive(Debug, Clone)]
pub struct MoveOperation {
    /// The chunk to move.
    pub chunk_id: ChunkId,
    /// Source server.
    pub source_server: DataServerId,
    /// Target server.
    pub target_server: DataServerId,
}

/// Balance statistics.
#[derive(Debug, Clone, Default)]
pub struct BalanceStats {
    pub num_servers: usize,
    pub min_usage: f64,
    pub max_usage: f64,
    pub avg_usage: f64,
    pub total_capacity: u64,
    pub total_used: u64,
    pub balance_score: f64,
}

impl Default for Balancer {
    fn default() -> Self {
        Self::new(PlacementEngine::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn make_server(id: u64, used: u64, capacity: u64) -> DataServerInfo {
        DataServerInfo {
            id,
            address: format!("127.0.0.1:{}", 9000 + id),
            capacity,
            used,
            status: ServerStatus::Online,
            last_heartbeat: SystemTime::now(),
        }
    }

    #[test]
    fn test_needs_rebalance() {
        let balancer = Balancer::default();

        // Balanced cluster
        let balanced = vec![
            make_server(1, 500, 1000),
            make_server(2, 480, 1000),
            make_server(3, 520, 1000),
        ];
        assert!(!balancer.needs_rebalance(&balanced));

        // Unbalanced cluster
        let unbalanced = vec![
            make_server(1, 900, 1000),
            make_server(2, 100, 1000),
            make_server(3, 500, 1000),
        ];
        assert!(balancer.needs_rebalance(&unbalanced));
    }

    #[test]
    fn test_balance_score() {
        let balancer = Balancer::default();

        // Perfectly balanced
        let perfect = vec![
            make_server(1, 500, 1000),
            make_server(2, 500, 1000),
            make_server(3, 500, 1000),
        ];
        let score = balancer.calculate_balance_score(&perfect);
        assert!(score > 95.0);

        // Unbalanced
        let unbalanced = vec![
            make_server(1, 900, 1000),
            make_server(2, 100, 1000),
        ];
        let score = balancer.calculate_balance_score(&unbalanced);
        assert!(score < 70.0);
    }

    #[test]
    fn test_get_stats() {
        let balancer = Balancer::default();

        let servers = vec![
            make_server(1, 400, 1000),
            make_server(2, 600, 1000),
        ];

        let stats = balancer.get_stats(&servers);
        assert_eq!(stats.num_servers, 2);
        assert_eq!(stats.total_capacity, 2000);
        assert_eq!(stats.total_used, 1000);
        assert_eq!(stats.avg_usage, 50.0);
    }
}
