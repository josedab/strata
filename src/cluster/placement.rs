//! Placement engine for distributing chunks across data servers.

use crate::error::{Result, StrataError};
use crate::types::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};

/// Placement engine for deciding where to store chunk shards.
#[derive(Debug, Clone)]
pub struct PlacementEngine {
    /// Minimum number of replicas per shard.
    min_replicas: usize,
    /// Strategy for placement.
    strategy: PlacementStrategy,
}

/// Placement strategy.
#[derive(Debug, Clone, Copy)]
pub enum PlacementStrategy {
    /// Spread shards across servers randomly.
    Random,
    /// Spread shards to maximize failure domain separation.
    SpreadDomains,
    /// Place shards on least-loaded servers.
    LeastLoaded,
}

impl PlacementEngine {
    pub fn new(min_replicas: usize, strategy: PlacementStrategy) -> Self {
        Self {
            min_replicas,
            strategy,
        }
    }

    /// Choose data servers for storing shards.
    ///
    /// Returns a list of server IDs, one for each shard.
    pub fn choose_locations(
        &self,
        num_shards: usize,
        servers: &[DataServerInfo],
        exclude: &HashSet<DataServerId>,
    ) -> Result<Vec<DataServerId>> {
        // Filter out excluded and offline servers
        let available: Vec<_> = servers
            .iter()
            .filter(|s| s.status == ServerStatus::Online && !exclude.contains(&s.id))
            .collect();

        if available.len() < num_shards {
            return Err(StrataError::PlacementFailed(format!(
                "Not enough servers: need {}, have {}",
                num_shards,
                available.len()
            )));
        }

        match self.strategy {
            PlacementStrategy::Random => self.place_random(&available, num_shards),
            PlacementStrategy::SpreadDomains => self.place_spread(&available, num_shards),
            PlacementStrategy::LeastLoaded => self.place_least_loaded(&available, num_shards),
        }
    }

    /// Choose servers randomly.
    fn place_random(
        &self,
        servers: &[&DataServerInfo],
        num_shards: usize,
    ) -> Result<Vec<DataServerId>> {
        let mut rng = thread_rng();
        let mut selected: Vec<_> = servers.to_vec();
        selected.shuffle(&mut rng);

        Ok(selected.into_iter().take(num_shards).map(|s| s.id).collect())
    }

    /// Place shards to maximize failure domain separation.
    fn place_spread(
        &self,
        servers: &[&DataServerInfo],
        num_shards: usize,
    ) -> Result<Vec<DataServerId>> {
        // For now, same as random but sorted by address to spread geographically
        // In a real implementation, this would consider failure domains (racks, zones, etc.)
        let mut sorted: Vec<_> = servers.to_vec();
        sorted.sort_by_key(|s| &s.address);

        // Distribute evenly across sorted servers
        let mut result = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let idx = (i * sorted.len()) / num_shards;
            result.push(sorted[idx].id);
        }

        Ok(result)
    }

    /// Place shards on least loaded servers.
    fn place_least_loaded(
        &self,
        servers: &[&DataServerInfo],
        num_shards: usize,
    ) -> Result<Vec<DataServerId>> {
        let mut sorted: Vec<_> = servers.to_vec();
        sorted.sort_by(|a, b| {
            let usage_a = a.usage_percent();
            let usage_b = b.usage_percent();
            usage_a.partial_cmp(&usage_b).unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(sorted.into_iter().take(num_shards).map(|s| s.id).collect())
    }

    /// Choose a server for replicating a shard during recovery.
    pub fn choose_recovery_target(
        &self,
        servers: &[DataServerInfo],
        existing_locations: &[DataServerId],
    ) -> Result<DataServerId> {
        let exclude: HashSet<_> = existing_locations.iter().copied().collect();
        let locations = self.choose_locations(1, servers, &exclude)?;
        locations
            .into_iter()
            .next()
            .ok_or_else(|| StrataError::PlacementFailed("No available server".into()))
    }

    /// Calculate the ideal distribution of chunks across servers.
    pub fn calculate_ideal_distribution(
        &self,
        total_chunks: usize,
        servers: &[DataServerInfo],
    ) -> HashMap<DataServerId, usize> {
        let total_capacity: u64 = servers.iter().map(|s| s.capacity).sum();

        if total_capacity == 0 {
            return HashMap::new();
        }

        servers
            .iter()
            .map(|s| {
                let share = (s.capacity as f64 / total_capacity as f64) * total_chunks as f64;
                (s.id, share.round() as usize)
            })
            .collect()
    }
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new(1, PlacementStrategy::LeastLoaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn make_servers(n: usize) -> Vec<DataServerInfo> {
        (0..n as u64)
            .map(|i| DataServerInfo {
                id: i + 1,
                address: format!("127.0.0.1:{}", 9000 + i),
                capacity: 1_000_000_000,
                used: i * 100_000_000,
                status: ServerStatus::Online,
                last_heartbeat: SystemTime::now(),
            })
            .collect()
    }

    #[test]
    fn test_random_placement() {
        let engine = PlacementEngine::new(1, PlacementStrategy::Random);
        let servers = make_servers(5);
        let exclude = HashSet::new();

        let locations = engine.choose_locations(3, &servers, &exclude).unwrap();
        assert_eq!(locations.len(), 3);

        // All locations should be unique
        let unique: HashSet<_> = locations.iter().copied().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_least_loaded_placement() {
        let engine = PlacementEngine::new(1, PlacementStrategy::LeastLoaded);
        let servers = make_servers(5);
        let exclude = HashSet::new();

        let locations = engine.choose_locations(3, &servers, &exclude).unwrap();

        // Should pick the 3 least loaded servers (ids 1, 2, 3)
        assert!(locations.contains(&1));
        assert!(locations.contains(&2));
        assert!(locations.contains(&3));
    }

    #[test]
    fn test_exclude_servers() {
        let engine = PlacementEngine::new(1, PlacementStrategy::Random);
        let servers = make_servers(5);
        let exclude: HashSet<_> = [1, 2, 3].into_iter().collect();

        let locations = engine.choose_locations(2, &servers, &exclude).unwrap();

        // Should only use servers 4 and 5
        for loc in &locations {
            assert!(!exclude.contains(loc));
        }
    }

    #[test]
    fn test_not_enough_servers() {
        let engine = PlacementEngine::new(1, PlacementStrategy::Random);
        let servers = make_servers(2);
        let exclude = HashSet::new();

        let result = engine.choose_locations(5, &servers, &exclude);
        assert!(result.is_err());
    }

    #[test]
    fn test_ideal_distribution() {
        let engine = PlacementEngine::default();
        let servers = make_servers(3);

        let dist = engine.calculate_ideal_distribution(100, &servers);

        // With equal capacity, each server should get ~33 chunks
        let total: usize = dist.values().sum();
        assert!(total >= 99 && total <= 101); // Allow rounding
    }
}
