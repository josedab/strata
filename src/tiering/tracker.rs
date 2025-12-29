// Access pattern tracking for tiering decisions

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Access type for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessType {
    Read,
    Write,
    Metadata,
    List,
}

/// Access record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessRecord {
    pub timestamp: SystemTime,
    pub access_type: AccessType,
    pub bytes_transferred: u64,
    pub latency_ms: u32,
    pub client_id: Option<String>,
}

/// Object access statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectAccessStats {
    /// Object path/key
    pub path: String,
    /// Total access count
    pub total_accesses: u64,
    /// Read count
    pub read_count: u64,
    /// Write count
    pub write_count: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// First access time
    pub first_access: SystemTime,
    /// Last access time
    pub last_access: SystemTime,
    /// Recent access records (limited history)
    pub recent_accesses: VecDeque<AccessRecord>,
    /// Access pattern score (higher = more active)
    pub heat_score: f64,
}

impl ObjectAccessStats {
    /// Creates new stats for an object
    pub fn new(path: String) -> Self {
        let now = SystemTime::now();
        Self {
            path,
            total_accesses: 0,
            read_count: 0,
            write_count: 0,
            bytes_read: 0,
            bytes_written: 0,
            first_access: now,
            last_access: now,
            recent_accesses: VecDeque::with_capacity(100),
            heat_score: 0.0,
        }
    }

    /// Records an access
    pub fn record_access(&mut self, record: AccessRecord) {
        self.total_accesses += 1;
        self.last_access = record.timestamp;

        match record.access_type {
            AccessType::Read => {
                self.read_count += 1;
                self.bytes_read += record.bytes_transferred;
            }
            AccessType::Write => {
                self.write_count += 1;
                self.bytes_written += record.bytes_transferred;
            }
            AccessType::Metadata | AccessType::List => {}
        }

        // Keep limited history
        if self.recent_accesses.len() >= 100 {
            self.recent_accesses.pop_front();
        }
        self.recent_accesses.push_back(record);

        // Update heat score
        self.update_heat_score();
    }

    /// Updates the heat score based on access patterns
    fn update_heat_score(&mut self) {
        let now = SystemTime::now();
        let mut score = 0.0;

        for access in &self.recent_accesses {
            let age = now.duration_since(access.timestamp)
                .unwrap_or(Duration::from_secs(0));

            // Exponential decay - recent accesses count more
            let decay = (-age.as_secs_f64() / 86400.0).exp(); // 1-day half-life

            // Weight by access type
            let weight = match access.access_type {
                AccessType::Read => 1.0,
                AccessType::Write => 2.0, // Writes indicate more active use
                AccessType::Metadata => 0.1,
                AccessType::List => 0.1,
            };

            score += decay * weight;
        }

        self.heat_score = score;
    }

    /// Gets access count in a time window
    pub fn accesses_in_window(&self, window: Duration) -> u64 {
        let now = SystemTime::now();
        let cutoff = now - window;

        self.recent_accesses.iter()
            .filter(|r| r.timestamp >= cutoff)
            .count() as u64
    }

    /// Calculates access frequency (per day)
    pub fn access_frequency(&self) -> f64 {
        let age = SystemTime::now()
            .duration_since(self.first_access)
            .unwrap_or(Duration::from_secs(1));

        let days = age.as_secs_f64() / 86400.0;
        if days < 0.001 {
            return 0.0;
        }

        self.total_accesses as f64 / days
    }
}

/// Configuration for access tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerConfig {
    /// Maximum objects to track
    pub max_tracked_objects: usize,
    /// Maximum history per object
    pub max_history_per_object: usize,
    /// How long to keep access records
    #[serde(with = "crate::config::humantime_serde")]
    pub retention_period: Duration,
    /// Aggregation interval
    #[serde(with = "crate::config::humantime_serde")]
    pub aggregation_interval: Duration,
    /// Enable sampling for high-traffic objects
    pub enable_sampling: bool,
    /// Sample rate (1 = 100%, 10 = 10%, etc.)
    pub sample_rate: u32,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            max_tracked_objects: 1_000_000,
            max_history_per_object: 100,
            retention_period: Duration::from_secs(30 * 24 * 3600), // 30 days
            aggregation_interval: Duration::from_secs(3600), // 1 hour
            enable_sampling: true,
            sample_rate: 1, // Track everything
        }
    }
}

/// Access tracker for tiering decisions
pub struct AccessTracker {
    config: TrackerConfig,
    stats: Arc<RwLock<HashMap<String, ObjectAccessStats>>>,
    aggregated: Arc<RwLock<AggregatedStats>>,
    sample_counter: Arc<std::sync::atomic::AtomicU64>,
}

/// Aggregated statistics across all objects
#[derive(Debug, Clone, Default)]
pub struct AggregatedStats {
    pub total_objects_tracked: u64,
    pub total_accesses: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub hot_objects: u64,  // heat_score > 10
    pub warm_objects: u64, // heat_score > 1
    pub cold_objects: u64, // heat_score > 0.1
    pub frozen_objects: u64, // heat_score <= 0.1
    pub last_aggregation: Option<SystemTime>,
}

impl AccessTracker {
    /// Creates a new access tracker
    pub fn new(config: TrackerConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(HashMap::new())),
            aggregated: Arc::new(RwLock::new(AggregatedStats::default())),
            sample_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Creates a tracker with default config
    pub fn default_tracker() -> Self {
        Self::new(TrackerConfig::default())
    }

    /// Records an access event
    pub async fn record(&self, path: &str, access_type: AccessType, bytes: u64, latency_ms: u32) -> Result<()> {
        // Sampling logic
        if self.config.enable_sampling && self.config.sample_rate > 1 {
            let count = self.sample_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count % self.config.sample_rate as u64 != 0 {
                return Ok(());
            }
        }

        let record = AccessRecord {
            timestamp: SystemTime::now(),
            access_type,
            bytes_transferred: bytes,
            latency_ms,
            client_id: None,
        };

        let mut stats = self.stats.write().await;

        // Check capacity
        if stats.len() >= self.config.max_tracked_objects && !stats.contains_key(path) {
            // Evict coldest object
            self.evict_coldest(&mut stats);
        }

        let obj_stats = stats
            .entry(path.to_string())
            .or_insert_with(|| ObjectAccessStats::new(path.to_string()));

        obj_stats.record_access(record);

        debug!(path = %path, access_type = ?access_type, "Recorded access");

        Ok(())
    }

    /// Records a read access
    pub async fn record_read(&self, path: &str, bytes: u64, latency_ms: u32) -> Result<()> {
        self.record(path, AccessType::Read, bytes, latency_ms).await
    }

    /// Records a write access
    pub async fn record_write(&self, path: &str, bytes: u64, latency_ms: u32) -> Result<()> {
        self.record(path, AccessType::Write, bytes, latency_ms).await
    }

    /// Gets stats for an object
    pub async fn get_stats(&self, path: &str) -> Option<ObjectAccessStats> {
        let stats = self.stats.read().await;
        stats.get(path).cloned()
    }

    /// Gets heat score for an object
    pub async fn get_heat_score(&self, path: &str) -> f64 {
        let stats = self.stats.read().await;
        stats.get(path).map(|s| s.heat_score).unwrap_or(0.0)
    }

    /// Gets all objects above a heat threshold
    pub async fn get_hot_objects(&self, min_heat: f64) -> Vec<String> {
        let stats = self.stats.read().await;
        stats.iter()
            .filter(|(_, s)| s.heat_score >= min_heat)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Gets all objects below a heat threshold
    pub async fn get_cold_objects(&self, max_heat: f64) -> Vec<String> {
        let stats = self.stats.read().await;
        stats.iter()
            .filter(|(_, s)| s.heat_score <= max_heat)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Gets objects not accessed within a duration
    pub async fn get_untouched_objects(&self, not_accessed_for: Duration) -> Vec<String> {
        let now = SystemTime::now();
        let cutoff = now - not_accessed_for;

        let stats = self.stats.read().await;
        stats.iter()
            .filter(|(_, s)| s.last_access < cutoff)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Gets objects with access pattern matching criteria
    pub async fn find_objects(
        &self,
        min_accesses: Option<u64>,
        max_accesses: Option<u64>,
        min_heat: Option<f64>,
        max_heat: Option<f64>,
        prefix: Option<&str>,
        limit: usize,
    ) -> Vec<ObjectAccessStats> {
        let stats = self.stats.read().await;

        stats.values()
            .filter(|s| {
                if let Some(min) = min_accesses {
                    if s.total_accesses < min {
                        return false;
                    }
                }
                if let Some(max) = max_accesses {
                    if s.total_accesses > max {
                        return false;
                    }
                }
                if let Some(min) = min_heat {
                    if s.heat_score < min {
                        return false;
                    }
                }
                if let Some(max) = max_heat {
                    if s.heat_score > max {
                        return false;
                    }
                }
                if let Some(p) = prefix {
                    if !s.path.starts_with(p) {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Aggregates statistics
    pub async fn aggregate(&self) -> AggregatedStats {
        let stats = self.stats.read().await;

        let mut agg = AggregatedStats {
            total_objects_tracked: stats.len() as u64,
            total_accesses: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            hot_objects: 0,
            warm_objects: 0,
            cold_objects: 0,
            frozen_objects: 0,
            last_aggregation: Some(SystemTime::now()),
        };

        for obj in stats.values() {
            agg.total_accesses += obj.total_accesses;
            agg.total_bytes_read += obj.bytes_read;
            agg.total_bytes_written += obj.bytes_written;

            if obj.heat_score > 10.0 {
                agg.hot_objects += 1;
            } else if obj.heat_score > 1.0 {
                agg.warm_objects += 1;
            } else if obj.heat_score > 0.1 {
                agg.cold_objects += 1;
            } else {
                agg.frozen_objects += 1;
            }
        }

        // Update cached aggregation
        let mut cached = self.aggregated.write().await;
        *cached = agg.clone();

        info!(
            objects = agg.total_objects_tracked,
            hot = agg.hot_objects,
            warm = agg.warm_objects,
            cold = agg.cold_objects,
            frozen = agg.frozen_objects,
            "Aggregated access statistics"
        );

        agg
    }

    /// Gets cached aggregated stats
    pub async fn get_aggregated_stats(&self) -> AggregatedStats {
        self.aggregated.read().await.clone()
    }

    /// Cleans up old records
    pub async fn cleanup(&self) {
        let cutoff = SystemTime::now() - self.config.retention_period;
        let mut stats = self.stats.write().await;

        let before = stats.len();
        stats.retain(|_, v| v.last_access >= cutoff);
        let removed = before - stats.len();

        if removed > 0 {
            info!(removed = removed, "Cleaned up stale access records");
        }
    }

    /// Evicts the coldest object from tracking
    fn evict_coldest(&self, stats: &mut HashMap<String, ObjectAccessStats>) {
        if let Some((coldest, _)) = stats.iter()
            .min_by(|a, b| a.1.heat_score.partial_cmp(&b.1.heat_score).unwrap())
        {
            let coldest = coldest.clone();
            stats.remove(&coldest);
            debug!(path = %coldest, "Evicted coldest object from tracking");
        }
    }

    /// Exports stats for persistence
    pub async fn export(&self) -> Vec<ObjectAccessStats> {
        let stats = self.stats.read().await;
        stats.values().cloned().collect()
    }

    /// Imports stats from persistence
    pub async fn import(&self, data: Vec<ObjectAccessStats>) -> Result<()> {
        let mut stats = self.stats.write().await;

        for obj in data {
            stats.insert(obj.path.clone(), obj);
        }

        info!(count = stats.len(), "Imported access statistics");
        Ok(())
    }

    /// Starts background cleanup task
    pub fn start_cleanup_task(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                self.cleanup().await;
                self.aggregate().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_access_tracking() {
        let tracker = AccessTracker::default_tracker();

        // Record some accesses
        tracker.record_read("/test/file.txt", 1024, 10).await.unwrap();
        tracker.record_read("/test/file.txt", 2048, 15).await.unwrap();
        tracker.record_write("/test/file.txt", 512, 20).await.unwrap();

        let stats = tracker.get_stats("/test/file.txt").await.unwrap();
        assert_eq!(stats.total_accesses, 3);
        assert_eq!(stats.read_count, 2);
        assert_eq!(stats.write_count, 1);
        assert_eq!(stats.bytes_read, 3072);
        assert_eq!(stats.bytes_written, 512);
        assert!(stats.heat_score > 0.0);
    }

    #[tokio::test]
    async fn test_heat_classification() {
        let tracker = AccessTracker::default_tracker();

        // Create hot object (many accesses)
        for _ in 0..50 {
            tracker.record_read("/hot/file.txt", 100, 5).await.unwrap();
        }

        // Create cold object (one access)
        tracker.record_read("/cold/file.txt", 100, 5).await.unwrap();

        let hot_objects = tracker.get_hot_objects(10.0).await;
        assert!(hot_objects.contains(&"/hot/file.txt".to_string()));
        assert!(!hot_objects.contains(&"/cold/file.txt".to_string()));
    }

    #[tokio::test]
    async fn test_aggregation() {
        let tracker = AccessTracker::default_tracker();

        tracker.record_read("/a.txt", 100, 5).await.unwrap();
        tracker.record_read("/b.txt", 200, 10).await.unwrap();
        tracker.record_write("/c.txt", 300, 15).await.unwrap();

        let agg = tracker.aggregate().await;
        assert_eq!(agg.total_objects_tracked, 3);
        assert_eq!(agg.total_accesses, 3);
        assert_eq!(agg.total_bytes_read, 300);
        assert_eq!(agg.total_bytes_written, 300);
    }
}
