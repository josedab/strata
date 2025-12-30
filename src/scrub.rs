//! Background data scrubbing for Strata.
//!
//! Proactively verifies data integrity and repairs corruption before data loss.
//! Performs continuous verification of stored shards against checksums.

use crate::error::{Result, StrataError};
use crate::types::ChunkId;

/// Shard index type.
pub type ShardIndex = u32;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Scrub priority level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrubPriority {
    /// Low priority - background scrubbing.
    Low,
    /// Normal priority - regular scheduled scrubs.
    #[default]
    Normal,
    /// High priority - user requested or suspected corruption.
    High,
    /// Critical - immediate verification needed.
    Critical,
}

/// Result of scrubbing a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardScrubResult {
    /// Shard is healthy.
    Healthy,
    /// Checksum mismatch detected.
    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    /// Shard is missing.
    Missing,
    /// Shard is corrupted (unreadable).
    Corrupted {
        error: String,
    },
    /// Shard was repaired.
    Repaired,
    /// Repair failed.
    RepairFailed {
        error: String,
    },
}

impl ShardScrubResult {
    /// Check if the result indicates a problem.
    pub fn is_error(&self) -> bool {
        !matches!(self, ShardScrubResult::Healthy | ShardScrubResult::Repaired)
    }
}

/// Result of scrubbing a chunk (all shards).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkScrubResult {
    /// Chunk ID.
    pub chunk_id: ChunkId,
    /// When the scrub started.
    pub started_at: DateTime<Utc>,
    /// When the scrub completed.
    pub completed_at: DateTime<Utc>,
    /// Results for each shard.
    pub shard_results: HashMap<ShardIndex, ShardScrubResult>,
    /// Overall status.
    pub status: ChunkScrubStatus,
    /// Bytes verified.
    pub bytes_verified: u64,
    /// Number of shards repaired.
    pub shards_repaired: usize,
}

impl ChunkScrubResult {
    /// Check if all shards are healthy.
    pub fn is_healthy(&self) -> bool {
        self.status == ChunkScrubStatus::Healthy
    }

    /// Get count of healthy shards.
    pub fn healthy_count(&self) -> usize {
        self.shard_results
            .values()
            .filter(|r| matches!(r, ShardScrubResult::Healthy | ShardScrubResult::Repaired))
            .count()
    }

    /// Get count of corrupted shards.
    pub fn corrupted_count(&self) -> usize {
        self.shard_results.values().filter(|r| r.is_error()).count()
    }
}

/// Overall status of a chunk scrub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkScrubStatus {
    /// All shards verified successfully.
    Healthy,
    /// Some shards had issues but were repaired.
    Repaired,
    /// Corruption detected but still recoverable.
    Degraded,
    /// Data loss detected (too many shards corrupted).
    DataLoss,
    /// Scrub failed to complete.
    Failed,
}

/// Scrub configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrubConfig {
    /// Whether scrubbing is enabled.
    pub enabled: bool,
    /// How often to run full scrubs (in hours).
    pub full_scrub_interval_hours: u64,
    /// Maximum concurrent scrub operations.
    pub max_concurrent: usize,
    /// I/O throttle - max bytes per second (0 = unlimited).
    pub throttle_bytes_per_sec: u64,
    /// Pause between chunks to reduce I/O impact.
    pub pause_between_chunks_ms: u64,
    /// Automatically repair detected corruption.
    pub auto_repair: bool,
    /// Maximum repair attempts per shard.
    pub max_repair_attempts: usize,
    /// Priority threshold for immediate scrubbing.
    pub priority_threshold: ScrubPriority,
    /// Time window for scrubbing (start hour, 0-23).
    pub scrub_window_start: u8,
    /// Time window for scrubbing (end hour, 0-23).
    pub scrub_window_end: u8,
    /// Enable scrub window restriction.
    pub use_scrub_window: bool,
}

impl Default for ScrubConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            full_scrub_interval_hours: 168, // Weekly
            max_concurrent: 4,
            throttle_bytes_per_sec: 50 * 1024 * 1024, // 50 MB/s
            pause_between_chunks_ms: 100,
            auto_repair: true,
            max_repair_attempts: 3,
            priority_threshold: ScrubPriority::Normal,
            scrub_window_start: 2,  // 2 AM
            scrub_window_end: 6,    // 6 AM
            use_scrub_window: false,
        }
    }
}

impl ScrubConfig {
    /// Aggressive scrubbing configuration.
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            full_scrub_interval_hours: 24, // Daily
            max_concurrent: 8,
            throttle_bytes_per_sec: 0, // Unlimited
            pause_between_chunks_ms: 0,
            auto_repair: true,
            max_repair_attempts: 5,
            priority_threshold: ScrubPriority::Low,
            use_scrub_window: false,
            ..Default::default()
        }
    }

    /// Conservative scrubbing configuration.
    pub fn conservative() -> Self {
        Self {
            enabled: true,
            full_scrub_interval_hours: 720, // Monthly
            max_concurrent: 2,
            throttle_bytes_per_sec: 10 * 1024 * 1024, // 10 MB/s
            pause_between_chunks_ms: 500,
            auto_repair: true,
            max_repair_attempts: 3,
            priority_threshold: ScrubPriority::High,
            use_scrub_window: true,
            ..Default::default()
        }
    }

    /// Check if we're in the scrub window.
    pub fn in_scrub_window(&self) -> bool {
        if !self.use_scrub_window {
            return true;
        }

        let hour = Utc::now().format("%H").to_string().parse::<u8>().unwrap_or(0);

        if self.scrub_window_start <= self.scrub_window_end {
            hour >= self.scrub_window_start && hour < self.scrub_window_end
        } else {
            // Window spans midnight
            hour >= self.scrub_window_start || hour < self.scrub_window_end
        }
    }
}

/// A scrub task.
#[derive(Debug, Clone)]
pub struct ScrubTask {
    /// Chunk to scrub.
    pub chunk_id: ChunkId,
    /// Priority level.
    pub priority: ScrubPriority,
    /// When the task was queued.
    pub queued_at: Instant,
    /// Specific shards to scrub (empty = all).
    pub specific_shards: HashSet<ShardIndex>,
    /// Whether to attempt repair.
    pub repair: bool,
}

impl ScrubTask {
    /// Create a new scrub task.
    pub fn new(chunk_id: ChunkId) -> Self {
        Self {
            chunk_id,
            priority: ScrubPriority::Normal,
            queued_at: Instant::now(),
            specific_shards: HashSet::new(),
            repair: true,
        }
    }

    /// Set priority.
    pub fn with_priority(mut self, priority: ScrubPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set specific shards to scrub.
    pub fn with_shards(mut self, shards: HashSet<ShardIndex>) -> Self {
        self.specific_shards = shards;
        self
    }

    /// Disable repair.
    pub fn without_repair(mut self) -> Self {
        self.repair = false;
        self
    }
}

/// Scrub event.
#[derive(Debug, Clone)]
pub enum ScrubEvent {
    /// Scrub started for a chunk.
    Started { chunk_id: ChunkId },
    /// Scrub completed for a chunk.
    Completed { result: ChunkScrubResult },
    /// Corruption detected.
    CorruptionDetected {
        chunk_id: ChunkId,
        shard: ShardIndex,
        result: ShardScrubResult,
    },
    /// Repair attempted.
    RepairAttempted {
        chunk_id: ChunkId,
        shard: ShardIndex,
        success: bool,
    },
    /// Full scrub cycle started.
    FullScrubStarted { total_chunks: usize },
    /// Full scrub cycle completed.
    FullScrubCompleted { stats: ScrubStats },
}

/// Scrub statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScrubStats {
    /// Total chunks scrubbed.
    pub chunks_scrubbed: u64,
    /// Total shards scrubbed.
    pub shards_scrubbed: u64,
    /// Total bytes verified.
    pub bytes_verified: u64,
    /// Healthy chunks.
    pub healthy_chunks: u64,
    /// Chunks with repaired shards.
    pub repaired_chunks: u64,
    /// Degraded chunks (corruption but recoverable).
    pub degraded_chunks: u64,
    /// Chunks with data loss.
    pub data_loss_chunks: u64,
    /// Total shards repaired.
    pub shards_repaired: u64,
    /// Total repair failures.
    pub repair_failures: u64,
    /// Scrub errors.
    pub errors: u64,
    /// Duration in seconds.
    pub duration_secs: u64,
    /// Last full scrub completion time.
    pub last_full_scrub: Option<DateTime<Utc>>,
    /// Current scrub progress (0-100).
    pub progress_percent: f64,
}

/// Trait for shard verification.
#[async_trait::async_trait]
pub trait ShardVerifier: Send + Sync {
    /// Verify a shard's integrity.
    async fn verify_shard(
        &self,
        chunk_id: &ChunkId,
        shard_index: ShardIndex,
    ) -> Result<ShardScrubResult>;

    /// Repair a corrupted shard.
    async fn repair_shard(
        &self,
        chunk_id: &ChunkId,
        shard_index: ShardIndex,
    ) -> Result<ShardScrubResult>;

    /// Get all shard indices for a chunk.
    async fn get_shard_indices(&self, chunk_id: &ChunkId) -> Result<Vec<ShardIndex>>;

    /// List all chunks.
    async fn list_chunks(&self) -> Result<Vec<ChunkId>>;
}

/// The scrub manager.
pub struct ScrubManager {
    config: ScrubConfig,
    verifier: Arc<dyn ShardVerifier>,
    queue: RwLock<VecDeque<ScrubTask>>,
    in_progress: RwLock<HashSet<ChunkId>>,
    results: RwLock<VecDeque<ChunkScrubResult>>,
    stats: RwLock<ScrubStats>,
    events: broadcast::Sender<ScrubEvent>,
    running: AtomicBool,
    paused: AtomicBool,
    semaphore: Arc<Semaphore>,
}

impl ScrubManager {
    /// Create a new scrub manager.
    pub fn new(config: ScrubConfig, verifier: Arc<dyn ShardVerifier>) -> Arc<Self> {
        let (events, _) = broadcast::channel(1000);

        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            verifier,
            queue: RwLock::new(VecDeque::new()),
            in_progress: RwLock::new(HashSet::new()),
            results: RwLock::new(VecDeque::with_capacity(1000)),
            stats: RwLock::new(ScrubStats::default()),
            events,
            running: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        })
    }

    /// Start the scrub manager.
    pub async fn start(self: Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("Scrub manager already running");
            return;
        }

        info!("Starting scrub manager");

        // Start background workers
        let manager = Arc::clone(&self);
        tokio::spawn(async move {
            manager.run_scheduler().await;
        });

        let manager = Arc::clone(&self);
        tokio::spawn(async move {
            manager.run_worker().await;
        });
    }

    /// Stop the scrub manager.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("Scrub manager stopped");
    }

    /// Pause scrubbing.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        info!("Scrubbing paused");
    }

    /// Resume scrubbing.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        info!("Scrubbing resumed");
    }

    /// Check if scrubbing is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    async fn run_scheduler(&self) {
        let interval_secs = self.config.full_scrub_interval_hours * 3600;
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;

            if !self.config.enabled || self.is_paused() {
                continue;
            }

            if !self.config.in_scrub_window() {
                debug!("Outside scrub window, skipping scheduled scrub");
                continue;
            }

            info!("Starting scheduled full scrub");
            if let Err(e) = self.queue_full_scrub().await {
                error!(?e, "Failed to queue full scrub");
            }
        }
    }

    async fn run_worker(&self) {
        while self.running.load(Ordering::SeqCst) {
            if self.is_paused() || !self.config.enabled {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Get next task
            let task = {
                let mut queue = self.queue.write().await;

                // Sort by priority (higher priority first)
                let mut tasks: Vec<_> = queue.drain(..).collect();
                tasks.sort_by(|a, b| b.priority.cmp(&a.priority));
                *queue = tasks.into_iter().collect();

                queue.pop_front()
            };

            if let Some(task) = task {
                // Check if already in progress
                {
                    let in_progress = self.in_progress.read().await;
                    if in_progress.contains(&task.chunk_id) {
                        continue;
                    }
                }

                // Acquire semaphore permit
                let _permit = self.semaphore.acquire().await.unwrap();

                // Mark as in progress
                {
                    let mut in_progress = self.in_progress.write().await;
                    in_progress.insert(task.chunk_id.clone());
                }

                // Process the task
                let result = self.scrub_chunk(&task).await;

                // Remove from in progress
                {
                    let mut in_progress = self.in_progress.write().await;
                    in_progress.remove(&task.chunk_id);
                }

                // Store result
                if let Ok(result) = result {
                    let mut results = self.results.write().await;
                    if results.len() >= 1000 {
                        results.pop_front();
                    }
                    results.push_back(result.clone());

                    let _ = self.events.send(ScrubEvent::Completed { result });
                }

                // Throttle
                if self.config.pause_between_chunks_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(
                        self.config.pause_between_chunks_ms,
                    ))
                    .await;
                }
            } else {
                // No tasks, wait a bit
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    async fn scrub_chunk(&self, task: &ScrubTask) -> Result<ChunkScrubResult> {
        let started_at = Utc::now();
        let _ = self.events.send(ScrubEvent::Started {
            chunk_id: task.chunk_id.clone(),
        });

        // Get shards to scrub
        let shards = if task.specific_shards.is_empty() {
            self.verifier.get_shard_indices(&task.chunk_id).await?
        } else {
            task.specific_shards.iter().cloned().collect()
        };

        let mut shard_results = HashMap::new();
        let bytes_verified = 0u64; // TODO: Track actual bytes from verifier
        let mut shards_repaired = 0usize;

        for shard in shards {
            // Verify shard
            let result = self.verifier.verify_shard(&task.chunk_id, shard).await?;

            // Handle corruption
            if result.is_error() {
                let _ = self.events.send(ScrubEvent::CorruptionDetected {
                    chunk_id: task.chunk_id.clone(),
                    shard,
                    result: result.clone(),
                });

                // Attempt repair if configured
                if task.repair && self.config.auto_repair {
                    let mut repaired = false;

                    for attempt in 0..self.config.max_repair_attempts {
                        debug!(chunk_id = ?task.chunk_id, shard, attempt, "Attempting repair");

                        match self.verifier.repair_shard(&task.chunk_id, shard).await {
                            Ok(ShardScrubResult::Repaired) => {
                                shards_repaired += 1;
                                repaired = true;
                                let _ = self.events.send(ScrubEvent::RepairAttempted {
                                    chunk_id: task.chunk_id.clone(),
                                    shard,
                                    success: true,
                                });
                                shard_results.insert(shard, ShardScrubResult::Repaired);
                                break;
                            }
                            Ok(result) => {
                                shard_results.insert(shard, result);
                                break;
                            }
                            Err(e) => {
                                warn!(?e, chunk_id = ?task.chunk_id, shard, attempt, "Repair attempt failed");
                                if attempt == self.config.max_repair_attempts - 1 {
                                    let _ = self.events.send(ScrubEvent::RepairAttempted {
                                        chunk_id: task.chunk_id.clone(),
                                        shard,
                                        success: false,
                                    });
                                    shard_results.insert(
                                        shard,
                                        ShardScrubResult::RepairFailed {
                                            error: e.to_string(),
                                        },
                                    );
                                }
                            }
                        }
                    }

                    if !repaired && !shard_results.contains_key(&shard) {
                        shard_results.insert(shard, result);
                    }
                } else {
                    shard_results.insert(shard, result);
                }
            } else {
                shard_results.insert(shard, result);
            }
        }

        // Determine overall status
        let healthy_count = shard_results
            .values()
            .filter(|r| matches!(r, ShardScrubResult::Healthy))
            .count();
        let repaired_count = shard_results
            .values()
            .filter(|r| matches!(r, ShardScrubResult::Repaired))
            .count();
        let error_count = shard_results.values().filter(|r| r.is_error()).count();
        let total = shard_results.len();

        let status = if healthy_count == total {
            ChunkScrubStatus::Healthy
        } else if healthy_count + repaired_count == total {
            ChunkScrubStatus::Repaired
        } else if error_count < total / 2 {
            // Assuming we can tolerate up to half shards missing with erasure coding
            ChunkScrubStatus::Degraded
        } else {
            ChunkScrubStatus::DataLoss
        };

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.chunks_scrubbed += 1;
            stats.shards_scrubbed += total as u64;
            stats.bytes_verified += bytes_verified;

            match status {
                ChunkScrubStatus::Healthy => stats.healthy_chunks += 1,
                ChunkScrubStatus::Repaired => stats.repaired_chunks += 1,
                ChunkScrubStatus::Degraded => stats.degraded_chunks += 1,
                ChunkScrubStatus::DataLoss => stats.data_loss_chunks += 1,
                ChunkScrubStatus::Failed => stats.errors += 1,
            }

            stats.shards_repaired += shards_repaired as u64;
        }

        let completed_at = Utc::now();

        Ok(ChunkScrubResult {
            chunk_id: task.chunk_id.clone(),
            started_at,
            completed_at,
            shard_results,
            status,
            bytes_verified,
            shards_repaired,
        })
    }

    /// Queue a single chunk for scrubbing.
    pub async fn queue_chunk(&self, task: ScrubTask) {
        let mut queue = self.queue.write().await;
        queue.push_back(task);
    }

    /// Queue a full scrub of all chunks.
    pub async fn queue_full_scrub(&self) -> Result<usize> {
        let chunks = self.verifier.list_chunks().await?;
        let total = chunks.len();

        let _ = self.events.send(ScrubEvent::FullScrubStarted { total_chunks: total });

        {
            let mut queue = self.queue.write().await;
            for chunk_id in chunks {
                queue.push_back(ScrubTask::new(chunk_id));
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.progress_percent = 0.0;
        }

        Ok(total)
    }

    /// Get scrub statistics.
    pub async fn stats(&self) -> ScrubStats {
        self.stats.read().await.clone()
    }

    /// Get recent scrub results.
    pub async fn recent_results(&self, limit: usize) -> Vec<ChunkScrubResult> {
        let results = self.results.read().await;
        results.iter().rev().take(limit).cloned().collect()
    }

    /// Get queue length.
    pub async fn queue_length(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Subscribe to scrub events.
    pub fn subscribe(&self) -> broadcast::Receiver<ScrubEvent> {
        self.events.subscribe()
    }

    /// Check if a chunk is currently being scrubbed.
    pub async fn is_scrubbing(&self, chunk_id: &ChunkId) -> bool {
        self.in_progress.read().await.contains(chunk_id)
    }

    /// Get result for a specific chunk.
    pub async fn get_result(&self, chunk_id: &ChunkId) -> Option<ChunkScrubResult> {
        let results = self.results.read().await;
        results.iter().rev().find(|r| &r.chunk_id == chunk_id).cloned()
    }
}

/// Simple in-memory verifier for testing.
pub struct MockShardVerifier {
    chunks: RwLock<HashMap<ChunkId, Vec<ShardIndex>>>,
    corrupted: RwLock<HashSet<(ChunkId, ShardIndex)>>,
}

impl MockShardVerifier {
    /// Create a new mock verifier.
    pub fn new() -> Self {
        Self {
            chunks: RwLock::new(HashMap::new()),
            corrupted: RwLock::new(HashSet::new()),
        }
    }

    /// Add a chunk.
    pub async fn add_chunk(&self, chunk_id: ChunkId, shards: Vec<ShardIndex>) {
        let mut chunks = self.chunks.write().await;
        chunks.insert(chunk_id, shards);
    }

    /// Mark a shard as corrupted.
    pub async fn mark_corrupted(&self, chunk_id: ChunkId, shard: ShardIndex) {
        let mut corrupted = self.corrupted.write().await;
        corrupted.insert((chunk_id, shard));
    }

    /// Clear corruption.
    pub async fn clear_corruption(&self, chunk_id: &ChunkId, shard: ShardIndex) {
        let mut corrupted = self.corrupted.write().await;
        corrupted.remove(&(chunk_id.clone(), shard));
    }
}

impl Default for MockShardVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ShardVerifier for MockShardVerifier {
    async fn verify_shard(
        &self,
        chunk_id: &ChunkId,
        shard_index: ShardIndex,
    ) -> Result<ShardScrubResult> {
        let corrupted = self.corrupted.read().await;
        if corrupted.contains(&(chunk_id.clone(), shard_index)) {
            Ok(ShardScrubResult::ChecksumMismatch {
                expected: 12345,
                actual: 0,
            })
        } else {
            Ok(ShardScrubResult::Healthy)
        }
    }

    async fn repair_shard(
        &self,
        chunk_id: &ChunkId,
        shard_index: ShardIndex,
    ) -> Result<ShardScrubResult> {
        // Simulate repair by clearing corruption
        let mut corrupted = self.corrupted.write().await;
        corrupted.remove(&(chunk_id.clone(), shard_index));
        Ok(ShardScrubResult::Repaired)
    }

    async fn get_shard_indices(&self, chunk_id: &ChunkId) -> Result<Vec<ShardIndex>> {
        let chunks = self.chunks.read().await;
        chunks
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| StrataError::ChunkNotFound(chunk_id.to_string()))
    }

    async fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        let chunks = self.chunks.read().await;
        Ok(chunks.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scrub_config_defaults() {
        let config = ScrubConfig::default();
        assert!(config.enabled);
        assert!(config.auto_repair);
        assert_eq!(config.max_concurrent, 4);
    }

    #[test]
    fn test_scrub_config_presets() {
        let aggressive = ScrubConfig::aggressive();
        assert_eq!(aggressive.full_scrub_interval_hours, 24);
        assert_eq!(aggressive.throttle_bytes_per_sec, 0);

        let conservative = ScrubConfig::conservative();
        assert_eq!(conservative.full_scrub_interval_hours, 720);
        assert!(conservative.use_scrub_window);
    }

    #[test]
    fn test_shard_scrub_result_is_error() {
        assert!(!ShardScrubResult::Healthy.is_error());
        assert!(!ShardScrubResult::Repaired.is_error());
        assert!(ShardScrubResult::Missing.is_error());
        assert!(ShardScrubResult::ChecksumMismatch {
            expected: 1,
            actual: 2
        }
        .is_error());
    }

    #[test]
    fn test_scrub_task_builder() {
        let chunk_id = ChunkId::new();
        let task = ScrubTask::new(chunk_id.clone())
            .with_priority(ScrubPriority::High)
            .without_repair();

        assert_eq!(task.chunk_id, chunk_id);
        assert_eq!(task.priority, ScrubPriority::High);
        assert!(!task.repair);
    }

    #[tokio::test]
    async fn test_mock_verifier() {
        let verifier = MockShardVerifier::new();
        let chunk_id = ChunkId::new();

        verifier.add_chunk(chunk_id.clone(), vec![0, 1, 2, 3]).await;

        // All shards should be healthy
        let result = verifier.verify_shard(&chunk_id, 0).await.unwrap();
        assert!(matches!(result, ShardScrubResult::Healthy));

        // Mark shard as corrupted
        verifier.mark_corrupted(chunk_id.clone(), 1).await;

        let result = verifier.verify_shard(&chunk_id, 1).await.unwrap();
        assert!(result.is_error());

        // Repair shard
        let result = verifier.repair_shard(&chunk_id, 1).await.unwrap();
        assert!(matches!(result, ShardScrubResult::Repaired));

        // Should be healthy now
        let result = verifier.verify_shard(&chunk_id, 1).await.unwrap();
        assert!(matches!(result, ShardScrubResult::Healthy));
    }

    #[tokio::test]
    async fn test_scrub_manager_queue() {
        let verifier = Arc::new(MockShardVerifier::new());
        let chunk_1 = ChunkId::new();
        let chunk_2 = ChunkId::new();
        verifier.add_chunk(chunk_1.clone(), vec![0, 1, 2]).await;
        verifier.add_chunk(chunk_2.clone(), vec![0, 1, 2]).await;

        let manager = ScrubManager::new(ScrubConfig::default(), verifier);

        manager.queue_chunk(ScrubTask::new(chunk_1)).await;
        manager.queue_chunk(ScrubTask::new(chunk_2)).await;

        assert_eq!(manager.queue_length().await, 2);
    }

    #[tokio::test]
    async fn test_scrub_manager_full_scrub() {
        let verifier = Arc::new(MockShardVerifier::new());
        let chunk_1 = ChunkId::new();
        let chunk_2 = ChunkId::new();
        let chunk_3 = ChunkId::new();
        verifier.add_chunk(chunk_1, vec![0, 1, 2]).await;
        verifier.add_chunk(chunk_2, vec![0, 1, 2]).await;
        verifier.add_chunk(chunk_3, vec![0, 1, 2]).await;

        let manager = ScrubManager::new(ScrubConfig::default(), verifier);

        let count = manager.queue_full_scrub().await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(manager.queue_length().await, 3);
    }

    #[tokio::test]
    async fn test_chunk_scrub_result() {
        let mut result = ChunkScrubResult {
            chunk_id: ChunkId::new(),
            started_at: Utc::now(),
            completed_at: Utc::now(),
            shard_results: HashMap::new(),
            status: ChunkScrubStatus::Healthy,
            bytes_verified: 1024,
            shards_repaired: 0,
        };

        result.shard_results.insert(0, ShardScrubResult::Healthy);
        result.shard_results.insert(1, ShardScrubResult::Healthy);
        result.shard_results.insert(2, ShardScrubResult::Repaired);

        assert_eq!(result.healthy_count(), 3);
        assert_eq!(result.corrupted_count(), 0);
    }

    #[test]
    fn test_scrub_priority_ordering() {
        assert!(ScrubPriority::Critical > ScrubPriority::High);
        assert!(ScrubPriority::High > ScrubPriority::Normal);
        assert!(ScrubPriority::Normal > ScrubPriority::Low);
    }
}
