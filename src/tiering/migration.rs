// Data migration manager for tier transitions

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use super::policy::TierType;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Migration job status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    /// Queued for migration
    Pending,
    /// Currently migrating
    InProgress,
    /// Successfully migrated
    Completed,
    /// Migration failed
    Failed,
    /// Migration cancelled
    Cancelled,
}

/// Migration direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationDirection {
    /// Moving to hotter tier (promotion)
    Promote,
    /// Moving to colder tier (demotion)
    Demote,
}

/// Individual migration job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationJob {
    /// Unique job ID
    pub id: String,
    /// Object path
    pub path: String,
    /// Source tier
    pub source_tier: TierType,
    /// Target tier
    pub target_tier: TierType,
    /// Migration direction
    pub direction: MigrationDirection,
    /// Object size in bytes
    pub size: u64,
    /// Current status
    pub status: MigrationStatus,
    /// Job priority (lower = higher priority)
    pub priority: i32,
    /// Creation time
    pub created_at: SystemTime,
    /// Start time
    pub started_at: Option<SystemTime>,
    /// Completion time
    pub completed_at: Option<SystemTime>,
    /// Error message if failed
    pub error: Option<String>,
    /// Bytes transferred so far
    pub bytes_transferred: u64,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Associated chunks
    pub chunks: Vec<ChunkId>,
}

impl MigrationJob {
    /// Creates a new migration job
    pub fn new(
        path: String,
        source_tier: TierType,
        target_tier: TierType,
        size: u64,
        chunks: Vec<ChunkId>,
    ) -> Self {
        let direction = if target_tier.cost_factor() > source_tier.cost_factor() {
            MigrationDirection::Promote
        } else {
            MigrationDirection::Demote
        };

        // Promotions get higher priority
        let priority = match direction {
            MigrationDirection::Promote => 0,
            MigrationDirection::Demote => 10,
        };

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            path,
            source_tier,
            target_tier,
            direction,
            size,
            status: MigrationStatus::Pending,
            priority,
            created_at: SystemTime::now(),
            started_at: None,
            completed_at: None,
            error: None,
            bytes_transferred: 0,
            retry_count: 0,
            chunks,
        }
    }

    /// Progress percentage
    pub fn progress(&self) -> f64 {
        if self.size == 0 {
            return 100.0;
        }
        (self.bytes_transferred as f64 / self.size as f64) * 100.0
    }

    /// Duration of migration
    pub fn duration(&self) -> Option<Duration> {
        let start = self.started_at?;
        let end = self.completed_at.unwrap_or_else(SystemTime::now);
        end.duration_since(start).ok()
    }

    /// Throughput in bytes per second
    pub fn throughput(&self) -> Option<f64> {
        let duration = self.duration()?;
        if duration.as_secs_f64() < 0.001 {
            return None;
        }
        Some(self.bytes_transferred as f64 / duration.as_secs_f64())
    }
}

/// Migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum concurrent migrations
    pub max_concurrent: usize,
    /// Maximum bandwidth (bytes/sec, 0 = unlimited)
    pub max_bandwidth: u64,
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Maximum retries per job
    pub max_retries: u32,
    /// Retry delay
    #[serde(with = "crate::config::humantime_serde")]
    pub retry_delay: Duration,
    /// Queue size limit
    pub max_queue_size: usize,
    /// Enable parallel chunk migration
    pub parallel_chunks: bool,
    /// Verify data after migration
    pub verify_after_migration: bool,
    /// Delete source after successful migration
    pub delete_source_after: bool,
    /// Prioritize promotions over demotions
    pub prioritize_promotions: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_bandwidth: 0, // Unlimited
            batch_size: 100,
            max_retries: 3,
            retry_delay: Duration::from_secs(60),
            max_queue_size: 10000,
            parallel_chunks: true,
            verify_after_migration: true,
            delete_source_after: true,
            prioritize_promotions: true,
        }
    }
}

impl MigrationConfig {
    /// High-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            max_concurrent: 16,
            batch_size: 500,
            parallel_chunks: true,
            verify_after_migration: false, // Skip for speed
            ..Default::default()
        }
    }

    /// Safe/conservative configuration
    pub fn safe() -> Self {
        Self {
            max_concurrent: 2,
            batch_size: 50,
            max_retries: 5,
            verify_after_migration: true,
            delete_source_after: false, // Keep source for safety
            ..Default::default()
        }
    }
}

/// Migration statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationStats {
    pub pending_jobs: u64,
    pub active_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub cancelled_jobs: u64,
    pub total_bytes_migrated: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub average_throughput: f64,
}

/// Tier storage backend trait
#[async_trait::async_trait]
pub trait TierStorage: Send + Sync {
    /// Gets the tier type
    fn tier_type(&self) -> TierType;

    /// Reads data from this tier
    async fn read(&self, chunks: &[ChunkId]) -> Result<Vec<Vec<u8>>>;

    /// Writes data to this tier
    async fn write(&self, chunks: &[(ChunkId, Vec<u8>)]) -> Result<()>;

    /// Deletes data from this tier
    async fn delete(&self, chunks: &[ChunkId]) -> Result<()>;

    /// Verifies data integrity
    async fn verify(&self, chunks: &[ChunkId]) -> Result<Vec<bool>>;

    /// Available capacity in bytes
    async fn available_capacity(&self) -> Result<u64>;
}

/// Migration manager
pub struct MigrationManager {
    config: MigrationConfig,
    queue: Arc<RwLock<VecDeque<MigrationJob>>>,
    active: Arc<RwLock<HashMap<String, MigrationJob>>>,
    history: Arc<RwLock<VecDeque<MigrationJob>>>,
    stats: Arc<RwLock<MigrationStats>>,
    semaphore: Arc<Semaphore>,
    tiers: Arc<RwLock<HashMap<TierType, Arc<dyn TierStorage>>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl MigrationManager {
    /// Creates a new migration manager
    pub fn new(config: MigrationConfig) -> Self {
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);

        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            queue: Arc::new(RwLock::new(VecDeque::new())),
            active: Arc::new(RwLock::new(HashMap::new())),
            history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            stats: Arc::new(RwLock::new(MigrationStats::default())),
            tiers: Arc::new(RwLock::new(HashMap::new())),
            shutdown,
            shutdown_rx,
        }
    }

    /// Registers a tier storage backend
    pub async fn register_tier(&self, storage: Arc<dyn TierStorage>) {
        let mut tiers = self.tiers.write().await;
        let tier_type = storage.tier_type();
        tiers.insert(tier_type, storage);
        info!(tier = ?tier_type, "Registered tier storage");
    }

    /// Queues a migration job
    pub async fn queue_migration(&self, job: MigrationJob) -> Result<String> {
        let mut queue = self.queue.write().await;

        if queue.len() >= self.config.max_queue_size {
            return Err(StrataError::ResourceExhausted(
                "Migration queue full".to_string()
            ));
        }

        let job_id = job.id.clone();

        // Insert in priority order
        if self.config.prioritize_promotions {
            let pos = queue.iter()
                .position(|j| j.priority > job.priority)
                .unwrap_or(queue.len());
            queue.insert(pos, job);
        } else {
            queue.push_back(job);
        }

        let mut stats = self.stats.write().await;
        stats.pending_jobs += 1;

        info!(job_id = %job_id, "Queued migration job");
        Ok(job_id)
    }

    /// Queues multiple migrations
    pub async fn queue_migrations(&self, jobs: Vec<MigrationJob>) -> Result<Vec<String>> {
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            ids.push(self.queue_migration(job).await?);
        }
        Ok(ids)
    }

    /// Gets job status
    pub async fn get_job(&self, job_id: &str) -> Option<MigrationJob> {
        // Check active
        if let Some(job) = self.active.read().await.get(job_id) {
            return Some(job.clone());
        }

        // Check queue
        if let Some(job) = self.queue.read().await.iter().find(|j| j.id == job_id) {
            return Some(job.clone());
        }

        // Check history
        self.history.read().await.iter().find(|j| j.id == job_id).cloned()
    }

    /// Cancels a job
    pub async fn cancel_job(&self, job_id: &str) -> Result<bool> {
        // Try to remove from queue
        let mut queue = self.queue.write().await;
        if let Some(pos) = queue.iter().position(|j| j.id == job_id) {
            let mut job = queue.remove(pos).unwrap();
            job.status = MigrationStatus::Cancelled;
            job.completed_at = Some(SystemTime::now());

            let mut history = self.history.write().await;
            history.push_front(job);

            let mut stats = self.stats.write().await;
            stats.pending_jobs = stats.pending_jobs.saturating_sub(1);
            stats.cancelled_jobs += 1;

            info!(job_id = %job_id, "Cancelled migration job");
            return Ok(true);
        }

        // Can't cancel active jobs easily
        if self.active.read().await.contains_key(job_id) {
            warn!(job_id = %job_id, "Cannot cancel active migration");
            return Ok(false);
        }

        Ok(false)
    }

    /// Gets current statistics
    pub async fn get_stats(&self) -> MigrationStats {
        self.stats.read().await.clone()
    }

    /// Gets queue depth
    pub async fn queue_depth(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Gets active job count
    pub async fn active_count(&self) -> usize {
        self.active.read().await.len()
    }

    /// Runs the migration worker
    pub async fn run(&self) {
        info!("Starting migration manager");
        let mut shutdown_rx = self.shutdown_rx.clone();
        let semaphore = Arc::clone(&self.semaphore);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Migration manager shutting down");
                        break;
                    }
                }
                permit = Arc::clone(&semaphore).acquire_owned() => {
                    let permit = match permit {
                        Ok(p) => p,
                        Err(_) => break,
                    };

                    // Get next job from queue
                    let job = {
                        let mut queue = self.queue.write().await;
                        queue.pop_front()
                    };

                    if let Some(mut job) = job {
                        // Move to active
                        job.status = MigrationStatus::InProgress;
                        job.started_at = Some(SystemTime::now());

                        {
                            let mut active = self.active.write().await;
                            active.insert(job.id.clone(), job.clone());
                        }
                        {
                            let mut stats = self.stats.write().await;
                            stats.pending_jobs = stats.pending_jobs.saturating_sub(1);
                            stats.active_jobs += 1;
                        }

                        // Execute migration
                        let tiers = Arc::clone(&self.tiers);
                        let active = Arc::clone(&self.active);
                        let history = Arc::clone(&self.history);
                        let stats = Arc::clone(&self.stats);
                        let config = self.config.clone();

                        tokio::spawn(async move {
                            let result = Self::execute_migration(&job, &tiers, &config).await;

                            // Update job status
                            let mut completed_job = job.clone();
                            completed_job.completed_at = Some(SystemTime::now());

                            match result {
                                Ok(bytes) => {
                                    completed_job.status = MigrationStatus::Completed;
                                    completed_job.bytes_transferred = bytes;

                                    let mut stats = stats.write().await;
                                    stats.completed_jobs += 1;
                                    stats.total_bytes_migrated += bytes;
                                    match completed_job.direction {
                                        MigrationDirection::Promote => stats.promotions += 1,
                                        MigrationDirection::Demote => stats.demotions += 1,
                                    }

                                    info!(
                                        job_id = %completed_job.id,
                                        path = %completed_job.path,
                                        bytes = bytes,
                                        "Migration completed"
                                    );
                                }
                                Err(e) => {
                                    completed_job.status = MigrationStatus::Failed;
                                    completed_job.error = Some(e.to_string());

                                    let mut stats = stats.write().await;
                                    stats.failed_jobs += 1;

                                    error!(
                                        job_id = %completed_job.id,
                                        path = %completed_job.path,
                                        error = %e,
                                        "Migration failed"
                                    );
                                }
                            }

                            // Move from active to history
                            {
                                let mut active = active.write().await;
                                active.remove(&completed_job.id);
                            }
                            {
                                let mut stats = stats.write().await;
                                stats.active_jobs = stats.active_jobs.saturating_sub(1);
                            }
                            {
                                let mut history = history.write().await;
                                history.push_front(completed_job);
                                while history.len() > 1000 {
                                    history.pop_back();
                                }
                            }

                            drop(permit);
                        });
                    } else {
                        // No jobs, release permit and wait
                        drop(permit);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    /// Executes a single migration
    async fn execute_migration(
        job: &MigrationJob,
        tiers: &Arc<RwLock<HashMap<TierType, Arc<dyn TierStorage>>>>,
        config: &MigrationConfig,
    ) -> Result<u64> {
        let tiers = tiers.read().await;

        let source = tiers.get(&job.source_tier)
            .ok_or_else(|| StrataError::NotFound(format!("Source tier {:?}", job.source_tier)))?;
        let target = tiers.get(&job.target_tier)
            .ok_or_else(|| StrataError::NotFound(format!("Target tier {:?}", job.target_tier)))?;

        debug!(
            job_id = %job.id,
            path = %job.path,
            source = ?job.source_tier,
            target = ?job.target_tier,
            chunks = job.chunks.len(),
            "Executing migration"
        );

        // Check target capacity
        let available = target.available_capacity().await?;
        if available < job.size {
            return Err(StrataError::ResourceExhausted(
                format!("Insufficient capacity on {:?} tier", job.target_tier)
            ));
        }

        // Read from source
        let data = source.read(&job.chunks).await?;
        let total_bytes: u64 = data.iter().map(|d| d.len() as u64).sum();

        // Write to target
        let chunks_with_data: Vec<_> = job.chunks.iter()
            .cloned()
            .zip(data.into_iter())
            .collect();
        target.write(&chunks_with_data).await?;

        // Verify if configured
        if config.verify_after_migration {
            let verified = target.verify(&job.chunks).await?;
            if verified.iter().any(|v| !v) {
                return Err(StrataError::DataCorruption(
                    "Verification failed after migration".to_string()
                ));
            }
        }

        // Delete from source if configured
        if config.delete_source_after {
            source.delete(&job.chunks).await?;
        }

        Ok(total_bytes)
    }

    /// Shuts down the manager
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Gets pending jobs
    pub async fn get_pending_jobs(&self) -> Vec<MigrationJob> {
        self.queue.read().await.iter().cloned().collect()
    }

    /// Gets active jobs
    pub async fn get_active_jobs(&self) -> Vec<MigrationJob> {
        self.active.read().await.values().cloned().collect()
    }

    /// Gets recent history
    pub async fn get_history(&self, limit: usize) -> Vec<MigrationJob> {
        self.history.read().await
            .iter()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Estimates time to complete queue
    pub async fn estimate_completion(&self) -> Option<Duration> {
        let stats = self.stats.read().await;
        if stats.average_throughput < 1.0 {
            return None;
        }

        let queue = self.queue.read().await;
        let total_bytes: u64 = queue.iter().map(|j| j.size).sum();

        let seconds = total_bytes as f64 / stats.average_throughput;
        Some(Duration::from_secs_f64(seconds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_job_creation() {
        let job = MigrationJob::new(
            "/test/file.txt".to_string(),
            TierType::Hot,
            TierType::Cold,
            1024,
            vec![ChunkId::new()],
        );

        assert_eq!(job.direction, MigrationDirection::Demote);
        assert_eq!(job.status, MigrationStatus::Pending);
        assert!(job.priority > 0); // Demotions have lower priority
    }

    #[test]
    fn test_promotion_job() {
        let job = MigrationJob::new(
            "/test/hot.txt".to_string(),
            TierType::Cold,
            TierType::Hot,
            1024,
            vec![ChunkId::new()],
        );

        assert_eq!(job.direction, MigrationDirection::Promote);
        assert_eq!(job.priority, 0); // Promotions have highest priority
    }

    #[tokio::test]
    async fn test_queue_management() {
        let manager = MigrationManager::new(MigrationConfig::default());

        let job1 = MigrationJob::new(
            "/file1.txt".to_string(),
            TierType::Hot,
            TierType::Cold,
            100,
            vec![],
        );
        let job2 = MigrationJob::new(
            "/file2.txt".to_string(),
            TierType::Cold,
            TierType::Hot,
            200,
            vec![],
        );

        let id1 = manager.queue_migration(job1).await.unwrap();
        let id2 = manager.queue_migration(job2).await.unwrap();

        // Promotion should be first (higher priority)
        let pending = manager.get_pending_jobs().await;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, id2); // Promotion first
        assert_eq!(pending[1].id, id1);

        let stats = manager.get_stats().await;
        assert_eq!(stats.pending_jobs, 2);
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let manager = MigrationManager::new(MigrationConfig::default());

        let job = MigrationJob::new(
            "/test.txt".to_string(),
            TierType::Hot,
            TierType::Cold,
            100,
            vec![],
        );
        let id = manager.queue_migration(job).await.unwrap();

        let cancelled = manager.cancel_job(&id).await.unwrap();
        assert!(cancelled);

        let stats = manager.get_stats().await;
        assert_eq!(stats.pending_jobs, 0);
        assert_eq!(stats.cancelled_jobs, 1);
    }
}
