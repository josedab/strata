//! Garbage collection for orphaned chunks in Strata.
//!
//! Provides mechanisms to identify and clean up chunks that are no longer
//! referenced by any files.

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use std::collections::{HashMap, HashSet};
// Atomic operations reserved for future optimization
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Garbage collection configuration.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// How often to run GC scans.
    pub scan_interval: Duration,
    /// Minimum age before a chunk is considered for GC.
    pub min_age: Duration,
    /// Maximum chunks to delete per GC cycle.
    pub batch_size: usize,
    /// Enable dry run mode (report but don't delete).
    pub dry_run: bool,
    /// Grace period after a chunk becomes unreferenced.
    pub grace_period: Duration,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(3600), // 1 hour
            min_age: Duration::from_secs(86400),      // 24 hours
            batch_size: 1000,
            dry_run: false,
            grace_period: Duration::from_secs(3600), // 1 hour grace period
        }
    }
}

impl GcConfig {
    /// Aggressive GC configuration.
    pub fn aggressive() -> Self {
        Self {
            scan_interval: Duration::from_secs(300), // 5 minutes
            min_age: Duration::from_secs(3600),      // 1 hour
            batch_size: 5000,
            dry_run: false,
            grace_period: Duration::from_secs(300), // 5 minute grace period
        }
    }

    /// Conservative GC configuration.
    pub fn conservative() -> Self {
        Self {
            scan_interval: Duration::from_secs(86400), // 24 hours
            min_age: Duration::from_secs(604800),      // 7 days
            batch_size: 100,
            dry_run: false,
            grace_period: Duration::from_secs(86400), // 24 hour grace period
        }
    }
}

/// Chunk reference tracking.
#[derive(Debug, Clone)]
pub struct ChunkReference {
    /// Chunk ID.
    pub chunk_id: ChunkId,
    /// Inode IDs that reference this chunk.
    pub references: HashSet<u64>,
    /// When this chunk was first seen.
    pub first_seen: Instant,
    /// When this chunk became unreferenced (if applicable).
    pub unreferenced_at: Option<Instant>,
}

impl ChunkReference {
    /// Create a new chunk reference.
    pub fn new(chunk_id: ChunkId) -> Self {
        let now = Instant::now();
        Self {
            chunk_id,
            references: HashSet::new(),
            first_seen: now,
            // Starts unreferenced since no references yet
            unreferenced_at: Some(now),
        }
    }

    /// Add a reference.
    pub fn add_reference(&mut self, inode: u64) {
        self.references.insert(inode);
        self.unreferenced_at = None;
    }

    /// Remove a reference.
    pub fn remove_reference(&mut self, inode: u64) {
        self.references.remove(&inode);
        if self.references.is_empty() && self.unreferenced_at.is_none() {
            self.unreferenced_at = Some(Instant::now());
        }
    }

    /// Check if the chunk is orphaned.
    pub fn is_orphan(&self) -> bool {
        self.references.is_empty()
    }

    /// Check if the chunk is eligible for GC.
    pub fn is_gc_eligible(&self, min_age: Duration, grace_period: Duration) -> bool {
        if !self.is_orphan() {
            return false;
        }

        // Must be old enough
        if self.first_seen.elapsed() < min_age {
            return false;
        }

        // Must have been unreferenced long enough
        if let Some(unreferenced_at) = self.unreferenced_at {
            unreferenced_at.elapsed() >= grace_period
        } else {
            false
        }
    }
}

/// Garbage collection statistics.
#[derive(Debug, Clone, Default)]
pub struct GcStats {
    /// Number of GC cycles run.
    pub cycles: u64,
    /// Total chunks scanned.
    pub chunks_scanned: u64,
    /// Total orphans found.
    pub orphans_found: u64,
    /// Total chunks deleted.
    pub chunks_deleted: u64,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Last GC cycle duration.
    pub last_cycle_duration: Duration,
    /// Last GC cycle timestamp.
    pub last_cycle_time: Option<Instant>,
    /// Errors during GC.
    pub errors: u64,
}

/// Result of a GC scan.
#[derive(Debug, Clone)]
pub struct GcScanResult {
    /// Chunks identified as orphans.
    pub orphans: Vec<ChunkId>,
    /// Chunks deleted (if not dry run).
    pub deleted: Vec<ChunkId>,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Errors encountered.
    pub errors: Vec<String>,
    /// Duration of the scan.
    pub duration: Duration,
}

/// Chunk garbage collector.
pub struct ChunkGarbageCollector {
    /// Configuration.
    config: GcConfig,
    /// Chunk reference tracking.
    references: RwLock<HashMap<ChunkId, ChunkReference>>,
    /// Statistics.
    stats: RwLock<GcStats>,
    /// Whether GC is currently running.
    running: RwLock<bool>,
}

impl ChunkGarbageCollector {
    /// Create a new garbage collector.
    pub fn new(config: GcConfig) -> Self {
        Self {
            config,
            references: RwLock::new(HashMap::new()),
            stats: RwLock::new(GcStats::default()),
            running: RwLock::new(false),
        }
    }

    /// Register a chunk reference.
    pub async fn add_reference(&self, chunk_id: ChunkId, inode: u64) {
        let mut refs = self.references.write().await;
        refs.entry(chunk_id)
            .or_insert_with(|| ChunkReference::new(chunk_id))
            .add_reference(inode);
    }

    /// Remove a chunk reference.
    pub async fn remove_reference(&self, chunk_id: ChunkId, inode: u64) {
        let mut refs = self.references.write().await;
        if let Some(ref_entry) = refs.get_mut(&chunk_id) {
            ref_entry.remove_reference(inode);
        }
    }

    /// Track a new chunk.
    pub async fn track_chunk(&self, chunk_id: ChunkId) {
        let mut refs = self.references.write().await;
        refs.entry(chunk_id)
            .or_insert_with(|| ChunkReference::new(chunk_id));
    }

    /// Remove chunk from tracking (after deletion).
    pub async fn untrack_chunk(&self, chunk_id: ChunkId) {
        let mut refs = self.references.write().await;
        refs.remove(&chunk_id);
    }

    /// Find orphaned chunks eligible for deletion.
    pub async fn find_orphans(&self) -> Vec<ChunkId> {
        let refs = self.references.read().await;
        refs.values()
            .filter(|r| r.is_gc_eligible(self.config.min_age, self.config.grace_period))
            .map(|r| r.chunk_id)
            .take(self.config.batch_size)
            .collect()
    }

    /// Run a GC scan.
    pub async fn run_gc<F, Fut>(&self, delete_fn: F) -> Result<GcScanResult>
    where
        F: Fn(ChunkId) -> Fut,
        Fut: std::future::Future<Output = Result<u64>>,
    {
        // Check if already running
        {
            let mut running = self.running.write().await;
            if *running {
                return Err(StrataError::Internal("GC already running".to_string()));
            }
            *running = true;
        }

        let start = Instant::now();
        let mut result = GcScanResult {
            orphans: Vec::new(),
            deleted: Vec::new(),
            bytes_reclaimed: 0,
            errors: Vec::new(),
            duration: Duration::default(),
        };

        // Find orphans
        let orphans = self.find_orphans().await;
        result.orphans = orphans.clone();

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.chunks_scanned += self.references.read().await.len() as u64;
            stats.orphans_found += orphans.len() as u64;
        }

        // Delete orphans (unless dry run)
        if !self.config.dry_run {
            for chunk_id in orphans {
                match delete_fn(chunk_id).await {
                    Ok(bytes) => {
                        result.deleted.push(chunk_id);
                        result.bytes_reclaimed += bytes;
                        self.untrack_chunk(chunk_id).await;
                    }
                    Err(e) => {
                        result.errors.push(format!("Failed to delete {}: {}", chunk_id, e));
                        self.stats.write().await.errors += 1;
                    }
                }
            }
        }

        result.duration = start.elapsed();

        // Update final stats
        {
            let mut stats = self.stats.write().await;
            stats.cycles += 1;
            stats.chunks_deleted += result.deleted.len() as u64;
            stats.bytes_reclaimed += result.bytes_reclaimed;
            stats.last_cycle_duration = result.duration;
            stats.last_cycle_time = Some(Instant::now());
        }

        // Mark as not running
        *self.running.write().await = false;

        Ok(result)
    }

    /// Get current statistics.
    pub async fn stats(&self) -> GcStats {
        self.stats.read().await.clone()
    }

    /// Get number of tracked chunks.
    pub async fn tracked_count(&self) -> usize {
        self.references.read().await.len()
    }

    /// Get number of orphaned chunks.
    pub async fn orphan_count(&self) -> usize {
        let refs = self.references.read().await;
        refs.values().filter(|r| r.is_orphan()).count()
    }

    /// Check if GC is currently running.
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Force immediate GC on a specific chunk.
    pub async fn force_gc_chunk<F, Fut>(&self, chunk_id: ChunkId, delete_fn: F) -> Result<u64>
    where
        F: FnOnce(ChunkId) -> Fut,
        Fut: std::future::Future<Output = Result<u64>>,
    {
        // Verify it's an orphan
        {
            let refs = self.references.read().await;
            if let Some(ref_entry) = refs.get(&chunk_id) {
                if !ref_entry.is_orphan() {
                    return Err(StrataError::Internal(format!(
                        "Chunk {} still has {} references",
                        chunk_id,
                        ref_entry.references.len()
                    )));
                }
            }
        }

        // Delete it
        let bytes = delete_fn(chunk_id).await?;
        self.untrack_chunk(chunk_id).await;

        let mut stats = self.stats.write().await;
        stats.chunks_deleted += 1;
        stats.bytes_reclaimed += bytes;

        Ok(bytes)
    }
}

/// Background GC runner.
pub struct GcRunner {
    /// Garbage collector.
    gc: Arc<ChunkGarbageCollector>,
    /// Shutdown signal.
    shutdown: tokio::sync::broadcast::Receiver<()>,
}

impl GcRunner {
    /// Create a new GC runner.
    pub fn new(gc: Arc<ChunkGarbageCollector>, shutdown: tokio::sync::broadcast::Receiver<()>) -> Self {
        Self { gc, shutdown }
    }

    /// Run the GC loop.
    pub async fn run<F, Fut>(mut self, delete_fn: F)
    where
        F: Fn(ChunkId) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<u64>> + Send,
    {
        let interval = self.gc.config.scan_interval;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    let delete_fn = delete_fn.clone();
                    match self.gc.run_gc(delete_fn).await {
                        Ok(result) => {
                            if !result.deleted.is_empty() {
                                tracing::info!(
                                    deleted = result.deleted.len(),
                                    bytes = result.bytes_reclaimed,
                                    duration_ms = result.duration.as_millis(),
                                    "GC cycle completed"
                                );
                            }
                            if !result.errors.is_empty() {
                                tracing::warn!(errors = ?result.errors, "GC encountered errors");
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "GC cycle failed");
                        }
                    }
                }
                _ = self.shutdown.recv() => {
                    tracing::info!("GC runner shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_reference() {
        let chunk_id = ChunkId::new();
        let mut ref_entry = ChunkReference::new(chunk_id);

        assert!(ref_entry.is_orphan());

        ref_entry.add_reference(1);
        assert!(!ref_entry.is_orphan());

        ref_entry.add_reference(2);
        assert!(!ref_entry.is_orphan());

        ref_entry.remove_reference(1);
        assert!(!ref_entry.is_orphan());

        ref_entry.remove_reference(2);
        assert!(ref_entry.is_orphan());
        assert!(ref_entry.unreferenced_at.is_some());
    }

    #[test]
    fn test_gc_eligibility() {
        let chunk_id = ChunkId::new();
        let mut ref_entry = ChunkReference::new(chunk_id);

        // Not orphan
        ref_entry.add_reference(1);
        assert!(!ref_entry.is_gc_eligible(Duration::ZERO, Duration::ZERO));

        // Orphan but too young
        ref_entry.remove_reference(1);
        assert!(!ref_entry.is_gc_eligible(Duration::from_secs(3600), Duration::ZERO));

        // Orphan and old enough but grace period not passed
        // (would need to manipulate time to test properly)
    }

    #[tokio::test]
    async fn test_gc_add_remove_reference() {
        let gc = ChunkGarbageCollector::new(GcConfig::default());
        let chunk_id = ChunkId::new();

        gc.add_reference(chunk_id, 1).await;
        gc.add_reference(chunk_id, 2).await;

        assert_eq!(gc.tracked_count().await, 1);
        assert_eq!(gc.orphan_count().await, 0);

        gc.remove_reference(chunk_id, 1).await;
        gc.remove_reference(chunk_id, 2).await;

        assert_eq!(gc.orphan_count().await, 1);
    }

    #[tokio::test]
    async fn test_gc_track_untrack() {
        let gc = ChunkGarbageCollector::new(GcConfig::default());
        let chunk_id = ChunkId::new();

        gc.track_chunk(chunk_id).await;
        assert_eq!(gc.tracked_count().await, 1);

        gc.untrack_chunk(chunk_id).await;
        assert_eq!(gc.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn test_gc_find_orphans() {
        let config = GcConfig {
            min_age: Duration::ZERO,
            grace_period: Duration::ZERO,
            ..Default::default()
        };
        let gc = ChunkGarbageCollector::new(config);

        let chunk1 = ChunkId::new();
        let chunk2 = ChunkId::new();

        gc.track_chunk(chunk1).await;
        gc.add_reference(chunk2, 1).await;

        // chunk1 is orphan, chunk2 has reference
        let orphans = gc.find_orphans().await;
        assert_eq!(orphans.len(), 1);
        assert!(orphans.contains(&chunk1));
    }

    #[tokio::test]
    async fn test_gc_run() {
        let config = GcConfig {
            min_age: Duration::ZERO,
            grace_period: Duration::ZERO,
            dry_run: false,
            ..Default::default()
        };
        let gc = ChunkGarbageCollector::new(config);

        let chunk_id = ChunkId::new();
        gc.track_chunk(chunk_id).await;

        // Run GC with a mock delete function
        let result = gc.run_gc(|_| async { Ok(1024u64) }).await.unwrap();

        assert_eq!(result.orphans.len(), 1);
        assert_eq!(result.deleted.len(), 1);
        assert_eq!(result.bytes_reclaimed, 1024);
        assert_eq!(gc.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn test_gc_dry_run() {
        let config = GcConfig {
            min_age: Duration::ZERO,
            grace_period: Duration::ZERO,
            dry_run: true,
            ..Default::default()
        };
        let gc = ChunkGarbageCollector::new(config);

        let chunk_id = ChunkId::new();
        gc.track_chunk(chunk_id).await;

        let result = gc.run_gc(|_| async { Ok(1024u64) }).await.unwrap();

        assert_eq!(result.orphans.len(), 1);
        assert_eq!(result.deleted.len(), 0); // Not deleted in dry run
        assert_eq!(gc.tracked_count().await, 1); // Still tracked
    }

    #[tokio::test]
    async fn test_gc_stats() {
        let config = GcConfig {
            min_age: Duration::ZERO,
            grace_period: Duration::ZERO,
            ..Default::default()
        };
        let gc = ChunkGarbageCollector::new(config);

        gc.track_chunk(ChunkId::new()).await;
        gc.run_gc(|_| async { Ok(512u64) }).await.unwrap();

        let stats = gc.stats().await;
        assert_eq!(stats.cycles, 1);
        assert_eq!(stats.chunks_deleted, 1);
        assert_eq!(stats.bytes_reclaimed, 512);
    }

    #[tokio::test]
    async fn test_gc_already_running() {
        let gc = Arc::new(ChunkGarbageCollector::new(GcConfig::default()));
        *gc.running.write().await = true;

        let result = gc.run_gc(|_| async { Ok(0u64) }).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_config_presets() {
        let aggressive = GcConfig::aggressive();
        assert_eq!(aggressive.scan_interval, Duration::from_secs(300));

        let conservative = GcConfig::conservative();
        assert_eq!(conservative.scan_interval, Duration::from_secs(86400));
    }
}
