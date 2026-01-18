//! Prefetch integration for the data layer.
//!
//! This module integrates the predictive prefetching engine with chunk storage
//! to optimize sequential reads and predict access patterns.

use crate::data::chunk_storage::ChunkStorage;
use crate::error::{Result, StrataError};
use crate::prefetch::{
    AccessEvent, AccessType, PrefetchConfig, PrefetchEngine, PrefetchPrediction,
    PrefetchProvider, PrefetchScheduler, PrefetchStats,
};
use crate::types::ChunkId;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Chunk prefetch provider that integrates with ChunkStorage.
pub struct ChunkPrefetchProvider {
    /// Underlying chunk storage.
    storage: Arc<ChunkStorage>,
    /// Prefetch cache for chunks loaded ahead of time.
    prefetch_cache: Arc<RwLock<lru::LruCache<ChunkId, Arc<Vec<u8>>>>>,
}

impl ChunkPrefetchProvider {
    /// Create a new chunk prefetch provider.
    pub fn new(storage: Arc<ChunkStorage>, cache_entries: usize) -> Self {
        let cache_entries =
            std::num::NonZeroUsize::new(cache_entries).unwrap_or(std::num::NonZeroUsize::MIN);
        Self {
            storage,
            prefetch_cache: Arc::new(RwLock::new(lru::LruCache::new(cache_entries))),
        }
    }

    /// Get a prefetched chunk from cache.
    pub async fn get_prefetched(&self, chunk_id: &ChunkId) -> Option<Arc<Vec<u8>>> {
        self.prefetch_cache.write().await.get(chunk_id).cloned()
    }

    /// Check if a chunk is in the prefetch cache.
    pub async fn is_prefetched(&self, chunk_id: &ChunkId) -> bool {
        self.prefetch_cache.read().await.contains(chunk_id)
    }
}

#[async_trait::async_trait]
impl PrefetchProvider for ChunkPrefetchProvider {
    async fn prefetch(&self, path: &Path, byte_range: Option<(u64, u64)>) -> Result<u64> {
        // Parse chunk ID from path
        let chunk_id = path_to_chunk_id(path)?;

        // Check if already prefetched
        if self.is_prefetched(&chunk_id).await {
            return Ok(0);
        }

        // Determine shard index from byte range or default to 0
        let shard_index = byte_range.map(|(start, _)| (start / (64 * 1024)) as usize).unwrap_or(0);

        // Read from storage
        match self.storage.read_shard(chunk_id, shard_index) {
            Ok(data) => {
                let bytes = data.len() as u64;
                self.prefetch_cache
                    .write()
                    .await
                    .put(chunk_id, Arc::new(data));
                debug!(chunk_id = %chunk_id, bytes = bytes, "Prefetched chunk");
                Ok(bytes)
            }
            Err(e) => {
                warn!(chunk_id = %chunk_id, error = %e, "Failed to prefetch chunk");
                Err(e)
            }
        }
    }

    async fn is_cached(&self, path: &Path, _byte_range: Option<(u64, u64)>) -> Result<bool> {
        let chunk_id = path_to_chunk_id(path)?;
        Ok(self.is_prefetched(&chunk_id).await)
    }
}

/// Convert a path to a ChunkId.
fn path_to_chunk_id(path: &Path) -> Result<ChunkId> {
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| StrataError::InvalidInput("Invalid chunk path".to_string()))?;

    ChunkId::parse(filename)
        .map_err(|e| StrataError::InvalidInput(format!("Invalid chunk ID: {}", e)))
}

/// Convert a ChunkId to a path.
fn chunk_id_to_path(chunk_id: ChunkId) -> PathBuf {
    PathBuf::from(chunk_id.to_string())
}

/// Data prefetch manager integrating prefetching with chunk operations.
pub struct DataPrefetchManager {
    /// Prefetch engine.
    engine: Arc<PrefetchEngine<ChunkPrefetchProvider>>,
    /// Prefetch provider.
    provider: Arc<ChunkPrefetchProvider>,
    /// Configuration.
    config: PrefetchConfig,
    /// Scheduler handle.
    scheduler_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DataPrefetchManager {
    /// Create a new data prefetch manager.
    pub fn new(storage: Arc<ChunkStorage>, config: PrefetchConfig) -> Self {
        let provider = Arc::new(ChunkPrefetchProvider::new(storage, 1000));
        let (engine, _rx) = PrefetchEngine::new(config.clone(), provider.clone());

        let engine = Arc::new(engine);

        Self {
            engine,
            provider,
            config,
            scheduler_handle: None,
        }
    }

    /// Start the prefetch scheduler in the background.
    pub fn start_scheduler(&mut self, _shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
        let provider = self.provider.clone();
        let config = self.config.clone();
        let stats = Arc::new(RwLock::new(PrefetchStats::default()));

        // Re-create engine and scheduler to get fresh channel
        let (engine, rx) = PrefetchEngine::new(config.clone(), provider.clone());
        self.engine = Arc::new(engine);

        let scheduler = PrefetchScheduler::new(provider, config, stats);

        let handle = tokio::spawn(async move {
            scheduler.run(rx).await;
        });

        self.scheduler_handle = Some(handle);
        info!("Data prefetch scheduler started");
    }

    /// Record a chunk read for pattern learning.
    pub async fn record_chunk_read(
        &self,
        chunk_id: ChunkId,
        shard_index: usize,
        offset: u64,
        length: u64,
        accessor_id: &str,
    ) -> Result<Vec<PrefetchPrediction>> {
        let event = AccessEvent {
            path: chunk_id_to_path(chunk_id),
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
            offset: offset + (shard_index as u64 * 64 * 1024), // Include shard offset
            length,
            accessor_id: accessor_id.to_string(),
        };

        self.engine.record_access(event).await
    }

    /// Record a sequential chunk read pattern.
    pub async fn record_sequential_read(
        &self,
        chunk_id: ChunkId,
        shard_index: usize,
        accessor_id: &str,
    ) -> Result<Option<PrefetchPrediction>> {
        let event = AccessEvent {
            path: chunk_id_to_path(chunk_id),
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
            offset: shard_index as u64 * 64 * 1024,
            length: 64 * 1024, // Standard shard size
            accessor_id: accessor_id.to_string(),
        };

        let predictions = self.engine.record_access(event).await?;

        // Return the highest confidence sequential prediction
        Ok(predictions
            .into_iter()
            .filter(|p| p.source == crate::prefetch::PredictionSource::Sequential)
            .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal)))
    }

    /// Try to get a prefetched chunk.
    pub async fn get_prefetched(&self, chunk_id: &ChunkId) -> Option<Arc<Vec<u8>>> {
        let result = self.provider.get_prefetched(chunk_id).await;
        if result.is_some() {
            // Record hit
            self.engine
                .record_hit(&chunk_id_to_path(*chunk_id), 64 * 1024)
                .await;
        }
        result
    }

    /// Get prefetch statistics.
    pub async fn stats(&self) -> PrefetchStats {
        self.engine.stats().await
    }

    /// Cleanup stale prefetch state.
    pub async fn cleanup(&self) {
        self.engine.cleanup().await;
    }

    /// Decay models to adapt to changing patterns.
    pub async fn decay_models(&self) {
        self.engine.decay_models().await;
    }

    /// End a session (e.g., when a client disconnects).
    pub async fn end_session(&self, accessor_id: &str) {
        self.engine.end_session(accessor_id).await;
    }
}

/// Read-ahead buffer for sequential file reads.
pub struct ReadAheadBuffer {
    /// File path or identifier.
    file_id: String,
    /// Current position.
    position: u64,
    /// Buffer of prefetched chunks.
    buffer: Vec<(ChunkId, usize, Arc<Vec<u8>>)>,
    /// Maximum chunks to buffer.
    max_chunks: usize,
    /// Sequential read count.
    sequential_count: u32,
}

impl ReadAheadBuffer {
    /// Create a new read-ahead buffer.
    pub fn new(file_id: String, max_chunks: usize) -> Self {
        Self {
            file_id,
            position: 0,
            buffer: Vec::with_capacity(max_chunks),
            max_chunks,
            sequential_count: 0,
        }
    }

    /// Record a read and detect if it's sequential.
    pub fn record_read(&mut self, offset: u64, length: u64) -> bool {
        let expected = self.position;
        let is_sequential = offset == expected || offset == 0 && self.position == 0;

        if is_sequential {
            self.sequential_count += 1;
            self.position = offset + length;
        } else {
            self.sequential_count = 1;
            self.position = offset + length;
        }

        // Return true if we have a strong sequential pattern
        self.sequential_count >= 3
    }

    /// Get the number of chunks to read ahead based on pattern strength.
    pub fn read_ahead_count(&self) -> usize {
        match self.sequential_count {
            0..=2 => 0,
            3..=5 => 1,
            6..=10 => 2,
            _ => self.max_chunks.min(4),
        }
    }

    /// Add a chunk to the buffer.
    pub fn add_chunk(&mut self, chunk_id: ChunkId, shard_index: usize, data: Arc<Vec<u8>>) {
        if self.buffer.len() >= self.max_chunks {
            self.buffer.remove(0);
        }
        self.buffer.push((chunk_id, shard_index, data));
    }

    /// Get a buffered chunk if available.
    pub fn get_chunk(&mut self, chunk_id: &ChunkId, shard_index: usize) -> Option<Arc<Vec<u8>>> {
        let pos = self
            .buffer
            .iter()
            .position(|(id, idx, _)| id == chunk_id && *idx == shard_index)?;
        let (_, _, data) = self.buffer.remove(pos);
        Some(data)
    }

    /// Check if a chunk is in the buffer.
    pub fn has_chunk(&self, chunk_id: &ChunkId, shard_index: usize) -> bool {
        self.buffer
            .iter()
            .any(|(id, idx, _)| id == chunk_id && *idx == shard_index)
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.sequential_count = 0;
    }
}

/// Configuration for read-ahead behavior.
#[derive(Debug, Clone)]
pub struct ReadAheadConfig {
    /// Enable read-ahead.
    pub enabled: bool,
    /// Maximum chunks to buffer per file.
    pub max_chunks_per_file: usize,
    /// Minimum sequential reads before enabling read-ahead.
    pub min_sequential_reads: u32,
    /// Maximum concurrent read-ahead operations.
    pub max_concurrent: usize,
}

impl Default for ReadAheadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_chunks_per_file: 4,
            min_sequential_reads: 3,
            max_concurrent: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_ahead_buffer() {
        let mut buffer = ReadAheadBuffer::new("test".to_string(), 4);

        // First read starts at 0
        assert!(!buffer.record_read(0, 1024));

        // Sequential reads
        assert!(!buffer.record_read(1024, 1024));
        assert!(buffer.record_read(2048, 1024)); // Third sequential read triggers

        assert_eq!(buffer.read_ahead_count(), 1);
    }

    #[test]
    fn test_read_ahead_buffer_non_sequential() {
        let mut buffer = ReadAheadBuffer::new("test".to_string(), 4);

        buffer.record_read(0, 1024);
        buffer.record_read(1024, 1024);
        buffer.record_read(1024, 1024); // Jump back resets count

        assert_eq!(buffer.sequential_count, 1);
        assert_eq!(buffer.read_ahead_count(), 0);
    }

    #[test]
    fn test_read_ahead_buffer_chunks() {
        let mut buffer = ReadAheadBuffer::new("test".to_string(), 2);

        let chunk1 = ChunkId::new();
        let chunk2 = ChunkId::new();
        let chunk3 = ChunkId::new();

        buffer.add_chunk(chunk1, 0, Arc::new(vec![1, 2, 3]));
        buffer.add_chunk(chunk2, 0, Arc::new(vec![4, 5, 6]));

        assert!(buffer.has_chunk(&chunk1, 0));
        assert!(buffer.has_chunk(&chunk2, 0));

        // Adding third chunk should evict first
        buffer.add_chunk(chunk3, 0, Arc::new(vec![7, 8, 9]));
        assert!(!buffer.has_chunk(&chunk1, 0));
        assert!(buffer.has_chunk(&chunk2, 0));
        assert!(buffer.has_chunk(&chunk3, 0));
    }
}
