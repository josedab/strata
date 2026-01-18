//! Request batching for metadata operations.
//!
//! This module provides batching of metadata operations to reduce round-trip latency
//! and improve throughput when processing many small requests.
//!
//! # How it works
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Request Batcher                            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  1. Requests arrive and are queued                             │
//! │  2. Batcher waits for batch window or batch size               │
//! │  3. All queued requests are combined into single batch         │
//! │  4. Batch is processed atomically                              │
//! │  5. Results are distributed back to individual requesters      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use crate::types::{Inode, InodeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, info, warn};

/// Configuration for request batching.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of requests in a batch.
    pub max_batch_size: usize,
    /// Maximum time to wait for a full batch.
    pub max_batch_delay: Duration,
    /// Enable batching.
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(5),
            enabled: true,
        }
    }
}

impl BatchConfig {
    /// Low-latency configuration (smaller batches, shorter delays).
    pub fn low_latency() -> Self {
        Self {
            max_batch_size: 20,
            max_batch_delay: Duration::from_millis(1),
            enabled: true,
        }
    }

    /// High-throughput configuration (larger batches, longer delays).
    pub fn high_throughput() -> Self {
        Self {
            max_batch_size: 500,
            max_batch_delay: Duration::from_millis(10),
            enabled: true,
        }
    }
}

/// A lookup request in the batch.
#[derive(Debug)]
struct LookupRequest {
    /// Parent inode ID.
    parent: InodeId,
    /// Name to look up.
    name: String,
    /// Channel to send result back.
    response_tx: oneshot::Sender<Result<Option<Inode>>>,
}

/// A getattr request in the batch.
#[derive(Debug)]
struct GetattrRequest {
    /// Inode ID.
    inode_id: InodeId,
    /// Channel to send result back.
    response_tx: oneshot::Sender<Result<Option<Inode>>>,
}

/// A batch of lookup requests.
#[derive(Debug, Default)]
struct LookupBatch {
    /// Requests in the batch.
    requests: Vec<LookupRequest>,
    /// First request time.
    first_request: Option<Instant>,
}

/// A batch of getattr requests.
#[derive(Debug, Default)]
struct GetattrBatch {
    /// Requests in the batch.
    requests: Vec<GetattrRequest>,
    /// First request time.
    first_request: Option<Instant>,
}

/// Statistics for batching operations.
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total requests processed.
    pub total_requests: u64,
    /// Total batches processed.
    pub total_batches: u64,
    /// Average batch size.
    pub avg_batch_size: f64,
    /// Requests that bypassed batching.
    pub unbatched_requests: u64,
    /// Total time saved by batching (estimated).
    pub time_saved_ms: u64,
}

impl BatchStats {
    /// Calculate batching efficiency.
    pub fn efficiency(&self) -> f64 {
        if self.total_batches == 0 {
            return 0.0;
        }
        self.avg_batch_size / self.total_requests as f64
    }
}

/// Trait for the metadata backend that processes batched requests.
#[async_trait::async_trait]
pub trait BatchableMetadataBackend: Send + Sync {
    /// Batch lookup operation.
    async fn batch_lookup(&self, requests: Vec<(InodeId, String)>) -> Vec<Result<Option<Inode>>>;

    /// Batch getattr operation.
    async fn batch_getattr(&self, inode_ids: Vec<InodeId>) -> Vec<Result<Option<Inode>>>;
}

/// Request batcher for metadata operations.
pub struct MetadataBatcher<B: BatchableMetadataBackend> {
    /// Backend for processing batches.
    backend: Arc<B>,
    /// Configuration.
    config: BatchConfig,
    /// Lookup request sender.
    lookup_tx: mpsc::Sender<LookupRequest>,
    /// Getattr request sender.
    getattr_tx: mpsc::Sender<GetattrRequest>,
    /// Statistics.
    stats: Arc<RwLock<BatchStats>>,
}

impl<B: BatchableMetadataBackend + 'static> MetadataBatcher<B> {
    /// Create a new metadata batcher.
    pub fn new(backend: Arc<B>, config: BatchConfig) -> Self {
        let (lookup_tx, lookup_rx) = mpsc::channel(10000);
        let (getattr_tx, getattr_rx) = mpsc::channel(10000);
        let stats = Arc::new(RwLock::new(BatchStats::default()));

        // Start lookup batcher task
        {
            let backend = backend.clone();
            let config = config.clone();
            let stats = stats.clone();
            tokio::spawn(async move {
                run_lookup_batcher(backend, config, lookup_rx, stats).await;
            });
        }

        // Start getattr batcher task
        {
            let backend = backend.clone();
            let config = config.clone();
            let stats = stats.clone();
            tokio::spawn(async move {
                run_getattr_batcher(backend, config, getattr_rx, stats).await;
            });
        }

        Self {
            backend,
            config,
            lookup_tx,
            getattr_tx,
            stats,
        }
    }

    /// Lookup an inode by parent and name (batched).
    pub async fn lookup(&self, parent: InodeId, name: String) -> Result<Option<Inode>> {
        if !self.config.enabled {
            // Bypass batching
            let results = self.backend.batch_lookup(vec![(parent, name)]).await;
            return results.into_iter().next().unwrap_or(Err(StrataError::Internal(
                "No result from backend".to_string(),
            )));
        }

        let (response_tx, response_rx) = oneshot::channel();

        let request = LookupRequest {
            parent,
            name,
            response_tx,
        };

        self.lookup_tx
            .send(request)
            .await
            .map_err(|_| StrataError::Internal("Batcher channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StrataError::Internal("Response channel closed".to_string()))?
    }

    /// Get attributes for an inode (batched).
    pub async fn getattr(&self, inode_id: InodeId) -> Result<Option<Inode>> {
        if !self.config.enabled {
            // Bypass batching
            let results = self.backend.batch_getattr(vec![inode_id]).await;
            return results.into_iter().next().unwrap_or(Err(StrataError::Internal(
                "No result from backend".to_string(),
            )));
        }

        let (response_tx, response_rx) = oneshot::channel();

        let request = GetattrRequest {
            inode_id,
            response_tx,
        };

        self.getattr_tx
            .send(request)
            .await
            .map_err(|_| StrataError::Internal("Batcher channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StrataError::Internal("Response channel closed".to_string()))?
    }

    /// Get batching statistics.
    pub async fn stats(&self) -> BatchStats {
        self.stats.read().await.clone()
    }
}

/// Run the lookup batcher loop.
async fn run_lookup_batcher<B: BatchableMetadataBackend>(
    backend: Arc<B>,
    config: BatchConfig,
    mut rx: mpsc::Receiver<LookupRequest>,
    stats: Arc<RwLock<BatchStats>>,
) {
    let mut batch = LookupBatch::default();

    loop {
        let timeout = if batch.requests.is_empty() {
            Duration::from_secs(3600) // Long timeout when empty
        } else {
            let elapsed = batch
                .first_request
                .map(|t| t.elapsed())
                .unwrap_or(Duration::ZERO);
            config.max_batch_delay.saturating_sub(elapsed)
        };

        tokio::select! {
            request = rx.recv() => {
                match request {
                    Some(req) => {
                        if batch.requests.is_empty() {
                            batch.first_request = Some(Instant::now());
                        }
                        batch.requests.push(req);

                        if batch.requests.len() >= config.max_batch_size {
                            process_lookup_batch(&backend, &mut batch, &stats).await;
                        }
                    }
                    None => {
                        // Channel closed, process remaining
                        if !batch.requests.is_empty() {
                            process_lookup_batch(&backend, &mut batch, &stats).await;
                        }
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                if !batch.requests.is_empty() {
                    process_lookup_batch(&backend, &mut batch, &stats).await;
                }
            }
        }
    }
}

/// Process a batch of lookup requests.
async fn process_lookup_batch<B: BatchableMetadataBackend>(
    backend: &Arc<B>,
    batch: &mut LookupBatch,
    stats: &Arc<RwLock<BatchStats>>,
) {
    let requests = std::mem::take(&mut batch.requests);
    batch.first_request = None;

    let batch_size = requests.len();
    if batch_size == 0 {
        return;
    }

    debug!(batch_size = batch_size, "Processing lookup batch");

    // Extract lookup keys
    let lookup_keys: Vec<(InodeId, String)> = requests
        .iter()
        .map(|r| (r.parent, r.name.clone()))
        .collect();

    // Process batch
    let results = backend.batch_lookup(lookup_keys).await;

    // Send results back
    for (request, result) in requests.into_iter().zip(results.into_iter()) {
        let _ = request.response_tx.send(result);
    }

    // Update stats
    {
        let mut s = stats.write().await;
        s.total_requests += batch_size as u64;
        s.total_batches += 1;
        s.avg_batch_size = s.total_requests as f64 / s.total_batches as f64;
        // Estimate time saved: ~1ms per request that would have been individual
        if batch_size > 1 {
            s.time_saved_ms += ((batch_size - 1) as u64) * 1;
        }
    }
}

/// Run the getattr batcher loop.
async fn run_getattr_batcher<B: BatchableMetadataBackend>(
    backend: Arc<B>,
    config: BatchConfig,
    mut rx: mpsc::Receiver<GetattrRequest>,
    stats: Arc<RwLock<BatchStats>>,
) {
    let mut batch = GetattrBatch::default();

    loop {
        let timeout = if batch.requests.is_empty() {
            Duration::from_secs(3600)
        } else {
            let elapsed = batch
                .first_request
                .map(|t| t.elapsed())
                .unwrap_or(Duration::ZERO);
            config.max_batch_delay.saturating_sub(elapsed)
        };

        tokio::select! {
            request = rx.recv() => {
                match request {
                    Some(req) => {
                        if batch.requests.is_empty() {
                            batch.first_request = Some(Instant::now());
                        }
                        batch.requests.push(req);

                        if batch.requests.len() >= config.max_batch_size {
                            process_getattr_batch(&backend, &mut batch, &stats).await;
                        }
                    }
                    None => {
                        if !batch.requests.is_empty() {
                            process_getattr_batch(&backend, &mut batch, &stats).await;
                        }
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                if !batch.requests.is_empty() {
                    process_getattr_batch(&backend, &mut batch, &stats).await;
                }
            }
        }
    }
}

/// Process a batch of getattr requests.
async fn process_getattr_batch<B: BatchableMetadataBackend>(
    backend: &Arc<B>,
    batch: &mut GetattrBatch,
    stats: &Arc<RwLock<BatchStats>>,
) {
    let requests = std::mem::take(&mut batch.requests);
    batch.first_request = None;

    let batch_size = requests.len();
    if batch_size == 0 {
        return;
    }

    debug!(batch_size = batch_size, "Processing getattr batch");

    // Extract inode IDs
    let inode_ids: Vec<InodeId> = requests.iter().map(|r| r.inode_id).collect();

    // Process batch
    let results = backend.batch_getattr(inode_ids).await;

    // Send results back
    for (request, result) in requests.into_iter().zip(results.into_iter()) {
        let _ = request.response_tx.send(result);
    }

    // Update stats
    {
        let mut s = stats.write().await;
        s.total_requests += batch_size as u64;
        s.total_batches += 1;
        s.avg_batch_size = s.total_requests as f64 / s.total_batches as f64;
        if batch_size > 1 {
            s.time_saved_ms += ((batch_size - 1) as u64) * 1;
        }
    }
}

/// In-memory metadata backend for testing.
pub struct InMemoryMetadataBackend {
    /// Stored inodes.
    inodes: Arc<RwLock<HashMap<InodeId, Inode>>>,
    /// Directory entries: (parent_id, name) -> inode_id.
    entries: Arc<RwLock<HashMap<(InodeId, String), InodeId>>>,
}

impl Default for InMemoryMetadataBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMetadataBackend {
    /// Create a new in-memory backend.
    pub fn new() -> Self {
        Self {
            inodes: Arc::new(RwLock::new(HashMap::new())),
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an inode.
    pub async fn add_inode(&self, inode: Inode) {
        self.inodes.write().await.insert(inode.id, inode);
    }

    /// Add a directory entry.
    pub async fn add_entry(&self, parent: InodeId, name: String, inode_id: InodeId) {
        self.entries.write().await.insert((parent, name), inode_id);
    }
}

#[async_trait::async_trait]
impl BatchableMetadataBackend for InMemoryMetadataBackend {
    async fn batch_lookup(&self, requests: Vec<(InodeId, String)>) -> Vec<Result<Option<Inode>>> {
        let entries = self.entries.read().await;
        let inodes = self.inodes.read().await;

        requests
            .into_iter()
            .map(|(parent, name)| {
                let inode_id = entries.get(&(parent, name));
                match inode_id {
                    Some(&id) => Ok(inodes.get(&id).cloned()),
                    None => Ok(None),
                }
            })
            .collect()
    }

    async fn batch_getattr(&self, inode_ids: Vec<InodeId>) -> Vec<Result<Option<Inode>>> {
        let inodes = self.inodes.read().await;

        inode_ids
            .into_iter()
            .map(|id| Ok(inodes.get(&id).cloned()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileType;

    fn create_test_inode(id: InodeId) -> Inode {
        Inode::new_file(id, 0o644, 1000, 1000)
    }

    #[tokio::test]
    async fn test_in_memory_backend() {
        let backend = InMemoryMetadataBackend::new();

        let inode = create_test_inode(1);
        backend.add_inode(inode.clone()).await;
        backend.add_entry(0, "test.txt".to_string(), 1).await;

        let results = backend.batch_lookup(vec![(0, "test.txt".to_string())]).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].as_ref().unwrap().is_some());

        let results = backend.batch_getattr(vec![1]).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].as_ref().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_batcher_single_request() {
        let backend = Arc::new(InMemoryMetadataBackend::new());

        let inode = create_test_inode(1);
        backend.add_inode(inode.clone()).await;
        backend.add_entry(0, "test.txt".to_string(), 1).await;

        let batcher = MetadataBatcher::new(backend, BatchConfig::default());

        let result = batcher.lookup(0, "test.txt".to_string()).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, 1);
    }

    #[tokio::test]
    async fn test_batcher_multiple_concurrent_requests() {
        let backend = Arc::new(InMemoryMetadataBackend::new());

        // Add test data
        for i in 1..=10 {
            let inode = create_test_inode(i);
            backend.add_inode(inode).await;
            backend
                .add_entry(0, format!("file{}.txt", i), i)
                .await;
        }

        let batcher = Arc::new(MetadataBatcher::new(
            backend,
            BatchConfig {
                max_batch_size: 5,
                max_batch_delay: Duration::from_millis(50),
                enabled: true,
            },
        ));

        // Fire multiple concurrent requests
        let mut handles = Vec::new();
        for i in 1..=10 {
            let batcher = batcher.clone();
            let handle = tokio::spawn(async move {
                batcher.lookup(0, format!("file{}.txt", i)).await
            });
            handles.push(handle);
        }

        // Wait for all results
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert!(result.is_some());
        }

        // Check stats
        let stats = batcher.stats().await;
        assert!(stats.total_requests >= 10);
        assert!(stats.total_batches > 0);
    }

    #[tokio::test]
    async fn test_batcher_disabled() {
        let backend = Arc::new(InMemoryMetadataBackend::new());

        let inode = create_test_inode(1);
        backend.add_inode(inode.clone()).await;
        backend.add_entry(0, "test.txt".to_string(), 1).await;

        let batcher = MetadataBatcher::new(
            backend,
            BatchConfig {
                enabled: false,
                ..Default::default()
            },
        );

        let result = batcher.lookup(0, "test.txt".to_string()).await.unwrap();
        assert!(result.is_some());
    }
}
