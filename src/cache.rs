//! Read caching module for Strata.
//!
//! Provides LRU caching for chunk data to improve read performance. The cache
//! automatically evicts least-recently-used entries when capacity is reached
//! and supports time-based expiration.
//!
//! # Features
//!
//! - **LRU Eviction**: Least recently used entries are evicted first
//! - **Size Limits**: Configurable maximum cache size in bytes
//! - **Time-based Expiration**: Entries expire after a configurable duration
//! - **Statistics**: Track hit/miss ratios and cache efficiency
//! - **Thread-safe**: Safe for concurrent access using async locks
//!
//! # Configuration Presets
//!
//! | Preset | Size | Max Entries | Max Age |
//! |--------|------|-------------|---------|
//! | `small()` | 64 MB | 1,000 | 2 minutes |
//! | `default()` | 256 MB | 10,000 | 5 minutes |
//! | `large()` | 1 GB | 100,000 | 10 minutes |
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::cache::{ReadCache, ReadCacheConfig, CacheKey};
//! use strata::types::ChunkId;
//!
//! // Create a cache with default configuration
//! let cache = ReadCache::new(ReadCacheConfig::default());
//!
//! // Store data
//! let chunk_id = ChunkId::new();
//! let key = CacheKey::chunk(chunk_id);
//! cache.put(key, vec![1, 2, 3, 4, 5]).await;
//!
//! // Retrieve data
//! if let Some(data) = cache.get(&key).await {
//!     println!("Cache hit: {} bytes", data.len());
//! }
//!
//! // Check statistics
//! let stats = cache.stats().await;
//! println!("Hit ratio: {:.1}%", stats.hit_ratio() * 100.0);
//! ```
//!
//! # Cache Invalidation
//!
//! Entries can be invalidated individually or by chunk:
//!
//! ```rust,ignore
//! // Remove a specific entry
//! cache.remove(&key).await;
//!
//! // Invalidate all entries for a chunk (including all shards)
//! cache.invalidate_chunk(chunk_id).await;
//!
//! // Clear entire cache
//! cache.clear().await;
//! ```

use crate::types::ChunkId;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache statistics.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Total number of cache hits.
    pub hits: u64,
    /// Total number of cache misses.
    pub misses: u64,
    /// Total bytes read from cache.
    pub bytes_hit: u64,
    /// Total bytes that were cache misses.
    pub bytes_missed: u64,
    /// Current number of cached entries.
    pub entries: usize,
    /// Current cache size in bytes.
    pub size_bytes: u64,
    /// Maximum cache size in bytes.
    pub max_size_bytes: u64,
}

impl CacheStats {
    /// Calculate hit ratio.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 0.0;
        }
        self.hits as f64 / total as f64
    }

    /// Calculate fill ratio.
    pub fn fill_ratio(&self) -> f64 {
        if self.max_size_bytes == 0 {
            return 0.0;
        }
        self.size_bytes as f64 / self.max_size_bytes as f64
    }
}

/// Cache entry with metadata.
#[derive(Clone)]
struct CacheEntry {
    /// The cached data.
    data: Arc<Vec<u8>>,
    /// When this entry was cached.
    cached_at: Instant,
    /// Number of times this entry was accessed.
    access_count: u64,
}

impl CacheEntry {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(data),
            cached_at: Instant::now(),
            access_count: 1,
        }
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn age(&self) -> Duration {
        self.cached_at.elapsed()
    }
}

/// Cache key combining chunk ID and optional shard index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Chunk ID.
    pub chunk_id: ChunkId,
    /// Shard index (None for whole chunk).
    pub shard_idx: Option<u32>,
}

impl CacheKey {
    /// Create a key for a whole chunk.
    pub fn chunk(chunk_id: ChunkId) -> Self {
        Self {
            chunk_id,
            shard_idx: None,
        }
    }

    /// Create a key for a specific shard.
    pub fn shard(chunk_id: ChunkId, shard_idx: u32) -> Self {
        Self {
            chunk_id,
            shard_idx: Some(shard_idx),
        }
    }
}

/// Configuration for the read cache.
#[derive(Debug, Clone)]
pub struct ReadCacheConfig {
    /// Maximum cache size in bytes.
    pub max_size_bytes: u64,
    /// Maximum number of entries.
    pub max_entries: usize,
    /// Maximum age for cache entries.
    pub max_age: Duration,
    /// Enable cache statistics.
    pub enable_stats: bool,
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 256 * 1024 * 1024, // 256 MB
            max_entries: 10_000,
            max_age: Duration::from_secs(300), // 5 minutes
            enable_stats: true,
        }
    }
}

impl ReadCacheConfig {
    /// Small cache configuration.
    pub fn small() -> Self {
        Self {
            max_size_bytes: 64 * 1024 * 1024, // 64 MB
            max_entries: 1_000,
            max_age: Duration::from_secs(120),
            enable_stats: true,
        }
    }

    /// Large cache configuration.
    pub fn large() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024, // 1 GB
            max_entries: 100_000,
            max_age: Duration::from_secs(600),
            enable_stats: true,
        }
    }
}

/// Read cache for chunk data.
pub struct ReadCache {
    /// The LRU cache.
    cache: RwLock<LruCache<CacheKey, CacheEntry>>,
    /// Configuration.
    config: ReadCacheConfig,
    /// Current cache size in bytes.
    current_size: AtomicU64,
    /// Cache hits counter.
    hits: AtomicU64,
    /// Cache misses counter.
    misses: AtomicU64,
    /// Bytes hit counter.
    bytes_hit: AtomicU64,
    /// Bytes missed counter.
    bytes_missed: AtomicU64,
}

impl ReadCache {
    /// Create a new read cache.
    pub fn new(config: ReadCacheConfig) -> Self {
        let max_entries = NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::MIN);

        Self {
            cache: RwLock::new(LruCache::new(max_entries)),
            config,
            current_size: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bytes_hit: AtomicU64::new(0),
            bytes_missed: AtomicU64::new(0),
        }
    }

    /// Get data from cache.
    pub async fn get(&self, key: &CacheKey) -> Option<Arc<Vec<u8>>> {
        let mut cache = self.cache.write().await;

        if let Some(entry) = cache.get_mut(key) {
            // Check if entry has expired
            if entry.age() > self.config.max_age {
                let size = entry.size() as u64;
                cache.pop(key);
                self.current_size.fetch_sub(size, Ordering::Relaxed);

                if self.config.enable_stats {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }

            entry.access_count += 1;

            if self.config.enable_stats {
                self.hits.fetch_add(1, Ordering::Relaxed);
                self.bytes_hit
                    .fetch_add(entry.data.len() as u64, Ordering::Relaxed);
            }

            Some(Arc::clone(&entry.data))
        } else {
            if self.config.enable_stats {
                self.misses.fetch_add(1, Ordering::Relaxed);
            }
            None
        }
    }

    /// Put data into cache.
    pub async fn put(&self, key: CacheKey, data: Vec<u8>) {
        let data_size = data.len() as u64;

        // Don't cache if it would exceed max size
        if data_size > self.config.max_size_bytes {
            return;
        }

        let mut cache = self.cache.write().await;

        // Evict entries if needed to make room
        while self.current_size.load(Ordering::Relaxed) + data_size > self.config.max_size_bytes {
            if let Some((_, evicted)) = cache.pop_lru() {
                self.current_size
                    .fetch_sub(evicted.size() as u64, Ordering::Relaxed);
            } else {
                break;
            }
        }

        // Remove existing entry if present
        if let Some(old) = cache.pop(&key) {
            self.current_size
                .fetch_sub(old.size() as u64, Ordering::Relaxed);
        }

        // Add new entry
        let entry = CacheEntry::new(data);
        self.current_size
            .fetch_add(entry.size() as u64, Ordering::Relaxed);
        cache.put(key, entry);
    }

    /// Remove an entry from cache.
    pub async fn remove(&self, key: &CacheKey) -> bool {
        let mut cache = self.cache.write().await;

        if let Some(entry) = cache.pop(key) {
            self.current_size
                .fetch_sub(entry.size() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Invalidate all entries for a chunk.
    pub async fn invalidate_chunk(&self, chunk_id: ChunkId) {
        let mut cache = self.cache.write().await;
        let mut to_remove = Vec::new();

        // Collect keys to remove
        for (key, _) in cache.iter() {
            if key.chunk_id == chunk_id {
                to_remove.push(*key);
            }
        }

        // Remove entries
        for key in to_remove {
            if let Some(entry) = cache.pop(&key) {
                self.current_size
                    .fetch_sub(entry.size() as u64, Ordering::Relaxed);
            }
        }
    }

    /// Clear the entire cache.
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;

        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_hit: self.bytes_hit.load(Ordering::Relaxed),
            bytes_missed: self.bytes_missed.load(Ordering::Relaxed),
            entries: cache.len(),
            size_bytes: self.current_size.load(Ordering::Relaxed),
            max_size_bytes: self.config.max_size_bytes,
        }
    }

    /// Get cache size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get number of entries.
    pub async fn len(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Check if cache is empty.
    pub async fn is_empty(&self) -> bool {
        self.cache.read().await.is_empty()
    }

    /// Evict expired entries.
    pub async fn evict_expired(&self) -> usize {
        let mut cache = self.cache.write().await;
        let mut evicted = 0;
        let mut to_remove = Vec::new();

        for (key, entry) in cache.iter() {
            if entry.age() > self.config.max_age {
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            if let Some(entry) = cache.pop(&key) {
                self.current_size
                    .fetch_sub(entry.size() as u64, Ordering::Relaxed);
                evicted += 1;
            }
        }

        evicted
    }
}

impl Default for ReadCache {
    fn default() -> Self {
        Self::new(ReadCacheConfig::default())
    }
}

/// Cache warming helper for pre-populating the cache.
pub struct CacheWarmer {
    cache: Arc<ReadCache>,
}

impl CacheWarmer {
    /// Create a new cache warmer.
    pub fn new(cache: Arc<ReadCache>) -> Self {
        Self { cache }
    }

    /// Warm the cache with data.
    pub async fn warm(&self, entries: Vec<(CacheKey, Vec<u8>)>) {
        for (key, data) in entries {
            self.cache.put(key, data).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk_id() -> ChunkId {
        ChunkId::new()
    }

    #[tokio::test]
    async fn test_cache_put_get() {
        let cache = ReadCache::new(ReadCacheConfig::default());
        let key = CacheKey::chunk(make_chunk_id());
        let data = vec![1, 2, 3, 4, 5];

        cache.put(key, data.clone()).await;

        let result = cache.get(&key).await;
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), data);
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = ReadCache::new(ReadCacheConfig::default());
        let key = CacheKey::chunk(make_chunk_id());

        let result = cache.get(&key).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cache_remove() {
        let cache = ReadCache::new(ReadCacheConfig::default());
        let key = CacheKey::chunk(make_chunk_id());
        let data = vec![1, 2, 3];

        cache.put(key, data).await;
        assert!(cache.get(&key).await.is_some());

        assert!(cache.remove(&key).await);
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = ReadCache::new(ReadCacheConfig::default());

        for i in 0..10 {
            let key = CacheKey::shard(make_chunk_id(), i);
            cache.put(key, vec![i as u8]).await;
        }

        assert!(!cache.is_empty().await);
        cache.clear().await;
        assert!(cache.is_empty().await);
        assert_eq!(cache.size_bytes(), 0);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let config = ReadCacheConfig {
            enable_stats: true,
            ..Default::default()
        };
        let cache = ReadCache::new(config);
        let key = CacheKey::chunk(make_chunk_id());
        let data = vec![1, 2, 3, 4, 5];

        // Miss
        cache.get(&key).await;

        // Put and hit
        cache.put(key, data.clone()).await;
        cache.get(&key).await;

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.bytes_hit, 5);
        assert_eq!(stats.entries, 1);
    }

    #[tokio::test]
    async fn test_cache_lru_eviction() {
        let config = ReadCacheConfig {
            max_size_bytes: 100, // Very small cache
            max_entries: 10,
            ..Default::default()
        };
        let cache = ReadCache::new(config);

        // Fill cache
        for i in 0..5 {
            let key = CacheKey::shard(make_chunk_id(), i);
            cache.put(key, vec![0; 30]).await; // 30 bytes each
        }

        // Cache should have evicted some entries
        assert!(cache.size_bytes() <= 100);
    }

    #[tokio::test]
    async fn test_cache_invalidate_chunk() {
        let cache = ReadCache::new(ReadCacheConfig::default());
        let chunk_id = make_chunk_id();

        // Add multiple shards for the same chunk
        for i in 0..3 {
            let key = CacheKey::shard(chunk_id, i);
            cache.put(key, vec![i as u8]).await;
        }

        // Add another chunk
        let other_chunk = make_chunk_id();
        let other_key = CacheKey::chunk(other_chunk);
        cache.put(other_key, vec![99]).await;

        assert_eq!(cache.len().await, 4);

        // Invalidate the first chunk
        cache.invalidate_chunk(chunk_id).await;

        assert_eq!(cache.len().await, 1);
        assert!(cache.get(&other_key).await.is_some());
    }

    #[tokio::test]
    async fn test_cache_key_types() {
        let chunk_id = make_chunk_id();

        let chunk_key = CacheKey::chunk(chunk_id);
        let shard_key = CacheKey::shard(chunk_id, 0);

        assert!(chunk_key.shard_idx.is_none());
        assert_eq!(shard_key.shard_idx, Some(0));
    }

    #[tokio::test]
    async fn test_hit_ratio() {
        let stats = CacheStats {
            hits: 75,
            misses: 25,
            ..Default::default()
        };

        assert!((stats.hit_ratio() - 0.75).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_config_presets() {
        let small = ReadCacheConfig::small();
        assert_eq!(small.max_size_bytes, 64 * 1024 * 1024);

        let large = ReadCacheConfig::large();
        assert_eq!(large.max_size_bytes, 1024 * 1024 * 1024);
    }
}

// ============================================================================
// Adaptive Caching
// ============================================================================

/// Configuration for adaptive caching behavior.
#[derive(Debug, Clone)]
pub struct AdaptiveCacheConfig {
    /// Minimum cache size in bytes.
    pub min_size_bytes: u64,
    /// Maximum cache size in bytes.
    pub max_size_bytes: u64,
    /// Target hit ratio (0.0 - 1.0).
    pub target_hit_ratio: f64,
    /// How often to evaluate cache performance.
    pub evaluation_interval: Duration,
    /// Minimum samples before making adjustments.
    pub min_samples_for_adjustment: u64,
    /// Size adjustment step (percentage).
    pub adjustment_step_percent: f64,
    /// Enable adaptive sizing.
    pub enable_adaptive_sizing: bool,
    /// Enable adaptive TTL.
    pub enable_adaptive_ttl: bool,
    /// Minimum TTL.
    pub min_ttl: Duration,
    /// Maximum TTL.
    pub max_ttl: Duration,
}

impl Default for AdaptiveCacheConfig {
    fn default() -> Self {
        Self {
            min_size_bytes: 64 * 1024 * 1024,       // 64 MB
            max_size_bytes: 2 * 1024 * 1024 * 1024, // 2 GB
            target_hit_ratio: 0.8,
            evaluation_interval: Duration::from_secs(60),
            min_samples_for_adjustment: 1000,
            adjustment_step_percent: 10.0,
            enable_adaptive_sizing: true,
            enable_adaptive_ttl: true,
            min_ttl: Duration::from_secs(30),
            max_ttl: Duration::from_secs(3600),
        }
    }
}

impl AdaptiveCacheConfig {
    /// Conservative configuration (slower adjustments).
    pub fn conservative() -> Self {
        Self {
            min_size_bytes: 128 * 1024 * 1024,
            max_size_bytes: 4 * 1024 * 1024 * 1024,
            target_hit_ratio: 0.85,
            evaluation_interval: Duration::from_secs(300),
            min_samples_for_adjustment: 5000,
            adjustment_step_percent: 5.0,
            enable_adaptive_sizing: true,
            enable_adaptive_ttl: true,
            min_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(7200),
        }
    }

    /// Aggressive configuration (faster adjustments).
    pub fn aggressive() -> Self {
        Self {
            min_size_bytes: 32 * 1024 * 1024,
            max_size_bytes: 8 * 1024 * 1024 * 1024,
            target_hit_ratio: 0.75,
            evaluation_interval: Duration::from_secs(30),
            min_samples_for_adjustment: 500,
            adjustment_step_percent: 20.0,
            enable_adaptive_sizing: true,
            enable_adaptive_ttl: true,
            min_ttl: Duration::from_secs(15),
            max_ttl: Duration::from_secs(1800),
        }
    }
}

/// Workload pattern classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPattern {
    /// Random access pattern with low locality.
    Random,
    /// Sequential access pattern with high locality.
    Sequential,
    /// Mixed workload with moderate locality.
    Mixed,
    /// Hot/cold pattern with a working set.
    HotCold,
}

/// Adaptive cache statistics.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveStats {
    /// Current effective cache size.
    pub effective_size_bytes: u64,
    /// Current effective TTL.
    pub effective_ttl: Duration,
    /// Detected workload pattern.
    pub detected_pattern: Option<WorkloadPattern>,
    /// Number of size adjustments made.
    pub size_adjustments: u64,
    /// Number of TTL adjustments made.
    pub ttl_adjustments: u64,
    /// Last evaluation timestamp.
    pub last_evaluation: Option<Instant>,
    /// Rolling hit ratio (recent).
    pub rolling_hit_ratio: f64,
    /// Memory efficiency (useful bytes / total bytes).
    pub memory_efficiency: f64,
}

/// Adaptive cache manager that wraps ReadCache and adjusts parameters dynamically.
pub struct AdaptiveCache {
    /// Underlying cache.
    cache: Arc<ReadCache>,
    /// Adaptive configuration.
    config: AdaptiveCacheConfig,
    /// Adaptive statistics.
    stats: Arc<RwLock<AdaptiveStats>>,
    /// Historical hit ratios for pattern detection.
    hit_ratio_history: Arc<RwLock<Vec<f64>>>,
    /// Current effective size (may differ from cache config).
    effective_size: Arc<AtomicU64>,
    /// Current effective TTL in seconds.
    effective_ttl_secs: Arc<AtomicU64>,
    /// Previous stats snapshot for delta calculation.
    prev_stats: Arc<RwLock<CacheStats>>,
}

impl AdaptiveCache {
    /// Create a new adaptive cache.
    pub fn new(initial_size_bytes: u64, config: AdaptiveCacheConfig) -> Self {
        let cache_config = ReadCacheConfig {
            max_size_bytes: initial_size_bytes,
            max_entries: (initial_size_bytes / (64 * 1024)) as usize, // Estimate based on chunk size
            max_age: Duration::from_secs(300),
            enable_stats: true,
        };

        let cache = Arc::new(ReadCache::new(cache_config));
        let effective_ttl_secs = 300u64;

        Self {
            cache,
            config,
            stats: Arc::new(RwLock::new(AdaptiveStats {
                effective_size_bytes: initial_size_bytes,
                effective_ttl: Duration::from_secs(effective_ttl_secs),
                ..Default::default()
            })),
            hit_ratio_history: Arc::new(RwLock::new(Vec::with_capacity(100))),
            effective_size: Arc::new(AtomicU64::new(initial_size_bytes)),
            effective_ttl_secs: Arc::new(AtomicU64::new(effective_ttl_secs)),
            prev_stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Get data from cache.
    pub async fn get(&self, key: &CacheKey) -> Option<Arc<Vec<u8>>> {
        self.cache.get(key).await
    }

    /// Put data into cache.
    pub async fn put(&self, key: CacheKey, data: Vec<u8>) {
        self.cache.put(key, data).await;
    }

    /// Remove an entry from cache.
    pub async fn remove(&self, key: &CacheKey) -> bool {
        self.cache.remove(key).await
    }

    /// Get current cache statistics.
    pub async fn stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Get adaptive cache statistics.
    pub async fn adaptive_stats(&self) -> AdaptiveStats {
        self.stats.read().await.clone()
    }

    /// Evaluate cache performance and adjust parameters.
    /// Call this periodically (e.g., every minute).
    pub async fn evaluate_and_adjust(&self) {
        let current_stats = self.cache.stats().await;
        let prev_stats = self.prev_stats.read().await.clone();

        // Calculate delta stats
        let delta_hits = current_stats.hits.saturating_sub(prev_stats.hits);
        let delta_misses = current_stats.misses.saturating_sub(prev_stats.misses);
        let total_requests = delta_hits + delta_misses;

        // Need minimum samples to make decisions
        if total_requests < self.config.min_samples_for_adjustment {
            return;
        }

        let current_hit_ratio = if total_requests > 0 {
            delta_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        // Update history
        {
            let mut history = self.hit_ratio_history.write().await;
            history.push(current_hit_ratio);
            if history.len() > 100 {
                history.remove(0);
            }
        }

        // Detect workload pattern
        let pattern = self.detect_workload_pattern().await;

        // Adjust cache size if enabled
        if self.config.enable_adaptive_sizing {
            self.adjust_size(current_hit_ratio, pattern).await;
        }

        // Adjust TTL if enabled
        if self.config.enable_adaptive_ttl {
            self.adjust_ttl(current_hit_ratio, pattern).await;
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.effective_size_bytes = self.effective_size.load(Ordering::Relaxed);
            stats.effective_ttl = Duration::from_secs(self.effective_ttl_secs.load(Ordering::Relaxed));
            stats.detected_pattern = Some(pattern);
            stats.last_evaluation = Some(Instant::now());
            stats.rolling_hit_ratio = current_hit_ratio;
            stats.memory_efficiency = self.calculate_memory_efficiency(&current_stats);
        }

        // Update previous stats
        *self.prev_stats.write().await = current_stats;
    }

    /// Detect workload pattern from access history.
    async fn detect_workload_pattern(&self) -> WorkloadPattern {
        let history = self.hit_ratio_history.read().await;

        if history.len() < 10 {
            return WorkloadPattern::Mixed;
        }

        let avg_hit_ratio: f64 = history.iter().sum::<f64>() / history.len() as f64;

        // Calculate variance
        let variance: f64 = history
            .iter()
            .map(|r| (r - avg_hit_ratio).powi(2))
            .sum::<f64>()
            / history.len() as f64;

        let std_dev = variance.sqrt();

        // Classify based on hit ratio and stability
        if avg_hit_ratio > 0.9 && std_dev < 0.05 {
            WorkloadPattern::Sequential
        } else if avg_hit_ratio < 0.3 && std_dev < 0.1 {
            WorkloadPattern::Random
        } else if std_dev > 0.2 {
            WorkloadPattern::HotCold
        } else {
            WorkloadPattern::Mixed
        }
    }

    /// Adjust cache size based on performance.
    async fn adjust_size(&self, hit_ratio: f64, pattern: WorkloadPattern) {
        let current_size = self.effective_size.load(Ordering::Relaxed);
        let step = (current_size as f64 * self.config.adjustment_step_percent / 100.0) as u64;

        let new_size = if hit_ratio < self.config.target_hit_ratio - 0.1 {
            // Hit ratio too low, increase cache size
            match pattern {
                WorkloadPattern::Random => {
                    // Random workload benefits less from larger cache
                    (current_size + step / 2).min(self.config.max_size_bytes)
                }
                WorkloadPattern::Sequential | WorkloadPattern::HotCold => {
                    // These patterns benefit more from larger cache
                    (current_size + step * 2).min(self.config.max_size_bytes)
                }
                WorkloadPattern::Mixed => {
                    (current_size + step).min(self.config.max_size_bytes)
                }
            }
        } else if hit_ratio > self.config.target_hit_ratio + 0.1 {
            // Hit ratio exceeds target, can reduce cache size
            (current_size.saturating_sub(step / 2)).max(self.config.min_size_bytes)
        } else {
            current_size // No adjustment needed
        };

        if new_size != current_size {
            self.effective_size.store(new_size, Ordering::Relaxed);
            let mut stats = self.stats.write().await;
            stats.size_adjustments += 1;
            tracing::info!(
                old_size = current_size,
                new_size = new_size,
                hit_ratio = hit_ratio,
                "Adjusted cache size"
            );
        }
    }

    /// Adjust TTL based on workload pattern.
    async fn adjust_ttl(&self, hit_ratio: f64, pattern: WorkloadPattern) {
        let current_ttl = self.effective_ttl_secs.load(Ordering::Relaxed);

        let new_ttl = match pattern {
            WorkloadPattern::Sequential => {
                // Sequential access: shorter TTL since data is accessed once
                self.config.min_ttl.as_secs().max(current_ttl.saturating_sub(30))
            }
            WorkloadPattern::HotCold => {
                // Hot/cold: longer TTL to keep hot data
                self.config.max_ttl.as_secs().min(current_ttl + 60)
            }
            WorkloadPattern::Random => {
                // Random: shorter TTL since locality is low
                self.config.min_ttl.as_secs().max(current_ttl.saturating_sub(15))
            }
            WorkloadPattern::Mixed => {
                // Mixed: adjust based on hit ratio
                if hit_ratio < self.config.target_hit_ratio {
                    self.config.max_ttl.as_secs().min(current_ttl + 30)
                } else {
                    current_ttl
                }
            }
        };

        let new_ttl = new_ttl
            .max(self.config.min_ttl.as_secs())
            .min(self.config.max_ttl.as_secs());

        if new_ttl != current_ttl {
            self.effective_ttl_secs.store(new_ttl, Ordering::Relaxed);
            let mut stats = self.stats.write().await;
            stats.ttl_adjustments += 1;
            tracing::debug!(
                old_ttl = current_ttl,
                new_ttl = new_ttl,
                pattern = ?pattern,
                "Adjusted cache TTL"
            );
        }
    }

    /// Calculate memory efficiency.
    fn calculate_memory_efficiency(&self, stats: &CacheStats) -> f64 {
        if stats.bytes_hit + stats.bytes_missed == 0 {
            return 0.0;
        }
        stats.bytes_hit as f64 / (stats.bytes_hit + stats.bytes_missed) as f64
    }

    /// Get the underlying cache for direct access.
    pub fn inner(&self) -> &Arc<ReadCache> {
        &self.cache
    }

    /// Get effective cache size.
    pub fn effective_size(&self) -> u64 {
        self.effective_size.load(Ordering::Relaxed)
    }

    /// Get effective TTL.
    pub fn effective_ttl(&self) -> Duration {
        Duration::from_secs(self.effective_ttl_secs.load(Ordering::Relaxed))
    }

    /// Run the adaptive evaluation loop.
    pub async fn run_adaptive_loop(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let interval = self.config.evaluation_interval;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    self.evaluate_and_adjust().await;
                }
                _ = shutdown.recv() => {
                    tracing::info!("Adaptive cache loop shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod adaptive_tests {
    use super::*;

    #[tokio::test]
    async fn test_adaptive_cache_basic() {
        let cache = AdaptiveCache::new(
            256 * 1024 * 1024,
            AdaptiveCacheConfig::default(),
        );

        let key = CacheKey::chunk(ChunkId::new());
        let data = vec![1, 2, 3, 4, 5];

        cache.put(key, data.clone()).await;

        let result = cache.get(&key).await;
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), data);
    }

    #[tokio::test]
    async fn test_adaptive_stats() {
        let cache = AdaptiveCache::new(
            256 * 1024 * 1024,
            AdaptiveCacheConfig::default(),
        );

        let stats = cache.adaptive_stats().await;
        assert_eq!(stats.effective_size_bytes, 256 * 1024 * 1024);
        assert_eq!(stats.effective_ttl, Duration::from_secs(300));
    }

    #[tokio::test]
    async fn test_workload_pattern_detection() {
        let cache = AdaptiveCache::new(
            256 * 1024 * 1024,
            AdaptiveCacheConfig::default(),
        );

        // Add some history
        {
            let mut history = cache.hit_ratio_history.write().await;
            for _ in 0..20 {
                history.push(0.95); // High stable hit ratio
            }
        }

        let pattern = cache.detect_workload_pattern().await;
        assert_eq!(pattern, WorkloadPattern::Sequential);
    }

    #[test]
    fn test_adaptive_config_presets() {
        let conservative = AdaptiveCacheConfig::conservative();
        assert_eq!(conservative.adjustment_step_percent, 5.0);

        let aggressive = AdaptiveCacheConfig::aggressive();
        assert_eq!(aggressive.adjustment_step_percent, 20.0);
    }
}
