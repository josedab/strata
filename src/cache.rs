//! Read caching module for Strata.
//!
//! Provides LRU caching for chunk data to improve read performance.

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
