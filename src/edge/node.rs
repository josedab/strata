// Edge Cache Node

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Edge node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Node ID
    pub node_id: String,
    /// Node name
    pub name: String,
    /// Geographic region
    pub region: String,
    /// Geographic location
    pub location: GeoLocation,
    /// Cache capacity in bytes
    pub capacity: u64,
    /// Default TTL in seconds
    pub default_ttl_secs: u64,
    /// Maximum object size
    pub max_object_size: u64,
    /// Enable compression
    pub compress: bool,
    /// Compression threshold
    pub compress_threshold: usize,
    /// Cache policy
    pub policy: CachePolicy,
    /// Origin endpoint
    pub origin: String,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "edge-node".to_string(),
            region: "us-east-1".to_string(),
            location: GeoLocation::default(),
            capacity: 10 * 1024 * 1024 * 1024, // 10GB
            default_ttl_secs: 3600,
            max_object_size: 100 * 1024 * 1024, // 100MB
            compress: true,
            compress_threshold: 1024,
            policy: CachePolicy::Lru,
            origin: String::new(),
        }
    }
}

/// Geographic location
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeoLocation {
    /// Latitude
    pub latitude: f64,
    /// Longitude
    pub longitude: f64,
    /// City
    pub city: Option<String>,
    /// Country code
    pub country: Option<String>,
}

/// Cache eviction policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    /// Least Recently Used
    Lru,
    /// Least Frequently Used
    Lfu,
    /// First In First Out
    Fifo,
    /// Time-based eviction only
    Ttl,
    /// Adaptive policy
    Adaptive,
}

/// Cached object
#[derive(Debug, Clone)]
pub struct CachedObject {
    /// Object key
    pub key: String,
    /// Object data
    pub data: Vec<u8>,
    /// Content type
    pub content_type: String,
    /// ETag
    pub etag: String,
    /// Size in bytes
    pub size: u64,
    /// Creation time
    pub created_at: Instant,
    /// Expiration time
    pub expires_at: Instant,
    /// Last accessed time
    pub last_accessed: Instant,
    /// Access count
    pub access_count: u64,
    /// Is compressed
    pub compressed: bool,
    /// Original size (if compressed)
    pub original_size: Option<u64>,
    /// Custom headers
    pub headers: HashMap<String, String>,
}

impl CachedObject {
    /// Creates a new cached object
    pub fn new(key: String, data: Vec<u8>, ttl: Duration) -> Self {
        let now = Instant::now();
        let size = data.len() as u64;

        Self {
            key,
            data,
            content_type: "application/octet-stream".to_string(),
            etag: uuid::Uuid::new_v4().to_string(),
            size,
            created_at: now,
            expires_at: now + ttl,
            last_accessed: now,
            access_count: 0,
            compressed: false,
            original_size: None,
            headers: HashMap::new(),
        }
    }

    /// Checks if object is expired
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Updates access time and count
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }

    /// Gets time until expiration
    pub fn ttl(&self) -> Duration {
        let now = Instant::now();
        if now >= self.expires_at {
            Duration::ZERO
        } else {
            self.expires_at - now
        }
    }
}

/// Edge cache node
pub struct EdgeNode {
    /// Configuration
    config: EdgeConfig,
    /// Cache storage
    cache: Arc<RwLock<HashMap<String, CachedObject>>>,
    /// LRU order tracking
    lru_order: Arc<RwLock<Vec<String>>>,
    /// Current size
    current_size: AtomicU64,
    /// Statistics
    stats: Arc<EdgeStats>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

/// Edge node statistics
pub struct EdgeStats {
    /// Cache hits
    pub hits: AtomicU64,
    /// Cache misses
    pub misses: AtomicU64,
    /// Bytes served from cache
    pub bytes_served: AtomicU64,
    /// Bytes fetched from origin
    pub bytes_fetched: AtomicU64,
    /// Evictions
    pub evictions: AtomicU64,
    /// Objects cached
    pub objects_cached: AtomicU64,
    /// Expired objects
    pub expired: AtomicU64,
}

impl Default for EdgeStats {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bytes_served: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            objects_cached: AtomicU64::new(0),
            expired: AtomicU64::new(0),
        }
    }
}

impl EdgeNode {
    /// Creates a new edge node
    pub fn new(config: EdgeConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
            lru_order: Arc::new(RwLock::new(Vec::new())),
            current_size: AtomicU64::new(0),
            stats: Arc::new(EdgeStats::default()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts the edge node
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = true;
        Ok(())
    }

    /// Stops the edge node
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    /// Gets an object from cache
    pub async fn get(&self, key: &str) -> Option<CachedObject> {
        let mut cache = self.cache.write().await;

        if let Some(obj) = cache.get_mut(key) {
            if obj.is_expired() {
                // Remove expired object
                let size = obj.size;
                cache.remove(key);
                self.current_size.fetch_sub(size, Ordering::Relaxed);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            obj.touch();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_served.fetch_add(obj.size, Ordering::Relaxed);

            // Update LRU order
            drop(cache);
            self.update_lru(key).await;

            let cache = self.cache.read().await;
            return cache.get(key).cloned();
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Puts an object in cache
    pub async fn put(&self, key: String, data: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let data_size = data.len() as u64;

        if data_size > self.config.max_object_size {
            return Err(StrataError::InvalidOperation(
                "Object exceeds maximum size".to_string(),
            ));
        }

        // Evict if necessary
        while self.current_size.load(Ordering::Relaxed) + data_size > self.config.capacity {
            if !self.evict_one().await {
                break;
            }
        }

        let ttl = ttl.unwrap_or(Duration::from_secs(self.config.default_ttl_secs));
        let obj = CachedObject::new(key.clone(), data, ttl);

        let mut cache = self.cache.write().await;

        // Remove existing object if present
        if let Some(existing) = cache.remove(&key) {
            self.current_size.fetch_sub(existing.size, Ordering::Relaxed);
        }

        cache.insert(key.clone(), obj);
        self.current_size.fetch_add(data_size, Ordering::Relaxed);
        self.stats.objects_cached.fetch_add(1, Ordering::Relaxed);

        drop(cache);

        // Update LRU order
        self.update_lru(&key).await;

        Ok(())
    }

    /// Removes an object from cache
    pub async fn remove(&self, key: &str) -> bool {
        let mut cache = self.cache.write().await;

        if let Some(obj) = cache.remove(key) {
            self.current_size.fetch_sub(obj.size, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Updates LRU order
    async fn update_lru(&self, key: &str) {
        let mut order = self.lru_order.write().await;
        order.retain(|k| k != key);
        order.push(key.to_string());
    }

    /// Evicts one object based on policy
    async fn evict_one(&self) -> bool {
        match self.config.policy {
            CachePolicy::Lru => self.evict_lru().await,
            CachePolicy::Lfu => self.evict_lfu().await,
            CachePolicy::Fifo => self.evict_fifo().await,
            CachePolicy::Ttl => self.evict_expired().await,
            CachePolicy::Adaptive => self.evict_adaptive().await,
        }
    }

    /// Evicts least recently used object
    async fn evict_lru(&self) -> bool {
        let mut order = self.lru_order.write().await;

        if let Some(key) = order.first().cloned() {
            order.remove(0);
            drop(order);

            let mut cache = self.cache.write().await;
            if let Some(obj) = cache.remove(&key) {
                self.current_size.fetch_sub(obj.size, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        false
    }

    /// Evicts least frequently used object
    async fn evict_lfu(&self) -> bool {
        let mut cache = self.cache.write().await;

        let min_key = cache.iter()
            .min_by_key(|(_, v)| v.access_count)
            .map(|(k, _)| k.clone());

        if let Some(key) = min_key {
            if let Some(obj) = cache.remove(&key) {
                self.current_size.fetch_sub(obj.size, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        false
    }

    /// Evicts oldest object (FIFO)
    async fn evict_fifo(&self) -> bool {
        let mut cache = self.cache.write().await;

        let oldest_key = cache.iter()
            .min_by_key(|(_, v)| v.created_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key {
            if let Some(obj) = cache.remove(&key) {
                self.current_size.fetch_sub(obj.size, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        false
    }

    /// Evicts expired objects
    async fn evict_expired(&self) -> bool {
        let mut cache = self.cache.write().await;

        let expired_keys: Vec<_> = cache.iter()
            .filter(|(_, v)| v.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        let evicted = !expired_keys.is_empty();
        for key in expired_keys {
            if let Some(obj) = cache.remove(&key) {
                self.current_size.fetch_sub(obj.size, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
            }
        }

        evicted
    }

    /// Adaptive eviction
    async fn evict_adaptive(&self) -> bool {
        // First try expired
        if self.evict_expired().await {
            return true;
        }
        // Then LRU
        self.evict_lru().await
    }

    /// Clears all cached objects
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        self.current_size.store(0, Ordering::Relaxed);

        let mut order = self.lru_order.write().await;
        order.clear();
    }

    /// Gets cache statistics
    pub fn stats(&self) -> EdgeStatsSnapshot {
        EdgeStatsSnapshot {
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            bytes_served: self.stats.bytes_served.load(Ordering::Relaxed),
            bytes_fetched: self.stats.bytes_fetched.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            objects_cached: self.stats.objects_cached.load(Ordering::Relaxed),
            expired: self.stats.expired.load(Ordering::Relaxed),
            current_size: self.current_size.load(Ordering::Relaxed),
            capacity: self.config.capacity,
        }
    }

    /// Gets configuration
    pub fn config(&self) -> &EdgeConfig {
        &self.config
    }

    /// Gets object count
    pub async fn object_count(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Lists cached keys
    pub async fn list_keys(&self) -> Vec<String> {
        self.cache.read().await.keys().cloned().collect()
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bytes_served: u64,
    pub bytes_fetched: u64,
    pub evictions: u64,
    pub objects_cached: u64,
    pub expired: u64,
    pub current_size: u64,
    pub capacity: u64,
}

impl EdgeStatsSnapshot {
    /// Gets hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Gets utilization
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.current_size as f64 / self.capacity as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_edge_node_basic() {
        let config = EdgeConfig {
            capacity: 1024 * 1024,
            ..Default::default()
        };
        let node = EdgeNode::new(config);
        node.start().await.unwrap();

        // Put and get
        node.put("key1".to_string(), vec![1, 2, 3], None).await.unwrap();

        let obj = node.get("key1").await.unwrap();
        assert_eq!(obj.data, vec![1, 2, 3]);

        let stats = node.stats();
        assert_eq!(stats.hits, 1);
    }

    #[tokio::test]
    async fn test_edge_node_eviction() {
        let config = EdgeConfig {
            capacity: 100, // Small capacity
            policy: CachePolicy::Lru,
            ..Default::default()
        };
        let node = EdgeNode::new(config);

        // Fill cache
        node.put("key1".to_string(), vec![0u8; 50], None).await.unwrap();
        node.put("key2".to_string(), vec![0u8; 50], None).await.unwrap();

        // This should evict key1
        node.put("key3".to_string(), vec![0u8; 50], None).await.unwrap();

        assert!(node.get("key1").await.is_none());
        assert!(node.get("key3").await.is_some());
    }

    #[tokio::test]
    async fn test_edge_node_expiration() {
        let config = EdgeConfig::default();
        let node = EdgeNode::new(config);

        // Put with very short TTL
        node.put(
            "key1".to_string(),
            vec![1, 2, 3],
            Some(Duration::from_millis(1)),
        ).await.unwrap();

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Should be expired
        assert!(node.get("key1").await.is_none());
    }
}
