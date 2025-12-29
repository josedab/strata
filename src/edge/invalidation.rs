// Edge Cache Invalidation

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Invalidation strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvalidationStrategy {
    /// Immediate invalidation
    Immediate,
    /// Lazy invalidation on next access
    Lazy,
    /// Time-based invalidation
    TimeBased,
    /// Version-based invalidation
    Versioned,
    /// Tag-based invalidation
    Tagged,
    /// Purge everything
    Purge,
}

/// Invalidation event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationEvent {
    /// Event ID
    pub id: u64,
    /// Strategy used
    pub strategy: InvalidationStrategy,
    /// Pattern or key to invalidate
    pub pattern: InvalidationPattern,
    /// Source node that initiated
    pub source_node: String,
    /// Timestamp
    pub timestamp: u64,
    /// Version (for versioned invalidation)
    pub version: Option<u64>,
    /// Tags (for tagged invalidation)
    pub tags: Vec<String>,
    /// Priority (higher = more urgent)
    pub priority: u8,
}

impl InvalidationEvent {
    /// Creates a new invalidation event
    pub fn new(strategy: InvalidationStrategy, pattern: InvalidationPattern, source: String) -> Self {
        Self {
            id: 0,
            strategy,
            pattern,
            source_node: source,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            version: None,
            tags: Vec::new(),
            priority: 5,
        }
    }

    /// Sets version for versioned invalidation
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }

    /// Sets tags for tagged invalidation
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Sets priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// Pattern for matching keys to invalidate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvalidationPattern {
    /// Exact key match
    Exact(String),
    /// Prefix match
    Prefix(String),
    /// Suffix match
    Suffix(String),
    /// Glob pattern
    Glob(String),
    /// Regex pattern
    Regex(String),
    /// All keys
    All,
    /// Multiple patterns
    Multi(Vec<InvalidationPattern>),
}

impl InvalidationPattern {
    /// Checks if a key matches this pattern
    pub fn matches(&self, key: &str) -> bool {
        match self {
            InvalidationPattern::Exact(pattern) => key == pattern,
            InvalidationPattern::Prefix(prefix) => key.starts_with(prefix),
            InvalidationPattern::Suffix(suffix) => key.ends_with(suffix),
            InvalidationPattern::Glob(pattern) => Self::glob_match(pattern, key),
            InvalidationPattern::Regex(pattern) => {
                regex::Regex::new(pattern)
                    .map(|re| re.is_match(key))
                    .unwrap_or(false)
            }
            InvalidationPattern::All => true,
            InvalidationPattern::Multi(patterns) => {
                patterns.iter().any(|p| p.matches(key))
            }
        }
    }

    /// Simple glob matching (supports * and ?)
    fn glob_match(pattern: &str, text: &str) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut text_chars = text.chars().peekable();

        while let Some(p) = pattern_chars.next() {
            match p {
                '*' => {
                    // Match zero or more characters
                    if pattern_chars.peek().is_none() {
                        return true;
                    }
                    // Try matching from current position onwards
                    let remaining_pattern: String = pattern_chars.collect();
                    let remaining_text: String = text_chars.collect();
                    for i in 0..=remaining_text.len() {
                        if Self::glob_match(&remaining_pattern, &remaining_text[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                '?' => {
                    // Match exactly one character
                    if text_chars.next().is_none() {
                        return false;
                    }
                }
                c => {
                    if text_chars.next() != Some(c) {
                        return false;
                    }
                }
            }
        }

        text_chars.peek().is_none()
    }
}

/// Invalidation manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationConfig {
    /// Default strategy
    pub default_strategy: InvalidationStrategy,
    /// Batch size for bulk invalidations
    pub batch_size: usize,
    /// Propagation delay in milliseconds
    pub propagation_delay_ms: u64,
    /// Enable cascade invalidation
    pub cascade: bool,
    /// Maximum pending invalidations
    pub max_pending: usize,
    /// Retry failed invalidations
    pub retry_failed: bool,
    /// Maximum retries
    pub max_retries: u32,
}

impl Default for InvalidationConfig {
    fn default() -> Self {
        Self {
            default_strategy: InvalidationStrategy::Immediate,
            batch_size: 100,
            propagation_delay_ms: 0,
            cascade: true,
            max_pending: 10000,
            retry_failed: true,
            max_retries: 3,
        }
    }
}

/// Invalidation manager
pub struct InvalidationManager {
    /// Configuration
    config: InvalidationConfig,
    /// Local node ID
    local_node_id: String,
    /// Event counter
    event_counter: AtomicU64,
    /// Pending invalidations
    pending: Arc<RwLock<Vec<InvalidationEvent>>>,
    /// Processed event IDs (for deduplication)
    processed: Arc<RwLock<HashSet<u64>>>,
    /// Event broadcast channel
    broadcast_tx: broadcast::Sender<InvalidationEvent>,
    /// Tag index (tag -> keys)
    tag_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Version index (key -> version)
    version_index: Arc<RwLock<HashMap<String, u64>>>,
    /// Statistics
    stats: Arc<InvalidationStats>,
    /// Invalidation handlers
    handlers: Arc<RwLock<Vec<Box<dyn InvalidationHandler>>>>,
}

/// Invalidation handler trait
pub trait InvalidationHandler: Send + Sync {
    /// Called when keys should be invalidated
    fn invalidate(&self, keys: &[String]) -> Result<usize>;

    /// Called for pattern-based invalidation
    fn invalidate_pattern(&self, pattern: &InvalidationPattern) -> Result<usize>;

    /// Gets all keys (for pattern matching)
    fn get_keys(&self) -> Result<Vec<String>>;
}

/// Invalidation statistics
pub struct InvalidationStats {
    /// Events processed
    pub events_processed: AtomicU64,
    /// Keys invalidated
    pub keys_invalidated: AtomicU64,
    /// Events propagated
    pub events_propagated: AtomicU64,
    /// Failed invalidations
    pub failed: AtomicU64,
    /// Duplicate events filtered
    pub duplicates_filtered: AtomicU64,
}

impl Default for InvalidationStats {
    fn default() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            keys_invalidated: AtomicU64::new(0),
            events_propagated: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            duplicates_filtered: AtomicU64::new(0),
        }
    }
}

impl InvalidationManager {
    /// Creates a new invalidation manager
    pub fn new(local_node_id: String, config: InvalidationConfig) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1000);

        Self {
            config,
            local_node_id,
            event_counter: AtomicU64::new(0),
            pending: Arc::new(RwLock::new(Vec::new())),
            processed: Arc::new(RwLock::new(HashSet::new())),
            broadcast_tx,
            tag_index: Arc::new(RwLock::new(HashMap::new())),
            version_index: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(InvalidationStats::default()),
            handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Registers an invalidation handler
    pub async fn register_handler(&self, handler: Box<dyn InvalidationHandler>) {
        let mut handlers = self.handlers.write().await;
        handlers.push(handler);
    }

    /// Subscribes to invalidation events
    pub fn subscribe(&self) -> broadcast::Receiver<InvalidationEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Invalidates a single key
    pub async fn invalidate_key(&self, key: &str) -> Result<InvalidationResult> {
        let event = InvalidationEvent::new(
            self.config.default_strategy,
            InvalidationPattern::Exact(key.to_string()),
            self.local_node_id.clone(),
        );

        self.process_event(event).await
    }

    /// Invalidates keys matching a prefix
    pub async fn invalidate_prefix(&self, prefix: &str) -> Result<InvalidationResult> {
        let event = InvalidationEvent::new(
            self.config.default_strategy,
            InvalidationPattern::Prefix(prefix.to_string()),
            self.local_node_id.clone(),
        );

        self.process_event(event).await
    }

    /// Invalidates keys matching a pattern
    pub async fn invalidate_pattern(&self, pattern: InvalidationPattern) -> Result<InvalidationResult> {
        let event = InvalidationEvent::new(
            self.config.default_strategy,
            pattern,
            self.local_node_id.clone(),
        );

        self.process_event(event).await
    }

    /// Invalidates keys by tag
    pub async fn invalidate_by_tag(&self, tag: &str) -> Result<InvalidationResult> {
        let tag_index = self.tag_index.read().await;

        let keys: Vec<String> = tag_index
            .get(tag)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default();

        drop(tag_index);

        if keys.is_empty() {
            return Ok(InvalidationResult {
                keys_invalidated: 0,
                nodes_notified: 0,
                strategy: InvalidationStrategy::Tagged,
            });
        }

        let pattern = InvalidationPattern::Multi(
            keys.iter().map(|k| InvalidationPattern::Exact(k.clone())).collect()
        );

        let mut event = InvalidationEvent::new(
            InvalidationStrategy::Tagged,
            pattern,
            self.local_node_id.clone(),
        );
        event.tags = vec![tag.to_string()];

        self.process_event(event).await
    }

    /// Invalidates keys by version (keys older than specified version)
    pub async fn invalidate_by_version(&self, key: &str, version: u64) -> Result<InvalidationResult> {
        let version_index = self.version_index.read().await;

        if let Some(&current_version) = version_index.get(key) {
            if current_version >= version {
                // Already at or newer version
                return Ok(InvalidationResult {
                    keys_invalidated: 0,
                    nodes_notified: 0,
                    strategy: InvalidationStrategy::Versioned,
                });
            }
        }

        drop(version_index);

        let mut event = InvalidationEvent::new(
            InvalidationStrategy::Versioned,
            InvalidationPattern::Exact(key.to_string()),
            self.local_node_id.clone(),
        );
        event = event.with_version(version);

        // Update version index
        let mut version_index = self.version_index.write().await;
        version_index.insert(key.to_string(), version);
        drop(version_index);

        self.process_event(event).await
    }

    /// Purges all cached content
    pub async fn purge_all(&self) -> Result<InvalidationResult> {
        let event = InvalidationEvent::new(
            InvalidationStrategy::Purge,
            InvalidationPattern::All,
            self.local_node_id.clone(),
        );

        self.process_event(event).await
    }

    /// Processes an invalidation event
    async fn process_event(&self, mut event: InvalidationEvent) -> Result<InvalidationResult> {
        // Assign event ID
        event.id = self.event_counter.fetch_add(1, Ordering::SeqCst);

        // Check for duplicates
        {
            let processed = self.processed.read().await;
            if processed.contains(&event.id) {
                self.stats.duplicates_filtered.fetch_add(1, Ordering::Relaxed);
                return Ok(InvalidationResult {
                    keys_invalidated: 0,
                    nodes_notified: 0,
                    strategy: event.strategy,
                });
            }
        }

        // Mark as processed
        {
            let mut processed = self.processed.write().await;
            processed.insert(event.id);

            // Limit processed set size
            if processed.len() > 100000 {
                // Keep only recent IDs
                let threshold = event.id.saturating_sub(50000);
                processed.retain(|&id| id > threshold);
            }
        }

        // Execute invalidation based on strategy
        let keys_invalidated = match event.strategy {
            InvalidationStrategy::Immediate => {
                self.execute_immediate(&event).await?
            }
            InvalidationStrategy::Lazy => {
                self.queue_lazy(&event).await?
            }
            InvalidationStrategy::TimeBased => {
                self.execute_immediate(&event).await?
            }
            InvalidationStrategy::Versioned => {
                self.execute_immediate(&event).await?
            }
            InvalidationStrategy::Tagged => {
                self.execute_immediate(&event).await?
            }
            InvalidationStrategy::Purge => {
                self.execute_purge().await?
            }
        };

        // Broadcast event to subscribers
        let _ = self.broadcast_tx.send(event.clone());
        self.stats.events_propagated.fetch_add(1, Ordering::Relaxed);

        self.stats.events_processed.fetch_add(1, Ordering::Relaxed);
        self.stats.keys_invalidated.fetch_add(keys_invalidated as u64, Ordering::Relaxed);

        Ok(InvalidationResult {
            keys_invalidated,
            nodes_notified: 1, // Local node
            strategy: event.strategy,
        })
    }

    /// Executes immediate invalidation
    async fn execute_immediate(&self, event: &InvalidationEvent) -> Result<usize> {
        let handlers = self.handlers.read().await;
        let mut total_invalidated = 0;

        for handler in handlers.iter() {
            match handler.invalidate_pattern(&event.pattern) {
                Ok(count) => total_invalidated += count,
                Err(e) => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("Invalidation handler failed: {}", e);
                }
            }
        }

        Ok(total_invalidated)
    }

    /// Queues lazy invalidation
    async fn queue_lazy(&self, event: &InvalidationEvent) -> Result<usize> {
        let mut pending = self.pending.write().await;

        if pending.len() >= self.config.max_pending {
            return Err(StrataError::InvalidOperation(
                "Pending invalidation queue full".to_string(),
            ));
        }

        pending.push(event.clone());
        Ok(0) // Not immediately invalidated
    }

    /// Executes purge (invalidates everything)
    async fn execute_purge(&self) -> Result<usize> {
        let handlers = self.handlers.read().await;
        let mut total_invalidated = 0;

        for handler in handlers.iter() {
            match handler.invalidate_pattern(&InvalidationPattern::All) {
                Ok(count) => total_invalidated += count,
                Err(e) => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("Purge handler failed: {}", e);
                }
            }
        }

        // Clear indexes
        {
            let mut tag_index = self.tag_index.write().await;
            tag_index.clear();
        }
        {
            let mut version_index = self.version_index.write().await;
            version_index.clear();
        }

        Ok(total_invalidated)
    }

    /// Processes pending lazy invalidations
    pub async fn process_pending(&self) -> Result<usize> {
        let mut pending = self.pending.write().await;
        let events: Vec<_> = pending.drain(..).collect();
        drop(pending);

        let mut total = 0;
        for event in events {
            match self.execute_immediate(&event).await {
                Ok(count) => total += count,
                Err(e) => {
                    tracing::warn!("Failed to process pending invalidation: {}", e);
                    if self.config.retry_failed {
                        let mut pending = self.pending.write().await;
                        if pending.len() < self.config.max_pending {
                            pending.push(event);
                        }
                    }
                }
            }
        }

        Ok(total)
    }

    /// Adds a tag to a key
    pub async fn tag_key(&self, key: &str, tag: &str) {
        let mut tag_index = self.tag_index.write().await;
        tag_index
            .entry(tag.to_string())
            .or_insert_with(HashSet::new)
            .insert(key.to_string());
    }

    /// Removes a tag from a key
    pub async fn untag_key(&self, key: &str, tag: &str) {
        let mut tag_index = self.tag_index.write().await;
        if let Some(keys) = tag_index.get_mut(tag) {
            keys.remove(key);
            if keys.is_empty() {
                tag_index.remove(tag);
            }
        }
    }

    /// Gets keys with a specific tag
    pub async fn get_tagged_keys(&self, tag: &str) -> Vec<String> {
        let tag_index = self.tag_index.read().await;
        tag_index
            .get(tag)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Sets version for a key
    pub async fn set_version(&self, key: &str, version: u64) {
        let mut version_index = self.version_index.write().await;
        version_index.insert(key.to_string(), version);
    }

    /// Gets version for a key
    pub async fn get_version(&self, key: &str) -> Option<u64> {
        let version_index = self.version_index.read().await;
        version_index.get(key).copied()
    }

    /// Receives an invalidation event from another node
    pub async fn receive_event(&self, event: InvalidationEvent) -> Result<()> {
        // Check if already processed
        {
            let processed = self.processed.read().await;
            if processed.contains(&event.id) {
                self.stats.duplicates_filtered.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }

        // Process the event
        self.process_event(event).await?;
        Ok(())
    }

    /// Gets statistics
    pub fn stats(&self) -> InvalidationStatsSnapshot {
        InvalidationStatsSnapshot {
            events_processed: self.stats.events_processed.load(Ordering::Relaxed),
            keys_invalidated: self.stats.keys_invalidated.load(Ordering::Relaxed),
            events_propagated: self.stats.events_propagated.load(Ordering::Relaxed),
            failed: self.stats.failed.load(Ordering::Relaxed),
            duplicates_filtered: self.stats.duplicates_filtered.load(Ordering::Relaxed),
        }
    }

    /// Gets pending count
    pub async fn pending_count(&self) -> usize {
        self.pending.read().await.len()
    }
}

/// Invalidation result
#[derive(Debug, Clone)]
pub struct InvalidationResult {
    /// Number of keys invalidated
    pub keys_invalidated: usize,
    /// Number of nodes notified
    pub nodes_notified: usize,
    /// Strategy used
    pub strategy: InvalidationStrategy,
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationStatsSnapshot {
    pub events_processed: u64,
    pub keys_invalidated: u64,
    pub events_propagated: u64,
    pub failed: u64,
    pub duplicates_filtered: u64,
}

/// Batch invalidation builder
pub struct BatchInvalidation {
    patterns: Vec<InvalidationPattern>,
    strategy: InvalidationStrategy,
    tags: Vec<String>,
    priority: u8,
}

impl BatchInvalidation {
    /// Creates a new batch invalidation
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            strategy: InvalidationStrategy::Immediate,
            tags: Vec::new(),
            priority: 5,
        }
    }

    /// Adds an exact key
    pub fn key(mut self, key: &str) -> Self {
        self.patterns.push(InvalidationPattern::Exact(key.to_string()));
        self
    }

    /// Adds a prefix pattern
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.patterns.push(InvalidationPattern::Prefix(prefix.to_string()));
        self
    }

    /// Adds a glob pattern
    pub fn glob(mut self, pattern: &str) -> Self {
        self.patterns.push(InvalidationPattern::Glob(pattern.to_string()));
        self
    }

    /// Sets the strategy
    pub fn strategy(mut self, strategy: InvalidationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Adds tags
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Sets priority
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Executes the batch invalidation
    pub async fn execute(self, manager: &InvalidationManager) -> Result<InvalidationResult> {
        let pattern = if self.patterns.len() == 1 {
            self.patterns.into_iter().next().unwrap()
        } else {
            InvalidationPattern::Multi(self.patterns)
        };

        let mut event = InvalidationEvent::new(
            self.strategy,
            pattern,
            manager.local_node_id.clone(),
        );
        event.tags = self.tags;
        event.priority = self.priority;

        manager.process_event(event).await
    }
}

impl Default for BatchInvalidation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(InvalidationPattern::Exact("key1".to_string()).matches("key1"));
        assert!(!InvalidationPattern::Exact("key1".to_string()).matches("key2"));

        assert!(InvalidationPattern::Prefix("user:".to_string()).matches("user:123"));
        assert!(!InvalidationPattern::Prefix("user:".to_string()).matches("group:123"));

        assert!(InvalidationPattern::Suffix(".json".to_string()).matches("data.json"));
        assert!(!InvalidationPattern::Suffix(".json".to_string()).matches("data.xml"));

        assert!(InvalidationPattern::Glob("user:*".to_string()).matches("user:123"));
        assert!(InvalidationPattern::Glob("user:*:profile".to_string()).matches("user:123:profile"));
        assert!(InvalidationPattern::Glob("*.json".to_string()).matches("data.json"));

        assert!(InvalidationPattern::All.matches("anything"));
    }

    #[tokio::test]
    async fn test_invalidation_manager() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new("node-1".to_string(), config);

        // Invalidate a key
        let result = manager.invalidate_key("test-key").await.unwrap();
        assert_eq!(result.strategy, InvalidationStrategy::Immediate);

        // Invalidate by prefix
        let result = manager.invalidate_prefix("cache:").await.unwrap();
        assert_eq!(result.strategy, InvalidationStrategy::Immediate);

        // Check stats
        let stats = manager.stats();
        assert_eq!(stats.events_processed, 2);
    }

    #[tokio::test]
    async fn test_tag_invalidation() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new("node-1".to_string(), config);

        // Tag some keys
        manager.tag_key("key1", "group-a").await;
        manager.tag_key("key2", "group-a").await;
        manager.tag_key("key3", "group-b").await;

        // Get tagged keys
        let keys = manager.get_tagged_keys("group-a").await;
        assert_eq!(keys.len(), 2);

        // Invalidate by tag
        let result = manager.invalidate_by_tag("group-a").await.unwrap();
        assert_eq!(result.strategy, InvalidationStrategy::Tagged);
    }

    #[tokio::test]
    async fn test_version_invalidation() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new("node-1".to_string(), config);

        // Set initial version
        manager.set_version("key1", 1).await;

        // Invalidate with newer version
        let result = manager.invalidate_by_version("key1", 2).await.unwrap();
        assert_eq!(result.strategy, InvalidationStrategy::Versioned);

        // Verify version updated
        assert_eq!(manager.get_version("key1").await, Some(2));
    }

    #[test]
    fn test_batch_invalidation() {
        let batch = BatchInvalidation::new()
            .key("key1")
            .prefix("cache:")
            .glob("user:*")
            .strategy(InvalidationStrategy::Immediate)
            .priority(10);

        assert_eq!(batch.patterns.len(), 3);
        assert_eq!(batch.priority, 10);
    }
}
