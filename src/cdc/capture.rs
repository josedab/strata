// CDC Change Capture

use super::event::{ChangeEvent, ChangeEventType, EventPayload};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

/// Change capture mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    /// Capture all changes synchronously (higher latency, guaranteed delivery)
    Synchronous,
    /// Capture changes asynchronously (lower latency, best effort)
    Asynchronous,
    /// Only capture changes periodically from changelog
    Polling,
    /// Use database triggers (for metadata changes)
    Trigger,
}

impl Default for CaptureMode {
    fn default() -> Self {
        CaptureMode::Asynchronous
    }
}

/// Change capture configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureConfig {
    /// Capture mode
    pub mode: CaptureMode,
    /// Buffer size for async capture
    pub buffer_size: usize,
    /// Enable batching
    pub enable_batching: bool,
    /// Batch size
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Include data in events (for small objects)
    pub include_data: bool,
    /// Maximum data size to include
    pub max_inline_data_size: usize,
    /// Capture schema changes
    pub capture_schema_changes: bool,
    /// Capture ACL changes
    pub capture_acl_changes: bool,
    /// Capture lifecycle events
    pub capture_lifecycle_events: bool,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            mode: CaptureMode::Asynchronous,
            buffer_size: 10000,
            enable_batching: true,
            batch_size: 100,
            batch_timeout_ms: 100,
            include_data: false,
            max_inline_data_size: 65536, // 64KB
            capture_schema_changes: true,
            capture_acl_changes: true,
            capture_lifecycle_events: true,
        }
    }
}

/// Change capture engine
pub struct ChangeCapture {
    /// Configuration
    config: CaptureConfig,
    /// Sequence counter
    sequence: AtomicU64,
    /// Event sender
    event_tx: mpsc::Sender<ChangeEvent>,
    /// Event receiver (for internal processing)
    event_rx: Arc<RwLock<Option<mpsc::Receiver<ChangeEvent>>>>,
    /// Broadcast channel for subscribers
    broadcast_tx: broadcast::Sender<ChangeEvent>,
    /// Capture filters
    filters: Arc<RwLock<Vec<CaptureFilter>>>,
    /// Statistics
    stats: Arc<CaptureStats>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

/// Capture filter for selective capture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureFilter {
    /// Filter ID
    pub id: String,
    /// Filter name
    pub name: String,
    /// Event types to capture
    pub event_types: Option<Vec<ChangeEventType>>,
    /// Bucket pattern (glob)
    pub bucket_pattern: Option<String>,
    /// Key prefix
    pub key_prefix: Option<String>,
    /// Key pattern (regex)
    pub key_pattern: Option<String>,
    /// Minimum size
    pub min_size: Option<u64>,
    /// Maximum size
    pub max_size: Option<u64>,
    /// Include or exclude
    pub include: bool,
}

impl CaptureFilter {
    /// Creates an include filter
    pub fn include() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: String::new(),
            event_types: None,
            bucket_pattern: None,
            key_prefix: None,
            key_pattern: None,
            min_size: None,
            max_size: None,
            include: true,
        }
    }

    /// Creates an exclude filter
    pub fn exclude() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: String::new(),
            event_types: None,
            bucket_pattern: None,
            key_prefix: None,
            key_pattern: None,
            min_size: None,
            max_size: None,
            include: false,
        }
    }

    /// Sets filter name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Sets event types
    pub fn with_event_types(mut self, types: Vec<ChangeEventType>) -> Self {
        self.event_types = Some(types);
        self
    }

    /// Sets bucket pattern
    pub fn with_bucket(mut self, pattern: impl Into<String>) -> Self {
        self.bucket_pattern = Some(pattern.into());
        self
    }

    /// Sets key prefix
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// Checks if an event matches this filter
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        // Check event type
        if let Some(ref types) = self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }

        // Check bucket pattern
        if let Some(ref pattern) = self.bucket_pattern {
            if !glob_match(pattern, &event.bucket) {
                return false;
            }
        }

        // Check key prefix
        if let Some(ref prefix) = self.key_prefix {
            if !event.key.starts_with(prefix) {
                return false;
            }
        }

        // Check key pattern
        if let Some(ref pattern) = self.key_pattern {
            if let Ok(re) = regex::Regex::new(pattern) {
                if !re.is_match(&event.key) {
                    return false;
                }
            }
        }

        // Check size
        if let Some(size) = event.payload.size {
            if let Some(min) = self.min_size {
                if size < min {
                    return false;
                }
            }
            if let Some(max) = self.max_size {
                if size > max {
                    return false;
                }
            }
        }

        true
    }
}

/// Simple glob matching
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.ends_with('*') {
        let prefix = &pattern[..pattern.len() - 1];
        return text.starts_with(prefix);
    }
    if pattern.starts_with('*') {
        let suffix = &pattern[1..];
        return text.ends_with(suffix);
    }
    pattern == text
}

/// Capture statistics
pub struct CaptureStats {
    /// Events captured
    pub events_captured: AtomicU64,
    /// Events filtered
    pub events_filtered: AtomicU64,
    /// Events sent
    pub events_sent: AtomicU64,
    /// Events dropped (buffer full)
    pub events_dropped: AtomicU64,
    /// Bytes captured
    pub bytes_captured: AtomicU64,
    /// Current buffer size
    pub buffer_size: AtomicU64,
}

impl Default for CaptureStats {
    fn default() -> Self {
        Self {
            events_captured: AtomicU64::new(0),
            events_filtered: AtomicU64::new(0),
            events_sent: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            bytes_captured: AtomicU64::new(0),
            buffer_size: AtomicU64::new(0),
        }
    }
}

impl ChangeCapture {
    /// Creates a new change capture instance
    pub fn new(config: CaptureConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(config.buffer_size);
        let (broadcast_tx, _) = broadcast::channel(config.buffer_size);

        Self {
            config,
            sequence: AtomicU64::new(0),
            event_tx,
            event_rx: Arc::new(RwLock::new(Some(event_rx))),
            broadcast_tx,
            filters: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(CaptureStats::default()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts the capture engine
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Err(StrataError::InvalidOperation(
                "Capture engine already running".to_string(),
            ));
        }
        *running = true;
        Ok(())
    }

    /// Stops the capture engine
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    /// Captures a change event
    pub async fn capture(&self, event_type: ChangeEventType, bucket: &str, key: &str) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let event = ChangeEvent::new(event_type, bucket, key, sequence);
        self.capture_event(event).await
    }

    /// Captures a full change event
    pub async fn capture_event(&self, event: ChangeEvent) -> Result<()> {
        self.stats.events_captured.fetch_add(1, Ordering::Relaxed);

        // Apply filters
        let filters = self.filters.read().await;
        let mut included = filters.is_empty(); // Include by default if no filters

        for filter in filters.iter() {
            if filter.matches(&event) {
                included = filter.include;
            }
        }

        if !included {
            self.stats.events_filtered.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Update stats
        if let Some(size) = event.payload.size {
            self.stats.bytes_captured.fetch_add(size, Ordering::Relaxed);
        }

        // Send to channel
        match self.event_tx.try_send(event.clone()) {
            Ok(_) => {
                self.stats.events_sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.stats.events_dropped.fetch_add(1, Ordering::Relaxed);
                if self.config.mode == CaptureMode::Synchronous {
                    return Err(StrataError::BufferFull("Capture buffer full".to_string()));
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(StrataError::Internal("Capture channel closed".to_string()));
            }
        }

        // Broadcast to subscribers
        let _ = self.broadcast_tx.send(event);

        Ok(())
    }

    /// Captures a create event
    pub async fn capture_create(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        content_type: Option<&str>,
        etag: Option<&str>,
    ) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let payload = EventPayload {
            size: Some(size),
            content_type: content_type.map(|s| s.to_string()),
            etag: etag.map(|s| s.to_string()),
            ..Default::default()
        };
        let event = ChangeEvent::new(ChangeEventType::Create, bucket, key, sequence)
            .with_payload(payload);
        self.capture_event(event).await
    }

    /// Captures an update event
    pub async fn capture_update(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        old_version: Option<&str>,
        new_version: &str,
    ) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let payload = EventPayload {
            size: Some(size),
            ..Default::default()
        };
        let mut event = ChangeEvent::new(ChangeEventType::Update, bucket, key, sequence)
            .with_payload(payload)
            .with_version(new_version);
        event.previous_version_id = old_version.map(|s| s.to_string());
        self.capture_event(event).await
    }

    /// Captures a delete event
    pub async fn capture_delete(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let mut event = ChangeEvent::new(ChangeEventType::Delete, bucket, key, sequence);
        if let Some(v) = version_id {
            event = event.with_version(v);
        }
        self.capture_event(event).await
    }

    /// Captures a rename event
    pub async fn capture_rename(
        &self,
        bucket: &str,
        old_key: &str,
        new_key: &str,
    ) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let payload = EventPayload {
            old_key: Some(old_key.to_string()),
            new_key: Some(new_key.to_string()),
            ..Default::default()
        };
        let event = ChangeEvent::new(ChangeEventType::Rename, bucket, new_key, sequence)
            .with_payload(payload);
        self.capture_event(event).await
    }

    /// Subscribes to change events
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Adds a capture filter
    pub async fn add_filter(&self, filter: CaptureFilter) {
        let mut filters = self.filters.write().await;
        filters.push(filter);
    }

    /// Removes a filter by ID
    pub async fn remove_filter(&self, filter_id: &str) -> bool {
        let mut filters = self.filters.write().await;
        let len_before = filters.len();
        filters.retain(|f| f.id != filter_id);
        filters.len() < len_before
    }

    /// Gets capture statistics
    pub fn stats(&self) -> CaptureStatsSnapshot {
        CaptureStatsSnapshot {
            events_captured: self.stats.events_captured.load(Ordering::Relaxed),
            events_filtered: self.stats.events_filtered.load(Ordering::Relaxed),
            events_sent: self.stats.events_sent.load(Ordering::Relaxed),
            events_dropped: self.stats.events_dropped.load(Ordering::Relaxed),
            bytes_captured: self.stats.bytes_captured.load(Ordering::Relaxed),
            current_sequence: self.sequence.load(Ordering::Relaxed),
        }
    }

    /// Gets the configuration
    pub fn config(&self) -> &CaptureConfig {
        &self.config
    }

    /// Takes the event receiver for processing
    pub async fn take_receiver(&self) -> Option<mpsc::Receiver<ChangeEvent>> {
        self.event_rx.write().await.take()
    }
}

/// Snapshot of capture statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureStatsSnapshot {
    pub events_captured: u64,
    pub events_filtered: u64,
    pub events_sent: u64,
    pub events_dropped: u64,
    pub bytes_captured: u64,
    pub current_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_change_capture() {
        let config = CaptureConfig::default();
        let capture = ChangeCapture::new(config);
        capture.start().await.unwrap();

        // Subscribe
        let mut rx = capture.subscribe();

        // Capture events
        capture.capture_create("bucket", "key1", 1000, Some("text/plain"), None).await.unwrap();
        capture.capture_update("bucket", "key1", 2000, Some("v1"), "v2").await.unwrap();
        capture.capture_delete("bucket", "key1", Some("v2")).await.unwrap();

        // Verify stats
        let stats = capture.stats();
        assert_eq!(stats.events_captured, 3);
        assert_eq!(stats.events_sent, 3);
    }

    #[tokio::test]
    async fn test_capture_filter() {
        let config = CaptureConfig::default();
        let capture = ChangeCapture::new(config);

        // Add filter to only capture deletes
        let filter = CaptureFilter::include()
            .with_name("deletes-only")
            .with_event_types(vec![ChangeEventType::Delete]);
        capture.add_filter(filter).await;

        capture.start().await.unwrap();

        // Capture events
        capture.capture(ChangeEventType::Create, "bucket", "key1").await.unwrap();
        capture.capture(ChangeEventType::Delete, "bucket", "key2").await.unwrap();

        let stats = capture.stats();
        assert_eq!(stats.events_captured, 2);
        assert_eq!(stats.events_filtered, 1); // Create was filtered
        assert_eq!(stats.events_sent, 1); // Only delete was sent
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("bucket-*", "bucket-test"));
        assert!(glob_match("*-logs", "app-logs"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("bucket-*", "other"));
    }
}
