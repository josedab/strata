// CDC Event Streaming

use super::connector::{Connector, ConnectorConfig, ConnectorFactory};
use super::event::ChangeEvent;
use super::subscription::{Subscription, SubscriptionManager};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Stream name
    pub name: String,
    /// Enable the stream
    pub enabled: bool,
    /// Connector configurations
    pub connectors: Vec<ConnectorConfig>,
    /// Buffer size
    pub buffer_size: usize,
    /// Worker count
    pub worker_count: usize,
    /// Batch settings
    pub batch: BatchSettings,
    /// Delivery guarantee
    pub delivery: DeliveryGuarantee,
    /// Ordering guarantee
    pub ordering: OrderingGuarantee,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            connectors: Vec::new(),
            buffer_size: 10000,
            worker_count: 4,
            batch: BatchSettings::default(),
            delivery: DeliveryGuarantee::AtLeastOnce,
            ordering: OrderingGuarantee::PerKey,
        }
    }
}

/// Batch settings for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSettings {
    /// Enable batching
    pub enabled: bool,
    /// Maximum events per batch
    pub max_events: usize,
    /// Maximum bytes per batch
    pub max_bytes: usize,
    /// Maximum wait time in milliseconds
    pub max_wait_ms: u64,
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_events: 100,
            max_bytes: 1048576, // 1MB
            max_wait_ms: 100,
        }
    }
}

/// Delivery guarantee
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// Best effort (may lose events)
    BestEffort,
    /// At least once (may duplicate)
    AtLeastOnce,
    /// Exactly once (no loss, no duplicates)
    ExactlyOnce,
}

/// Ordering guarantee
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingGuarantee {
    /// No ordering guarantee
    None,
    /// Per-key ordering (events for same key are ordered)
    PerKey,
    /// Per-bucket ordering
    PerBucket,
    /// Global ordering (total order)
    Global,
}

/// Event stream manager
pub struct EventStream {
    /// Configuration
    config: StreamConfig,
    /// Active connectors
    connectors: Arc<RwLock<Vec<Box<dyn Connector>>>>,
    /// Subscription manager
    subscriptions: Arc<SubscriptionManager>,
    /// Event buffer
    buffer: Arc<RwLock<Vec<ChangeEvent>>>,
    /// Running state
    running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<StreamStats>,
    /// Checkpoint for exactly-once
    checkpoint: Arc<RwLock<StreamCheckpoint>>,
}

/// Stream statistics
pub struct StreamStats {
    /// Events received
    pub events_received: AtomicU64,
    /// Events sent
    pub events_sent: AtomicU64,
    /// Events failed
    pub events_failed: AtomicU64,
    /// Batches sent
    pub batches_sent: AtomicU64,
    /// Current lag
    pub current_lag: AtomicU64,
}

impl Default for StreamStats {
    fn default() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_sent: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            current_lag: AtomicU64::new(0),
        }
    }
}

/// Stream checkpoint for exactly-once delivery
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamCheckpoint {
    /// Last processed sequence per connector
    pub connector_sequences: HashMap<String, u64>,
    /// Pending events (for retry)
    pub pending_events: Vec<u64>,
    /// Last checkpoint time
    pub last_checkpoint: u64,
}

impl EventStream {
    /// Creates a new event stream
    pub fn new(config: StreamConfig) -> Result<Self> {
        let mut connectors: Vec<Box<dyn Connector>> = Vec::new();

        for conn_config in &config.connectors {
            let connector = ConnectorFactory::create(conn_config.clone())?;
            connectors.push(connector);
        }

        Ok(Self {
            config,
            connectors: Arc::new(RwLock::new(connectors)),
            subscriptions: Arc::new(SubscriptionManager::new()),
            buffer: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(StreamStats::default()),
            checkpoint: Arc::new(RwLock::new(StreamCheckpoint::default())),
        })
    }

    /// Starts the event stream
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StrataError::InvalidOperation(
                "Stream already running".to_string(),
            ));
        }

        // Connect all connectors
        let mut connectors = self.connectors.write().await;
        for connector in connectors.iter_mut() {
            connector.connect().await?;
        }

        Ok(())
    }

    /// Stops the event stream
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);

        // Disconnect all connectors
        let mut connectors = self.connectors.write().await;
        for connector in connectors.iter_mut() {
            connector.disconnect().await?;
        }

        Ok(())
    }

    /// Publishes an event to the stream
    pub async fn publish(&self, event: ChangeEvent) -> Result<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(StrataError::InvalidOperation(
                "Stream not running".to_string(),
            ));
        }

        self.stats.events_received.fetch_add(1, Ordering::Relaxed);

        // Check subscriptions
        let subscriptions = self.subscriptions.list().await;
        let matching: Vec<_> = subscriptions
            .iter()
            .filter(|s| s.matches(&event))
            .collect();

        if matching.is_empty() && !subscriptions.is_empty() {
            // No matching subscriptions, skip
            return Ok(());
        }

        // Buffer event if batching enabled
        if self.config.batch.enabled {
            let mut buffer = self.buffer.write().await;
            buffer.push(event.clone());

            if buffer.len() >= self.config.batch.max_events {
                let events: Vec<_> = buffer.drain(..).collect();
                drop(buffer);
                self.flush_batch(events).await?;
            }
        } else {
            self.send_event(&event).await?;
        }

        Ok(())
    }

    /// Sends a single event to all connectors
    async fn send_event(&self, event: &ChangeEvent) -> Result<()> {
        let connectors = self.connectors.read().await;
        let mut last_error = None;

        for connector in connectors.iter() {
            match connector.send(event).await {
                Ok(_) => {
                    self.stats.events_sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.stats.events_failed.fetch_add(1, Ordering::Relaxed);
                    last_error = Some(e);
                }
            }
        }

        if let Some(e) = last_error {
            if self.config.delivery == DeliveryGuarantee::ExactlyOnce
                || self.config.delivery == DeliveryGuarantee::AtLeastOnce
            {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Flushes a batch of events
    async fn flush_batch(&self, events: Vec<ChangeEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let connectors = self.connectors.read().await;

        for connector in connectors.iter() {
            match connector.send_batch(&events).await {
                Ok(_) => {
                    self.stats.events_sent.fetch_add(events.len() as u64, Ordering::Relaxed);
                    self.stats.batches_sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.stats.events_failed.fetch_add(events.len() as u64, Ordering::Relaxed);
                    if self.config.delivery != DeliveryGuarantee::BestEffort {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Forces a flush of buffered events
    pub async fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.write().await;
        let events: Vec<_> = buffer.drain(..).collect();
        drop(buffer);

        if !events.is_empty() {
            self.flush_batch(events).await?;
        }

        Ok(())
    }

    /// Adds a subscription
    pub async fn subscribe(&self, subscription: Subscription) -> Result<String> {
        self.subscriptions.add(subscription).await
    }

    /// Removes a subscription
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<()> {
        self.subscriptions.remove(subscription_id).await
    }

    /// Gets stream statistics
    pub fn stats(&self) -> StreamStatsSnapshot {
        StreamStatsSnapshot {
            events_received: self.stats.events_received.load(Ordering::Relaxed),
            events_sent: self.stats.events_sent.load(Ordering::Relaxed),
            events_failed: self.stats.events_failed.load(Ordering::Relaxed),
            batches_sent: self.stats.batches_sent.load(Ordering::Relaxed),
            current_lag: self.stats.current_lag.load(Ordering::Relaxed),
        }
    }

    /// Gets the configuration
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// Checks if stream is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Creates a background worker that processes events from a capture source
    pub async fn run_from_capture(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<ChangeEvent>,
    ) {
        while let Some(event) = rx.recv().await {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Err(e) = self.publish(event).await {
                tracing::error!("Failed to publish event: {}", e);
            }
        }
    }
}

/// Snapshot of stream statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatsSnapshot {
    pub events_received: u64,
    pub events_sent: u64,
    pub events_failed: u64,
    pub batches_sent: u64,
    pub current_lag: u64,
}

/// Stream builder for fluent API
pub struct StreamBuilder {
    config: StreamConfig,
}

impl StreamBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: StreamConfig {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    pub fn connector(mut self, config: ConnectorConfig) -> Self {
        self.config.connectors.push(config);
        self
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    pub fn workers(mut self, count: usize) -> Self {
        self.config.worker_count = count;
        self
    }

    pub fn delivery(mut self, guarantee: DeliveryGuarantee) -> Self {
        self.config.delivery = guarantee;
        self
    }

    pub fn ordering(mut self, guarantee: OrderingGuarantee) -> Self {
        self.config.ordering = guarantee;
        self
    }

    pub fn batch(mut self, settings: BatchSettings) -> Self {
        self.config.batch = settings;
        self
    }

    pub fn build(self) -> Result<EventStream> {
        EventStream::new(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::connector::ConnectorType;
    use crate::cdc::event::ChangeEventType;

    #[tokio::test]
    async fn test_event_stream() {
        let config = StreamConfig {
            name: "test-stream".to_string(),
            batch: BatchSettings {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let stream = EventStream::new(config).unwrap();
        stream.start().await.unwrap();

        let event = ChangeEvent::new(ChangeEventType::Create, "bucket", "key", 1);
        stream.publish(event).await.unwrap();

        let stats = stream.stats();
        assert_eq!(stats.events_received, 1);

        stream.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_batching() {
        let config = StreamConfig {
            name: "batch-stream".to_string(),
            batch: BatchSettings {
                enabled: true,
                max_events: 5,
                max_wait_ms: 1000,
                ..Default::default()
            },
            ..Default::default()
        };

        let stream = EventStream::new(config).unwrap();
        stream.start().await.unwrap();

        // Add 4 events (below threshold)
        for i in 0..4 {
            let event = ChangeEvent::new(ChangeEventType::Create, "bucket", format!("key{}", i), i as u64);
            stream.publish(event).await.unwrap();
        }

        // Manual flush
        stream.flush().await.unwrap();

        stream.stop().await.unwrap();
    }

    #[test]
    fn test_stream_builder() {
        let stream = StreamBuilder::new("builder-test")
            .buffer_size(5000)
            .workers(2)
            .delivery(DeliveryGuarantee::AtLeastOnce)
            .build()
            .unwrap();

        assert_eq!(stream.config().name, "builder-test");
        assert_eq!(stream.config().buffer_size, 5000);
    }
}
