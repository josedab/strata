//! Event notification system for Strata.
//!
//! Provides pub/sub and webhook notifications for file system events.
//! Enables integration with external systems and workflow automation.

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, InodeId, NodeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, info};

/// Event type categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    // File events
    FileCreated,
    FileModified,
    FileDeleted,
    FileRenamed,
    FileTruncated,
    FileRead,

    // Directory events
    DirectoryCreated,
    DirectoryDeleted,
    DirectoryRenamed,

    // Attribute events
    AttributeChanged,
    OwnerChanged,
    PermissionChanged,

    // Chunk events
    ChunkCreated,
    ChunkDeleted,
    ChunkReplicated,
    ChunkCorrupted,
    ChunkRepaired,

    // Snapshot events
    SnapshotCreated,
    SnapshotDeleted,
    SnapshotRestored,

    // Cluster events
    NodeJoined,
    NodeLeft,
    NodeFailed,
    LeaderChanged,
    RebalanceStarted,
    RebalanceCompleted,

    // System events
    QuotaWarning,
    QuotaExceeded,
    ErrorOccurred,
}

impl EventType {
    /// Get the category for this event type.
    pub fn category(&self) -> EventCategory {
        match self {
            EventType::FileCreated
            | EventType::FileModified
            | EventType::FileDeleted
            | EventType::FileRenamed
            | EventType::FileTruncated
            | EventType::FileRead => EventCategory::File,

            EventType::DirectoryCreated
            | EventType::DirectoryDeleted
            | EventType::DirectoryRenamed => EventCategory::Directory,

            EventType::AttributeChanged
            | EventType::OwnerChanged
            | EventType::PermissionChanged => EventCategory::Attribute,

            EventType::ChunkCreated
            | EventType::ChunkDeleted
            | EventType::ChunkReplicated
            | EventType::ChunkCorrupted
            | EventType::ChunkRepaired => EventCategory::Chunk,

            EventType::SnapshotCreated
            | EventType::SnapshotDeleted
            | EventType::SnapshotRestored => EventCategory::Snapshot,

            EventType::NodeJoined
            | EventType::NodeLeft
            | EventType::NodeFailed
            | EventType::LeaderChanged
            | EventType::RebalanceStarted
            | EventType::RebalanceCompleted => EventCategory::Cluster,

            EventType::QuotaWarning | EventType::QuotaExceeded | EventType::ErrorOccurred => {
                EventCategory::System
            }
        }
    }
}

/// Event category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventCategory {
    File,
    Directory,
    Attribute,
    Chunk,
    Snapshot,
    Cluster,
    System,
}

/// Event payload - details about what happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    /// File or directory event.
    FileSystem {
        path: String,
        inode: Option<InodeId>,
        old_path: Option<String>,
        size: Option<u64>,
        user_id: Option<String>,
    },
    /// Chunk event.
    Chunk {
        chunk_id: ChunkId,
        node_id: Option<NodeId>,
        shard_index: Option<u32>,
    },
    /// Snapshot event.
    Snapshot {
        snapshot_id: String,
        name: String,
        scope: String,
    },
    /// Cluster event.
    Cluster {
        node_id: NodeId,
        node_addr: Option<String>,
        new_leader: Option<NodeId>,
    },
    /// System event.
    System {
        message: String,
        details: HashMap<String, String>,
    },
}

/// A single event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event ID.
    pub id: String,
    /// Event type.
    pub event_type: EventType,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Event payload.
    pub payload: EventPayload,
    /// Source node.
    pub source_node: NodeId,
    /// Correlation ID for related events.
    pub correlation_id: Option<String>,
    /// Event metadata.
    pub metadata: HashMap<String, String>,
}

impl Event {
    /// Create a new event.
    pub fn new(event_type: EventType, payload: EventPayload, source_node: NodeId) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            event_type,
            timestamp: Utc::now(),
            payload,
            source_node,
            correlation_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Set correlation ID.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Add metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Create a file system event.
    pub fn file_system(
        event_type: EventType,
        path: impl Into<String>,
        inode: Option<InodeId>,
        source_node: NodeId,
    ) -> Self {
        Self::new(
            event_type,
            EventPayload::FileSystem {
                path: path.into(),
                inode,
                old_path: None,
                size: None,
                user_id: None,
            },
            source_node,
        )
    }

    /// Create a chunk event.
    pub fn chunk(
        event_type: EventType,
        chunk_id: ChunkId,
        node_id: Option<NodeId>,
        source_node: NodeId,
    ) -> Self {
        Self::new(
            event_type,
            EventPayload::Chunk {
                chunk_id,
                node_id,
                shard_index: None,
            },
            source_node,
        )
    }

    /// Create a cluster event.
    pub fn cluster(event_type: EventType, node_id: NodeId, source_node: NodeId) -> Self {
        Self::new(
            event_type,
            EventPayload::Cluster {
                node_id,
                node_addr: None,
                new_leader: None,
            },
            source_node,
        )
    }
}

/// Subscription filter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventFilter {
    /// Event types to include (empty = all).
    pub event_types: HashSet<EventType>,
    /// Categories to include (empty = all).
    pub categories: HashSet<EventCategory>,
    /// Path prefix filter.
    pub path_prefix: Option<String>,
    /// Specific inode filter.
    pub inode: Option<InodeId>,
    /// Specific node filter.
    pub node_id: Option<NodeId>,
}

impl EventFilter {
    /// Create a new filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter for specific event types.
    pub fn event_types(mut self, types: impl IntoIterator<Item = EventType>) -> Self {
        self.event_types = types.into_iter().collect();
        self
    }

    /// Filter for specific categories.
    pub fn categories(mut self, categories: impl IntoIterator<Item = EventCategory>) -> Self {
        self.categories = categories.into_iter().collect();
        self
    }

    /// Filter by path prefix.
    pub fn path_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.path_prefix = Some(prefix.into());
        self
    }

    /// Filter by inode.
    pub fn inode(mut self, inode: InodeId) -> Self {
        self.inode = Some(inode);
        self
    }

    /// Filter by node.
    pub fn node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Check if an event matches this filter.
    pub fn matches(&self, event: &Event) -> bool {
        // Check event type
        if !self.event_types.is_empty() && !self.event_types.contains(&event.event_type) {
            return false;
        }

        // Check category
        if !self.categories.is_empty() && !self.categories.contains(&event.event_type.category()) {
            return false;
        }

        // Check path prefix
        if let Some(ref prefix) = self.path_prefix {
            if let EventPayload::FileSystem { ref path, .. } = event.payload {
                if !path.starts_with(prefix) {
                    return false;
                }
            }
        }

        // Check inode
        if let Some(filter_inode) = self.inode {
            if let EventPayload::FileSystem { inode, .. } = &event.payload {
                if *inode != Some(filter_inode) {
                    return false;
                }
            }
        }

        // Check node
        if let Some(filter_node) = self.node_id {
            match &event.payload {
                EventPayload::Cluster { node_id, .. } if *node_id != filter_node => {
                    return false;
                }
                EventPayload::Chunk {
                    node_id: Some(n), ..
                } if *n != filter_node => {
                    return false;
                }
                _ => {}
            }
        }

        true
    }
}

/// Webhook configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook ID.
    pub id: String,
    /// Webhook name.
    pub name: String,
    /// Target URL.
    pub url: String,
    /// HTTP method (POST, PUT).
    pub method: String,
    /// Custom headers.
    pub headers: HashMap<String, String>,
    /// Event filter.
    pub filter: EventFilter,
    /// Whether the webhook is enabled.
    pub enabled: bool,
    /// Secret for signing payloads.
    pub secret: Option<String>,
    /// Retry configuration.
    pub retry_count: usize,
    /// Timeout in milliseconds.
    pub timeout_ms: u64,
    /// Batch events (send multiple events per request).
    pub batch: bool,
    /// Batch size.
    pub batch_size: usize,
    /// Batch timeout in milliseconds.
    pub batch_timeout_ms: u64,
}

impl WebhookConfig {
    /// Create a new webhook configuration.
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            url: url.into(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            filter: EventFilter::new(),
            enabled: true,
            secret: None,
            retry_count: 3,
            timeout_ms: 30000,
            batch: false,
            batch_size: 100,
            batch_timeout_ms: 1000,
        }
    }

    /// Set the event filter.
    pub fn with_filter(mut self, filter: EventFilter) -> Self {
        self.filter = filter;
        self
    }

    /// Set a secret for payload signing.
    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// Add a header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Enable batching.
    pub fn with_batching(mut self, size: usize, timeout_ms: u64) -> Self {
        self.batch = true;
        self.batch_size = size;
        self.batch_timeout_ms = timeout_ms;
        self
    }

    /// Disable the webhook.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

/// Webhook delivery result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDelivery {
    /// Delivery ID.
    pub id: String,
    /// Webhook ID.
    pub webhook_id: String,
    /// Event IDs that were delivered.
    pub event_ids: Vec<String>,
    /// Whether delivery was successful.
    pub success: bool,
    /// HTTP status code.
    pub status_code: Option<u16>,
    /// Response body (truncated).
    pub response_body: Option<String>,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of attempts.
    pub attempts: usize,
    /// When the delivery was attempted.
    pub attempted_at: DateTime<Utc>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Subscription for in-process event streaming.
pub struct Subscription {
    /// Subscription ID.
    pub id: String,
    /// Event filter.
    pub filter: EventFilter,
    /// Event receiver.
    receiver: mpsc::Receiver<Event>,
}

impl Subscription {
    /// Receive the next event.
    pub async fn recv(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }

    /// Try to receive an event without blocking.
    pub fn try_recv(&mut self) -> Option<Event> {
        self.receiver.try_recv().ok()
    }
}

/// Event bus statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventBusStats {
    /// Total events published.
    pub events_published: u64,
    /// Total events delivered to subscriptions.
    pub events_delivered: u64,
    /// Total events dropped (buffer full).
    pub events_dropped: u64,
    /// Total webhook deliveries attempted.
    pub webhook_deliveries: u64,
    /// Successful webhook deliveries.
    pub webhook_successes: u64,
    /// Failed webhook deliveries.
    pub webhook_failures: u64,
    /// Current subscription count.
    pub subscription_count: usize,
    /// Current webhook count.
    pub webhook_count: usize,
}

/// Event bus configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// Maximum events to buffer.
    pub buffer_size: usize,
    /// Maximum subscriptions.
    pub max_subscriptions: usize,
    /// Maximum webhooks.
    pub max_webhooks: usize,
    /// Event history size.
    pub history_size: usize,
    /// Whether to log all events.
    pub log_events: bool,
    /// Connect timeout for webhook HTTP client.
    pub webhook_connect_timeout: Duration,
    /// Default request timeout for webhook HTTP client (overridden by per-webhook config).
    pub webhook_request_timeout: Duration,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10000,
            max_subscriptions: 100,
            max_webhooks: 50,
            history_size: 1000,
            log_events: false,
            webhook_connect_timeout: Duration::from_secs(5),
            webhook_request_timeout: Duration::from_secs(30),
        }
    }
}

/// Internal subscription state.
struct SubscriptionState {
    filter: EventFilter,
    sender: mpsc::Sender<Event>,
}

/// The event bus.
pub struct EventBus {
    config: EventBusConfig,
    node_id: NodeId,
    subscriptions: RwLock<HashMap<String, SubscriptionState>>,
    webhooks: RwLock<HashMap<String, WebhookConfig>>,
    history: RwLock<VecDeque<Event>>,
    broadcast: broadcast::Sender<Event>,
    stats: Arc<EventBusStatsInner>,
    http_client: reqwest::Client,
}

struct EventBusStatsInner {
    events_published: AtomicU64,
    events_delivered: AtomicU64,
    events_dropped: AtomicU64,
    webhook_deliveries: AtomicU64,
    webhook_successes: AtomicU64,
    webhook_failures: AtomicU64,
}

impl Default for EventBusStatsInner {
    fn default() -> Self {
        Self {
            events_published: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            webhook_deliveries: AtomicU64::new(0),
            webhook_successes: AtomicU64::new(0),
            webhook_failures: AtomicU64::new(0),
        }
    }
}

impl EventBus {
    /// Create a new event bus.
    pub fn new(config: EventBusConfig, node_id: NodeId) -> Arc<Self> {
        let (broadcast, _) = broadcast::channel(config.buffer_size);

        // Extract timeout values before moving config
        let connect_timeout = config.webhook_connect_timeout;
        let request_timeout = config.webhook_request_timeout;

        let http_client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Arc::new(Self {
            config,
            node_id,
            subscriptions: RwLock::new(HashMap::new()),
            webhooks: RwLock::new(HashMap::new()),
            history: RwLock::new(VecDeque::new()),
            broadcast,
            stats: Arc::new(EventBusStatsInner::default()),
            http_client,
        })
    }

    /// Publish an event.
    pub async fn publish(&self, event: Event) {
        self.stats.events_published.fetch_add(1, Ordering::Relaxed);

        if self.config.log_events {
            debug!(event_type = ?event.event_type, event_id = %event.id, "Event published");
        }

        // Add to history
        {
            let mut history = self.history.write().await;
            if history.len() >= self.config.history_size {
                history.pop_front();
            }
            history.push_back(event.clone());
        }

        // Broadcast to all subscribers
        let _ = self.broadcast.send(event.clone());

        // Deliver to filtered subscriptions
        let subscriptions = self.subscriptions.read().await;
        for (_, state) in subscriptions.iter() {
            if state.filter.matches(&event) {
                if state.sender.try_send(event.clone()).is_ok() {
                    self.stats.events_delivered.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.stats.events_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Deliver to webhooks (async)
        let webhooks = self.webhooks.read().await;
        for webhook in webhooks.values() {
            if webhook.enabled && webhook.filter.matches(&event) {
                let webhook = webhook.clone();
                let event = event.clone();
                let client = self.http_client.clone(); // reqwest::Client is internally Arc-wrapped
                let stats = Arc::clone(&self.stats);

                tokio::spawn(async move {
                    stats.webhook_deliveries.fetch_add(1, Ordering::Relaxed);
                    let result =
                        Self::deliver_webhook_static(client, &webhook, vec![event]).await;
                    if result.is_some() {
                        stats.webhook_successes.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.webhook_failures.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        }
    }

    /// Publish a file system event.
    pub async fn publish_fs_event(
        &self,
        event_type: EventType,
        path: impl Into<String>,
        inode: Option<InodeId>,
    ) {
        let event = Event::file_system(event_type, path, inode, self.node_id);
        self.publish(event).await;
    }

    /// Subscribe to events with a filter.
    pub async fn subscribe(&self, filter: EventFilter) -> Result<Subscription> {
        let subscriptions = self.subscriptions.read().await;
        if subscriptions.len() >= self.config.max_subscriptions {
            return Err(StrataError::Internal("Maximum subscriptions reached".to_string()));
        }
        drop(subscriptions);

        let (sender, receiver) = mpsc::channel(1000);
        let id = uuid::Uuid::new_v4().to_string();

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(
            id.clone(),
            SubscriptionState {
                filter: filter.clone(),
                sender,
            },
        );

        Ok(Subscription {
            id,
            filter,
            receiver,
        })
    }

    /// Subscribe to the broadcast channel (receives all events).
    pub fn subscribe_broadcast(&self) -> broadcast::Receiver<Event> {
        self.broadcast.subscribe()
    }

    /// Unsubscribe.
    pub async fn unsubscribe(&self, subscription_id: &str) {
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.remove(subscription_id);
    }

    /// Register a webhook.
    pub async fn register_webhook(&self, config: WebhookConfig) -> Result<String> {
        let webhooks = self.webhooks.read().await;
        if webhooks.len() >= self.config.max_webhooks {
            return Err(StrataError::Internal("Maximum webhooks reached".to_string()));
        }
        drop(webhooks);

        let id = config.id.clone();
        let mut webhooks = self.webhooks.write().await;
        webhooks.insert(id.clone(), config);

        info!(webhook_id = %id, "Registered webhook");
        Ok(id)
    }

    /// Unregister a webhook.
    pub async fn unregister_webhook(&self, webhook_id: &str) -> Result<()> {
        let mut webhooks = self.webhooks.write().await;
        webhooks.remove(webhook_id);
        info!(webhook_id = %webhook_id, "Unregistered webhook");
        Ok(())
    }

    /// Update a webhook.
    pub async fn update_webhook(&self, config: WebhookConfig) -> Result<()> {
        let mut webhooks = self.webhooks.write().await;
        if !webhooks.contains_key(&config.id) {
            return Err(StrataError::NotFound(format!(
                "Webhook not found: {}",
                config.id
            )));
        }
        webhooks.insert(config.id.clone(), config);
        Ok(())
    }

    /// List webhooks.
    pub async fn list_webhooks(&self) -> Vec<WebhookConfig> {
        let webhooks = self.webhooks.read().await;
        webhooks.values().cloned().collect()
    }

    /// Get event history.
    pub async fn history(&self, limit: usize) -> Vec<Event> {
        let history = self.history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get events matching a filter from history.
    pub async fn query_history(&self, filter: &EventFilter, limit: usize) -> Vec<Event> {
        let history = self.history.read().await;
        history
            .iter()
            .rev()
            .filter(|e| filter.matches(e))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get statistics.
    pub async fn stats(&self) -> EventBusStats {
        let subscriptions = self.subscriptions.read().await;
        let webhooks = self.webhooks.read().await;

        EventBusStats {
            events_published: self.stats.events_published.load(Ordering::Relaxed),
            events_delivered: self.stats.events_delivered.load(Ordering::Relaxed),
            events_dropped: self.stats.events_dropped.load(Ordering::Relaxed),
            webhook_deliveries: self.stats.webhook_deliveries.load(Ordering::Relaxed),
            webhook_successes: self.stats.webhook_successes.load(Ordering::Relaxed),
            webhook_failures: self.stats.webhook_failures.load(Ordering::Relaxed),
            subscription_count: subscriptions.len(),
            webhook_count: webhooks.len(),
        }
    }

    async fn deliver_webhook_static(
        client: reqwest::Client,
        config: &WebhookConfig,
        events: Vec<Event>,
    ) -> Option<WebhookDelivery> {
        let start = std::time::Instant::now();
        let event_ids: Vec<_> = events.iter().map(|e| e.id.clone()).collect();

        let payload = if config.batch {
            serde_json::to_string(&events).ok()?
        } else {
            serde_json::to_string(&events.first()?).ok()?
        };

        let mut attempts = 0;
        let mut last_error = None;
        let mut status_code = None;
        let mut response_body = None;

        while attempts < config.retry_count {
            attempts += 1;

            let mut request = match config.method.as_str() {
                "PUT" => client.put(&config.url),
                _ => client.post(&config.url),
            };

            request = request
                .header("Content-Type", "application/json")
                .timeout(Duration::from_millis(config.timeout_ms))
                .body(payload.clone());

            for (key, value) in &config.headers {
                request = request.header(key, value);
            }

            // Add signature if secret is configured
            if let Some(ref secret) = config.secret {
                let signature = Self::compute_signature(&payload, secret);
                request = request.header("X-Strata-Signature", signature);
            }

            match request.send().await {
                Ok(response) => {
                    status_code = Some(response.status().as_u16());
                    response_body = response.text().await.ok().map(|s| {
                        if s.len() > 1000 {
                            format!("{}...", &s[..1000])
                        } else {
                            s
                        }
                    });

                    if status_code.unwrap() >= 200 && status_code.unwrap() < 300 {
                        return Some(WebhookDelivery {
                            id: uuid::Uuid::new_v4().to_string(),
                            webhook_id: config.id.clone(),
                            event_ids,
                            success: true,
                            status_code,
                            response_body,
                            error: None,
                            attempts,
                            attempted_at: Utc::now(),
                            duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }

                    last_error = Some(format!("HTTP {}", status_code.unwrap()));
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                }
            }

            // Exponential backoff
            if attempts < config.retry_count {
                tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempts as u32))).await;
            }
        }

        Some(WebhookDelivery {
            id: uuid::Uuid::new_v4().to_string(),
            webhook_id: config.id.clone(),
            event_ids,
            success: false,
            status_code,
            response_body,
            error: last_error,
            attempts,
            attempted_at: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn compute_signature(payload: &str, secret: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        hasher.update(payload.as_bytes());
        format!("sha256={:x}", hasher.finalize())
    }
}

/// Helper for emitting common events.
pub struct EventEmitter {
    bus: Arc<EventBus>,
}

impl EventEmitter {
    /// Create a new emitter.
    pub fn new(bus: Arc<EventBus>) -> Self {
        Self { bus }
    }

    /// Emit file created event.
    pub async fn file_created(&self, path: &str, inode: Option<InodeId>, size: u64) {
        let mut event = Event::file_system(
            EventType::FileCreated,
            path,
            inode,
            self.bus.node_id,
        );
        if let EventPayload::FileSystem { size: ref mut s, .. } = event.payload {
            *s = Some(size);
        }
        self.bus.publish(event).await;
    }

    /// Emit file modified event.
    pub async fn file_modified(&self, path: &str, inode: Option<InodeId>, size: u64) {
        let mut event = Event::file_system(
            EventType::FileModified,
            path,
            inode,
            self.bus.node_id,
        );
        if let EventPayload::FileSystem { size: ref mut s, .. } = event.payload {
            *s = Some(size);
        }
        self.bus.publish(event).await;
    }

    /// Emit file deleted event.
    pub async fn file_deleted(&self, path: &str, inode: Option<InodeId>) {
        self.bus
            .publish(Event::file_system(
                EventType::FileDeleted,
                path,
                inode,
                self.bus.node_id,
            ))
            .await;
    }

    /// Emit file renamed event.
    pub async fn file_renamed(&self, old_path: &str, new_path: &str, inode: Option<InodeId>) {
        let mut event = Event::file_system(
            EventType::FileRenamed,
            new_path,
            inode,
            self.bus.node_id,
        );
        if let EventPayload::FileSystem { old_path: ref mut op, .. } = event.payload {
            *op = Some(old_path.to_string());
        }
        self.bus.publish(event).await;
    }

    /// Emit directory created event.
    pub async fn directory_created(&self, path: &str, inode: Option<InodeId>) {
        self.bus
            .publish(Event::file_system(
                EventType::DirectoryCreated,
                path,
                inode,
                self.bus.node_id,
            ))
            .await;
    }

    /// Emit node joined event.
    pub async fn node_joined(&self, node_id: NodeId, addr: Option<String>) {
        let mut event = Event::cluster(EventType::NodeJoined, node_id, self.bus.node_id);
        if let EventPayload::Cluster { node_addr, .. } = &mut event.payload {
            *node_addr = addr;
        }
        self.bus.publish(event).await;
    }

    /// Emit leader changed event.
    pub async fn leader_changed(&self, old_leader: NodeId, new_leader: NodeId) {
        let mut event = Event::cluster(EventType::LeaderChanged, old_leader, self.bus.node_id);
        if let EventPayload::Cluster {
            new_leader: ref mut nl,
            ..
        } = event.payload
        {
            *nl = Some(new_leader);
        }
        self.bus.publish(event).await;
    }

    /// Emit chunk corrupted event.
    pub async fn chunk_corrupted(&self, chunk_id: ChunkId, node_id: NodeId, shard: u32) {
        let mut event = Event::chunk(
            EventType::ChunkCorrupted,
            chunk_id,
            Some(node_id),
            self.bus.node_id,
        );
        if let EventPayload::Chunk { shard_index, .. } = &mut event.payload {
            *shard_index = Some(shard);
        }
        self.bus.publish(event).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_category() {
        assert_eq!(EventType::FileCreated.category(), EventCategory::File);
        assert_eq!(EventType::DirectoryCreated.category(), EventCategory::Directory);
        assert_eq!(EventType::NodeJoined.category(), EventCategory::Cluster);
    }

    #[test]
    fn test_event_creation() {
        let event = Event::file_system(EventType::FileCreated, "/test/file.txt", Some(123), 1);

        assert_eq!(event.event_type, EventType::FileCreated);
        assert_eq!(event.source_node, 1);

        if let EventPayload::FileSystem { path, inode, .. } = &event.payload {
            assert_eq!(path, "/test/file.txt");
            assert_eq!(*inode, Some(123));
        } else {
            panic!("Wrong payload type");
        }
    }

    #[test]
    fn test_event_filter_matches() {
        let event = Event::file_system(EventType::FileCreated, "/data/test.txt", Some(100), 1);

        // Empty filter matches all
        let filter = EventFilter::new();
        assert!(filter.matches(&event));

        // Type filter
        let filter = EventFilter::new().event_types([EventType::FileCreated]);
        assert!(filter.matches(&event));

        let filter = EventFilter::new().event_types([EventType::FileDeleted]);
        assert!(!filter.matches(&event));

        // Path prefix filter
        let filter = EventFilter::new().path_prefix("/data");
        assert!(filter.matches(&event));

        let filter = EventFilter::new().path_prefix("/other");
        assert!(!filter.matches(&event));

        // Category filter
        let filter = EventFilter::new().categories([EventCategory::File]);
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_webhook_config() {
        let config = WebhookConfig::new("test-hook", "https://example.com/webhook")
            .with_secret("my-secret")
            .with_header("Authorization", "Bearer token")
            .with_filter(EventFilter::new().event_types([EventType::FileCreated]))
            .with_batching(10, 500);

        assert_eq!(config.name, "test-hook");
        assert!(config.secret.is_some());
        assert!(config.batch);
        assert_eq!(config.batch_size, 10);
    }

    #[tokio::test]
    async fn test_event_bus_publish() {
        let bus = EventBus::new(EventBusConfig::default(), 1);

        let event = Event::file_system(EventType::FileCreated, "/test.txt", None, 1);
        bus.publish(event).await;

        let stats = bus.stats().await;
        assert_eq!(stats.events_published, 1);
    }

    #[tokio::test]
    async fn test_event_bus_subscribe() {
        let bus = EventBus::new(EventBusConfig::default(), 1);

        let mut sub = bus
            .subscribe(EventFilter::new().event_types([EventType::FileCreated]))
            .await
            .unwrap();

        // Publish matching event
        let event = Event::file_system(EventType::FileCreated, "/test.txt", None, 1);
        bus.publish(event).await;

        // Publish non-matching event
        let event2 = Event::file_system(EventType::FileDeleted, "/test.txt", None, 1);
        bus.publish(event2).await;

        // Should receive only the matching event
        let received = sub.try_recv();
        assert!(received.is_some());
        assert_eq!(received.unwrap().event_type, EventType::FileCreated);

        // Should not receive the non-matching event
        let received2 = sub.try_recv();
        assert!(received2.is_none());
    }

    #[tokio::test]
    async fn test_event_bus_history() {
        let bus = EventBus::new(EventBusConfig::default(), 1);

        for i in 0..5 {
            let event = Event::file_system(
                EventType::FileCreated,
                format!("/file{}.txt", i),
                None,
                1,
            );
            bus.publish(event).await;
        }

        let history = bus.history(3).await;
        assert_eq!(history.len(), 3);
    }

    #[tokio::test]
    async fn test_webhook_registration() {
        let bus = EventBus::new(EventBusConfig::default(), 1);

        let config = WebhookConfig::new("test", "https://example.com/hook");
        let id = bus.register_webhook(config).await.unwrap();

        let webhooks = bus.list_webhooks().await;
        assert_eq!(webhooks.len(), 1);

        bus.unregister_webhook(&id).await.unwrap();

        let webhooks = bus.list_webhooks().await;
        assert_eq!(webhooks.len(), 0);
    }

    #[test]
    fn test_compute_signature() {
        let signature = EventBus::compute_signature("test payload", "secret");
        assert!(signature.starts_with("sha256="));
    }

    #[tokio::test]
    async fn test_event_emitter() {
        let bus = EventBus::new(EventBusConfig::default(), 1);
        let emitter = EventEmitter::new(Arc::clone(&bus));

        emitter.file_created("/new-file.txt", Some(123), 1024).await;
        emitter.file_deleted("/old-file.txt", None).await;

        let history = bus.history(10).await;
        assert_eq!(history.len(), 2);
    }
}
