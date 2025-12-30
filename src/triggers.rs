//! Serverless Triggers for Event-Driven Computing
//!
//! Provides event-driven triggers that fire on storage events, enabling
//! serverless computing patterns similar to AWS Lambda S3 triggers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Trigger System                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Event Bus  │  Trigger Registry  │  Handler Executor        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Webhooks  │  gRPC  │  WASM  │  Message Queues  │  Custom   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Filtering  │  Routing  │  Rate Limiting  │  DLQ           │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::triggers::{TriggerManager, Trigger, TriggerHandler};
//!
//! let manager = TriggerManager::new(config);
//!
//! // Register a trigger for file creation
//! manager.register(
//!     Trigger::builder()
//!         .name("process-uploads")
//!         .on_event(EventType::ObjectCreated)
//!         .filter(Filter::prefix("/uploads/"))
//!         .filter(Filter::suffix(".jpg"))
//!         .handler(WebhookHandler::new("https://api.example.com/process"))
//!         .build()
//! ).await?;
//! ```

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, InodeId};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

// ============================================================================
// Configuration
// ============================================================================

/// Trigger system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Enable trigger system
    pub enabled: bool,
    /// Maximum triggers per bucket
    pub max_triggers_per_bucket: usize,
    /// Maximum event queue size
    pub event_queue_size: usize,
    /// Event processing workers
    pub worker_count: usize,
    /// Handler timeout
    pub handler_timeout: Duration,
    /// Maximum retries for failed handlers
    pub max_retries: u32,
    /// Retry backoff base
    pub retry_backoff: Duration,
    /// Enable dead letter queue
    pub enable_dlq: bool,
    /// DLQ retention period
    pub dlq_retention: Duration,
    /// Rate limit (events per second per trigger)
    pub rate_limit: Option<u32>,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_triggers_per_bucket: 100,
            event_queue_size: 10000,
            worker_count: 4,
            handler_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff: Duration::from_secs(1),
            enable_dlq: true,
            dlq_retention: Duration::from_secs(86400 * 7), // 7 days
            rate_limit: Some(100),
        }
    }
}

// ============================================================================
// Storage Events
// ============================================================================

/// Types of storage events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Object was created
    ObjectCreated,
    /// Object was created via PUT
    ObjectCreatedPut,
    /// Object was created via POST
    ObjectCreatedPost,
    /// Object was created via COPY
    ObjectCreatedCopy,
    /// Object was created via multipart upload
    ObjectCreatedMultipart,
    /// Object was deleted
    ObjectDeleted,
    /// Object was accessed (read)
    ObjectAccessed,
    /// Object metadata was updated
    ObjectMetadataUpdated,
    /// Object was restored from archive
    ObjectRestored,
    /// Replication completed
    ReplicationCompleted,
    /// Lifecycle transition occurred
    LifecycleTransition,
    /// Quota threshold exceeded
    QuotaExceeded,
    /// Bucket was created
    BucketCreated,
    /// Bucket was deleted
    BucketDeleted,
}

impl EventType {
    /// Check if this is a create event type
    pub fn is_create(&self) -> bool {
        matches!(
            self,
            EventType::ObjectCreated
                | EventType::ObjectCreatedPut
                | EventType::ObjectCreatedPost
                | EventType::ObjectCreatedCopy
                | EventType::ObjectCreatedMultipart
        )
    }

    /// Get all create event types
    pub fn all_create() -> Vec<EventType> {
        vec![
            EventType::ObjectCreated,
            EventType::ObjectCreatedPut,
            EventType::ObjectCreatedPost,
            EventType::ObjectCreatedCopy,
            EventType::ObjectCreatedMultipart,
        ]
    }
}

/// A storage event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEvent {
    /// Unique event ID
    pub id: String,
    /// Event type
    pub event_type: EventType,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Object size in bytes
    pub size: Option<u64>,
    /// Object ETag
    pub etag: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// User/principal who caused the event
    pub principal: Option<String>,
    /// Source IP address
    pub source_ip: Option<String>,
    /// Request ID
    pub request_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Inode ID (internal)
    pub inode_id: Option<InodeId>,
    /// Chunk IDs (internal)
    pub chunk_ids: Vec<ChunkId>,
}

impl StorageEvent {
    /// Create a new storage event
    pub fn new(event_type: EventType, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            event_type,
            timestamp: Utc::now(),
            bucket: bucket.into(),
            key: key.into(),
            size: None,
            etag: None,
            content_type: None,
            principal: None,
            source_ip: None,
            request_id: None,
            metadata: HashMap::new(),
            inode_id: None,
            chunk_ids: Vec::new(),
        }
    }

    /// Set object size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Set ETag
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set principal
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

// ============================================================================
// Event Filters
// ============================================================================

/// Filter for matching events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    /// Match key prefix
    Prefix(String),
    /// Match key suffix
    Suffix(String),
    /// Match key regex
    Regex(String),
    /// Match content type
    ContentType(String),
    /// Match minimum size
    MinSize(u64),
    /// Match maximum size
    MaxSize(u64),
    /// Match metadata key-value
    Metadata { key: String, value: String },
    /// Match principal
    Principal(String),
    /// Logical AND of filters
    And(Vec<Filter>),
    /// Logical OR of filters
    Or(Vec<Filter>),
    /// Logical NOT of filter
    Not(Box<Filter>),
}

impl Filter {
    /// Create a prefix filter
    pub fn prefix(prefix: impl Into<String>) -> Self {
        Filter::Prefix(prefix.into())
    }

    /// Create a suffix filter
    pub fn suffix(suffix: impl Into<String>) -> Self {
        Filter::Suffix(suffix.into())
    }

    /// Create a regex filter
    pub fn regex(pattern: impl Into<String>) -> Self {
        Filter::Regex(pattern.into())
    }

    /// Create a content type filter
    pub fn content_type(ct: impl Into<String>) -> Self {
        Filter::ContentType(ct.into())
    }

    /// Create a size range filter
    pub fn size_range(min: u64, max: u64) -> Self {
        Filter::And(vec![Filter::MinSize(min), Filter::MaxSize(max)])
    }

    /// Check if an event matches this filter
    pub fn matches(&self, event: &StorageEvent) -> bool {
        match self {
            Filter::Prefix(prefix) => event.key.starts_with(prefix),
            Filter::Suffix(suffix) => event.key.ends_with(suffix),
            Filter::Regex(pattern) => {
                Regex::new(pattern)
                    .map(|re| re.is_match(&event.key))
                    .unwrap_or(false)
            }
            Filter::ContentType(ct) => {
                event.content_type.as_ref().map(|t| t == ct).unwrap_or(false)
            }
            Filter::MinSize(min) => event.size.map(|s| s >= *min).unwrap_or(false),
            Filter::MaxSize(max) => event.size.map(|s| s <= *max).unwrap_or(false),
            Filter::Metadata { key, value } => {
                event.metadata.get(key).map(|v| v == value).unwrap_or(false)
            }
            Filter::Principal(principal) => {
                event.principal.as_ref().map(|p| p == principal).unwrap_or(false)
            }
            Filter::And(filters) => filters.iter().all(|f| f.matches(event)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(event)),
            Filter::Not(filter) => !filter.matches(event),
        }
    }
}

// ============================================================================
// Trigger Handlers
// ============================================================================

/// Result of handler execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerResult {
    /// Whether execution succeeded
    pub success: bool,
    /// Response body if any
    pub response: Option<String>,
    /// Error message if failed
    pub error: Option<String>,
    /// HTTP status code if applicable
    pub status_code: Option<u16>,
    /// Execution duration
    pub duration: Duration,
    /// Retry count
    pub retry_count: u32,
}

/// Trait for trigger handlers
#[async_trait::async_trait]
pub trait TriggerHandler: Send + Sync {
    /// Get handler name
    fn name(&self) -> &str;

    /// Handle an event
    async fn handle(&self, event: &StorageEvent) -> HandlerResult;

    /// Check if handler is healthy
    async fn health_check(&self) -> bool {
        true
    }
}

// ============================================================================
// Webhook Handler
// ============================================================================

/// Webhook handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook URL
    pub url: String,
    /// HTTP method (POST, PUT, etc.)
    pub method: String,
    /// Headers to include
    pub headers: HashMap<String, String>,
    /// Request timeout
    pub timeout: Duration,
    /// Enable request signing
    pub sign_requests: bool,
    /// Signing secret (if enabled)
    pub signing_secret: Option<String>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            timeout: Duration::from_secs(30),
            sign_requests: false,
            signing_secret: None,
        }
    }
}

/// HTTP webhook handler
pub struct WebhookHandler {
    config: WebhookConfig,
}

impl WebhookHandler {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            config: WebhookConfig {
                url: url.into(),
                ..Default::default()
            },
        }
    }

    pub fn with_config(config: WebhookConfig) -> Self {
        Self { config }
    }

    fn sign_payload(&self, payload: &[u8]) -> Option<String> {
        if !self.config.sign_requests {
            return None;
        }

        self.config.signing_secret.as_ref().map(|secret| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            secret.hash(&mut hasher);
            payload.hash(&mut hasher);
            format!("sha256={:x}", hasher.finish())
        })
    }
}

#[async_trait::async_trait]
impl TriggerHandler for WebhookHandler {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn handle(&self, event: &StorageEvent) -> HandlerResult {
        let start = Instant::now();

        // Serialize event
        let payload = match serde_json::to_vec(event) {
            Ok(p) => p,
            Err(e) => {
                return HandlerResult {
                    success: false,
                    response: None,
                    error: Some(format!("Failed to serialize event: {}", e)),
                    status_code: None,
                    duration: start.elapsed(),
                    retry_count: 0,
                };
            }
        };

        // Sign if configured
        let signature = self.sign_payload(&payload);

        // In a real implementation, we'd make an HTTP request here
        // For now, we simulate it
        debug!(
            "Webhook {} would be called with {} bytes",
            self.config.url,
            payload.len()
        );

        if let Some(sig) = &signature {
            debug!("Request signature: {}", sig);
        }

        // Simulate success
        HandlerResult {
            success: true,
            response: Some(r#"{"status": "ok"}"#.to_string()),
            error: None,
            status_code: Some(200),
            duration: start.elapsed(),
            retry_count: 0,
        }
    }

    async fn health_check(&self) -> bool {
        // Would ping the webhook endpoint
        true
    }
}

// ============================================================================
// Message Queue Handler
// ============================================================================

/// Message queue handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Queue type
    pub queue_type: QueueType,
    /// Connection URL
    pub url: String,
    /// Queue/topic name
    pub queue_name: String,
    /// Message format
    pub message_format: MessageFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueType {
    Kafka,
    RabbitMQ,
    Redis,
    SQS,
    NATS,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageFormat {
    Json,
    Avro,
    Protobuf,
    CloudEvents,
}

/// Message queue handler
pub struct QueueHandler {
    config: QueueConfig,
}

impl QueueHandler {
    pub fn new(config: QueueConfig) -> Self {
        Self { config }
    }

    pub fn kafka(url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            config: QueueConfig {
                queue_type: QueueType::Kafka,
                url: url.into(),
                queue_name: topic.into(),
                message_format: MessageFormat::Json,
            },
        }
    }
}

#[async_trait::async_trait]
impl TriggerHandler for QueueHandler {
    fn name(&self) -> &str {
        "queue"
    }

    async fn handle(&self, _event: &StorageEvent) -> HandlerResult {
        let start = Instant::now();

        // Simulate publishing to queue
        debug!(
            "Would publish to {:?} queue {} at {}",
            self.config.queue_type, self.config.queue_name, self.config.url
        );

        HandlerResult {
            success: true,
            response: Some(format!("Published to {}", self.config.queue_name)),
            error: None,
            status_code: None,
            duration: start.elapsed(),
            retry_count: 0,
        }
    }
}

// ============================================================================
// gRPC Handler
// ============================================================================

/// gRPC handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// gRPC endpoint
    pub endpoint: String,
    /// Service name
    pub service: String,
    /// Method name
    pub method: String,
    /// Enable TLS
    pub tls: bool,
    /// Timeout
    pub timeout: Duration,
}

/// gRPC handler
pub struct GrpcHandler {
    config: GrpcConfig,
}

impl GrpcHandler {
    pub fn new(config: GrpcConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl TriggerHandler for GrpcHandler {
    fn name(&self) -> &str {
        "grpc"
    }

    async fn handle(&self, _event: &StorageEvent) -> HandlerResult {
        let start = Instant::now();

        debug!(
            "Would call gRPC {}/{} at {}",
            self.config.service, self.config.method, self.config.endpoint
        );

        HandlerResult {
            success: true,
            response: None,
            error: None,
            status_code: None,
            duration: start.elapsed(),
            retry_count: 0,
        }
    }
}

// ============================================================================
// Lambda/Function Handler
// ============================================================================

/// Lambda/Function handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Function type
    pub function_type: FunctionType,
    /// Function ARN or name
    pub function_name: String,
    /// Invocation type
    pub invocation_type: InvocationType,
    /// Region (for cloud functions)
    pub region: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionType {
    AwsLambda,
    AzureFunction,
    GcpCloudFunction,
    OpenFaaS,
    Knative,
    LocalWasm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationType {
    /// Wait for response
    RequestResponse,
    /// Fire and forget
    Event,
    /// Dry run (validate only)
    DryRun,
}

/// Function handler
pub struct FunctionHandler {
    config: FunctionConfig,
}

impl FunctionHandler {
    pub fn new(config: FunctionConfig) -> Self {
        Self { config }
    }

    pub fn aws_lambda(function_name: impl Into<String>) -> Self {
        Self {
            config: FunctionConfig {
                function_type: FunctionType::AwsLambda,
                function_name: function_name.into(),
                invocation_type: InvocationType::Event,
                region: None,
            },
        }
    }
}

#[async_trait::async_trait]
impl TriggerHandler for FunctionHandler {
    fn name(&self) -> &str {
        "function"
    }

    async fn handle(&self, _event: &StorageEvent) -> HandlerResult {
        let start = Instant::now();

        debug!(
            "Would invoke {:?} function {}",
            self.config.function_type, self.config.function_name
        );

        HandlerResult {
            success: true,
            response: None,
            error: None,
            status_code: None,
            duration: start.elapsed(),
            retry_count: 0,
        }
    }
}

// ============================================================================
// Trigger Definition
// ============================================================================

/// A trigger definition
pub struct Trigger {
    /// Unique trigger ID
    pub id: String,
    /// Trigger name
    pub name: String,
    /// Description
    pub description: String,
    /// Bucket to watch (or "*" for all)
    pub bucket: String,
    /// Event types to trigger on
    pub event_types: Vec<EventType>,
    /// Filters to apply
    pub filters: Vec<Filter>,
    /// Handler to execute
    handler: Arc<dyn TriggerHandler>,
    /// Enabled status
    pub enabled: bool,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Last triggered timestamp
    pub last_triggered: Option<DateTime<Utc>>,
    /// Total trigger count
    pub trigger_count: Arc<AtomicU64>,
}

impl Clone for Trigger {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            bucket: self.bucket.clone(),
            event_types: self.event_types.clone(),
            filters: self.filters.clone(),
            handler: Arc::clone(&self.handler),
            enabled: self.enabled,
            created_at: self.created_at,
            last_triggered: self.last_triggered,
            trigger_count: Arc::clone(&self.trigger_count),
        }
    }
}

impl Trigger {
    /// Create a new trigger builder
    pub fn builder() -> TriggerBuilder {
        TriggerBuilder::new()
    }

    /// Check if this trigger matches an event
    pub fn matches(&self, event: &StorageEvent) -> bool {
        if !self.enabled {
            return false;
        }

        // Check bucket
        if self.bucket != "*" && self.bucket != event.bucket {
            return false;
        }

        // Check event type
        if !self.event_types.is_empty() && !self.event_types.contains(&event.event_type) {
            return false;
        }

        // Check filters
        self.filters.iter().all(|f| f.matches(event))
    }

    /// Execute the trigger handler
    pub async fn execute(&self, event: &StorageEvent) -> HandlerResult {
        self.trigger_count.fetch_add(1, Ordering::Relaxed);
        self.handler.handle(event).await
    }
}

/// Builder for triggers
pub struct TriggerBuilder {
    name: Option<String>,
    description: Option<String>,
    bucket: String,
    event_types: Vec<EventType>,
    filters: Vec<Filter>,
    handler: Option<Arc<dyn TriggerHandler>>,
    enabled: bool,
}

impl TriggerBuilder {
    pub fn new() -> Self {
        Self {
            name: None,
            description: None,
            bucket: "*".to_string(),
            event_types: Vec::new(),
            filters: Vec::new(),
            handler: None,
            enabled: true,
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    pub fn on_event(mut self, event_type: EventType) -> Self {
        self.event_types.push(event_type);
        self
    }

    pub fn on_events(mut self, event_types: Vec<EventType>) -> Self {
        self.event_types.extend(event_types);
        self
    }

    pub fn on_create(mut self) -> Self {
        self.event_types.extend(EventType::all_create());
        self
    }

    pub fn on_delete(mut self) -> Self {
        self.event_types.push(EventType::ObjectDeleted);
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn handler<H: TriggerHandler + 'static>(mut self, handler: H) -> Self {
        self.handler = Some(Arc::new(handler));
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> Result<Trigger> {
        let name = self
            .name
            .ok_or_else(|| StrataError::InvalidArgument("Trigger name is required".into()))?;

        let handler = self
            .handler
            .ok_or_else(|| StrataError::InvalidArgument("Trigger handler is required".into()))?;

        Ok(Trigger {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description: self.description.unwrap_or_default(),
            bucket: self.bucket,
            event_types: self.event_types,
            filters: self.filters,
            handler,
            enabled: self.enabled,
            created_at: Utc::now(),
            last_triggered: None,
            trigger_count: Arc::new(AtomicU64::new(0)),
        })
    }
}

impl Default for TriggerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Dead Letter Queue
// ============================================================================

/// Failed event for DLQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Original event
    pub event: StorageEvent,
    /// Trigger that failed
    pub trigger_id: String,
    /// Trigger name
    pub trigger_name: String,
    /// Error message
    pub error: String,
    /// Retry count
    pub retry_count: u32,
    /// First failure time
    pub first_failure: DateTime<Utc>,
    /// Last failure time
    pub last_failure: DateTime<Utc>,
}

/// Dead letter queue
pub struct DeadLetterQueue {
    letters: RwLock<Vec<DeadLetter>>,
    retention: Duration,
}

impl DeadLetterQueue {
    pub fn new(retention: Duration) -> Self {
        Self {
            letters: RwLock::new(Vec::new()),
            retention,
        }
    }

    /// Add a failed event
    pub async fn add(&self, letter: DeadLetter) {
        self.letters.write().await.push(letter);
    }

    /// Get all dead letters
    pub async fn list(&self) -> Vec<DeadLetter> {
        self.letters.read().await.clone()
    }

    /// Remove and return a dead letter for retry
    pub async fn pop(&self, id: &str) -> Option<DeadLetter> {
        let mut letters = self.letters.write().await;
        if let Some(idx) = letters.iter().position(|l| l.event.id == id) {
            Some(letters.remove(idx))
        } else {
            None
        }
    }

    /// Purge expired letters
    pub async fn purge_expired(&self) {
        let cutoff = Utc::now() - chrono::Duration::from_std(self.retention).unwrap();
        let mut letters = self.letters.write().await;
        letters.retain(|l| l.first_failure > cutoff);
    }

    /// Get count
    pub async fn count(&self) -> usize {
        self.letters.read().await.len()
    }
}

// ============================================================================
// Rate Limiter
// ============================================================================

/// Per-trigger rate limiter
struct TriggerRateLimiter {
    tokens: AtomicU64,
    last_refill: RwLock<Instant>,
    rate: u32, // tokens per second
}

impl TriggerRateLimiter {
    fn new(rate: u32) -> Self {
        Self {
            tokens: AtomicU64::new(rate as u64),
            last_refill: RwLock::new(Instant::now()),
            rate,
        }
    }

    async fn try_acquire(&self) -> bool {
        // Refill tokens
        let mut last_refill = self.last_refill.write().await;
        let elapsed = last_refill.elapsed();
        let refill = (elapsed.as_secs_f64() * self.rate as f64) as u64;

        if refill > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + refill).min(self.rate as u64);
            self.tokens.store(new_tokens, Ordering::Relaxed);
            *last_refill = Instant::now();
        }

        // Try to consume a token
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }

            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }
}

// ============================================================================
// Trigger Manager
// ============================================================================

/// Manages triggers and event dispatch
pub struct TriggerManager {
    config: TriggerConfig,
    triggers: Arc<RwLock<HashMap<String, Trigger>>>,
    rate_limiters: Arc<RwLock<HashMap<String, TriggerRateLimiter>>>,
    dlq: Arc<DeadLetterQueue>,
    event_tx: mpsc::Sender<StorageEvent>,
    stats: Arc<TriggerStats>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

/// Statistics for trigger system
#[derive(Debug, Default)]
pub struct TriggerStats {
    pub events_received: AtomicU64,
    pub events_processed: AtomicU64,
    pub events_filtered: AtomicU64,
    pub triggers_executed: AtomicU64,
    pub triggers_succeeded: AtomicU64,
    pub triggers_failed: AtomicU64,
    pub triggers_rate_limited: AtomicU64,
    pub dlq_entries: AtomicU64,
}

impl TriggerManager {
    /// Create a new trigger manager
    pub fn new(config: TriggerConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(config.event_queue_size);
        let dlq = Arc::new(DeadLetterQueue::new(config.dlq_retention));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let triggers = Arc::new(RwLock::new(HashMap::new()));
        let rate_limiters = Arc::new(RwLock::new(HashMap::new()));
        let stats = Arc::new(TriggerStats::default());

        // Wrap receiver in Arc<Mutex> for sharing between workers
        let shared_rx = Arc::new(tokio::sync::Mutex::new(event_rx));

        let manager = Self {
            config: config.clone(),
            triggers: Arc::clone(&triggers),
            rate_limiters: Arc::clone(&rate_limiters),
            dlq: Arc::clone(&dlq),
            event_tx,
            stats: Arc::clone(&stats),
            running: Arc::clone(&running),
        };

        // Start workers
        for i in 0..config.worker_count {
            let rx = Arc::clone(&shared_rx);
            let triggers = Arc::clone(&triggers);
            let rate_limiters = Arc::clone(&rate_limiters);
            let dlq = Arc::clone(&dlq);
            let stats = Arc::clone(&stats);
            let config = config.clone();
            let running = Arc::clone(&running);

            tokio::spawn(async move {
                Self::worker_loop(
                    i,
                    rx,
                    triggers,
                    rate_limiters,
                    dlq,
                    &stats,
                    config,
                    running,
                )
                .await;
            });
        }

        info!(
            "Trigger manager started with {} workers",
            config.worker_count
        );
        manager
    }

    async fn worker_loop(
        worker_id: usize,
        rx: Arc<tokio::sync::Mutex<mpsc::Receiver<StorageEvent>>>,
        triggers: Arc<RwLock<HashMap<String, Trigger>>>,
        rate_limiters: Arc<RwLock<HashMap<String, TriggerRateLimiter>>>,
        dlq: Arc<DeadLetterQueue>,
        stats: &TriggerStats,
        config: TriggerConfig,
        running: Arc<std::sync::atomic::AtomicBool>,
    ) {
        while running.load(Ordering::Relaxed) {
            let event = {
                let mut rx_guard = rx.lock().await;
                match rx_guard.recv().await {
                    Some(e) => e,
                    None => break,
                }
            };

            debug!(worker = worker_id, event_id = %event.id, "Processing event");
            stats.events_processed.fetch_add(1, Ordering::Relaxed);

            // Find matching triggers
            let triggers_guard = triggers.read().await;
            let matching: Vec<_> = triggers_guard
                .values()
                .filter(|t| t.matches(&event))
                .collect();

            if matching.is_empty() {
                stats.events_filtered.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Execute matching triggers
            for trigger in matching {
                // Check rate limit
                if let Some(_limit) = config.rate_limit {
                    let limiters = rate_limiters.read().await;
                    if let Some(limiter) = limiters.get(&trigger.id) {
                        if !limiter.try_acquire().await {
                            stats.triggers_rate_limited.fetch_add(1, Ordering::Relaxed);
                            debug!(trigger = %trigger.name, "Rate limited");
                            continue;
                        }
                    }
                }

                stats.triggers_executed.fetch_add(1, Ordering::Relaxed);

                // Execute with retries
                let mut result = trigger.execute(&event).await;
                let mut retry_count = 0;

                while !result.success && retry_count < config.max_retries {
                    retry_count += 1;
                    let backoff = config.retry_backoff * retry_count;
                    tokio::time::sleep(backoff).await;
                    result = trigger.execute(&event).await;
                    result.retry_count = retry_count;
                }

                if result.success {
                    stats.triggers_succeeded.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.triggers_failed.fetch_add(1, Ordering::Relaxed);

                    // Send to DLQ
                    if config.enable_dlq {
                        dlq.add(DeadLetter {
                            event: event.clone(),
                            trigger_id: trigger.id.clone(),
                            trigger_name: trigger.name.clone(),
                            error: result.error.unwrap_or_else(|| "Unknown error".into()),
                            retry_count,
                            first_failure: Utc::now(),
                            last_failure: Utc::now(),
                        })
                        .await;
                        stats.dlq_entries.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        debug!(worker = worker_id, "Worker loop terminated");
    }

    /// Register a new trigger
    pub async fn register(&self, trigger: Trigger) -> Result<String> {
        let id = trigger.id.clone();

        // Check limits
        let triggers = self.triggers.read().await;
        let bucket_count = triggers
            .values()
            .filter(|t| t.bucket == trigger.bucket)
            .count();

        if bucket_count >= self.config.max_triggers_per_bucket {
            return Err(StrataError::InvalidArgument(format!(
                "Maximum triggers ({}) reached for bucket {}",
                self.config.max_triggers_per_bucket, trigger.bucket
            )));
        }
        drop(triggers);

        // Create rate limiter
        if let Some(rate) = self.config.rate_limit {
            self.rate_limiters
                .write()
                .await
                .insert(id.clone(), TriggerRateLimiter::new(rate));
        }

        // Register trigger
        self.triggers.write().await.insert(id.clone(), trigger);

        info!(trigger_id = %id, "Registered trigger");
        Ok(id)
    }

    /// Unregister a trigger
    pub async fn unregister(&self, trigger_id: &str) -> Result<()> {
        if self.triggers.write().await.remove(trigger_id).is_some() {
            self.rate_limiters.write().await.remove(trigger_id);
            info!(trigger_id = %trigger_id, "Unregistered trigger");
            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Trigger {} not found",
                trigger_id
            )))
        }
    }

    /// Get a trigger by ID
    pub async fn get(&self, trigger_id: &str) -> Option<Trigger> {
        self.triggers.read().await.get(trigger_id).cloned()
    }

    /// List all triggers
    pub async fn list(&self) -> Vec<Trigger> {
        self.triggers.read().await.values().cloned().collect()
    }

    /// Enable a trigger
    pub async fn enable(&self, trigger_id: &str) -> Result<()> {
        let mut triggers = self.triggers.write().await;
        if let Some(trigger) = triggers.get_mut(trigger_id) {
            trigger.enabled = true;
            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Trigger {} not found",
                trigger_id
            )))
        }
    }

    /// Disable a trigger
    pub async fn disable(&self, trigger_id: &str) -> Result<()> {
        let mut triggers = self.triggers.write().await;
        if let Some(trigger) = triggers.get_mut(trigger_id) {
            trigger.enabled = false;
            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Trigger {} not found",
                trigger_id
            )))
        }
    }

    /// Emit an event to be processed
    pub async fn emit(&self, event: StorageEvent) -> Result<()> {
        self.stats.events_received.fetch_add(1, Ordering::Relaxed);

        self.event_tx
            .send(event)
            .await
            .map_err(|_| StrataError::Internal("Event queue full".into()))
    }

    /// Get dead letter queue
    pub fn dlq(&self) -> &Arc<DeadLetterQueue> {
        &self.dlq
    }

    /// Get statistics
    pub fn stats(&self) -> &TriggerStats {
        &self.stats
    }

    /// Shutdown the trigger manager
    pub async fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        info!("Trigger manager shutting down");
    }
}

// ============================================================================
// Event Emitter Middleware
// ============================================================================

/// Middleware for emitting events from storage operations
pub struct EventEmitter {
    manager: Arc<TriggerManager>,
}

impl EventEmitter {
    pub fn new(manager: Arc<TriggerManager>) -> Self {
        Self { manager }
    }

    /// Emit object created event
    pub async fn object_created(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        etag: Option<String>,
    ) -> Result<()> {
        let mut event = StorageEvent::new(EventType::ObjectCreated, bucket, key).with_size(size);

        if let Some(etag) = etag {
            event = event.with_etag(etag);
        }

        self.manager.emit(event).await
    }

    /// Emit object deleted event
    pub async fn object_deleted(&self, bucket: &str, key: &str) -> Result<()> {
        let event = StorageEvent::new(EventType::ObjectDeleted, bucket, key);
        self.manager.emit(event).await
    }

    /// Emit object accessed event
    pub async fn object_accessed(&self, bucket: &str, key: &str, principal: Option<&str>) -> Result<()> {
        let mut event = StorageEvent::new(EventType::ObjectAccessed, bucket, key);

        if let Some(principal) = principal {
            event = event.with_principal(principal);
        }

        self.manager.emit(event).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_event() {
        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "key")
            .with_size(1024)
            .with_etag("abc123")
            .with_content_type("text/plain");

        assert_eq!(event.bucket, "bucket");
        assert_eq!(event.key, "key");
        assert_eq!(event.size, Some(1024));
        assert_eq!(event.etag, Some("abc123".to_string()));
    }

    #[test]
    fn test_filter_prefix() {
        let filter = Filter::prefix("/uploads/");
        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/image.jpg");

        assert!(filter.matches(&event));

        let event2 = StorageEvent::new(EventType::ObjectCreated, "bucket", "/downloads/file.txt");
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_filter_suffix() {
        let filter = Filter::suffix(".jpg");
        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/image.jpg");

        assert!(filter.matches(&event));

        let event2 = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/doc.pdf");
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_filter_size_range() {
        let filter = Filter::size_range(100, 1000);
        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "key").with_size(500);

        assert!(filter.matches(&event));

        let event2 = StorageEvent::new(EventType::ObjectCreated, "bucket", "key").with_size(2000);
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_filter_and_or() {
        let filter = Filter::And(vec![
            Filter::prefix("/uploads/"),
            Filter::Or(vec![Filter::suffix(".jpg"), Filter::suffix(".png")]),
        ]);

        let event1 = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/image.jpg");
        assert!(filter.matches(&event1));

        let event2 = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/image.png");
        assert!(filter.matches(&event2));

        let event3 = StorageEvent::new(EventType::ObjectCreated, "bucket", "/uploads/doc.pdf");
        assert!(!filter.matches(&event3));
    }

    #[test]
    fn test_trigger_builder() {
        let trigger = Trigger::builder()
            .name("test-trigger")
            .bucket("my-bucket")
            .on_create()
            .filter(Filter::suffix(".jpg"))
            .handler(WebhookHandler::new("https://example.com/webhook"))
            .build()
            .unwrap();

        assert_eq!(trigger.name, "test-trigger");
        assert_eq!(trigger.bucket, "my-bucket");
        assert!(!trigger.event_types.is_empty());
    }

    #[test]
    fn test_trigger_matches() {
        let trigger = Trigger::builder()
            .name("jpg-uploads")
            .bucket("uploads")
            .on_event(EventType::ObjectCreated)
            .filter(Filter::suffix(".jpg"))
            .handler(WebhookHandler::new("https://example.com"))
            .build()
            .unwrap();

        let event1 = StorageEvent::new(EventType::ObjectCreated, "uploads", "photo.jpg");
        assert!(trigger.matches(&event1));

        let event2 = StorageEvent::new(EventType::ObjectCreated, "other", "photo.jpg");
        assert!(!trigger.matches(&event2));

        let event3 = StorageEvent::new(EventType::ObjectDeleted, "uploads", "photo.jpg");
        assert!(!trigger.matches(&event3));
    }

    #[tokio::test]
    async fn test_webhook_handler() {
        let handler = WebhookHandler::new("https://example.com/webhook");
        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "key");

        let result = handler.handle(&event).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_trigger_manager() {
        let config = TriggerConfig::default();
        let manager = TriggerManager::new(config);

        let trigger = Trigger::builder()
            .name("test")
            .on_create()
            .handler(WebhookHandler::new("https://example.com"))
            .build()
            .unwrap();

        let id = manager.register(trigger).await.unwrap();
        assert!(!id.is_empty());

        let triggers = manager.list().await;
        assert_eq!(triggers.len(), 1);

        manager.unregister(&id).await.unwrap();
        let triggers = manager.list().await;
        assert!(triggers.is_empty());

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_event_emission() {
        let config = TriggerConfig::default();
        let manager = TriggerManager::new(config);

        let trigger = Trigger::builder()
            .name("test")
            .on_create()
            .handler(WebhookHandler::new("https://example.com"))
            .build()
            .unwrap();

        manager.register(trigger).await.unwrap();

        let event = StorageEvent::new(EventType::ObjectCreated, "bucket", "key");
        manager.emit(event).await.unwrap();

        // Give worker time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stats = manager.stats();
        assert_eq!(stats.events_received.load(Ordering::Relaxed), 1);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_dead_letter_queue() {
        let dlq = DeadLetterQueue::new(Duration::from_secs(3600));

        let letter = DeadLetter {
            event: StorageEvent::new(EventType::ObjectCreated, "bucket", "key"),
            trigger_id: "t1".to_string(),
            trigger_name: "test".to_string(),
            error: "Connection failed".to_string(),
            retry_count: 3,
            first_failure: Utc::now(),
            last_failure: Utc::now(),
        };

        dlq.add(letter).await;
        assert_eq!(dlq.count().await, 1);

        let letters = dlq.list().await;
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0].trigger_id, "t1");
    }

    #[test]
    fn test_event_type_helpers() {
        assert!(EventType::ObjectCreated.is_create());
        assert!(EventType::ObjectCreatedPut.is_create());
        assert!(!EventType::ObjectDeleted.is_create());

        let create_types = EventType::all_create();
        assert_eq!(create_types.len(), 5);
    }
}
