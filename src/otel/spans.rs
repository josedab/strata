// Span management for distributed tracing

use super::context::{SpanContext, SpanId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Span kind
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpanKind {
    /// Internal operation
    #[default]
    Internal,
    /// Server receiving request
    Server,
    /// Client making request
    Client,
    /// Producer sending message
    Producer,
    /// Consumer receiving message
    Consumer,
}

/// Span status
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpanStatus {
    /// Status not set
    #[default]
    Unset,
    /// Operation completed successfully
    Ok,
    /// Operation failed with error
    Error { message: String },
}

/// Attribute value types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
    FloatArray(Vec<f64>),
    BoolArray(Vec<bool>),
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<i64> for AttributeValue {
    fn from(v: i64) -> Self {
        AttributeValue::Int(v)
    }
}

impl From<f64> for AttributeValue {
    fn from(v: f64) -> Self {
        AttributeValue::Float(v)
    }
}

impl From<bool> for AttributeValue {
    fn from(v: bool) -> Self {
        AttributeValue::Bool(v)
    }
}

/// Span event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event name
    pub name: String,
    /// Event timestamp
    pub timestamp: SystemTime,
    /// Event attributes
    pub attributes: HashMap<String, AttributeValue>,
}

impl SpanEvent {
    /// Creates a new event
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timestamp: SystemTime::now(),
            attributes: HashMap::new(),
        }
    }

    /// Adds an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Span link to another span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLink {
    /// Linked span context
    pub context: SpanContext,
    /// Link attributes
    pub attributes: HashMap<String, AttributeValue>,
}

/// A complete span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    /// Span name
    pub name: String,
    /// Span context
    pub context: SpanContext,
    /// Parent span ID
    pub parent_span_id: Option<SpanId>,
    /// Span kind
    pub kind: SpanKind,
    /// Start time
    pub start_time: SystemTime,
    /// End time
    pub end_time: Option<SystemTime>,
    /// Span status
    pub status: SpanStatus,
    /// Attributes
    pub attributes: HashMap<String, AttributeValue>,
    /// Events
    pub events: Vec<SpanEvent>,
    /// Links to other spans
    pub links: Vec<SpanLink>,
    /// Dropped attribute count
    pub dropped_attributes_count: u32,
    /// Dropped event count
    pub dropped_events_count: u32,
    /// Dropped link count
    pub dropped_links_count: u32,
}

impl Span {
    /// Maximum attributes per span
    pub const MAX_ATTRIBUTES: usize = 128;
    /// Maximum events per span
    pub const MAX_EVENTS: usize = 128;
    /// Maximum links per span
    pub const MAX_LINKS: usize = 128;

    /// Creates a new span
    pub fn new(name: impl Into<String>, context: SpanContext) -> Self {
        Self {
            name: name.into(),
            context,
            parent_span_id: None,
            kind: SpanKind::Internal,
            start_time: SystemTime::now(),
            end_time: None,
            status: SpanStatus::Unset,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
        }
    }

    /// Sets parent span ID
    pub fn with_parent(mut self, parent_id: SpanId) -> Self {
        self.parent_span_id = Some(parent_id);
        self
    }

    /// Sets span kind
    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    /// Adds an attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) {
        if self.attributes.len() >= Self::MAX_ATTRIBUTES {
            self.dropped_attributes_count += 1;
            return;
        }
        self.attributes.insert(key.into(), value.into());
    }

    /// Adds an event
    pub fn add_event(&mut self, event: SpanEvent) {
        if self.events.len() >= Self::MAX_EVENTS {
            self.dropped_events_count += 1;
            return;
        }
        self.events.push(event);
    }

    /// Adds a link
    pub fn add_link(&mut self, link: SpanLink) {
        if self.links.len() >= Self::MAX_LINKS {
            self.dropped_links_count += 1;
            return;
        }
        self.links.push(link);
    }

    /// Sets the status
    pub fn set_status(&mut self, status: SpanStatus) {
        self.status = status;
    }

    /// Ends the span
    pub fn end(&mut self) {
        self.end_time = Some(SystemTime::now());
    }

    /// Gets the duration
    pub fn duration(&self) -> Option<Duration> {
        let end = self.end_time?;
        end.duration_since(self.start_time).ok()
    }

    /// Checks if span is sampled
    pub fn is_sampled(&self) -> bool {
        self.context.is_sampled()
    }
}

/// Span builder for fluent API
pub struct SpanBuilder {
    name: String,
    kind: SpanKind,
    parent: Option<SpanContext>,
    attributes: HashMap<String, AttributeValue>,
    links: Vec<SpanLink>,
    start_time: Option<SystemTime>,
}

impl SpanBuilder {
    /// Creates a new span builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SpanKind::Internal,
            parent: None,
            attributes: HashMap::new(),
            links: Vec::new(),
            start_time: None,
        }
    }

    /// Sets the span kind
    pub fn kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets as server span
    pub fn server(self) -> Self {
        self.kind(SpanKind::Server)
    }

    /// Sets as client span
    pub fn client(self) -> Self {
        self.kind(SpanKind::Client)
    }

    /// Sets as producer span
    pub fn producer(self) -> Self {
        self.kind(SpanKind::Producer)
    }

    /// Sets as consumer span
    pub fn consumer(self) -> Self {
        self.kind(SpanKind::Consumer)
    }

    /// Sets the parent context
    pub fn parent(mut self, parent: SpanContext) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Adds an attribute
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Adds a link
    pub fn link(mut self, context: SpanContext) -> Self {
        self.links.push(SpanLink {
            context,
            attributes: HashMap::new(),
        });
        self
    }

    /// Sets start time
    pub fn start_time(mut self, time: SystemTime) -> Self {
        self.start_time = Some(time);
        self
    }

    /// Builds the span
    pub fn build(self) -> Span {
        let context = if let Some(parent) = &self.parent {
            parent.child()
        } else {
            SpanContext::root()
        };

        let mut span = Span::new(self.name, context);
        span.kind = self.kind;
        span.parent_span_id = self.parent.map(|p| p.span_id);
        span.attributes = self.attributes;
        span.links = self.links;

        if let Some(start) = self.start_time {
            span.start_time = start;
        }

        span
    }

    /// Starts the span (same as build)
    pub fn start(self) -> Span {
        self.build()
    }
}

/// Active span wrapper for automatic ending
pub struct ActiveSpan {
    span: Arc<RwLock<Span>>,
    on_end: Option<Box<dyn FnOnce(Span) + Send + 'static>>,
}

impl ActiveSpan {
    /// Creates a new active span
    pub fn new(span: Span) -> Self {
        Self {
            span: Arc::new(RwLock::new(span)),
            on_end: None,
        }
    }

    /// Sets a callback for when span ends
    pub fn on_end<F>(mut self, f: F) -> Self
    where
        F: FnOnce(Span) + Send + 'static,
    {
        self.on_end = Some(Box::new(f));
        self
    }

    /// Gets the span context
    pub async fn context(&self) -> SpanContext {
        self.span.read().await.context.clone()
    }

    /// Sets an attribute
    pub async fn set_attribute(&self, key: impl Into<String>, value: impl Into<AttributeValue>) {
        self.span.write().await.set_attribute(key, value);
    }

    /// Adds an event
    pub async fn add_event(&self, event: SpanEvent) {
        self.span.write().await.add_event(event);
    }

    /// Records an exception
    pub async fn record_exception(&self, error: &dyn std::error::Error) {
        let event = SpanEvent::new("exception")
            .with_attribute("exception.type", std::any::type_name_of_val(error))
            .with_attribute("exception.message", error.to_string());

        self.span.write().await.add_event(event);
        self.span.write().await.set_status(SpanStatus::Error {
            message: error.to_string(),
        });
    }

    /// Sets status to OK
    pub async fn set_ok(&self) {
        self.span.write().await.set_status(SpanStatus::Ok);
    }

    /// Sets status to error
    pub async fn set_error(&self, message: impl Into<String>) {
        self.span.write().await.set_status(SpanStatus::Error {
            message: message.into(),
        });
    }

    /// Ends the span
    pub async fn end(self) {
        let mut span = self.span.write().await;
        span.end();

        let completed_span = span.clone();
        drop(span);

        if let Some(callback) = self.on_end {
            callback(completed_span);
        }
    }
}

/// Semantic conventions for common attributes
pub mod semantic {
    pub const SERVICE_NAME: &str = "service.name";
    pub const SERVICE_VERSION: &str = "service.version";
    pub const SERVICE_INSTANCE_ID: &str = "service.instance.id";

    pub const HTTP_METHOD: &str = "http.method";
    pub const HTTP_URL: &str = "http.url";
    pub const HTTP_STATUS_CODE: &str = "http.status_code";
    pub const HTTP_REQUEST_BODY_SIZE: &str = "http.request.body.size";
    pub const HTTP_RESPONSE_BODY_SIZE: &str = "http.response.body.size";

    pub const RPC_SYSTEM: &str = "rpc.system";
    pub const RPC_SERVICE: &str = "rpc.service";
    pub const RPC_METHOD: &str = "rpc.method";
    pub const RPC_GRPC_STATUS_CODE: &str = "rpc.grpc.status_code";

    pub const DB_SYSTEM: &str = "db.system";
    pub const DB_NAME: &str = "db.name";
    pub const DB_OPERATION: &str = "db.operation";
    pub const DB_STATEMENT: &str = "db.statement";

    pub const MESSAGING_SYSTEM: &str = "messaging.system";
    pub const MESSAGING_DESTINATION: &str = "messaging.destination";
    pub const MESSAGING_OPERATION: &str = "messaging.operation";

    pub const NET_PEER_NAME: &str = "net.peer.name";
    pub const NET_PEER_PORT: &str = "net.peer.port";
    pub const NET_TRANSPORT: &str = "net.transport";

    // Strata-specific
    pub const STRATA_BUCKET: &str = "strata.bucket";
    pub const STRATA_OBJECT_KEY: &str = "strata.object.key";
    pub const STRATA_OPERATION: &str = "strata.operation";
    pub const STRATA_CHUNK_ID: &str = "strata.chunk.id";
    pub const STRATA_NODE_ID: &str = "strata.node.id";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_creation() {
        let span = SpanBuilder::new("test-operation")
            .kind(SpanKind::Server)
            .attribute("key", "value")
            .start();

        assert_eq!(span.name, "test-operation");
        assert_eq!(span.kind, SpanKind::Server);
        assert!(span.attributes.contains_key("key"));
    }

    #[test]
    fn test_span_events() {
        let mut span = Span::new("test", SpanContext::root());

        let event = SpanEvent::new("test-event")
            .with_attribute("detail", "info");
        span.add_event(event);

        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "test-event");
    }

    #[test]
    fn test_span_status() {
        let mut span = Span::new("test", SpanContext::root());

        span.set_status(SpanStatus::Ok);
        assert_eq!(span.status, SpanStatus::Ok);

        span.set_status(SpanStatus::Error {
            message: "failed".to_string(),
        });
        match span.status {
            SpanStatus::Error { message } => assert_eq!(message, "failed"),
            _ => panic!("Expected error status"),
        }
    }

    #[test]
    fn test_span_builder_with_parent() {
        let parent = SpanContext::root();
        let child = SpanBuilder::new("child")
            .parent(parent.clone())
            .start();

        assert_eq!(child.context.trace_id, parent.trace_id);
        assert_ne!(child.context.span_id, parent.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }

    #[test]
    fn test_attribute_limits() {
        let mut span = Span::new("test", SpanContext::root());

        for i in 0..150 {
            span.set_attribute(format!("key{}", i), i as i64);
        }

        assert_eq!(span.attributes.len(), Span::MAX_ATTRIBUTES);
        assert!(span.dropped_attributes_count > 0);
    }

    #[tokio::test]
    async fn test_active_span() {
        let span = SpanBuilder::new("active-test").start();
        let active = ActiveSpan::new(span);

        active.set_attribute("test", "value").await;
        active.set_ok().await;

        let ctx = active.context().await;
        assert!(ctx.is_valid());
    }
}
