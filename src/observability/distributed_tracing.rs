//! Distributed tracing support for Strata.
//!
//! This module provides utilities for creating and propagating trace contexts
//! across service boundaries for distributed request tracing.

use axum::http;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Trace ID for correlating requests across services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(u128);

impl TraceId {
    /// Generate a new trace ID.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let count = COUNTER.fetch_add(1, Ordering::Relaxed);

        // Combine timestamp with counter for uniqueness
        let id = ((timestamp as u128) << 64) | (count as u128);
        Self(id)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> Option<Self> {
        u128::from_str_radix(s, 16).ok().map(Self)
    }

    /// Convert to hex string.
    pub fn to_hex(&self) -> String {
        format!("{:032x}", self.0)
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Span ID for identifying a specific operation within a trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(u64);

impl SpanId {
    /// Generate a new span ID.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u32;

        let count = COUNTER.fetch_add(1, Ordering::Relaxed) as u32;

        let id = ((timestamp as u64) << 32) | (count as u64);
        Self(id)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> Option<Self> {
        u64::from_str_radix(s, 16).ok().map(Self)
    }

    /// Convert to hex string.
    pub fn to_hex(&self) -> String {
        format!("{:016x}", self.0)
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Trace context for propagating across service boundaries.
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// The trace ID.
    pub trace_id: TraceId,
    /// The span ID of the current span.
    pub span_id: SpanId,
    /// The parent span ID, if any.
    pub parent_span_id: Option<SpanId>,
    /// Whether the trace is sampled.
    pub sampled: bool,
    /// Baggage items (key-value pairs propagated with the trace).
    pub baggage: HashMap<String, String>,
}

impl TraceContext {
    /// Create a new trace context (starts a new trace).
    pub fn new() -> Self {
        Self {
            trace_id: TraceId::new(),
            span_id: SpanId::new(),
            parent_span_id: None,
            sampled: true,
            baggage: HashMap::new(),
        }
    }

    /// Create a child span context.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::new(),
            parent_span_id: Some(self.span_id),
            sampled: self.sampled,
            baggage: self.baggage.clone(),
        }
    }

    /// Add a baggage item.
    pub fn with_baggage(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.baggage.insert(key.into(), value.into());
        self
    }

    /// Set sampling decision.
    pub fn with_sampled(mut self, sampled: bool) -> Self {
        self.sampled = sampled;
        self
    }

    /// Extract from HTTP headers.
    pub fn from_headers(headers: &http::HeaderMap) -> Option<Self> {
        // Support W3C Trace Context format
        let traceparent = headers
            .get("traceparent")
            .and_then(|v| v.to_str().ok())?;

        Self::from_traceparent(traceparent)
    }

    /// Parse W3C traceparent header.
    fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() < 4 {
            return None;
        }

        let _version = parts[0]; // Currently always "00"
        let trace_id = TraceId::from_hex(parts[1])?;
        let parent_span_id = SpanId::from_hex(parts[2])?;
        let flags = u8::from_str_radix(parts[3], 16).ok()?;
        let sampled = (flags & 0x01) != 0;

        Some(Self {
            trace_id,
            span_id: SpanId::new(),
            parent_span_id: Some(parent_span_id),
            sampled,
            baggage: HashMap::new(),
        })
    }

    /// Convert to W3C traceparent header value.
    pub fn to_traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!(
            "00-{}-{}-{}",
            self.trace_id.to_hex(),
            self.span_id.to_hex(),
            flags
        )
    }

    /// Inject into HTTP headers.
    pub fn inject_headers(&self, headers: &mut http::HeaderMap) {
        if let Ok(value) = self.to_traceparent().parse() {
            headers.insert("traceparent", value);
        }

        // Inject baggage
        if !self.baggage.is_empty() {
            let baggage_value: String = self
                .baggage
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",");

            if let Ok(value) = baggage_value.parse() {
                headers.insert("baggage", value);
            }
        }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// A span representing a unit of work.
pub struct Span {
    /// The trace context.
    context: TraceContext,
    /// Span name/operation.
    name: String,
    /// Start time.
    start_time: SystemTime,
    /// Span attributes.
    attributes: HashMap<String, SpanValue>,
    /// Span events.
    events: Vec<SpanEvent>,
    /// Span status.
    status: SpanStatus,
}

/// Values that can be attached to spans.
#[derive(Debug, Clone)]
pub enum SpanValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl From<&str> for SpanValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<String> for SpanValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<i64> for SpanValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for SpanValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for SpanValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

/// An event that occurred during a span.
#[derive(Debug, Clone)]
pub struct SpanEvent {
    /// Event name.
    pub name: String,
    /// Event timestamp.
    pub timestamp: SystemTime,
    /// Event attributes.
    pub attributes: HashMap<String, SpanValue>,
}

/// Span completion status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    /// Span completed successfully.
    Ok,
    /// Span completed with an error.
    Error,
    /// Span status is unset.
    Unset,
}

impl Default for SpanStatus {
    fn default() -> Self {
        Self::Unset
    }
}

impl Span {
    /// Create a new span.
    pub fn new(name: impl Into<String>, context: TraceContext) -> Self {
        Self {
            context,
            name: name.into(),
            start_time: SystemTime::now(),
            attributes: HashMap::new(),
            events: Vec::new(),
            status: SpanStatus::Unset,
        }
    }

    /// Create a root span (starts a new trace).
    pub fn root(name: impl Into<String>) -> Self {
        Self::new(name, TraceContext::new())
    }

    /// Get the trace context.
    pub fn context(&self) -> &TraceContext {
        &self.context
    }

    /// Set an attribute.
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<SpanValue>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Add an event.
    pub fn add_event(&mut self, name: impl Into<String>) {
        self.events.push(SpanEvent {
            name: name.into(),
            timestamp: SystemTime::now(),
            attributes: HashMap::new(),
        });
    }

    /// Add an event with attributes.
    pub fn add_event_with_attributes(
        &mut self,
        name: impl Into<String>,
        attributes: HashMap<String, SpanValue>,
    ) {
        self.events.push(SpanEvent {
            name: name.into(),
            timestamp: SystemTime::now(),
            attributes,
        });
    }

    /// Set span status.
    pub fn set_status(&mut self, status: SpanStatus) {
        self.status = status;
    }

    /// Record an error.
    pub fn record_error(&mut self, error: &dyn std::error::Error) {
        self.set_status(SpanStatus::Error);
        self.set_attribute("error.type", std::any::type_name_of_val(error));
        self.set_attribute("error.message", error.to_string());
    }

    /// Get span duration.
    pub fn duration(&self) -> std::time::Duration {
        self.start_time.elapsed().unwrap_or_default()
    }

    /// Create a child span.
    pub fn child(&self, name: impl Into<String>) -> Self {
        Self::new(name, self.context.child())
    }

    /// Get the trace ID.
    pub fn trace_id(&self) -> TraceId {
        self.context.trace_id
    }

    /// Get the span ID.
    pub fn span_id(&self) -> SpanId {
        self.context.span_id
    }

    /// Get the span name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        // Log span completion for debugging
        let duration = self.duration();
        tracing::debug!(
            trace_id = %self.context.trace_id,
            span_id = %self.context.span_id,
            parent_span_id = ?self.context.parent_span_id.map(|s| s.to_hex()),
            name = %self.name,
            duration_ms = duration.as_millis() as u64,
            status = ?self.status,
            "Span completed"
        );
    }
}

/// Trait for types that can provide trace context.
pub trait Traceable {
    /// Get the trace context.
    fn trace_context(&self) -> Option<&TraceContext>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_generation() {
        let id1 = TraceId::new();
        let id2 = TraceId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_trace_id_hex() {
        let id = TraceId::new();
        let hex = id.to_hex();
        assert_eq!(hex.len(), 32);

        let parsed = TraceId::from_hex(&hex);
        assert_eq!(parsed, Some(id));
    }

    #[test]
    fn test_span_id_generation() {
        let id1 = SpanId::new();
        let id2 = SpanId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_span_id_hex() {
        let id = SpanId::new();
        let hex = id.to_hex();
        assert_eq!(hex.len(), 16);

        let parsed = SpanId::from_hex(&hex);
        assert_eq!(parsed, Some(id));
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.child();

        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx = TraceContext::new().with_sampled(true);
        let header = ctx.to_traceparent();

        let parsed = TraceContext::from_traceparent(&header);
        assert!(parsed.is_some());

        let parsed = parsed.unwrap();
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.parent_span_id, Some(ctx.span_id));
        assert!(parsed.sampled);
    }

    #[test]
    fn test_span_creation() {
        let span = Span::root("test-operation");
        assert_eq!(span.name(), "test-operation");
    }

    #[test]
    fn test_span_attributes() {
        let mut span = Span::root("test");
        span.set_attribute("key", "value");
        span.set_attribute("count", 42i64);
        span.set_attribute("enabled", true);
    }

    #[test]
    fn test_span_events() {
        let mut span = Span::root("test");
        span.add_event("something happened");
        assert_eq!(span.events.len(), 1);
    }

    #[test]
    fn test_span_child() {
        let parent = Span::root("parent");
        let child = parent.child("child");

        assert_eq!(parent.trace_id(), child.trace_id());
        assert_ne!(parent.span_id(), child.span_id());
    }

    #[test]
    fn test_baggage() {
        let ctx = TraceContext::new()
            .with_baggage("user_id", "123")
            .with_baggage("tenant", "acme");

        assert_eq!(ctx.baggage.get("user_id"), Some(&"123".to_string()));
        assert_eq!(ctx.baggage.get("tenant"), Some(&"acme".to_string()));
    }
}
