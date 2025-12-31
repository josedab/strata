// Trace context propagation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// Inline URL encoding/decoding to avoid external dependency
mod url_util {
    pub fn encode(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        for byte in s.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(byte as char);
                }
                _ => {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
        result
    }

    pub fn decode(s: &str) -> Result<String, ()> {
        let mut result = Vec::new();
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let high = hex_val(bytes[i + 1])?;
                let low = hex_val(bytes[i + 2])?;
                result.push((high << 4) | low);
                i += 3;
            } else if bytes[i] == b'+' {
                result.push(b' ');
                i += 1;
            } else {
                result.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(result).map_err(|_| ())
    }

    fn hex_val(c: u8) -> Result<u8, ()> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(()),
        }
    }
}

// Inline hex encoding/decoding to avoid external dependency
mod hex_util {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: &[u8]) -> String {
        let mut result = String::with_capacity(bytes.len() * 2);
        for &byte in bytes {
            result.push(HEX_CHARS[(byte >> 4) as usize] as char);
            result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
        }
        result
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if s.len() % 2 != 0 {
            return Err(());
        }
        let mut result = Vec::with_capacity(s.len() / 2);
        let chars: Vec<char> = s.chars().collect();
        for i in (0..chars.len()).step_by(2) {
            let high = hex_val(chars[i])?;
            let low = hex_val(chars[i + 1])?;
            result.push((high << 4) | low);
        }
        Ok(result)
    }

    fn hex_val(c: char) -> Result<u8, ()> {
        match c {
            '0'..='9' => Ok(c as u8 - b'0'),
            'a'..='f' => Ok(c as u8 - b'a' + 10),
            'A'..='F' => Ok(c as u8 - b'A' + 10),
            _ => Err(()),
        }
    }
}

/// Trace ID (128-bit)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    /// Creates a new random trace ID
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }

    /// Creates a trace ID from bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Converts to hex string
    pub fn to_hex(&self) -> String {
        hex_util::encode(&self.0)
    }

    /// Parses from hex string
    pub fn from_hex(s: &str) -> Option<Self> {
        let bytes = hex_util::decode(s).ok()?;
        if bytes.len() != 16 {
            return None;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
    }

    /// Checks if trace ID is valid (non-zero)
    pub fn is_valid(&self) -> bool {
        self.0 != [0u8; 16]
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Span ID (64-bit)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId(pub [u8; 8]);

impl SpanId {
    /// Creates a new random span ID
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 8];
        rng.fill(&mut bytes);
        Self(bytes)
    }

    /// Creates a span ID from bytes
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Converts to hex string
    pub fn to_hex(&self) -> String {
        hex_util::encode(&self.0)
    }

    /// Parses from hex string
    pub fn from_hex(s: &str) -> Option<Self> {
        let bytes = hex_util::decode(s).ok()?;
        if bytes.len() != 8 {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
    }

    /// Checks if span ID is valid (non-zero)
    pub fn is_valid(&self) -> bool {
        self.0 != [0u8; 8]
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Trace flags
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TraceFlags(pub u8);

impl TraceFlags {
    /// Empty flags
    pub const NONE: Self = Self(0x00);
    /// Trace is sampled
    pub const SAMPLED: Self = Self(0x01);

    /// Checks if sampled flag is set
    pub fn is_sampled(&self) -> bool {
        self.0 & 0x01 != 0
    }

    /// Sets the sampled flag
    pub fn with_sampled(mut self, sampled: bool) -> Self {
        if sampled {
            self.0 |= 0x01;
        } else {
            self.0 &= !0x01;
        }
        self
    }
}

/// Span context for propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    /// Trace ID
    pub trace_id: TraceId,
    /// Span ID
    pub span_id: SpanId,
    /// Trace flags
    pub trace_flags: TraceFlags,
    /// Trace state (vendor-specific)
    pub trace_state: TraceState,
    /// Is remote (received from another service)
    pub is_remote: bool,
}

impl SpanContext {
    /// Creates a new span context
    pub fn new(trace_id: TraceId, span_id: SpanId) -> Self {
        Self {
            trace_id,
            span_id,
            trace_flags: TraceFlags::NONE,
            trace_state: TraceState::default(),
            is_remote: false,
        }
    }

    /// Creates a new root span context
    pub fn root() -> Self {
        Self::new(TraceId::new(), SpanId::new())
    }

    /// Creates a child span context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::new(),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
            is_remote: false,
        }
    }

    /// Checks if context is valid
    pub fn is_valid(&self) -> bool {
        self.trace_id.is_valid() && self.span_id.is_valid()
    }

    /// Checks if sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags.is_sampled()
    }

    /// Sets sampled flag
    pub fn with_sampled(mut self, sampled: bool) -> Self {
        self.trace_flags = self.trace_flags.with_sampled(sampled);
        self
    }

    /// Converts to W3C traceparent header value
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id.to_hex(),
            self.span_id.to_hex(),
            self.trace_flags.0
        )
    }

    /// Parses from W3C traceparent header value
    pub fn from_traceparent(value: &str) -> Option<Self> {
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }

        let trace_id = TraceId::from_hex(parts[1])?;
        let span_id = SpanId::from_hex(parts[2])?;
        let flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id,
            span_id,
            trace_flags: TraceFlags(flags),
            trace_state: TraceState::default(),
            is_remote: true,
        })
    }
}

/// Trace state for vendor-specific data
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TraceState {
    entries: Vec<(String, String)>,
}

impl TraceState {
    /// Maximum entries in trace state
    pub const MAX_ENTRIES: usize = 32;

    /// Creates empty trace state
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Gets a value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Sets a value
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        // Remove existing if present
        self.entries.retain(|(k, _)| k != &key);

        // Add new entry at front (most recent)
        self.entries.insert(0, (key, value));

        // Trim to max size
        self.entries.truncate(Self::MAX_ENTRIES);
    }

    /// Removes a value
    pub fn remove(&mut self, key: &str) {
        self.entries.retain(|(k, _)| k != key);
    }

    /// Converts to tracestate header value
    pub fn to_header(&self) -> String {
        self.entries.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parses from tracestate header value
    pub fn from_header(value: &str) -> Self {
        let entries: Vec<(String, String)> = value
            .split(',')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?.trim().to_string();
                let value = parts.next()?.trim().to_string();
                if key.is_empty() || value.is_empty() {
                    None
                } else {
                    Some((key, value))
                }
            })
            .take(Self::MAX_ENTRIES)
            .collect();

        Self { entries }
    }
}

/// Baggage for cross-service context propagation
#[derive(Debug, Clone, Default)]
pub struct Baggage {
    items: HashMap<String, String>,
}

impl Baggage {
    /// Maximum items in baggage
    pub const MAX_ITEMS: usize = 180;
    /// Maximum item size
    pub const MAX_ITEM_SIZE: usize = 4096;

    /// Creates empty baggage
    pub fn new() -> Self {
        Self { items: HashMap::new() }
    }

    /// Gets a value
    pub fn get(&self, key: &str) -> Option<&str> {
        self.items.get(key).map(|s| s.as_str())
    }

    /// Sets a value
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        if key.len() + value.len() > Self::MAX_ITEM_SIZE {
            return;
        }

        self.items.insert(key, value);

        // Trim if too many items
        while self.items.len() > Self::MAX_ITEMS {
            if let Some(key) = self.items.keys().next().cloned() {
                self.items.remove(&key);
            }
        }
    }

    /// Removes a value
    pub fn remove(&mut self, key: &str) {
        self.items.remove(key);
    }

    /// Iterates over items
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.items.iter()
    }

    /// Converts to baggage header value
    pub fn to_header(&self) -> String {
        self.items.iter()
            .map(|(k, v)| format!("{}={}", url_util::encode(k), url_util::encode(v)))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parses from baggage header value
    pub fn from_header(value: &str) -> Self {
        let items: HashMap<String, String> = value
            .split(',')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?;
                let value = parts.next()?;
                let key = url_util::decode(key.trim()).ok()?;
                let value = url_util::decode(value.trim()).ok()?;
                Some((key, value))
            })
            .take(Self::MAX_ITEMS)
            .collect();

        Self { items }
    }
}

/// Trace context holder for current execution
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// Current span context
    pub span: SpanContext,
    /// Parent span ID
    pub parent_span_id: Option<SpanId>,
    /// Baggage
    pub baggage: Baggage,
}

impl TraceContext {
    /// Creates a new root trace context
    pub fn root() -> Self {
        Self {
            span: SpanContext::root(),
            parent_span_id: None,
            baggage: Baggage::new(),
        }
    }

    /// Creates a child trace context
    pub fn child(&self) -> Self {
        Self {
            span: self.span.child(),
            parent_span_id: Some(self.span.span_id),
            baggage: self.baggage.clone(),
        }
    }

    /// Continues from a remote context
    pub fn from_remote(span: SpanContext) -> Self {
        Self {
            span,
            parent_span_id: None,
            baggage: Baggage::new(),
        }
    }
}

// Thread-local context storage
tokio::task_local! {
    static CURRENT_CONTEXT: Arc<RwLock<Option<TraceContext>>>;
}

/// Gets the current trace context
pub async fn current_context() -> Option<TraceContext> {
    CURRENT_CONTEXT.try_with(|ctx| {
        let guard = futures::executor::block_on(ctx.read());
        guard.clone()
    }).ok().flatten()
}

/// Sets the current trace context
pub async fn set_context(ctx: TraceContext) {
    CURRENT_CONTEXT.try_with(|current| {
        let mut guard = futures::executor::block_on(current.write());
        *guard = Some(ctx);
    }).ok();
}

/// Context guard for scope-based context management
pub struct ContextGuard {
    previous: Option<TraceContext>,
}

impl ContextGuard {
    /// Creates a new context guard
    pub async fn new(ctx: TraceContext) -> Self {
        let previous = current_context().await;
        set_context(ctx).await;
        Self { previous }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        if let Some(ctx) = self.previous.take() {
            // Can't use async in drop, but we try our best
            let _ = CURRENT_CONTEXT.try_with(|current| {
                if let Ok(mut guard) = current.try_write() {
                    *guard = Some(ctx);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id() {
        let id = TraceId::new();
        assert!(id.is_valid());

        let hex = id.to_hex();
        assert_eq!(hex.len(), 32);

        let parsed = TraceId::from_hex(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_span_id() {
        let id = SpanId::new();
        assert!(id.is_valid());

        let hex = id.to_hex();
        assert_eq!(hex.len(), 16);

        let parsed = SpanId::from_hex(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_trace_flags() {
        let flags = TraceFlags::NONE;
        assert!(!flags.is_sampled());

        let sampled = flags.with_sampled(true);
        assert!(sampled.is_sampled());
    }

    #[test]
    fn test_traceparent() {
        let ctx = SpanContext::root().with_sampled(true);
        let header = ctx.to_traceparent();

        let parsed = SpanContext::from_traceparent(&header).unwrap();
        assert_eq!(ctx.trace_id, parsed.trace_id);
        assert_eq!(ctx.span_id, parsed.span_id);
        assert!(parsed.is_sampled());
        assert!(parsed.is_remote);
    }

    #[test]
    fn test_trace_state() {
        let mut state = TraceState::new();
        state.set("vendor1", "value1");
        state.set("vendor2", "value2");

        assert_eq!(state.get("vendor1"), Some("value1"));
        assert_eq!(state.get("vendor2"), Some("value2"));

        let header = state.to_header();
        let parsed = TraceState::from_header(&header);

        assert_eq!(parsed.get("vendor1"), Some("value1"));
        assert_eq!(parsed.get("vendor2"), Some("value2"));
    }

    #[test]
    fn test_baggage() {
        let mut baggage = Baggage::new();
        baggage.set("user_id", "12345");
        baggage.set("session", "abc");

        assert_eq!(baggage.get("user_id"), Some("12345"));

        let header = baggage.to_header();
        let parsed = Baggage::from_header(&header);

        assert_eq!(parsed.get("user_id"), Some("12345"));
        assert_eq!(parsed.get("session"), Some("abc"));
    }

    #[test]
    fn test_child_context() {
        let root = TraceContext::root();
        let child = root.child();

        assert_eq!(root.span.trace_id, child.span.trace_id);
        assert_ne!(root.span.span_id, child.span.span_id);
        assert_eq!(child.parent_span_id, Some(root.span.span_id));
    }
}
