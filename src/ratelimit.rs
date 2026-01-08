//! Rate limiting for Strata services.
//!
//! Provides token bucket and sliding window rate limiters to protect services
//! from overload and ensure fair resource allocation.
//!
//! # Algorithms
//!
//! - **Token Bucket**: Allows burst traffic up to bucket capacity, then rate-limited
//! - **Sliding Window**: Tracks requests in a rolling time window
//!
//! # Configuration Presets
//!
//! | Preset | Max Requests | Window | Burst |
//! |--------|--------------|--------|-------|
//! | `strict()` | 100/s | 1s | 10 |
//! | `default()` | 1,000/s | 1s | 100 |
//! | `relaxed()` | 10,000/s | 1s | 1,000 |
//! | `disabled()` | Unlimited | - | - |
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::ratelimit::{RateLimiter, RateLimitConfig};
//!
//! let limiter = RateLimiter::new(RateLimitConfig::default());
//!
//! // Check if request is allowed
//! if limiter.try_acquire("client-123").await {
//!     // Process request
//! } else {
//!     // Return 429 Too Many Requests
//! }
//!
//! // Get current usage for a client
//! let usage = limiter.get_usage("client-123").await;
//! println!("Requests: {}/{}", usage.current, usage.limit);
//! ```
//!
//! # Per-Client Limits
//!
//! Rate limits can be applied globally or per-client for multi-tenant scenarios.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Rate limiter configuration.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window.
    pub max_requests: u64,
    /// Time window duration.
    pub window: Duration,
    /// Burst allowance (additional requests allowed temporarily).
    pub burst: u64,
    /// Enable rate limiting.
    pub enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 1000,
            window: Duration::from_secs(1),
            burst: 100,
            enabled: true,
        }
    }
}

impl RateLimitConfig {
    /// Strict rate limiting configuration.
    pub fn strict() -> Self {
        Self {
            max_requests: 100,
            window: Duration::from_secs(1),
            burst: 10,
            enabled: true,
        }
    }

    /// Relaxed rate limiting configuration.
    pub fn relaxed() -> Self {
        Self {
            max_requests: 10000,
            window: Duration::from_secs(1),
            burst: 1000,
            enabled: true,
        }
    }

    /// Disabled rate limiting.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Token bucket rate limiter.
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens (capacity).
    capacity: u64,
    /// Current available tokens.
    tokens: AtomicU64,
    /// Tokens added per second.
    refill_rate: f64,
    /// Last refill time.
    last_refill: RwLock<Instant>,
}

impl TokenBucket {
    /// Create a new token bucket.
    pub fn new(capacity: u64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_rate,
            last_refill: RwLock::new(Instant::now()),
        }
    }

    /// Create from rate limit config.
    pub fn from_config(config: &RateLimitConfig) -> Self {
        let capacity = config.max_requests + config.burst;
        let refill_rate = config.max_requests as f64 / config.window.as_secs_f64();
        Self::new(capacity, refill_rate)
    }

    /// Try to acquire a token. Returns true if successful.
    pub async fn try_acquire(&self) -> bool {
        self.try_acquire_n(1).await
    }

    /// Try to acquire N tokens. Returns true if successful.
    pub async fn try_acquire_n(&self, n: u64) -> bool {
        // Refill tokens based on elapsed time
        self.refill().await;

        // Try to acquire tokens atomically
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < n {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - n, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Get current available tokens.
    pub fn available(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Refill tokens based on elapsed time.
    async fn refill(&self) {
        let mut last_refill = self.last_refill.write().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed.as_millis() > 0 {
            let new_tokens = (elapsed.as_secs_f64() * self.refill_rate) as u64;
            if new_tokens > 0 {
                let current = self.tokens.load(Ordering::Relaxed);
                let new_value = (current + new_tokens).min(self.capacity);
                self.tokens.store(new_value, Ordering::Relaxed);
                *last_refill = now;
            }
        }
    }
}

/// Sliding window rate limiter.
#[derive(Debug)]
pub struct SlidingWindowLimiter {
    /// Window duration.
    window: Duration,
    /// Maximum requests per window.
    max_requests: u64,
    /// Request timestamps.
    requests: RwLock<Vec<Instant>>,
}

impl SlidingWindowLimiter {
    /// Create a new sliding window limiter.
    pub fn new(window: Duration, max_requests: u64) -> Self {
        Self {
            window,
            max_requests,
            requests: RwLock::new(Vec::new()),
        }
    }

    /// Create from rate limit config.
    pub fn from_config(config: &RateLimitConfig) -> Self {
        Self::new(config.window, config.max_requests)
    }

    /// Try to make a request. Returns true if allowed.
    pub async fn try_request(&self) -> bool {
        let now = Instant::now();
        let window_start = now - self.window;

        let mut requests = self.requests.write().await;

        // Remove old requests outside the window
        requests.retain(|&t| t > window_start);

        // Check if we can make a new request
        if requests.len() as u64 >= self.max_requests {
            return false;
        }

        requests.push(now);
        true
    }

    /// Get current request count in the window.
    pub async fn current_count(&self) -> u64 {
        let now = Instant::now();
        let window_start = now - self.window;

        let requests = self.requests.read().await;
        requests.iter().filter(|&&t| t > window_start).count() as u64
    }

    /// Get remaining requests allowed in the window.
    pub async fn remaining(&self) -> u64 {
        let count = self.current_count().await;
        self.max_requests.saturating_sub(count)
    }
}

/// Per-client rate limiter.
pub struct ClientRateLimiter {
    /// Rate limit configuration.
    config: RateLimitConfig,
    /// Per-client token buckets.
    clients: RwLock<HashMap<String, Arc<TokenBucket>>>,
    /// Global rate limiter.
    global: TokenBucket,
}

impl ClientRateLimiter {
    /// Create a new client rate limiter.
    pub fn new(config: RateLimitConfig) -> Self {
        let global = TokenBucket::from_config(&config);
        Self {
            config,
            clients: RwLock::new(HashMap::new()),
            global,
        }
    }

    /// Check if request is allowed for a client.
    pub async fn check(&self, client_id: &str) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult::Allowed;
        }

        // Check global limit first
        if !self.global.try_acquire().await {
            return RateLimitResult::GlobalLimitExceeded {
                retry_after: self.config.window,
            };
        }

        // Check per-client limit
        let bucket = self.get_or_create_bucket(client_id).await;
        if !bucket.try_acquire().await {
            return RateLimitResult::ClientLimitExceeded {
                client_id: client_id.to_string(),
                retry_after: self.config.window,
            };
        }

        RateLimitResult::Allowed
    }

    /// Get or create a token bucket for a client.
    async fn get_or_create_bucket(&self, client_id: &str) -> Arc<TokenBucket> {
        // Try to get existing bucket
        {
            let clients = self.clients.read().await;
            if let Some(bucket) = clients.get(client_id) {
                return Arc::clone(bucket);
            }
        }

        // Create new bucket
        let mut clients = self.clients.write().await;
        let bucket = clients
            .entry(client_id.to_string())
            .or_insert_with(|| Arc::new(TokenBucket::from_config(&self.config)));
        Arc::clone(bucket)
    }

    /// Get rate limit info for a client.
    pub async fn get_info(&self, client_id: &str) -> RateLimitInfo {
        let bucket = self.get_or_create_bucket(client_id).await;
        RateLimitInfo {
            limit: self.config.max_requests,
            remaining: bucket.available(),
            reset: self.config.window,
        }
    }

    /// Clean up inactive clients (call periodically).
    pub async fn cleanup(&self, _inactive_threshold: Duration) {
        let mut clients = self.clients.write().await;
        // In production, track last access time and remove inactive clients
        // For now, just limit the size
        if clients.len() > 10000 {
            clients.clear();
        }
    }
}

/// Result of rate limit check.
#[derive(Debug, Clone)]
pub enum RateLimitResult {
    /// Request is allowed.
    Allowed,
    /// Global rate limit exceeded.
    GlobalLimitExceeded {
        /// When to retry.
        retry_after: Duration,
    },
    /// Per-client rate limit exceeded.
    ClientLimitExceeded {
        /// Client identifier.
        client_id: String,
        /// When to retry.
        retry_after: Duration,
    },
}

impl RateLimitResult {
    /// Check if request is allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, RateLimitResult::Allowed)
    }

    /// Get retry-after duration if rate limited.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            RateLimitResult::Allowed => None,
            RateLimitResult::GlobalLimitExceeded { retry_after } => Some(*retry_after),
            RateLimitResult::ClientLimitExceeded { retry_after, .. } => Some(*retry_after),
        }
    }
}

/// Rate limit information for headers.
#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    /// Maximum requests allowed.
    pub limit: u64,
    /// Remaining requests in window.
    pub remaining: u64,
    /// Time until reset.
    pub reset: Duration,
}

/// Request cost calculator for weighted rate limiting.
pub struct CostCalculator {
    /// Base cost for any request.
    base_cost: u64,
    /// Cost multipliers by operation type.
    multipliers: HashMap<String, u64>,
}

impl CostCalculator {
    /// Create a new cost calculator.
    pub fn new(base_cost: u64) -> Self {
        Self {
            base_cost,
            multipliers: HashMap::new(),
        }
    }

    /// Set cost multiplier for an operation.
    pub fn with_multiplier(mut self, operation: impl Into<String>, multiplier: u64) -> Self {
        self.multipliers.insert(operation.into(), multiplier);
        self
    }

    /// Calculate cost for an operation.
    pub fn calculate(&self, operation: &str, size_bytes: Option<u64>) -> u64 {
        let multiplier = self.multipliers.get(operation).copied().unwrap_or(1);
        let base = self.base_cost * multiplier;

        // Add size-based cost for data operations
        if let Some(size) = size_bytes {
            let size_cost = size / (1024 * 1024); // 1 token per MB
            base + size_cost
        } else {
            base
        }
    }
}

impl Default for CostCalculator {
    fn default() -> Self {
        Self::new(1)
            .with_multiplier("read", 1)
            .with_multiplier("write", 2)
            .with_multiplier("delete", 1)
            .with_multiplier("list", 1)
            .with_multiplier("upload", 3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_acquire() {
        let bucket = TokenBucket::new(10, 10.0);

        // Should be able to acquire up to capacity
        for _ in 0..10 {
            assert!(bucket.try_acquire().await);
        }

        // Should fail after exhausting tokens
        assert!(!bucket.try_acquire().await);
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(10, 1000.0); // High refill rate for test

        // Exhaust all tokens
        for _ in 0..10 {
            bucket.try_acquire().await;
        }
        assert_eq!(bucket.available(), 0);

        // Wait a bit for refill
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Trigger refill by trying to acquire (available() doesn't trigger refill)
        assert!(bucket.try_acquire().await); // Should succeed after refill
    }

    #[tokio::test]
    async fn test_sliding_window_limiter() {
        let limiter = SlidingWindowLimiter::new(Duration::from_millis(100), 5);

        // Should allow up to max_requests
        for _ in 0..5 {
            assert!(limiter.try_request().await);
        }

        // Should reject additional requests
        assert!(!limiter.try_request().await);

        // Wait for window to slide
        tokio::time::sleep(Duration::from_millis(110)).await;

        // Should allow requests again
        assert!(limiter.try_request().await);
    }

    #[tokio::test]
    async fn test_client_rate_limiter() {
        let config = RateLimitConfig {
            max_requests: 10, // Higher global limit to allow both clients
            window: Duration::from_secs(1),
            burst: 5,
            enabled: true,
        };
        let limiter = ClientRateLimiter::new(config);

        // Use 5 requests for client1 (less than their per-client limit)
        for _ in 0..5 {
            assert!(limiter.check("client1").await.is_allowed());
        }

        // Client 2 still has quota (per-client limit is separate)
        for _ in 0..5 {
            assert!(limiter.check("client2").await.is_allowed());
        }

        // Both clients should still have some quota left with burst
        assert!(limiter.check("client1").await.is_allowed());
    }

    #[tokio::test]
    async fn test_rate_limit_disabled() {
        let config = RateLimitConfig::disabled();
        let limiter = ClientRateLimiter::new(config);

        // Should always allow when disabled
        for _ in 0..100 {
            assert!(limiter.check("client").await.is_allowed());
        }
    }

    #[tokio::test]
    async fn test_rate_limit_info() {
        let config = RateLimitConfig {
            max_requests: 10,
            window: Duration::from_secs(1),
            burst: 5,
            enabled: true,
        };
        let limiter = ClientRateLimiter::new(config);

        let info = limiter.get_info("client").await;
        assert_eq!(info.limit, 10);
        assert!(info.remaining > 0);
    }

    #[tokio::test]
    async fn test_cost_calculator() {
        let calc = CostCalculator::default();

        assert_eq!(calc.calculate("read", None), 1);
        assert_eq!(calc.calculate("write", None), 2);
        assert_eq!(calc.calculate("upload", None), 3);

        // With size
        assert_eq!(calc.calculate("upload", Some(5 * 1024 * 1024)), 8); // 3 + 5MB
    }

    #[test]
    fn test_rate_limit_result() {
        let allowed = RateLimitResult::Allowed;
        assert!(allowed.is_allowed());
        assert!(allowed.retry_after().is_none());

        let limited = RateLimitResult::GlobalLimitExceeded {
            retry_after: Duration::from_secs(1),
        };
        assert!(!limited.is_allowed());
        assert_eq!(limited.retry_after(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_config_presets() {
        let strict = RateLimitConfig::strict();
        assert_eq!(strict.max_requests, 100);

        let relaxed = RateLimitConfig::relaxed();
        assert_eq!(relaxed.max_requests, 10000);

        let disabled = RateLimitConfig::disabled();
        assert!(!disabled.enabled);
    }
}
