//! Resilience patterns for Strata services.
//!
//! Provides circuit breaker, retry logic with exponential backoff,
//! and other resilience mechanisms.

use crate::error::{Result, StrataError};
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;

// ============================================================================
// Circuit Breaker
// ============================================================================

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, requests flow normally.
    Closed,
    /// Circuit is open, requests fail immediately.
    Open,
    /// Circuit is half-open, allowing test requests.
    HalfOpen,
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit.
    pub failure_threshold: u64,
    /// Duration to wait before attempting to close the circuit.
    pub reset_timeout: Duration,
    /// Number of successful requests needed to close the circuit.
    pub success_threshold: u64,
    /// Time window for counting failures.
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold: 3,
            failure_window: Duration::from_secs(60),
        }
    }
}

impl CircuitBreakerConfig {
    /// Aggressive circuit breaker (opens quickly).
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
            success_threshold: 5,
            failure_window: Duration::from_secs(30),
        }
    }

    /// Lenient circuit breaker (more tolerant of failures).
    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            reset_timeout: Duration::from_secs(15),
            success_threshold: 2,
            failure_window: Duration::from_secs(120),
        }
    }
}

/// Circuit breaker for protecting against cascading failures.
pub struct CircuitBreaker {
    /// Configuration.
    config: CircuitBreakerConfig,
    /// Current state.
    state: RwLock<CircuitState>,
    /// Failure count in current window.
    failure_count: AtomicU64,
    /// Success count in half-open state.
    success_count: AtomicU64,
    /// When the circuit was opened.
    opened_at: RwLock<Option<Instant>>,
    /// Last failure time.
    last_failure: RwLock<Option<Instant>>,
    /// Name for logging.
    name: String,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    pub fn new(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            opened_at: RwLock::new(None),
            last_failure: RwLock::new(None),
            name: name.into(),
        }
    }

    /// Get current state.
    pub async fn state(&self) -> CircuitState {
        self.maybe_transition().await;
        *self.state.read().await
    }

    /// Check if requests should be allowed.
    pub async fn should_allow(&self) -> bool {
        let state = self.state().await;
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => true, // Allow test request
        }
    }

    /// Record a successful request.
    pub async fn record_success(&self) {
        let mut state = self.state.write().await;

        match *state {
            CircuitState::Closed => {
                // Reset failure count on success in closed state
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.config.success_threshold {
                    // Enough successes, close the circuit
                    *state = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                    *self.opened_at.write().await = None;
                    tracing::info!(circuit = %self.name, "Circuit closed after successful recovery");
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
            }
        }
    }

    /// Record a failed request.
    pub async fn record_failure(&self) {
        let mut state = self.state.write().await;
        *self.last_failure.write().await = Some(Instant::now());

        match *state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.config.failure_threshold {
                    // Too many failures, open the circuit
                    *state = CircuitState::Open;
                    *self.opened_at.write().await = Some(Instant::now());
                    tracing::warn!(
                        circuit = %self.name,
                        failures = count,
                        "Circuit opened due to failures"
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Failed during recovery, go back to open
                *state = CircuitState::Open;
                *self.opened_at.write().await = Some(Instant::now());
                self.success_count.store(0, Ordering::Relaxed);
                tracing::warn!(circuit = %self.name, "Circuit re-opened after half-open failure");
            }
            CircuitState::Open => {
                // Already open, just update opened_at to extend timeout
            }
        }
    }

    /// Check for state transitions.
    async fn maybe_transition(&self) {
        let mut state = self.state.write().await;

        // Check if we should transition from Open to HalfOpen
        if *state == CircuitState::Open {
            if let Some(opened_at) = *self.opened_at.read().await {
                if opened_at.elapsed() >= self.config.reset_timeout {
                    *state = CircuitState::HalfOpen;
                    self.success_count.store(0, Ordering::Relaxed);
                    tracing::info!(circuit = %self.name, "Circuit transitioning to half-open");
                }
            }
        }

        // Check if failures have aged out
        if *state == CircuitState::Closed {
            if let Some(last_failure) = *self.last_failure.read().await {
                if last_failure.elapsed() >= self.config.failure_window {
                    self.failure_count.store(0, Ordering::Relaxed);
                }
            }
        }
    }

    /// Execute a function with circuit breaker protection.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if !self.should_allow().await {
            return Err(StrataError::ConnectionFailed(format!(
                "Circuit breaker '{}' is open",
                self.name
            )));
        }

        match f().await {
            Ok(result) => {
                self.record_success().await;
                Ok(result)
            }
            Err(e) => {
                if e.is_retryable() {
                    self.record_failure().await;
                }
                Err(e)
            }
        }
    }

    /// Get circuit breaker statistics.
    pub async fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            name: self.name.clone(),
            state: self.state().await,
            failure_count: self.failure_count.load(Ordering::Relaxed),
            success_count: self.success_count.load(Ordering::Relaxed),
        }
    }
}

/// Circuit breaker statistics.
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    /// Circuit breaker name.
    pub name: String,
    /// Current state.
    pub state: CircuitState,
    /// Current failure count.
    pub failure_count: u64,
    /// Success count (in half-open state).
    pub success_count: u64,
}

// ============================================================================
// Retry with Exponential Backoff
// ============================================================================

/// Retry configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Initial delay before first retry.
    pub initial_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Multiplier for exponential backoff.
    pub multiplier: f64,
    /// Add jitter to delays.
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Quick retry configuration.
    pub fn quick() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: true,
        }
    }

    /// Patient retry configuration.
    pub fn patient() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }

    /// Calculate delay for a given attempt.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay =
            self.initial_delay.as_secs_f64() * self.multiplier.powi(attempt as i32 - 1);
        let delay = Duration::from_secs_f64(base_delay.min(self.max_delay.as_secs_f64()));

        if self.jitter {
            // Add up to 25% jitter
            let jitter_factor = 1.0 + (rand_jitter() * 0.25);
            Duration::from_secs_f64(delay.as_secs_f64() * jitter_factor)
        } else {
            delay
        }
    }
}

/// Generate random jitter factor (0.0 to 1.0).
fn rand_jitter() -> f64 {
    // Simple pseudo-random based on time
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

/// Retry executor with exponential backoff.
pub struct RetryExecutor {
    config: RetryConfig,
}

impl RetryExecutor {
    /// Create a new retry executor.
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Execute a function with retries.
    pub async fn execute<F, Fut, T>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.config.max_attempts {
            attempt += 1;

            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }

                    last_error = Some(e);

                    if attempt < self.config.max_attempts {
                        let delay = self.config.delay_for_attempt(attempt);
                        tracing::debug!(
                            attempt = attempt,
                            max_attempts = self.config.max_attempts,
                            delay_ms = delay.as_millis(),
                            "Retrying after failure"
                        );
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StrataError::Internal("Retry exhausted without error".to_string())
        }))
    }

    /// Execute with circuit breaker.
    pub async fn execute_with_circuit_breaker<F, Fut, T>(
        &self,
        circuit_breaker: &CircuitBreaker,
        mut f: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.config.max_attempts {
            attempt += 1;

            match circuit_breaker.call(&mut f).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }

                    last_error = Some(e);

                    if attempt < self.config.max_attempts {
                        let delay = self.config.delay_for_attempt(attempt);
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StrataError::Internal("Retry exhausted without error".to_string())
        }))
    }
}

// ============================================================================
// Bulkhead (Concurrency Limiter)
// ============================================================================

/// Bulkhead for limiting concurrent operations.
pub struct Bulkhead {
    /// Maximum concurrent operations.
    max_concurrent: usize,
    /// Current concurrent operations.
    current: AtomicUsize,
    /// Name for logging.
    name: String,
}

impl Bulkhead {
    /// Create a new bulkhead.
    pub fn new(name: impl Into<String>, max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            current: AtomicUsize::new(0),
            name: name.into(),
        }
    }

    /// Try to acquire a permit.
    pub fn try_acquire(&self) -> Option<BulkheadPermit<'_>> {
        loop {
            let current = self.current.load(Ordering::Relaxed);
            if current >= self.max_concurrent {
                return None;
            }
            if self
                .current
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(BulkheadPermit { bulkhead: self });
            }
        }
    }

    /// Get current concurrent operations.
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Get available permits.
    pub fn available(&self) -> usize {
        self.max_concurrent.saturating_sub(self.current())
    }

    /// Execute with bulkhead protection.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let _permit = self.try_acquire().ok_or_else(|| {
            StrataError::ConnectionFailed(format!("Bulkhead '{}' is full", self.name))
        })?;

        f().await
    }
}

/// Permit for a bulkhead slot.
pub struct BulkheadPermit<'a> {
    bulkhead: &'a Bulkhead,
}

impl Drop for BulkheadPermit<'_> {
    fn drop(&mut self) {
        self.bulkhead.current.fetch_sub(1, Ordering::Relaxed);
    }
}

// ============================================================================
// Timeout Wrapper
// ============================================================================

/// Execute an operation with a timeout.
pub async fn with_timeout<F, Fut, T>(timeout: Duration, f: F) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    tokio::time::timeout(timeout, f())
        .await
        .map_err(|_| StrataError::Timeout(timeout.as_millis() as u64))?
}

// ============================================================================
// Resilient Client Builder
// ============================================================================

/// Builder for creating a resilient client wrapper.
pub struct ResilientClient {
    /// Circuit breaker.
    pub circuit_breaker: Arc<CircuitBreaker>,
    /// Retry executor.
    pub retry: RetryExecutor,
    /// Bulkhead for concurrency limiting.
    pub bulkhead: Arc<Bulkhead>,
    /// Default timeout.
    pub timeout: Duration,
}

impl ResilientClient {
    /// Create a new resilient client.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            circuit_breaker: Arc::new(CircuitBreaker::new(
                format!("{}-circuit", name),
                CircuitBreakerConfig::default(),
            )),
            retry: RetryExecutor::new(RetryConfig::default()),
            bulkhead: Arc::new(Bulkhead::new(format!("{}-bulkhead", name), 100)),
            timeout: Duration::from_secs(30),
        }
    }

    /// Set circuit breaker config.
    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig, name: &str) -> Self {
        self.circuit_breaker = Arc::new(CircuitBreaker::new(name, config));
        self
    }

    /// Set retry config.
    pub fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry = RetryExecutor::new(config);
        self
    }

    /// Set bulkhead config.
    pub fn with_bulkhead(mut self, max_concurrent: usize, name: &str) -> Self {
        self.bulkhead = Arc::new(Bulkhead::new(name, max_concurrent));
        self
    }

    /// Set timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Execute an operation with all resilience patterns.
    pub async fn execute<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnMut() -> Fut + Clone,
        Fut: Future<Output = Result<T>>,
    {
        let timeout = self.timeout;
        let circuit_breaker = Arc::clone(&self.circuit_breaker);
        let bulkhead = Arc::clone(&self.bulkhead);

        // Bulkhead -> Circuit Breaker -> Retry -> Timeout -> Operation
        bulkhead
            .call(|| async {
                self.retry
                    .execute_with_circuit_breaker(&circuit_breaker, || {
                        let mut f = f.clone();
                        async move { with_timeout(timeout, || f()).await }
                    })
                    .await
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());

        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.should_allow().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Record failures
        for _ in 0..3 {
            cb.record_failure().await;
        }

        assert_eq!(cb.state().await, CircuitState::Open);
        assert!(!cb.should_allow().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_millis(50),
            success_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Open the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Wait for reset timeout
        sleep(Duration::from_millis(60)).await;

        // Should be half-open
        assert_eq!(cb.state().await, CircuitState::HalfOpen);
        assert!(cb.should_allow().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_millis(10),
            success_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Open the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Wait for reset timeout and trigger transition to half-open
        sleep(Duration::from_millis(20)).await;
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        // Record successes in half-open to close
        cb.record_success().await;
        cb.record_success().await;

        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_retry_success() {
        let retry = RetryExecutor::new(RetryConfig::default());
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<u64> = retry
            .execute(|| {
                let counter = Arc::clone(&counter_clone);
                async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_eventual_success() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            jitter: false,
            ..Default::default()
        };
        let retry = RetryExecutor::new(config);
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<u64> = retry
            .execute(|| {
                let counter = Arc::clone(&counter_clone);
                async move {
                    let count = counter.fetch_add(1, Ordering::Relaxed);
                    if count < 2 {
                        Err(StrataError::Timeout(100))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let config = RetryConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        };
        let retry = RetryExecutor::new(config);

        let result: Result<()> = retry
            .execute(|| async { Err(StrataError::Timeout(100)) })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bulkhead() {
        let bulkhead = Bulkhead::new("test", 2);

        let permit1 = bulkhead.try_acquire();
        let permit2 = bulkhead.try_acquire();
        let permit3 = bulkhead.try_acquire();

        assert!(permit1.is_some());
        assert!(permit2.is_some());
        assert!(permit3.is_none()); // Full

        drop(permit1);
        let permit4 = bulkhead.try_acquire();
        assert!(permit4.is_some()); // Slot freed
    }

    #[tokio::test]
    async fn test_retry_delay_calculation() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };

        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));
    }

    #[tokio::test]
    async fn test_timeout_wrapper() {
        // Success within timeout
        let result: Result<u32> =
            with_timeout(Duration::from_secs(1), || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);

        // Timeout exceeded
        let result: Result<u32> = with_timeout(Duration::from_millis(10), || async {
            sleep(Duration::from_millis(100)).await;
            Ok(42)
        })
        .await;
        assert!(matches!(result, Err(StrataError::Timeout(_))));
    }

    #[test]
    fn test_config_presets() {
        let quick = RetryConfig::quick();
        assert_eq!(quick.max_attempts, 3);
        assert_eq!(quick.initial_delay, Duration::from_millis(50));

        let patient = RetryConfig::patient();
        assert_eq!(patient.max_attempts, 5);

        let aggressive = CircuitBreakerConfig::aggressive();
        assert_eq!(aggressive.failure_threshold, 3);

        let lenient = CircuitBreakerConfig::lenient();
        assert_eq!(lenient.failure_threshold, 10);
    }
}
