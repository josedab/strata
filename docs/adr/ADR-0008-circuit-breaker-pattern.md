# ADR-0008: Circuit Breaker for Cascading Failure Prevention

## Status

Accepted

## Context

In a distributed system, services depend on each other:
- Metadata servers depend on Raft peers
- Data servers depend on metadata for chunk location
- Clients depend on both metadata and data servers

When a service fails or becomes slow, naive retry behavior causes cascading failures:

1. Service A becomes slow (e.g., 10 second response time)
2. Callers to A retry aggressively, creating more load
3. A's queue grows, making it even slower
4. Callers' threads/tasks block waiting for A
5. Callers themselves become slow
6. Callers' callers experience delays
7. Entire system degrades

This "retry storm" can take down healthy services and make recovery difficult.

### Real-World Scenario

A metadata server under high load:
- Normal latency: 10ms
- Under stress: 5000ms
- Client timeout: 1000ms

Without circuit breaker:
- Clients timeout after 1s, retry immediately
- Each retry adds to server queue
- Server never recovers because load keeps increasing
- All clients eventually fail

## Decision

We will implement the **Circuit Breaker pattern** to prevent cascading failures.

### State Machine

```
     ┌─────────────────────────────────────────────┐
     │                                             │
     ▼                                             │
┌─────────┐  failure_threshold   ┌──────────┐     │
│ CLOSED  │ ───────────────────► │   OPEN   │     │
│(normal) │   exceeded           │ (reject) │     │
└─────────┘                      └──────────┘     │
     ▲                                │           │
     │                                │ timeout   │
     │ success_threshold              ▼           │
     │    reached              ┌───────────┐      │
     └──────────────────────── │ HALF-OPEN │ ─────┘
                               │  (probe)  │  failure
                               └───────────┘
```

### Implementation

```rust
pub enum CircuitState {
    Closed,    // Normal operation, requests flow through
    Open,      // Failing, reject requests immediately
    HalfOpen,  // Testing if service recovered
}

pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitState>>,
    config: CircuitBreakerConfig,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    last_failure_time: AtomicU64,
}

impl CircuitBreaker {
    /// Check if request should be allowed
    pub async fn should_allow(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout elapsed, transition to half-open
                if self.timeout_elapsed() {
                    drop(state);
                    self.transition_to_half_open().await;
                    true  // Allow probe request
                } else {
                    false  // Still open, reject
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests to probe
                self.half_open_requests() < self.config.half_open_max_requests
            }
        }
    }

    /// Record request outcome
    pub async fn record_success(&self) { ... }
    pub async fn record_failure(&self) { ... }
}
```

### Usage Pattern

```rust
async fn call_metadata_service(&self, request: Request) -> Result<Response> {
    // Check circuit breaker first
    if !self.circuit_breaker.should_allow().await {
        return Err(StrataError::ServiceUnavailable("circuit open".into()));
    }

    // Make the actual call
    match self.do_request(request).await {
        Ok(response) => {
            self.circuit_breaker.record_success().await;
            Ok(response)
        }
        Err(e) => {
            self.circuit_breaker.record_failure().await;
            Err(e)
        }
    }
}
```

## Consequences

### Positive

- **Fail fast**: Requests fail immediately when service is known-bad
- **Recovery time**: Failing service gets breathing room to recover
- **Resource protection**: Callers don't waste threads/connections on doomed requests
- **Automatic recovery**: Half-open state probes for recovery without manual intervention
- **Observability**: Circuit state is a clear health indicator

### Negative

- **Complexity**: Additional state machine to maintain and test
- **False positives**: Transient failures may open circuit unnecessarily
- **Coordination**: Multiple callers have independent circuits (no global view)
- **Tuning required**: Thresholds need adjustment for each service

### Configuration Trade-offs

| Parameter | Low Value | High Value |
|-----------|-----------|------------|
| `failure_threshold` | Opens quickly, more false positives | Tolerates more failures |
| `success_threshold` | Closes quickly, may oscillate | More confident recovery |
| `timeout` | Probes frequently | Longer recovery time |
| `half_open_max_requests` | Conservative probing | Faster recovery detection |

### Monitoring

Circuit breaker state should be exposed as metrics:
- `circuit_breaker_state` (gauge): 0=closed, 1=open, 2=half-open
- `circuit_breaker_failures_total` (counter): Failures recorded
- `circuit_breaker_rejections_total` (counter): Requests rejected
- `circuit_breaker_state_transitions_total` (counter): State changes

### Implications

- Every external dependency should have a circuit breaker
- Alerts should fire when circuits open
- Dashboards should show circuit state per service
- Load tests should verify circuit behavior under failure

## References

- Nygard, M. (2007). "Release It!" - Original circuit breaker pattern description
- `src/resilience.rs:18-147` - CircuitBreaker implementation
- `src/resilience.rs:54-73` - Configuration presets
