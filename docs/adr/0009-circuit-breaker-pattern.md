# ADR-0009: Circuit Breaker Pattern for Fault Isolation

## Status

**Accepted**

## Context

Strata depends on multiple external services and components:
- Data servers for chunk storage
- Metadata nodes for consensus
- External authentication providers
- Network infrastructure

When a dependency fails, naive retry behavior can cause cascading failures:
1. Client retries failed request
2. Retry adds load to struggling service
3. Service becomes more overloaded
4. More requests fail, more retries
5. System spirals into complete failure

This is the **retry storm** problem. We needed a mechanism to:
- Detect when a dependency is unhealthy
- Stop sending requests to unhealthy services
- Allow time for recovery
- Automatically resume when service recovers

## Decision

We implemented the **Circuit Breaker pattern** with three states:

```rust
// src/resilience.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation, requests flow through
    Closed,
    /// Dependency unhealthy, requests fail immediately
    Open,
    /// Testing if dependency recovered
    HalfOpen,
}
```

### State Transitions

```
                    ┌─────────────────────────────────────────┐
                    │                                         │
                    ▼                                         │
              ┌──────────┐                                    │
              │  CLOSED  │ ◄──────────────────────────────────┤
              │ (normal) │     success_threshold successes    │
              └────┬─────┘                                    │
                   │                                          │
                   │ failure_threshold                        │
                   │ failures in window                       │
                   │                                          │
                   ▼                                          │
              ┌──────────┐                                    │
              │   OPEN   │                                    │
              │  (fail   │                                    │
              │  fast)   │                                    │
              └────┬─────┘                                    │
                   │                                          │
                   │ reset_timeout                            │
                   │ elapsed                                  │
                   │                                          │
                   ▼                                          │
              ┌──────────┐                                    │
              │ HALFOPEN │ ────────────────────────────────────
              │ (probe)  │     failure → back to OPEN
              └──────────┘
```

### Configuration

```rust
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,

    /// Time to wait before trying again
    pub reset_timeout: Duration,

    /// Successes needed to close circuit
    pub success_threshold: u32,

    /// Window for counting failures
    pub failure_window: Duration,
}

impl CircuitBreakerConfig {
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(10),
            success_threshold: 1,
            failure_window: Duration::from_secs(30),
        }
    }

    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            reset_timeout: Duration::from_secs(60),
            success_threshold: 3,
            failure_window: Duration::from_secs(120),
        }
    }
}
```

### Usage Pattern

```rust
pub struct CircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicU32,
    last_failure: Mutex<Option<Instant>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    pub async fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: Future<Output = Result<T, E>>,
    {
        // Check if circuit allows request
        match self.current_state() {
            CircuitState::Open => {
                if self.should_try_reset() {
                    self.transition_to(CircuitState::HalfOpen);
                } else {
                    return Err(CircuitBreakerError::Open);
                }
            }
            CircuitState::HalfOpen => {
                // Only allow one probe request
            }
            CircuitState::Closed => {
                // Normal flow
            }
        }

        // Execute the call
        match f.await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(CircuitBreakerError::Failed(e))
            }
        }
    }
}
```

### Integration with Data Servers

Each data server connection has its own circuit breaker:

```rust
pub struct DataClient {
    servers: HashMap<NodeId, ServerConnection>,
}

struct ServerConnection {
    client: HttpClient,
    circuit_breaker: CircuitBreaker,
}

impl DataClient {
    pub async fn read_chunk(&self, server_id: NodeId, chunk_id: ChunkId) -> Result<Vec<u8>> {
        let conn = self.servers.get(&server_id)
            .ok_or(StrataError::UnknownServer(server_id))?;

        conn.circuit_breaker
            .call(conn.client.get_chunk(chunk_id))
            .await
            .map_err(|e| match e {
                CircuitBreakerError::Open => StrataError::DataServerUnavailable(server_id),
                CircuitBreakerError::Failed(e) => e,
            })
    }
}
```

## Consequences

### Positive

- **Prevents cascading failures**: Unhealthy services isolated quickly
- **Fast failure**: Open circuit fails immediately, no wasted timeout
- **Automatic recovery**: HalfOpen state probes for recovery
- **Resource conservation**: No retry storms consuming resources
- **Visibility**: Circuit state is observable/alertable

### Negative

- **Temporary unavailability**: Open circuit means temporary data unavailability
- **Configuration complexity**: Thresholds need tuning per deployment
- **False positives**: Transient issues can open circuit unnecessarily
- **Cold start**: After recovery, service may be cold (caches empty)

### Preset Selection Guide

| Scenario | Preset | Rationale |
|----------|--------|-----------|
| Latency-sensitive workloads | `aggressive()` | Fail fast, minimize latency impact |
| Batch processing | `lenient()` | Tolerate more failures, prioritize completion |
| Cross-datacenter | Custom with longer timeouts | Account for network variability |

### Monitoring Requirements

Essential metrics to track:
- Circuit state per server (closed/open/half-open)
- Failure count and rate
- Time spent in open state
- Recovery success rate

```rust
// Example metric emission
metrics::gauge!("circuit_breaker_state", state as f64, "server" => server_id);
metrics::counter!("circuit_breaker_failures", 1, "server" => server_id);
metrics::histogram!("circuit_breaker_open_duration", duration);
```

### Operational Implications

- **Alerting**: Alert when circuit opens (dependency issue)
- **Dashboards**: Visualize circuit states across cluster
- **Tuning**: Adjust thresholds based on observed failure patterns
- **Testing**: Chaos engineering to verify circuit behavior

## References

- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html) - Martin Fowler
- [Release It!](https://pragprog.com/titles/mnee2/release-it-second-edition/) - Michael Nygard
- `src/resilience.rs` - Full CircuitBreaker implementation
- `src/client/mod.rs` - Circuit breaker integration with data client
