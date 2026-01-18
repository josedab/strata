# ADR-0007: Configuration Presets Pattern

## Status

Accepted

## Context

Distributed systems have many tunable parameters:

- Timeout durations (connection, request, heartbeat)
- Retry counts and backoff strategies
- Cache sizes and eviction policies
- Thread pool sizes
- Buffer sizes
- Threshold values (failure detection, circuit breaker)

Exposing all parameters directly creates problems:

1. **Overwhelming operators**: Dozens of parameters with non-obvious interactions
2. **Dangerous defaults**: Users may not know safe values
3. **Configuration drift**: Different deployments diverge without clear intent
4. **Documentation burden**: Each parameter needs explanation
5. **Testing complexity**: Exponential combinations to validate

We observed that most configurations fall into a few common patterns:
- "I want maximum performance, I'll accept some risk"
- "I want maximum safety, I'll accept lower performance"
- "I want a balanced default"

## Decision

We will provide **named configuration presets** that encode operational intent, while still allowing parameter override when needed.

### Pattern Implementation

```rust
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub timeout: Duration,
    pub half_open_max_requests: u32,
}

impl CircuitBreakerConfig {
    /// Balanced defaults suitable for most deployments
    pub fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }

    /// Opens circuit quickly, recovers slowly. Use when failures are costly.
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 5,
            timeout: Duration::from_secs(60),
            half_open_max_requests: 1,
        }
    }

    /// Tolerates more failures before opening. Use for flaky dependencies.
    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 2,
            timeout: Duration::from_secs(15),
            half_open_max_requests: 5,
        }
    }
}
```

### Presets Across Modules

| Module | Presets |
|--------|---------|
| CircuitBreaker | `default()`, `aggressive()`, `lenient()` |
| ConnectionPool | `default()`, `high_throughput()`, `low_resource()` |
| ReadCache | `small()`, `default()`, `large()` |
| RateLimit | `strict()`, `relaxed()`, `disabled()` |
| Compression | `fast()`, `balanced()`, `best_compression()` |
| ErasureCoding | `default()`, `large_cluster()`, `cost_optimized()`, `small_cluster()` |
| RetryPolicy | `default()`, `aggressive()`, `patient()` |

### Usage in Configuration Files

```toml
[circuit_breaker]
preset = "aggressive"

[cache]
preset = "large"

[compression]
preset = "fast"
# Override specific parameter if needed
level = 3
```

## Consequences

### Positive

- **Simplicity**: Operators choose intent, not implementation details
- **Safety**: Presets are tested combinations that work well together
- **Documentation**: Preset names are self-documenting
- **Standardization**: Common configurations across deployments
- **Escape hatch**: Individual parameters can still be overridden
- **Evolution**: Preset definitions can improve without config file changes

### Negative

- **Opacity**: Operators may not understand what a preset does
- **Inflexibility**: Edge cases may not fit any preset
- **Versioning**: Preset behavior changes could surprise users
- **Discoverability**: Users must know presets exist

### Preset Naming Convention

| Adjective | Meaning |
|-----------|---------|
| `default` / `balanced` | Good for most cases |
| `aggressive` | Prioritize speed/action over caution |
| `conservative` / `lenient` | Prioritize safety over speed |
| `strict` | Enforce limits tightly |
| `relaxed` / `permissive` | Allow more flexibility |
| `fast` | Optimize for low latency |
| `high_throughput` | Optimize for volume |
| `low_resource` | Minimize memory/CPU usage |
| `small` / `large` | Size-based variants |

### Implications

- All configurable components should offer presets
- Preset names should follow the naming convention
- Documentation should explain what each preset optimizes for
- Breaking changes to preset behavior need version notes
- Monitoring should report which presets are in use

## References

- `src/resilience.rs:54-73` - CircuitBreakerConfig presets
- `src/pool.rs:47-71` - PoolConfig presets
- `src/cache.rs` - ReadCacheConfig presets
- `src/ratelimit.rs:74-100` - RateLimitConfig presets
- `src/compression.rs:92-107` - CompressionConfig presets
- `src/types.rs:382-426` - ErasureCodingConfig presets
