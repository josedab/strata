# ADR-0008: Configuration Presets for Common Scenarios

## Status

**Accepted**

## Context

Strata has many tunable parameters:
- Raft election timeouts
- Erasure coding ratios
- Cache sizes
- Connection pool limits
- Circuit breaker thresholds
- Rate limits
- Compression algorithms

Each parameter has complex interactions with others and with the deployment environment. Operators face challenges:

1. **Cognitive overload**: Dozens of parameters to understand
2. **Invalid combinations**: Some parameter combinations don't make sense
3. **Environment mismatch**: Development settings don't work in production
4. **Best practices**: Optimal values are non-obvious

We needed a configuration approach that:
- Provides sensible defaults for common scenarios
- Allows full customization when needed
- Makes it hard to choose invalid configurations
- Encodes operational best practices

## Decision

We implement **named presets** for complex configuration structures, with builder methods for customization:

### Pattern: Preset Methods

```rust
// src/config/mod.rs
impl StrataConfig {
    /// Sensible defaults for development/testing
    pub fn development() -> Self {
        Self {
            node: NodeConfig { id: 1, role: NodeRole::Combined },
            erasure: ErasureCodingConfig::SMALL_CLUSTER,  // 2+1
            cache: CacheConfig::small(),
            ..Default::default()
        }
    }

    /// Production configuration for small clusters (3-6 nodes)
    pub fn production_small() -> Self {
        Self {
            erasure: ErasureCodingConfig::DEFAULT,  // 4+2
            cache: CacheConfig::default(),
            ..Default::default()
        }
    }

    /// Production configuration for large clusters (12+ nodes)
    pub fn production_large() -> Self {
        Self {
            erasure: ErasureCodingConfig::LARGE_CLUSTER,  // 8+4
            cache: CacheConfig::large(),
            ..Default::default()
        }
    }
}
```

### Pattern: Component Presets

Each subsystem has its own presets:

**Erasure Coding (`src/types.rs`):**
```rust
impl ErasureCodingConfig {
    pub const SMALL_CLUSTER: Self = Self { data_shards: 2, parity_shards: 1 };    // 3 nodes min
    pub const DEFAULT: Self = Self { data_shards: 4, parity_shards: 2 };          // 6 nodes min
    pub const LARGE_CLUSTER: Self = Self { data_shards: 8, parity_shards: 4 };    // 12 nodes min
    pub const COST_OPTIMIZED: Self = Self { data_shards: 10, parity_shards: 4 };  // 14 nodes min
}
```

**Circuit Breaker (`src/resilience.rs`):**
```rust
impl CircuitBreakerConfig {
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,        // Open after 3 failures
            reset_timeout: Duration::from_secs(10),
            success_threshold: 1,        // Close after 1 success
            failure_window: Duration::from_secs(30),
        }
    }

    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,       // Open after 10 failures
            reset_timeout: Duration::from_secs(60),
            success_threshold: 3,        // Close after 3 successes
            failure_window: Duration::from_secs(120),
        }
    }
}
```

**Connection Pool (`src/pool.rs`):**
```rust
impl PoolConfig {
    pub fn high_throughput() -> Self {
        Self {
            min_connections: 10,
            max_connections: 100,
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(3600),
        }
    }

    pub fn low_resource() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            idle_timeout: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(1800),
        }
    }
}
```

**Rate Limiting (`src/ratelimit.rs`):**
```rust
impl RateLimitConfig {
    pub fn strict() -> Self {
        Self { max_requests: 50, window: Duration::from_secs(1), burst: 10 }
    }

    pub fn default() -> Self {
        Self { max_requests: 100, window: Duration::from_secs(1), burst: 200 }
    }

    pub fn relaxed() -> Self {
        Self { max_requests: 1000, window: Duration::from_secs(1), burst: 2000 }
    }
}
```

### Pattern: Builder for Customization

Presets can be modified using builder pattern:

```rust
let config = StrataConfig::production_small()
    .with_cache_size(8 * 1024 * 1024 * 1024)  // 8GB cache
    .with_erasure(ErasureCodingConfig::COST_OPTIMIZED);
```

## Consequences

### Positive

- **Quick start**: New deployments work with `StrataConfig::development()`
- **Best practices baked in**: Presets encode operational experience
- **Self-documenting**: Preset names communicate intent
- **Validation**: Presets are known-good combinations
- **Flexibility preserved**: Full customization still available

### Negative

- **Hidden complexity**: Users may not understand underlying parameters
- **Preset proliferation**: Risk of too many presets
- **Maintenance burden**: Presets must be updated as system evolves
- **False confidence**: Users may assume preset is optimal for their case

### Preset Selection Guide

| Scenario | Recommended Preset |
|----------|-------------------|
| Local development | `development()` |
| CI/testing | `development()` |
| Small production (3-6 nodes) | `production_small()` |
| Large production (12+ nodes) | `production_large()` |
| Cost-sensitive (50+ nodes) | Custom with `COST_OPTIMIZED` erasure |

### Operational Implications

- **Documentation**: Document when to use each preset
- **Monitoring**: Alert if using development preset in production
- **Migration**: Provide guidance for moving between presets
- **Audit**: Log which preset/config is active

### Example Usage

**Minimal development setup:**
```rust
let config = StrataConfig::development();
let server = StrataServer::new(config).await?;
```

**Production with customization:**
```rust
let config = StrataConfig::production_small()
    .with_node_id(node_id_from_env())
    .with_raft_peers(peers_from_discovery())
    .with_cache_size(available_memory() / 4);

let server = StrataServer::new(config).await?;
```

**Full manual configuration:**
```rust
let config = StrataConfig {
    node: NodeConfig { id: 1, role: NodeRole::Metadata },
    raft: RaftConfig {
        election_timeout_min: Duration::from_millis(500),
        election_timeout_max: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(150),
        snapshot_threshold: 50_000,
    },
    erasure: ErasureCodingConfig { data_shards: 6, parity_shards: 3 },
    // ... etc
};
```

## References

- `src/config/mod.rs` - Main configuration with presets
- `src/types.rs:376-427` - ErasureCodingConfig presets
- `src/resilience.rs:54-72` - CircuitBreakerConfig presets
- `src/pool.rs:46-70` - PoolConfig presets
- `src/ratelimit.rs:74-80` - RateLimitConfig presets
