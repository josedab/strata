# ADR-0011: Pluggable Placement Strategies

## Status

**Accepted**

## Context

When a client writes data to Strata, chunks must be placed on data servers. The placement decision affects:
- **Durability**: Chunks on same server/rack have correlated failure risk
- **Performance**: Chunks on nearby servers have lower read latency
- **Balance**: Uneven placement causes hot spots
- **Efficiency**: Placement affects recovery time after failures

Different deployments have different priorities:
- Single-rack deployment: Can't spread across racks
- Multi-datacenter: Must spread across availability zones
- High-throughput: Should use least-loaded servers
- Development: Just needs to work

We needed a placement system that:
- Supports multiple algorithms
- Is configurable per deployment
- Can evolve without changing client code
- Respects failure domain boundaries

## Decision

We implement **pluggable placement strategies** as an enum with runtime dispatch:

```rust
// src/cluster/placement.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlacementStrategy {
    /// Random placement across available servers
    Random,
    /// Spread across failure domains (racks, zones)
    SpreadDomains,
    /// Place on least loaded servers
    LeastLoaded,
}
```

### Strategy Implementations

**Random Placement:**
```rust
impl PlacementStrategy {
    pub fn select_servers(
        &self,
        servers: &[ServerInfo],
        count: usize,
        excluded: &HashSet<NodeId>,
    ) -> Result<Vec<NodeId>> {
        match self {
            PlacementStrategy::Random => {
                let available: Vec<_> = servers
                    .iter()
                    .filter(|s| s.is_healthy() && !excluded.contains(&s.id))
                    .collect();

                if available.len() < count {
                    return Err(StrataError::InsufficientServers);
                }

                let mut rng = rand::thread_rng();
                let selected: Vec<_> = available
                    .choose_multiple(&mut rng, count)
                    .map(|s| s.id)
                    .collect();

                Ok(selected)
            }
            // ... other strategies
        }
    }
}
```

**Spread Domains (Rack/Zone Aware):**
```rust
PlacementStrategy::SpreadDomains => {
    // Group servers by failure domain
    let by_domain: HashMap<String, Vec<&ServerInfo>> = servers
        .iter()
        .filter(|s| s.is_healthy() && !excluded.contains(&s.id))
        .fold(HashMap::new(), |mut acc, s| {
            acc.entry(s.failure_domain.clone()).or_default().push(s);
            acc
        });

    // Round-robin across domains
    let domains: Vec<_> = by_domain.keys().collect();
    let mut selected = Vec::with_capacity(count);
    let mut domain_idx = 0;

    while selected.len() < count {
        let domain = &domains[domain_idx % domains.len()];
        if let Some(servers) = by_domain.get(*domain) {
            if let Some(server) = servers.iter()
                .find(|s| !selected.contains(&s.id))
            {
                selected.push(server.id);
            }
        }
        domain_idx += 1;

        // Prevent infinite loop if not enough servers
        if domain_idx > count * domains.len() {
            break;
        }
    }

    if selected.len() < count {
        return Err(StrataError::InsufficientDomains);
    }

    Ok(selected)
}
```

**Least Loaded:**
```rust
PlacementStrategy::LeastLoaded => {
    let mut available: Vec<_> = servers
        .iter()
        .filter(|s| s.is_healthy() && !excluded.contains(&s.id))
        .collect();

    // Sort by load (chunk count or byte usage)
    available.sort_by_key(|s| s.chunk_count);

    if available.len() < count {
        return Err(StrataError::InsufficientServers);
    }

    Ok(available[..count].iter().map(|s| s.id).collect())
}
```

### Server Metadata

Placement decisions use server metadata:

```rust
pub struct ServerInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub failure_domain: String,  // e.g., "rack-1", "us-east-1a"
    pub chunk_count: u64,
    pub bytes_used: u64,
    pub bytes_capacity: u64,
    pub is_healthy: bool,
    pub last_heartbeat: Instant,
}
```

### Configuration

Strategy is configured at cluster level:

```toml
[cluster]
placement_strategy = "SpreadDomains"  # or "Random", "LeastLoaded"

[[servers]]
id = 1
address = "10.0.1.1:9001"
failure_domain = "rack-1"

[[servers]]
id = 2
address = "10.0.1.2:9001"
failure_domain = "rack-2"
```

## Consequences

### Positive

- **Flexibility**: Choose strategy appropriate for deployment
- **Evolvability**: Add new strategies without changing clients
- **Testability**: Strategies are isolated and testable
- **Failure domain awareness**: SpreadDomains maximizes durability
- **Load balancing**: LeastLoaded prevents hot spots

### Negative

- **Configuration complexity**: Operators must understand strategies
- **Domain metadata requirement**: SpreadDomains needs failure_domain configured
- **Suboptimal for edge cases**: General strategies may not fit all workloads
- **No automatic adaptation**: Strategy doesn't change based on conditions

### Strategy Selection Guide

| Deployment | Recommended | Rationale |
|------------|-------------|-----------|
| Single rack | `Random` or `LeastLoaded` | Can't spread domains anyway |
| Multi-rack | `SpreadDomains` | Survive rack failure |
| Multi-zone | `SpreadDomains` | Survive zone failure |
| Development | `Random` | Simplest, no domain config needed |
| Write-heavy | `LeastLoaded` | Distribute write load |
| Read-heavy | `SpreadDomains` | Spread read load naturally |

### Failure Domain Configuration

For SpreadDomains to work effectively:

```toml
# Single datacenter with racks
[[servers]]
id = 1
failure_domain = "rack-a"

[[servers]]
id = 2
failure_domain = "rack-b"

[[servers]]
id = 3
failure_domain = "rack-c"

# Multi-datacenter
[[servers]]
id = 1
failure_domain = "us-east-1a"

[[servers]]
id = 2
failure_domain = "us-east-1b"

[[servers]]
id = 3
failure_domain = "us-west-2a"
```

### Integration with Erasure Coding

Placement works with erasure coding to maximize durability:

```
4+2 Erasure Coding + SpreadDomains
─────────────────────────────────
Shard 0 → Rack A, Server 1
Shard 1 → Rack B, Server 3
Shard 2 → Rack C, Server 5
Shard 3 → Rack A, Server 2
Parity 0 → Rack B, Server 4
Parity 1 → Rack C, Server 6

Result: Any single rack failure loses at most 2 shards → data survives
```

### Operational Implications

- **Monitoring**: Track shard distribution across domains
- **Alerting**: Alert if placement fails (insufficient domains)
- **Rebalancing**: Consider placement when rebalancing
- **Capacity planning**: Plan for domain-aware capacity

## References

- [HDFS Rack Awareness](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Rack_Awareness) - Similar concept in Hadoop
- `src/cluster/placement.rs` - PlacementStrategy implementation
- `src/cluster/balancer.rs` - Rebalancing with placement awareness
- `src/types.rs` - ServerInfo structure
