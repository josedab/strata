# ADR-0002: Reed-Solomon Erasure Coding over Replication

## Status

**Accepted**

## Context

Distributed storage systems must protect against data loss from disk failures, node failures, and other hardware issues. The two primary approaches are:

1. **Replication**: Store N complete copies of data (typically N=3)
2. **Erasure coding**: Split data into k data shards + m parity shards, where any k shards can reconstruct the original

We needed to choose a durability strategy that balances:
- Storage efficiency (cost per usable byte)
- Fault tolerance (number of simultaneous failures survived)
- Read/write performance
- Recovery complexity

### Comparison

| Approach | Storage Overhead | Fault Tolerance | Read Performance | Recovery |
|----------|-----------------|-----------------|------------------|----------|
| 3-replica | 3.0x (200% overhead) | 2 failures | Fast (read any copy) | Simple |
| 2-replica | 2.0x (100% overhead) | 1 failure | Fast | Simple |
| RS(4,2) | 1.5x (50% overhead) | 2 failures | Slower (read 4+ shards) | Complex |
| RS(10,4) | 1.4x (40% overhead) | 4 failures | Slower (read 10+ shards) | Complex |

At petabyte scale, the difference between 3.0x and 1.5x overhead is significant—potentially millions of dollars in storage costs.

## Decision

We chose **Reed-Solomon erasure coding** as the primary durability mechanism with configurable data/parity ratios:

```rust
// src/types.rs
pub struct ErasureCodingConfig {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl ErasureCodingConfig {
    pub const DEFAULT: Self = Self { data_shards: 4, parity_shards: 2 };
    pub const LARGE_CLUSTER: Self = Self { data_shards: 8, parity_shards: 4 };
    pub const COST_OPTIMIZED: Self = Self { data_shards: 10, parity_shards: 4 };
    pub const SMALL_CLUSTER: Self = Self { data_shards: 2, parity_shards: 1 };
}
```

Implementation details:

1. **Library choice**: We use the `reed-solomon-erasure` crate which provides optimized SIMD implementations

2. **Default configuration (4+2)**:
   - Survives any 2 simultaneous failures
   - 1.5x storage overhead (same fault tolerance as 3-replica at half the cost)
   - Requires minimum 6 data servers for optimal placement

3. **Chunk-level encoding**: Each 64MB chunk is independently encoded into 6 shards (4 data + 2 parity), each shard ~10.7MB

4. **Shard placement**: Shards are distributed across different failure domains (racks, availability zones) to maximize independence

```
Original Chunk (64MB)
        │
        ▼
┌─────────────────────────────────────────────────┐
│           Reed-Solomon Encoder (4+2)             │
└─────────────────────────────────────────────────┘
        │
        ├──► Shard 0 (16MB) ──► Server A (Rack 1)
        ├──► Shard 1 (16MB) ──► Server B (Rack 2)
        ├──► Shard 2 (16MB) ──► Server C (Rack 3)
        ├──► Shard 3 (16MB) ──► Server D (Rack 1)
        ├──► Parity 0 (16MB) ─► Server E (Rack 2)
        └──► Parity 1 (16MB) ─► Server F (Rack 3)
```

## Consequences

### Positive

- **50% storage cost reduction**: 1.5x overhead vs 3.0x for equivalent fault tolerance
- **Scalable fault tolerance**: Can increase parity shards for higher durability requirements
- **Cost optimization path**: 10+4 configuration reduces overhead to 1.4x for large clusters
- **Configurable per-deployment**: Small clusters can use 2+1, large clusters can use 10+4

### Negative

- **Higher read latency**: Must read k shards and decode; cannot read from single replica
- **Higher write latency**: Must encode before writing; encoding is CPU-intensive
- **Complex recovery**: Reconstruction requires reading k surviving shards and re-encoding
- **Minimum node requirements**: 4+2 requires 6 nodes for optimal placement
- **Network amplification**: Reading 64MB object requires fetching ~64MB of shards (no read amplification), but with higher latency due to parallelization overhead

### Mitigations

- **Caching**: Hot data cached locally avoids repeated decode operations
- **SIMD acceleration**: reed-solomon-erasure uses AVX2/NEON for fast encoding
- **Parallel I/O**: Shards read in parallel, total latency ≈ slowest shard + decode time
- **Background reconstruction**: Failed shards reconstructed asynchronously, not blocking reads

### Operational Implications

- **Cluster sizing**: Plan for k+m minimum nodes; 4+2 needs at least 6 servers
- **Failure domain awareness**: Configure placement to spread shards across racks/zones
- **CPU provisioning**: Encode/decode is CPU-bound; provision accordingly
- **Monitoring**: Track encode/decode latencies, reconstruction queue depth

## References

- [Erasure Coding in Windows Azure Storage](https://www.usenix.org/system/files/conference/atc12/atc12-final181_0.pdf)
- `src/erasure/mod.rs` - ErasureCoder implementation
- `src/types.rs:376-427` - ErasureCodingConfig with presets
- `src/cluster/placement.rs` - Shard placement strategies
