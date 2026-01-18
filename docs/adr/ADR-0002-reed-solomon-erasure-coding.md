# ADR-0002: Reed-Solomon Erasure Coding Over Replication

## Status

Accepted

## Context

Distributed file systems must protect data against disk failures, node failures, and data corruption. Two primary approaches exist:

### Replication
Store N complete copies of each data block. For example, 3x replication stores three copies, tolerating loss of any two.

- **Storage overhead**: 200% (3x the original data size)
- **Write cost**: 3 network transfers per write
- **Recovery**: Simple copy from surviving replica

### Erasure Coding
Split data into K data shards and compute M parity shards using mathematical encoding. Any K of the K+M shards can reconstruct the original data.

- **Storage overhead**: M/K (e.g., 2/4 = 50% for 4+2 configuration)
- **Write cost**: K+M network transfers, plus encoding CPU
- **Recovery**: Mathematical reconstruction from K surviving shards

At scale, the storage cost difference is significant:
- 1 PB of data with 3x replication requires 3 PB of storage
- 1 PB of data with 4+2 erasure coding requires 1.5 PB of storage

For a distributed file system expected to store petabytes, this 50% reduction in storage costs is substantial.

## Decision

We will use **Reed-Solomon erasure coding** as the primary data durability mechanism.

Configuration is flexible with presets for common scenarios:

| Preset | Data Shards | Parity Shards | Overhead | Fault Tolerance |
|--------|-------------|---------------|----------|-----------------|
| Default | 4 | 2 | 50% | 2 failures |
| Large Cluster | 10 | 4 | 40% | 4 failures |
| Cost Optimized | 16 | 4 | 25% | 4 failures |
| Small Cluster | 2 | 1 | 50% | 1 failure |

Implementation uses the `reed-solomon-erasure` crate, which provides:
- Galois field arithmetic over GF(2^8)
- SIMD-optimized encoding/decoding
- Incremental parity updates

Key components:
- `src/erasure/mod.rs`: `ErasureCoder` with encode/decode/reconstruct methods
- `src/types.rs`: `ErasureCodingConfig` with preset configurations
- `src/data/chunk_storage.rs`: Integration with chunk storage layer

## Consequences

### Positive

- **50-75% storage cost reduction** compared to 3x replication
- **Configurable tradeoffs**: Operators can tune data/parity ratio for their needs
- **Same durability guarantees**: 4+2 tolerates same failures as 3x replication
- **Scalable**: Higher ratios (16+4) become practical with large clusters
- **Proven technology**: Reed-Solomon is used in CDs, QR codes, RAID, cloud storage

### Negative

- **CPU overhead**: Encoding and decoding require computation
- **Reconstruction latency**: Recovering data requires reading K shards and computing
- **Complexity**: More moving parts than simple replication
- **Minimum cluster size**: Need at least K+M nodes for full distribution
- **Partial read overhead**: Reading small portions still requires K shard accesses

### Mitigations

- **SIMD optimization**: Encoding uses vectorized instructions where available
- **Caching**: Hot data is cached to avoid repeated reconstruction
- **Parallel I/O**: Shard reads happen concurrently across nodes
- **Replication for hot data**: Future work may add replication tier for frequently accessed data

### Implications

- Chunk size should be large enough to amortize encoding overhead (default: 64MB)
- Cluster should have at least K+M nodes for optimal shard distribution
- Background scrubbing needed to detect and repair bit rot before accumulating failures
- Placement strategy must ensure shards land on different failure domains

## References

- Reed, I. S., & Solomon, G. (1960). "Polynomial Codes over Certain Finite Fields"
- `src/erasure/mod.rs` - Erasure coding implementation
- `src/types.rs:382-426` - Configuration presets
