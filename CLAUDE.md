# CLAUDE.md - Strata Project Guidelines

## Project Overview

Strata is a distributed file system combining POSIX compatibility with S3 access. It provides enterprise-grade distributed storage with Raft-based metadata consensus, Reed-Solomon erasure coding, and self-healing capabilities.

## Build Commands

```bash
# Build (default features: fuse + s3)
cargo build

# Build release
cargo build --release

# Build without optional features
cargo build --no-default-features --features s3

# Check compilation
cargo check

# Run linting
cargo clippy

# Format code
cargo fmt
```

## Test Commands

```bash
# Run all tests
cargo test

# Run tests with specific features
cargo test --no-default-features --features s3

# Run a specific test
cargo test test_name

# Run tests in a specific module
cargo test module_name::

# Run benchmarks
cargo bench
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Strata                               │
├─────────────────────────────────────────────────────────────┤
│  Access Layer: FUSE Client | S3 Gateway | Native Client     │
├─────────────────────────────────────────────────────────────┤
│  Metadata Cluster: Raft Consensus | State Machine           │
├─────────────────────────────────────────────────────────────┤
│  Data Servers: Chunk Storage | Erasure Coding | Caching     │
├─────────────────────────────────────────────────────────────┤
│  Cluster Management: Placement | Recovery | Rebalancing     │
└─────────────────────────────────────────────────────────────┘
```

## Code Organization

```
src/
├── lib.rs              # Library root, re-exports, server startup
├── main.rs             # CLI entry point
├── error.rs            # Error types (StrataError, Result)
├── types.rs            # Core types (ChunkId, InodeId, NodeId, Inode)
├── config/             # Configuration management
│
├── raft/               # Raft consensus implementation
│   ├── node.rs         # RaftNode - main consensus logic
│   ├── state.rs        # RaftState - leader/follower/candidate
│   ├── log.rs          # RaftLog - replicated log
│   ├── storage.rs      # Persistent storage
│   └── rpc.rs          # RPC types
│
├── metadata/           # Metadata service
│   ├── state_machine.rs # File system state machine
│   ├── operations.rs   # Metadata operations
│   └── server.rs       # Metadata gRPC server
│
├── data/               # Data service
│   ├── chunk_storage.rs # Local chunk storage
│   └── server.rs       # Data gRPC server
│
├── erasure/            # Reed-Solomon erasure coding
├── cluster/            # Cluster management
│   ├── placement.rs    # Shard placement strategies
│   ├── recovery.rs     # Data recovery
│   ├── balancer.rs     # Load balancing
│   ├── failure_detector.rs # Phi accrual failure detection
│   └── executor.rs     # Cluster state management
│
├── auth/               # Authentication & authorization
│   ├── token.rs        # JWT token handling
│   ├── acl.rs          # POSIX ACLs
│   └── middleware.rs   # Auth middleware
│
├── fuse/               # FUSE filesystem (optional)
│   ├── filesystem.rs   # FUSE operations
│   └── client.rs       # Metadata/data client integration
│
├── s3/                 # S3 gateway (optional)
│   ├── gateway.rs      # S3 API server
│   └── handlers.rs     # S3 request handlers
│
├── client/             # Client libraries
├── observability/      # Metrics, tracing
│
├── audit.rs            # Audit logging (SOC2/HIPAA/GDPR)
├── backup.rs           # Backup/restore functionality
├── cache.rs            # LRU caching layer
├── compression.rs      # Data compression (LZ4, Zstd, Snappy)
├── encryption.rs       # Encryption at rest (AES-256-GCM, ChaCha20)
├── events.rs           # Event notifications (pub/sub, webhooks)
├── gc.rs               # Garbage collection
├── health.rs           # Health checks (liveness, readiness)
├── lock.rs             # Distributed locking with leases
├── pool.rs             # Connection pooling
├── quota.rs            # Quota enforcement
├── ratelimit.rs        # Rate limiting (token bucket, sliding window)
├── resilience.rs       # Circuit breaker, retry, bulkhead patterns
├── scrub.rs            # Background data scrubbing
├── shutdown.rs         # Graceful shutdown coordination
├── snapshot.rs         # Point-in-time snapshots
└── tls.rs              # TLS configuration
```

## Key Types

- `ChunkId` - Unique chunk identifier (UUID wrapper)
- `InodeId` - Inode number (u64)
- `NodeId` - Cluster node identifier (u64)
- `Inode` - File/directory metadata
- `Result<T>` - Alias for `Result<T, StrataError>`

## Coding Patterns

### Error Handling
```rust
use crate::error::{Result, StrataError};

pub fn my_function() -> Result<()> {
    something_fallible()?;
    Ok(())
}
```

### Async Traits
```rust
use async_trait::async_trait;

#[async_trait]
pub trait MyTrait {
    async fn do_something(&self) -> Result<()>;
}
```

### Configuration with Presets
```rust
impl MyConfig {
    pub fn default() -> Self { /* balanced defaults */ }
    pub fn aggressive() -> Self { /* performance-focused */ }
    pub fn conservative() -> Self { /* safety-focused */ }
}
```

### Tests in Same File
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() { }

    #[tokio::test]
    async fn test_async() { }
}
```

## Features

- `fuse` - FUSE filesystem support (requires libfuse)
- `s3` - S3-compatible gateway

## Important Notes

1. **ChunkId vs String**: `ChunkId` is a newtype around `Uuid`, not `String`. Use `ChunkId::new()` to create.

2. **Async Runtime**: Uses Tokio. All async code should use `tokio::spawn`, `tokio::sync` primitives.

3. **Serialization**: Uses `serde` with `#[serde(rename_all = "snake_case")]` for enums.

4. **Tracing**: Use `tracing` macros (`info!`, `debug!`, `error!`, `warn!`) not `println!`.

5. **No Unwrap in Library Code**: Use `?` operator or proper error handling. `unwrap()` only in tests.

6. **Arc for Shared State**: Use `Arc<RwLock<T>>` or `Arc<Mutex<T>>` for shared mutable state.

## Running the Server

```bash
# Development mode
cargo run -- --config config.toml

# Or use the server binary
cargo run --bin strata-server

# Mount FUSE filesystem
cargo run --bin strata-fuse -- /mnt/strata
```

## Integration Tests

Located in `tests/`:
- `client_integration.rs` - Client connectivity tests
- `data_integration.rs` - Chunk storage tests
- `erasure_integration.rs` - Erasure coding tests
- `metadata_integration.rs` - Metadata operations tests

## Benchmarks

Located in `benches/`:
- `chunk_storage.rs` - Storage performance
- `erasure_coding.rs` - Encoding/decoding performance

Run with: `cargo bench`
