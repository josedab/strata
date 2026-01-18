# ADR-0004: Async Rust with Tokio Runtime

## Status

Accepted

## Context

A distributed file system must handle thousands of concurrent operations:
- Client connections (potentially tens of thousands)
- Inter-node replication traffic
- Background maintenance tasks (scrubbing, garbage collection)
- Health checks and heartbeats

Traditional threading models have limitations:

### Thread-per-Connection
- Each connection gets a dedicated OS thread
- Simple programming model
- Memory overhead: ~2-8 MB stack per thread
- Context switching overhead at scale
- 10,000 connections = 10,000 threads = 20-80 GB just for stacks

### Event Loop (Single-threaded)
- One thread handles all connections via epoll/kqueue
- Very efficient for I/O-bound workloads
- Cannot utilize multiple CPU cores
- Long computations block all connections

### Async/Await with Work-Stealing
- Cooperative multitasking with lightweight tasks
- Tasks yield at await points, allowing interleaving
- Work-stealing scheduler balances load across CPU cores
- Memory efficient: tasks are small state machines

Rust's async ecosystem has matured significantly, with Tokio emerging as the de facto standard runtime.

## Decision

We will build Strata entirely on **async Rust with the Tokio runtime**.

Specific choices:
- **Runtime**: Tokio with `rt-multi-thread` for work-stealing across cores
- **Synchronization**: `tokio::sync` primitives (Mutex, RwLock, channels)
- **I/O**: `tokio::io` for async file and network operations
- **Timers**: `tokio::time` for timeouts and intervals
- **Task spawning**: `tokio::spawn` for background work

All public APIs are async:
```rust
pub async fn lookup(&self, parent: InodeId, name: &str) -> Result<Option<Inode>>
pub async fn create_file(&self, parent: InodeId, name: &str, ...) -> Result<CreateResponse>
```

Internal services use async traits:
```rust
#[async_trait]
pub trait Storage {
    async fn get(&self, chunk_id: &ChunkId) -> Result<Vec<u8>>;
    async fn put(&self, chunk_id: &ChunkId, data: &[u8]) -> Result<()>;
}
```

## Consequences

### Positive

- **Scalability**: Handle 100K+ concurrent connections with bounded memory
- **Efficiency**: No wasted threads waiting on I/O
- **CPU utilization**: Work-stealing maximizes core usage
- **Ecosystem**: Rich async libraries (hyper, tonic, reqwest, axum)
- **Composition**: Async functions compose naturally with `?` and combinators

### Negative

- **Learning curve**: Async Rust has unique concepts (Pin, lifetimes in futures)
- **Debugging difficulty**: Stack traces are less informative
- **Blocking pitfall**: Accidentally blocking the runtime degrades all tasks
- **Ecosystem fragmentation**: Some libraries are sync-only
- **Colored functions**: Async is "contagious" - callers must also be async

### Specific Tokio Features Used

| Feature | Purpose |
|---------|---------|
| `rt-multi-thread` | Work-stealing scheduler |
| `sync` | Async Mutex, RwLock, mpsc, broadcast, watch |
| `time` | Sleep, timeout, interval |
| `signal` | Graceful shutdown on SIGTERM/SIGINT |
| `fs` | Async file operations |
| `net` | Async TCP/UDP |

### Mitigations for Blocking

- `tokio::task::spawn_blocking` for CPU-intensive work (erasure coding)
- Dedicated thread pools for file I/O where beneficial
- Timeouts on all external calls to prevent indefinite blocking

### Implications

- All contributors must understand async Rust
- Code reviews must check for accidental blocking
- Benchmarks should measure under concurrent load, not just sequential
- Logging and tracing must handle async context propagation

## References

- Tokio documentation: https://tokio.rs
- `Cargo.toml:32-42` - Tokio feature configuration
- `src/lib.rs` - Async server startup
- `src/shutdown.rs` - Async shutdown coordination
