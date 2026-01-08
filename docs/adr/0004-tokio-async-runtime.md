# ADR-0004: Tokio Multi-threaded Async Runtime

## Status

**Accepted**

## Context

Strata is a network-intensive application handling:
- Concurrent client connections (potentially thousands)
- Parallel I/O to multiple data servers
- Background tasks (health checks, garbage collection, replication)
- Timer-based operations (lease expiration, election timeouts)

We needed an async runtime that could efficiently handle this concurrency without spawning OS threads for each connection.

### Options Considered

1. **Tokio (multi-threaded)**: Industry standard, work-stealing scheduler, excellent ecosystem
2. **Tokio (single-threaded)**: Lower overhead but limited to one core
3. **async-std**: Alternative runtime, smaller ecosystem
4. **smol**: Minimal runtime, fewer features
5. **Synchronous with thread pool**: Traditional approach, higher memory per connection

## Decision

We chose **Tokio with multi-threaded runtime** and explicit feature selection:

```toml
# Cargo.toml
[dependencies.tokio]
version = "1.45"
features = [
    "rt-multi-thread",  # Work-stealing thread pool
    "sync",             # Channels, mutexes, semaphores
    "net",              # TCP/UDP networking
    "time",             # Timers, intervals, timeouts
    "signal",           # Unix signal handling
    "macros",           # #[tokio::main], #[tokio::test]
    "io-util",          # AsyncRead/AsyncWrite utilities
    "fs",               # Async filesystem operations
]
```

### Why Not `features = ["full"]`?

Explicit feature selection over `full` provides:
- **Faster compilation**: Only compile what we use
- **Smaller binary**: Unused features not included
- **Explicit dependencies**: Clear what capabilities we rely on
- **Security**: Reduced attack surface from unused code

### Usage Patterns

**1. Concurrent I/O with `tokio::spawn`:**
```rust
// Reading shards in parallel
let handles: Vec<_> = shard_locations
    .iter()
    .map(|loc| tokio::spawn(read_shard(loc.clone())))
    .collect();

let shards = futures::future::try_join_all(handles).await?;
```

**2. Synchronization with `tokio::sync`:**
```rust
use tokio::sync::{RwLock, broadcast, watch, mpsc};

// Shared state with RwLock
let state = Arc::new(RwLock::new(ClusterState::new()));

// Shutdown coordination with broadcast
let (shutdown_tx, _) = broadcast::channel(1);

// Status updates with watch
let (status_tx, status_rx) = watch::channel(Status::Starting);
```

**3. Timeouts and deadlines:**
```rust
use tokio::time::{timeout, Duration};

let result = timeout(
    Duration::from_secs(30),
    fetch_from_data_server(chunk_id)
).await??;
```

**4. Graceful shutdown with signal handling:**
```rust
use tokio::signal;

tokio::select! {
    _ = signal::ctrl_c() => {
        info!("Received SIGINT, initiating shutdown");
        shutdown_coordinator.initiate().await;
    }
    _ = server.run() => {}
}
```

## Consequences

### Positive

- **Efficient concurrency**: Thousands of connections with minimal thread overhead
- **Work stealing**: Automatic load balancing across CPU cores
- **Rich ecosystem**: Compatible with tower, hyper, axum, tonic, reqwest
- **Mature and stable**: Battle-tested in production at scale
- **Excellent tooling**: tokio-console for debugging, tracing integration

### Negative

- **Colored functions**: Async functions can only be called from async contexts
- **Debugging complexity**: Stack traces can be harder to read
- **Learning curve**: Team must understand async/await patterns
- **Runtime overhead**: Small but non-zero cost for task scheduling
- **Blocking code hazards**: Accidentally blocking in async context starves other tasks

### Mitigations for Blocking Code

```rust
// Use spawn_blocking for CPU-intensive work
let encoded = tokio::task::spawn_blocking(move || {
    erasure_encoder.encode(&data)
}).await?;

// Use block_in_place for synchronous code that can't be moved
tokio::task::block_in_place(|| {
    synchronous_library_call()
});
```

### Performance Characteristics

- **Thread pool size**: Defaults to number of CPU cores
- **Task overhead**: ~few hundred bytes per spawned task
- **Context switch**: Sub-microsecond between tasks on same thread
- **I/O multiplexing**: epoll (Linux) / kqueue (macOS) for efficient event notification

### Operational Implications

- **Thread count tuning**: Set `TOKIO_WORKER_THREADS` for non-default pool size
- **Monitoring**: Use `tokio-console` during development, metrics in production
- **Profiling**: Use `tokio-tracing` integration for async-aware profiling

## References

- [Tokio Documentation](https://tokio.rs/)
- `Cargo.toml:30-41` - Tokio dependency with explicit features
- `src/lib.rs` - Main async entry points
- `src/shutdown.rs` - Graceful shutdown using tokio channels
