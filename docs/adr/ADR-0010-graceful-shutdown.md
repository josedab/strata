# ADR-0010: Graceful Shutdown with Coordinated Termination

## Status

Accepted

## Context

Server processes receive termination signals (SIGTERM, SIGINT) when:
- Kubernetes rolls out a new deployment
- An operator restarts the service
- The system shuts down
- A container orchestrator reschedules

Abrupt termination causes problems:

### Data Loss
- In-flight writes may be partially complete
- Raft log entries may not be flushed
- Cache contents lost without persistence

### Client Disruption
- Active requests fail with connection reset
- Clients must retry, adding latency
- No opportunity for graceful handoff

### Cluster Instability
- Raft leader disappearing triggers election
- Consensus stalls during election timeout
- Dependent services see failures cascade

### Resource Leaks
- File handles not closed
- Network connections not terminated cleanly
- Temporary files not cleaned up

A production system needs orderly shutdown that:
1. Stops accepting new work
2. Completes in-flight operations
3. Persists necessary state
4. Releases resources cleanly
5. Exits within a deadline

## Decision

We will implement **coordinated graceful shutdown** using Tokio's async primitives.

### Architecture

```
                    ┌─────────────────────────┐
                    │    Signal Handler       │
                    │   (SIGTERM, SIGINT)     │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   ShutdownCoordinator   │
                    │   (broadcast channel)   │
                    └───────────┬─────────────┘
                                │
           ┌────────────────────┼────────────────────┐
           ▼                    ▼                    ▼
    ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
    │  Metadata   │      │    Data     │      │   Client    │
    │   Server    │      │   Server    │      │  Handlers   │
    └─────────────┘      └─────────────┘      └─────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
    ┌─────────────────────────────────────────────────────┐
    │              Completion Tracking                     │
    │                (watch channel)                       │
    └─────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │     Force Exit          │
                    │  (after 30s timeout)    │
                    └─────────────────────────┘
```

### Implementation

```rust
pub struct ShutdownCoordinator {
    notify: broadcast::Sender<()>,
    completion: watch::Sender<bool>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        let (notify, _) = broadcast::channel(1);
        let (completion, _) = watch::channel(false);
        Self { notify, completion }
    }

    /// Signal all services to begin shutdown
    pub fn trigger(&self) {
        let _ = self.notify.send(());
    }

    /// Get a receiver for shutdown notification
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify.subscribe()
    }

    /// Mark shutdown as complete
    pub fn complete(&self) {
        let _ = self.completion.send(true);
    }
}
```

### Service Integration

```rust
async fn run_metadata_server(
    shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut shutdown = shutdown;

    loop {
        tokio::select! {
            // Normal operation
            request = accept_request() => {
                handle_request(request).await?;
            }

            // Shutdown signal received
            _ = shutdown.recv() => {
                info!("Metadata server shutting down");

                // Stop accepting new requests
                // Wait for in-flight requests to complete
                drain_requests().await;

                // Flush Raft log
                flush_raft_log().await?;

                // Close connections gracefully
                close_connections().await;

                break;
            }
        }
    }

    Ok(())
}
```

### Shutdown Sequence

1. **Signal received**: SIGTERM or SIGINT caught
2. **Broadcast**: ShutdownCoordinator notifies all services
3. **Drain**: Services stop accepting new work, complete in-flight
4. **Persist**: Flush logs, caches, state to disk
5. **Disconnect**: Close client connections with proper errors
6. **Raft handoff**: If leader, attempt leadership transfer (future)
7. **Resource cleanup**: Close files, release locks
8. **Exit**: Process terminates with status 0

### Timeout Handling

```rust
// Maximum time to wait for graceful shutdown
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

async fn shutdown_with_timeout(coordinator: ShutdownCoordinator) {
    coordinator.trigger();

    // Wait for completion or timeout
    let result = tokio::time::timeout(
        SHUTDOWN_TIMEOUT,
        coordinator.wait_for_completion()
    ).await;

    if result.is_err() {
        warn!("Graceful shutdown timed out, forcing exit");
    }

    std::process::exit(0);
}
```

## Consequences

### Positive

- **Zero request loss**: In-flight operations complete successfully
- **Data durability**: State is persisted before exit
- **Client experience**: Connections close gracefully, not reset
- **Cluster stability**: Raft has time for orderly transition
- **Kubernetes compatibility**: Respects terminationGracePeriodSeconds

### Negative

- **Complexity**: Shutdown logic in every service component
- **Testing difficulty**: Hard to test all shutdown scenarios
- **Timeout tuning**: Wrong timeout causes data loss or slow deploys
- **Partial shutdown**: Services may complete at different speeds

### Kubernetes Integration

```yaml
# Pod spec
spec:
  terminationGracePeriodSeconds: 45  # > our 30s timeout
  containers:
  - name: strata
    lifecycle:
      preStop:
        exec:
          command: ["/bin/sh", "-c", "sleep 5"]  # Allow LB to drain
```

### Health Check Coordination

During shutdown:
- Readiness probe returns unhealthy immediately
- Liveness probe remains healthy until timeout
- Load balancers stop sending new traffic

### Implications

- All async tasks must be cancellation-safe or use shutdown channel
- Long-running operations should check for shutdown periodically
- Metrics should track shutdown duration and success rate
- Runbooks should document expected shutdown behavior

## References

- `src/shutdown.rs` - ShutdownCoordinator implementation
- `src/lib.rs:265-277` - Server shutdown integration
- Kubernetes documentation on pod termination
