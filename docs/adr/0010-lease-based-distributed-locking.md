# ADR-0010: Lease-Based Distributed Locking

## Status

**Accepted**

## Context

Distributed systems often need coordination mechanisms to prevent conflicting operations:
- Two clients shouldn't write to the same file region simultaneously
- Garbage collection shouldn't delete chunks while they're being read
- Cluster rebalancing shouldn't move a chunk while it's being written

Traditional mutexes don't work across network boundaries. We needed a distributed locking mechanism that handles:
- **Network partitions**: Client may be unreachable but still running
- **Client crashes**: Client may die without releasing lock
- **Split-brain**: Multiple clients may think they hold the lock

### The Deadlock Problem

Consider persistent locks without expiration:
1. Client A acquires lock on file F
2. Client A crashes (or network partitions)
3. Lock is never released
4. File F is permanently inaccessible

This is a critical failure mode for a storage system.

## Decision

We implement **lease-based locks** that automatically expire:

```rust
// src/lock.rs
pub struct Lock {
    /// What is being locked
    pub scope: LockScope,
    /// Who holds the lock
    pub holder: ClientId,
    /// When the lock was acquired
    pub acquired_at: SystemTime,
    /// When the lock expires if not renewed
    pub expires_at: SystemTime,
    /// Unique token for fencing
    pub token: LockToken,
}

pub enum LockScope {
    /// Lock on an inode (file or directory)
    Inode(InodeId),
    /// Lock on a path
    Path(PathBuf),
    /// Lock on a byte range within a file
    ByteRange { inode: InodeId, start: u64, end: u64 },
    /// Named lock for custom coordination
    Named(String),
}
```

### Lease Semantics

1. **Acquire**: Client requests lock with duration (e.g., 30 seconds)
2. **Grant**: Server grants lock with expiration time and unique token
3. **Renew**: Client periodically renews before expiration
4. **Release**: Client explicitly releases, or lease expires
5. **Fencing**: Operations include lock token; server rejects stale tokens

```rust
impl LockManager {
    pub async fn acquire(
        &self,
        scope: LockScope,
        holder: ClientId,
        duration: Duration,
    ) -> Result<Lock> {
        let now = SystemTime::now();
        let expires_at = now + duration;
        let token = LockToken::new();

        // Check for existing lock
        if let Some(existing) = self.locks.get(&scope) {
            if existing.expires_at > now {
                return Err(StrataError::LockHeld {
                    scope,
                    holder: existing.holder,
                    expires_at: existing.expires_at,
                });
            }
            // Existing lock expired, can be overwritten
        }

        let lock = Lock {
            scope: scope.clone(),
            holder,
            acquired_at: now,
            expires_at,
            token,
        };

        self.locks.insert(scope, lock.clone());
        Ok(lock)
    }

    pub async fn renew(&self, token: LockToken, duration: Duration) -> Result<Lock> {
        let lock = self.locks.get_by_token(&token)
            .ok_or(StrataError::LockNotFound)?;

        if lock.expires_at < SystemTime::now() {
            return Err(StrataError::LockExpired);
        }

        let new_expires = SystemTime::now() + duration;
        self.locks.update_expiration(token, new_expires);

        Ok(lock.with_expiration(new_expires))
    }
}
```

### Fencing with Lock Tokens

Lock tokens prevent stale operations after lock expiration:

```rust
pub async fn write_with_lock(
    &self,
    inode: InodeId,
    data: &[u8],
    lock_token: LockToken,
) -> Result<()> {
    // Verify lock is still valid
    let lock = self.lock_manager.get_by_token(&lock_token)
        .ok_or(StrataError::LockNotFound)?;

    if lock.expires_at < SystemTime::now() {
        return Err(StrataError::LockExpired);
    }

    if !matches!(lock.scope, LockScope::Inode(id) if id == inode) {
        return Err(StrataError::LockScopeMismatch);
    }

    // Proceed with write
    self.do_write(inode, data).await
}
```

### Client-Side Lease Renewal

Clients must renew leases before expiration:

```rust
impl LockHandle {
    pub fn start_renewal_task(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let renewal_interval = self.lock.duration / 3;

            loop {
                tokio::time::sleep(renewal_interval).await;

                match self.client.renew_lock(self.lock.token).await {
                    Ok(new_lock) => {
                        self.lock = new_lock;
                    }
                    Err(e) => {
                        warn!("Lock renewal failed: {}", e);
                        // Lock will expire; operations will fail with fencing
                        break;
                    }
                }
            }
        })
    }
}
```

## Consequences

### Positive

- **No permanent deadlocks**: Leases expire automatically
- **Partition tolerance**: Network issues cause lock loss, not permanent blocking
- **Crash safety**: Client crash = lock expires after timeout
- **Fencing tokens**: Prevents stale operations after lock loss
- **Flexible scopes**: Lock files, byte ranges, or custom resources

### Negative

- **Renewal overhead**: Clients must maintain background renewal tasks
- **Clock dependency**: Requires reasonably synchronized clocks
- **Unavailability window**: After crash, resource locked until lease expires
- **Complexity**: More complex than simple mutexes

### Lease Duration Guidelines

| Scenario | Duration | Renewal | Rationale |
|----------|----------|---------|-----------|
| Short operations | 10s | 3s | Quick recovery if client fails |
| Long operations | 60s | 20s | Reduced renewal overhead |
| Batch processing | 300s | 100s | Minimize interruptions |

### Clock Skew Considerations

Lock validity depends on time comparisons. With clock skew:
- Server at T=100, Client at T=105
- Server grants 30s lease, expires at T=130 (server time)
- Client thinks it has until T=135
- At server T=130, lock expires; client still thinks it's valid

**Mitigation**: Use conservative lease durations, account for reasonable skew (~5s).

### Operational Implications

- **Monitoring**: Track active locks, expiration times, renewal rates
- **Debugging**: Log lock acquisitions and releases with tokens
- **Tuning**: Adjust lease durations based on operation patterns
- **Clock sync**: Ensure NTP is running on all nodes

## References

- [Chubby Lock Service](https://research.google/pubs/pub27897/) - Google's distributed lock service
- [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) - Martin Kleppmann
- `src/lock.rs` - LockManager implementation
- `src/metadata/operations.rs` - Lock-related metadata operations
