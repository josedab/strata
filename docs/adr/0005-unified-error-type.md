# ADR-0005: Unified Error Type with Retryability Classification

## Status

**Accepted**

## Context

Distributed systems have many failure modes:
- Network timeouts
- Node unavailability
- Consensus failures (not leader, no quorum)
- Storage errors (disk full, corruption)
- Client errors (invalid input, not found)
- Authentication/authorization failures

Different failure types require different handling:
- **Transient failures** should be retried (timeout, temporary unavailability)
- **Permanent failures** should not be retried (not found, permission denied)
- **Client errors** should be returned immediately with helpful messages

We needed an error handling strategy that:
1. Distinguishes error categories for intelligent retry logic
2. Maps to POSIX errno for FUSE compatibility
3. Provides context for debugging
4. Is type-safe and exhaustive

### Options Considered

1. **Generic `Box<dyn Error>`**: Flexible but loses type information, no compile-time handling
2. **String error messages**: Simple but no structured handling possible
3. **Multiple domain-specific error enums**: Type-safe per domain but fragmented
4. **Single unified error enum**: Comprehensive but potentially large

## Decision

We implemented a **single `StrataError` enum** with 40+ variants covering all failure domains:

```rust
// src/error.rs
#[derive(Debug, Error)]
pub enum StrataError {
    // Consensus errors
    #[error("Not the Raft leader, current leader: {0:?}")]
    NotLeader(Option<NodeId>),

    #[error("No quorum available for operation")]
    NoQuorum,

    // Storage errors
    #[error("Inode not found: {0}")]
    InodeNotFound(InodeId),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(ChunkId),

    // Network errors
    #[error("Connection timeout to {0}")]
    ConnectionTimeout(String),

    #[error("Data server unavailable: {0}")]
    DataServerUnavailable(NodeId),

    // Client errors
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    // ... 35+ more variants
}
```

### Retryability Classification

The `is_retryable()` method categorizes errors:

```rust
impl StrataError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StrataError::NotLeader(_)
            | StrataError::ConnectionTimeout(_)
            | StrataError::DataServerUnavailable(_)
            | StrataError::NetworkTimeout
            | StrataError::TemporaryUnavailable
            | StrataError::RateLimited { .. }
            | StrataError::LeadershipLost
            | StrataError::ConsensusTimeout
        )
    }
}
```

### POSIX errno Mapping

For FUSE compatibility, errors map to errno values:

```rust
impl StrataError {
    pub fn to_errno(&self) -> i32 {
        match self {
            StrataError::InodeNotFound(_) => libc::ENOENT,
            StrataError::PermissionDenied(_) => libc::EACCES,
            StrataError::NotADirectory(_) => libc::ENOTDIR,
            StrataError::IsADirectory(_) => libc::EISDIR,
            StrataError::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            StrataError::FileTooLarge => libc::EFBIG,
            StrataError::NoSpace => libc::ENOSPC,
            StrataError::QuotaExceeded { .. } => libc::EDQUOT,
            StrataError::ReadOnlyFilesystem => libc::EROFS,
            _ => libc::EIO, // Generic I/O error for unknown cases
        }
    }
}
```

### Result Type Alias

A convenience alias reduces boilerplate:

```rust
pub type Result<T> = std::result::Result<T, StrataError>;
```

## Consequences

### Positive

- **Compile-time exhaustiveness**: `match` statements must handle all variants
- **Intelligent retry logic**: Clients can automatically retry transient failures
- **FUSE compatibility**: Errors translate correctly to POSIX semantics
- **Rich context**: Each variant carries relevant data (node IDs, paths, etc.)
- **Single import**: One error type across entire codebase

### Negative

- **Large enum**: 40+ variants can be unwieldy to maintain
- **Not extensible**: Adding variants requires modifying central enum
- **Binary size**: All variants compiled even if unused in some contexts
- **Potential for catch-all**: Easy to match `_` and miss specific handling

### Usage Patterns

**Retry loop with exponential backoff:**
```rust
async fn with_retry<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempts = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempts < 3 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(attempts))).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

**Pattern matching for specific handling:**
```rust
match result {
    Ok(data) => process(data),
    Err(StrataError::NotLeader(Some(leader))) => {
        // Redirect to leader
        redirect_to(leader).await
    }
    Err(StrataError::QuotaExceeded { used, limit, .. }) => {
        warn!("Quota exceeded: {}/{}", used, limit);
        Err(StrataError::QuotaExceeded { used, limit, path: None })
    }
    Err(e) => Err(e),
}
```

### Operational Implications

- **Logging**: Error variants provide structured fields for log aggregation
- **Metrics**: Can count errors by variant for alerting
- **Client SDKs**: Can expose retry logic based on `is_retryable()`

## References

- `src/error.rs` - Full StrataError implementation
- `src/fuse/filesystem.rs` - FUSE error handling using `to_errno()`
- `src/client/mod.rs` - Client retry logic using `is_retryable()`
