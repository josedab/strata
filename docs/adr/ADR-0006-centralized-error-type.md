# ADR-0006: Centralized Error Type with POSIX Mapping

## Status

Accepted

## Context

Error handling in a distributed file system is complex:

1. **Diverse failure modes**: Network errors, disk errors, consensus failures, permission denied, quota exceeded, file not found, etc.

2. **Multiple interfaces**: Errors must be meaningful whether surfaced via FUSE, S3, or native client

3. **POSIX compatibility**: FUSE requires returning standard errno values that applications understand

4. **Retry decisions**: Some errors are transient (network timeout) while others are permanent (file not found)

5. **Debugging**: Error messages must be informative for operators

Without a unified approach, error handling becomes inconsistent:
- Different modules define their own error types
- POSIX mapping is ad-hoc and incomplete
- Retry logic is duplicated
- Error messages vary in quality

## Decision

We will define a **single `StrataError` enum** that covers all error conditions, with built-in conversions and metadata.

```rust
// src/error.rs
#[derive(Debug, Clone, thiserror::Error)]
pub enum StrataError {
    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Consensus error: {0}")]
    ConsensusError(String),

    // ... 40+ variants covering all failure scenarios
}

pub type Result<T> = std::result::Result<T, StrataError>;
```

### Key Features

**1. POSIX errno Mapping**

```rust
impl StrataError {
    pub fn to_errno(&self) -> i32 {
        match self {
            Self::NotFound(_) => libc::ENOENT,
            Self::PermissionDenied(_) => libc::EACCES,
            Self::QuotaExceeded(_) => libc::EDQUOT,
            Self::AlreadyExists(_) => libc::EEXIST,
            Self::NotADirectory(_) => libc::ENOTDIR,
            Self::IsADirectory(_) => libc::EISDIR,
            Self::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            // ... complete mapping
        }
    }
}
```

**2. Retryability Classification**

```rust
impl StrataError {
    pub fn is_retryable(&self) -> bool {
        matches!(self,
            Self::NetworkError(_) |
            Self::Timeout(_) |
            Self::ServiceUnavailable(_) |
            Self::LeadershipLost(_) |
            Self::ResourceBusy(_)
        )
    }
}
```

**3. Automatic Conversions**

```rust
impl From<std::io::Error> for StrataError { ... }
impl From<tokio::time::error::Elapsed> for StrataError { ... }
impl From<serde_json::Error> for StrataError { ... }
```

## Consequences

### Positive

- **Consistency**: All code uses the same error type and patterns
- **FUSE compatibility**: Automatic errno conversion for POSIX interface
- **Smart retries**: Clients can automatically retry transient failures
- **Rich context**: Error messages include relevant details
- **Type safety**: Compiler ensures error handling is exhaustive
- **Single source of truth**: One place to update error handling logic

### Negative

- **Large enum**: 40+ variants means a large type definition
- **Centralization**: All modules depend on `error.rs`
- **Message duplication**: Error context may be redundant with message
- **Breaking changes**: Adding variants is backwards-compatible, but removing isn't

### Error Categories

| Category | Examples | errno | Retryable |
|----------|----------|-------|-----------|
| Not Found | File, directory, chunk, node | ENOENT | No |
| Permission | Auth failed, ACL denied | EACCES/EPERM | No |
| Conflict | Already exists, version mismatch | EEXIST | No |
| Resource | Quota, rate limit, capacity | EDQUOT/ENOSPC | Maybe |
| Network | Timeout, connection refused | EIO | Yes |
| Consensus | No leader, leadership lost | EAGAIN | Yes |
| Internal | Corruption, invariant violation | EIO | No |

### Usage Pattern

```rust
use crate::error::{Result, StrataError};

pub async fn create_file(&self, parent: InodeId, name: &str) -> Result<Inode> {
    // Validate
    if name.is_empty() {
        return Err(StrataError::InvalidArgument("name cannot be empty".into()));
    }

    // Check parent exists
    let parent_inode = self.get_inode(parent).await?
        .ok_or_else(|| StrataError::NotFound(format!("parent inode {}", parent)))?;

    // Check not duplicate
    if self.lookup(parent, name).await?.is_some() {
        return Err(StrataError::AlreadyExists(format!("{}/{}", parent, name)));
    }

    // Proceed with creation...
}
```

### Implications

- New error conditions should be added to `StrataError`, not defined locally
- Error messages should be actionable (what failed, why, what to do)
- FUSE handler must call `to_errno()` on all errors
- HTTP APIs should map to appropriate status codes (404, 403, 409, 500, etc.)

## References

- `src/error.rs` - Error type definition
- `src/fuse/filesystem.rs` - FUSE error handling
- `src/client/mod.rs` - Client retry logic using `is_retryable()`
