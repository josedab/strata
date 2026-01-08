# Error Handling API Reference

This document describes error handling patterns in Strata.

## Result Type

Strata uses a custom `Result` type alias:

```rust
pub type Result<T> = std::result::Result<T, StrataError>;
```

## StrataError

The main error type for all Strata operations.

```rust
use strata::error::{Result, StrataError};

fn my_function() -> Result<()> {
    // Use ? operator for error propagation
    something_that_might_fail()?;
    Ok(())
}
```

### Error Categories

#### Raft Consensus Errors

| Error | Description |
|-------|-------------|
| `NotLeader { leader: Option<u64> }` | Operation requires leader, redirects to current leader |
| `RaftConsensus(String)` | General consensus failure |
| `RaftLog(String)` | Log replication error |
| `ElectionTimeout` | Election did not complete in time |
| `QuorumNotReached { got, need }` | Insufficient nodes for quorum |

**Example:**
```rust
match result {
    Err(StrataError::NotLeader { leader: Some(id) }) => {
        println!("Redirect to leader node {}", id);
    }
    Err(StrataError::NotLeader { leader: None }) => {
        println!("No leader available, retry later");
    }
    _ => {}
}
```

#### Metadata Errors

| Error | Description |
|-------|-------------|
| `InodeNotFound(u64)` | Inode does not exist |
| `FileNotFound(String)` | File path not found |
| `DirectoryNotFound(String)` | Directory path not found |
| `FileExists(String)` | File already exists |
| `NotADirectory(String)` | Expected directory, got file |
| `NotAFile(String)` | Expected file, got directory |
| `DirectoryNotEmpty(String)` | Cannot delete non-empty directory |
| `InvalidPath(String)` | Malformed path |
| `PermissionDenied(String)` | Access denied |

#### Chunk/Data Errors

| Error | Description |
|-------|-------------|
| `ChunkNotFound(String)` | Chunk does not exist |
| `InsufficientShards { available, required }` | Not enough shards for reconstruction |
| `ShardCorruption { index }` | Shard data is corrupted |
| `ChecksumMismatch { expected, actual }` | Data integrity failure |
| `DataServerUnavailable(String)` | Cannot reach data server |
| `WriteFailed(String)` | Write operation failed |
| `ReadFailed(String)` | Read operation failed |

#### Cluster Errors

| Error | Description |
|-------|-------------|
| `NodeNotFound(u64)` | Node not in cluster |
| `ClusterNotReady` | Cluster not initialized |
| `PlacementFailed(String)` | Cannot place data |
| `RecoveryFailed(String)` | Data recovery failed |
| `RebalanceFailed(String)` | Rebalancing operation failed |

#### Configuration Errors

| Error | Description |
|-------|-------------|
| `Config(String)` | General configuration error |
| `InvalidConfig { field, reason }` | Specific field is invalid |

#### Storage Errors

| Error | Description |
|-------|-------------|
| `Storage(String)` | General storage error |
| `CapacityExceeded(String)` | Storage full |
| `QuotaExceeded(String)` | User/path quota exceeded |

#### Network Errors

| Error | Description |
|-------|-------------|
| `ConnectionFailed(String)` | Cannot establish connection |
| `Timeout(u64)` | Operation timed out (milliseconds) |
| `Network(String)` | General network error |

#### Serialization Errors

| Error | Description |
|-------|-------------|
| `Serialization(String)` | Failed to serialize |
| `Deserialization(String)` | Failed to deserialize |

#### External Errors

| Error | Description |
|-------|-------------|
| `Io(std::io::Error)` | Standard I/O error |
| `RocksDb(String)` | RocksDB storage error |
| `Internal(String)` | Internal/unexpected error |

### Error Methods

#### to_errno

Convert error to POSIX errno for FUSE operations:

```rust
impl StrataError {
    pub fn to_errno(&self) -> i32;
}
```

**Mapping:**

| Error | Errno | Value |
|-------|-------|-------|
| `FileNotFound` | `ENOENT` | 2 |
| `FileExists` | `EEXIST` | 17 |
| `NotADirectory` | `ENOTDIR` | 20 |
| `NotAFile` | `EISDIR` | 21 |
| `DirectoryNotEmpty` | `ENOTEMPTY` | 39 |
| `InvalidPath` | `EINVAL` | 22 |
| `PermissionDenied` | `EACCES` | 13 |
| `QuotaExceeded` | `EDQUOT` | 122 |
| `CapacityExceeded` | `ENOSPC` | 28 |
| `NotLeader` | `EAGAIN` | 11 |
| `Timeout` | `ETIMEDOUT` | 110 |
| Default | `EIO` | 5 |

**Example:**
```rust
use strata::error::StrataError;

let err = StrataError::FileNotFound("/path/to/file".into());
assert_eq!(err.to_errno(), libc::ENOENT);
```

#### is_retryable

Check if an error is transient and can be retried:

```rust
impl StrataError {
    pub fn is_retryable(&self) -> bool;
}
```

**Retryable Errors:**
- `NotLeader` - Leader may have changed
- `Timeout` - Operation may succeed on retry
- `DataServerUnavailable` - Server may recover
- `ClusterNotReady` - Cluster may initialize
- `QuorumNotReached` - Nodes may become available
- `Unavailable` - Service may recover

**Example:**
```rust
use strata::error::StrataError;
use std::time::Duration;

async fn with_retry<F, T>(mut f: F, max_retries: u32) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut attempt = 0;
    loop {
        match f() {
            Ok(result) => return Ok(result),
            Err(e) if e.is_retryable() && attempt < max_retries => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Error Conversions

StrataError implements `From` for common error types:

```rust
// From std::io::Error
impl From<std::io::Error> for StrataError;

// From RocksDB errors
impl From<rocksdb::Error> for StrataError;

// From serialization errors
impl From<bincode::Error> for StrataError;
impl From<serde_json::Error> for StrataError;
```

**Example:**
```rust
use strata::error::Result;

fn read_file(path: &str) -> Result<Vec<u8>> {
    // std::io::Error automatically converts to StrataError
    let data = std::fs::read(path)?;
    Ok(data)
}
```

## Best Practices

### Use the ? Operator

```rust
use strata::error::{Result, StrataError};

fn process_file(path: &str) -> Result<()> {
    let data = read_data(path)?;
    let parsed = parse_data(&data)?;
    save_result(parsed)?;
    Ok(())
}
```

### Create Specific Errors

```rust
use strata::error::{Result, StrataError};

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(StrataError::InvalidPath("path cannot be empty".into()));
    }
    if !path.starts_with('/') {
        return Err(StrataError::InvalidPath("path must be absolute".into()));
    }
    Ok(())
}
```

### Handle Retryable Errors

```rust
use strata::error::{Result, StrataError};

fn handle_operation_result(result: Result<()>) {
    match result {
        Ok(()) => println!("Success"),
        Err(ref e) if e.is_retryable() => {
            println!("Transient error, will retry: {}", e);
        }
        Err(e) => {
            println!("Fatal error: {}", e);
        }
    }
}
```

### Log Errors with Context

```rust
use strata::error::{Result, StrataError};
use tracing::{error, warn};

fn process_with_logging(id: u64) -> Result<()> {
    match do_operation(id) {
        Ok(result) => Ok(result),
        Err(ref e) if e.is_retryable() => {
            warn!(id = %id, error = %e, "Retryable error");
            Err(e)
        }
        Err(e) => {
            error!(id = %id, error = %e, "Operation failed");
            Err(e)
        }
    }
}
```

## See Also

- [Core Types](types.md) - Data types
- [Client Library](client.md) - Client API
