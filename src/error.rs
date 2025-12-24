//! Error types for the Strata distributed file system.
//!
//! This module provides a unified error type [`StrataError`] for all Strata operations,
//! along with a convenient [`Result`] type alias.
//!
//! # Error Categories
//!
//! Errors are organized into the following categories:
//!
//! - **Raft Consensus**: Errors related to leader election and log replication
//! - **Metadata**: File and directory operation errors
//! - **Data/Storage**: Chunk storage and erasure coding errors
//! - **Cluster**: Node management and placement errors
//! - **Network**: Connection and timeout errors
//! - **Configuration**: Invalid settings or missing configuration
//!
//! # Example
//!
//! ```rust
//! use strata::error::{Result, StrataError};
//!
//! fn read_file(path: &str) -> Result<Vec<u8>> {
//!     if path.is_empty() {
//!         return Err(StrataError::InvalidPath("path cannot be empty".into()));
//!     }
//!     // ... read file
//!     Ok(vec![])
//! }
//!
//! fn handle_error(err: &StrataError) {
//!     if err.is_retryable() {
//!         println!("Retrying operation...");
//!     } else {
//!         println!("Fatal error: {}", err);
//!     }
//! }
//! ```
//!
//! # FUSE Integration
//!
//! Errors can be converted to POSIX errno values for FUSE operations:
//!
//! ```rust,ignore
//! use strata::error::StrataError;
//!
//! let err = StrataError::FileNotFound("/path/to/file".into());
//! assert_eq!(err.to_errno(), libc::ENOENT);
//! ```

use std::io;
use thiserror::Error;

/// Main error type for Strata operations.
#[derive(Error, Debug)]
pub enum StrataError {
    // Raft consensus errors
    #[error("Not the leader. Leader is: {leader:?}")]
    NotLeader { leader: Option<u64> },

    #[error("Raft consensus failed: {0}")]
    RaftConsensus(String),

    #[error("Raft log error: {0}")]
    RaftLog(String),

    #[error("Election timeout")]
    ElectionTimeout,

    #[error("Quorum not reached: got {got}, need {need}")]
    QuorumNotReached { got: usize, need: usize },

    // Metadata errors
    #[error("Inode not found: {0}")]
    InodeNotFound(u64),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),

    #[error("File already exists: {0}")]
    FileExists(String),

    #[error("Not a directory: {0}")]
    NotADirectory(String),

    #[error("Not a file: {0}")]
    NotAFile(String),

    #[error("Directory not empty: {0}")]
    DirectoryNotEmpty(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    // Chunk and data errors
    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    #[error("Insufficient shards: have {available}, need {required}")]
    InsufficientShards { available: usize, required: usize },

    #[error("Shard corruption detected at index {index}")]
    ShardCorruption { index: usize },

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Data server unavailable: {0}")]
    DataServerUnavailable(String),

    #[error("Write failed: {0}")]
    WriteFailed(String),

    #[error("Read failed: {0}")]
    ReadFailed(String),

    // Cluster errors
    #[error("Node not found: {0}")]
    NodeNotFound(u64),

    #[error("Cluster not ready")]
    ClusterNotReady,

    #[error("Placement failed: {0}")]
    PlacementFailed(String),

    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    #[error("Rebalance failed: {0}")]
    RebalanceFailed(String),

    // Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Invalid configuration: {field}: {reason}")]
    InvalidConfig { field: String, reason: String },

    // Storage errors
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Capacity exceeded: {0}")]
    CapacityExceeded(String),

    #[error("Quota exceeded for path: {0}")]
    QuotaExceeded(String),

    // Network errors
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request timeout after {0}ms")]
    Timeout(u64),

    #[error("Operation timed out: {0}")]
    TimeoutStr(String),

    #[error("Network error: {0}")]
    Network(String),

    // Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    // External errors
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("Internal error: {0}")]
    Internal(String),

    // Additional error variants
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Data corruption: {0}")]
    DataCorruption(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Unavailable: {0}")]
    Unavailable(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("Buffer full: {0}")]
    BufferFull(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("External error: {0}")]
    External(String),

    #[error("Rate limited: {0}")]
    RateLimited(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Not connected: {0}")]
    NotConnected(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl StrataError {
    /// Convert to POSIX errno for FUSE operations.
    pub fn to_errno(&self) -> i32 {
        match self {
            StrataError::InodeNotFound(_) | StrataError::FileNotFound(_) | StrataError::NotFound(_) => libc::ENOENT,
            StrataError::DirectoryNotFound(_) => libc::ENOENT,
            StrataError::FileExists(_) => libc::EEXIST,
            StrataError::NotADirectory(_) => libc::ENOTDIR,
            StrataError::NotAFile(_) => libc::EISDIR,
            StrataError::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            StrataError::InvalidPath(_) => libc::EINVAL,
            StrataError::PermissionDenied(_) => libc::EACCES,
            StrataError::QuotaExceeded(_) => libc::EDQUOT,
            StrataError::CapacityExceeded(_) => libc::ENOSPC,
            StrataError::NotLeader { .. } => libc::EAGAIN,
            StrataError::Timeout(_) => libc::ETIMEDOUT,
            StrataError::Io(e) => e.raw_os_error().unwrap_or(libc::EIO),
            StrataError::AlreadyExists(_) => libc::EEXIST,
            StrataError::Forbidden(_) => libc::EACCES,
            StrataError::InvalidOperation(_) => libc::EINVAL,
            StrataError::ResourceExhausted(_) => libc::ENOSPC,
            StrataError::DataCorruption(_) => libc::EIO,
            StrataError::Unavailable(_) => libc::EAGAIN,
            StrataError::Conflict(_) => libc::EBUSY,
            StrataError::PreconditionFailed(_) => libc::EINVAL,
            _ => libc::EIO,
        }
    }

    /// Check if error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StrataError::NotLeader { .. }
                | StrataError::Timeout(_)
                | StrataError::DataServerUnavailable(_)
                | StrataError::ClusterNotReady
                | StrataError::QuorumNotReached { .. }
                | StrataError::Unavailable(_)
        )
    }
}

impl From<rocksdb::Error> for StrataError {
    fn from(e: rocksdb::Error) -> Self {
        StrataError::RocksDb(e.to_string())
    }
}

impl From<bincode::Error> for StrataError {
    fn from(e: bincode::Error) -> Self {
        StrataError::Serialization(e.to_string())
    }
}

impl From<serde_json::Error> for StrataError {
    fn from(e: serde_json::Error) -> Self {
        StrataError::Serialization(e.to_string())
    }
}

/// Result type alias for Strata operations.
pub type Result<T> = std::result::Result<T, StrataError>;
