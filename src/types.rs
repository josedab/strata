//! Core type definitions for the Strata distributed file system.
//!
//! This module contains the fundamental data types used throughout Strata,
//! including inode structures, chunk identifiers, and cluster configuration.
//!
//! # Key Types
//!
//! - [`ChunkId`]: Unique identifier for data chunks (UUID-based)
//! - [`Inode`]: File/directory metadata (POSIX-compatible)
//! - [`Directory`]: Directory entries and structure
//! - [`ErasureCodingConfig`]: Reed-Solomon coding parameters
//!
//! # Type Aliases
//!
//! Common identifiers are defined as type aliases for clarity:
//!
//! - [`InodeId`] = `u64`: Inode number
//! - [`NodeId`] = `u64`: Cluster node identifier
//! - [`Term`] = `u64`: Raft term number
//! - [`LogIndex`] = `u64`: Raft log position
//!
//! # Examples
//!
//! ## Creating Files and Directories
//!
//! ```rust
//! use strata::types::{Inode, Directory, FileType};
//!
//! // Create a new file inode
//! let file = Inode::new_file(1, 0o644, 1000, 1000);
//! assert!(file.is_file());
//!
//! // Create a new directory
//! let dir_inode = Inode::new_directory(2, 0o755, 1000, 1000);
//! let mut dir = Directory::new(2, 1);
//! dir.add_entry("file.txt".into(), 1, FileType::RegularFile);
//! ```
//!
//! ## Working with Chunks
//!
//! ```rust
//! use strata::types::ChunkId;
//!
//! let chunk_id = ChunkId::new();
//! println!("Chunk: {}", chunk_id);
//!
//! // Chunks can be created from bytes
//! let bytes = chunk_id.as_bytes();
//! let same_id = ChunkId::from_bytes(*bytes);
//! assert_eq!(chunk_id, same_id);
//! ```
//!
//! ## Erasure Coding Configuration
//!
//! ```rust
//! use strata::types::ErasureCodingConfig;
//!
//! // Default 4+2 configuration (can tolerate 2 node failures)
//! let config = ErasureCodingConfig::DEFAULT;
//! assert_eq!(config.total_shards(), 6);
//! assert_eq!(config.storage_overhead(), 1.5); // 50% overhead
//!
//! // Cost-optimized 10+4 for larger deployments
//! let cost_opt = ErasureCodingConfig::COST_OPTIMIZED;
//! assert!(cost_opt.storage_overhead() < 1.5); // Only 40% overhead
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

/// Unique identifier for an inode.
pub type InodeId = u64;

/// Unique identifier for a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkId(pub Uuid);

impl ChunkId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for ChunkId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a node in the cluster.
pub type NodeId = u64;

/// Unique identifier for a data server.
pub type DataServerId = u64;

/// File handle identifier.
pub type FileHandle = u64;

/// Raft term number.
pub type Term = u64;

/// Raft log index.
pub type LogIndex = u64;

/// File type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

impl FileType {
    pub fn to_mode(&self) -> u32 {
        match self {
            FileType::RegularFile => libc::S_IFREG as u32,
            FileType::Directory => libc::S_IFDIR as u32,
            FileType::Symlink => libc::S_IFLNK as u32,
        }
    }
}

/// Inode structure representing a file or directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inode {
    pub id: InodeId,
    pub file_type: FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub nlink: u32,
    pub chunks: Vec<ChunkId>,
    pub symlink_target: Option<String>,
    pub extended_attrs: HashMap<String, Vec<u8>>,
    pub generation: u64,
}

impl Inode {
    pub fn new_file(id: InodeId, mode: u32, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            file_type: FileType::RegularFile,
            mode: mode | libc::S_IFREG as u32,
            uid,
            gid,
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 1,
            chunks: Vec::new(),
            symlink_target: None,
            extended_attrs: HashMap::new(),
            generation: 1,
        }
    }

    pub fn new_directory(id: InodeId, mode: u32, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            file_type: FileType::Directory,
            mode: mode | libc::S_IFDIR as u32,
            uid,
            gid,
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 2, // . and parent link
            chunks: Vec::new(),
            symlink_target: None,
            extended_attrs: HashMap::new(),
            generation: 1,
        }
    }

    pub fn new_symlink(id: InodeId, uid: u32, gid: u32, target: String) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            file_type: FileType::Symlink,
            mode: 0o777 | libc::S_IFLNK as u32,
            uid,
            gid,
            size: target.len() as u64,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 1,
            chunks: Vec::new(),
            symlink_target: Some(target),
            extended_attrs: HashMap::new(),
            generation: 1,
        }
    }

    pub fn is_dir(&self) -> bool {
        self.file_type == FileType::Directory
    }

    pub fn is_file(&self) -> bool {
        self.file_type == FileType::RegularFile
    }

    pub fn is_symlink(&self) -> bool {
        self.file_type == FileType::Symlink
    }

    pub fn touch(&mut self) {
        let now = SystemTime::now();
        self.mtime = now;
        self.ctime = now;
    }
}

/// Directory entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub inode: InodeId,
    pub file_type: FileType,
}

/// Directory structure containing entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Directory {
    pub inode_id: InodeId,
    pub parent: InodeId,
    pub entries: HashMap<String, DirEntry>,
}

impl Directory {
    pub fn new(inode_id: InodeId, parent: InodeId) -> Self {
        let mut entries = HashMap::new();

        // Add . and .. entries
        entries.insert(
            ".".to_string(),
            DirEntry {
                name: ".".to_string(),
                inode: inode_id,
                file_type: FileType::Directory,
            },
        );
        entries.insert(
            "..".to_string(),
            DirEntry {
                name: "..".to_string(),
                inode: parent,
                file_type: FileType::Directory,
            },
        );

        Self {
            inode_id,
            parent,
            entries,
        }
    }

    pub fn add_entry(&mut self, name: String, inode: InodeId, file_type: FileType) {
        self.entries.insert(
            name.clone(),
            DirEntry {
                name,
                inode,
                file_type,
            },
        );
    }

    pub fn remove_entry(&mut self, name: &str) -> Option<DirEntry> {
        if name == "." || name == ".." {
            return None;
        }
        self.entries.remove(name)
    }

    pub fn get_entry(&self, name: &str) -> Option<&DirEntry> {
        self.entries.get(name)
    }

    pub fn is_empty(&self) -> bool {
        // Only . and .. entries
        self.entries.len() <= 2
    }
}

/// Chunk metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub id: ChunkId,
    pub size: u64,
    pub checksum: u32,
    pub locations: Vec<DataServerId>,
    pub version: u64,
}

impl ChunkMeta {
    pub fn new(id: ChunkId, size: u64, checksum: u32) -> Self {
        Self {
            id,
            size,
            checksum,
            locations: Vec::new(),
            version: 1,
        }
    }
}

/// Lease for file handles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub handle: FileHandle,
    pub inode: InodeId,
    pub holder: NodeId,
    pub expires: SystemTime,
    pub exclusive: bool,
}

/// Data server information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataServerInfo {
    pub id: DataServerId,
    pub address: String,
    pub capacity: u64,
    pub used: u64,
    pub status: ServerStatus,
    pub last_heartbeat: SystemTime,
}

impl DataServerInfo {
    pub fn available_space(&self) -> u64 {
        self.capacity.saturating_sub(self.used)
    }

    pub fn usage_percent(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        (self.used as f64 / self.capacity as f64) * 100.0
    }
}

/// Server status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerStatus {
    Online,
    Offline,
    Draining,
    Maintenance,
}

/// Erasure coding configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ErasureCodingConfig {
    /// Number of data shards (k).
    pub data_shards: usize,
    /// Number of parity shards (m).
    pub parity_shards: usize,
}

impl ErasureCodingConfig {
    /// Default 4+2 configuration (1.5x storage overhead).
    pub const DEFAULT: Self = Self {
        data_shards: 4,
        parity_shards: 2,
    };

    /// 8+4 for larger clusters.
    pub const LARGE_CLUSTER: Self = Self {
        data_shards: 8,
        parity_shards: 4,
    };

    /// 10+4 for cost-optimized storage (1.4x overhead).
    pub const COST_OPTIMIZED: Self = Self {
        data_shards: 10,
        parity_shards: 4,
    };

    /// 2+1 for small clusters.
    pub const SMALL_CLUSTER: Self = Self {
        data_shards: 2,
        parity_shards: 1,
    };

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    pub fn storage_overhead(&self) -> f64 {
        self.total_shards() as f64 / self.data_shards as f64
    }

    pub fn min_required_shards(&self) -> usize {
        self.data_shards
    }
}

impl Default for ErasureCodingConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Chunk size configuration.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// S3 bucket configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    pub name: String,
    pub created: SystemTime,
    pub owner: String,
    pub versioning: bool,
}

/// Cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub metadata_nodes: Vec<String>,
    pub data_nodes: Vec<String>,
    pub erasure_config: ErasureCodingConfig,
    pub chunk_size: usize,
    pub replication_factor: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: Uuid::new_v4().to_string(),
            metadata_nodes: Vec::new(),
            data_nodes: Vec::new(),
            erasure_config: ErasureCodingConfig::default(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            replication_factor: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_id_creation() {
        let id1 = ChunkId::new();
        let id2 = ChunkId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_inode_creation() {
        let inode = Inode::new_file(1, 0o644, 1000, 1000);
        assert!(inode.is_file());
        assert!(!inode.is_dir());
        assert_eq!(inode.nlink, 1);
    }

    #[test]
    fn test_directory_operations() {
        let mut dir = Directory::new(1, 0);
        assert!(dir.is_empty());

        dir.add_entry("test.txt".to_string(), 2, FileType::RegularFile);
        assert!(!dir.is_empty());

        let entry = dir.get_entry("test.txt");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().inode, 2);

        dir.remove_entry("test.txt");
        assert!(dir.is_empty());
    }

    #[test]
    fn test_erasure_config() {
        let config = ErasureCodingConfig::DEFAULT;
        assert_eq!(config.total_shards(), 6);
        assert_eq!(config.min_required_shards(), 4);
        assert!((config.storage_overhead() - 1.5).abs() < 0.001);
    }
}
