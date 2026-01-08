# Core Types API Reference

This document describes the core data types used throughout Strata.

## Identifiers

### ChunkId

Unique identifier for data chunks.

```rust
pub struct ChunkId(pub Uuid);

impl ChunkId {
    /// Create a new random chunk ID
    pub fn new() -> Self;

    /// Create from raw bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self;

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8; 16];
}
```

**Example:**
```rust
use strata::types::ChunkId;

let chunk_id = ChunkId::new();
println!("Chunk: {}", chunk_id);

// Round-trip through bytes
let bytes = chunk_id.as_bytes();
let same_id = ChunkId::from_bytes(*bytes);
assert_eq!(chunk_id, same_id);
```

### Type Aliases

| Type | Definition | Description |
|------|------------|-------------|
| `InodeId` | `u64` | Inode number |
| `NodeId` | `u64` | Cluster node identifier |
| `DataServerId` | `u64` | Data server identifier |
| `FileHandle` | `u64` | Open file handle |
| `Term` | `u64` | Raft term number |
| `LogIndex` | `u64` | Raft log position |

## File System Types

### FileType

Enumeration of file types.

```rust
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

impl FileType {
    /// Convert to POSIX mode bits
    pub fn to_mode(&self) -> u32;
}
```

### Inode

File or directory metadata structure.

```rust
pub struct Inode {
    pub id: InodeId,
    pub file_type: FileType,
    pub mode: u32,           // POSIX permission bits
    pub uid: u32,            // Owner user ID
    pub gid: u32,            // Owner group ID
    pub size: u64,           // File size in bytes
    pub atime: SystemTime,   // Last access time
    pub mtime: SystemTime,   // Last modification time
    pub ctime: SystemTime,   // Last status change time
    pub nlink: u32,          // Hard link count
    pub chunks: Vec<ChunkId>, // Data chunks (files only)
    pub symlink_target: Option<String>,
    pub extended_attrs: HashMap<String, Vec<u8>>,
    pub generation: u64,
}
```

**Constructor Methods:**

```rust
impl Inode {
    /// Create a new file inode
    pub fn new_file(id: InodeId, mode: u32, uid: u32, gid: u32) -> Self;

    /// Create a new directory inode
    pub fn new_directory(id: InodeId, mode: u32, uid: u32, gid: u32) -> Self;

    /// Create a new symlink inode
    pub fn new_symlink(id: InodeId, uid: u32, gid: u32, target: String) -> Self;
}
```

**Query Methods:**

```rust
impl Inode {
    pub fn is_dir(&self) -> bool;
    pub fn is_file(&self) -> bool;
    pub fn is_symlink(&self) -> bool;
    pub fn touch(&mut self);  // Update mtime/ctime
}
```

**Example:**
```rust
use strata::types::{Inode, FileType};

// Create a file with 644 permissions
let file = Inode::new_file(1, 0o644, 1000, 1000);
assert!(file.is_file());
assert_eq!(file.nlink, 1);

// Create a directory with 755 permissions
let dir = Inode::new_directory(2, 0o755, 1000, 1000);
assert!(dir.is_dir());
assert_eq!(dir.nlink, 2); // . and ..
```

### Directory

Directory structure containing entries.

```rust
pub struct Directory {
    pub inode_id: InodeId,
    pub parent: InodeId,
    pub entries: HashMap<String, DirEntry>,
}

pub struct DirEntry {
    pub name: String,
    pub inode: InodeId,
    pub file_type: FileType,
}
```

**Methods:**

```rust
impl Directory {
    /// Create a new directory (adds . and .. entries)
    pub fn new(inode_id: InodeId, parent: InodeId) -> Self;

    /// Add an entry
    pub fn add_entry(&mut self, name: String, inode: InodeId, file_type: FileType);

    /// Remove an entry (cannot remove . or ..)
    pub fn remove_entry(&mut self, name: &str) -> Option<DirEntry>;

    /// Get an entry by name
    pub fn get_entry(&self, name: &str) -> Option<&DirEntry>;

    /// Check if directory is empty (only . and ..)
    pub fn is_empty(&self) -> bool;
}
```

**Example:**
```rust
use strata::types::{Directory, FileType};

let mut dir = Directory::new(1, 0); // Root directory
assert!(dir.is_empty());

dir.add_entry("file.txt".to_string(), 2, FileType::RegularFile);
assert!(!dir.is_empty());

let entry = dir.get_entry("file.txt").unwrap();
assert_eq!(entry.inode, 2);
```

## Chunk Types

### ChunkMeta

Metadata for a stored chunk.

```rust
pub struct ChunkMeta {
    pub id: ChunkId,
    pub size: u64,
    pub checksum: u32,         // CRC32 checksum
    pub locations: Vec<DataServerId>,
    pub version: u64,
}

impl ChunkMeta {
    pub fn new(id: ChunkId, size: u64, checksum: u32) -> Self;
}
```

### StoredChunk

Local chunk storage metadata (in data servers).

```rust
pub struct StoredChunk {
    pub id: ChunkId,
    pub shard_index: usize,
    pub size: u64,
    pub checksum: u32,         // CRC32
    pub sha256: [u8; 32],      // SHA256 for integrity
    pub version: u64,
}

impl StoredChunk {
    pub fn new(id: ChunkId, shard_index: usize, data: &[u8]) -> Self;

    /// Verify CRC32 checksum
    pub fn verify(&self, data: &[u8]) -> bool;

    /// Verify SHA256 hash
    pub fn verify_sha256(&self, data: &[u8]) -> bool;
}
```

## Erasure Coding

### ErasureCodingConfig

Reed-Solomon erasure coding parameters.

```rust
pub struct ErasureCodingConfig {
    pub data_shards: usize,    // k - data shards
    pub parity_shards: usize,  // m - parity shards
}

impl ErasureCodingConfig {
    /// Default 4+2 configuration (1.5x overhead)
    pub const DEFAULT: Self;

    /// 8+4 for larger clusters
    pub const LARGE_CLUSTER: Self;

    /// 10+4 for cost-optimized storage (1.4x overhead)
    pub const COST_OPTIMIZED: Self;

    /// 2+1 for small clusters
    pub const SMALL_CLUSTER: Self;

    /// Total shards (k + m)
    pub fn total_shards(&self) -> usize;

    /// Storage overhead ratio
    pub fn storage_overhead(&self) -> f64;

    /// Minimum shards needed to reconstruct data
    pub fn min_required_shards(&self) -> usize;
}
```

**Configuration Presets:**

| Preset | Data (k) | Parity (m) | Total | Overhead | Fault Tolerance |
|--------|----------|------------|-------|----------|-----------------|
| `SMALL_CLUSTER` | 2 | 1 | 3 | 1.50x | 1 failure |
| `DEFAULT` | 4 | 2 | 6 | 1.50x | 2 failures |
| `LARGE_CLUSTER` | 8 | 4 | 12 | 1.50x | 4 failures |
| `COST_OPTIMIZED` | 10 | 4 | 14 | 1.40x | 4 failures |

**Example:**
```rust
use strata::types::ErasureCodingConfig;

let config = ErasureCodingConfig::DEFAULT;
assert_eq!(config.total_shards(), 6);
assert_eq!(config.min_required_shards(), 4);
assert!((config.storage_overhead() - 1.5).abs() < 0.001);

// Cost-optimized has lower overhead
let cost_opt = ErasureCodingConfig::COST_OPTIMIZED;
assert!(cost_opt.storage_overhead() < 1.5);
```

## Server Types

### DataServerInfo

Information about a data server.

```rust
pub struct DataServerInfo {
    pub id: DataServerId,
    pub address: String,
    pub capacity: u64,
    pub used: u64,
    pub status: ServerStatus,
    pub last_heartbeat: SystemTime,
}

impl DataServerInfo {
    /// Available storage space
    pub fn available_space(&self) -> u64;

    /// Storage usage percentage
    pub fn usage_percent(&self) -> f64;
}
```

### ServerStatus

```rust
pub enum ServerStatus {
    Online,      // Normal operation
    Offline,     // Not responding
    Draining,    // Being decommissioned
    Maintenance, // Temporarily unavailable
}
```

## Cluster Configuration

### ClusterConfig

```rust
pub struct ClusterConfig {
    pub cluster_id: String,
    pub metadata_nodes: Vec<String>,
    pub data_nodes: Vec<String>,
    pub erasure_config: ErasureCodingConfig,
    pub chunk_size: usize,
    pub replication_factor: usize,
}
```

**Default Values:**
- `chunk_size`: 64MB (67,108,864 bytes)
- `replication_factor`: 3
- `erasure_config`: 4+2

## Lease Types

### Lease

File handle lease for concurrent access control.

```rust
pub struct Lease {
    pub handle: FileHandle,
    pub inode: InodeId,
    pub holder: NodeId,
    pub expires: SystemTime,
    pub exclusive: bool,
}
```

## S3 Types

### BucketConfig

```rust
pub struct BucketConfig {
    pub name: String,
    pub created: SystemTime,
    pub owner: String,
    pub versioning: bool,
}
```

## Constants

```rust
/// Default chunk size: 64MB
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;
```

## See Also

- [Error Handling](errors.md) - Error types
- [Client Library](client.md) - Client API
