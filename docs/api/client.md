# Client Library API Reference

Strata provides native Rust client libraries for interacting with metadata and data services.

## Overview

The client library consists of two main clients:

| Client | Purpose | Default Port |
|--------|---------|--------------|
| `MetadataClient` | File system operations (create, delete, lookup) | 9000 |
| `DataClient` | Chunk storage operations (read, write, delete) | 9001 |

Both clients use **HTTP/REST** with JSON serialization for communication.

## MetadataClient

The `MetadataClient` handles all file system namespace operations.

### Creating a Client

```rust
use strata::client::MetadataClient;
use std::net::SocketAddr;
use std::time::Duration;

// From SocketAddr
let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
let client = MetadataClient::new(addr);

// From address string
let client = MetadataClient::from_addr("127.0.0.1:9000")?;

// With custom timeouts
let client = MetadataClient::with_timeouts(
    addr,
    Duration::from_secs(10),  // connect timeout
    Duration::from_secs(60),  // request timeout
);
```

### Default Timeouts

| Timeout | Default Value |
|---------|---------------|
| Connect | 5 seconds |
| Request | 30 seconds |

### Operations

#### Health Check

```rust
/// Check if the metadata service is healthy
pub async fn health(&self) -> Result<bool>
```

**Example:**
```rust
if client.health().await? {
    println!("Metadata service is healthy");
}
```

#### Lookup

```rust
/// Look up a file or directory by name within a parent directory
pub async fn lookup(&self, parent: InodeId, name: &str) -> Result<LookupResponse>

pub struct LookupResponse {
    pub found: bool,
    pub inode: Option<Inode>,
}
```

**Example:**
```rust
let response = client.lookup(1, "myfile.txt").await?;
if response.found {
    let inode = response.inode.unwrap();
    println!("Found inode {} with size {}", inode.id, inode.size);
}
```

#### Create File

```rust
/// Create a new file
pub async fn create_file(
    &self,
    parent: InodeId,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<CreateResponse>

pub struct CreateResponse {
    pub success: bool,
    pub inode: Option<InodeId>,
    pub error: Option<String>,
}
```

**Example:**
```rust
// Create a file with 644 permissions
let response = client.create_file(
    1,           // parent inode (root)
    "newfile.txt",
    0o644,       // mode
    1000,        // uid
    1000,        // gid
).await?;

if response.success {
    println!("Created file with inode {}", response.inode.unwrap());
}
```

#### Create Directory

```rust
/// Create a new directory
pub async fn create_directory(
    &self,
    parent: InodeId,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<CreateResponse>
```

**Example:**
```rust
// Create a directory with 755 permissions
let response = client.create_directory(
    1,           // parent inode (root)
    "mydir",
    0o755,       // mode
    1000,        // uid
    1000,        // gid
).await?;
```

#### Delete

```rust
/// Delete a file or directory
pub async fn delete(&self, parent: InodeId, name: &str) -> Result<DeleteResponse>

pub struct DeleteResponse {
    pub success: bool,
    pub error: Option<String>,
}
```

**Example:**
```rust
let response = client.delete(1, "oldfile.txt").await?;
if !response.success {
    eprintln!("Delete failed: {}", response.error.unwrap_or_default());
}
```

#### Rename

```rust
/// Rename/move a file or directory
pub async fn rename(
    &self,
    src_parent: InodeId,
    src_name: &str,
    dst_parent: InodeId,
    dst_name: &str,
) -> Result<DeleteResponse>
```

**Example:**
```rust
// Move file from /src/file.txt to /dst/file.txt
let response = client.rename(
    src_dir_inode,
    "file.txt",
    dst_dir_inode,
    "file.txt",
).await?;
```

#### Read Directory

```rust
/// List entries in a directory
pub async fn readdir(&self, inode: InodeId) -> Result<ReaddirResponse>

pub struct ReaddirResponse {
    pub success: bool,
    pub entries: Vec<DirEntry>,
    pub error: Option<String>,
}

pub struct DirEntry {
    pub name: String,
    pub inode: InodeId,
    pub file_type: FileType,
}
```

**Example:**
```rust
let response = client.readdir(1).await?;  // List root directory
for entry in response.entries {
    let type_char = match entry.file_type {
        FileType::Directory => 'd',
        FileType::RegularFile => '-',
        FileType::Symlink => 'l',
    };
    println!("{} {} (inode {})", type_char, entry.name, entry.inode);
}
```

#### Get Attributes

```rust
/// Get file/directory attributes
pub async fn getattr(&self, inode: InodeId) -> Result<GetattrResponse>

pub struct GetattrResponse {
    pub found: bool,
    pub inode: Option<Inode>,
}
```

**Example:**
```rust
let response = client.getattr(123).await?;
if let Some(inode) = response.inode {
    println!("Size: {} bytes", inode.size);
    println!("Mode: {:o}", inode.mode);
    println!("Owner: {}:{}", inode.uid, inode.gid);
}
```

#### Add Chunk

```rust
/// Associate a chunk with an inode
pub async fn add_chunk(&self, inode: InodeId, chunk_id: ChunkId) -> Result<()>
```

**Example:**
```rust
let chunk_id = ChunkId::new();
client.add_chunk(file_inode, chunk_id).await?;
```

## DataClient

The `DataClient` handles chunk storage operations on data servers.

### Creating a Client

```rust
use strata::client::DataClient;

// From SocketAddr
let client = DataClient::new(addr);

// From address string
let client = DataClient::from_addr("127.0.0.1:9001")?;

// With custom timeouts
let client = DataClient::with_timeouts(
    addr,
    Duration::from_secs(10),
    Duration::from_secs(120),  // Longer timeout for data operations
);
```

### Operations

#### Health Check

```rust
/// Check if the data service is healthy
pub async fn health(&self) -> Result<bool>
```

#### Server Status

```rust
/// Get data server status
pub async fn status(&self) -> Result<DataServerStatus>

pub struct DataServerStatus {
    pub server_id: u64,
    pub usage_bytes: u64,
    pub healthy: bool,
}
```

**Example:**
```rust
let status = client.status().await?;
println!("Server {}: {} bytes used, healthy={}",
    status.server_id,
    status.usage_bytes,
    status.healthy
);
```

#### Write Chunk

```rust
/// Write a chunk (automatically erasure-encoded)
pub async fn write_chunk(&self, chunk_id: ChunkId, data: &[u8]) -> Result<WriteChunkResponse>

pub struct WriteChunkResponse {
    pub success: bool,
    pub shards: Vec<usize>,  // Indices of written shards
    pub error: Option<String>,
}
```

**Example:**
```rust
let chunk_id = ChunkId::new();
let data = b"Hello, Strata!";

let response = client.write_chunk(chunk_id, data).await?;
if response.success {
    println!("Wrote {} shards", response.shards.len());
}
```

#### Read Chunk

```rust
/// Read a chunk (automatically reconstructed from shards)
pub async fn read_chunk(&self, chunk_id: ChunkId, size: u64) -> Result<Vec<u8>>
```

**Example:**
```rust
let data = client.read_chunk(chunk_id, expected_size).await?;
println!("Read {} bytes", data.len());
```

#### Delete Chunk

```rust
/// Delete a chunk and all its shards
pub async fn delete_chunk(&self, chunk_id: ChunkId) -> Result<()>
```

**Example:**
```rust
client.delete_chunk(chunk_id).await?;
```

#### Write Shard

```rust
/// Write a specific shard (low-level)
pub async fn write_shard(
    &self,
    chunk_id: ChunkId,
    shard_index: usize,
    data: &[u8],
) -> Result<()>
```

**Example:**
```rust
// Write shard 0 of a chunk
client.write_shard(chunk_id, 0, shard_data).await?;
```

#### Read Shard

```rust
/// Read a specific shard (low-level)
pub async fn read_shard(&self, chunk_id: ChunkId, shard_index: usize) -> Result<Vec<u8>>
```

**Example:**
```rust
let shard_data = client.read_shard(chunk_id, 0).await?;
```

## Complete Example

Here's a complete example showing file creation and data storage:

```rust
use strata::client::{MetadataClient, DataClient};
use strata::types::ChunkId;
use strata::error::Result;

async fn create_and_write_file() -> Result<()> {
    // Connect to services
    let metadata = MetadataClient::from_addr("127.0.0.1:9000")?;
    let data = DataClient::from_addr("127.0.0.1:9001")?;

    // Verify services are healthy
    assert!(metadata.health().await?);
    assert!(data.health().await?);

    // Create a file in the root directory
    let create_resp = metadata.create_file(
        1,              // parent (root)
        "example.txt",
        0o644,
        1000,
        1000,
    ).await?;

    let file_inode = create_resp.inode.expect("file created");
    println!("Created file with inode {}", file_inode);

    // Write data to a chunk
    let chunk_id = ChunkId::new();
    let file_data = b"This is the file content";

    let write_resp = data.write_chunk(chunk_id, file_data).await?;
    println!("Wrote {} shards", write_resp.shards.len());

    // Associate chunk with the file
    metadata.add_chunk(file_inode, chunk_id).await?;

    // Read it back
    let read_data = data.read_chunk(chunk_id, file_data.len() as u64).await?;
    assert_eq!(read_data, file_data);

    // List directory to verify
    let dir_resp = metadata.readdir(1).await?;
    for entry in dir_resp.entries {
        println!("  {} (inode {})", entry.name, entry.inode);
    }

    Ok(())
}
```

## Error Handling

Both clients return `Result<T, StrataError>`. Common errors:

| Error | Cause |
|-------|-------|
| `ConnectionFailed` | Cannot reach the service |
| `Timeout` | Request timed out |
| `FileNotFound` | File/directory doesn't exist |
| `FileExists` | File already exists (create) |
| `ChunkNotFound` | Chunk doesn't exist |
| `InsufficientShards` | Not enough shards for reconstruction |

**Error Handling Example:**
```rust
use strata::error::StrataError;

match client.lookup(1, "missing.txt").await {
    Ok(resp) if resp.found => {
        println!("Found: {:?}", resp.inode);
    }
    Ok(resp) => {
        println!("File not found");
    }
    Err(StrataError::ConnectionFailed(msg)) => {
        eprintln!("Cannot connect to metadata service: {}", msg);
    }
    Err(StrataError::Timeout(ms)) => {
        eprintln!("Request timed out after {}ms", ms);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

## Connection Management

### Reusing Clients

Clients maintain an internal HTTP connection pool. Create one client and reuse it:

```rust
// Good - reuse the client
let client = MetadataClient::from_addr("127.0.0.1:9000")?;
for name in files {
    client.lookup(1, &name).await?;
}

// Bad - creating new client for each request
for name in files {
    let client = MetadataClient::from_addr("127.0.0.1:9000")?;
    client.lookup(1, &name).await?;
}
```

### Thread Safety

Both `MetadataClient` and `DataClient` are thread-safe and can be shared across async tasks using `Arc`:

```rust
use std::sync::Arc;

let client = Arc::new(MetadataClient::from_addr("127.0.0.1:9000")?);

let handles: Vec<_> = (0..10).map(|i| {
    let client = Arc::clone(&client);
    tokio::spawn(async move {
        client.lookup(1, &format!("file{}.txt", i)).await
    })
}).collect();

for handle in handles {
    handle.await??;
}
```

## Protocol Details

### Transport

| Aspect | Details |
|--------|---------|
| Protocol | HTTP/1.1 |
| Content-Type | `application/json` |
| Methods | GET, POST, PUT, DELETE |

### Endpoints

**Metadata Service (port 9000):**

| Operation | Method | Endpoint |
|-----------|--------|----------|
| Health | GET | `/health` |
| Lookup | POST | `/metadata/lookup` |
| Create File | POST | `/metadata/create_file` |
| Create Directory | POST | `/metadata/create_directory` |
| Delete | POST | `/metadata/delete` |
| Rename | POST | `/metadata/rename` |
| Read Directory | POST | `/metadata/readdir` |
| Get Attributes | POST | `/metadata/getattr` |
| Add Chunk | POST | `/metadata/add_chunk` |

**Data Service (port 9001):**

| Operation | Method | Endpoint |
|-----------|--------|----------|
| Health | GET | `/health` |
| Status | GET | `/status` |
| Write Chunk | PUT | `/chunk/{chunk_id}` |
| Read Chunk | GET | `/chunk/{chunk_id}?size=N` |
| Delete Chunk | DELETE | `/chunk/{chunk_id}` |
| Write Shard | PUT | `/shard/{chunk_id}/{shard_index}` |
| Read Shard | GET | `/shard/{chunk_id}/{shard_index}` |

## See Also

- [Core Types](types.md) - Type definitions
- [Error Handling](errors.md) - Error types
- [Architecture Overview](../architecture/overview.md) - System design
