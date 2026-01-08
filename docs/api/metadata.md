# Metadata API Reference

The Metadata API manages file system namespace operations including files, directories, and chunk tracking.

## Overview

The metadata service runs as a Raft cluster providing strong consistency for all namespace operations. Clients communicate via gRPC.

### Connection

```bash
# Default metadata server address
METADATA_ADDR=localhost:9000
```

## Operations

### File Operations

#### CreateFile

Creates a new file in the specified directory.

```protobuf
message CreateFileRequest {
    uint64 parent = 1;      // Parent directory inode
    string name = 2;        // File name
    uint32 mode = 3;        // Permission bits (e.g., 0644)
    uint32 uid = 4;         // Owner user ID
    uint32 gid = 5;         // Owner group ID
}

message CreateFileResponse {
    uint64 inode = 1;       // Created file inode
}
```

**Errors:**
- `ALREADY_EXISTS` - File with same name exists in parent
- `NOT_FOUND` - Parent directory doesn't exist
- `PERMISSION_DENIED` - Insufficient permissions

#### DeleteFile

Removes a file from the namespace.

```protobuf
message DeleteRequest {
    uint64 parent = 1;      // Parent directory inode
    string name = 2;        // File name to delete
}
```

**Behavior:**
- Decrements link count
- File data is garbage collected when link count reaches 0
- Returns success even if file doesn't exist (idempotent)

### Directory Operations

#### CreateDirectory

Creates a new directory.

```protobuf
message CreateDirectoryRequest {
    uint64 parent = 1;      // Parent directory inode
    string name = 2;        // Directory name
    uint32 mode = 3;        // Permission bits (e.g., 0755)
    uint32 uid = 4;         // Owner user ID
    uint32 gid = 5;         // Owner group ID
}
```

#### ListDirectory

Lists contents of a directory.

```protobuf
message ListDirectoryRequest {
    uint64 inode = 1;       // Directory inode to list
}

message DirectoryEntry {
    string name = 1;        // Entry name
    uint64 inode = 2;       // Entry inode
    FileType type = 3;      // File, directory, symlink, etc.
}
```

### Link Operations

#### CreateHardLink

Creates a hard link to an existing file.

```protobuf
message LinkRequest {
    uint64 parent = 1;      // Directory for new link
    string name = 2;        // Name of new link
    uint64 inode = 3;       // Target file inode
}
```

**Constraints:**
- Cannot link to directories
- Target must exist
- Increments link count

#### CreateSymlink

Creates a symbolic link.

```protobuf
message SymlinkRequest {
    uint64 parent = 1;      // Parent directory
    string name = 2;        // Symlink name
    string target = 3;      // Target path (can be relative)
    uint32 uid = 4;
    uint32 gid = 5;
}
```

### Rename

Moves/renames a file or directory.

```protobuf
message RenameRequest {
    uint64 src_parent = 1;  // Source directory
    string src_name = 2;    // Source name
    uint64 dst_parent = 3;  // Destination directory
    string dst_name = 4;    // Destination name
}
```

**Behavior:**
- Atomic operation
- Overwrites destination if exists (for files)
- Fails if destination is non-empty directory

### Attribute Operations

#### SetAttr

Modifies file/directory attributes.

```protobuf
message SetAttrRequest {
    uint64 inode = 1;
    optional uint32 mode = 2;   // New permissions
    optional uint32 uid = 3;    // New owner
    optional uint32 gid = 4;    // New group
    optional uint64 size = 5;   // Truncate to size
    optional int64 atime = 6;   // Access time
    optional int64 mtime = 7;   // Modification time
}
```

#### SetXattr / RemoveXattr

Manages extended attributes.

```protobuf
message SetXattrRequest {
    uint64 inode = 1;
    string name = 2;        // Attribute name
    bytes value = 3;        // Attribute value
}
```

## Chunk Management

### AllocateChunks

Allocates storage for file data.

```protobuf
message AllocateChunksRequest {
    uint64 inode = 1;
    repeated ChunkAllocation chunks = 2;
}

message ChunkAllocation {
    string chunk_id = 1;    // UUID for the chunk
    uint64 offset = 2;      // Byte offset in file
    uint64 size = 3;        // Chunk size
}
```

### UpdateChunkLocations

Updates data server locations for a chunk.

```protobuf
message UpdateChunkLocationsRequest {
    string chunk_id = 1;
    repeated string locations = 2;  // Data server addresses
}
```

## Multipart Upload Operations

For S3-compatible multipart uploads:

### InitiateMultipartUpload

```protobuf
message InitiateMultipartUploadRequest {
    uint64 bucket_inode = 1;
    string key = 2;
    string upload_id = 3;   // Client-generated UUID
}
```

### UploadPart

```protobuf
message UploadPartRequest {
    string upload_id = 1;
    uint32 part_number = 2; // 1-10000
    string chunk_id = 3;
    uint64 size = 4;
    string etag = 5;
}
```

**Constraints:**
- Part numbers: 1 to 10,000
- Minimum part size: 5MB (except last part)
- Maximum part size: 5GB

### CompleteMultipartUpload

```protobuf
message CompleteMultipartUploadRequest {
    string upload_id = 1;
    repeated CompletedPart parts = 2;
}

message CompletedPart {
    uint32 part_number = 1;
    string etag = 2;
}
```

### AbortMultipartUpload

```protobuf
message AbortMultipartUploadRequest {
    string upload_id = 1;
}
```

## Data Server Registry

### RegisterDataServer

```protobuf
message RegisterDataServerRequest {
    uint64 server_id = 1;
    string address = 2;     // host:port
    uint64 capacity = 3;    // Total capacity in bytes
}
```

### UpdateDataServerStatus

```protobuf
message UpdateDataServerStatusRequest {
    uint64 server_id = 1;
    uint64 used = 2;        // Used capacity
    bool online = 3;        // Server availability
}
```

## Consistency Model

- **Strong consistency**: All operations are linearizable
- **Raft replication**: Metadata replicated to all cluster nodes
- **Leader-based**: Writes go through the Raft leader
- **Read-your-writes**: Clients see their own writes immediately

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Lookup | O(1) | Hash-based directory lookup |
| List | O(n) | n = directory entries |
| Create/Delete | O(log n) | Raft log replication |
| Rename | O(1) | Atomic metadata update |

## See Also

- [Data API](data.md) - Chunk storage operations
- [Types Reference](types.md) - Common types
- [Architecture Overview](../architecture/overview.md) - System design
