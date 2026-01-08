# Data API Reference

The Data API handles chunk storage operations on data servers, including read, write, and integrity verification.

## Overview

Data servers store file content as erasure-coded chunks. Each chunk is split into data and parity shards distributed across multiple servers for durability.

### Connection

```bash
# Default data server address
DATA_ADDR=localhost:9001
```

## Chunk Operations

### WriteChunk

Writes a chunk shard to storage.

```protobuf
message WriteChunkRequest {
    string chunk_id = 1;    // UUID identifying the chunk
    uint32 shard_index = 2; // Shard number (0 to total_shards-1)
    bytes data = 3;         // Shard data
}

message WriteChunkResponse {
    StoredChunk metadata = 1;
}

message StoredChunk {
    string id = 1;
    uint32 shard_index = 2;
    uint64 size = 3;
    uint32 checksum = 4;    // CRC32
    bytes sha256 = 5;       // SHA-256 hash
    uint64 version = 6;
}
```

**Behavior:**
- Data is persisted to disk before acknowledgment
- Checksum computed and stored with metadata
- Overwrites existing shard if present

### ReadChunk

Reads a chunk shard from storage.

```protobuf
message ReadChunkRequest {
    string chunk_id = 1;
    uint32 shard_index = 2;
}

message ReadChunkResponse {
    bytes data = 1;
    StoredChunk metadata = 2;
}
```

**Behavior:**
- Verifies checksum before returning
- Returns from cache if available (LRU cache)
- Returns `NOT_FOUND` if shard doesn't exist

### DeleteChunk

Removes a chunk shard from storage.

```protobuf
message DeleteChunkRequest {
    string chunk_id = 1;
    uint32 shard_index = 2;
}
```

**Behavior:**
- Removes shard from disk and cache
- Idempotent (success if already deleted)

### ListChunks

Lists all chunks/shards stored on this server.

```protobuf
message ListChunksRequest {
    optional string prefix = 1;  // Filter by chunk ID prefix
    optional uint32 limit = 2;   // Max results
}

message ListChunksResponse {
    repeated StoredChunk chunks = 1;
}
```

## Integrity Operations

### VerifyChunk

Verifies chunk integrity without returning data.

```protobuf
message VerifyChunkRequest {
    string chunk_id = 1;
    uint32 shard_index = 2;
}

message VerifyChunkResponse {
    bool valid = 1;
    optional string error = 2;  // If invalid, reason
}
```

**Checks performed:**
- File exists on disk
- CRC32 checksum matches
- SHA-256 hash matches (optional, slower)

### ScrubChunk

Performs deep verification and repairs if possible.

```protobuf
message ScrubChunkRequest {
    string chunk_id = 1;
    uint32 shard_index = 2;
    bool repair = 3;        // Attempt repair if corrupted
}

message ScrubChunkResponse {
    ScrubResult result = 1;
}

enum ScrubResult {
    OK = 0;
    CORRUPTED = 1;
    REPAIRED = 2;
    UNRECOVERABLE = 3;
}
```

## Erasure Coding

### Encoding

Data is encoded using Reed-Solomon erasure coding before storage:

```
Original Chunk (64MB)
        |
        v
  Erasure Encoder (4+2)
        |
        +---> Shard 0 (16MB) --> Data Server A
        +---> Shard 1 (16MB) --> Data Server B
        +---> Shard 2 (16MB) --> Data Server C
        +---> Shard 3 (16MB) --> Data Server D
        +---> Parity 0 (16MB) --> Data Server E
        +---> Parity 1 (16MB) --> Data Server F
```

### Configuration

| Preset | Data Shards | Parity Shards | Overhead | Fault Tolerance |
|--------|-------------|---------------|----------|-----------------|
| `small` | 2 | 1 | 1.5x | 1 failure |
| `default` | 4 | 2 | 1.5x | 2 failures |
| `large` | 8 | 4 | 1.5x | 4 failures |
| `durability` | 6 | 6 | 2.0x | 6 failures |

### Reconstruction

When reading, only `data_shards` are needed:

```
Available: Shards 0, 1, 3, Parity 0  (Shard 2 failed)
           |
           v
    Erasure Decoder
           |
           v
    Original Data (reconstructed)
```

## Caching

Data servers implement an LRU cache for frequently accessed chunks:

### Cache Configuration

```json
{
  "data": {
    "cache_size": "1GB",
    "cache_policy": "lru"
  }
}
```

### Cache Behavior

| Operation | Cache Impact |
|-----------|--------------|
| Read (hit) | Returns cached data, O(1) |
| Read (miss) | Reads from disk, adds to cache |
| Write | Adds to cache after disk write |
| Delete | Removes from cache |

### Cache Optimization

The cache uses `Arc<Vec<u8>>` for shared references:
- Multiple concurrent readers share the same cached data
- No data copying for cache hits
- Automatic eviction when cache is full

## Storage Layout

Chunks are stored on disk with a sharded directory structure:

```
data_dir/
├── 00/
│   ├── 00a1b2c3-..._0.dat    # Chunk data
│   └── 00a1b2c3-..._0.meta   # Chunk metadata
├── 01/
│   └── ...
├── ff/
│   └── ...
```

The first byte of the chunk ID determines the subdirectory (256 buckets).

## Error Handling

| Error | HTTP/gRPC Code | Description |
|-------|----------------|-------------|
| `NOT_FOUND` | 404 / NOT_FOUND | Chunk shard doesn't exist |
| `CHECKSUM_MISMATCH` | 500 / DATA_LOSS | Data corruption detected |
| `DISK_FULL` | 507 / RESOURCE_EXHAUSTED | No space for write |
| `INTERNAL` | 500 / INTERNAL | Server error |

## Performance Characteristics

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Write (uncached) | ~1-5ms | Disk-bound |
| Read (cached) | ~10-100us | Memory-bound |
| Read (uncached) | ~1-5ms | Disk-bound |
| Verify | ~1-10ms | CPU-bound |

## Monitoring

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `strata_data_read_bytes` | Counter | Total bytes read |
| `strata_data_write_bytes` | Counter | Total bytes written |
| `strata_data_cache_hits` | Counter | Cache hit count |
| `strata_data_cache_misses` | Counter | Cache miss count |
| `strata_data_checksum_errors` | Counter | Corruption events |

### Health Check

```bash
curl http://localhost:9001/health
# Returns: {"status": "healthy", "chunks": 12345, "capacity_used": "45%"}
```

## See Also

- [Metadata API](metadata.md) - Namespace operations
- [Architecture Overview](../architecture/overview.md) - System design
- [Erasure Coding](../architecture/erasure.md) - Encoding details
