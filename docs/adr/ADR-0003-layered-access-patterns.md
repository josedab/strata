# ADR-0003: Layered Access Patterns (FUSE + S3 + Native)

## Status

Accepted

## Context

Users interact with storage systems through different interfaces depending on their use case:

### POSIX/File System Access
- Legacy applications expect standard file system semantics
- Tools like `ls`, `cp`, `grep` work unchanged
- Strong consistency expectations (read-after-write)
- Hierarchical namespace with directories

### Object Storage (S3) Access
- Cloud-native applications use S3 APIs
- Flat namespace with prefixes simulating directories
- Eventual consistency acceptable for many workloads
- HTTP-based, language-agnostic

### Native/SDK Access
- High-performance applications need optimized protocols
- Batch operations, streaming, custom retry logic
- Tight integration with application code

Building a storage system that only supports one interface limits adoption. HDFS's lack of S3 compatibility pushed users to maintain separate systems. Pure object stores like S3 can't run legacy POSIX applications.

We want Strata to be the single storage layer for diverse workloads.

## Decision

We will provide **three independent access paths** to the same underlying data:

### 1. FUSE Filesystem (`src/fuse/`)
- Implements FUSE (Filesystem in Userspace) protocol
- Mounts as a local directory (e.g., `/mnt/strata`)
- Full POSIX semantics: read, write, mkdir, chmod, symlink, etc.
- Optional feature flag: `--features fuse`

### 2. S3 Gateway (`src/s3/`)
- HTTP server implementing S3-compatible REST API
- Supports core operations: GET, PUT, DELETE, LIST, multipart upload
- Buckets map to top-level directories
- Optional feature flag: `--features s3`

### 3. Native Client (`src/client/`)
- Rust library with async API
- Direct communication with metadata and data servers
- Highest performance, most control
- Always available (core functionality)

All three access paths share:
- Same metadata cluster (Raft consensus)
- Same data storage (erasure-coded chunks)
- Same authentication and authorization
- Same quota and rate limiting

## Consequences

### Positive

- **Broad compatibility**: Run existing applications without modification
- **Migration path**: Move from S3 to POSIX or vice versa without data migration
- **Right tool for the job**: Use FUSE for interactive work, S3 for cloud apps, native for performance
- **Single source of truth**: One storage system instead of siloed solutions
- **Unified operations**: Single monitoring, backup, and management plane

### Negative

- **Implementation cost**: Three protocol implementations to maintain
- **Semantic gaps**: S3 and POSIX have different consistency models
- **Testing matrix**: Must verify each protocol handles edge cases correctly
- **Feature parity**: New features must be implemented across all interfaces

### Semantic Mapping Challenges

| POSIX Concept | S3 Mapping |
|---------------|------------|
| Directory | Key prefix ending in `/` |
| Hard links | Not supported |
| Symlinks | Stored as small objects with metadata |
| Permissions | Mapped to ACLs |
| Rename | Copy + Delete (not atomic cross-bucket) |
| Append | Multipart upload continuation |

### Implications

- Documentation must explain semantic differences between interfaces
- Tests must cover cross-protocol scenarios (write via S3, read via FUSE)
- Feature flags allow deploying only needed interfaces
- Performance characteristics differ (FUSE has kernel overhead, S3 has HTTP overhead)

## References

- `src/fuse/mod.rs` - FUSE implementation
- `src/s3/gateway.rs` - S3 gateway
- `src/client/mod.rs` - Native client
- `Cargo.toml:13-16` - Feature flag definitions
