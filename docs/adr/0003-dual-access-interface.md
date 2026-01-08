# ADR-0003: Dual Access Interface (FUSE + S3)

## Status

**Accepted**

## Context

Modern storage systems serve diverse workloads with different access patterns:

1. **Legacy applications** expect POSIX file system semantics (open, read, write, seek, close)
2. **Cloud-native applications** expect object storage APIs (S3-compatible PUT, GET, DELETE)
3. **Data pipelines** may use both—POSIX for processing, S3 for ingestion/export

We needed to decide how to expose Strata's storage to applications:

**Option A: POSIX-only (FUSE)**
- Pro: Full file system semantics, compatible with any application
- Con: No native cloud integration, requires mount on every client

**Option B: S3-only**
- Pro: Cloud-native, works with existing S3 tooling (AWS CLI, SDKs)
- Con: No POSIX semantics (no append, no rename, no hard links)

**Option C: Dual interface (FUSE + S3)**
- Pro: Maximum compatibility, serves both workload types
- Con: Implementation complexity, potential consistency issues

## Decision

We implemented **both FUSE and S3 interfaces** as optional features, enabled by default:

```toml
# Cargo.toml
[features]
default = ["fuse", "s3"]
fuse = ["fuser"]
s3 = []  # No additional dependencies
```

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                    │
├──────────────────────┬──────────────────────────────────┤
│    POSIX Apps        │         Cloud-Native Apps         │
│  (cat, cp, vim)      │      (AWS CLI, boto3, SDKs)       │
└──────────┬───────────┴──────────────┬───────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────────┐  ┌───────────────────────────────┐
│     FUSE Client      │  │         S3 Gateway            │
│   (src/fuse/)        │  │        (src/s3/)              │
│                      │  │                               │
│ • Mount as /mnt/xyz  │  │ • HTTP on port 9002           │
│ • Full POSIX ops     │  │ • SigV4 authentication        │
│ • Kernel integration │  │ • Versioning, lifecycle, etc. │
└──────────┬───────────┘  └──────────────┬────────────────┘
           │                             │
           └──────────────┬──────────────┘
                          │
                          ▼
           ┌──────────────────────────────┐
           │     Unified Storage Layer     │
           │  (Metadata Client + Data      │
           │         Client)               │
           └──────────────────────────────┘
```

### Feature Parity

Both interfaces access the same underlying storage, ensuring:
- File created via FUSE is immediately accessible via S3
- Object uploaded via S3 appears in mounted filesystem
- Metadata (size, timestamps) is consistent across interfaces

### S3 Feature Completeness

The S3 gateway implements enterprise features:
- Object versioning
- Lifecycle policies
- Server-side encryption (SSE-S3, SSE-KMS)
- Object locking (WORM compliance)
- Bucket replication
- S3 Select (SQL queries)
- CORS with credentials

## Consequences

### Positive

- **Universal compatibility**: Serves both legacy and cloud-native workloads
- **Migration flexibility**: Applications can migrate from POSIX to S3 incrementally
- **Operational simplicity**: Single storage system for diverse teams
- **AWS tooling compatibility**: Standard S3 SDKs and CLI work unmodified
- **Feature toggles**: Can disable unused interface to reduce attack surface

### Negative

- **Implementation burden**: Two complete API surfaces to maintain
- **Semantic mismatch**: Some operations don't translate cleanly:
  - S3 has no `append`; must re-upload entire object
  - POSIX has no `versioning`; overwrite replaces
  - S3 keys can contain `/` without creating directories
- **Testing complexity**: Must test both interfaces and their interactions
- **Potential confusion**: Users may not understand which interface to use

### Consistency Model

Both interfaces share the metadata cluster (Raft), ensuring:
- **Read-after-write consistency**: Successful write via either interface is immediately visible
- **Atomic operations**: File/object creation is atomic
- **No cross-interface transactions**: Cannot atomically modify via both interfaces

### Operational Implications

- **FUSE deployment**: Requires FUSE kernel module, mount privileges
- **S3 deployment**: Stateless HTTP server, easier to scale horizontally
- **Documentation**: Must document semantic differences between interfaces
- **Monitoring**: Separate metrics for each interface to identify usage patterns

## References

- `src/fuse/mod.rs` - FUSE filesystem implementation
- `src/s3/mod.rs` - S3 gateway with 20+ submodules
- `src/s3/gateway.rs` - S3 HTTP server setup
- `docs/api/s3.md` - S3 API compatibility matrix
