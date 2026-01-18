# ADR-0005: Hybrid RPC Strategy (gRPC Internal, REST External)

## Status

Accepted

## Context

Strata has two distinct communication patterns with different requirements:

### Internal Cluster Communication
- Raft consensus messages (AppendEntries, RequestVote)
- Chunk replication between data nodes
- Metadata state synchronization
- Health checks and failure detection

Requirements:
- Low latency (consensus is latency-sensitive)
- High throughput (replication moves large data volumes)
- Efficient binary serialization
- Streaming support (for large transfers)
- Strong typing for correctness

### External Client Communication
- Metadata operations (lookup, create, delete)
- Chunk read/write requests
- Health and status endpoints
- S3-compatible API

Requirements:
- Language-agnostic (clients in any language)
- Easy debugging (human-readable when needed)
- Wide tooling support (curl, browsers, load balancers)
- Firewall-friendly (HTTP on standard ports)

One protocol cannot optimally serve both needs.

## Decision

We will use a **hybrid RPC strategy**:

### gRPC with Tonic for Internal Communication

```rust
// Internal cluster RPC using Tonic
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn append_entries(&self, request: Request<AppendEntriesRequest>)
        -> Result<Response<AppendEntriesResponse>, Status>;

    async fn request_vote(&self, request: Request<RequestVoteRequest>)
        -> Result<Response<RequestVoteResponse>, Status>;
}
```

Benefits for internal use:
- Protocol Buffers provide compact binary serialization
- HTTP/2 multiplexing reduces connection overhead
- Bidirectional streaming for efficient replication
- Generated client/server code ensures type safety
- Built-in deadlines and cancellation

### HTTP REST with JSON for External Communication

```rust
// External client API using reqwest
impl MetadataClient {
    pub async fn lookup(&self, parent: InodeId, name: &str) -> Result<Option<Inode>> {
        let response = self.client
            .post(&format!("{}/metadata/lookup", self.base_url))
            .json(&LookupRequest { parent, name: name.to_string() })
            .send()
            .await?;
        // ...
    }
}
```

Benefits for external use:
- Any HTTP client can interact (curl, Python requests, etc.)
- JSON is human-readable for debugging
- Standard HTTP semantics (caching, proxies, load balancers)
- OpenAPI/Swagger documentation possible
- Browser-based tooling works directly

## Consequences

### Positive

- **Optimized for each use case**: Binary efficiency internally, accessibility externally
- **Debuggability**: Can inspect client requests with standard tools
- **Performance**: Raft messages use efficient binary protocol
- **Ecosystem leverage**: gRPC and REST both have excellent Rust support
- **Gradual adoption**: Clients can start with REST, migrate to native if needed

### Negative

- **Two protocols to maintain**: Different serialization, error handling, testing
- **Cognitive overhead**: Contributors must understand both patterns
- **Potential inconsistency**: Must ensure both protocols expose same capabilities
- **Dependency weight**: Both tonic (gRPC) and reqwest/axum (HTTP) in dependency tree

### Protocol Mapping

| Operation | Internal (gRPC) | External (REST) |
|-----------|-----------------|-----------------|
| Raft AppendEntries | `RaftService::append_entries` | N/A |
| Raft RequestVote | `RaftService::request_vote` | N/A |
| Chunk Replication | `DataService::replicate` | N/A |
| Metadata Lookup | N/A | `POST /metadata/lookup` |
| Create File | N/A | `POST /metadata/create_file` |
| Health Check | `HealthService::check` | `GET /health` |
| S3 Operations | N/A | S3-compatible REST |

### Serialization Choices

| Protocol | Format | Library |
|----------|--------|---------|
| gRPC | Protocol Buffers | prost |
| REST | JSON | serde_json |
| Raft RPC | MessagePack/bincode | rmp-serde/bincode |

### Implications

- API versioning strategies may differ (gRPC uses package versions, REST uses URL paths)
- Error codes must be mapped consistently across protocols
- Load testing must cover both protocol paths
- Documentation needs to cover both APIs

## References

- `Cargo.toml:55-56` - tonic and prost dependencies
- `src/raft/rpc.rs` - Raft RPC type definitions
- `src/client/mod.rs` - REST client implementation
- `src/metadata/server.rs` - gRPC metadata server
