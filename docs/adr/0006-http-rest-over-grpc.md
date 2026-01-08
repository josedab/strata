# ADR-0006: HTTP REST over gRPC for Internal Communication

## Status

**Accepted**

## Context

Strata's components communicate across the network:
- Clients → Metadata cluster (file operations)
- Clients → Data servers (chunk read/write)
- S3 Gateway → Metadata/Data (proxied operations)
- Metadata nodes → Metadata nodes (Raft consensus)

We needed to choose a communication protocol that balances:
- **Performance**: Latency and throughput for storage operations
- **Debuggability**: Ability to inspect traffic, troubleshoot issues
- **Ecosystem**: Library support, tooling availability
- **Complexity**: Implementation and operational overhead

### Options Considered

1. **gRPC (Protocol Buffers)**
   - Pro: Efficient binary serialization, strong typing, streaming support
   - Con: Harder to debug (binary), requires protobuf toolchain, HTTP/2 complexity

2. **HTTP REST (JSON)**
   - Pro: Human-readable, easy to debug with curl, universal tooling
   - Con: Larger payloads, no built-in schema validation

3. **Custom binary protocol**
   - Pro: Maximum performance, tailored to our needs
   - Con: No ecosystem, must implement everything

4. **HTTP REST + gRPC hybrid**
   - Pro: Best of both worlds
   - Con: Two systems to maintain

## Decision

We chose **HTTP REST with JSON** for client-server communication:

```rust
// src/client/mod.rs
pub struct MetadataClient {
    client: reqwest::Client,
    base_url: String,
}

impl MetadataClient {
    pub async fn lookup(&self, parent: InodeId, name: &str) -> Result<Option<Inode>> {
        let url = format!("{}/lookup/{}/{}", self.base_url, parent, name);
        let response = self.client
            .get(&url)
            .timeout(self.config.request_timeout)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(Some(response.json().await?)),
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(StrataError::from_status(status, response.text().await?)),
        }
    }
}
```

### Rationale

1. **Debuggability is critical for distributed systems**: When things go wrong (and they will), being able to `curl` an endpoint or inspect traffic with standard tools is invaluable.

2. **JSON overhead is acceptable for metadata**: Metadata operations are small payloads (file names, sizes, timestamps). The ~2x size increase of JSON vs protobuf is negligible.

3. **Data plane can use efficient binary**: Chunk data is transferred as raw bytes, not JSON-serialized. Only metadata about chunks uses JSON.

4. **Simpler deployment**: No protobuf compiler in build chain, no schema synchronization concerns.

### API Design

```
Metadata API:
  GET  /health                    → Health check
  GET  /lookup/{parent}/{name}    → Lookup child inode
  GET  /inode/{id}                → Get inode by ID
  POST /create                    → Create file/directory
  POST /write                     → Update file content metadata
  DELETE /inode/{id}              → Remove inode

Data API:
  GET  /chunk/{id}                → Read chunk data (binary response)
  PUT  /chunk/{id}                → Write chunk data (binary body)
  HEAD /chunk/{id}                → Check chunk existence
  DELETE /chunk/{id}              → Delete chunk
```

### Client Configuration

```rust
pub struct ClientConfig {
    pub connect_timeout: Duration,   // Default: 5s
    pub request_timeout: Duration,   // Default: 30s
    pub pool_idle_timeout: Duration, // Default: 90s
    pub pool_max_idle: usize,        // Default: 32
}
```

## Consequences

### Positive

- **Easy debugging**: Use curl, httpie, or browser dev tools to inspect API
- **Universal client support**: Any language with HTTP support works
- **Simple testing**: Integration tests use standard HTTP testing tools
- **Clear error messages**: JSON error responses are human-readable
- **No build tooling**: No protoc, no schema compilation step

### Negative

- **Larger payload size**: JSON ~2x larger than protobuf for structured data
- **No streaming**: HTTP/1.1 request-response model (could upgrade to HTTP/2)
- **No schema enforcement**: JSON parsing can fail at runtime
- **Slower serialization**: JSON parse/serialize slower than protobuf

### Mitigations

1. **Binary for bulk data**: Chunk content transferred as `application/octet-stream`, not JSON
2. **Connection pooling**: reqwest maintains connection pool to amortize TCP/TLS setup
3. **Compression**: Can enable gzip for JSON payloads if needed
4. **Serde validation**: Rust's serde provides compile-time type checking on our side

### Performance Impact

For typical metadata operations:
- **JSON overhead**: ~1-2KB per request vs ~500B for protobuf
- **Latency impact**: ~100-200μs additional parse time (negligible vs network RTT)
- **Throughput**: Limited by network, not serialization

For data operations:
- **No JSON overhead**: Raw binary transfer
- **Same performance**: As efficient as any binary protocol

### What We Didn't Use

The codebase has `tonic` and `prost` dependencies but they are not actively used for RPC. They may have been considered initially or used for specific internal purposes.

## References

- `src/client/mod.rs` - MetadataClient and DataClient implementation
- `src/metadata/server.rs` - Metadata HTTP server
- `src/data/server.rs` - Data HTTP server
- `Cargo.toml:43-46` - reqwest dependency
