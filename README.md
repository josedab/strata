# Strata

A distributed file system combining POSIX compatibility with S3 access, written in Rust.

## Features

- **Raft-based Metadata Cluster**: Consistent metadata management with leader election
- **Erasure Coding**: Reed-Solomon encoding for storage efficiency and fault tolerance
- **POSIX Access**: FUSE-based filesystem for standard file operations
- **S3 Gateway**: S3-compatible API for object storage access
- **Self-Healing**: Automatic detection and recovery from failures
- **Cluster Management**: Dynamic rebalancing and placement strategies

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Access                            │
├─────────────────┬───────────────────┬───────────────────────┤
│   FUSE Client   │    S3 Gateway     │      CLI Tools        │
└────────┬────────┴─────────┬─────────┴───────────┬───────────┘
         │                  │                     │
         ▼                  ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                  Metadata Cluster (Raft)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │  Leader  │──│ Follower │──│ Follower │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Data Servers                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Server 1 │  │ Server 2 │  │ Server 3 │  │ Server N │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Building

```bash
# Build all components
cargo build --release

# Build with FUSE support
cargo build --release --features fuse
```

### Running a Server

```bash
# Start a single-node server
cargo run --release -- server --node-id 1

# Start with custom ports
cargo run --release -- server \
    --node-id 1 \
    --metadata-addr 0.0.0.0:9000 \
    --data-addr 0.0.0.0:9001 \
    --s3-addr 0.0.0.0:9002 \
    --data-dir /var/lib/strata
```

### Mounting (FUSE)

```bash
# Mount the filesystem
cargo run --release --features fuse --bin strata-mount -- /mnt/strata

# With custom metadata server
cargo run --release --features fuse --bin strata-mount -- \
    --metadata-addr 192.168.1.100:9000 \
    /mnt/strata
```

### CLI Operations

```bash
# Check cluster status
cargo run -- cluster status

# List files
cargo run -- fs ls /

# Create directory
cargo run -- fs mkdir /mydir

# Show version
cargo run -- version
```

### S3 Access

```bash
# Using AWS CLI (with appropriate endpoint)
aws --endpoint-url http://localhost:9002 s3 ls
aws --endpoint-url http://localhost:9002 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9002 s3 cp file.txt s3://mybucket/
```

## Configuration

Strata can be configured via:
1. Command-line arguments
2. Environment variables (prefixed with `STRATA_`)
3. Configuration file (TOML format)

### Environment Variables

- `STRATA_NODE_ID`: Node identifier
- `STRATA_CONFIG`: Path to configuration file
- `STRATA_LOG_LEVEL`: Log level (trace, debug, info, warn, error)

### Configuration File Example

```toml
[node]
id = 1
role = "combined"

[metadata]
bind_addr = "0.0.0.0:9000"
raft_peers = ["node2:9000", "node3:9000"]

[data]
bind_addr = "0.0.0.0:9001"

[s3]
bind_addr = "0.0.0.0:9002"
region = "us-east-1"

[storage]
data_dir = "/var/lib/strata/data"
metadata_dir = "/var/lib/strata/metadata"

[erasure_coding]
data_shards = 4
parity_shards = 2

[observability]
metrics_addr = "0.0.0.0:9090"
log_level = "info"
```

## Erasure Coding

Strata uses Reed-Solomon erasure coding for data protection:

- **Default**: 4 data shards + 2 parity shards (can survive 2 failures)
- **Configurable**: Adjust data/parity ratio based on requirements
- **Space efficiency**: ~1.5x overhead vs 3x for triple replication

## Metrics

Prometheus metrics are exposed at `/metrics`:

- `strata_cluster_nodes_total`: Total nodes in cluster
- `strata_cluster_nodes_online`: Online nodes
- `strata_raft_term`: Current Raft term
- `strata_data_reads_total`: Total read operations
- `strata_data_writes_total`: Total write operations
- `strata_s3_requests_total`: S3 API requests

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run -- server --node-id 1

# Check for issues
cargo clippy

# Format code
cargo fmt
```

## License

MIT
