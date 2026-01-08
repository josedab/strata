# Strata

A distributed file system combining POSIX compatibility with S3 access, written in Rust.

[![Build Status](https://img.shields.io/github/actions/workflow/status/strata-storage/strata/ci.yml?branch=main)](https://github.com/strata-storage/strata/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

## Overview

Strata is an enterprise-grade distributed storage system designed for modern infrastructure. It provides:

- **Unified Access**: POSIX file system and S3-compatible object storage in one system
- **Strong Consistency**: Raft-based metadata cluster ensures data consistency across failures
- **High Durability**: Reed-Solomon erasure coding with configurable fault tolerance
- **Self-Healing**: Automatic failure detection and data recovery
- **Horizontal Scaling**: Add data servers to increase capacity without downtime

## Key Features

| Feature | Description |
|---------|-------------|
| **Raft Consensus** | Consistent metadata with automatic leader election |
| **Erasure Coding** | Reed-Solomon 4+2 default (configurable) for 1.5x storage overhead |
| **POSIX via FUSE** | Mount as a local filesystem with full POSIX semantics |
| **S3 Gateway** | Full S3-compatible API with versioning, encryption, CORS, and more |
| **Self-Healing** | Phi-accrual failure detection with automatic recovery |
| **Placement Strategies** | Random, spread-domains, or least-loaded placement |
| **Encryption** | AES-256-GCM or ChaCha20-Poly1305 at rest |
| **Observability** | Prometheus metrics and structured logging |

### S3 Gateway Features

| Feature | Description |
|---------|-------------|
| **Object Versioning** | Preserve and restore object versions |
| **Lifecycle Policies** | Automated transitions and expiration |
| **Server-Side Encryption** | AES-256 and KMS encryption at rest |
| **Object Locking (WORM)** | Compliance and governance retention modes |
| **Bucket Replication** | Cross-region replication for disaster recovery |
| **Storage Classes** | STANDARD, GLACIER, DEEP_ARCHIVE with restore |
| **S3 Select** | SQL queries on CSV/JSON objects |
| **CORS Support** | Cross-origin requests with credentials |

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
│  │  Leader  │◄─┤ Follower │◄─┤ Follower │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
│       │              │              │                        │
│       └──────────────┴──────────────┘                        │
│              Raft Consensus                                  │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Data Servers                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Server 1 │  │ Server 2 │  │ Server 3 │  │ Server N │    │
│  │ Shards   │  │ Shards   │  │ Shards   │  │ Shards   │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│              Erasure Coded Data Storage                      │
└─────────────────────────────────────────────────────────────┘
```

For detailed architecture documentation, see [docs/architecture/overview.md](docs/architecture/overview.md).

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

# Upload with server-side encryption
aws --endpoint-url http://localhost:9002 s3 cp file.txt s3://mybucket/ --sse AES256

# Enable bucket versioning
aws --endpoint-url http://localhost:9002 s3api put-bucket-versioning \
    --bucket mybucket --versioning-configuration Status=Enabled

# Query data with S3 Select
aws --endpoint-url http://localhost:9002 s3api select-object-content \
    --bucket mybucket --key data.csv \
    --expression "SELECT * FROM s3object WHERE price > 100" \
    --expression-type SQL \
    --input-serialization '{"CSV": {"FileHeaderInfo": "USE"}}' \
    --output-serialization '{"CSV": {}}' output.csv
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

[s3.auth]
allow_anonymous = false
credentials = [
    { access_key_id = "AKIAIOSFODNN7EXAMPLE", secret_key = "wJalrXUtnFEMI/K7MDENG", user_id = "admin" }
]

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
# Run all tests
cargo test

# Run tests with specific features
cargo test --no-default-features --features s3

# Run with verbose logging
RUST_LOG=debug cargo run -- server --node-id 1

# Check for issues
cargo clippy

# Format code
cargo fmt

# Run benchmarks
cargo bench
```

## Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture/overview.md) | System design and components |
| [Architecture Diagrams](docs/architecture/diagrams.md) | Visual architecture diagrams |
| [S3 API Reference](docs/api/s3.md) | S3 compatibility matrix and usage |
| [Deployment Guide](docs/guides/deployment.md) | Production deployment instructions |
| [Operator's Guide](docs/guides/operations.md) | Monitoring and maintenance |
| [Configuration Reference](docs/guides/configuration.md) | All configuration options |
| [API Reference](docs/api/types.md) | Core types and error handling |

### Generate Rust Documentation

```bash
cargo doc --open
```

## Project Structure

```
strata/
├── src/
│   ├── lib.rs          # Library root
│   ├── main.rs         # CLI entry point
│   ├── raft/           # Raft consensus implementation
│   ├── metadata/       # Metadata service (file system state)
│   ├── data/           # Data server (chunk storage)
│   ├── cluster/        # Cluster management
│   ├── fuse/           # FUSE filesystem client
│   ├── s3/             # S3 gateway
│   ├── auth/           # Authentication & authorization
│   ├── erasure/        # Reed-Solomon encoding
│   └── ...             # Additional modules
├── docs/               # Documentation
├── tests/              # Integration tests
├── benches/            # Performance benchmarks
├── operator/           # Kubernetes operator
├── terraform/          # Terraform provider
└── cli/                # CLI tool
```

## Requirements

- **Rust**: 1.70 or later
- **OS**: Linux (production), macOS (development)
- **Optional**: libfuse3-dev for FUSE support

## Contributing

Contributions are welcome! Please see our contributing guidelines.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgments

Strata builds on these excellent projects:
- [Raft](https://raft.github.io/) - Consensus algorithm
- [RocksDB](https://rocksdb.org/) - Persistent storage
- [reed-solomon-erasure](https://crates.io/crates/reed-solomon-erasure) - Erasure coding
- [fuser](https://crates.io/crates/fuser) - FUSE bindings
- [Tokio](https://tokio.rs/) - Async runtime

## License

MIT License - see [LICENSE](LICENSE) for details.
