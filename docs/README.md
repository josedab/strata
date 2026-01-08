# Strata Documentation

Welcome to the Strata documentation. Strata is a distributed file system combining POSIX compatibility with S3 access, written in Rust.

## Documentation Index

### Getting Started
- [Quick Start Guide](guides/quickstart.md) - Get Strata running in minutes
- [Installation](guides/installation.md) - Detailed installation instructions

### Architecture
- [Architecture Overview](architecture/overview.md) - High-level system design
- [Architecture Diagrams](architecture/diagrams.md) - Visual architecture diagrams
- [Raft Consensus](architecture/raft.md) - Metadata cluster consensus
- [Data Storage](architecture/data-storage.md) - Chunk storage and erasure coding
- [Cluster Management](architecture/cluster.md) - Node management and rebalancing

### Guides
- [Deployment Guide](guides/deployment.md) - Production deployment instructions
- [Operator's Guide](guides/operations.md) - Day-to-day operations and monitoring
- [Configuration Reference](guides/configuration.md) - All configuration options
- [Troubleshooting](guides/troubleshooting.md) - Common issues and solutions

### API Reference
- [Core Types](api/types.md) - Core data types and structures
- [Error Handling](api/errors.md) - Error types and handling patterns
- [Client Library](api/client.md) - Native Rust client API (MetadataClient, DataClient)
- [Metadata API](api/metadata.md) - Namespace and file operations
- [Data API](api/data.md) - Chunk storage operations
- [S3 API Reference](api/s3.md) - Full S3 API compatibility documentation

### S3 Features
The S3 gateway provides comprehensive S3 compatibility:

| Feature | Description |
|---------|-------------|
| **Object Versioning** | Preserve and restore object versions |
| **Lifecycle Policies** | Automated transitions and expiration |
| **Server-Side Encryption** | AES-256 and KMS encryption at rest |
| **Object Locking (WORM)** | Compliance and governance retention modes |
| **Bucket Replication** | Cross-region replication for DR |
| **Storage Classes** | STANDARD, GLACIER, DEEP_ARCHIVE with restore |
| **S3 Select** | SQL queries on CSV/JSON objects |
| **CORS Support** | Cross-origin requests with credentials |

See [S3 API Reference](api/s3.md) for detailed usage examples.

> **Note**: Strata uses **HTTP/REST with JSON** for inter-service communication, not gRPC. The `tonic`/`prost` dependencies in `Cargo.toml` are currently unused.

### Development
- [Contributing](../CONTRIBUTING.md) - How to contribute
- [Building from Source](guides/building.md) - Build instructions
- [Testing](guides/testing.md) - Running tests

## Quick Links

| Resource | Description |
|----------|-------------|
| [GitHub Repository](https://github.com/strata-storage/strata) | Source code |
| [Issue Tracker](https://github.com/strata-storage/strata/issues) | Report bugs |
| [Changelog](../CHANGELOG.md) | Version history |

## Support

For questions and support:
- Open a [GitHub Issue](https://github.com/strata-storage/strata/issues)
- Check the [Troubleshooting Guide](guides/troubleshooting.md)
