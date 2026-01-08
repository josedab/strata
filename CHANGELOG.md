# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open source release
- Raft-based metadata consensus cluster
- Reed-Solomon erasure coding (4+2 default)
- FUSE filesystem client for POSIX access
- S3-compatible gateway with:
  - Object versioning
  - Lifecycle policies
  - Server-side encryption (AES-256, KMS)
  - Object locking (WORM compliance)
  - Bucket replication
  - Storage classes (STANDARD, GLACIER, DEEP_ARCHIVE)
  - S3 Select (SQL queries on CSV/JSON)
  - CORS support
- Authentication and authorization (JWT, ACLs)
- Encryption at rest (AES-256-GCM, ChaCha20-Poly1305)
- Compression (LZ4, Zstd, Snappy)
- Prometheus metrics and structured logging
- Distributed locking with leases
- Circuit breaker and retry patterns
- Graceful shutdown coordination
- Point-in-time snapshots
- Background data scrubbing
- Quota enforcement
- Rate limiting
- Audit logging (SOC2/HIPAA/GDPR)
- CLI tools for cluster management
- Multi-language SDKs (Go, Python, JavaScript)
- Kubernetes operator
- Terraform provider

### Security
- Constant-time JWT verification
- Path traversal protection
- Input validation for S3 keys and bucket names
- Critical modules (raft, metadata, auth) deny panic-prone patterns

## [0.1.0] - TBD

Initial release.

[Unreleased]: https://github.com/strata-storage/strata/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/strata-storage/strata/releases/tag/v0.1.0
