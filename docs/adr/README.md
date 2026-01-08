# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Strata distributed file system.

## What are ADRs?

ADRs document significant architectural decisions made during the development of Strata. Each ADR captures:
- **Context**: The situation and forces that led to the decision
- **Decision**: What we decided to do and why
- **Consequences**: The resulting impacts, both positive and negative

## ADR Index

### Foundation Decisions

| ADR | Title | Status | Summary |
|-----|-------|--------|---------|
| [0001](0001-raft-consensus-for-metadata.md) | Raft Consensus for Metadata | Accepted | Custom Raft implementation for metadata replication, chosen over Paxos for understandability and over external dependencies for control. |
| [0002](0002-erasure-coding-over-replication.md) | Erasure Coding Over Replication | Accepted | Reed-Solomon erasure coding (4+2 default) instead of 3x replication, reducing storage overhead from 200% to 50%. |
| [0003](0003-dual-access-interface.md) | Dual Access Interface (FUSE + S3) | Accepted | Support both POSIX (via FUSE) and object storage (via S3 gateway) access patterns through shared metadata/data layers. |
| [0004](0004-tokio-async-runtime.md) | Tokio as Async Runtime | Accepted | Tokio multi-threaded runtime with explicit feature selection for async I/O throughout the system. |

### Runtime & Safety Decisions

| ADR | Title | Status | Summary |
|-----|-------|--------|---------|
| [0005](0005-unified-error-type.md) | Unified Error Type with Retryability | Accepted | Single `StrataError` enum with 40+ variants including `is_retryable()` method for intelligent retry logic. |
| [0006](0006-http-rest-over-grpc.md) | HTTP REST Over gRPC | Accepted | HTTP REST with reqwest client for inter-service communication, prioritizing debuggability over performance. |
| [0007](0007-deny-panics-in-critical-modules.md) | Deny Panics in Critical Modules | Accepted | `#![deny(clippy::unwrap_used)]` in raft, metadata, and auth modules to prevent crashes from propagating. |
| [0008](0008-configuration-presets.md) | Configuration Presets | Accepted | Named presets (development, production_small, production_large) for complex configurations with builder customization. |

### Operability Decisions

| ADR | Title | Status | Summary |
|-----|-------|--------|---------|
| [0009](0009-circuit-breaker-pattern.md) | Circuit Breaker Pattern | Accepted | Three-state circuit breaker (Closed/Open/HalfOpen) to prevent cascading failures when dependencies are unhealthy. |
| [0010](0010-lease-based-distributed-locking.md) | Lease-Based Distributed Locking | Accepted | Time-bounded locks with automatic expiration to prevent deadlocks in distributed operations. |
| [0011](0011-pluggable-placement-strategies.md) | Pluggable Placement Strategies | Accepted | Configurable chunk placement (Random, SpreadDomains, LeastLoaded) to optimize for different deployment topologies. |
| [0012](0012-multi-algorithm-compression-encryption.md) | Multi-Algorithm Compression & Encryption | Accepted | Enum-based algorithm selection (LZ4/Zstd/Snappy, AES-GCM/ChaCha20) with versioned format tags per chunk. |

## ADR Lifecycle

1. **Proposed**: Decision under discussion
2. **Accepted**: Decision approved and implemented
3. **Deprecated**: Decision superseded by a newer ADR
4. **Superseded**: Replaced by another ADR (links to replacement)

## Creating New ADRs

When adding a new ADR:

1. Copy the template below
2. Use the next available number (e.g., `0013-*.md`)
3. Fill in all sections
4. Submit for review
5. Update this index after acceptance

### Template

```markdown
# ADR-NNNN: Title

## Status

**Proposed** | **Accepted** | **Deprecated** | **Superseded by [ADR-XXXX](link)**

## Context

What is the issue? What forces are at play?

## Decision

What did we decide? Include code examples if helpful.

## Consequences

### Positive
- Benefit 1
- Benefit 2

### Negative
- Tradeoff 1
- Tradeoff 2

## References

- Links to related docs, papers, or code
```

## Reading Guide

For those new to the codebase, we recommend reading the ADRs in this order:

1. **Start with foundations**: ADR-0001 (Raft) and ADR-0002 (Erasure Coding) explain the core distributed systems concepts
2. **Understand access patterns**: ADR-0003 (Dual Interface) shows how clients interact with Strata
3. **Learn the runtime**: ADR-0004 (Tokio) and ADR-0005 (Error Handling) cover execution fundamentals
4. **Study safety mechanisms**: ADR-0007 (No Panics), ADR-0009 (Circuit Breaker), ADR-0010 (Locking) explain reliability patterns
5. **Configure for your environment**: ADR-0008 (Presets), ADR-0011 (Placement), ADR-0012 (Compression/Encryption) cover operational tuning

## Related Documentation

- [Architecture Overview](../architecture/overview.md)
- [Architecture Diagrams](../architecture/diagrams.md)
- [Configuration Guide](../guides/configuration.md)
- [API Documentation](../api/)
