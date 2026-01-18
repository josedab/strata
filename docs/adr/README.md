# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) documenting significant technical decisions made during Strata's development.

## What is an ADR?

An ADR captures the context, decision, and consequences of an architecturally significant choice. They help new team members understand "why is it built this way?" and provide historical context for future decisions.

## Index

| ADR | Title | Status | Summary |
|-----|-------|--------|---------|
| [ADR-0001](ADR-0001-raft-consensus-for-metadata.md) | Raft Consensus for Metadata | Accepted | Use Raft for linearizable metadata consistency |
| [ADR-0002](ADR-0002-reed-solomon-erasure-coding.md) | Reed-Solomon Erasure Coding | Accepted | Use erasure coding over replication for storage efficiency |
| [ADR-0003](ADR-0003-layered-access-patterns.md) | Layered Access Patterns | Accepted | Provide FUSE, S3, and native client interfaces |
| [ADR-0004](ADR-0004-async-rust-with-tokio.md) | Async Rust with Tokio | Accepted | Build on async/await for scalable concurrency |
| [ADR-0005](ADR-0005-hybrid-rpc-strategy.md) | Hybrid RPC Strategy | Accepted | gRPC for internal, REST for external communication |
| [ADR-0006](ADR-0006-centralized-error-type.md) | Centralized Error Type | Accepted | Single error enum with POSIX errno mapping |
| [ADR-0007](ADR-0007-configuration-presets-pattern.md) | Configuration Presets Pattern | Accepted | Named presets for operational simplicity |
| [ADR-0008](ADR-0008-circuit-breaker-pattern.md) | Circuit Breaker Pattern | Accepted | Prevent cascading failures with circuit breakers |
| [ADR-0009](ADR-0009-strict-lint-enforcement.md) | Strict Lint Enforcement | Accepted | Deny unwrap/expect in critical modules |
| [ADR-0010](ADR-0010-graceful-shutdown.md) | Graceful Shutdown | Accepted | Coordinated shutdown with broadcast channels |
| [ADR-0011](ADR-0011-feature-flags.md) | Feature Flags | Accepted | Optional FUSE and S3 via Cargo features |
| [ADR-0012](ADR-0012-audit-logging.md) | Audit Logging | Accepted | Compliance-ready audit trail |

## Decision Categories

### Foundation
- **ADR-0001**: How we ensure metadata consistency (Raft)
- **ADR-0002**: How we protect data durability (erasure coding)

### Access Layer
- **ADR-0003**: How clients interact with the system (multiple interfaces)
- **ADR-0005**: How components communicate (hybrid RPC)

### Implementation
- **ADR-0004**: How we handle concurrency (async Rust)
- **ADR-0006**: How we handle errors (centralized type)
- **ADR-0009**: How we ensure code safety (strict lints)

### Operations
- **ADR-0007**: How we simplify configuration (presets)
- **ADR-0010**: How we handle shutdown (graceful termination)
- **ADR-0011**: How we support different deployments (feature flags)

### Resilience & Compliance
- **ADR-0008**: How we prevent cascading failures (circuit breaker)
- **ADR-0012**: How we meet compliance requirements (audit logging)

## Creating New ADRs

When making a significant architectural decision:

1. Copy the template below
2. Number sequentially (ADR-0013, ADR-0014, etc.)
3. Fill in all sections
4. Submit for review with the related code changes

### Template

```markdown
# ADR-NNNN: [Short Title]

## Status

[Proposed | Accepted | Deprecated | Superseded by ADR-XXXX]

## Context

[What is the issue that we're seeing that is motivating this decision?]

## Decision

[What is the change that we're proposing and/or doing?]

## Consequences

### Positive
[What becomes easier or possible as a result?]

### Negative
[What becomes harder or is ruled out?]

### Implications
[What other decisions or changes does this require?]

## References

[Links to relevant code, documentation, or external resources]
```

## Superseding Decisions

When a decision is superseded:

1. Update the old ADR's status to "Superseded by ADR-XXXX"
2. Reference the old ADR in the new one's Context section
3. Explain why the original decision is being changed

---

*Last updated: 2026-01-17*
