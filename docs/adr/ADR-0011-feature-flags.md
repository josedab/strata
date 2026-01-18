# ADR-0011: Feature Flags for Optional Functionality

## Status

Accepted

## Context

Strata provides multiple access interfaces (FUSE, S3, native client), but not all deployments need all interfaces:

| Deployment | FUSE | S3 | Native |
|------------|------|-----|--------|
| Legacy app migration | Yes | No | No |
| Cloud-native backend | No | Yes | No |
| High-performance compute | No | No | Yes |
| General purpose | Yes | Yes | Yes |

Including all interfaces unconditionally has costs:

### Dependency Overhead
- FUSE requires `libfuse` system library
- S3 gateway pulls in HTTP server dependencies
- Each interface adds compile time and binary size

### Platform Limitations
- FUSE is Linux/macOS only (not Windows without WSL)
- Some embedded systems lack FUSE support
- Containers may not have FUSE privileges

### Security Surface
- Unused code is still attack surface
- Fewer features = smaller audit scope
- Principle of least privilege

### Operational Complexity
- Unused features still appear in configuration
- Documentation covers irrelevant options
- Monitoring includes unused metrics

## Decision

We will use **Cargo feature flags** to make FUSE and S3 optional, with both enabled by default.

### Cargo.toml Configuration

```toml
[features]
default = ["fuse", "s3"]
fuse = ["dep:fuser"]
s3 = []

[dependencies]
fuser = { version = "0.14", optional = true }
```

### Conditional Compilation

```rust
// src/lib.rs

#[cfg(feature = "fuse")]
pub mod fuse;

#[cfg(feature = "s3")]
pub mod s3;

// Always available
pub mod client;
pub mod metadata;
pub mod data;
pub mod raft;
```

### Build Variants

```bash
# Full build (default)
cargo build

# Without FUSE (for containers, CI, Windows)
cargo build --no-default-features --features s3

# Without S3 (pure POSIX deployment)
cargo build --no-default-features --features fuse

# Minimal (native client only)
cargo build --no-default-features

# Explicit full features
cargo build --features "fuse,s3"
```

### Binary Size Impact

| Configuration | Binary Size (release) | Dependencies |
|---------------|----------------------|--------------|
| Full | ~25 MB | 180+ crates |
| No FUSE | ~23 MB | 170+ crates |
| No S3 | ~22 MB | 165+ crates |
| Minimal | ~20 MB | 150+ crates |

*Approximate values; actual sizes depend on optimization level*

## Consequences

### Positive

- **Flexibility**: Deploy only what's needed
- **Smaller binaries**: Reduce download and memory footprint
- **Faster builds**: Fewer dependencies to compile
- **Platform support**: Build without FUSE on Windows
- **Security**: Smaller attack surface for minimal deployments
- **Clear boundaries**: Feature modules are isolated

### Negative

- **Testing matrix**: Must test all feature combinations
- **Documentation**: Must document feature availability
- **User confusion**: "Why doesn't S3 work?" (feature not enabled)
- **Conditional code**: `#[cfg]` attributes add complexity
- **Dependency management**: Optional deps have different semantics

### Feature Combinations to Test

| fuse | s3 | Status |
|------|-----|--------|
| Yes | Yes | Primary (default) |
| Yes | No | Supported |
| No | Yes | Supported |
| No | No | Supported (minimal) |

### CI Configuration

```yaml
jobs:
  test:
    strategy:
      matrix:
        features:
          - ""  # default (fuse,s3)
          - "--no-default-features --features fuse"
          - "--no-default-features --features s3"
          - "--no-default-features"
    steps:
      - run: cargo test ${{ matrix.features }}
      - run: cargo clippy ${{ matrix.features }}
```

### User Documentation

```markdown
## Installation

### Full Installation (Recommended)
```bash
cargo install strata
```

### Without FUSE Support
For environments without FUSE (containers, Windows):
```bash
cargo install strata --no-default-features --features s3
```
```

### Future Features

Additional optional features may be added:
- `metrics` - Prometheus metrics endpoint
- `tracing` - Distributed tracing
- `tls` - TLS support (currently always included)
- `compression-zstd` - Zstd compression (vs just LZ4)

### Implications

- Feature-gated code must not break other features
- Public API should be consistent regardless of features
- Error messages should indicate when a feature is needed
- Release builds should test all supported combinations

## References

- `Cargo.toml:13-16` - Feature definitions
- `src/lib.rs:170-174` - Conditional module inclusion
- Cargo documentation on features
