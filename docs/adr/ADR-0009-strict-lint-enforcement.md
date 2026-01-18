# ADR-0009: Strict Lint Enforcement in Critical Modules

## Status

Accepted

## Context

Rust provides strong compile-time safety guarantees, but some runtime behaviors can still cause problems:

### The `unwrap()` / `expect()` Problem

```rust
// This compiles fine but panics at runtime if None
let value = some_option.unwrap();

// This compiles fine but panics if the lock is poisoned
let guard = mutex.lock().unwrap();
```

Panics in server code cause:
- **Service disruption**: The thread or task crashes
- **Data inconsistency**: Partially completed operations may leave invalid state
- **Cascading failures**: If a consensus leader panics, the cluster must re-elect
- **Security vulnerabilities**: Panic messages may leak sensitive information

### Critical vs. Non-Critical Code

Not all code is equally sensitive:

| Module | Criticality | Panic Impact |
|--------|-------------|--------------|
| Raft consensus | Critical | Split-brain, data loss |
| Authentication | Critical | Security bypass |
| Metadata operations | Critical | Filesystem corruption |
| CLI argument parsing | Low | User retry |
| Test code | Low | Test failure |

Applying the same strictness everywhere would be counterproductive—test code legitimately uses `unwrap()` for brevity.

## Decision

We will **selectively apply strict lint denial** to critical modules using Rust's module-level attributes.

### Implementation

```rust
// src/raft/mod.rs
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

// This will now cause a compile error:
// let term = self.current_term.unwrap();

// Must use explicit error handling:
let term = self.current_term.ok_or(RaftError::NoTerm)?;
```

### Modules with Strict Enforcement

| Module | Path | Rationale |
|--------|------|-----------|
| Raft Consensus | `src/raft/mod.rs` | Panics violate consensus safety |
| Metadata Operations | `src/metadata/mod.rs` | Panics corrupt filesystem state |
| Authentication | `src/auth/mod.rs` | Panics may bypass security checks |

### Modules Without Strict Enforcement

| Module | Rationale |
|--------|-----------|
| `src/main.rs` | CLI can panic on invalid args |
| `tests/` | Test code uses unwrap for brevity |
| `benches/` | Benchmark code prioritizes clarity |

### Alternative Approaches Considered

**Global Clippy Configuration**
```toml
# Cargo.toml
[lints.clippy]
unwrap_used = "deny"
```

Rejected: Too broad, makes test code verbose and harder to read.

**Runtime Panic Hooks**
```rust
std::panic::set_hook(Box::new(|info| {
    // Log and recover
}));
```

Rejected: Recovery from arbitrary panics is unreliable; prevention is better.

## Consequences

### Positive

- **Compile-time safety**: Unwrap in critical paths is caught before deployment
- **Explicit error handling**: Forces developers to think about failure modes
- **Auditability**: `grep` for `#![deny(clippy::unwrap_used)]` shows protected modules
- **Gradual adoption**: Can add more modules over time
- **Documentation**: Lint attributes document criticality

### Negative

- **Verbose code**: Every `Option` and `Result` needs explicit handling
- **Learning curve**: Contributors must understand the pattern
- **False positives**: Some unwraps are actually safe (e.g., after `is_some()` check)
- **Inconsistency**: Different rules in different modules

### Handling "Safe" Unwraps

When an unwrap is provably safe, use a comment explaining why:

```rust
// In non-critical module, or with documented safety:
let value = map.get(&key)
    .expect("key always present after initialization");  // Only if expect_used not denied

// In critical module, still must handle explicitly:
let value = map.get(&key)
    .ok_or_else(|| InvariantViolation("key missing after init"))?;
```

### CI Integration

```yaml
# .github/workflows/ci.yml
- name: Clippy
  run: cargo clippy -- -D warnings
```

The module-level `#![deny(...)]` ensures CI catches violations automatically.

### Implications

- Code reviews should verify critical modules don't bypass lints
- New critical modules should add the deny attributes
- Error types must cover all failure cases (no "this can't happen")
- Consider adding `#![deny(clippy::panic)]` for even stricter enforcement

## References

- `src/raft/mod.rs:9-10` - Raft module lint configuration
- `src/metadata/mod.rs:9-10` - Metadata module lint configuration
- `src/auth/mod.rs:5-6` - Auth module lint configuration
- `Cargo.toml:153-154` - Documentation of the pattern
