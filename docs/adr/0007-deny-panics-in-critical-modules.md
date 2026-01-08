# ADR-0007: Deny Panics in Critical Modules

## Status

**Accepted**

## Context

Rust provides two error handling mechanisms:
1. **`Result<T, E>`**: Explicit error handling, caller must handle failure
2. **`panic!`/`unwrap()`/`expect()`**: Aborts current thread, potentially crashing the process

In most application code, panics are acceptable for "impossible" situations. However, in a distributed storage system, certain code paths are critical:

- **Consensus (Raft)**: A panic during log replication could corrupt cluster state
- **Metadata operations**: A panic during file creation could leave orphaned data
- **Authentication**: A panic could bypass security checks

If these modules panic, the consequences are severe:
- Data corruption or loss
- Cluster split-brain
- Security vulnerabilities
- Service unavailability

## Decision

We use Clippy's `deny` attribute to **forbid `unwrap()` and `expect()` in critical modules**:

```rust
// src/raft/mod.rs
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

// src/metadata/mod.rs
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

// src/auth/mod.rs
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
```

### What This Means

Code that would compile elsewhere fails in these modules:

```rust
// This compiles in src/main.rs
let value = some_option.unwrap();

// This FAILS to compile in src/raft/mod.rs
let value = some_option.unwrap();
// error: use of `unwrap` is disallowed in this module
```

### Required Patterns

Instead of panicking, all fallible operations must use `Result`:

```rust
// Before (forbidden):
fn get_entry(&self, index: usize) -> &LogEntry {
    self.entries.get(index).unwrap()
}

// After (required):
fn get_entry(&self, index: usize) -> Result<&LogEntry> {
    self.entries.get(index)
        .ok_or_else(|| StrataError::InvalidLogIndex(index))
}
```

### Handling "Impossible" Cases

For truly impossible cases, use explicit panics with documentation:

```rust
// Instead of .unwrap(), document why this can't fail:
fn validated_operation(&self) -> Result<()> {
    // Invariant: self.state is always initialized after new()
    // This is checked by the type system via private constructor
    let state = self.state.as_ref()
        .ok_or_else(|| StrataError::Internal("state not initialized".into()))?;

    // ... use state
    Ok(())
}
```

### Project-Wide Lint Configuration

Additional safety lints are configured in `Cargo.toml`:

```toml
[lints.rust]
unsafe_code = "warn"

[lints.clippy]
unwrap_used = "warn"      # Warn globally (deny in critical modules)
expect_used = "warn"      # Warn globally (deny in critical modules)
panic = "warn"            # Warn on explicit panic!()
```

## Consequences

### Positive

- **Prevents silent corruption**: Errors propagate rather than crashing
- **Graceful degradation**: System can return errors to clients instead of dying
- **Audit trail**: All error paths are explicit in code
- **Consensus safety**: Raft can't half-apply an operation then crash
- **Security**: Auth can't bypass checks via panic

### Negative

- **More verbose code**: Every fallible operation needs explicit handling
- **Result proliferation**: Functions that "can't fail" still return Result
- **Development friction**: Initial development is slower
- **Not foolproof**: Can still panic via array indexing, integer overflow, etc.

### Additional Protections

Beyond the lint denials, we also:

1. **Catch panics at service boundaries**:
   ```rust
   let result = std::panic::catch_unwind(|| {
       handle_request(request)
   });
   ```

2. **Use checked arithmetic**:
   ```rust
   let new_index = current_index.checked_add(1)
       .ok_or(StrataError::Overflow)?;
   ```

3. **Validate array indices**:
   ```rust
   let entry = entries.get(index)
       .ok_or(StrataError::IndexOutOfBounds)?;
   ```

### Modules with Denials

| Module | Rationale |
|--------|-----------|
| `src/raft/` | Consensus corruption would affect entire cluster |
| `src/metadata/` | State machine must be consistent |
| `src/auth/` | Security-critical code path |

### Modules Without Denials

| Module | Rationale |
|--------|-----------|
| `src/fuse/` | FUSE driver can handle panics via process restart |
| `src/s3/` | Request handler panics are isolated |
| `src/cli/` | CLI crash is acceptable |
| Tests | Panics are expected in test assertions |

## References

- [Clippy: unwrap_used](https://rust-lang.github.io/rust-clippy/master/index.html#unwrap_used)
- `src/raft/mod.rs:7-10` - Deny attributes
- `src/metadata/mod.rs:7-10` - Deny attributes
- `src/auth/mod.rs:5-10` - Deny attributes
- `Cargo.toml:145-153` - Project-wide lint configuration
