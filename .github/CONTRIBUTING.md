# Contributing to Strata

Thank you for your interest in contributing to Strata! This document provides guidelines and information for contributors.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/strata.git
   cd strata
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/strata-storage/strata.git
   ```
4. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites

- Rust 1.70+ (`rustup update stable`)
- libfuse3-dev (Linux) or macfuse (macOS) for FUSE support
- Docker and Docker Compose (for integration tests)

### Building

```bash
# Build with all features
cargo build

# Build release
cargo build --release

# Build without FUSE
cargo build --no-default-features --features s3
```

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with logging
RUST_LOG=debug cargo test -- --nocapture

# Run integration tests
cargo test --test '*'
```

### Code Quality

```bash
# Format code
cargo fmt

# Run linter
cargo clippy

# Check for security issues
cargo audit
```

## Code Style

### General Guidelines

- Follow Rust idioms and best practices
- Use `tracing` macros (`info!`, `debug!`, `error!`) instead of `println!`
- Avoid `unwrap()` and `expect()` in library code - use proper error handling
- Add `#[cfg(test)]` module for unit tests in the same file

### Error Handling

```rust
use crate::error::{Result, StrataError};

pub fn my_function() -> Result<()> {
    something_fallible()?;
    Ok(())
}
```

### Async Code

```rust
use async_trait::async_trait;

#[async_trait]
pub trait MyTrait {
    async fn do_something(&self) -> Result<()>;
}
```

### Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        // Test code
    }

    #[tokio::test]
    async fn test_async() {
        // Async test code
    }
}
```

## Pull Request Process

1. **Update documentation** if you're changing behavior
2. **Add tests** for new functionality
3. **Run the full test suite** before submitting
4. **Update CHANGELOG.md** if applicable
5. **Fill out the PR template** completely
6. **Request review** from maintainers

### PR Title Format

Use conventional commit format:
- `feat: add new feature`
- `fix: resolve bug in X`
- `docs: update quickstart guide`
- `refactor: simplify error handling`
- `test: add integration tests for Y`
- `chore: update dependencies`

### Review Process

- At least one maintainer approval required
- All CI checks must pass
- Address review feedback promptly
- Squash commits before merge (or maintainer will)

## Issue Guidelines

### Bug Reports

- Use the bug report template
- Include reproduction steps
- Provide relevant logs and configuration
- Specify your environment

### Feature Requests

- Use the feature request template
- Explain the problem you're solving
- Describe your proposed solution
- Consider alternatives

## Areas for Contribution

### Good First Issues

Look for issues labeled `good first issue` - these are suitable for newcomers.

### Desired Contributions

- Documentation improvements
- Test coverage
- Performance optimizations
- Bug fixes
- S3 compatibility improvements
- Client library development

### Major Features

For major features, please open an issue first to discuss the approach. This ensures:
- The feature aligns with project goals
- No duplicate work
- Architectural considerations are addressed

## Community Guidelines

- Be respectful and inclusive
- Help newcomers
- Give constructive feedback
- Assume good intentions

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

- Open a [Discussion](https://github.com/strata-storage/strata/discussions)
- File an issue with the `question` label
- Review existing documentation

Thank you for contributing to Strata!
