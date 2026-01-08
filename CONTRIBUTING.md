# Contributing to Strata

Thank you for your interest in contributing to Strata! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct. Please be respectful and constructive in all interactions.

## Getting Started

### Prerequisites

- **Rust**: 1.70 or later (install via [rustup](https://rustup.rs/))
- **Linux/macOS**: Primary development platforms
- **libfuse3-dev** (optional): Required for FUSE filesystem support on Linux

### Development Setup

```bash
# Clone the repository
git clone https://github.com/strata-storage/strata.git
cd strata

# Build the project
cargo build

# Run tests
cargo test

# Run lints
cargo clippy

# Format code
cargo fmt
```

## How to Contribute

### Reporting Bugs

1. Check existing [issues](https://github.com/strata-storage/strata/issues) to avoid duplicates
2. Use the bug report template
3. Include:
   - Rust version (`rustc --version`)
   - Operating system and version
   - Steps to reproduce
   - Expected vs actual behavior
   - Relevant logs or error messages

### Suggesting Features

1. Check existing issues for similar suggestions
2. Use the feature request template
3. Describe:
   - The problem you're trying to solve
   - Your proposed solution
   - Alternatives you've considered

### Pull Requests

1. **Fork** the repository
2. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following our coding standards
4. **Test your changes**:
   ```bash
   cargo test --all-features
   cargo clippy -- -D warnings
   cargo fmt --check
   ```
5. **Commit** with clear, descriptive messages
6. **Push** to your fork
7. **Open a Pull Request** against `main`

### PR Requirements

- [ ] All tests pass
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy -- -D warnings`)
- [ ] Documentation updated (if applicable)
- [ ] Changelog entry added (if applicable)

## Coding Standards

### Rust Style

- Follow standard Rust naming conventions
- Use `rustfmt` for formatting
- Address all `clippy` warnings
- Write doc comments for public APIs

### Error Handling

- Use `Result<T, StrataError>` (via `crate::error::Result`)
- Avoid `unwrap()` and `expect()` in library code
- Critical modules (`raft`, `metadata`, `auth`) deny `unwrap_used` and `expect_used`

### Testing

- Write unit tests in the same file (`#[cfg(test)]`)
- Integration tests go in `tests/`
- Use descriptive test names
- Test edge cases and error conditions

### Documentation

- All public items must have doc comments (`///`)
- Include examples in doc comments where helpful
- Module-level docs (`//!`) should explain purpose

### Unsafe Code

- Minimize use of `unsafe`
- Every `unsafe` block must have a `// SAFETY:` comment
- Explain the invariants that make the code safe

## Project Structure

```
strata/
├── src/
│   ├── lib.rs          # Library root
│   ├── main.rs         # CLI entry point
│   ├── error.rs        # Error types
│   ├── types.rs        # Core types
│   ├── raft/           # Raft consensus
│   ├── metadata/       # Metadata service
│   ├── data/           # Data server
│   ├── cluster/        # Cluster management
│   ├── fuse/           # FUSE filesystem
│   ├── s3/             # S3 gateway
│   └── auth/           # Authentication
├── tests/              # Integration tests
├── benches/            # Benchmarks
└── docs/               # Documentation
```

## Communication

- **Issues**: Bug reports and feature requests
- **Discussions**: Questions and general discussion
- **Pull Requests**: Code contributions

## License

By contributing to Strata, you agree that your contributions will be licensed under the MIT License.

## Recognition

Contributors are recognized in release notes. Thank you for helping make Strata better!
