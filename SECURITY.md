# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

1. **Do NOT** open a public issue for security vulnerabilities
2. Email security concerns to: [security@strata-storage.io](mailto:security@strata-storage.io)
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

### What to Expect

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 7 days
- **Resolution Timeline**: Depends on severity
  - Critical: Within 7 days
  - High: Within 30 days
  - Medium/Low: Within 90 days

### Disclosure Policy

- We follow coordinated disclosure
- We'll work with you on timing of any public disclosure
- Credit will be given to reporters (unless anonymity is requested)

## Security Measures

### Code Security

- Critical modules (`src/raft/`, `src/metadata/`, `src/auth/`) use `#![deny(clippy::unwrap_used)]` to prevent panics
- All `unsafe` blocks require `// SAFETY:` comments explaining invariants
- Path traversal prevention in S3 handlers and multi-tenancy isolation
- Constant-time comparison for JWT verification

### Cryptography

- JWT tokens use HMAC-SHA256 via the `ring` crate
- Encryption at rest: AES-256-GCM or ChaCha20-Poly1305
- TLS support for all network communication

### Input Validation

- S3 key validation (max length, no "..", no null bytes)
- Bucket name validation (length, character restrictions)
- Path normalization to prevent traversal attacks

### Authentication

- JWT-based authentication with configurable expiration
- ACL-based authorization (POSIX-compatible)
- Support for anonymous access (configurable, disabled by default)

## Security Best Practices for Deployment

1. **Network Security**
   - Use TLS for all external communication
   - Restrict metadata cluster ports to internal networks
   - Use firewalls to limit access

2. **Authentication**
   - Disable anonymous access in production
   - Use strong JWT secrets
   - Rotate credentials regularly

3. **Encryption**
   - Enable encryption at rest for sensitive data
   - Use secure key management

4. **Monitoring**
   - Enable audit logging
   - Monitor for unusual access patterns
   - Set up alerts for authentication failures

5. **Updates**
   - Keep Strata updated to the latest version
   - Monitor security advisories
   - Run `cargo audit` regularly on dependencies

## Known Limitations

- The PMEM and io_uring modules use `unsafe` code for performance
- Emulated modes (for testing) do not provide the same security guarantees as production configurations

## Security Advisories

Security advisories will be published via:
- GitHub Security Advisories
- Release notes
- Direct notification to known deployers (for critical issues)
