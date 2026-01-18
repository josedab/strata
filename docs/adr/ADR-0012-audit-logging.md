# ADR-0012: Audit Logging for Compliance Requirements

## Status

Accepted

## Context

Enterprise storage systems must meet regulatory and compliance requirements:

### SOC 2 (Service Organization Control 2)
- Track who accessed what data and when
- Demonstrate access controls are enforced
- Provide evidence for auditors

### HIPAA (Health Insurance Portability and Accountability Act)
- Log all access to protected health information (PHI)
- Maintain audit trails for 6 years
- Support breach investigation

### GDPR (General Data Protection Regulation)
- Track processing of personal data
- Support data subject access requests
- Demonstrate lawful processing basis

### PCI DSS (Payment Card Industry Data Security Standard)
- Log all access to cardholder data
- Track administrative actions
- Detect and respond to security events

### Common Requirements

All frameworks require:
1. **Who**: Identity of the actor (user, service account)
2. **What**: Action performed (read, write, delete, admin)
3. **When**: Timestamp with sufficient precision
4. **Where**: Resource affected (file path, bucket, object)
5. **Outcome**: Success or failure, with reason

Standard application logging (`info!`, `debug!`) is insufficient:
- Mixed with operational logs
- Inconsistent format
- May be filtered or rotated
- Not tamper-evident

## Decision

We will implement a **dedicated audit logging subsystem** with:

### Structured Audit Events

```rust
pub enum AuditCategory {
    Authentication,    // Login, logout, token operations
    Authorization,     // Permission checks, ACL changes
    FileOperation,     // Create, read, write, delete files
    DirectoryOperation, // Mkdir, rmdir, readdir
    MetadataChange,    // Chmod, chown, setxattr
    AdminAction,       // Configuration changes, user management
    SecurityEvent,     // Failed auth, permission denied
    DataAccess,        // Bulk data operations
}

pub enum AuditAction {
    // Authentication
    LoginSuccess,
    LoginFailure,
    Logout,
    TokenIssued,
    TokenRevoked,

    // File Operations
    FileCreate,
    FileRead,
    FileWrite,
    FileDelete,
    FileRename,

    // Directory Operations
    DirectoryCreate,
    DirectoryRead,
    DirectoryDelete,

    // And 20+ more action types...
}

pub struct AuditEvent {
    pub timestamp: DateTime<Utc>,
    pub category: AuditCategory,
    pub action: AuditAction,
    pub actor: AuditActor,
    pub resource: Option<String>,
    pub outcome: AuditOutcome,
    pub details: HashMap<String, String>,
    pub request_id: Option<String>,
    pub client_ip: Option<IpAddr>,
}
```

### Audit Actor Tracking

```rust
pub struct AuditActor {
    pub user_id: Option<String>,
    pub username: Option<String>,
    pub service_account: Option<String>,
    pub session_id: Option<String>,
    pub auth_method: AuthMethod,
}
```

### Usage in Code

```rust
impl MetadataService {
    pub async fn create_file(&self, req: CreateFileRequest) -> Result<Inode> {
        // Perform the operation
        let result = self.do_create_file(&req).await;

        // Audit log regardless of outcome
        self.audit.log(AuditEvent {
            timestamp: Utc::now(),
            category: AuditCategory::FileOperation,
            action: AuditAction::FileCreate,
            actor: req.actor.clone(),
            resource: Some(format!("{}/{}", req.parent, req.name)),
            outcome: match &result {
                Ok(_) => AuditOutcome::Success,
                Err(e) => AuditOutcome::Failure(e.to_string()),
            },
            details: hashmap! {
                "parent_inode" => req.parent.to_string(),
                "filename" => req.name.clone(),
                "mode" => format!("{:o}", req.mode),
            },
            request_id: req.request_id,
            client_ip: req.client_ip,
        }).await;

        result
    }
}
```

### Output Format (JSON Lines)

```json
{"timestamp":"2024-01-15T10:30:00Z","category":"FileOperation","action":"FileCreate","actor":{"user_id":"u123","username":"alice"},"resource":"/data/report.pdf","outcome":"Success","details":{"mode":"644"},"request_id":"req-456","client_ip":"10.0.1.50"}
{"timestamp":"2024-01-15T10:30:05Z","category":"Authorization","action":"PermissionDenied","actor":{"user_id":"u789","username":"bob"},"resource":"/secrets/key.pem","outcome":"Failure","details":{"required":"read","actual":"none"},"client_ip":"10.0.1.51"}
```

## Consequences

### Positive

- **Compliance ready**: Meets SOC2, HIPAA, GDPR, PCI DSS requirements
- **Incident investigation**: Full trail of who did what
- **Security monitoring**: Detect suspicious patterns (failed auths, bulk access)
- **Structured format**: Easy to parse, query, and analyze
- **Separation**: Audit logs distinct from operational logs

### Negative

- **Performance overhead**: Every auditable operation has logging cost
- **Storage cost**: Audit logs can be voluminous (especially for read-heavy workloads)
- **Complexity**: Must ensure audit logging doesn't fail silently
- **Privacy**: Audit logs themselves contain sensitive data

### Audit Log Destinations

| Destination | Use Case |
|-------------|----------|
| Local file | Development, small deployments |
| Syslog | Integration with existing SIEM |
| S3/GCS | Long-term archival |
| Elasticsearch | Real-time search and analysis |
| Kafka | Streaming to multiple consumers |

### Retention Policies

| Regulation | Minimum Retention |
|------------|-------------------|
| SOC 2 | 1 year |
| HIPAA | 6 years |
| GDPR | Duration of processing + 3 years |
| PCI DSS | 1 year (3 months immediately available) |

### Security Considerations

- Audit logs should be write-only (no delete/modify)
- Consider cryptographic chaining for tamper evidence
- Access to audit logs should itself be audited
- Encrypt audit logs at rest and in transit

### Implications

- All data access paths must call audit logging
- Audit sink failures should not block operations (async, buffered)
- Log rotation must preserve older logs per retention policy
- Monitoring should alert on audit logging failures

## References

- `src/audit.rs` - Audit logging implementation
- SOC 2 Trust Services Criteria
- HIPAA Security Rule (45 CFR 164.312)
- GDPR Article 30 (Records of processing activities)
- PCI DSS Requirement 10 (Track and monitor access)
