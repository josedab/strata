// WORM audit logging for compliance

use super::retention::RetentionMode;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// Audit event type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    // Retention events
    RetentionSet,
    RetentionExtended,
    RetentionExpired,

    // Legal hold events
    LegalHoldPlaced,
    LegalHoldReleased,
    ObjectAddedToHold,
    ObjectRemovedFromHold,

    // Access events
    AccessDenied,
    AccessGranted,
    GovernanceBypass,

    // Object events
    ObjectCreated,
    ObjectDeleted,
    ObjectVersionCreated,

    // Configuration events
    BucketWormEnabled,
    BucketWormLocked,
    PolicyCreated,
    PolicyModified,

    // System events
    ComplianceValidation,
    AuditExport,
}

/// Audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: String,
    /// Event timestamp
    pub timestamp: SystemTime,
    /// Event type
    pub event_type: AuditEventType,
    /// Bucket name
    pub bucket: Option<String>,
    /// Object key
    pub object_key: Option<String>,
    /// Object version
    pub version_id: Option<String>,
    /// User who performed action
    pub user: String,
    /// User's IP address
    pub source_ip: Option<String>,
    /// Action result (success/failure)
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Additional details
    pub details: AuditDetails,
}

/// Audit event details
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuditDetails {
    /// Retention mode
    pub retention_mode: Option<RetentionMode>,
    /// Retention until date
    pub retain_until: Option<SystemTime>,
    /// Previous retention until (for extensions)
    pub previous_until: Option<SystemTime>,
    /// Legal hold ID
    pub hold_id: Option<String>,
    /// Legal hold name
    pub hold_name: Option<String>,
    /// Access type attempted
    pub access_type: Option<String>,
    /// Reason for denial
    pub denial_reason: Option<String>,
    /// Policy ID
    pub policy_id: Option<String>,
    /// Custom metadata
    pub metadata: Option<serde_json::Value>,
}

impl AuditEvent {
    /// Creates a new audit event
    pub fn new(event_type: AuditEventType, user: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: SystemTime::now(),
            event_type,
            bucket: None,
            object_key: None,
            version_id: None,
            user: user.into(),
            source_ip: None,
            success: true,
            error: None,
            details: AuditDetails::default(),
        }
    }

    /// Sets bucket
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Sets object key
    pub fn with_object(mut self, key: impl Into<String>) -> Self {
        self.object_key = Some(key.into());
        self
    }

    /// Sets version
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version_id = Some(version.into());
        self
    }

    /// Sets source IP
    pub fn with_source_ip(mut self, ip: impl Into<String>) -> Self {
        self.source_ip = Some(ip.into());
        self
    }

    /// Marks as failed
    pub fn failed(mut self, error: impl Into<String>) -> Self {
        self.success = false;
        self.error = Some(error.into());
        self
    }

    /// Sets details
    pub fn with_details(mut self, details: AuditDetails) -> Self {
        self.details = details;
        self
    }
}

/// Audit query filters
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    pub event_types: Option<Vec<AuditEventType>>,
    pub bucket: Option<String>,
    pub object_key: Option<String>,
    pub user: Option<String>,
    pub from: Option<SystemTime>,
    pub to: Option<SystemTime>,
    pub success_only: Option<bool>,
    pub limit: Option<usize>,
}

impl AuditQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn object(mut self, key: impl Into<String>) -> Self {
        self.object_key = Some(key.into());
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn from(mut self, from: SystemTime) -> Self {
        self.from = Some(from);
        self
    }

    pub fn to(mut self, to: SystemTime) -> Self {
        self.to = Some(to);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// WORM audit log
pub struct WormAuditLog {
    /// In-memory event buffer
    events: Arc<RwLock<VecDeque<AuditEvent>>>,
    /// Maximum buffer size
    max_buffer_size: usize,
}

impl WormAuditLog {
    /// Creates a new audit log
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(10000))),
            max_buffer_size: 100000,
        }
    }

    /// Logs an audit event
    pub async fn log(&self, event: AuditEvent) {
        let mut events = self.events.write().await;

        // Trim if buffer full
        while events.len() >= self.max_buffer_size {
            events.pop_front();
        }

        events.push_back(event);
    }

    /// Logs a retention change
    pub async fn log_retention_change(
        &self,
        bucket: &str,
        object_key: &str,
        user: &str,
        mode: RetentionMode,
        until: Option<SystemTime>,
    ) {
        let event = AuditEvent::new(AuditEventType::RetentionSet, user)
            .with_bucket(bucket)
            .with_object(object_key)
            .with_details(AuditDetails {
                retention_mode: Some(mode),
                retain_until: until,
                ..Default::default()
            });

        self.log(event).await;
    }

    /// Logs a legal hold action
    pub async fn log_legal_hold(
        &self,
        event_type: AuditEventType,
        hold_id: &str,
        hold_name: &str,
        user: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
    ) {
        let mut event = AuditEvent::new(event_type, user)
            .with_details(AuditDetails {
                hold_id: Some(hold_id.to_string()),
                hold_name: Some(hold_name.to_string()),
                ..Default::default()
            });

        if let Some(b) = bucket {
            event = event.with_bucket(b);
        }
        if let Some(k) = object_key {
            event = event.with_object(k);
        }

        self.log(event).await;
    }

    /// Logs an access attempt
    pub async fn log_access(
        &self,
        bucket: &str,
        object_key: &str,
        user: &str,
        access_type: &str,
        allowed: bool,
        reason: Option<&str>,
    ) {
        let event_type = if allowed {
            AuditEventType::AccessGranted
        } else {
            AuditEventType::AccessDenied
        };

        let mut event = AuditEvent::new(event_type, user)
            .with_bucket(bucket)
            .with_object(object_key)
            .with_details(AuditDetails {
                access_type: Some(access_type.to_string()),
                denial_reason: reason.map(|s| s.to_string()),
                ..Default::default()
            });

        if !allowed {
            event.success = false;
        }

        self.log(event).await;
    }

    /// Logs a governance mode bypass
    pub async fn log_governance_bypass(
        &self,
        bucket: &str,
        object_key: &str,
        user: &str,
        action: &str,
    ) {
        let event = AuditEvent::new(AuditEventType::GovernanceBypass, user)
            .with_bucket(bucket)
            .with_object(object_key)
            .with_details(AuditDetails {
                access_type: Some(action.to_string()),
                ..Default::default()
            });

        self.log(event).await;
    }

    /// Queries audit events
    pub async fn query(&self, query: AuditQuery) -> Vec<AuditEvent> {
        let events = self.events.read().await;

        events.iter()
            .filter(|e| {
                // Filter by event types
                if let Some(ref types) = query.event_types {
                    if !types.iter().any(|t| std::mem::discriminant(t) == std::mem::discriminant(&e.event_type)) {
                        return false;
                    }
                }

                // Filter by bucket
                if let Some(ref bucket) = query.bucket {
                    if e.bucket.as_ref() != Some(bucket) {
                        return false;
                    }
                }

                // Filter by object
                if let Some(ref key) = query.object_key {
                    if e.object_key.as_ref() != Some(key) {
                        return false;
                    }
                }

                // Filter by user
                if let Some(ref user) = query.user {
                    if &e.user != user {
                        return false;
                    }
                }

                // Filter by time range
                if let Some(from) = query.from {
                    if e.timestamp < from {
                        return false;
                    }
                }
                if let Some(to) = query.to {
                    if e.timestamp > to {
                        return false;
                    }
                }

                // Filter by success
                if let Some(success_only) = query.success_only {
                    if e.success != success_only {
                        return false;
                    }
                }

                true
            })
            .take(query.limit.unwrap_or(1000))
            .cloned()
            .collect()
    }

    /// Gets events for an object
    pub async fn get_object_history(&self, bucket: &str, object_key: &str) -> Vec<AuditEvent> {
        self.query(AuditQuery::new().bucket(bucket).object(object_key)).await
    }

    /// Gets events for a user
    pub async fn get_user_activity(&self, user: &str, limit: usize) -> Vec<AuditEvent> {
        self.query(AuditQuery::new().user(user).limit(limit)).await
    }

    /// Exports events to JSON
    pub async fn export_json(&self, query: AuditQuery) -> String {
        let events = self.query(query).await;
        serde_json::to_string_pretty(&events).unwrap_or_default()
    }

    /// Gets event count
    pub async fn count(&self) -> usize {
        self.events.read().await.len()
    }

    /// Gets recent events
    pub async fn recent(&self, limit: usize) -> Vec<AuditEvent> {
        let events = self.events.read().await;
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Clears old events (for retention policy)
    pub async fn purge_before(&self, before: SystemTime) -> usize {
        let mut events = self.events.write().await;
        let before_len = events.len();

        events.retain(|e| e.timestamp >= before);

        before_len - events.len()
    }
}

impl Default for WormAuditLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Generates compliance audit report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    pub generated_at: SystemTime,
    pub period_start: SystemTime,
    pub period_end: SystemTime,
    pub total_events: u64,
    pub events_by_type: std::collections::HashMap<String, u64>,
    pub access_denied_count: u64,
    pub governance_bypass_count: u64,
    pub retention_changes: u64,
    pub legal_hold_events: u64,
    pub unique_users: u64,
    pub unique_objects: u64,
}

impl AuditReport {
    /// Generates a report from audit events
    pub async fn generate(log: &WormAuditLog, from: SystemTime, to: SystemTime) -> Self {
        let events = log.query(AuditQuery::new().from(from).to(to)).await;

        let mut events_by_type = std::collections::HashMap::new();
        let mut users = std::collections::HashSet::new();
        let mut objects = std::collections::HashSet::new();
        let mut access_denied = 0u64;
        let mut governance_bypass = 0u64;
        let mut retention_changes = 0u64;
        let mut legal_hold_events = 0u64;

        for event in &events {
            let type_name = format!("{:?}", event.event_type);
            *events_by_type.entry(type_name).or_insert(0) += 1;

            users.insert(&event.user);
            if let Some(ref key) = event.object_key {
                objects.insert(key);
            }

            match event.event_type {
                AuditEventType::AccessDenied => access_denied += 1,
                AuditEventType::GovernanceBypass => governance_bypass += 1,
                AuditEventType::RetentionSet | AuditEventType::RetentionExtended => {
                    retention_changes += 1;
                }
                AuditEventType::LegalHoldPlaced | AuditEventType::LegalHoldReleased |
                AuditEventType::ObjectAddedToHold | AuditEventType::ObjectRemovedFromHold => {
                    legal_hold_events += 1;
                }
                _ => {}
            }
        }

        Self {
            generated_at: SystemTime::now(),
            period_start: from,
            period_end: to,
            total_events: events.len() as u64,
            events_by_type,
            access_denied_count: access_denied,
            governance_bypass_count: governance_bypass,
            retention_changes,
            legal_hold_events,
            unique_users: users.len() as u64,
            unique_objects: objects.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_audit_logging() {
        let log = WormAuditLog::new();

        log.log_retention_change(
            "bucket",
            "file.txt",
            "admin",
            RetentionMode::Compliance,
            Some(SystemTime::now() + Duration::from_secs(86400)),
        ).await;

        log.log_access(
            "bucket",
            "file.txt",
            "user",
            "delete",
            false,
            Some("Under retention"),
        ).await;

        let events = log.query(AuditQuery::new().bucket("bucket")).await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_audit_query() {
        let log = WormAuditLog::new();

        log.log_access("bucket1", "file1.txt", "user1", "read", true, None).await;
        log.log_access("bucket1", "file2.txt", "user2", "read", true, None).await;
        log.log_access("bucket2", "file3.txt", "user1", "read", true, None).await;

        // Query by bucket
        let events = log.query(AuditQuery::new().bucket("bucket1")).await;
        assert_eq!(events.len(), 2);

        // Query by user
        let events = log.query(AuditQuery::new().user("user1")).await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_audit_report() {
        let log = WormAuditLog::new();
        let now = SystemTime::now();

        log.log_access("bucket", "file.txt", "user", "delete", false, Some("denied")).await;
        log.log_governance_bypass("bucket", "file.txt", "admin", "delete").await;
        log.log_retention_change("bucket", "file.txt", "admin", RetentionMode::Compliance, None).await;

        let report = AuditReport::generate(
            &log,
            now - Duration::from_secs(3600),
            now + Duration::from_secs(3600),
        ).await;

        assert_eq!(report.total_events, 3);
        assert_eq!(report.access_denied_count, 1);
        assert_eq!(report.governance_bypass_count, 1);
        assert_eq!(report.retention_changes, 1);
    }
}
