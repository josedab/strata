//! Audit logging for Strata.
//!
//! Provides comprehensive audit trail for compliance (SOC2, HIPAA, GDPR).
//! Tracks all file system operations with who, what, when, and outcome.

use crate::error::Result;
use crate::types::{InodeId, NodeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{error, info, warn};

/// Audit event category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditCategory {
    /// Authentication events.
    Authentication,
    /// Authorization/access control events.
    Authorization,
    /// File operations (create, read, write, delete).
    FileOperation,
    /// Directory operations.
    DirectoryOperation,
    /// Administrative operations.
    Administration,
    /// Configuration changes.
    Configuration,
    /// Security events.
    Security,
    /// System events.
    System,
}

/// Audit event action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    // Authentication
    Login,
    Logout,
    LoginFailed,
    TokenRefresh,
    TokenRevoke,

    // File operations
    FileCreate,
    FileRead,
    FileWrite,
    FileDelete,
    FileRename,
    FileTruncate,
    FileChmod,
    FileChown,
    FileSetAttr,

    // Directory operations
    DirCreate,
    DirDelete,
    DirList,
    DirRename,

    // Link operations
    LinkCreate,
    LinkDelete,
    SymlinkCreate,

    // Lock operations
    LockAcquire,
    LockRelease,
    LockDenied,
    LockExpired,

    // Admin operations
    NodeJoin,
    NodeLeave,
    NodeFailover,
    ClusterRebalance,
    BackupCreate,
    BackupRestore,
    SnapshotCreate,
    SnapshotDelete,
    SnapshotRestore,

    // Configuration
    ConfigChange,
    QuotaChange,
    AclChange,

    // Security
    AccessDenied,
    QuotaExceeded,
    RateLimited,
    InvalidToken,
    EncryptionKeyRotate,

    // System
    ServiceStart,
    ServiceStop,
    GarbageCollection,
    DataScrub,
}

/// Audit event outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// Operation succeeded.
    Success,
    /// Operation failed.
    Failure,
    /// Operation denied (authorization).
    Denied,
    /// Operation partially succeeded.
    Partial,
}

/// Resource type being accessed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResource {
    /// File resource.
    File { path: String, inode: Option<InodeId> },
    /// Directory resource.
    Directory { path: String, inode: Option<InodeId> },
    /// User/account resource.
    User { user_id: String },
    /// Node resource.
    Node { node_id: NodeId },
    /// Cluster resource.
    Cluster,
    /// Configuration resource.
    Config { key: String },
    /// Snapshot resource.
    Snapshot { id: String },
    /// Backup resource.
    Backup { id: String },
}

/// Principal (who performed the action).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditPrincipal {
    /// User ID or service account.
    pub user_id: String,
    /// User's groups.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub groups: Vec<String>,
    /// Source IP address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    /// Session/token ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Client application.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_app: Option<String>,
}

impl AuditPrincipal {
    /// Create a new principal.
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            groups: Vec::new(),
            source_ip: None,
            session_id: None,
            client_app: None,
        }
    }

    /// Create a system principal.
    pub fn system() -> Self {
        Self::new("system")
    }

    /// Create an anonymous principal.
    pub fn anonymous() -> Self {
        Self::new("anonymous")
    }

    /// Set source IP.
    pub fn with_source_ip(mut self, ip: impl Into<String>) -> Self {
        self.source_ip = Some(ip.into());
        self
    }

    /// Set session ID.
    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// Set groups.
    pub fn with_groups(mut self, groups: Vec<String>) -> Self {
        self.groups = groups;
        self
    }

    /// Set client application.
    pub fn with_client_app(mut self, app: impl Into<String>) -> Self {
        self.client_app = Some(app.into());
        self
    }
}

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID.
    pub id: String,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Event category.
    pub category: AuditCategory,
    /// Specific action.
    pub action: AuditAction,
    /// Who performed the action.
    pub principal: AuditPrincipal,
    /// What was accessed/modified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<AuditResource>,
    /// Operation outcome.
    pub outcome: AuditOutcome,
    /// Error message if failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Additional context.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty", default)]
    pub details: std::collections::HashMap<String, String>,
    /// Duration in microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_us: Option<u64>,
    /// Node that generated this event.
    pub node_id: NodeId,
    /// Request ID for correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event builder.
    pub fn builder(category: AuditCategory, action: AuditAction) -> AuditEventBuilder {
        AuditEventBuilder::new(category, action)
    }
}

/// Builder for audit events.
pub struct AuditEventBuilder {
    category: AuditCategory,
    action: AuditAction,
    principal: Option<AuditPrincipal>,
    resource: Option<AuditResource>,
    outcome: AuditOutcome,
    error: Option<String>,
    details: std::collections::HashMap<String, String>,
    duration_us: Option<u64>,
    request_id: Option<String>,
}

impl AuditEventBuilder {
    /// Create a new builder.
    pub fn new(category: AuditCategory, action: AuditAction) -> Self {
        Self {
            category,
            action,
            principal: None,
            resource: None,
            outcome: AuditOutcome::Success,
            error: None,
            details: std::collections::HashMap::new(),
            duration_us: None,
            request_id: None,
        }
    }

    /// Set the principal.
    pub fn principal(mut self, principal: AuditPrincipal) -> Self {
        self.principal = Some(principal);
        self
    }

    /// Set the resource.
    pub fn resource(mut self, resource: AuditResource) -> Self {
        self.resource = Some(resource);
        self
    }

    /// Set file resource.
    pub fn file(mut self, path: impl Into<String>, inode: Option<InodeId>) -> Self {
        self.resource = Some(AuditResource::File {
            path: path.into(),
            inode,
        });
        self
    }

    /// Set directory resource.
    pub fn directory(mut self, path: impl Into<String>, inode: Option<InodeId>) -> Self {
        self.resource = Some(AuditResource::Directory {
            path: path.into(),
            inode,
        });
        self
    }

    /// Set the outcome.
    pub fn outcome(mut self, outcome: AuditOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Mark as success.
    pub fn success(mut self) -> Self {
        self.outcome = AuditOutcome::Success;
        self
    }

    /// Mark as failure with error.
    pub fn failure(mut self, error: impl Into<String>) -> Self {
        self.outcome = AuditOutcome::Failure;
        self.error = Some(error.into());
        self
    }

    /// Mark as denied.
    pub fn denied(mut self, reason: impl Into<String>) -> Self {
        self.outcome = AuditOutcome::Denied;
        self.error = Some(reason.into());
        self
    }

    /// Add a detail.
    pub fn detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Set duration.
    pub fn duration_us(mut self, duration: u64) -> Self {
        self.duration_us = Some(duration);
        self
    }

    /// Set request ID.
    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }

    /// Build the event.
    pub fn build(self, node_id: NodeId) -> AuditEvent {
        AuditEvent {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            category: self.category,
            action: self.action,
            principal: self.principal.unwrap_or_else(AuditPrincipal::anonymous),
            resource: self.resource,
            outcome: self.outcome,
            error: self.error,
            details: self.details,
            duration_us: self.duration_us,
            node_id,
            request_id: self.request_id,
        }
    }
}

/// Audit log configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Whether audit logging is enabled.
    pub enabled: bool,
    /// Log file path.
    pub log_path: PathBuf,
    /// Maximum log file size in bytes before rotation.
    pub max_file_size: u64,
    /// Number of rotated files to keep.
    pub max_files: usize,
    /// Whether to log to stdout as well.
    pub log_to_stdout: bool,
    /// Categories to log (empty = all).
    pub categories: Vec<AuditCategory>,
    /// Actions to exclude from logging.
    pub exclude_actions: Vec<AuditAction>,
    /// Buffer size for async logging.
    pub buffer_size: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_path: PathBuf::from("/var/log/strata/audit.log"),
            max_file_size: 100 * 1024 * 1024, // 100MB
            max_files: 10,
            log_to_stdout: false,
            categories: Vec::new(), // All categories
            exclude_actions: vec![AuditAction::FileRead], // Exclude reads by default (too noisy)
            buffer_size: 10000,
            flush_interval_ms: 1000,
        }
    }
}

impl AuditConfig {
    /// Configuration for development.
    pub fn development() -> Self {
        Self {
            enabled: true,
            log_path: PathBuf::from("./audit.log"),
            max_file_size: 10 * 1024 * 1024, // 10MB
            max_files: 3,
            log_to_stdout: true,
            categories: Vec::new(),
            exclude_actions: Vec::new(), // Log everything
            buffer_size: 1000,
            flush_interval_ms: 100,
        }
    }

    /// Configuration for compliance (logs everything).
    pub fn compliance() -> Self {
        Self {
            enabled: true,
            log_path: PathBuf::from("/var/log/strata/audit.log"),
            max_file_size: 500 * 1024 * 1024, // 500MB
            max_files: 30,
            log_to_stdout: false,
            categories: Vec::new(),
            exclude_actions: Vec::new(), // Log everything
            buffer_size: 50000,
            flush_interval_ms: 500,
        }
    }

    /// Check if an event should be logged.
    pub fn should_log(&self, event: &AuditEvent) -> bool {
        if !self.enabled {
            return false;
        }

        // Check category filter
        if !self.categories.is_empty() && !self.categories.contains(&event.category) {
            return false;
        }

        // Check action exclusion
        if self.exclude_actions.contains(&event.action) {
            return false;
        }

        true
    }
}

/// Audit log sink trait.
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    /// Write an audit event.
    async fn write(&self, event: &AuditEvent) -> Result<()>;
    /// Flush buffered events.
    async fn flush(&self) -> Result<()>;
    /// Close the sink.
    async fn close(&self) -> Result<()>;
}

/// File-based audit sink.
pub struct FileAuditSink {
    file: RwLock<Option<File>>,
    path: PathBuf,
    max_size: u64,
    current_size: RwLock<u64>,
}

impl FileAuditSink {
    /// Create a new file sink.
    pub async fn new(path: PathBuf, max_size: u64) -> Result<Self> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let metadata = file.metadata().await?;
        let current_size = metadata.len();

        Ok(Self {
            file: RwLock::new(Some(file)),
            path,
            max_size,
            current_size: RwLock::new(current_size),
        })
    }

    async fn rotate_if_needed(&self) -> Result<()> {
        let size = *self.current_size.read().await;
        if size >= self.max_size {
            // Rotate the file
            let mut file_guard = self.file.write().await;
            if let Some(file) = file_guard.take() {
                drop(file);

                // Rename current file
                let rotated_path = self.path.with_extension(format!(
                    "{}.log",
                    Utc::now().format("%Y%m%d_%H%M%S")
                ));
                tokio::fs::rename(&self.path, &rotated_path).await?;

                // Create new file
                let new_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .await?;

                *file_guard = Some(new_file);
                *self.current_size.write().await = 0;

                info!(?rotated_path, "Rotated audit log");
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuditSink for FileAuditSink {
    async fn write(&self, event: &AuditEvent) -> Result<()> {
        self.rotate_if_needed().await?;

        let line = serde_json::to_string(event)? + "\n";
        let bytes = line.as_bytes();

        let mut file_guard = self.file.write().await;
        if let Some(file) = file_guard.as_mut() {
            file.write_all(bytes).await?;
            *self.current_size.write().await += bytes.len() as u64;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut file_guard = self.file.write().await;
        if let Some(file) = file_guard.as_mut() {
            file.flush().await?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut file_guard = self.file.write().await;
        if let Some(file) = file_guard.take() {
            file.sync_all().await?;
        }
        Ok(())
    }
}

/// In-memory audit sink for testing.
pub struct MemoryAuditSink {
    events: RwLock<VecDeque<AuditEvent>>,
    max_events: usize,
}

impl MemoryAuditSink {
    /// Create a new memory sink.
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(max_events)),
            max_events,
        }
    }

    /// Get all stored events.
    pub async fn events(&self) -> Vec<AuditEvent> {
        self.events.read().await.iter().cloned().collect()
    }

    /// Clear all events.
    pub async fn clear(&self) {
        self.events.write().await.clear();
    }
}

#[async_trait::async_trait]
impl AuditSink for MemoryAuditSink {
    async fn write(&self, event: &AuditEvent) -> Result<()> {
        let mut events = self.events.write().await;
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// The main audit logger.
pub struct AuditLogger {
    config: AuditConfig,
    node_id: NodeId,
    sender: mpsc::Sender<AuditEvent>,
    broadcast: broadcast::Sender<AuditEvent>,
}

impl AuditLogger {
    /// Create a new audit logger.
    pub async fn new(config: AuditConfig, node_id: NodeId) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        let (broadcast, _) = broadcast::channel(1000);

        let logger = Self {
            config: config.clone(),
            node_id,
            sender,
            broadcast: broadcast.clone(),
        };

        // Start background writer
        if config.enabled {
            let sink = FileAuditSink::new(config.log_path.clone(), config.max_file_size).await?;
            tokio::spawn(Self::background_writer(
                receiver,
                Arc::new(sink),
                config,
                broadcast,
            ));
        }

        Ok(logger)
    }

    /// Create an audit logger with a custom sink.
    pub fn with_sink(
        config: AuditConfig,
        node_id: NodeId,
        sink: Arc<dyn AuditSink>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        let (broadcast, _) = broadcast::channel(1000);

        if config.enabled {
            let config_clone = config.clone();
            let broadcast_clone = broadcast.clone();
            tokio::spawn(Self::background_writer(
                receiver,
                sink,
                config_clone,
                broadcast_clone,
            ));
        }

        Self {
            config,
            node_id,
            sender,
            broadcast,
        }
    }

    async fn background_writer(
        mut receiver: mpsc::Receiver<AuditEvent>,
        sink: Arc<dyn AuditSink>,
        config: AuditConfig,
        broadcast: broadcast::Sender<AuditEvent>,
    ) {
        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_millis(config.flush_interval_ms));

        loop {
            tokio::select! {
                Some(event) = receiver.recv() => {
                    if let Err(e) = sink.write(&event).await {
                        error!(?e, "Failed to write audit event");
                    }
                    // Broadcast for subscribers
                    let _ = broadcast.send(event);
                }
                _ = flush_interval.tick() => {
                    if let Err(e) = sink.flush().await {
                        error!(?e, "Failed to flush audit log");
                    }
                }
            }
        }
    }

    /// Log an audit event.
    pub async fn log(&self, event: AuditEvent) {
        if !self.config.should_log(&event) {
            return;
        }

        if self.config.log_to_stdout {
            if let Ok(json) = serde_json::to_string(&event) {
                info!(target: "strata::audit", "{}", json);
            }
        }

        if let Err(e) = self.sender.send(event).await {
            warn!(?e, "Failed to queue audit event");
        }
    }

    /// Log using a builder.
    pub async fn log_event(&self, category: AuditCategory, action: AuditAction) -> AuditEventBuilder {
        AuditEventBuilder::new(category, action)
    }

    /// Quick log helper for file operations.
    pub async fn log_file_op(
        &self,
        action: AuditAction,
        principal: AuditPrincipal,
        path: &str,
        inode: Option<InodeId>,
        outcome: AuditOutcome,
        error: Option<&str>,
    ) {
        let mut builder = AuditEvent::builder(AuditCategory::FileOperation, action)
            .principal(principal)
            .file(path, inode)
            .outcome(outcome);

        if let Some(e) = error {
            builder = builder.detail("error", e);
        }

        self.log(builder.build(self.node_id)).await;
    }

    /// Subscribe to audit events.
    pub fn subscribe(&self) -> broadcast::Receiver<AuditEvent> {
        self.broadcast.subscribe()
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Query interface for audit logs.
pub struct AuditQuery {
    /// Start time filter.
    pub start_time: Option<DateTime<Utc>>,
    /// End time filter.
    pub end_time: Option<DateTime<Utc>>,
    /// Principal filter.
    pub principal: Option<String>,
    /// Category filter.
    pub categories: Vec<AuditCategory>,
    /// Action filter.
    pub actions: Vec<AuditAction>,
    /// Outcome filter.
    pub outcomes: Vec<AuditOutcome>,
    /// Resource path pattern.
    pub path_pattern: Option<String>,
    /// Maximum results.
    pub limit: usize,
    /// Offset for pagination.
    pub offset: usize,
}

impl Default for AuditQuery {
    fn default() -> Self {
        Self {
            start_time: None,
            end_time: None,
            principal: None,
            categories: Vec::new(),
            actions: Vec::new(),
            outcomes: Vec::new(),
            path_pattern: None,
            limit: 100,
            offset: 0,
        }
    }
}

impl AuditQuery {
    /// Create a new query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by time range.
    pub fn time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Filter by principal.
    pub fn by_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Filter by category.
    pub fn category(mut self, category: AuditCategory) -> Self {
        self.categories.push(category);
        self
    }

    /// Filter by action.
    pub fn action(mut self, action: AuditAction) -> Self {
        self.actions.push(action);
        self
    }

    /// Filter by outcome.
    pub fn outcome(mut self, outcome: AuditOutcome) -> Self {
        self.outcomes.push(outcome);
        self
    }

    /// Set limit.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Check if an event matches this query.
    pub fn matches(&self, event: &AuditEvent) -> bool {
        // Time range check
        if let Some(start) = self.start_time {
            if event.timestamp < start {
                return false;
            }
        }
        if let Some(end) = self.end_time {
            if event.timestamp > end {
                return false;
            }
        }

        // Principal check
        if let Some(ref principal) = self.principal {
            if &event.principal.user_id != principal {
                return false;
            }
        }

        // Category check
        if !self.categories.is_empty() && !self.categories.contains(&event.category) {
            return false;
        }

        // Action check
        if !self.actions.is_empty() && !self.actions.contains(&event.action) {
            return false;
        }

        // Outcome check
        if !self.outcomes.is_empty() && !self.outcomes.contains(&event.outcome) {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileCreate)
            .principal(AuditPrincipal::new("user1"))
            .file("/path/to/file", Some(123))
            .success()
            .detail("size", "1024")
            .build(1);

        assert_eq!(event.category, AuditCategory::FileOperation);
        assert_eq!(event.action, AuditAction::FileCreate);
        assert_eq!(event.principal.user_id, "user1");
        assert_eq!(event.outcome, AuditOutcome::Success);
        assert_eq!(event.node_id, 1);
    }

    #[test]
    fn test_audit_principal() {
        let principal = AuditPrincipal::new("alice")
            .with_source_ip("192.168.1.1")
            .with_session("session-123")
            .with_groups(vec!["admin".to_string(), "users".to_string()]);

        assert_eq!(principal.user_id, "alice");
        assert_eq!(principal.source_ip, Some("192.168.1.1".to_string()));
        assert_eq!(principal.groups.len(), 2);
    }

    #[test]
    fn test_audit_config_should_log() {
        let config = AuditConfig {
            enabled: true,
            exclude_actions: vec![AuditAction::FileRead],
            ..Default::default()
        };

        let read_event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileRead)
            .build(1);
        let write_event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileWrite)
            .build(1);

        assert!(!config.should_log(&read_event));
        assert!(config.should_log(&write_event));
    }

    #[test]
    fn test_audit_config_disabled() {
        let config = AuditConfig {
            enabled: false,
            ..Default::default()
        };

        let event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileWrite)
            .build(1);

        assert!(!config.should_log(&event));
    }

    #[test]
    fn test_audit_query_matches() {
        let event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileCreate)
            .principal(AuditPrincipal::new("alice"))
            .success()
            .build(1);

        let query = AuditQuery::new()
            .by_principal("alice")
            .category(AuditCategory::FileOperation);

        assert!(query.matches(&event));

        let query2 = AuditQuery::new().by_principal("bob");
        assert!(!query2.matches(&event));
    }

    #[tokio::test]
    async fn test_memory_audit_sink() {
        let sink = MemoryAuditSink::new(100);

        let event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileCreate)
            .build(1);

        sink.write(&event).await.unwrap();

        let events = sink.events().await;
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_audit_sink_overflow() {
        let sink = MemoryAuditSink::new(3);

        for i in 0..5 {
            let event = AuditEvent::builder(AuditCategory::FileOperation, AuditAction::FileCreate)
                .detail("index", i.to_string())
                .build(1);
            sink.write(&event).await.unwrap();
        }

        let events = sink.events().await;
        assert_eq!(events.len(), 3);
        // Should have the last 3 events
        assert_eq!(events[0].details.get("index"), Some(&"2".to_string()));
    }

    #[test]
    fn test_audit_event_serialization() {
        let event = AuditEvent::builder(AuditCategory::Security, AuditAction::AccessDenied)
            .principal(AuditPrincipal::new("attacker").with_source_ip("10.0.0.1"))
            .file("/etc/passwd", None)
            .denied("Permission denied")
            .build(1);

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("access_denied"));
        assert!(json.contains("attacker"));

        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, AuditAction::AccessDenied);
    }

    #[test]
    fn test_config_presets() {
        let dev = AuditConfig::development();
        assert!(dev.log_to_stdout);
        assert!(dev.exclude_actions.is_empty());

        let compliance = AuditConfig::compliance();
        assert!(!compliance.log_to_stdout);
        assert!(compliance.exclude_actions.is_empty());
    }
}
