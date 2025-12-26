//! Point-in-time snapshots for Strata.
//!
//! Provides snapshot functionality for data protection and recovery.
//! Supports file, directory, and volume-level snapshots.

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, InodeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Snapshot scope - what is being snapshotted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotScope {
    /// Snapshot of an entire volume.
    Volume { volume_id: String },
    /// Snapshot of a directory tree.
    Directory { path: String, recursive: bool },
    /// Snapshot of a single file.
    File { path: String },
    /// Snapshot by inode.
    Inode { inode: InodeId },
}

impl std::fmt::Display for SnapshotScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotScope::Volume { volume_id } => write!(f, "volume:{}", volume_id),
            SnapshotScope::Directory { path, .. } => write!(f, "dir:{}", path),
            SnapshotScope::File { path } => write!(f, "file:{}", path),
            SnapshotScope::Inode { inode } => write!(f, "inode:{}", inode),
        }
    }
}

/// Snapshot status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    /// Snapshot is being created.
    Creating,
    /// Snapshot is complete and ready.
    Ready,
    /// Snapshot is being deleted.
    Deleting,
    /// Snapshot creation failed.
    Failed,
    /// Snapshot is expired but not yet deleted.
    Expired,
}

/// Inode snapshot - captures inode state at point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeSnapshot {
    /// Original inode ID.
    pub inode_id: InodeId,
    /// Parent inode ID.
    pub parent_id: InodeId,
    /// File name.
    pub name: String,
    /// File type (file, directory, symlink).
    pub file_type: FileTypeSnapshot,
    /// File size in bytes.
    pub size: u64,
    /// Owner UID.
    pub uid: u32,
    /// Owner GID.
    pub gid: u32,
    /// Permission mode.
    pub mode: u32,
    /// Access time.
    pub atime: DateTime<Utc>,
    /// Modification time.
    pub mtime: DateTime<Utc>,
    /// Change time.
    pub ctime: DateTime<Utc>,
    /// Extended attributes.
    pub xattrs: HashMap<String, Vec<u8>>,
    /// Chunk references for file data.
    pub chunks: Vec<ChunkSnapshot>,
    /// Child inodes (for directories).
    pub children: Vec<InodeId>,
    /// Symlink target (for symlinks).
    pub symlink_target: Option<String>,
}

/// File type in snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileTypeSnapshot {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

/// Chunk reference in snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSnapshot {
    /// Chunk ID.
    pub chunk_id: ChunkId,
    /// Offset within the file.
    pub offset: u64,
    /// Length of data in this chunk.
    pub length: u64,
    /// Checksum of the chunk data.
    pub checksum: u32,
}

/// A point-in-time snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Description.
    pub description: Option<String>,
    /// What was snapshotted.
    pub scope: SnapshotScope,
    /// When the snapshot was created.
    pub created_at: DateTime<Utc>,
    /// When the snapshot expires (optional).
    pub expires_at: Option<DateTime<Utc>>,
    /// Current status.
    pub status: SnapshotStatus,
    /// User who created the snapshot.
    pub created_by: String,
    /// Snapshot data - captured inodes.
    pub inodes: HashMap<InodeId, InodeSnapshot>,
    /// Root inode(s) of the snapshot.
    pub root_inodes: Vec<InodeId>,
    /// Total size of snapshotted data.
    pub total_size: u64,
    /// Number of files in snapshot.
    pub file_count: u64,
    /// Number of directories in snapshot.
    pub directory_count: u64,
    /// Tags for organization.
    pub tags: HashMap<String, String>,
    /// Parent snapshot ID (for incremental snapshots).
    pub parent_snapshot_id: Option<String>,
    /// Whether this is an incremental snapshot.
    pub is_incremental: bool,
}

impl Snapshot {
    /// Create a new snapshot builder.
    pub fn builder(name: impl Into<String>, scope: SnapshotScope) -> SnapshotBuilder {
        SnapshotBuilder::new(name, scope)
    }

    /// Check if the snapshot has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Get snapshot age.
    pub fn age(&self) -> chrono::Duration {
        Utc::now().signed_duration_since(self.created_at)
    }
}

/// Builder for creating snapshots.
pub struct SnapshotBuilder {
    name: String,
    scope: SnapshotScope,
    description: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    created_by: String,
    tags: HashMap<String, String>,
    parent_snapshot_id: Option<String>,
}

impl SnapshotBuilder {
    /// Create a new builder.
    pub fn new(name: impl Into<String>, scope: SnapshotScope) -> Self {
        Self {
            name: name.into(),
            scope,
            description: None,
            expires_at: None,
            created_by: "system".to_string(),
            tags: HashMap::new(),
            parent_snapshot_id: None,
        }
    }

    /// Set description.
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set expiration time.
    pub fn expires_at(mut self, time: DateTime<Utc>) -> Self {
        self.expires_at = Some(time);
        self
    }

    /// Set expiration in days.
    pub fn expires_in_days(mut self, days: i64) -> Self {
        self.expires_at = Some(Utc::now() + chrono::Duration::days(days));
        self
    }

    /// Set creator.
    pub fn created_by(mut self, user: impl Into<String>) -> Self {
        self.created_by = user.into();
        self
    }

    /// Add a tag.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Set parent snapshot (for incremental).
    pub fn parent(mut self, snapshot_id: impl Into<String>) -> Self {
        self.parent_snapshot_id = Some(snapshot_id.into());
        self
    }

    /// Build the snapshot (without data).
    pub fn build(self) -> Snapshot {
        Snapshot {
            id: uuid::Uuid::new_v4().to_string(),
            name: self.name,
            description: self.description,
            scope: self.scope,
            created_at: Utc::now(),
            expires_at: self.expires_at,
            status: SnapshotStatus::Creating,
            created_by: self.created_by,
            inodes: HashMap::new(),
            root_inodes: Vec::new(),
            total_size: 0,
            file_count: 0,
            directory_count: 0,
            tags: self.tags,
            is_incremental: self.parent_snapshot_id.is_some(),
            parent_snapshot_id: self.parent_snapshot_id,
        }
    }
}

/// Snapshot policy for automatic snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPolicy {
    /// Policy ID.
    pub id: String,
    /// Policy name.
    pub name: String,
    /// Whether the policy is enabled.
    pub enabled: bool,
    /// Scope for snapshots.
    pub scope: SnapshotScope,
    /// Schedule (cron expression).
    pub schedule: String,
    /// Retention count - how many to keep.
    pub retention_count: usize,
    /// Retention days - how long to keep.
    pub retention_days: Option<u64>,
    /// Name prefix for created snapshots.
    pub name_prefix: String,
    /// Tags to apply.
    pub tags: HashMap<String, String>,
    /// Last run time.
    pub last_run: Option<DateTime<Utc>>,
    /// Next scheduled run.
    pub next_run: Option<DateTime<Utc>>,
}

impl SnapshotPolicy {
    /// Create a new policy.
    pub fn new(
        name: impl Into<String>,
        scope: SnapshotScope,
        schedule: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            enabled: true,
            scope,
            schedule: schedule.into(),
            retention_count: 7,
            retention_days: Some(30),
            name_prefix: "auto-".to_string(),
            tags: HashMap::new(),
            last_run: None,
            next_run: None,
        }
    }

    /// Daily snapshot policy.
    pub fn daily(name: impl Into<String>, scope: SnapshotScope) -> Self {
        Self::new(name, scope, "0 0 * * *")
            .with_retention_count(7)
            .with_retention_days(30)
    }

    /// Hourly snapshot policy.
    pub fn hourly(name: impl Into<String>, scope: SnapshotScope) -> Self {
        Self::new(name, scope, "0 * * * *")
            .with_retention_count(24)
            .with_retention_days(7)
    }

    /// Weekly snapshot policy.
    pub fn weekly(name: impl Into<String>, scope: SnapshotScope) -> Self {
        Self::new(name, scope, "0 0 * * 0")
            .with_retention_count(4)
            .with_retention_days(90)
    }

    /// Set retention count.
    pub fn with_retention_count(mut self, count: usize) -> Self {
        self.retention_count = count;
        self
    }

    /// Set retention days.
    pub fn with_retention_days(mut self, days: u64) -> Self {
        self.retention_days = Some(days);
        self
    }

    /// Disable the policy.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

/// Restore options.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// Target path for restore (None = original location).
    pub target_path: Option<String>,
    /// Overwrite existing files.
    pub overwrite: bool,
    /// Preserve ownership.
    pub preserve_ownership: bool,
    /// Preserve permissions.
    pub preserve_permissions: bool,
    /// Preserve timestamps.
    pub preserve_timestamps: bool,
    /// Specific inodes to restore (empty = all).
    pub specific_inodes: HashSet<InodeId>,
    /// Dry run - don't actually restore.
    pub dry_run: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            target_path: None,
            overwrite: false,
            preserve_ownership: true,
            preserve_permissions: true,
            preserve_timestamps: true,
            specific_inodes: HashSet::new(),
            dry_run: false,
        }
    }
}

impl RestoreOptions {
    /// Create restore options for original location.
    pub fn in_place() -> Self {
        Self {
            overwrite: true,
            ..Default::default()
        }
    }

    /// Create restore options for alternate location.
    pub fn to_path(path: impl Into<String>) -> Self {
        Self {
            target_path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Set specific inodes to restore.
    pub fn with_inodes(mut self, inodes: HashSet<InodeId>) -> Self {
        self.specific_inodes = inodes;
        self
    }

    /// Enable dry run.
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

/// Restore result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResult {
    /// Snapshot that was restored.
    pub snapshot_id: String,
    /// Whether the restore was successful.
    pub success: bool,
    /// Number of files restored.
    pub files_restored: u64,
    /// Number of directories restored.
    pub directories_restored: u64,
    /// Total bytes restored.
    pub bytes_restored: u64,
    /// Files that failed to restore.
    pub failed_files: Vec<String>,
    /// Errors encountered.
    pub errors: Vec<String>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Was this a dry run.
    pub dry_run: bool,
}

/// Snapshot manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Whether snapshots are enabled.
    pub enabled: bool,
    /// Maximum snapshots per scope.
    pub max_snapshots_per_scope: usize,
    /// Maximum total snapshots.
    pub max_total_snapshots: usize,
    /// Default retention days.
    pub default_retention_days: u64,
    /// Enable automatic cleanup of expired snapshots.
    pub auto_cleanup: bool,
    /// Cleanup check interval in hours.
    pub cleanup_interval_hours: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_snapshots_per_scope: 100,
            max_total_snapshots: 1000,
            default_retention_days: 30,
            auto_cleanup: true,
            cleanup_interval_hours: 24,
        }
    }
}

/// Trait for snapshot storage backend.
#[async_trait::async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Save a snapshot.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load a snapshot by ID.
    async fn load(&self, snapshot_id: &str) -> Result<Option<Snapshot>>;

    /// Delete a snapshot.
    async fn delete(&self, snapshot_id: &str) -> Result<()>;

    /// List all snapshots.
    async fn list(&self) -> Result<Vec<Snapshot>>;

    /// List snapshots by scope.
    async fn list_by_scope(&self, scope: &SnapshotScope) -> Result<Vec<Snapshot>>;
}

/// In-memory snapshot store.
pub struct MemorySnapshotStore {
    snapshots: RwLock<HashMap<String, Snapshot>>,
}

impl MemorySnapshotStore {
    /// Create a new memory store.
    pub fn new() -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SnapshotStore for MemorySnapshotStore {
    async fn save(&self, snapshot: &Snapshot) -> Result<()> {
        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(snapshot.id.clone(), snapshot.clone());
        Ok(())
    }

    async fn load(&self, snapshot_id: &str) -> Result<Option<Snapshot>> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots.get(snapshot_id).cloned())
    }

    async fn delete(&self, snapshot_id: &str) -> Result<()> {
        let mut snapshots = self.snapshots.write().await;
        snapshots.remove(snapshot_id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<Snapshot>> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots.values().cloned().collect())
    }

    async fn list_by_scope(&self, scope: &SnapshotScope) -> Result<Vec<Snapshot>> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots
            .values()
            .filter(|s| &s.scope == scope)
            .cloned()
            .collect())
    }
}

/// Trait for capturing inode data.
#[async_trait::async_trait]
pub trait InodeCapture: Send + Sync {
    /// Capture an inode's state.
    async fn capture_inode(&self, inode_id: InodeId) -> Result<InodeSnapshot>;

    /// List children of a directory inode.
    async fn list_children(&self, inode_id: InodeId) -> Result<Vec<InodeId>>;

    /// Resolve path to inode.
    async fn resolve_path(&self, path: &str) -> Result<InodeId>;
}

/// The snapshot manager.
pub struct SnapshotManager {
    config: SnapshotConfig,
    store: Arc<dyn SnapshotStore>,
    capture: Arc<dyn InodeCapture>,
    policies: RwLock<HashMap<String, SnapshotPolicy>>,
}

impl SnapshotManager {
    /// Create a new snapshot manager.
    pub fn new(
        config: SnapshotConfig,
        store: Arc<dyn SnapshotStore>,
        capture: Arc<dyn InodeCapture>,
    ) -> Self {
        Self {
            config,
            store,
            capture,
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Create a snapshot.
    pub async fn create_snapshot(&self, builder: SnapshotBuilder) -> Result<Snapshot> {
        if !self.config.enabled {
            return Err(StrataError::Internal("Snapshots are disabled".to_string()));
        }

        // Check limits
        let existing = self.store.list_by_scope(&builder.scope).await?;
        if existing.len() >= self.config.max_snapshots_per_scope {
            return Err(StrataError::Internal(format!(
                "Maximum snapshots ({}) reached for scope",
                self.config.max_snapshots_per_scope
            )));
        }

        let total = self.store.list().await?;
        if total.len() >= self.config.max_total_snapshots {
            return Err(StrataError::Internal(format!(
                "Maximum total snapshots ({}) reached",
                self.config.max_total_snapshots
            )));
        }

        let mut snapshot = builder.build();

        // Capture data based on scope (clone to avoid borrow conflicts)
        let scope = snapshot.scope.clone();
        match scope {
            SnapshotScope::File { path } => {
                let inode_id = self.capture.resolve_path(&path).await?;
                let inode_snapshot = self.capture.capture_inode(inode_id).await?;

                snapshot.total_size = inode_snapshot.size;
                snapshot.file_count = 1;
                snapshot.root_inodes.push(inode_id);
                snapshot.inodes.insert(inode_id, inode_snapshot);
            }
            SnapshotScope::Inode { inode } => {
                let inode_snapshot = self.capture.capture_inode(inode).await?;

                snapshot.total_size = inode_snapshot.size;
                snapshot.file_count = 1;
                snapshot.root_inodes.push(inode);
                snapshot.inodes.insert(inode, inode_snapshot);
            }
            SnapshotScope::Directory { path, recursive } => {
                let root_inode = self.capture.resolve_path(&path).await?;
                self.capture_directory(&mut snapshot, root_inode, recursive)
                    .await?;
                snapshot.root_inodes.push(root_inode);
            }
            SnapshotScope::Volume { .. } => {
                // For volumes, start from root inode (1)
                self.capture_directory(&mut snapshot, 1, true).await?;
                snapshot.root_inodes.push(1);
            }
        }

        snapshot.status = SnapshotStatus::Ready;

        // Save snapshot
        self.store.save(&snapshot).await?;

        info!(
            snapshot_id = %snapshot.id,
            name = %snapshot.name,
            files = snapshot.file_count,
            dirs = snapshot.directory_count,
            size = snapshot.total_size,
            "Created snapshot"
        );

        Ok(snapshot)
    }

    async fn capture_directory(
        &self,
        snapshot: &mut Snapshot,
        inode_id: InodeId,
        recursive: bool,
    ) -> Result<()> {
        let inode_snapshot = self.capture.capture_inode(inode_id).await?;

        match inode_snapshot.file_type {
            FileTypeSnapshot::Directory => {
                snapshot.directory_count += 1;
            }
            FileTypeSnapshot::File => {
                snapshot.file_count += 1;
                snapshot.total_size += inode_snapshot.size;
            }
            FileTypeSnapshot::Symlink => {
                snapshot.file_count += 1;
            }
        }

        let children = inode_snapshot.children.clone();
        snapshot.inodes.insert(inode_id, inode_snapshot);

        if recursive {
            for child_id in children {
                // Avoid infinite loops
                if !snapshot.inodes.contains_key(&child_id) {
                    Box::pin(self.capture_directory(snapshot, child_id, true)).await?;
                }
            }
        }

        Ok(())
    }

    /// Get a snapshot by ID.
    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<Option<Snapshot>> {
        self.store.load(snapshot_id).await
    }

    /// List all snapshots.
    pub async fn list_snapshots(&self) -> Result<Vec<Snapshot>> {
        self.store.list().await
    }

    /// List snapshots by scope.
    pub async fn list_by_scope(&self, scope: &SnapshotScope) -> Result<Vec<Snapshot>> {
        self.store.list_by_scope(scope).await
    }

    /// Delete a snapshot.
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        // Update status to deleting
        if let Some(mut snapshot) = self.store.load(snapshot_id).await? {
            snapshot.status = SnapshotStatus::Deleting;
            self.store.save(&snapshot).await?;
        }

        self.store.delete(snapshot_id).await?;

        info!(snapshot_id = %snapshot_id, "Deleted snapshot");
        Ok(())
    }

    /// Restore from a snapshot.
    pub async fn restore(&self, snapshot_id: &str, options: RestoreOptions) -> Result<RestoreResult> {
        let snapshot = self
            .store
            .load(snapshot_id)
            .await?
            .ok_or_else(|| StrataError::NotFound(format!("Snapshot not found: {}", snapshot_id)))?;

        if snapshot.status != SnapshotStatus::Ready {
            return Err(StrataError::Internal(format!(
                "Snapshot is not ready: {:?}",
                snapshot.status
            )));
        }

        let start = std::time::Instant::now();
        let mut result = RestoreResult {
            snapshot_id: snapshot_id.to_string(),
            success: true,
            files_restored: 0,
            directories_restored: 0,
            bytes_restored: 0,
            failed_files: Vec::new(),
            errors: Vec::new(),
            duration_ms: 0,
            dry_run: options.dry_run,
        };

        // Determine which inodes to restore
        let inodes_to_restore: Vec<_> = if options.specific_inodes.is_empty() {
            snapshot.inodes.values().collect()
        } else {
            snapshot
                .inodes
                .values()
                .filter(|i| options.specific_inodes.contains(&i.inode_id))
                .collect()
        };

        for inode in inodes_to_restore {
            match inode.file_type {
                FileTypeSnapshot::Directory => {
                    result.directories_restored += 1;
                }
                FileTypeSnapshot::File => {
                    result.files_restored += 1;
                    result.bytes_restored += inode.size;
                }
                FileTypeSnapshot::Symlink => {
                    result.files_restored += 1;
                }
            }

            if !options.dry_run {
                // Actual restore would happen here
                // This is a placeholder - real implementation would:
                // 1. Create directories
                // 2. Restore file data from chunks
                // 3. Set permissions, ownership, timestamps
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        info!(
            snapshot_id = %snapshot_id,
            files = result.files_restored,
            dirs = result.directories_restored,
            bytes = result.bytes_restored,
            dry_run = result.dry_run,
            "Restore completed"
        );

        Ok(result)
    }

    /// Add a snapshot policy.
    pub async fn add_policy(&self, policy: SnapshotPolicy) -> Result<()> {
        let mut policies = self.policies.write().await;
        policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    /// Remove a snapshot policy.
    pub async fn remove_policy(&self, policy_id: &str) -> Result<()> {
        let mut policies = self.policies.write().await;
        policies.remove(policy_id);
        Ok(())
    }

    /// List all policies.
    pub async fn list_policies(&self) -> Vec<SnapshotPolicy> {
        let policies = self.policies.read().await;
        policies.values().cloned().collect()
    }

    /// Cleanup expired snapshots.
    pub async fn cleanup_expired(&self) -> Result<usize> {
        if !self.config.auto_cleanup {
            return Ok(0);
        }

        let snapshots = self.store.list().await?;
        let mut deleted = 0;

        for snapshot in snapshots {
            if snapshot.is_expired() {
                self.delete_snapshot(&snapshot.id).await?;
                deleted += 1;
            }
        }

        if deleted > 0 {
            info!(count = deleted, "Cleaned up expired snapshots");
        }

        Ok(deleted)
    }

    /// Get snapshot statistics.
    pub async fn stats(&self) -> SnapshotStats {
        let snapshots = self.store.list().await.unwrap_or_default();

        let mut stats = SnapshotStats::default();
        stats.total_snapshots = snapshots.len();

        for snapshot in &snapshots {
            stats.total_size += snapshot.total_size;
            stats.total_files += snapshot.file_count;
            stats.total_directories += snapshot.directory_count;

            match snapshot.status {
                SnapshotStatus::Ready => stats.ready_count += 1,
                SnapshotStatus::Creating => stats.creating_count += 1,
                SnapshotStatus::Failed => stats.failed_count += 1,
                SnapshotStatus::Expired => stats.expired_count += 1,
                SnapshotStatus::Deleting => {}
            }
        }

        if let Some(oldest) = snapshots.iter().min_by_key(|s| s.created_at) {
            stats.oldest_snapshot = Some(oldest.created_at);
        }

        if let Some(newest) = snapshots.iter().max_by_key(|s| s.created_at) {
            stats.newest_snapshot = Some(newest.created_at);
        }

        stats
    }
}

/// Snapshot statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStats {
    /// Total number of snapshots.
    pub total_snapshots: usize,
    /// Total size of all snapshots.
    pub total_size: u64,
    /// Total files across all snapshots.
    pub total_files: u64,
    /// Total directories across all snapshots.
    pub total_directories: u64,
    /// Snapshots in ready state.
    pub ready_count: usize,
    /// Snapshots being created.
    pub creating_count: usize,
    /// Failed snapshots.
    pub failed_count: usize,
    /// Expired snapshots.
    pub expired_count: usize,
    /// Oldest snapshot time.
    pub oldest_snapshot: Option<DateTime<Utc>>,
    /// Newest snapshot time.
    pub newest_snapshot: Option<DateTime<Utc>>,
}

/// Mock inode capture for testing.
pub struct MockInodeCapture {
    inodes: RwLock<HashMap<InodeId, InodeSnapshot>>,
    paths: RwLock<HashMap<String, InodeId>>,
}

impl MockInodeCapture {
    /// Create a new mock capture.
    pub fn new() -> Self {
        Self {
            inodes: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
        }
    }

    /// Add an inode.
    pub async fn add_inode(&self, snapshot: InodeSnapshot) {
        let mut inodes = self.inodes.write().await;
        inodes.insert(snapshot.inode_id, snapshot);
    }

    /// Add a path mapping.
    pub async fn add_path(&self, path: &str, inode_id: InodeId) {
        let mut paths = self.paths.write().await;
        paths.insert(path.to_string(), inode_id);
    }
}

impl Default for MockInodeCapture {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl InodeCapture for MockInodeCapture {
    async fn capture_inode(&self, inode_id: InodeId) -> Result<InodeSnapshot> {
        let inodes = self.inodes.read().await;
        inodes
            .get(&inode_id)
            .cloned()
            .ok_or_else(|| StrataError::InodeNotFound(inode_id))
    }

    async fn list_children(&self, inode_id: InodeId) -> Result<Vec<InodeId>> {
        let inodes = self.inodes.read().await;
        if let Some(inode) = inodes.get(&inode_id) {
            Ok(inode.children.clone())
        } else {
            Ok(Vec::new())
        }
    }

    async fn resolve_path(&self, path: &str) -> Result<InodeId> {
        let paths = self.paths.read().await;
        paths
            .get(path)
            .copied()
            .ok_or_else(|| StrataError::FileNotFound(path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_inode(id: InodeId, name: &str, file_type: FileTypeSnapshot) -> InodeSnapshot {
        InodeSnapshot {
            inode_id: id,
            parent_id: 1,
            name: name.to_string(),
            file_type,
            size: 1024,
            uid: 1000,
            gid: 1000,
            mode: 0o644,
            atime: Utc::now(),
            mtime: Utc::now(),
            ctime: Utc::now(),
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            children: Vec::new(),
            symlink_target: None,
        }
    }

    #[test]
    fn test_snapshot_builder() {
        let snapshot = Snapshot::builder("test-snapshot", SnapshotScope::Volume {
            volume_id: "vol1".to_string(),
        })
        .description("Test snapshot")
        .created_by("admin")
        .expires_in_days(7)
        .tag("env", "test")
        .build();

        assert_eq!(snapshot.name, "test-snapshot");
        assert_eq!(snapshot.status, SnapshotStatus::Creating);
        assert!(snapshot.expires_at.is_some());
    }

    #[test]
    fn test_snapshot_scope_display() {
        let scope = SnapshotScope::File {
            path: "/path/to/file".to_string(),
        };
        assert_eq!(scope.to_string(), "file:/path/to/file");
    }

    #[test]
    fn test_snapshot_policy_presets() {
        let daily = SnapshotPolicy::daily(
            "daily-backup",
            SnapshotScope::Volume {
                volume_id: "vol1".to_string(),
            },
        );
        assert_eq!(daily.retention_count, 7);

        let hourly = SnapshotPolicy::hourly(
            "hourly-backup",
            SnapshotScope::Volume {
                volume_id: "vol1".to_string(),
            },
        );
        assert_eq!(hourly.retention_count, 24);
    }

    #[test]
    fn test_restore_options() {
        let options = RestoreOptions::to_path("/restore/target").dry_run();
        assert_eq!(options.target_path, Some("/restore/target".to_string()));
        assert!(options.dry_run);
    }

    #[tokio::test]
    async fn test_memory_snapshot_store() {
        let store = MemorySnapshotStore::new();

        let snapshot = Snapshot::builder("test", SnapshotScope::File {
            path: "/test".to_string(),
        })
        .build();

        store.save(&snapshot).await.unwrap();

        let loaded = store.load(&snapshot.id).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "test");

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 1);

        store.delete(&snapshot.id).await.unwrap();
        assert!(store.load(&snapshot.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_snapshot_manager_create() {
        let store = Arc::new(MemorySnapshotStore::new());
        let capture = Arc::new(MockInodeCapture::new());

        // Set up test data
        let inode = create_test_inode(100, "test.txt", FileTypeSnapshot::File);
        capture.add_inode(inode).await;
        capture.add_path("/test.txt", 100).await;

        let manager = SnapshotManager::new(
            SnapshotConfig::default(),
            store,
            capture,
        );

        let snapshot = manager
            .create_snapshot(Snapshot::builder(
                "test-snap",
                SnapshotScope::File {
                    path: "/test.txt".to_string(),
                },
            ))
            .await
            .unwrap();

        assert_eq!(snapshot.name, "test-snap");
        assert_eq!(snapshot.status, SnapshotStatus::Ready);
        assert_eq!(snapshot.file_count, 1);
    }

    #[tokio::test]
    async fn test_snapshot_expiry() {
        let snapshot = Snapshot::builder("test", SnapshotScope::File {
            path: "/test".to_string(),
        })
        .expires_in_days(-1) // Already expired
        .build();

        assert!(snapshot.is_expired());
    }

    #[test]
    fn test_snapshot_stats() {
        let stats = SnapshotStats {
            total_snapshots: 10,
            total_size: 1024 * 1024 * 100,
            ready_count: 8,
            failed_count: 2,
            ..Default::default()
        };

        assert_eq!(stats.total_snapshots, 10);
    }
}
