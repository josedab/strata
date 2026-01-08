//! Time-travel filesystem for Strata.
//!
//! Allows accessing any file at any point in history, like Git for your filesystem.
//! Builds on the snapshot system to provide seamless historical access.
//!
//! # Usage Examples
//!
//! ```text
//! // Access file at specific time
//! strata read /path/file@2024-01-15T10:30:00
//!
//! // Access file at relative time
//! strata read /path/file@-1h      // 1 hour ago
//! strata read /path/file@-7d      // 7 days ago
//!
//! // List file history
//! strata history /path/file
//!
//! // Diff between versions
//! strata diff /path/file@-1d /path/file
//! ```

use crate::error::{Result, StrataError};
use crate::snapshot::InodeSnapshot;
use crate::types::InodeId;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Snapshot identifier (local definition for time-travel module).
pub type SnapshotId = u64;

/// Time specification for accessing historical data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeSpec {
    /// Absolute timestamp.
    Absolute(DateTime<Utc>),
    /// Relative to now (negative duration = past).
    Relative(i64), // seconds
    /// Specific version number.
    Version(u64),
    /// Latest version (current).
    Latest,
    /// Named tag.
    Tag(String),
}

impl TimeSpec {
    /// Parse a time specification string.
    /// Formats:
    /// - ISO 8601: "2024-01-15T10:30:00Z"
    /// - Relative: "-1h", "-7d", "-30m", "-2w"
    /// - Version: "v123" or "#123"
    /// - Tag: "@release-1.0"
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();

        if s.is_empty() || s == "latest" || s == "now" {
            return Ok(Self::Latest);
        }

        // Relative time
        if s.starts_with('-') || s.starts_with('+') {
            return Self::parse_relative(s);
        }

        // Version number
        if s.starts_with('v') || s.starts_with('#') {
            let num: u64 = s[1..]
                .parse()
                .map_err(|_| StrataError::InvalidData("Invalid version number".into()))?;
            return Ok(Self::Version(num));
        }

        // Tag
        if s.starts_with('@') {
            return Ok(Self::Tag(s[1..].to_string()));
        }

        // Try ISO 8601
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(Self::Absolute(dt.with_timezone(&Utc)));
        }

        // Try common date formats
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return Ok(Self::Absolute(dt.and_utc()));
        }

        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return Ok(Self::Absolute(dt.and_utc()));
        }

        // If nothing else matches and it looks like a tag name (alphanumeric with dashes/underscores/dots),
        // treat it as a tag. This handles the case where TimePath::parse has already stripped the @.
        if s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.') {
            return Ok(Self::Tag(s.to_string()));
        }

        Err(StrataError::InvalidData(format!(
            "Cannot parse time specification: {}",
            s
        )))
    }

    fn parse_relative(s: &str) -> Result<Self> {
        let negative = s.starts_with('-');
        let s = s.trim_start_matches(['-', '+']);

        let (num_str, unit) = if s.ends_with(|c: char| c.is_alphabetic()) {
            let idx = s.len() - 1;
            (&s[..idx], &s[idx..])
        } else {
            return Err(StrataError::InvalidData(
                "Relative time must have unit (s, m, h, d, w)".into(),
            ));
        };

        let num: i64 = num_str
            .parse()
            .map_err(|_| StrataError::InvalidData("Invalid number in relative time".into()))?;

        let seconds = match unit {
            "s" => num,
            "m" => num * 60,
            "h" => num * 3600,
            "d" => num * 86400,
            "w" => num * 604800,
            _ => {
                return Err(StrataError::InvalidData(format!(
                    "Unknown time unit: {}",
                    unit
                )))
            }
        };

        Ok(Self::Relative(if negative { -seconds } else { seconds }))
    }

    /// Resolve to absolute timestamp.
    pub fn resolve(&self) -> DateTime<Utc> {
        match self {
            Self::Absolute(dt) => *dt,
            Self::Relative(secs) => Utc::now() + Duration::seconds(*secs),
            Self::Latest => Utc::now(),
            Self::Version(_) | Self::Tag(_) => Utc::now(), // Need context to resolve
        }
    }
}

impl std::fmt::Display for TimeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absolute(dt) => write!(f, "{}", dt.to_rfc3339()),
            Self::Relative(secs) => {
                if *secs < 0 {
                    write!(f, "{}s ago", -secs)
                } else {
                    write!(f, "+{}s", secs)
                }
            }
            Self::Version(v) => write!(f, "v{}", v),
            Self::Latest => write!(f, "latest"),
            Self::Tag(t) => write!(f, "@{}", t),
        }
    }
}

/// A time-qualified path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimePath {
    /// The file path.
    pub path: String,
    /// Time specification.
    pub time: TimeSpec,
}

impl TimePath {
    /// Find the rightmost unescaped @ symbol (escaped @@ pairs are skipped).
    fn find_time_separator(s: &str) -> Option<usize> {
        let bytes = s.as_bytes();
        let mut i = bytes.len();
        while i > 0 {
            i -= 1;
            if bytes[i] == b'@' {
                // Check if this @ is part of an @@ escape sequence
                if i > 0 && bytes[i - 1] == b'@' {
                    // Skip both @ symbols (the pair)
                    i -= 1;
                    continue;
                }
                return Some(i);
            }
        }
        None
    }

    /// Parse a time-qualified path.
    /// Format: "/path/to/file@timespec"
    /// Use @@ to escape a literal @ in the path.
    pub fn parse(s: &str) -> Result<Self> {
        if let Some(idx) = Self::find_time_separator(s) {
            let path = &s[..idx];
            let time_str = &s[idx + 1..];

            // Unescape @@ to @ in the path portion
            let path = path.replace("@@", "@");

            Ok(Self {
                path,
                time: TimeSpec::parse(time_str)?,
            })
        } else {
            // No time separator found - unescape @@ in path and use Latest
            Ok(Self {
                path: s.replace("@@", "@"),
                time: TimeSpec::Latest,
            })
        }
    }

    /// Check if this is a historical (non-current) access.
    pub fn is_historical(&self) -> bool {
        !matches!(self.time, TimeSpec::Latest)
    }
}

impl std::fmt::Display for TimePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if matches!(self.time, TimeSpec::Latest) {
            write!(f, "{}", self.path)
        } else {
            write!(f, "{}@{}", self.path, self.time)
        }
    }
}

/// A version of a file at a specific point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileVersion {
    /// Version number (monotonically increasing).
    pub version: u64,
    /// Timestamp of this version.
    pub timestamp: DateTime<Utc>,
    /// Inode snapshot at this version.
    pub inode: InodeSnapshot,
    /// Size at this version.
    pub size: u64,
    /// Change type.
    pub change_type: ChangeType,
    /// Associated snapshot ID (if from a snapshot).
    pub snapshot_id: Option<SnapshotId>,
    /// Optional commit message/description.
    pub message: Option<String>,
}

/// Type of change that created this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    /// File was created.
    Created,
    /// File content was modified.
    Modified,
    /// File metadata was changed.
    MetadataChanged,
    /// File was renamed.
    Renamed,
    /// File was deleted (tombstone).
    Deleted,
    /// Snapshot was taken.
    Snapshot,
}

impl std::fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Modified => write!(f, "modified"),
            Self::MetadataChanged => write!(f, "metadata"),
            Self::Renamed => write!(f, "renamed"),
            Self::Deleted => write!(f, "deleted"),
            Self::Snapshot => write!(f, "snapshot"),
        }
    }
}

/// History of a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHistory {
    /// Path of the file.
    pub path: String,
    /// Current inode ID.
    pub inode_id: InodeId,
    /// All versions, ordered by time (newest first).
    pub versions: Vec<FileVersion>,
    /// Named tags.
    pub tags: HashMap<String, u64>, // tag -> version
}

impl FileHistory {
    /// Get version at specific time.
    pub fn at_time(&self, time: DateTime<Utc>) -> Option<&FileVersion> {
        // Find latest version that's not after the requested time
        self.versions
            .iter()
            .find(|v| v.timestamp <= time && v.change_type != ChangeType::Deleted)
    }

    /// Get specific version number.
    pub fn at_version(&self, version: u64) -> Option<&FileVersion> {
        self.versions.iter().find(|v| v.version == version)
    }

    /// Get version by tag.
    pub fn at_tag(&self, tag: &str) -> Option<&FileVersion> {
        self.tags
            .get(tag)
            .and_then(|&version| self.at_version(version))
    }

    /// Get the latest version.
    pub fn latest(&self) -> Option<&FileVersion> {
        self.versions.first()
    }

    /// Check if file existed at time.
    pub fn existed_at(&self, time: DateTime<Utc>) -> bool {
        self.at_time(time).is_some()
    }
}

/// Configuration for time-travel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelConfig {
    /// Whether time-travel is enabled.
    pub enabled: bool,
    /// Maximum history retention period.
    pub retention_days: u32,
    /// Maximum versions to keep per file.
    pub max_versions_per_file: usize,
    /// Whether to track all modifications or just snapshots.
    pub track_all_modifications: bool,
    /// Minimum interval between tracked versions (seconds).
    pub min_version_interval: u64,
}

impl Default for TimeTravelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 90,
            max_versions_per_file: 1000,
            track_all_modifications: true,
            min_version_interval: 1,
        }
    }
}

impl TimeTravelConfig {
    /// Configuration for compliance (long retention).
    pub fn compliance() -> Self {
        Self {
            enabled: true,
            retention_days: 2555, // 7 years
            max_versions_per_file: 10000,
            track_all_modifications: true,
            min_version_interval: 0,
        }
    }

    /// Configuration for development (shorter retention).
    pub fn development() -> Self {
        Self {
            enabled: true,
            retention_days: 30,
            max_versions_per_file: 100,
            track_all_modifications: true,
            min_version_interval: 60, // 1 minute
        }
    }
}

/// Storage trait for version history.
#[async_trait::async_trait]
pub trait VersionStore: Send + Sync {
    /// Get file history.
    async fn get_history(&self, inode_id: InodeId) -> Result<Option<FileHistory>>;

    /// Save file history.
    async fn save_history(&self, inode_id: InodeId, history: &FileHistory) -> Result<()>;

    /// Add a new version.
    async fn add_version(&self, inode_id: InodeId, version: FileVersion) -> Result<()>;

    /// Get version at time.
    async fn get_version_at(&self, inode_id: InodeId, time: DateTime<Utc>)
        -> Result<Option<FileVersion>>;

    /// Prune old versions.
    async fn prune(&self, before: DateTime<Utc>) -> Result<u64>;

    /// List all tracked inodes.
    async fn list_tracked(&self) -> Result<Vec<InodeId>>;
}

/// In-memory version store.
pub struct MemoryVersionStore {
    histories: RwLock<HashMap<InodeId, FileHistory>>,
}

impl MemoryVersionStore {
    pub fn new() -> Self {
        Self {
            histories: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryVersionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl VersionStore for MemoryVersionStore {
    async fn get_history(&self, inode_id: InodeId) -> Result<Option<FileHistory>> {
        Ok(self.histories.read().await.get(&inode_id).cloned())
    }

    async fn save_history(&self, inode_id: InodeId, history: &FileHistory) -> Result<()> {
        self.histories.write().await.insert(inode_id, history.clone());
        Ok(())
    }

    async fn add_version(&self, inode_id: InodeId, version: FileVersion) -> Result<()> {
        let mut histories = self.histories.write().await;
        let history = histories.entry(inode_id).or_insert_with(|| FileHistory {
            path: String::new(),
            inode_id,
            versions: Vec::new(),
            tags: HashMap::new(),
        });

        // Insert in sorted order (newest first)
        let pos = history
            .versions
            .iter()
            .position(|v| v.timestamp < version.timestamp)
            .unwrap_or(history.versions.len());
        history.versions.insert(pos, version);

        Ok(())
    }

    async fn get_version_at(
        &self,
        inode_id: InodeId,
        time: DateTime<Utc>,
    ) -> Result<Option<FileVersion>> {
        let histories = self.histories.read().await;
        Ok(histories
            .get(&inode_id)
            .and_then(|h| h.at_time(time).cloned()))
    }

    async fn prune(&self, before: DateTime<Utc>) -> Result<u64> {
        let mut histories = self.histories.write().await;
        let mut pruned = 0u64;

        for history in histories.values_mut() {
            let original_len = history.versions.len();
            history.versions.retain(|v| v.timestamp >= before);
            pruned += (original_len - history.versions.len()) as u64;
        }

        Ok(pruned)
    }

    async fn list_tracked(&self) -> Result<Vec<InodeId>> {
        Ok(self.histories.read().await.keys().cloned().collect())
    }
}

/// The time-travel engine.
pub struct TimeTravelEngine<S: VersionStore> {
    config: TimeTravelConfig,
    store: Arc<S>,
    /// Path to inode mapping cache.
    path_cache: RwLock<HashMap<String, InodeId>>,
    /// Version counter per inode.
    version_counters: RwLock<HashMap<InodeId, u64>>,
}

impl<S: VersionStore> TimeTravelEngine<S> {
    /// Create a new time-travel engine.
    pub fn new(config: TimeTravelConfig, store: Arc<S>) -> Self {
        Self {
            config,
            store,
            path_cache: RwLock::new(HashMap::new()),
            version_counters: RwLock::new(HashMap::new()),
        }
    }

    /// Record a new version of a file.
    pub async fn record_version(
        &self,
        path: &str,
        inode_id: InodeId,
        inode_snapshot: InodeSnapshot,
        change_type: ChangeType,
        message: Option<String>,
    ) -> Result<u64> {
        if !self.config.enabled {
            return Ok(0);
        }

        // Get next version number
        let version = {
            let mut counters = self.version_counters.write().await;
            let counter = counters.entry(inode_id).or_insert(0);
            *counter += 1;
            *counter
        };

        // Check minimum interval
        if self.config.min_version_interval > 0 {
            if let Some(history) = self.store.get_history(inode_id).await? {
                if let Some(latest) = history.latest() {
                    let elapsed = (Utc::now() - latest.timestamp).num_seconds() as u64;
                    if elapsed < self.config.min_version_interval {
                        debug!(
                            path,
                            elapsed,
                            min = self.config.min_version_interval,
                            "Skipping version, too soon"
                        );
                        return Ok(version - 1);
                    }
                }
            }
        }

        let file_version = FileVersion {
            version,
            timestamp: Utc::now(),
            inode: inode_snapshot.clone(),
            size: inode_snapshot.size,
            change_type,
            snapshot_id: None,
            message,
        };

        self.store.add_version(inode_id, file_version).await?;

        // Update path cache
        self.path_cache
            .write()
            .await
            .insert(path.to_string(), inode_id);

        // Enforce max versions
        self.enforce_max_versions(inode_id).await?;

        info!(path, version, change = %change_type, "Recorded file version");

        Ok(version)
    }

    /// Get file at a specific time.
    pub async fn get_at(&self, time_path: &TimePath) -> Result<Option<FileVersion>> {
        let inode_id = match self.resolve_inode(&time_path.path).await? {
            Some(id) => id,
            None => return Ok(None),
        };

        match &time_path.time {
            TimeSpec::Absolute(dt) => self.store.get_version_at(inode_id, *dt).await,
            TimeSpec::Relative(secs) => {
                let time = Utc::now() + Duration::seconds(*secs);
                self.store.get_version_at(inode_id, time).await
            }
            TimeSpec::Version(v) => {
                let history = self.store.get_history(inode_id).await?;
                Ok(history.and_then(|h| h.at_version(*v).cloned()))
            }
            TimeSpec::Tag(tag) => {
                let history = self.store.get_history(inode_id).await?;
                Ok(history.and_then(|h| h.at_tag(tag).cloned()))
            }
            TimeSpec::Latest => {
                let history = self.store.get_history(inode_id).await?;
                Ok(history.and_then(|h| h.latest().cloned()))
            }
        }
    }

    /// Get complete file history.
    pub async fn get_history(&self, path: &str) -> Result<Option<FileHistory>> {
        let inode_id = match self.resolve_inode(path).await? {
            Some(id) => id,
            None => return Ok(None),
        };

        self.store.get_history(inode_id).await
    }

    /// Tag a specific version.
    pub async fn tag_version(
        &self,
        path: &str,
        version: u64,
        tag: &str,
    ) -> Result<()> {
        let inode_id = self
            .resolve_inode(path)
            .await?
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;

        let mut history = self
            .store
            .get_history(inode_id)
            .await?
            .ok_or_else(|| StrataError::NotFound(format!("No history for {}", path)))?;

        // Verify version exists
        if history.at_version(version).is_none() {
            return Err(StrataError::NotFound(format!("Version {} not found", version)));
        }

        history.tags.insert(tag.to_string(), version);
        self.store.save_history(inode_id, &history).await?;

        info!(path, version, tag, "Tagged version");
        Ok(())
    }

    /// List versions between two times.
    pub async fn list_versions(
        &self,
        path: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<FileVersion>> {
        let history = self
            .get_history(path)
            .await?
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;

        let from = from.unwrap_or(DateTime::UNIX_EPOCH.into());
        let to = to.unwrap_or_else(Utc::now);

        Ok(history
            .versions
            .into_iter()
            .filter(|v| v.timestamp >= from && v.timestamp <= to)
            .collect())
    }

    /// Diff between two versions.
    pub async fn diff(
        &self,
        path: &str,
        from: &TimeSpec,
        to: &TimeSpec,
    ) -> Result<VersionDiff> {
        let from_path = TimePath {
            path: path.to_string(),
            time: from.clone(),
        };
        let to_path = TimePath {
            path: path.to_string(),
            time: to.clone(),
        };

        let from_version = self.get_at(&from_path).await?;
        let to_version = self.get_at(&to_path).await?;

        Ok(VersionDiff {
            path: path.to_string(),
            from_version,
            to_version,
            changes: self.compute_changes(&from_path, &to_path).await?,
        })
    }

    /// Prune old versions based on retention policy.
    pub async fn prune(&self) -> Result<PruneResult> {
        let cutoff = Utc::now() - Duration::days(self.config.retention_days as i64);
        let versions_pruned = self.store.prune(cutoff).await?;

        info!(versions_pruned, retention_days = self.config.retention_days, "Pruned old versions");

        Ok(PruneResult {
            versions_pruned,
            cutoff_time: cutoff,
        })
    }

    async fn resolve_inode(&self, path: &str) -> Result<Option<InodeId>> {
        // Check cache first
        if let Some(&inode_id) = self.path_cache.read().await.get(path) {
            return Ok(Some(inode_id));
        }

        // Would normally resolve via metadata service
        // For now, return None (not found in cache)
        Ok(None)
    }

    async fn enforce_max_versions(&self, inode_id: InodeId) -> Result<()> {
        if let Some(mut history) = self.store.get_history(inode_id).await? {
            if history.versions.len() > self.config.max_versions_per_file {
                // Keep newest versions
                history.versions.truncate(self.config.max_versions_per_file);
                self.store.save_history(inode_id, &history).await?;
            }
        }
        Ok(())
    }

    async fn compute_changes(&self, _from: &TimePath, _to: &TimePath) -> Result<Vec<Change>> {
        // Simplified - would compare chunk lists in real implementation
        Ok(vec![])
    }

    /// Register a path-to-inode mapping.
    pub async fn register_path(&self, path: &str, inode_id: InodeId) {
        self.path_cache.write().await.insert(path.to_string(), inode_id);
    }
}

/// Diff between two versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionDiff {
    /// Path of the file.
    pub path: String,
    /// From version (None if didn't exist).
    pub from_version: Option<FileVersion>,
    /// To version (None if deleted).
    pub to_version: Option<FileVersion>,
    /// List of changes.
    pub changes: Vec<Change>,
}

/// A single change between versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    /// Byte offset of change.
    pub offset: u64,
    /// Length of old data.
    pub old_len: u64,
    /// Length of new data.
    pub new_len: u64,
}

/// Result of pruning operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResult {
    /// Number of versions pruned.
    pub versions_pruned: u64,
    /// Cutoff time used.
    pub cutoff_time: DateTime<Utc>,
}

/// Time-travel aware file handle.
pub struct TimeTravelHandle {
    /// Path with time specification.
    pub time_path: TimePath,
    /// Resolved version.
    pub version: FileVersion,
    /// Current read position.
    pub position: u64,
}

impl TimeTravelHandle {
    /// Create a new handle.
    pub fn new(time_path: TimePath, version: FileVersion) -> Self {
        Self {
            time_path,
            version,
            position: 0,
        }
    }

    /// Get file size.
    pub fn size(&self) -> u64 {
        self.version.size
    }

    /// Is this a historical (read-only) handle?
    pub fn is_read_only(&self) -> bool {
        self.time_path.is_historical()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timespec_parse_absolute() {
        let spec = TimeSpec::parse("2024-01-15T10:30:00Z").unwrap();
        assert!(matches!(spec, TimeSpec::Absolute(_)));
    }

    #[test]
    fn test_timespec_parse_relative() {
        let spec = TimeSpec::parse("-1h").unwrap();
        assert!(matches!(spec, TimeSpec::Relative(secs) if secs == -3600));

        let spec = TimeSpec::parse("-7d").unwrap();
        assert!(matches!(spec, TimeSpec::Relative(secs) if secs == -7 * 86400));

        let spec = TimeSpec::parse("-30m").unwrap();
        assert!(matches!(spec, TimeSpec::Relative(secs) if secs == -30 * 60));

        let spec = TimeSpec::parse("-2w").unwrap();
        assert!(matches!(spec, TimeSpec::Relative(secs) if secs == -2 * 604800));
    }

    #[test]
    fn test_timespec_parse_version() {
        let spec = TimeSpec::parse("v123").unwrap();
        assert!(matches!(spec, TimeSpec::Version(123)));

        let spec = TimeSpec::parse("#456").unwrap();
        assert!(matches!(spec, TimeSpec::Version(456)));
    }

    #[test]
    fn test_timespec_parse_tag() {
        let spec = TimeSpec::parse("@release-1.0").unwrap();
        assert!(matches!(spec, TimeSpec::Tag(t) if t == "release-1.0"));
    }

    #[test]
    fn test_timespec_parse_latest() {
        let spec = TimeSpec::parse("latest").unwrap();
        assert!(matches!(spec, TimeSpec::Latest));

        let spec = TimeSpec::parse("").unwrap();
        assert!(matches!(spec, TimeSpec::Latest));
    }

    #[test]
    fn test_timepath_parse() {
        let path = TimePath::parse("/path/to/file@-1h").unwrap();
        assert_eq!(path.path, "/path/to/file");
        assert!(matches!(path.time, TimeSpec::Relative(secs) if secs == -3600));

        let path = TimePath::parse("/path/to/file").unwrap();
        assert_eq!(path.path, "/path/to/file");
        assert!(matches!(path.time, TimeSpec::Latest));

        let path = TimePath::parse("/path/to/file@v42").unwrap();
        assert_eq!(path.path, "/path/to/file");
        assert!(matches!(path.time, TimeSpec::Version(42)));
    }

    #[test]
    fn test_timepath_escaped_at() {
        let path = TimePath::parse("/path/to/user@@example.com").unwrap();
        assert_eq!(path.path, "/path/to/user@example.com");
        assert!(matches!(path.time, TimeSpec::Latest));
    }

    #[test]
    fn test_config_presets() {
        let compliance = TimeTravelConfig::compliance();
        assert_eq!(compliance.retention_days, 2555);

        let dev = TimeTravelConfig::development();
        assert_eq!(dev.retention_days, 30);
    }

    #[tokio::test]
    async fn test_memory_version_store() {
        let store = MemoryVersionStore::new();
        let inode_id = 123;

        // Add version
        let version = FileVersion {
            version: 1,
            timestamp: Utc::now(),
            inode: InodeSnapshot {
                inode_id,
                parent_id: 0,
                name: "test.txt".to_string(),
                file_type: crate::snapshot::FileTypeSnapshot::File,
                size: 100,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                atime: Utc::now(),
                mtime: Utc::now(),
                ctime: Utc::now(),
                chunks: vec![],
                xattrs: std::collections::HashMap::new(),
                children: vec![],
                symlink_target: None,
            },
            size: 100,
            change_type: ChangeType::Created,
            snapshot_id: None,
            message: None,
        };

        store.add_version(inode_id, version.clone()).await.unwrap();

        // Get history
        let history = store.get_history(inode_id).await.unwrap().unwrap();
        assert_eq!(history.versions.len(), 1);
        assert_eq!(history.versions[0].version, 1);
    }

    #[tokio::test]
    async fn test_time_travel_engine() {
        let store = Arc::new(MemoryVersionStore::new());
        let config = TimeTravelConfig::default();
        let engine = TimeTravelEngine::new(config, store);

        let inode_id = 123;
        let path = "/test/file.txt";

        // Register path
        engine.register_path(path, inode_id).await;

        // Record versions
        let snapshot = InodeSnapshot {
            inode_id,
            parent_id: 0,
            name: "file.txt".to_string(),
            file_type: crate::snapshot::FileTypeSnapshot::File,
            size: 100,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            atime: Utc::now(),
            mtime: Utc::now(),
            ctime: Utc::now(),
            chunks: vec![],
            xattrs: std::collections::HashMap::new(),
            children: vec![],
            symlink_target: None,
        };

        let v1 = engine
            .record_version(path, inode_id, snapshot.clone(), ChangeType::Created, None)
            .await
            .unwrap();
        assert_eq!(v1, 1);

        // Get history
        let history = engine.get_history(path).await.unwrap().unwrap();
        assert_eq!(history.versions.len(), 1);
    }

    #[tokio::test]
    async fn test_time_travel_tagging() {
        let store = Arc::new(MemoryVersionStore::new());
        let config = TimeTravelConfig::default();
        let engine = TimeTravelEngine::new(config, store);

        let inode_id = 123;
        let path = "/test/file.txt";

        engine.register_path(path, inode_id).await;

        let snapshot = InodeSnapshot {
            inode_id,
            parent_id: 0,
            name: "file.txt".to_string(),
            file_type: crate::snapshot::FileTypeSnapshot::File,
            size: 100,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            atime: Utc::now(),
            mtime: Utc::now(),
            ctime: Utc::now(),
            chunks: vec![],
            xattrs: std::collections::HashMap::new(),
            children: vec![],
            symlink_target: None,
        };

        engine
            .record_version(path, inode_id, snapshot, ChangeType::Created, None)
            .await
            .unwrap();

        // Tag the version
        engine.tag_version(path, 1, "release-1.0").await.unwrap();

        // Get by tag
        let time_path = TimePath::parse(&format!("{}@release-1.0", path)).unwrap();
        let version = engine.get_at(&time_path).await.unwrap().unwrap();
        assert_eq!(version.version, 1);
    }

    #[test]
    fn test_file_history() {
        let mut history = FileHistory {
            path: "/test/file".to_string(),
            inode_id: 123,
            versions: vec![],
            tags: HashMap::new(),
        };

        let now = Utc::now();
        let earlier = now - Duration::hours(1);

        history.versions.push(FileVersion {
            version: 2,
            timestamp: now,
            inode: InodeSnapshot {
                inode_id: 123,
                parent_id: 0,
                name: "file".to_string(),
                file_type: crate::snapshot::FileTypeSnapshot::File,
                size: 200,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                atime: now,
                mtime: now,
                ctime: now,
                chunks: vec![],
                xattrs: HashMap::new(),
                children: vec![],
                symlink_target: None,
            },
            size: 200,
            change_type: ChangeType::Modified,
            snapshot_id: None,
            message: None,
        });

        history.versions.push(FileVersion {
            version: 1,
            timestamp: earlier,
            inode: InodeSnapshot {
                inode_id: 123,
                parent_id: 0,
                name: "file".to_string(),
                file_type: crate::snapshot::FileTypeSnapshot::File,
                size: 100,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                atime: earlier,
                mtime: earlier,
                ctime: earlier,
                chunks: vec![],
                xattrs: HashMap::new(),
                children: vec![],
                symlink_target: None,
            },
            size: 100,
            change_type: ChangeType::Created,
            snapshot_id: None,
            message: None,
        });

        // Latest should be version 2
        assert_eq!(history.latest().unwrap().version, 2);

        // At earlier time should be version 1
        let at_earlier = history.at_time(earlier + Duration::minutes(1)).unwrap();
        assert_eq!(at_earlier.version, 1);

        // At now should be version 2
        let at_now = history.at_time(now + Duration::seconds(1)).unwrap();
        assert_eq!(at_now.version, 2);
    }
}
