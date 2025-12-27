// Checkpoint management for consistent recovery points

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// Checkpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Directory for checkpoint storage
    pub checkpoint_dir: PathBuf,
    /// Automatic checkpoint interval
    pub auto_interval: Duration,
    /// Maximum checkpoints to keep
    pub max_checkpoints: u32,
    /// Minimum interval between checkpoints
    pub min_interval: Duration,
    /// Enable automatic checkpoints
    pub auto_enabled: bool,
    /// Compress checkpoint data
    pub compress: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_dir: PathBuf::from("/var/lib/strata/cdp/checkpoints"),
            auto_interval: Duration::from_secs(3600), // 1 hour
            max_checkpoints: 168,                     // 7 days hourly
            min_interval: Duration::from_secs(300),   // 5 minutes
            auto_enabled: true,
            compress: true,
        }
    }
}

/// A checkpoint represents a consistent point-in-time state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Checkpoint ID
    pub id: String,
    /// Checkpoint name
    pub name: String,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Journal sequence number at checkpoint
    pub sequence: u64,
    /// Checkpoint type
    pub checkpoint_type: CheckpointType,
    /// Status
    pub status: CheckpointStatus,
    /// Size in bytes
    pub size_bytes: u64,
    /// Number of objects captured
    pub object_count: u64,
    /// Bucket scope (None = all)
    pub bucket_scope: Option<String>,
    /// Metadata
    pub metadata: HashMap<String, String>,
    /// Path to checkpoint data
    pub data_path: Option<PathBuf>,
    /// Consistency token
    pub consistency_token: String,
}

/// Type of checkpoint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointType {
    /// Full checkpoint (complete state)
    Full,
    /// Incremental (delta from previous)
    Incremental,
    /// Manual user-triggered checkpoint
    Manual,
    /// Automatic scheduled checkpoint
    Automatic,
    /// Pre-maintenance checkpoint
    PreMaintenance,
}

/// Checkpoint status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    /// Checkpoint is being created
    Creating,
    /// Checkpoint is valid and complete
    Valid,
    /// Checkpoint creation failed
    Failed,
    /// Checkpoint is being deleted
    Deleting,
    /// Checkpoint is expired
    Expired,
}

impl Checkpoint {
    /// Creates a new checkpoint
    pub fn new(name: &str, sequence: u64, checkpoint_type: CheckpointType) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            created_at: SystemTime::now(),
            sequence,
            checkpoint_type,
            status: CheckpointStatus::Creating,
            size_bytes: 0,
            object_count: 0,
            bucket_scope: None,
            metadata: HashMap::new(),
            data_path: None,
            consistency_token: Self::generate_token(),
        }
    }

    /// Generates a consistency token
    fn generate_token() -> String {
        use std::time::UNIX_EPOCH;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", now)
    }

    /// Sets the bucket scope
    pub fn with_bucket(mut self, bucket: &str) -> Self {
        self.bucket_scope = Some(bucket.to_string());
        self
    }

    /// Adds metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Marks checkpoint as valid
    pub fn mark_valid(&mut self, size_bytes: u64, object_count: u64) {
        self.status = CheckpointStatus::Valid;
        self.size_bytes = size_bytes;
        self.object_count = object_count;
    }

    /// Marks checkpoint as failed
    pub fn mark_failed(&mut self) {
        self.status = CheckpointStatus::Failed;
    }

    /// Checks if checkpoint is usable for recovery
    pub fn is_valid(&self) -> bool {
        self.status == CheckpointStatus::Valid
    }
}

/// Checkpoint data containing object states
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    /// Checkpoint ID reference
    pub checkpoint_id: String,
    /// Object states
    pub objects: Vec<ObjectSnapshot>,
    /// Bucket metadata
    pub buckets: Vec<BucketSnapshot>,
}

/// Snapshot of an object's state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSnapshot {
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Version ID
    pub version_id: Option<String>,
    /// Size in bytes
    pub size: u64,
    /// ETag/checksum
    pub etag: String,
    /// Content type
    pub content_type: Option<String>,
    /// Last modified
    pub last_modified: SystemTime,
    /// Storage class
    pub storage_class: Option<String>,
    /// User metadata
    pub metadata: HashMap<String, String>,
    /// Data chunk locations
    pub chunk_ids: Vec<String>,
}

/// Snapshot of a bucket's state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketSnapshot {
    /// Bucket name
    pub name: String,
    /// Creation time
    pub created_at: SystemTime,
    /// Versioning enabled
    pub versioning: bool,
    /// Encryption configuration
    pub encryption: Option<String>,
    /// Lifecycle rules
    pub lifecycle_rules: Vec<String>,
    /// Tags
    pub tags: HashMap<String, String>,
}

/// Checkpoint manager
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,
    /// Active checkpoints
    checkpoints: Arc<RwLock<Vec<Checkpoint>>>,
    /// Last checkpoint time
    last_checkpoint: Arc<RwLock<Option<SystemTime>>>,
    /// Checkpoint in progress
    creating: Arc<RwLock<bool>>,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager
    pub async fn new(config: CheckpointConfig) -> Result<Self> {
        // Create checkpoint directory
        tokio::fs::create_dir_all(&config.checkpoint_dir).await?;

        let manager = Self {
            config: config.clone(),
            checkpoints: Arc::new(RwLock::new(Vec::new())),
            last_checkpoint: Arc::new(RwLock::new(None)),
            creating: Arc::new(RwLock::new(false)),
        };

        // Load existing checkpoints
        manager.load_checkpoints().await?;

        Ok(manager)
    }

    /// Loads existing checkpoints from disk
    async fn load_checkpoints(&self) -> Result<()> {
        let mut entries = tokio::fs::read_dir(&self.config.checkpoint_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "checkpoint").unwrap_or(false) {
                if let Ok(checkpoint) = self.load_checkpoint_metadata(&path).await {
                    self.checkpoints.write().await.push(checkpoint);
                }
            }
        }

        // Sort by creation time
        self.checkpoints
            .write()
            .await
            .sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(())
    }

    /// Loads checkpoint metadata from file
    async fn load_checkpoint_metadata(&self, path: &Path) -> Result<Checkpoint> {
        let mut file = File::open(path).await?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;
        let checkpoint: Checkpoint = serde_json::from_str(&contents)?;
        Ok(checkpoint)
    }

    /// Creates a new checkpoint
    pub async fn create_checkpoint(
        &self,
        name: &str,
        sequence: u64,
        checkpoint_type: CheckpointType,
    ) -> Result<Checkpoint> {
        // Check if checkpoint is already in progress
        {
            let mut creating = self.creating.write().await;
            if *creating {
                return Err(StrataError::Conflict(
                    "Checkpoint creation already in progress".to_string(),
                ));
            }
            *creating = true;
        }

        // Check minimum interval
        if let Some(last) = *self.last_checkpoint.read().await {
            if let Ok(elapsed) = last.elapsed() {
                if elapsed < self.config.min_interval {
                    *self.creating.write().await = false;
                    return Err(StrataError::RateLimited(format!(
                        "Checkpoint interval not reached. Wait {} seconds",
                        (self.config.min_interval - elapsed).as_secs()
                    )));
                }
            }
        }

        let mut checkpoint = Checkpoint::new(name, sequence, checkpoint_type);

        // Save checkpoint metadata
        let metadata_path = self
            .config
            .checkpoint_dir
            .join(format!("{}.checkpoint", checkpoint.id));

        checkpoint.data_path = Some(
            self.config
                .checkpoint_dir
                .join(format!("{}.data", checkpoint.id)),
        );

        // Write metadata
        let metadata_json = serde_json::to_string_pretty(&checkpoint)?;
        let mut file = File::create(&metadata_path).await?;
        file.write_all(metadata_json.as_bytes()).await?;
        file.sync_all().await?;

        // Add to list
        self.checkpoints.write().await.push(checkpoint.clone());

        // Update last checkpoint time
        *self.last_checkpoint.write().await = Some(SystemTime::now());

        // Reset creating flag
        *self.creating.write().await = false;

        // Cleanup old checkpoints
        self.cleanup_old_checkpoints().await?;

        Ok(checkpoint)
    }

    /// Saves checkpoint data
    pub async fn save_checkpoint_data(
        &self,
        checkpoint_id: &str,
        data: &CheckpointData,
    ) -> Result<()> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint = checkpoints
            .iter()
            .find(|c| c.id == checkpoint_id)
            .ok_or_else(|| StrataError::NotFound(format!("Checkpoint {}", checkpoint_id)))?;

        let data_path = checkpoint
            .data_path
            .as_ref()
            .ok_or_else(|| StrataError::Internal("Checkpoint has no data path".to_string()))?;

        let data_json = serde_json::to_vec(data)?;

        let mut file = File::create(data_path).await?;
        file.write_all(&data_json).await?;
        file.sync_all().await?;

        drop(checkpoints);

        // Update checkpoint status
        let mut checkpoints = self.checkpoints.write().await;
        if let Some(cp) = checkpoints.iter_mut().find(|c| c.id == checkpoint_id) {
            cp.mark_valid(data_json.len() as u64, data.objects.len() as u64);

            // Save updated metadata
            let metadata_path = self.config.checkpoint_dir.join(format!("{}.checkpoint", cp.id));
            let metadata_json = serde_json::to_string_pretty(cp)?;
            let mut file = File::create(&metadata_path).await?;
            file.write_all(metadata_json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Loads checkpoint data
    pub async fn load_checkpoint_data(&self, checkpoint_id: &str) -> Result<CheckpointData> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint = checkpoints
            .iter()
            .find(|c| c.id == checkpoint_id)
            .ok_or_else(|| StrataError::NotFound(format!("Checkpoint {}", checkpoint_id)))?;

        if !checkpoint.is_valid() {
            return Err(StrataError::InvalidArgument(
                "Checkpoint is not valid".to_string(),
            ));
        }

        let data_path = checkpoint
            .data_path
            .as_ref()
            .ok_or_else(|| StrataError::Internal("Checkpoint has no data path".to_string()))?;

        let mut file = File::open(data_path).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;

        let data: CheckpointData = serde_json::from_slice(&contents)?;
        Ok(data)
    }

    /// Lists all checkpoints
    pub async fn list_checkpoints(&self) -> Vec<Checkpoint> {
        self.checkpoints.read().await.clone()
    }

    /// Gets a checkpoint by ID
    pub async fn get_checkpoint(&self, id: &str) -> Option<Checkpoint> {
        self.checkpoints
            .read()
            .await
            .iter()
            .find(|c| c.id == id)
            .cloned()
    }

    /// Finds the nearest checkpoint to a timestamp
    pub async fn find_checkpoint_near(&self, target: SystemTime) -> Option<Checkpoint> {
        let checkpoints = self.checkpoints.read().await;

        checkpoints
            .iter()
            .filter(|c| c.is_valid() && c.created_at <= target)
            .max_by_key(|c| c.created_at)
            .cloned()
    }

    /// Deletes a checkpoint
    pub async fn delete_checkpoint(&self, id: &str) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;

        let idx = checkpoints
            .iter()
            .position(|c| c.id == id)
            .ok_or_else(|| StrataError::NotFound(format!("Checkpoint {}", id)))?;

        let checkpoint = checkpoints.remove(idx);

        // Delete files
        let metadata_path = self
            .config
            .checkpoint_dir
            .join(format!("{}.checkpoint", checkpoint.id));

        if let Err(e) = tokio::fs::remove_file(&metadata_path).await {
            tracing::warn!("Failed to remove checkpoint metadata: {}", e);
        }

        if let Some(data_path) = &checkpoint.data_path {
            if let Err(e) = tokio::fs::remove_file(data_path).await {
                tracing::warn!("Failed to remove checkpoint data: {}", e);
            }
        }

        Ok(())
    }

    /// Cleans up old checkpoints beyond max_checkpoints
    async fn cleanup_old_checkpoints(&self) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;

        if checkpoints.len() <= self.config.max_checkpoints as usize {
            return Ok(());
        }

        // Sort by creation time (newest first)
        checkpoints.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // Remove oldest checkpoints
        while checkpoints.len() > self.config.max_checkpoints as usize {
            if let Some(old) = checkpoints.pop() {
                // Delete files
                let metadata_path = self
                    .config
                    .checkpoint_dir
                    .join(format!("{}.checkpoint", old.id));

                let _ = tokio::fs::remove_file(&metadata_path).await;

                if let Some(data_path) = &old.data_path {
                    let _ = tokio::fs::remove_file(data_path).await;
                }
            }
        }

        Ok(())
    }

    /// Gets checkpoint statistics
    pub async fn stats(&self) -> CheckpointStats {
        let checkpoints = self.checkpoints.read().await;

        let valid_count = checkpoints.iter().filter(|c| c.is_valid()).count();
        let total_size: u64 = checkpoints.iter().map(|c| c.size_bytes).sum();
        let oldest = checkpoints.last().map(|c| c.created_at);
        let newest = checkpoints.first().map(|c| c.created_at);

        CheckpointStats {
            total_checkpoints: checkpoints.len(),
            valid_checkpoints: valid_count,
            total_size_bytes: total_size,
            oldest_checkpoint: oldest,
            newest_checkpoint: newest,
            auto_enabled: self.config.auto_enabled,
            auto_interval: self.config.auto_interval,
        }
    }

    /// Checks if automatic checkpoint should be created
    pub async fn should_auto_checkpoint(&self) -> bool {
        if !self.config.auto_enabled {
            return false;
        }

        if *self.creating.read().await {
            return false;
        }

        match *self.last_checkpoint.read().await {
            Some(last) => last.elapsed().map(|e| e >= self.config.auto_interval).unwrap_or(false),
            None => true,
        }
    }
}

/// Checkpoint statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStats {
    /// Total number of checkpoints
    pub total_checkpoints: usize,
    /// Number of valid checkpoints
    pub valid_checkpoints: usize,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Oldest checkpoint timestamp
    pub oldest_checkpoint: Option<SystemTime>,
    /// Newest checkpoint timestamp
    pub newest_checkpoint: Option<SystemTime>,
    /// Whether auto checkpoints are enabled
    pub auto_enabled: bool,
    /// Auto checkpoint interval
    pub auto_interval: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_creation() {
        let checkpoint = Checkpoint::new("Test", 100, CheckpointType::Manual);

        assert_eq!(checkpoint.name, "Test");
        assert_eq!(checkpoint.sequence, 100);
        assert_eq!(checkpoint.checkpoint_type, CheckpointType::Manual);
        assert_eq!(checkpoint.status, CheckpointStatus::Creating);
    }

    #[test]
    fn test_checkpoint_mark_valid() {
        let mut checkpoint = Checkpoint::new("Test", 100, CheckpointType::Full);

        checkpoint.mark_valid(1024 * 1024, 500);

        assert!(checkpoint.is_valid());
        assert_eq!(checkpoint.size_bytes, 1024 * 1024);
        assert_eq!(checkpoint.object_count, 500);
    }

    #[tokio::test]
    async fn test_checkpoint_manager() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            min_interval: Duration::from_secs(0),
            ..Default::default()
        };

        let manager = CheckpointManager::new(config).await.unwrap();

        let checkpoint = manager
            .create_checkpoint("Test", 1, CheckpointType::Manual)
            .await
            .unwrap();

        assert_eq!(checkpoint.name, "Test");

        let listed = manager.list_checkpoints().await;
        assert_eq!(listed.len(), 1);
    }

    #[tokio::test]
    async fn test_checkpoint_data_save_load() {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig {
            checkpoint_dir: dir.path().to_path_buf(),
            min_interval: Duration::from_secs(0),
            ..Default::default()
        };

        let manager = CheckpointManager::new(config).await.unwrap();

        let checkpoint = manager
            .create_checkpoint("Data Test", 1, CheckpointType::Full)
            .await
            .unwrap();

        let data = CheckpointData {
            checkpoint_id: checkpoint.id.clone(),
            objects: vec![ObjectSnapshot {
                bucket: "test".to_string(),
                key: "file.txt".to_string(),
                version_id: None,
                size: 1024,
                etag: "abc123".to_string(),
                content_type: Some("text/plain".to_string()),
                last_modified: SystemTime::now(),
                storage_class: None,
                metadata: HashMap::new(),
                chunk_ids: vec!["chunk1".to_string()],
            }],
            buckets: vec![],
        };

        manager
            .save_checkpoint_data(&checkpoint.id, &data)
            .await
            .unwrap();

        // Verify checkpoint is now valid
        let updated = manager.get_checkpoint(&checkpoint.id).await.unwrap();
        assert!(updated.is_valid());

        // Load data back
        let loaded = manager.load_checkpoint_data(&checkpoint.id).await.unwrap();
        assert_eq!(loaded.objects.len(), 1);
        assert_eq!(loaded.objects[0].key, "file.txt");
    }
}
