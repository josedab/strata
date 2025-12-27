// Change journal for continuous data protection

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, RwLock};

/// Type of change operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    /// Create a new object
    Create,
    /// Update existing object
    Update,
    /// Delete an object
    Delete,
    /// Rename/move an object
    Rename,
    /// Metadata-only change
    Metadata,
    /// Truncate file
    Truncate,
    /// Append to file
    Append,
}

/// A single change entry in the journal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEntry {
    /// Unique sequence number
    pub sequence: u64,
    /// Timestamp of the change
    pub timestamp: SystemTime,
    /// Type of change
    pub change_type: ChangeType,
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Previous key (for renames)
    pub previous_key: Option<String>,
    /// Version ID
    pub version_id: Option<String>,
    /// Object size after change
    pub size: Option<u64>,
    /// ETag/checksum
    pub etag: Option<String>,
    /// Previous data location (for recovery)
    pub previous_chunks: Option<Vec<ChunkId>>,
    /// New data location
    pub new_chunks: Option<Vec<ChunkId>>,
    /// Offset for partial writes
    pub offset: Option<u64>,
    /// Length for partial writes
    pub length: Option<u64>,
    /// User/principal that made the change
    pub principal: Option<String>,
    /// Source IP address
    pub source_ip: Option<String>,
    /// Custom metadata
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

impl ChangeEntry {
    /// Creates a new change entry
    pub fn new(sequence: u64, change_type: ChangeType, bucket: &str, key: &str) -> Self {
        Self {
            sequence,
            timestamp: SystemTime::now(),
            change_type,
            bucket: bucket.to_string(),
            key: key.to_string(),
            previous_key: None,
            version_id: None,
            size: None,
            etag: None,
            previous_chunks: None,
            new_chunks: None,
            offset: None,
            length: None,
            principal: None,
            source_ip: None,
            metadata: None,
        }
    }

    /// Sets the version ID
    pub fn with_version(mut self, version_id: String) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Sets the object size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Sets the previous chunks (for recovery)
    pub fn with_previous_chunks(mut self, chunks: Vec<ChunkId>) -> Self {
        self.previous_chunks = Some(chunks);
        self
    }

    /// Sets the new chunks
    pub fn with_new_chunks(mut self, chunks: Vec<ChunkId>) -> Self {
        self.new_chunks = Some(chunks);
        self
    }

    /// Sets the principal
    pub fn with_principal(mut self, principal: String) -> Self {
        self.principal = Some(principal);
        self
    }

    /// Calculates the size of this entry when serialized
    pub fn estimated_size(&self) -> usize {
        // Rough estimate
        100 + self.bucket.len() + self.key.len()
            + self.previous_key.as_ref().map(|k| k.len()).unwrap_or(0)
            + self.previous_chunks.as_ref().map(|c| c.len() * 36).unwrap_or(0)
            + self.new_chunks.as_ref().map(|c| c.len() * 36).unwrap_or(0)
    }
}

/// Journal configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Journal storage directory
    pub journal_dir: PathBuf,
    /// Maximum entries per segment file
    pub max_entries_per_segment: u64,
    /// Maximum segment file size in bytes
    pub max_segment_size: u64,
    /// Sync to disk after every write
    pub sync_on_write: bool,
    /// Retention duration for journal entries
    pub retention_duration: Duration,
    /// Enable compression for journal files
    pub compress: bool,
    /// Buffer size for writes
    pub write_buffer_size: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            journal_dir: PathBuf::from("/var/lib/strata/cdp/journal"),
            max_entries_per_segment: 100_000,
            max_segment_size: 100 * 1024 * 1024, // 100 MB
            sync_on_write: true,
            retention_duration: Duration::from_secs(7 * 24 * 3600), // 7 days
            compress: true,
            write_buffer_size: 64 * 1024,
        }
    }
}

/// Journal segment file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalSegment {
    /// Segment ID
    id: u64,
    /// Starting sequence number
    start_sequence: u64,
    /// Ending sequence number
    end_sequence: u64,
    /// Number of entries
    entry_count: u64,
    /// Segment file path
    path: PathBuf,
    /// Segment size in bytes
    size: u64,
    /// Creation time
    created_at: SystemTime,
    /// Whether segment is closed
    closed: bool,
}

/// Change journal for continuous data protection
pub struct ChangeJournal {
    /// Configuration
    config: JournalConfig,
    /// Current sequence number
    sequence: Arc<RwLock<u64>>,
    /// Active segment
    active_segment: Arc<RwLock<Option<JournalSegment>>>,
    /// All segments (including closed)
    segments: Arc<RwLock<Vec<JournalSegment>>>,
    /// In-memory buffer for recent entries
    buffer: Arc<RwLock<VecDeque<ChangeEntry>>>,
    /// Write channel
    write_tx: mpsc::Sender<ChangeEntry>,
    /// Buffer size limit
    buffer_limit: usize,
}

impl ChangeJournal {
    /// Creates a new change journal
    pub async fn new(config: JournalConfig) -> Result<Self> {
        // Create journal directory
        tokio::fs::create_dir_all(&config.journal_dir).await?;

        // Load existing segments
        let segments = Self::load_segments(&config.journal_dir).await?;
        let sequence = segments.last().map(|s| s.end_sequence + 1).unwrap_or(1);

        let (write_tx, mut write_rx) = mpsc::channel::<ChangeEntry>(10_000);

        let journal = Self {
            config: config.clone(),
            sequence: Arc::new(RwLock::new(sequence)),
            active_segment: Arc::new(RwLock::new(None)),
            segments: Arc::new(RwLock::new(segments)),
            buffer: Arc::new(RwLock::new(VecDeque::with_capacity(10_000))),
            write_tx,
            buffer_limit: 10_000,
        };

        // Start background writer
        let journal_clone = journal.clone_internals();
        tokio::spawn(async move {
            while let Some(entry) = write_rx.recv().await {
                if let Err(e) = Self::write_entry_internal(&journal_clone, entry).await {
                    tracing::error!("Failed to write journal entry: {}", e);
                }
            }
        });

        Ok(journal)
    }

    fn clone_internals(&self) -> JournalInternals {
        JournalInternals {
            config: self.config.clone(),
            sequence: Arc::clone(&self.sequence),
            active_segment: Arc::clone(&self.active_segment),
            segments: Arc::clone(&self.segments),
            buffer: Arc::clone(&self.buffer),
            buffer_limit: self.buffer_limit,
        }
    }

    /// Loads existing journal segments from disk
    async fn load_segments(journal_dir: &Path) -> Result<Vec<JournalSegment>> {
        let mut segments = Vec::new();

        let mut entries = tokio::fs::read_dir(journal_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "journal").unwrap_or(false) {
                if let Ok(segment) = Self::load_segment_metadata(&path).await {
                    segments.push(segment);
                }
            }
        }

        segments.sort_by_key(|s| s.start_sequence);
        Ok(segments)
    }

    /// Loads segment metadata from file header
    async fn load_segment_metadata(path: &Path) -> Result<JournalSegment> {
        let file = File::open(path).await?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut header = [0u8; 64];
        reader.read_exact(&mut header).await?;

        // Parse header (simplified)
        let id = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let start_sequence = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let end_sequence = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let entry_count = u64::from_le_bytes(header[24..32].try_into().unwrap());

        let metadata = tokio::fs::metadata(path).await?;

        Ok(JournalSegment {
            id,
            start_sequence,
            end_sequence,
            entry_count,
            path: path.to_path_buf(),
            size: metadata.len(),
            created_at: metadata.created().unwrap_or(SystemTime::now()),
            closed: true,
        })
    }

    /// Appends a change entry to the journal
    pub async fn append(&self, mut entry: ChangeEntry) -> Result<u64> {
        // Assign sequence number
        let sequence = {
            let mut seq = self.sequence.write().await;
            let current = *seq;
            *seq += 1;
            current
        };
        entry.sequence = sequence;

        // Send to async writer
        self.write_tx
            .send(entry.clone())
            .await
            .map_err(|_| StrataError::Internal("Journal write channel closed".to_string()))?;

        // Add to in-memory buffer
        {
            let mut buffer = self.buffer.write().await;
            buffer.push_back(entry);
            if buffer.len() > self.buffer_limit {
                buffer.pop_front();
            }
        }

        Ok(sequence)
    }

    /// Internal write implementation
    async fn write_entry_internal(
        internals: &JournalInternals,
        entry: ChangeEntry,
    ) -> Result<()> {
        let mut active = internals.active_segment.write().await;

        // Create new segment if needed
        if active.is_none() {
            let segment = Self::create_segment(&internals.config, entry.sequence).await?;
            *active = Some(segment);
        }

        // Write entry to segment
        let segment = active.as_mut().unwrap();
        let entry_bytes = serde_json::to_vec(&entry)?;

        let mut file = OpenOptions::new()
            .append(true)
            .open(&segment.path)
            .await?;

        // Write length-prefixed entry
        let len = entry_bytes.len() as u32;
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&entry_bytes).await?;

        if internals.config.sync_on_write {
            file.sync_data().await?;
        }

        segment.end_sequence = entry.sequence;
        segment.entry_count += 1;
        segment.size += 4 + entry_bytes.len() as u64;

        // Rotate if segment is full
        if segment.size >= internals.config.max_segment_size
            || segment.entry_count >= internals.config.max_entries_per_segment
        {
            let closed_segment = active.take().unwrap();
            internals.segments.write().await.push(closed_segment);
        }

        Ok(())
    }

    /// Creates a new journal segment
    async fn create_segment(config: &JournalConfig, start_sequence: u64) -> Result<JournalSegment> {
        let id = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let filename = format!("{:020}_{:016x}.journal", start_sequence, id);
        let path = config.journal_dir.join(&filename);

        let mut file = File::create(&path).await?;

        // Write header
        let mut header = [0u8; 64];
        header[0..8].copy_from_slice(&id.to_le_bytes());
        header[8..16].copy_from_slice(&start_sequence.to_le_bytes());
        header[16..24].copy_from_slice(&start_sequence.to_le_bytes()); // end = start initially
        // entry_count at [24..32] starts at 0

        file.write_all(&header).await?;
        file.sync_all().await?;

        Ok(JournalSegment {
            id,
            start_sequence,
            end_sequence: start_sequence,
            entry_count: 0,
            path,
            size: 64,
            created_at: SystemTime::now(),
            closed: false,
        })
    }

    /// Gets entries in a sequence range
    pub async fn get_entries(&self, from_sequence: u64, to_sequence: u64) -> Result<Vec<ChangeEntry>> {
        let mut entries = Vec::new();

        // First check in-memory buffer
        {
            let buffer = self.buffer.read().await;
            for entry in buffer.iter() {
                if entry.sequence >= from_sequence && entry.sequence <= to_sequence {
                    entries.push(entry.clone());
                }
            }
        }

        // If we have all entries from buffer, return
        if !entries.is_empty() && entries[0].sequence == from_sequence {
            return Ok(entries);
        }

        // Read from segment files
        let segments = self.segments.read().await;
        for segment in segments.iter() {
            if segment.end_sequence < from_sequence || segment.start_sequence > to_sequence {
                continue;
            }

            let segment_entries = self.read_segment_entries(segment, from_sequence, to_sequence).await?;
            entries.extend(segment_entries);
        }

        entries.sort_by_key(|e| e.sequence);
        entries.dedup_by_key(|e| e.sequence);
        Ok(entries)
    }

    /// Reads entries from a segment file
    async fn read_segment_entries(
        &self,
        segment: &JournalSegment,
        from_sequence: u64,
        to_sequence: u64,
    ) -> Result<Vec<ChangeEntry>> {
        let file = File::open(&segment.path).await?;
        let mut reader = BufReader::new(file);

        // Skip header
        let mut header = [0u8; 64];
        reader.read_exact(&mut header).await?;

        let mut entries = Vec::new();
        let mut len_buf = [0u8; 4];

        loop {
            match reader.read_exact(&mut len_buf).await {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut entry_buf = vec![0u8; len];
                    reader.read_exact(&mut entry_buf).await?;

                    let entry: ChangeEntry = serde_json::from_slice(&entry_buf)?;
                    let seq = entry.sequence;
                    if seq >= from_sequence && seq <= to_sequence {
                        entries.push(entry);
                    }
                    if seq > to_sequence {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(entries)
    }

    /// Gets entries for a specific object
    pub async fn get_object_history(
        &self,
        bucket: &str,
        key: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ChangeEntry>> {
        let buffer = self.buffer.read().await;

        let mut entries: Vec<ChangeEntry> = buffer
            .iter()
            .filter(|e| e.bucket == bucket && e.key == key)
            .cloned()
            .collect();

        entries.sort_by(|a, b| b.sequence.cmp(&a.sequence)); // Most recent first

        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        Ok(entries)
    }

    /// Gets the current sequence number
    pub async fn current_sequence(&self) -> u64 {
        *self.sequence.read().await
    }

    /// Gets entries since a timestamp
    pub async fn get_entries_since(&self, since: SystemTime) -> Result<Vec<ChangeEntry>> {
        let buffer = self.buffer.read().await;

        let entries: Vec<ChangeEntry> = buffer
            .iter()
            .filter(|e| e.timestamp >= since)
            .cloned()
            .collect();

        Ok(entries)
    }

    /// Truncates journal entries older than retention period
    pub async fn cleanup_old_entries(&self) -> Result<u64> {
        let cutoff = SystemTime::now() - self.config.retention_duration;
        let mut removed = 0u64;

        let mut segments = self.segments.write().await;
        let mut to_remove = Vec::new();

        for (idx, segment) in segments.iter().enumerate() {
            if segment.created_at < cutoff && segment.closed {
                // Remove segment file
                if let Err(e) = tokio::fs::remove_file(&segment.path).await {
                    tracing::warn!("Failed to remove old segment {:?}: {}", segment.path, e);
                } else {
                    to_remove.push(idx);
                    removed += segment.entry_count;
                }
            }
        }

        // Remove from list in reverse order to maintain indices
        for idx in to_remove.into_iter().rev() {
            segments.remove(idx);
        }

        Ok(removed)
    }

    /// Gets journal statistics
    pub async fn stats(&self) -> JournalStats {
        let segments = self.segments.read().await;
        let buffer = self.buffer.read().await;

        let total_entries: u64 = segments.iter().map(|s| s.entry_count).sum();
        let total_size: u64 = segments.iter().map(|s| s.size).sum();

        JournalStats {
            current_sequence: *self.sequence.read().await,
            segment_count: segments.len(),
            total_entries: total_entries + buffer.len() as u64,
            total_size,
            buffer_entries: buffer.len(),
            oldest_entry: segments.first().map(|s| s.created_at),
            newest_entry: buffer.back().map(|e| e.timestamp),
        }
    }
}

/// Internal state for async operations
struct JournalInternals {
    config: JournalConfig,
    sequence: Arc<RwLock<u64>>,
    active_segment: Arc<RwLock<Option<JournalSegment>>>,
    segments: Arc<RwLock<Vec<JournalSegment>>>,
    buffer: Arc<RwLock<VecDeque<ChangeEntry>>>,
    buffer_limit: usize,
}

/// Journal statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalStats {
    /// Current sequence number
    pub current_sequence: u64,
    /// Number of segment files
    pub segment_count: usize,
    /// Total number of entries
    pub total_entries: u64,
    /// Total size in bytes
    pub total_size: u64,
    /// Entries in memory buffer
    pub buffer_entries: usize,
    /// Oldest entry timestamp
    pub oldest_entry: Option<SystemTime>,
    /// Newest entry timestamp
    pub newest_entry: Option<SystemTime>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_change_entry_creation() {
        let entry = ChangeEntry::new(1, ChangeType::Create, "test-bucket", "test-key")
            .with_size(1024)
            .with_version("v1".to_string());

        assert_eq!(entry.sequence, 1);
        assert_eq!(entry.change_type, ChangeType::Create);
        assert_eq!(entry.bucket, "test-bucket");
        assert_eq!(entry.key, "test-key");
        assert_eq!(entry.size, Some(1024));
    }

    #[tokio::test]
    async fn test_journal_append() {
        let dir = tempdir().unwrap();
        let config = JournalConfig {
            journal_dir: dir.path().to_path_buf(),
            sync_on_write: false,
            ..Default::default()
        };

        let journal = ChangeJournal::new(config).await.unwrap();

        let entry = ChangeEntry::new(0, ChangeType::Create, "bucket", "key");
        let seq = journal.append(entry).await.unwrap();

        assert_eq!(seq, 1);
        assert_eq!(journal.current_sequence().await, 2);
    }

    #[tokio::test]
    async fn test_journal_get_history() {
        let dir = tempdir().unwrap();
        let config = JournalConfig {
            journal_dir: dir.path().to_path_buf(),
            sync_on_write: false,
            ..Default::default()
        };

        let journal = ChangeJournal::new(config).await.unwrap();

        // Add some entries
        for i in 0..5 {
            let entry = ChangeEntry::new(0, ChangeType::Update, "bucket", "key")
                .with_size(i * 100);
            journal.append(entry).await.unwrap();
        }

        // Get history
        let history = journal.get_object_history("bucket", "key", Some(3)).await.unwrap();
        assert_eq!(history.len(), 3);
    }
}
