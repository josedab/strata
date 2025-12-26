//! Backup and restore functionality for Strata.
//!
//! Provides utilities for creating and restoring backups of metadata and data.

use crate::error::{Result, StrataError};
use crate::metadata::MetadataStateMachine;
use crate::raft::StateMachine;
use crate::types::ChunkId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Backup manifest containing metadata about a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Backup format version.
    pub version: u32,
    /// When the backup was created.
    pub created_at: DateTime<Utc>,
    /// Human-readable description.
    pub description: Option<String>,
    /// Source cluster name.
    pub cluster_name: String,
    /// Node ID that created the backup.
    pub source_node: u64,
    /// Metadata snapshot checksum.
    pub metadata_checksum: String,
    /// Number of chunks included.
    pub chunk_count: usize,
    /// Total size of chunks in bytes.
    pub total_size: u64,
    /// List of chunk checksums.
    pub chunk_checksums: HashMap<ChunkId, String>,
    /// Backup type.
    pub backup_type: BackupType,
}

impl BackupManifest {
    /// Create a new backup manifest.
    pub fn new(cluster_name: impl Into<String>, source_node: u64) -> Self {
        Self {
            version: 1,
            created_at: Utc::now(),
            description: None,
            cluster_name: cluster_name.into(),
            source_node,
            metadata_checksum: String::new(),
            chunk_count: 0,
            total_size: 0,
            chunk_checksums: HashMap::new(),
            backup_type: BackupType::Full,
        }
    }

    /// Set description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set backup type.
    pub fn with_type(mut self, backup_type: BackupType) -> Self {
        self.backup_type = backup_type;
        self
    }
}

/// Type of backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    /// Full backup of all data.
    Full,
    /// Incremental backup since last full.
    Incremental,
    /// Metadata only backup.
    MetadataOnly,
}

/// Backup writer for creating backups.
pub struct BackupWriter {
    /// Output directory.
    output_dir: PathBuf,
    /// Manifest being built.
    manifest: BackupManifest,
    /// Chunks written.
    chunks_written: Vec<ChunkId>,
}

impl BackupWriter {
    /// Create a new backup writer.
    pub fn new(output_dir: impl AsRef<Path>, manifest: BackupManifest) -> Result<Self> {
        let output_dir = output_dir.as_ref().to_path_buf();
        fs::create_dir_all(&output_dir)?;

        Ok(Self {
            output_dir,
            manifest,
            chunks_written: Vec::new(),
        })
    }

    /// Write metadata snapshot.
    pub fn write_metadata(&mut self, state_machine: &MetadataStateMachine) -> Result<()> {
        let snapshot = state_machine.snapshot();
        let checksum = calculate_checksum(&snapshot);

        let metadata_path = self.output_dir.join("metadata.bin");
        let mut file = BufWriter::new(File::create(&metadata_path)?);
        file.write_all(&snapshot)?;
        file.flush()?;

        self.manifest.metadata_checksum = checksum;
        Ok(())
    }

    /// Write a chunk.
    pub fn write_chunk(&mut self, chunk_id: ChunkId, data: &[u8]) -> Result<()> {
        let chunk_dir = self.output_dir.join("chunks");
        fs::create_dir_all(&chunk_dir)?;

        let chunk_path = chunk_dir.join(format!("{}.bin", chunk_id));
        let mut file = BufWriter::new(File::create(&chunk_path)?);
        file.write_all(data)?;
        file.flush()?;

        let checksum = calculate_checksum(data);
        self.manifest.chunk_checksums.insert(chunk_id, checksum);
        self.manifest.chunk_count += 1;
        self.manifest.total_size += data.len() as u64;
        self.chunks_written.push(chunk_id);

        Ok(())
    }

    /// Finalize the backup and write the manifest.
    pub fn finalize(self) -> Result<BackupManifest> {
        let manifest_path = self.output_dir.join("manifest.json");
        let file = File::create(&manifest_path)?;
        serde_json::to_writer_pretty(file, &self.manifest)?;

        Ok(self.manifest)
    }
}

/// Backup reader for restoring backups.
pub struct BackupReader {
    /// Backup directory.
    backup_dir: PathBuf,
    /// Loaded manifest.
    manifest: BackupManifest,
}

impl BackupReader {
    /// Open an existing backup.
    pub fn open(backup_dir: impl AsRef<Path>) -> Result<Self> {
        let backup_dir = backup_dir.as_ref().to_path_buf();

        let manifest_path = backup_dir.join("manifest.json");
        if !manifest_path.exists() {
            return Err(StrataError::NotFound("Backup manifest not found".to_string()));
        }

        let file = File::open(&manifest_path)?;
        let manifest: BackupManifest = serde_json::from_reader(file)?;

        Ok(Self {
            backup_dir,
            manifest,
        })
    }

    /// Get the backup manifest.
    pub fn manifest(&self) -> &BackupManifest {
        &self.manifest
    }

    /// Read and verify metadata.
    pub fn read_metadata(&self) -> Result<Vec<u8>> {
        let metadata_path = self.backup_dir.join("metadata.bin");
        let mut file = BufReader::new(File::open(&metadata_path)?);

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let checksum = calculate_checksum(&data);
        if checksum != self.manifest.metadata_checksum {
            return Err(StrataError::InvalidData(
                "Metadata checksum mismatch".to_string(),
            ));
        }

        Ok(data)
    }

    /// Read and verify a chunk.
    pub fn read_chunk(&self, chunk_id: ChunkId) -> Result<Vec<u8>> {
        let chunk_path = self.backup_dir.join("chunks").join(format!("{}.bin", chunk_id));

        if !chunk_path.exists() {
            return Err(StrataError::ChunkNotFound(chunk_id.to_string()));
        }

        let mut file = BufReader::new(File::open(&chunk_path)?);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Verify checksum
        if let Some(expected) = self.manifest.chunk_checksums.get(&chunk_id) {
            let actual = calculate_checksum(&data);
            if &actual != expected {
                return Err(StrataError::ChecksumMismatch {
                    expected: 0, // Simplified
                    actual: 1,
                });
            }
        }

        Ok(data)
    }

    /// List all chunk IDs in the backup.
    pub fn list_chunks(&self) -> Vec<ChunkId> {
        self.manifest.chunk_checksums.keys().copied().collect()
    }

    /// Verify the entire backup integrity.
    pub fn verify(&self) -> Result<VerificationResult> {
        let mut result = VerificationResult {
            valid: true,
            metadata_ok: false,
            chunks_ok: 0,
            chunks_failed: 0,
            errors: Vec::new(),
        };

        // Verify metadata
        match self.read_metadata() {
            Ok(_) => result.metadata_ok = true,
            Err(e) => {
                result.valid = false;
                result.errors.push(format!("Metadata: {}", e));
            }
        }

        // Verify chunks
        for chunk_id in self.list_chunks() {
            match self.read_chunk(chunk_id) {
                Ok(_) => result.chunks_ok += 1,
                Err(e) => {
                    result.valid = false;
                    result.chunks_failed += 1;
                    result.errors.push(format!("Chunk {}: {}", chunk_id, e));
                }
            }
        }

        Ok(result)
    }
}

/// Result of backup verification.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Whether the backup is valid.
    pub valid: bool,
    /// Whether metadata is OK.
    pub metadata_ok: bool,
    /// Number of chunks that verified OK.
    pub chunks_ok: usize,
    /// Number of chunks that failed verification.
    pub chunks_failed: usize,
    /// List of errors.
    pub errors: Vec<String>,
}

/// Backup configuration.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Output directory for backups.
    pub output_dir: PathBuf,
    /// Compression enabled.
    pub compress: bool,
    /// Backup type.
    pub backup_type: BackupType,
    /// Include only specified chunks (None = all).
    pub include_chunks: Option<Vec<ChunkId>>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("./backups"),
            compress: true,
            backup_type: BackupType::Full,
            include_chunks: None,
        }
    }
}

/// Calculate SHA256 checksum of data.
fn calculate_checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Backup scheduler for automated backups.
pub struct BackupScheduler {
    /// Backup configuration.
    config: BackupConfig,
    /// Cluster name.
    cluster_name: String,
    /// Node ID.
    node_id: u64,
}

impl BackupScheduler {
    /// Create a new backup scheduler.
    pub fn new(config: BackupConfig, cluster_name: impl Into<String>, node_id: u64) -> Self {
        Self {
            config,
            cluster_name: cluster_name.into(),
            node_id,
        }
    }

    /// Create a backup.
    pub fn create_backup(&self, state_machine: &MetadataStateMachine) -> Result<BackupManifest> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let backup_name = format!("backup_{}_{}", self.cluster_name, timestamp);
        let backup_dir = self.config.output_dir.join(&backup_name);

        let manifest = BackupManifest::new(&self.cluster_name, self.node_id)
            .with_type(self.config.backup_type)
            .with_description(format!("Automated backup at {}", Utc::now()));

        let mut writer = BackupWriter::new(&backup_dir, manifest)?;
        writer.write_metadata(state_machine)?;

        writer.finalize()
    }

    /// Get the latest backup in the output directory.
    pub fn latest_backup(&self) -> Result<Option<PathBuf>> {
        let mut latest: Option<(PathBuf, std::time::SystemTime)> = None;

        for entry in fs::read_dir(&self.config.output_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() && path.join("manifest.json").exists() {
                let metadata = fs::metadata(&path)?;
                let modified = metadata.modified()?;

                match &latest {
                    None => latest = Some((path, modified)),
                    Some((_, latest_time)) if modified > *latest_time => {
                        latest = Some((path, modified));
                    }
                    _ => {}
                }
            }
        }

        Ok(latest.map(|(path, _)| path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_backup_manifest_creation() {
        let manifest = BackupManifest::new("test-cluster", 1)
            .with_description("Test backup")
            .with_type(BackupType::Full);

        assert_eq!(manifest.cluster_name, "test-cluster");
        assert_eq!(manifest.source_node, 1);
        assert!(manifest.description.is_some());
        assert_eq!(manifest.backup_type, BackupType::Full);
    }

    #[test]
    fn test_backup_write_read_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup1");

        // Create state machine and add some data
        let mut sm = MetadataStateMachine::new();
        use crate::metadata::MetadataOp;
        sm.apply_op(&MetadataOp::CreateFile {
            parent: 1,
            name: "test.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        // Create backup
        let manifest = BackupManifest::new("test", 1);
        let mut writer = BackupWriter::new(&backup_dir, manifest).unwrap();
        writer.write_metadata(&sm).unwrap();
        let manifest = writer.finalize().unwrap();

        assert!(!manifest.metadata_checksum.is_empty());

        // Read backup
        let reader = BackupReader::open(&backup_dir).unwrap();
        let metadata = reader.read_metadata().unwrap();
        assert!(!metadata.is_empty());
    }

    #[test]
    fn test_backup_write_read_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup2");

        let chunk_id = ChunkId::new();
        let chunk_data = vec![1, 2, 3, 4, 5];

        // Create backup with chunk
        let manifest = BackupManifest::new("test", 1);
        let mut writer = BackupWriter::new(&backup_dir, manifest).unwrap();
        writer.write_chunk(chunk_id, &chunk_data).unwrap();
        let manifest = writer.finalize().unwrap();

        assert_eq!(manifest.chunk_count, 1);
        assert_eq!(manifest.total_size, 5);

        // Read chunk
        let reader = BackupReader::open(&backup_dir).unwrap();
        let read_data = reader.read_chunk(chunk_id).unwrap();
        assert_eq!(read_data, chunk_data);
    }

    #[test]
    fn test_backup_verification() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup3");

        let sm = MetadataStateMachine::new();
        let manifest = BackupManifest::new("test", 1);
        let mut writer = BackupWriter::new(&backup_dir, manifest).unwrap();
        writer.write_metadata(&sm).unwrap();
        writer.finalize().unwrap();

        let reader = BackupReader::open(&backup_dir).unwrap();
        let result = reader.verify().unwrap();

        assert!(result.valid);
        assert!(result.metadata_ok);
        assert_eq!(result.chunks_failed, 0);
    }

    #[test]
    fn test_calculate_checksum() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"hello worlD";

        let c1 = calculate_checksum(data1);
        let c2 = calculate_checksum(data2);
        let c3 = calculate_checksum(data3);

        assert_eq!(c1, c2);
        assert_ne!(c1, c3);
    }

    #[test]
    fn test_backup_config_default() {
        let config = BackupConfig::default();
        assert!(config.compress);
        assert_eq!(config.backup_type, BackupType::Full);
    }
}
