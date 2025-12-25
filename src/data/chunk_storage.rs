//! Local chunk storage implementation.

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use tracing::{debug, error, warn};

/// Stored chunk with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredChunk {
    /// Chunk ID.
    pub id: ChunkId,
    /// Shard index (for erasure coded chunks).
    pub shard_index: usize,
    /// Data size.
    pub size: u64,
    /// CRC32 checksum.
    pub checksum: u32,
    /// SHA256 hash.
    pub sha256: [u8; 32],
    /// Version number.
    pub version: u64,
}

impl StoredChunk {
    pub fn new(id: ChunkId, shard_index: usize, data: &[u8]) -> Self {
        let checksum = crc32fast::hash(data);
        let sha256 = Sha256::digest(data).into();

        Self {
            id,
            shard_index,
            size: data.len() as u64,
            checksum,
            sha256,
            version: 1,
        }
    }

    /// Verify data against stored checksum.
    pub fn verify(&self, data: &[u8]) -> bool {
        crc32fast::hash(data) == self.checksum
    }

    /// Verify data against SHA256 hash.
    pub fn verify_sha256(&self, data: &[u8]) -> bool {
        let hash: [u8; 32] = Sha256::digest(data).into();
        hash == self.sha256
    }
}

/// Local chunk storage with caching.
pub struct ChunkStorage {
    /// Base directory for chunk files.
    data_dir: PathBuf,
    /// LRU cache for frequently accessed chunks.
    cache: Mutex<LruCache<(ChunkId, usize), Vec<u8>>>,
    /// Cache size in bytes.
    cache_size: usize,
    /// Current cache usage.
    cache_bytes_used: Mutex<usize>,
}

impl ChunkStorage {
    /// Create a new chunk storage.
    pub fn new<P: AsRef<Path>>(data_dir: P, cache_size: usize) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        // Create subdirectories for sharding
        for i in 0..256 {
            fs::create_dir_all(data_dir.join(format!("{:02x}", i)))?;
        }

        let cache_entries = NonZeroUsize::new(10000).expect("10000 is non-zero");

        Ok(Self {
            data_dir,
            cache: Mutex::new(LruCache::new(cache_entries)),
            cache_size,
            cache_bytes_used: Mutex::new(0),
        })
    }

    /// Write a chunk shard to storage.
    pub fn write_shard(
        &self,
        chunk_id: ChunkId,
        shard_index: usize,
        data: &[u8],
    ) -> Result<StoredChunk> {
        let path = self.shard_path(chunk_id, shard_index);
        let meta_path = self.shard_meta_path(chunk_id, shard_index);

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write data atomically
        let tmp_path = path.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &path)?;

        // Create and write metadata
        let meta = StoredChunk::new(chunk_id, shard_index, data);
        let meta_json = serde_json::to_vec(&meta)?;
        fs::write(&meta_path, &meta_json)?;

        // Update cache
        self.cache_put(chunk_id, shard_index, data.to_vec());

        debug!(
            chunk_id = %chunk_id,
            shard_index,
            size = data.len(),
            "Wrote shard"
        );

        Ok(meta)
    }

    /// Read a chunk shard from storage.
    pub fn read_shard(&self, chunk_id: ChunkId, shard_index: usize) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(data) = self.cache_get(chunk_id, shard_index) {
            return Ok(data);
        }

        let path = self.shard_path(chunk_id, shard_index);
        let meta_path = self.shard_meta_path(chunk_id, shard_index);

        // Read data
        let mut file = File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StrataError::ChunkNotFound(format!("{}:{}", chunk_id, shard_index))
            } else {
                e.into()
            }
        })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Verify checksum if metadata exists
        if meta_path.exists() {
            let meta_json = fs::read(&meta_path)?;
            let meta: StoredChunk = serde_json::from_slice(&meta_json)?;

            if !meta.verify(&data) {
                error!(
                    chunk_id = %chunk_id,
                    shard_index,
                    "Checksum mismatch"
                );
                return Err(StrataError::ChecksumMismatch {
                    expected: meta.checksum,
                    actual: crc32fast::hash(&data),
                });
            }
        }

        // Update cache
        self.cache_put(chunk_id, shard_index, data.clone());

        Ok(data)
    }

    /// Delete a chunk shard.
    pub fn delete_shard(&self, chunk_id: ChunkId, shard_index: usize) -> Result<()> {
        let path = self.shard_path(chunk_id, shard_index);
        let meta_path = self.shard_meta_path(chunk_id, shard_index);

        // Remove from cache
        self.cache_remove(chunk_id, shard_index);

        // Delete files
        if path.exists() {
            fs::remove_file(&path)?;
        }
        if meta_path.exists() {
            fs::remove_file(&meta_path)?;
        }

        debug!(chunk_id = %chunk_id, shard_index, "Deleted shard");

        Ok(())
    }

    /// Check if a shard exists.
    pub fn shard_exists(&self, chunk_id: ChunkId, shard_index: usize) -> bool {
        self.shard_path(chunk_id, shard_index).exists()
    }

    /// Get shard metadata.
    pub fn get_shard_meta(&self, chunk_id: ChunkId, shard_index: usize) -> Result<Option<StoredChunk>> {
        let meta_path = self.shard_meta_path(chunk_id, shard_index);

        if !meta_path.exists() {
            return Ok(None);
        }

        let meta_json = fs::read(&meta_path)?;
        let meta: StoredChunk = serde_json::from_slice(&meta_json)?;

        Ok(Some(meta))
    }

    /// List all shards for a chunk.
    pub fn list_shards(&self, chunk_id: ChunkId) -> Result<Vec<usize>> {
        let prefix = format!("{}_", chunk_id);
        let first_byte = chunk_id.as_bytes()[0];
        let dir = self.data_dir.join(format!("{:02x}", first_byte));

        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut shards = Vec::new();

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();

            if name.starts_with(&prefix) && name.ends_with(".dat") {
                // Extract shard index from filename
                if let Some(idx_str) = name.strip_prefix(&prefix).and_then(|s| s.strip_suffix(".dat")) {
                    if let Ok(idx) = idx_str.parse() {
                        shards.push(idx);
                    }
                }
            }
        }

        shards.sort();
        Ok(shards)
    }

    /// Scrub (verify integrity of) a shard.
    pub fn scrub_shard(&self, chunk_id: ChunkId, shard_index: usize) -> Result<bool> {
        let data = self.read_shard(chunk_id, shard_index)?;
        let meta = self.get_shard_meta(chunk_id, shard_index)?;

        if let Some(meta) = meta {
            // Check both CRC and SHA256
            let crc_ok = meta.verify(&data);
            let sha_ok = meta.verify_sha256(&data);

            if !crc_ok || !sha_ok {
                warn!(
                    chunk_id = %chunk_id,
                    shard_index,
                    crc_ok,
                    sha_ok,
                    "Scrub found corruption"
                );
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Calculate total storage used.
    pub fn calculate_usage(&self) -> Result<u64> {
        let mut total = 0u64;

        for i in 0..256 {
            let dir = self.data_dir.join(format!("{:02x}", i));
            if dir.exists() {
                for entry in fs::read_dir(&dir)? {
                    let entry = entry?;
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
            }
        }

        Ok(total)
    }

    // Path helpers

    fn shard_path(&self, chunk_id: ChunkId, shard_index: usize) -> PathBuf {
        let first_byte = chunk_id.as_bytes()[0];
        self.data_dir
            .join(format!("{:02x}", first_byte))
            .join(format!("{}_{}.dat", chunk_id, shard_index))
    }

    fn shard_meta_path(&self, chunk_id: ChunkId, shard_index: usize) -> PathBuf {
        let first_byte = chunk_id.as_bytes()[0];
        self.data_dir
            .join(format!("{:02x}", first_byte))
            .join(format!("{}_{}.meta", chunk_id, shard_index))
    }

    // Cache operations

    fn cache_get(&self, chunk_id: ChunkId, shard_index: usize) -> Option<Vec<u8>> {
        self.cache.lock().get(&(chunk_id, shard_index)).cloned()
    }

    fn cache_put(&self, chunk_id: ChunkId, shard_index: usize, data: Vec<u8>) {
        let data_len = data.len();

        // Check if we need to evict
        {
            let mut bytes_used = self.cache_bytes_used.lock();
            while *bytes_used + data_len > self.cache_size {
                let mut cache = self.cache.lock();
                if let Some((_, evicted)) = cache.pop_lru() {
                    *bytes_used = bytes_used.saturating_sub(evicted.len());
                } else {
                    break;
                }
            }
            *bytes_used += data_len;
        }

        self.cache.lock().put((chunk_id, shard_index), data);
    }

    fn cache_remove(&self, chunk_id: ChunkId, shard_index: usize) {
        if let Some(data) = self.cache.lock().pop(&(chunk_id, shard_index)) {
            let mut bytes_used = self.cache_bytes_used.lock();
            *bytes_used = bytes_used.saturating_sub(data.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_shard() {
        let dir = tempdir().unwrap();
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();

        let chunk_id = ChunkId::new();
        let data = b"Hello, World!";

        // Write
        let meta = storage.write_shard(chunk_id, 0, data).unwrap();
        assert_eq!(meta.size, 13);
        assert!(meta.verify(data));

        // Read
        let read_data = storage.read_shard(chunk_id, 0).unwrap();
        assert_eq!(&read_data, data);
    }

    #[test]
    fn test_delete_shard() {
        let dir = tempdir().unwrap();
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();

        let chunk_id = ChunkId::new();
        storage.write_shard(chunk_id, 0, b"test").unwrap();

        assert!(storage.shard_exists(chunk_id, 0));

        storage.delete_shard(chunk_id, 0).unwrap();
        assert!(!storage.shard_exists(chunk_id, 0));
    }

    #[test]
    fn test_list_shards() {
        let dir = tempdir().unwrap();
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();

        let chunk_id = ChunkId::new();
        storage.write_shard(chunk_id, 0, b"shard0").unwrap();
        storage.write_shard(chunk_id, 1, b"shard1").unwrap();
        storage.write_shard(chunk_id, 2, b"shard2").unwrap();

        let shards = storage.list_shards(chunk_id).unwrap();
        assert_eq!(shards, vec![0, 1, 2]);
    }

    #[test]
    fn test_scrub() {
        let dir = tempdir().unwrap();
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();

        let chunk_id = ChunkId::new();
        storage.write_shard(chunk_id, 0, b"test data").unwrap();

        // Should pass scrub
        assert!(storage.scrub_shard(chunk_id, 0).unwrap());
    }

    #[test]
    fn test_cache() {
        let dir = tempdir().unwrap();
        let storage = ChunkStorage::new(dir.path(), 1024).unwrap();

        let chunk_id = ChunkId::new();
        let data = vec![0u8; 100];

        storage.write_shard(chunk_id, 0, &data).unwrap();

        // Should be in cache
        assert!(storage.cache_get(chunk_id, 0).is_some());

        // Remove from cache and read again
        storage.cache_remove(chunk_id, 0);
        assert!(storage.cache_get(chunk_id, 0).is_none());

        // Read should repopulate cache
        storage.read_shard(chunk_id, 0).unwrap();
        assert!(storage.cache_get(chunk_id, 0).is_some());
    }
}
