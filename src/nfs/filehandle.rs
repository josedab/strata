//! NFS file handle generation and management.
//!
//! File handles are opaque identifiers that map to inodes in Strata.
//! This module provides:
//! - File handle generation from inode IDs
//! - Caching for fast lookup
//! - Expiration and invalidation

use crate::types::InodeId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Maximum file handle size (128 bytes for NFSv4).
pub const MAX_FH_SIZE: usize = 128;

/// File handle structure.
///
/// The file handle contains enough information to identify and validate
/// the referenced object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileHandle {
    /// Version for handle format changes.
    version: u8,
    /// Generation counter to detect stale handles.
    generation: u32,
    /// The inode ID.
    inode_id: InodeId,
    /// Handle creation timestamp (truncated).
    timestamp: u32,
    /// Verification checksum.
    checksum: u32,
}

impl FileHandle {
    /// Current file handle version.
    pub const VERSION: u8 = 1;

    /// Create a new file handle for an inode.
    pub fn new(inode_id: InodeId, generation: u32) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);

        let mut handle = Self {
            version: Self::VERSION,
            generation,
            inode_id,
            timestamp,
            checksum: 0,
        };
        handle.checksum = handle.compute_checksum();
        handle
    }

    /// Get the inode ID from the file handle.
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    /// Get the generation counter.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Validate the file handle checksum.
    pub fn is_valid(&self) -> bool {
        self.version == Self::VERSION && self.checksum == self.compute_checksum()
    }

    /// Encode the file handle to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(21);
        bytes.push(self.version);
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.inode_id.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());
        bytes.extend_from_slice(&self.checksum.to_be_bytes());
        bytes
    }

    /// Decode a file handle from bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 21 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let generation = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let inode_id = u64::from_be_bytes([
            bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
        ]);
        let timestamp = u32::from_be_bytes([bytes[13], bytes[14], bytes[15], bytes[16]]);
        let checksum = u32::from_be_bytes([bytes[17], bytes[18], bytes[19], bytes[20]]);

        let handle = Self {
            version,
            generation,
            inode_id,
            timestamp,
            checksum,
        };

        if handle.is_valid() {
            Some(handle)
        } else {
            None
        }
    }

    /// Compute the checksum for the file handle.
    fn compute_checksum(&self) -> u32 {
        // Simple checksum using FNV-1a
        let mut hash: u32 = 2166136261;
        hash ^= self.version as u32;
        hash = hash.wrapping_mul(16777619);
        hash ^= self.generation;
        hash = hash.wrapping_mul(16777619);
        hash ^= (self.inode_id >> 32) as u32;
        hash = hash.wrapping_mul(16777619);
        hash ^= self.inode_id as u32;
        hash = hash.wrapping_mul(16777619);
        hash ^= self.timestamp;
        hash = hash.wrapping_mul(16777619);
        hash
    }
}

/// Entry in the file handle cache.
struct CacheEntry {
    /// The file handle.
    handle: FileHandle,
    /// Last access time for LRU eviction.
    last_access: Instant,
}

/// File handle generator and cache.
///
/// Manages the creation and caching of file handles.
pub struct FileHandleGenerator {
    /// Generation counter for new handles.
    generation: AtomicU64,
    /// Cache of inode ID -> file handle.
    cache: RwLock<HashMap<InodeId, CacheEntry>>,
    /// Reverse cache of file handle bytes -> inode ID.
    reverse_cache: RwLock<HashMap<Vec<u8>, InodeId>>,
    /// Maximum cache size.
    max_entries: usize,
    /// Cache entry TTL.
    ttl: Duration,
}

impl FileHandleGenerator {
    /// Create a new file handle generator.
    pub fn new(max_entries: usize) -> Self {
        Self {
            generation: AtomicU64::new(1),
            cache: RwLock::new(HashMap::new()),
            reverse_cache: RwLock::new(HashMap::new()),
            max_entries,
            ttl: Duration::from_secs(3600), // 1 hour default TTL
        }
    }

    /// Generate or retrieve a file handle for an inode.
    pub fn get_or_create(&self, inode_id: InodeId) -> FileHandle {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&inode_id) {
                if entry.last_access.elapsed() < self.ttl {
                    return entry.handle.clone();
                }
            }
        }

        // Create new handle
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) as u32;
        let handle = FileHandle::new(inode_id, generation);

        // Insert into cache
        self.insert_cache(inode_id, handle.clone());

        handle
    }

    /// Look up an inode ID from a file handle.
    pub fn lookup(&self, fh_bytes: &[u8]) -> Option<InodeId> {
        // Try reverse cache first
        {
            let reverse = self.reverse_cache.read();
            if let Some(&inode_id) = reverse.get(fh_bytes) {
                // Update access time
                let mut cache = self.cache.write();
                if let Some(entry) = cache.get_mut(&inode_id) {
                    entry.last_access = Instant::now();
                }
                return Some(inode_id);
            }
        }

        // Decode the handle
        let handle = FileHandle::decode(fh_bytes)?;
        if !handle.is_valid() {
            return None;
        }

        // Add to cache
        self.insert_cache(handle.inode_id, handle.clone());

        Some(handle.inode_id)
    }

    /// Invalidate a cached file handle.
    pub fn invalidate(&self, inode_id: InodeId) {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.remove(&inode_id) {
            let mut reverse = self.reverse_cache.write();
            reverse.remove(&entry.handle.encode());
        }
    }

    /// Clear all cached handles.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let mut reverse = self.reverse_cache.write();
        cache.clear();
        reverse.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> FileHandleCacheStats {
        let cache = self.cache.read();
        FileHandleCacheStats {
            entries: cache.len(),
            max_entries: self.max_entries,
            generation: self.generation.load(Ordering::SeqCst),
        }
    }

    /// Insert into cache with eviction.
    fn insert_cache(&self, inode_id: InodeId, handle: FileHandle) {
        let mut cache = self.cache.write();
        let mut reverse = self.reverse_cache.write();

        // Evict if necessary
        if cache.len() >= self.max_entries {
            self.evict_lru(&mut cache, &mut reverse);
        }

        let encoded = handle.encode();
        reverse.insert(encoded, inode_id);
        cache.insert(
            inode_id,
            CacheEntry {
                handle,
                last_access: Instant::now(),
            },
        );
    }

    /// Evict least recently used entries.
    fn evict_lru(
        &self,
        cache: &mut HashMap<InodeId, CacheEntry>,
        reverse: &mut HashMap<Vec<u8>, InodeId>,
    ) {
        // Find the oldest entry
        let oldest = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(&id, _)| id);

        if let Some(inode_id) = oldest {
            if let Some(entry) = cache.remove(&inode_id) {
                reverse.remove(&entry.handle.encode());
            }
        }
    }
}

/// Statistics for the file handle cache.
#[derive(Debug, Clone)]
pub struct FileHandleCacheStats {
    /// Current number of entries.
    pub entries: usize,
    /// Maximum entries allowed.
    pub max_entries: usize,
    /// Current generation counter.
    pub generation: u64,
}

/// Root file handle (inode 1).
pub fn root_handle() -> FileHandle {
    FileHandle::new(1, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_handle_encode_decode() {
        let handle = FileHandle::new(12345, 1);
        assert!(handle.is_valid());

        let encoded = handle.encode();
        let decoded = FileHandle::decode(&encoded).unwrap();

        assert_eq!(decoded.inode_id(), 12345);
        assert_eq!(decoded.generation(), 1);
        assert!(decoded.is_valid());
    }

    #[test]
    fn test_file_handle_invalid() {
        // Too short
        assert!(FileHandle::decode(&[1, 2, 3]).is_none());

        // Wrong version
        let mut handle = FileHandle::new(123, 1);
        let mut bytes = handle.encode();
        bytes[0] = 99;
        assert!(FileHandle::decode(&bytes).is_none());

        // Corrupted checksum
        let mut bytes = handle.encode();
        bytes[20] ^= 0xFF;
        assert!(FileHandle::decode(&bytes).is_none());
    }

    #[test]
    fn test_file_handle_generator() {
        let gen = FileHandleGenerator::new(100);

        let fh1 = gen.get_or_create(100);
        let fh2 = gen.get_or_create(100);

        // Should return same handle
        assert_eq!(fh1.inode_id(), fh2.inode_id());
        assert_eq!(fh1.generation(), fh2.generation());

        // Lookup should work
        let inode_id = gen.lookup(&fh1.encode()).unwrap();
        assert_eq!(inode_id, 100);
    }

    #[test]
    fn test_file_handle_generator_different_inodes() {
        let gen = FileHandleGenerator::new(100);

        let fh1 = gen.get_or_create(100);
        let fh2 = gen.get_or_create(200);

        assert_eq!(fh1.inode_id(), 100);
        assert_eq!(fh2.inode_id(), 200);
        assert_ne!(fh1.generation(), fh2.generation());
    }

    #[test]
    fn test_file_handle_invalidate() {
        let gen = FileHandleGenerator::new(100);

        let fh = gen.get_or_create(100);
        let encoded = fh.encode();

        gen.invalidate(100);

        // Lookup should still work (decodes the handle)
        let inode_id = gen.lookup(&encoded).unwrap();
        assert_eq!(inode_id, 100);
    }

    #[test]
    fn test_root_handle() {
        let root = root_handle();
        assert_eq!(root.inode_id(), 1);
        assert!(root.is_valid());
    }
}
