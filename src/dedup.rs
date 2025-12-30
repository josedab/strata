//! Content-aware deduplication for Strata.
//!
//! Implements variable-length content-defined chunking (CDC) using a rolling hash
//! algorithm (Rabin fingerprinting) to find natural chunk boundaries. This achieves
//! much better deduplication ratios than fixed-size chunking, especially for files
//! that have insertions or deletions.
//!
//! # How It Works
//!
//! 1. Data is scanned with a rolling hash window
//! 2. When the hash matches a pattern, a chunk boundary is created
//! 3. Each chunk is hashed (SHA-256) to create a content address
//! 4. Chunks are stored once and referenced by their content hash
//! 5. Similar files share chunks, dramatically reducing storage

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Content hash - the unique identifier for deduplicated content.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get as hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> Result<Self> {
        let bytes = hex::decode(s).map_err(|e| StrataError::Internal(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(StrataError::Internal("Invalid hash length".into()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}

/// Configuration for content-defined chunking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkerConfig {
    /// Minimum chunk size in bytes.
    pub min_chunk_size: usize,
    /// Average chunk size in bytes (target).
    pub avg_chunk_size: usize,
    /// Maximum chunk size in bytes.
    pub max_chunk_size: usize,
    /// Window size for rolling hash.
    pub window_size: usize,
    /// Mask for chunk boundary detection (determines average size).
    pub chunk_mask: u64,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        // Default: 4KB min, 16KB avg, 64KB max
        Self {
            min_chunk_size: 4 * 1024,
            avg_chunk_size: 16 * 1024,
            max_chunk_size: 64 * 1024,
            window_size: 48,
            chunk_mask: (1 << 14) - 1, // ~16KB average
        }
    }
}

impl ChunkerConfig {
    /// Small chunks for high deduplication (good for VMs, databases).
    pub fn high_dedup() -> Self {
        Self {
            min_chunk_size: 2 * 1024,
            avg_chunk_size: 8 * 1024,
            max_chunk_size: 32 * 1024,
            window_size: 48,
            chunk_mask: (1 << 13) - 1, // ~8KB average
        }
    }

    /// Large chunks for throughput (good for media, backups).
    pub fn high_throughput() -> Self {
        Self {
            min_chunk_size: 16 * 1024,
            avg_chunk_size: 64 * 1024,
            max_chunk_size: 256 * 1024,
            window_size: 64,
            chunk_mask: (1 << 16) - 1, // ~64KB average
        }
    }

    /// Balanced configuration.
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// Rabin fingerprint rolling hash for content-defined chunking.
pub struct RabinChunker {
    config: ChunkerConfig,
    /// Precomputed table for Rabin fingerprint.
    table: [u64; 256],
    /// Precomputed out-table for sliding window.
    out_table: [u64; 256],
}

impl RabinChunker {
    /// Rabin polynomial (irreducible polynomial).
    const POLYNOMIAL: u64 = 0x3DA3358B4DC173;

    /// Create a new Rabin chunker.
    pub fn new(config: ChunkerConfig) -> Self {
        let table = Self::compute_table();
        let out_table = Self::compute_out_table(&table, config.window_size);
        Self {
            config,
            table,
            out_table,
        }
    }

    fn compute_table() -> [u64; 256] {
        let mut table = [0u64; 256];
        for i in 0..256 {
            let mut hash = i as u64;
            for _ in 0..8 {
                if hash & 1 == 1 {
                    hash = (hash >> 1) ^ Self::POLYNOMIAL;
                } else {
                    hash >>= 1;
                }
            }
            table[i] = hash;
        }
        table
    }

    fn compute_out_table(table: &[u64; 256], window_size: usize) -> [u64; 256] {
        let mut out_table = [0u64; 256];
        for i in 0..256 {
            let mut hash = table[i];
            for _ in 0..window_size - 1 {
                hash = (hash >> 8) ^ table[(hash & 0xff) as usize];
            }
            out_table[i] = hash;
        }
        out_table
    }

    /// Chunk data into content-defined chunks.
    pub fn chunk(&self, data: &[u8]) -> Vec<Chunk> {
        if data.is_empty() {
            return vec![];
        }

        let mut chunks = Vec::new();
        let mut chunk_start = 0;
        let mut hash: u64 = 0;
        let mut window = vec![0u8; self.config.window_size];
        let mut window_idx = 0;
        let mut window_filled = false;

        for (i, &byte) in data.iter().enumerate() {
            let chunk_len = i - chunk_start;

            // Update rolling hash
            if window_filled {
                let out_byte = window[window_idx];
                hash ^= self.out_table[out_byte as usize];
            }
            window[window_idx] = byte;
            window_idx = (window_idx + 1) % self.config.window_size;
            if window_idx == 0 {
                window_filled = true;
            }
            hash = (hash >> 8) ^ self.table[byte as usize];

            // Check for chunk boundary
            let is_boundary = chunk_len >= self.config.min_chunk_size
                && ((hash & self.config.chunk_mask) == 0
                    || chunk_len >= self.config.max_chunk_size);

            if is_boundary {
                let chunk_data = &data[chunk_start..=i];
                chunks.push(Chunk::new(chunk_data));
                chunk_start = i + 1;
                hash = 0;
                window_filled = false;
                window_idx = 0;
            }
        }

        // Handle remaining data
        if chunk_start < data.len() {
            let chunk_data = &data[chunk_start..];
            chunks.push(Chunk::new(chunk_data));
        }

        chunks
    }
}

/// A content-addressed chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Content hash (SHA-256).
    pub hash: ContentHash,
    /// Chunk size in bytes.
    pub size: usize,
    /// Chunk data (only present when not yet stored).
    #[serde(skip)]
    pub data: Option<Vec<u8>>,
}

impl Chunk {
    /// Create a new chunk from data.
    pub fn new(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash_bytes: [u8; 32] = hasher.finalize().into();

        Self {
            hash: ContentHash(hash_bytes),
            size: data.len(),
            data: Some(data.to_vec()),
        }
    }

    /// Create a reference to an existing chunk.
    pub fn reference(hash: ContentHash, size: usize) -> Self {
        Self {
            hash,
            size,
            data: None,
        }
    }
}

/// Recipe for reconstructing a file from chunks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRecipe {
    /// Original file size.
    pub total_size: u64,
    /// Ordered list of chunk references.
    pub chunks: Vec<ChunkRef>,
    /// Number of unique chunks.
    pub unique_chunks: usize,
    /// Deduplication ratio (1.0 = no dedup, 2.0 = 50% dedup).
    pub dedup_ratio: f64,
}

/// Reference to a chunk within a recipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    /// Content hash of the chunk.
    pub hash: ContentHash,
    /// Offset within the original file.
    pub offset: u64,
    /// Size of the chunk.
    pub size: usize,
}

/// Statistics for deduplication.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DedupStats {
    /// Total bytes processed.
    pub bytes_processed: u64,
    /// Total bytes after deduplication.
    pub bytes_stored: u64,
    /// Number of chunks created.
    pub chunks_created: u64,
    /// Number of duplicate chunks found.
    pub chunks_deduplicated: u64,
    /// Storage savings in bytes.
    pub bytes_saved: u64,
}

impl DedupStats {
    /// Calculate deduplication ratio.
    pub fn dedup_ratio(&self) -> f64 {
        if self.bytes_stored == 0 {
            1.0
        } else {
            self.bytes_processed as f64 / self.bytes_stored as f64
        }
    }

    /// Calculate storage efficiency percentage.
    pub fn efficiency_percent(&self) -> f64 {
        if self.bytes_processed == 0 {
            0.0
        } else {
            (self.bytes_saved as f64 / self.bytes_processed as f64) * 100.0
        }
    }
}

/// Deduplication index entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    /// Content hash.
    pub hash: ContentHash,
    /// Reference count.
    pub ref_count: u64,
    /// Size of the chunk.
    pub size: usize,
    /// Storage location (chunk ID where data is stored).
    pub storage_id: ChunkId,
    /// When this chunk was first stored.
    pub created_at: DateTime<Utc>,
    /// When this chunk was last referenced.
    pub last_accessed: DateTime<Utc>,
}

/// Configuration for the deduplication engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupConfig {
    /// Whether deduplication is enabled.
    pub enabled: bool,
    /// Chunker configuration.
    pub chunker: ChunkerConfig,
    /// Whether to perform inline deduplication.
    pub inline_dedup: bool,
    /// Whether to run background deduplication.
    pub background_dedup: bool,
    /// Minimum file size for deduplication (bytes).
    pub min_file_size: u64,
    /// Maximum memory for dedup index cache (bytes).
    pub index_cache_size: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            chunker: ChunkerConfig::default(),
            inline_dedup: true,
            background_dedup: true,
            min_file_size: 4096,
            index_cache_size: 256 * 1024 * 1024, // 256MB
        }
    }
}

/// Chunk storage trait for persisting deduplicated chunks.
#[async_trait::async_trait]
pub trait ChunkStore: Send + Sync {
    /// Check if a chunk exists.
    async fn exists(&self, hash: &ContentHash) -> Result<bool>;

    /// Get chunk data.
    async fn get(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>>;

    /// Store chunk data.
    async fn put(&self, hash: &ContentHash, data: &[u8]) -> Result<()>;

    /// Delete chunk data.
    async fn delete(&self, hash: &ContentHash) -> Result<()>;

    /// Increment reference count.
    async fn add_ref(&self, hash: &ContentHash) -> Result<u64>;

    /// Decrement reference count, returns new count.
    async fn remove_ref(&self, hash: &ContentHash) -> Result<u64>;

    /// Get reference count.
    async fn ref_count(&self, hash: &ContentHash) -> Result<u64>;
}

/// In-memory chunk store for testing.
pub struct MemoryChunkStore {
    chunks: RwLock<HashMap<ContentHash, Vec<u8>>>,
    refs: RwLock<HashMap<ContentHash, u64>>,
}

impl MemoryChunkStore {
    pub fn new() -> Self {
        Self {
            chunks: RwLock::new(HashMap::new()),
            refs: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ChunkStore for MemoryChunkStore {
    async fn exists(&self, hash: &ContentHash) -> Result<bool> {
        Ok(self.chunks.read().await.contains_key(hash))
    }

    async fn get(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>> {
        Ok(self.chunks.read().await.get(hash).cloned())
    }

    async fn put(&self, hash: &ContentHash, data: &[u8]) -> Result<()> {
        self.chunks.write().await.insert(hash.clone(), data.to_vec());
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash) -> Result<()> {
        self.chunks.write().await.remove(hash);
        self.refs.write().await.remove(hash);
        Ok(())
    }

    async fn add_ref(&self, hash: &ContentHash) -> Result<u64> {
        let mut refs = self.refs.write().await;
        let count = refs.entry(hash.clone()).or_insert(0);
        *count += 1;
        Ok(*count)
    }

    async fn remove_ref(&self, hash: &ContentHash) -> Result<u64> {
        let mut refs = self.refs.write().await;
        if let Some(count) = refs.get_mut(hash) {
            *count = count.saturating_sub(1);
            Ok(*count)
        } else {
            Ok(0)
        }
    }

    async fn ref_count(&self, hash: &ContentHash) -> Result<u64> {
        Ok(*self.refs.read().await.get(hash).unwrap_or(&0))
    }
}

/// The main deduplication engine.
pub struct DedupEngine<S: ChunkStore> {
    config: DedupConfig,
    chunker: RabinChunker,
    store: Arc<S>,
    stats: DedupStatsInner,
}

struct DedupStatsInner {
    bytes_processed: AtomicU64,
    bytes_stored: AtomicU64,
    chunks_created: AtomicU64,
    chunks_deduplicated: AtomicU64,
}

impl<S: ChunkStore> DedupEngine<S> {
    /// Create a new deduplication engine.
    pub fn new(config: DedupConfig, store: Arc<S>) -> Self {
        let chunker = RabinChunker::new(config.chunker.clone());
        Self {
            config,
            chunker,
            store,
            stats: DedupStatsInner {
                bytes_processed: AtomicU64::new(0),
                bytes_stored: AtomicU64::new(0),
                chunks_created: AtomicU64::new(0),
                chunks_deduplicated: AtomicU64::new(0),
            },
        }
    }

    /// Process data and return a chunk recipe.
    pub async fn process(&self, data: &[u8]) -> Result<ChunkRecipe> {
        if !self.config.enabled || (data.len() as u64) < self.config.min_file_size {
            // Below minimum size, store as single chunk
            let chunk = Chunk::new(data);
            let hash = chunk.hash.clone();

            if !self.store.exists(&hash).await? {
                self.store.put(&hash, data).await?;
                self.stats.bytes_stored.fetch_add(data.len() as u64, Ordering::Relaxed);
                self.stats.chunks_created.fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats.chunks_deduplicated.fetch_add(1, Ordering::Relaxed);
            }
            self.store.add_ref(&hash).await?;
            self.stats.bytes_processed.fetch_add(data.len() as u64, Ordering::Relaxed);

            return Ok(ChunkRecipe {
                total_size: data.len() as u64,
                chunks: vec![ChunkRef {
                    hash,
                    offset: 0,
                    size: data.len(),
                }],
                unique_chunks: 1,
                dedup_ratio: 1.0,
            });
        }

        // Chunk the data
        let chunks = self.chunker.chunk(data);
        let mut recipe_chunks = Vec::with_capacity(chunks.len());
        let mut offset = 0u64;
        let mut unique_count = 0usize;
        let mut stored_bytes = 0u64;

        for chunk in chunks {
            let hash = chunk.hash.clone();
            let size = chunk.size;

            // Check if chunk already exists
            let exists = self.store.exists(&hash).await?;

            if !exists {
                // Store new chunk
                if let Some(ref chunk_data) = chunk.data {
                    self.store.put(&hash, chunk_data).await?;
                    stored_bytes += size as u64;
                    unique_count += 1;
                    self.stats.chunks_created.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                self.stats.chunks_deduplicated.fetch_add(1, Ordering::Relaxed);
            }

            // Add reference
            self.store.add_ref(&hash).await?;

            recipe_chunks.push(ChunkRef {
                hash,
                offset,
                size,
            });

            offset += size as u64;
        }

        self.stats.bytes_processed.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats.bytes_stored.fetch_add(stored_bytes, Ordering::Relaxed);

        let dedup_ratio = if stored_bytes == 0 {
            data.len() as f64 // All chunks were duplicates
        } else {
            data.len() as f64 / stored_bytes as f64
        };

        Ok(ChunkRecipe {
            total_size: data.len() as u64,
            chunks: recipe_chunks,
            unique_chunks: unique_count,
            dedup_ratio,
        })
    }

    /// Reassemble data from a chunk recipe.
    pub async fn reassemble(&self, recipe: &ChunkRecipe) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(recipe.total_size as usize);

        for chunk_ref in &recipe.chunks {
            let chunk_data = self
                .store
                .get(&chunk_ref.hash)
                .await?
                .ok_or_else(|| StrataError::NotFound(format!("Chunk {}", chunk_ref.hash)))?;

            data.extend_from_slice(&chunk_data);
        }

        if data.len() != recipe.total_size as usize {
            return Err(StrataError::Internal(format!(
                "Reassembled size {} doesn't match expected {}",
                data.len(),
                recipe.total_size
            )));
        }

        Ok(data)
    }

    /// Release references from a recipe (for deletion).
    pub async fn release(&self, recipe: &ChunkRecipe) -> Result<Vec<ContentHash>> {
        let mut orphaned = Vec::new();

        for chunk_ref in &recipe.chunks {
            let remaining = self.store.remove_ref(&chunk_ref.hash).await?;
            if remaining == 0 {
                orphaned.push(chunk_ref.hash.clone());
            }
        }

        Ok(orphaned)
    }

    /// Delete orphaned chunks (ref_count = 0).
    pub async fn gc_orphans(&self, orphans: &[ContentHash]) -> Result<u64> {
        let mut deleted = 0;

        for hash in orphans {
            if self.store.ref_count(hash).await? == 0 {
                self.store.delete(hash).await?;
                deleted += 1;
            }
        }

        info!(deleted, "Garbage collected orphaned chunks");
        Ok(deleted)
    }

    /// Get current statistics.
    pub fn stats(&self) -> DedupStats {
        let bytes_processed = self.stats.bytes_processed.load(Ordering::Relaxed);
        let bytes_stored = self.stats.bytes_stored.load(Ordering::Relaxed);

        DedupStats {
            bytes_processed,
            bytes_stored,
            chunks_created: self.stats.chunks_created.load(Ordering::Relaxed),
            chunks_deduplicated: self.stats.chunks_deduplicated.load(Ordering::Relaxed),
            bytes_saved: bytes_processed.saturating_sub(bytes_stored),
        }
    }
}

/// Similarity detection using MinHash for finding similar files.
pub struct SimHasher {
    num_hashes: usize,
    hash_seeds: Vec<u64>,
}

impl SimHasher {
    /// Create a new similarity hasher.
    pub fn new(num_hashes: usize) -> Self {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let hash_seeds: Vec<u64> = (0..num_hashes).map(|_| rng.gen()).collect();
        Self {
            num_hashes,
            hash_seeds,
        }
    }

    /// Compute MinHash signature for a set of chunks.
    pub fn minhash(&self, chunks: &[ContentHash]) -> Vec<u64> {
        let mut signature = vec![u64::MAX; self.num_hashes];

        for chunk in chunks {
            for (i, &seed) in self.hash_seeds.iter().enumerate() {
                let hash = self.hash_with_seed(&chunk.0, seed);
                signature[i] = signature[i].min(hash);
            }
        }

        signature
    }

    /// Estimate Jaccard similarity between two signatures.
    pub fn similarity(sig1: &[u64], sig2: &[u64]) -> f64 {
        if sig1.len() != sig2.len() {
            return 0.0;
        }

        let matches = sig1.iter().zip(sig2).filter(|(a, b)| a == b).count();
        matches as f64 / sig1.len() as f64
    }

    fn hash_with_seed(&self, data: &[u8], seed: u64) -> u64 {
        let mut hash = seed;
        for &byte in data {
            hash = hash.wrapping_mul(0x517cc1b727220a95).wrapping_add(byte as u64);
        }
        hash
    }
}

/// Find similar files that might benefit from delta encoding.
#[derive(Debug, Clone)]
pub struct SimilarFile {
    /// File identifier.
    pub file_id: String,
    /// Similarity score (0.0 to 1.0).
    pub similarity: f64,
    /// Number of shared chunks.
    pub shared_chunks: usize,
}

/// Index for finding similar files.
pub struct SimilarityIndex {
    hasher: SimHasher,
    signatures: RwLock<HashMap<String, (Vec<u64>, Vec<ContentHash>)>>,
    similarity_threshold: f64,
}

impl SimilarityIndex {
    /// Create a new similarity index.
    pub fn new(num_hashes: usize, similarity_threshold: f64) -> Self {
        Self {
            hasher: SimHasher::new(num_hashes),
            signatures: RwLock::new(HashMap::new()),
            similarity_threshold,
        }
    }

    /// Add a file to the index.
    pub async fn add(&self, file_id: &str, chunks: Vec<ContentHash>) {
        let signature = self.hasher.minhash(&chunks);
        self.signatures
            .write()
            .await
            .insert(file_id.to_string(), (signature, chunks));
    }

    /// Remove a file from the index.
    pub async fn remove(&self, file_id: &str) {
        self.signatures.write().await.remove(file_id);
    }

    /// Find files similar to the given chunks.
    pub async fn find_similar(&self, chunks: &[ContentHash]) -> Vec<SimilarFile> {
        let query_sig = self.hasher.minhash(chunks);
        let query_set: std::collections::HashSet<_> = chunks.iter().collect();

        let signatures = self.signatures.read().await;
        let mut results = Vec::new();

        for (file_id, (sig, file_chunks)) in signatures.iter() {
            let similarity = SimHasher::similarity(&query_sig, sig);

            if similarity >= self.similarity_threshold {
                let file_set: std::collections::HashSet<_> = file_chunks.iter().collect();
                let shared = query_set.intersection(&file_set).count();

                results.push(SimilarFile {
                    file_id: file_id.clone(),
                    similarity,
                    shared_chunks: shared,
                });
            }
        }

        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap());
        results
    }
}

// Hex encoding helper (since we don't have the hex crate)
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> std::result::Result<Vec<u8>, String> {
        if s.len() % 2 != 0 {
            return Err("Invalid hex string length".into());
        }

        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_hash() {
        let data = b"hello world";
        let chunk = Chunk::new(data);
        let hex = chunk.hash.to_hex();
        assert_eq!(hex.len(), 64); // SHA-256 = 32 bytes = 64 hex chars

        let parsed = ContentHash::from_hex(&hex).unwrap();
        assert_eq!(parsed, chunk.hash);
    }

    #[test]
    fn test_chunker_config_presets() {
        let high_dedup = ChunkerConfig::high_dedup();
        assert_eq!(high_dedup.avg_chunk_size, 8 * 1024);

        let high_throughput = ChunkerConfig::high_throughput();
        assert_eq!(high_throughput.avg_chunk_size, 64 * 1024);
    }

    #[test]
    fn test_rabin_chunker_basic() {
        let config = ChunkerConfig::default();
        let chunker = RabinChunker::new(config);

        // Small data should be one chunk
        let small_data = vec![0u8; 1000];
        let chunks = chunker.chunk(&small_data);
        assert_eq!(chunks.len(), 1);

        // Large data should be multiple chunks
        let large_data = vec![0u8; 100_000];
        let chunks = chunker.chunk(&large_data);
        assert!(chunks.len() > 1);

        // Total size should match
        let total: usize = chunks.iter().map(|c| c.size).sum();
        assert_eq!(total, large_data.len());
    }

    #[test]
    fn test_rabin_chunker_deterministic() {
        let config = ChunkerConfig::default();
        let chunker = RabinChunker::new(config);

        let data = vec![42u8; 50_000];
        let chunks1 = chunker.chunk(&data);
        let chunks2 = chunker.chunk(&data);

        assert_eq!(chunks1.len(), chunks2.len());
        for (c1, c2) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(c1.hash, c2.hash);
            assert_eq!(c1.size, c2.size);
        }
    }

    #[test]
    fn test_chunk_creation() {
        let data = b"test data for chunking";
        let chunk = Chunk::new(data);

        assert_eq!(chunk.size, data.len());
        assert!(chunk.data.is_some());
        assert_eq!(chunk.data.as_ref().unwrap(), data);
    }

    #[tokio::test]
    async fn test_memory_chunk_store() {
        let store = MemoryChunkStore::new();
        let hash = ContentHash([1u8; 32]);
        let data = b"test data";

        // Put and get
        store.put(&hash, data).await.unwrap();
        assert!(store.exists(&hash).await.unwrap());

        let retrieved = store.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, data);

        // Reference counting
        assert_eq!(store.ref_count(&hash).await.unwrap(), 0);
        store.add_ref(&hash).await.unwrap();
        assert_eq!(store.ref_count(&hash).await.unwrap(), 1);
        store.add_ref(&hash).await.unwrap();
        assert_eq!(store.ref_count(&hash).await.unwrap(), 2);
        store.remove_ref(&hash).await.unwrap();
        assert_eq!(store.ref_count(&hash).await.unwrap(), 1);

        // Delete
        store.delete(&hash).await.unwrap();
        assert!(!store.exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn test_dedup_engine_basic() {
        let store = Arc::new(MemoryChunkStore::new());
        let config = DedupConfig::default();
        let engine = DedupEngine::new(config, store);

        let data = vec![42u8; 50_000];
        let recipe = engine.process(&data).await.unwrap();

        assert_eq!(recipe.total_size, data.len() as u64);
        assert!(!recipe.chunks.is_empty());

        // Reassemble
        let reassembled = engine.reassemble(&recipe).await.unwrap();
        assert_eq!(reassembled, data);
    }

    #[tokio::test]
    async fn test_dedup_engine_deduplication() {
        let store = Arc::new(MemoryChunkStore::new());
        let config = DedupConfig::default();
        let engine = DedupEngine::new(config, store);

        // Process same data twice
        let data = vec![42u8; 50_000];
        let recipe1 = engine.process(&data).await.unwrap();
        let recipe2 = engine.process(&data).await.unwrap();

        // Should have same chunks
        assert_eq!(recipe1.chunks.len(), recipe2.chunks.len());
        for (c1, c2) in recipe1.chunks.iter().zip(recipe2.chunks.iter()) {
            assert_eq!(c1.hash, c2.hash);
        }

        // Stats should show deduplication
        let stats = engine.stats();
        assert!(stats.chunks_deduplicated > 0);
    }

    #[tokio::test]
    async fn test_dedup_engine_release() {
        let store = Arc::new(MemoryChunkStore::new());
        let config = DedupConfig::default();
        let engine = DedupEngine::new(config, store.clone());

        let data = vec![42u8; 50_000];
        let recipe = engine.process(&data).await.unwrap();

        // All chunks should have ref_count = 1
        for chunk_ref in &recipe.chunks {
            assert_eq!(store.ref_count(&chunk_ref.hash).await.unwrap(), 1);
        }

        // Release
        let orphans = engine.release(&recipe).await.unwrap();

        // All should be orphaned
        assert_eq!(orphans.len(), recipe.chunks.len());
    }

    #[test]
    fn test_dedup_stats() {
        let stats = DedupStats {
            bytes_processed: 1000,
            bytes_stored: 500,
            chunks_created: 10,
            chunks_deduplicated: 5,
            bytes_saved: 500,
        };

        assert_eq!(stats.dedup_ratio(), 2.0);
        assert_eq!(stats.efficiency_percent(), 50.0);
    }

    #[test]
    fn test_simhasher() {
        let hasher = SimHasher::new(128);

        let chunks1 = vec![
            ContentHash([1u8; 32]),
            ContentHash([2u8; 32]),
            ContentHash([3u8; 32]),
        ];

        let chunks2 = vec![
            ContentHash([1u8; 32]),
            ContentHash([2u8; 32]),
            ContentHash([4u8; 32]),
        ];

        let sig1 = hasher.minhash(&chunks1);
        let sig2 = hasher.minhash(&chunks2);

        let similarity = SimHasher::similarity(&sig1, &sig2);
        assert!(similarity > 0.0 && similarity < 1.0);

        // Same chunks should have similarity 1.0
        let sig3 = hasher.minhash(&chunks1);
        assert_eq!(SimHasher::similarity(&sig1, &sig3), 1.0);
    }

    #[tokio::test]
    async fn test_similarity_index() {
        let index = SimilarityIndex::new(128, 0.5);

        let chunks1 = vec![
            ContentHash([1u8; 32]),
            ContentHash([2u8; 32]),
            ContentHash([3u8; 32]),
        ];

        let chunks2 = vec![
            ContentHash([1u8; 32]),
            ContentHash([2u8; 32]),
            ContentHash([4u8; 32]),
        ];

        let chunks3 = vec![
            ContentHash([10u8; 32]),
            ContentHash([11u8; 32]),
            ContentHash([12u8; 32]),
        ];

        index.add("file1", chunks1.clone()).await;
        index.add("file2", chunks2).await;
        index.add("file3", chunks3).await;

        // Find similar to chunks1
        let similar = index.find_similar(&chunks1).await;

        // Should find file1 (exact match) and file2 (similar)
        assert!(similar.iter().any(|s| s.file_id == "file1"));
        // file3 should not match (different chunks)
        let file3_match = similar.iter().find(|s| s.file_id == "file3");
        assert!(file3_match.is_none() || file3_match.unwrap().similarity < 0.5);
    }
}
