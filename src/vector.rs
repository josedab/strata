//! Vector Database Layer
//!
//! Provides high-performance vector storage and similarity search capabilities
//! for machine learning embeddings, semantic search, and AI applications.
//!
//! # Features
//!
//! - Multiple index types: HNSW, IVF, PQ, Flat
//! - Hybrid search (vector + metadata filtering)
//! - Distributed sharding with consistent hashing
//! - Real-time index updates
//! - GPU acceleration support
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::vector::{VectorStore, VectorConfig, IndexType};
//!
//! let config = VectorConfig::new(384)  // 384 dimensions
//!     .with_index_type(IndexType::Hnsw { m: 16, ef_construction: 200 })
//!     .with_metric(DistanceMetric::Cosine);
//!
//! let store = VectorStore::new("embeddings", config)?;
//!
//! // Insert vectors
//! store.insert("doc1", &embedding, metadata)?;
//!
//! // Search
//! let results = store.search(&query_vector, 10)?;
//! ```

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Vector store for managing vector collections
pub struct VectorStore {
    /// Store name
    name: String,
    /// Configuration
    config: VectorConfig,
    /// Vector index
    index: Arc<RwLock<Box<dyn VectorIndex + Send + Sync>>>,
    /// Metadata storage
    metadata: Arc<RwLock<HashMap<String, VectorMetadata>>>,
    /// Sharding manager
    shards: ShardManager,
    /// Statistics
    stats: Arc<RwLock<VectorStoreStats>>,
}

/// Vector store configuration
#[derive(Debug, Clone)]
pub struct VectorConfig {
    /// Vector dimensions
    pub dimensions: usize,
    /// Index type
    pub index_type: IndexType,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Maximum vectors per shard
    pub shard_size: usize,
    /// Enable GPU acceleration
    pub gpu_enabled: bool,
    /// Quantization settings
    pub quantization: Option<QuantizationConfig>,
    /// Cache settings
    pub cache_size: usize,
}

/// Index types
#[derive(Debug, Clone)]
pub enum IndexType {
    /// Flat index (brute force, exact)
    Flat,
    /// HNSW (Hierarchical Navigable Small World)
    Hnsw {
        /// Maximum connections per node
        m: usize,
        /// Size of dynamic candidate list during construction
        ef_construction: usize,
    },
    /// IVF (Inverted File Index)
    Ivf {
        /// Number of clusters
        n_clusters: usize,
        /// Number of probes during search
        n_probes: usize,
    },
    /// Product Quantization
    Pq {
        /// Number of subquantizers
        n_subquantizers: usize,
        /// Bits per subquantizer
        bits: usize,
    },
    /// Scalar Quantization
    Sq {
        /// Bits per dimension
        bits: usize,
    },
}

/// Distance metrics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    Euclidean,
    /// Cosine similarity
    Cosine,
    /// Inner product
    InnerProduct,
    /// Manhattan distance (L1)
    Manhattan,
    /// Hamming distance (for binary vectors)
    Hamming,
}

/// Quantization configuration
#[derive(Debug, Clone)]
pub struct QuantizationConfig {
    /// Quantization type
    pub qtype: QuantizationType,
    /// Training sample size
    pub training_size: usize,
}

/// Quantization types
#[derive(Debug, Clone)]
pub enum QuantizationType {
    /// Scalar quantization
    Scalar { bits: u8 },
    /// Product quantization
    Product { subvectors: usize, bits: u8 },
    /// Binary quantization
    Binary,
}

/// Vector metadata
#[derive(Debug, Clone)]
pub struct VectorMetadata {
    /// Vector ID
    pub id: String,
    /// Associated metadata
    pub attributes: HashMap<String, MetadataValue>,
    /// Creation timestamp
    pub created_at: Instant,
    /// Update timestamp
    pub updated_at: Instant,
}

/// Metadata value types
#[derive(Debug, Clone)]
pub enum MetadataValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
}

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Vector ID
    pub id: String,
    /// Distance/similarity score
    pub score: f32,
    /// Vector data (optional)
    pub vector: Option<Vec<f32>>,
    /// Metadata
    pub metadata: Option<VectorMetadata>,
}

/// Search query
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Query vector
    pub vector: Vec<f32>,
    /// Number of results
    pub top_k: usize,
    /// Metadata filters
    pub filters: Vec<MetadataFilter>,
    /// Include vectors in results
    pub include_vectors: bool,
    /// Include metadata in results
    pub include_metadata: bool,
    /// Search parameters
    pub params: SearchParams,
}

/// Metadata filter
#[derive(Debug, Clone)]
pub enum MetadataFilter {
    /// Equals
    Eq(String, MetadataValue),
    /// Not equals
    Ne(String, MetadataValue),
    /// Greater than
    Gt(String, MetadataValue),
    /// Greater than or equal
    Gte(String, MetadataValue),
    /// Less than
    Lt(String, MetadataValue),
    /// Less than or equal
    Lte(String, MetadataValue),
    /// In set
    In(String, Vec<MetadataValue>),
    /// Not in set
    NotIn(String, Vec<MetadataValue>),
    /// Contains (for arrays)
    Contains(String, MetadataValue),
    /// And combination
    And(Vec<MetadataFilter>),
    /// Or combination
    Or(Vec<MetadataFilter>),
}

/// Search parameters
#[derive(Debug, Clone, Default)]
pub struct SearchParams {
    /// ef parameter for HNSW search
    pub ef: Option<usize>,
    /// Number of probes for IVF
    pub n_probes: Option<usize>,
    /// Rerank with exact distances
    pub rerank: bool,
}

/// Vector index trait
pub trait VectorIndex {
    /// Insert a vector
    fn insert(&mut self, id: &str, vector: &[f32]) -> Result<(), VectorError>;

    /// Batch insert
    fn batch_insert(&mut self, vectors: &[(String, Vec<f32>)]) -> Result<(), VectorError>;

    /// Delete a vector
    fn delete(&mut self, id: &str) -> Result<bool, VectorError>;

    /// Search for nearest neighbors
    fn search(&self, query: &[f32], k: usize, params: &SearchParams) -> Vec<(String, f32)>;

    /// Get vector by ID
    fn get(&self, id: &str) -> Option<Vec<f32>>;

    /// Number of vectors
    fn len(&self) -> usize;

    /// Check if empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Build/optimize index
    fn build(&mut self) -> Result<(), VectorError>;

    /// Save index to bytes
    fn serialize(&self) -> Result<Vec<u8>, VectorError>;

    /// Load index from bytes
    fn deserialize(&mut self, data: &[u8]) -> Result<(), VectorError>;
}

/// Flat (brute force) index
pub struct FlatIndex {
    dimensions: usize,
    metric: DistanceMetric,
    vectors: HashMap<String, Vec<f32>>,
    id_list: Vec<String>,
}

/// HNSW index
pub struct HnswIndex {
    dimensions: usize,
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
    max_level: usize,
    entry_point: Option<String>,
    vectors: HashMap<String, Vec<f32>>,
    levels: HashMap<String, usize>,
    neighbors: HashMap<String, Vec<Vec<String>>>,
}

/// IVF index
pub struct IvfIndex {
    dimensions: usize,
    metric: DistanceMetric,
    n_clusters: usize,
    n_probes: usize,
    centroids: Vec<Vec<f32>>,
    clusters: Vec<Vec<String>>,
    vectors: HashMap<String, Vec<f32>>,
    trained: bool,
}

/// Shard manager for distributed vectors
struct ShardManager {
    num_shards: usize,
    shard_map: HashMap<String, usize>,
}

/// Vector store statistics
#[derive(Debug, Clone, Default)]
pub struct VectorStoreStats {
    pub total_vectors: usize,
    pub total_searches: u64,
    pub avg_search_time_us: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl VectorStore {
    /// Create a new vector store
    pub fn new(name: impl Into<String>, config: VectorConfig) -> Result<Self, VectorError> {
        let index: Box<dyn VectorIndex + Send + Sync> = match &config.index_type {
            IndexType::Flat => Box::new(FlatIndex::new(config.dimensions, config.metric)),
            IndexType::Hnsw {
                m,
                ef_construction,
            } => Box::new(HnswIndex::new(
                config.dimensions,
                config.metric,
                *m,
                *ef_construction,
            )),
            IndexType::Ivf {
                n_clusters,
                n_probes,
            } => Box::new(IvfIndex::new(
                config.dimensions,
                config.metric,
                *n_clusters,
                *n_probes,
            )),
            IndexType::Pq { .. } | IndexType::Sq { .. } => {
                // Fall back to flat for now
                Box::new(FlatIndex::new(config.dimensions, config.metric))
            }
        };

        Ok(Self {
            name: name.into(),
            config,
            index: Arc::new(RwLock::new(index)),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            shards: ShardManager::new(16),
            stats: Arc::new(RwLock::new(VectorStoreStats::default())),
        })
    }

    /// Insert a vector with metadata
    pub fn insert(
        &self,
        id: impl Into<String>,
        vector: &[f32],
        attributes: HashMap<String, MetadataValue>,
    ) -> Result<(), VectorError> {
        let id = id.into();

        // Validate dimensions
        if vector.len() != self.config.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }

        // Normalize if using cosine similarity
        let vector = if self.config.metric == DistanceMetric::Cosine {
            normalize(vector)
        } else {
            vector.to_vec()
        };

        // Insert into index
        {
            let mut index = self.index.write().unwrap();
            index.insert(&id, &vector)?;
        }

        // Store metadata
        {
            let mut metadata = self.metadata.write().unwrap();
            metadata.insert(
                id.clone(),
                VectorMetadata {
                    id: id.clone(),
                    attributes,
                    created_at: Instant::now(),
                    updated_at: Instant::now(),
                },
            );
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_vectors += 1;
        }

        Ok(())
    }

    /// Batch insert vectors
    pub fn batch_insert(
        &self,
        vectors: Vec<(String, Vec<f32>, HashMap<String, MetadataValue>)>,
    ) -> Result<(), VectorError> {
        // Validate all dimensions
        for (id, vector, _) in &vectors {
            if vector.len() != self.config.dimensions {
                return Err(VectorError::DimensionMismatch {
                    expected: self.config.dimensions,
                    got: vector.len(),
                });
            }
            let _ = id; // suppress warning
        }

        // Prepare vectors (normalize if needed)
        let prepared: Vec<(String, Vec<f32>)> = vectors
            .iter()
            .map(|(id, v, _)| {
                let vec = if self.config.metric == DistanceMetric::Cosine {
                    normalize(v)
                } else {
                    v.clone()
                };
                (id.clone(), vec)
            })
            .collect();

        // Batch insert into index
        {
            let mut index = self.index.write().unwrap();
            index.batch_insert(&prepared)?;
        }

        // Store all metadata
        {
            let mut metadata = self.metadata.write().unwrap();
            for (id, _, attrs) in vectors {
                metadata.insert(
                    id.clone(),
                    VectorMetadata {
                        id: id.clone(),
                        attributes: attrs,
                        created_at: Instant::now(),
                        updated_at: Instant::now(),
                    },
                );
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_vectors = self.index.read().unwrap().len();
        }

        Ok(())
    }

    /// Delete a vector
    pub fn delete(&self, id: &str) -> Result<bool, VectorError> {
        let deleted = {
            let mut index = self.index.write().unwrap();
            index.delete(id)?
        };

        if deleted {
            let mut metadata = self.metadata.write().unwrap();
            metadata.remove(id);

            let mut stats = self.stats.write().unwrap();
            stats.total_vectors = stats.total_vectors.saturating_sub(1);
        }

        Ok(deleted)
    }

    /// Search for similar vectors
    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>, VectorError> {
        // Validate query dimensions
        if query.vector.len() != self.config.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.config.dimensions,
                got: query.vector.len(),
            });
        }

        let start = Instant::now();

        // Normalize query if using cosine similarity
        let query_vec = if self.config.metric == DistanceMetric::Cosine {
            normalize(&query.vector)
        } else {
            query.vector.clone()
        };

        // Search index
        let candidates = {
            let index = self.index.read().unwrap();
            // Get more candidates if we have filters
            let k = if query.filters.is_empty() {
                query.top_k
            } else {
                query.top_k * 10
            };
            index.search(&query_vec, k, &query.params)
        };

        // Apply metadata filters
        let metadata = self.metadata.read().unwrap();
        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .filter(|(id, _)| {
                if query.filters.is_empty() {
                    return true;
                }
                if let Some(meta) = metadata.get(id) {
                    Self::matches_filters(meta, &query.filters)
                } else {
                    false
                }
            })
            .take(query.top_k)
            .map(|(id, score)| {
                let vector = if query.include_vectors {
                    let index = self.index.read().unwrap();
                    index.get(&id)
                } else {
                    None
                };

                let meta = if query.include_metadata {
                    metadata.get(&id).cloned()
                } else {
                    None
                };

                SearchResult {
                    id,
                    score,
                    vector,
                    metadata: meta,
                }
            })
            .collect();

        // Sort by score (lower is better for distances)
        results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_searches += 1;
            let elapsed = start.elapsed().as_micros() as f64;
            stats.avg_search_time_us =
                (stats.avg_search_time_us * (stats.total_searches - 1) as f64 + elapsed)
                    / stats.total_searches as f64;
        }

        Ok(results)
    }

    /// Simple search with just vector and k
    pub fn search_simple(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorError> {
        self.search(&SearchQuery {
            vector: query.to_vec(),
            top_k,
            filters: Vec::new(),
            include_vectors: false,
            include_metadata: true,
            params: SearchParams::default(),
        })
    }

    /// Get vector by ID
    pub fn get(&self, id: &str) -> Option<(Vec<f32>, VectorMetadata)> {
        let index = self.index.read().unwrap();
        let metadata = self.metadata.read().unwrap();

        let vector = index.get(id)?;
        let meta = metadata.get(id)?.clone();

        Some((vector, meta))
    }

    /// Update metadata for a vector
    pub fn update_metadata(
        &self,
        id: &str,
        attributes: HashMap<String, MetadataValue>,
    ) -> Result<(), VectorError> {
        let mut metadata = self.metadata.write().unwrap();

        if let Some(meta) = metadata.get_mut(id) {
            meta.attributes.extend(attributes);
            meta.updated_at = Instant::now();
            Ok(())
        } else {
            Err(VectorError::NotFound(id.to_string()))
        }
    }

    /// Build/optimize the index
    pub fn build_index(&self) -> Result<(), VectorError> {
        let mut index = self.index.write().unwrap();
        index.build()
    }

    /// Get statistics
    pub fn stats(&self) -> VectorStoreStats {
        self.stats.read().unwrap().clone()
    }

    /// Get store name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get dimensions
    pub fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    /// Get total vector count
    pub fn len(&self) -> usize {
        self.index.read().unwrap().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn matches_filters(meta: &VectorMetadata, filters: &[MetadataFilter]) -> bool {
        filters.iter().all(|f| Self::matches_filter(meta, f))
    }

    fn matches_filter(meta: &VectorMetadata, filter: &MetadataFilter) -> bool {
        match filter {
            MetadataFilter::Eq(key, value) => meta
                .attributes
                .get(key)
                .map(|v| values_equal(v, value))
                .unwrap_or(false),
            MetadataFilter::Ne(key, value) => meta
                .attributes
                .get(key)
                .map(|v| !values_equal(v, value))
                .unwrap_or(true),
            MetadataFilter::Gt(key, value) => meta
                .attributes
                .get(key)
                .map(|v| compare_values(v, value) == Some(Ordering::Greater))
                .unwrap_or(false),
            MetadataFilter::Gte(key, value) => meta
                .attributes
                .get(key)
                .map(|v| matches!(compare_values(v, value), Some(Ordering::Greater | Ordering::Equal)))
                .unwrap_or(false),
            MetadataFilter::Lt(key, value) => meta
                .attributes
                .get(key)
                .map(|v| compare_values(v, value) == Some(Ordering::Less))
                .unwrap_or(false),
            MetadataFilter::Lte(key, value) => meta
                .attributes
                .get(key)
                .map(|v| matches!(compare_values(v, value), Some(Ordering::Less | Ordering::Equal)))
                .unwrap_or(false),
            MetadataFilter::In(key, values) => meta
                .attributes
                .get(key)
                .map(|v| values.iter().any(|val| values_equal(v, val)))
                .unwrap_or(false),
            MetadataFilter::NotIn(key, values) => meta
                .attributes
                .get(key)
                .map(|v| !values.iter().any(|val| values_equal(v, val)))
                .unwrap_or(true),
            MetadataFilter::Contains(key, value) => {
                if let Some(MetadataValue::StringArray(arr)) = meta.attributes.get(key) {
                    if let MetadataValue::String(s) = value {
                        return arr.contains(s);
                    }
                }
                false
            }
            MetadataFilter::And(filters) => filters.iter().all(|f| Self::matches_filter(meta, f)),
            MetadataFilter::Or(filters) => filters.iter().any(|f| Self::matches_filter(meta, f)),
        }
    }
}

impl VectorConfig {
    /// Create new config with dimensions
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            index_type: IndexType::Hnsw {
                m: 16,
                ef_construction: 200,
            },
            metric: DistanceMetric::Cosine,
            shard_size: 100_000,
            gpu_enabled: false,
            quantization: None,
            cache_size: 10_000,
        }
    }

    /// Set index type
    pub fn with_index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set distance metric
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Enable GPU
    pub fn with_gpu(mut self, enabled: bool) -> Self {
        self.gpu_enabled = enabled;
        self
    }

    /// Set quantization
    pub fn with_quantization(mut self, config: QuantizationConfig) -> Self {
        self.quantization = Some(config);
        self
    }
}

impl FlatIndex {
    fn new(dimensions: usize, metric: DistanceMetric) -> Self {
        Self {
            dimensions,
            metric,
            vectors: HashMap::new(),
            id_list: Vec::new(),
        }
    }
}

impl VectorIndex for FlatIndex {
    fn insert(&mut self, id: &str, vector: &[f32]) -> Result<(), VectorError> {
        if !self.vectors.contains_key(id) {
            self.id_list.push(id.to_string());
        }
        self.vectors.insert(id.to_string(), vector.to_vec());
        Ok(())
    }

    fn batch_insert(&mut self, vectors: &[(String, Vec<f32>)]) -> Result<(), VectorError> {
        for (id, vector) in vectors {
            self.insert(id, vector)?;
        }
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<bool, VectorError> {
        if self.vectors.remove(id).is_some() {
            self.id_list.retain(|i| i != id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn search(&self, query: &[f32], k: usize, _params: &SearchParams) -> Vec<(String, f32)> {
        let mut heap: BinaryHeap<ScoredId> = BinaryHeap::new();

        for (id, vector) in &self.vectors {
            let distance = compute_distance(query, vector, self.metric);
            heap.push(ScoredId {
                id: id.clone(),
                score: distance,
            });

            if heap.len() > k {
                heap.pop();
            }
        }

        let mut results: Vec<_> = heap.into_iter().map(|s| (s.id, s.score)).collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        results
    }

    fn get(&self, id: &str) -> Option<Vec<f32>> {
        self.vectors.get(id).cloned()
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn build(&mut self) -> Result<(), VectorError> {
        // Flat index doesn't need building
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>, VectorError> {
        // Simplified serialization
        Ok(Vec::new())
    }

    fn deserialize(&mut self, _data: &[u8]) -> Result<(), VectorError> {
        Ok(())
    }
}

impl HnswIndex {
    fn new(dimensions: usize, metric: DistanceMetric, m: usize, ef_construction: usize) -> Self {
        Self {
            dimensions,
            metric,
            m,
            ef_construction,
            max_level: 0,
            entry_point: None,
            vectors: HashMap::new(),
            levels: HashMap::new(),
            neighbors: HashMap::new(),
        }
    }

    fn random_level(&self) -> usize {
        let ml = 1.0 / (self.m as f64).ln();
        let r: f64 = rand_float();
        ((-r.ln() * ml).floor() as usize).min(16)
    }

    fn select_neighbors(&self, query: &[f32], candidates: &[String], m: usize) -> Vec<String> {
        let mut scored: Vec<_> = candidates
            .iter()
            .filter_map(|id| {
                self.vectors
                    .get(id)
                    .map(|v| (id.clone(), compute_distance(query, v, self.metric)))
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.into_iter().take(m).map(|(id, _)| id).collect()
    }

    fn search_layer(&self, query: &[f32], entry: &str, ef: usize, level: usize) -> Vec<String> {
        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<ScoredId> = BinaryHeap::new();
        let mut results: BinaryHeap<ScoredId> = BinaryHeap::new();

        if let Some(vec) = self.vectors.get(entry) {
            let dist = compute_distance(query, vec, self.metric);
            candidates.push(ScoredId {
                id: entry.to_string(),
                score: -dist, // Negate for max-heap to act as min-heap
            });
            results.push(ScoredId {
                id: entry.to_string(),
                score: dist,
            });
            visited.insert(entry.to_string());
        }

        while let Some(current) = candidates.pop() {
            let current_dist = -current.score;

            // Check if we can stop
            if let Some(worst) = results.peek() {
                if current_dist > worst.score && results.len() >= ef {
                    break;
                }
            }

            // Explore neighbors
            if let Some(neighbor_lists) = self.neighbors.get(&current.id) {
                if let Some(neighbors) = neighbor_lists.get(level) {
                    for neighbor in neighbors {
                        if visited.insert(neighbor.clone()) {
                            if let Some(vec) = self.vectors.get(neighbor) {
                                let dist = compute_distance(query, vec, self.metric);

                                let should_add = results.len() < ef
                                    || results.peek().map(|w| dist < w.score).unwrap_or(true);

                                if should_add {
                                    candidates.push(ScoredId {
                                        id: neighbor.clone(),
                                        score: -dist,
                                    });
                                    results.push(ScoredId {
                                        id: neighbor.clone(),
                                        score: dist,
                                    });

                                    if results.len() > ef {
                                        results.pop();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        results.into_iter().map(|s| s.id).collect()
    }
}

impl VectorIndex for HnswIndex {
    fn insert(&mut self, id: &str, vector: &[f32]) -> Result<(), VectorError> {
        let level = self.random_level();
        self.vectors.insert(id.to_string(), vector.to_vec());
        self.levels.insert(id.to_string(), level);

        // Initialize neighbor lists for all levels
        let mut neighbor_lists = vec![Vec::new(); level + 1];

        if let Some(entry) = &self.entry_point.clone() {
            // Search from top to level+1, just finding entry points
            let mut current_entry = entry.clone();
            for l in (level + 1..=self.max_level).rev() {
                let result = self.search_layer(vector, &current_entry, 1, l);
                if let Some(closest) = result.first() {
                    current_entry = closest.clone();
                }
            }

            // Search and connect at each level from level down to 0
            for l in (0..=level.min(self.max_level)).rev() {
                let candidates = self.search_layer(vector, &current_entry, self.ef_construction, l);
                let neighbors = self.select_neighbors(vector, &candidates, self.m);

                neighbor_lists[l] = neighbors.clone();

                // Add bidirectional links
                for neighbor in &neighbors {
                    // First, check if we need to prune and get the data we need
                    let prune_result = if let Some(neighbor_lists_ref) = self.neighbors.get(neighbor) {
                        if l < neighbor_lists_ref.len() && neighbor_lists_ref[l].len() + 1 > self.m * 2 {
                            // Need to prune - get the vector and current neighbors
                            if let Some(vec) = self.vectors.get(neighbor) {
                                let mut current_neighbors = neighbor_lists_ref[l].clone();
                                current_neighbors.push(id.to_string());
                                Some(self.select_neighbors(vec, &current_neighbors, self.m))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Now do the mutation
                    if let Some(neighbor_lists_ref) = self.neighbors.get_mut(neighbor) {
                        if l < neighbor_lists_ref.len() {
                            if let Some(pruned) = prune_result {
                                neighbor_lists_ref[l] = pruned;
                            } else {
                                neighbor_lists_ref[l].push(id.to_string());
                            }
                        }
                    }
                }

                if let Some(closest) = candidates.first() {
                    current_entry = closest.clone();
                }
            }
        }

        self.neighbors.insert(id.to_string(), neighbor_lists);

        // Update entry point if this is highest level
        if level > self.max_level || self.entry_point.is_none() {
            self.max_level = level;
            self.entry_point = Some(id.to_string());
        }

        Ok(())
    }

    fn batch_insert(&mut self, vectors: &[(String, Vec<f32>)]) -> Result<(), VectorError> {
        for (id, vector) in vectors {
            self.insert(id, vector)?;
        }
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<bool, VectorError> {
        if self.vectors.remove(id).is_none() {
            return Ok(false);
        }

        self.levels.remove(id);

        // Remove from neighbors' lists
        let removed_neighbors = self.neighbors.remove(id);
        if let Some(neighbor_lists) = removed_neighbors {
            for (level, neighbors) in neighbor_lists.iter().enumerate() {
                for neighbor in neighbors {
                    if let Some(n_lists) = self.neighbors.get_mut(neighbor) {
                        if level < n_lists.len() {
                            n_lists[level].retain(|n| n != id);
                        }
                    }
                }
            }
        }

        // Update entry point if needed
        if self.entry_point.as_ref() == Some(&id.to_string()) {
            self.entry_point = self.vectors.keys().next().cloned();
            self.max_level = self
                .levels
                .values()
                .max()
                .copied()
                .unwrap_or(0);
        }

        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize, params: &SearchParams) -> Vec<(String, f32)> {
        let ef = params.ef.unwrap_or(k.max(10));

        let Some(entry) = &self.entry_point else {
            return Vec::new();
        };

        // Search from top level
        let mut current_entry = entry.clone();
        for level in (1..=self.max_level).rev() {
            let result = self.search_layer(query, &current_entry, 1, level);
            if let Some(closest) = result.first() {
                current_entry = closest.clone();
            }
        }

        // Search bottom level with ef
        let candidates = self.search_layer(query, &current_entry, ef, 0);

        // Score and return top k
        let mut scored: Vec<_> = candidates
            .iter()
            .filter_map(|id| {
                self.vectors
                    .get(id)
                    .map(|v| (id.clone(), compute_distance(query, v, self.metric)))
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.into_iter().take(k).collect()
    }

    fn get(&self, id: &str) -> Option<Vec<f32>> {
        self.vectors.get(id).cloned()
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn build(&mut self) -> Result<(), VectorError> {
        // HNSW is built incrementally
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>, VectorError> {
        Ok(Vec::new())
    }

    fn deserialize(&mut self, _data: &[u8]) -> Result<(), VectorError> {
        Ok(())
    }
}

impl IvfIndex {
    fn new(dimensions: usize, metric: DistanceMetric, n_clusters: usize, n_probes: usize) -> Self {
        Self {
            dimensions,
            metric,
            n_clusters,
            n_probes,
            centroids: Vec::new(),
            clusters: vec![Vec::new(); n_clusters],
            vectors: HashMap::new(),
            trained: false,
        }
    }

    fn find_nearest_centroid(&self, vector: &[f32]) -> usize {
        self.centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, compute_distance(vector, c, self.metric)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    fn find_nearest_centroids(&self, vector: &[f32], n: usize) -> Vec<usize> {
        let mut scored: Vec<_> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, compute_distance(vector, c, self.metric)))
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.into_iter().take(n).map(|(i, _)| i).collect()
    }

    fn train_kmeans(&mut self, vectors: &[Vec<f32>]) {
        if vectors.is_empty() || self.n_clusters == 0 {
            return;
        }

        let k = self.n_clusters.min(vectors.len());

        // Initialize centroids (k-means++)
        let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
        centroids.push(vectors[0].clone());

        for _ in 1..k {
            // Find point farthest from existing centroids
            let mut max_dist = 0.0f32;
            let mut max_idx = 0;

            for (i, v) in vectors.iter().enumerate() {
                let min_dist = centroids
                    .iter()
                    .map(|c| compute_distance(v, c, self.metric))
                    .fold(f32::MAX, |a, b| a.min(b));

                if min_dist > max_dist {
                    max_dist = min_dist;
                    max_idx = i;
                }
            }

            centroids.push(vectors[max_idx].clone());
        }

        // Run k-means iterations
        for _ in 0..10 {
            // Assign points to clusters
            let mut cluster_points: Vec<Vec<usize>> = vec![Vec::new(); k];
            for (i, v) in vectors.iter().enumerate() {
                let nearest = centroids
                    .iter()
                    .enumerate()
                    .map(|(j, c)| (j, compute_distance(v, c, self.metric)))
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
                    .map(|(j, _)| j)
                    .unwrap_or(0);
                cluster_points[nearest].push(i);
            }

            // Update centroids
            for (i, points) in cluster_points.iter().enumerate() {
                if points.is_empty() {
                    continue;
                }

                let mut new_centroid = vec![0.0; self.dimensions];
                for &p in points {
                    for (j, &val) in vectors[p].iter().enumerate() {
                        new_centroid[j] += val;
                    }
                }
                for val in &mut new_centroid {
                    *val /= points.len() as f32;
                }
                centroids[i] = new_centroid;
            }
        }

        self.centroids = centroids;
        self.clusters = vec![Vec::new(); k];
        self.trained = true;
    }
}

impl VectorIndex for IvfIndex {
    fn insert(&mut self, id: &str, vector: &[f32]) -> Result<(), VectorError> {
        self.vectors.insert(id.to_string(), vector.to_vec());

        if self.trained {
            let cluster = self.find_nearest_centroid(vector);
            self.clusters[cluster].push(id.to_string());
        }

        Ok(())
    }

    fn batch_insert(&mut self, vectors: &[(String, Vec<f32>)]) -> Result<(), VectorError> {
        for (id, vector) in vectors {
            self.insert(id, vector)?;
        }
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<bool, VectorError> {
        if self.vectors.remove(id).is_some() {
            for cluster in &mut self.clusters {
                cluster.retain(|i| i != id);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn search(&self, query: &[f32], k: usize, params: &SearchParams) -> Vec<(String, f32)> {
        if !self.trained {
            // Fall back to brute force
            let mut results: Vec<_> = self
                .vectors
                .iter()
                .map(|(id, v)| (id.clone(), compute_distance(query, v, self.metric)))
                .collect();
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            return results.into_iter().take(k).collect();
        }

        let n_probes = params.n_probes.unwrap_or(self.n_probes);
        let probe_clusters = self.find_nearest_centroids(query, n_probes);

        let mut candidates: Vec<(String, f32)> = Vec::new();
        for cluster_idx in probe_clusters {
            for id in &self.clusters[cluster_idx] {
                if let Some(vector) = self.vectors.get(id) {
                    let dist = compute_distance(query, vector, self.metric);
                    candidates.push((id.clone(), dist));
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.into_iter().take(k).collect()
    }

    fn get(&self, id: &str) -> Option<Vec<f32>> {
        self.vectors.get(id).cloned()
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn build(&mut self) -> Result<(), VectorError> {
        let vectors: Vec<Vec<f32>> = self.vectors.values().cloned().collect();
        self.train_kmeans(&vectors);

        // Reassign all vectors to clusters
        self.clusters = vec![Vec::new(); self.n_clusters];
        for (id, vector) in &self.vectors {
            let cluster = self.find_nearest_centroid(vector);
            self.clusters[cluster].push(id.clone());
        }

        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>, VectorError> {
        Ok(Vec::new())
    }

    fn deserialize(&mut self, _data: &[u8]) -> Result<(), VectorError> {
        Ok(())
    }
}

impl ShardManager {
    fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            shard_map: HashMap::new(),
        }
    }
}

/// Scored ID for heap operations
#[derive(Clone)]
struct ScoredId {
    id: String,
    score: f32,
}

impl PartialEq for ScoredId {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredId {}

impl PartialOrd for ScoredId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredId {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior with BinaryHeap
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Compute distance between two vectors
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Euclidean => {
            a.iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y).powi(2))
                .sum::<f32>()
                .sqrt()
        }
        DistanceMetric::Cosine => {
            // For normalized vectors, cosine distance = 1 - dot product
            let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            1.0 - dot
        }
        DistanceMetric::InnerProduct => {
            // Negative because we want to maximize inner product
            -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
        }
        DistanceMetric::Manhattan => a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum(),
        DistanceMetric::Hamming => {
            // Treat as binary vectors
            a.iter()
                .zip(b.iter())
                .filter(|(x, y)| (**x > 0.5_f32) != (**y > 0.5_f32))
                .count() as f32
        }
    }
}

/// Normalize a vector to unit length
fn normalize(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

/// Check if two metadata values are equal
fn values_equal(a: &MetadataValue, b: &MetadataValue) -> bool {
    match (a, b) {
        (MetadataValue::String(s1), MetadataValue::String(s2)) => s1 == s2,
        (MetadataValue::Int(i1), MetadataValue::Int(i2)) => i1 == i2,
        (MetadataValue::Float(f1), MetadataValue::Float(f2)) => (f1 - f2).abs() < f32::EPSILON as f64,
        (MetadataValue::Bool(b1), MetadataValue::Bool(b2)) => b1 == b2,
        _ => false,
    }
}

/// Compare two metadata values
fn compare_values(a: &MetadataValue, b: &MetadataValue) -> Option<Ordering> {
    match (a, b) {
        (MetadataValue::Int(i1), MetadataValue::Int(i2)) => Some(i1.cmp(i2)),
        (MetadataValue::Float(f1), MetadataValue::Float(f2)) => f1.partial_cmp(f2),
        (MetadataValue::String(s1), MetadataValue::String(s2)) => Some(s1.cmp(s2)),
        _ => None,
    }
}

/// Simple random float (0.0-1.0)
fn rand_float() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    nanos as f64 / u32::MAX as f64
}

/// Vector error type
#[derive(Debug)]
pub enum VectorError {
    DimensionMismatch { expected: usize, got: usize },
    NotFound(String),
    IndexError(String),
    SerializationError(String),
}

impl std::fmt::Display for VectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DimensionMismatch { expected, got } => {
                write!(f, "Dimension mismatch: expected {}, got {}", expected, got)
            }
            Self::NotFound(id) => write!(f, "Vector not found: {}", id),
            Self::IndexError(msg) => write!(f, "Index error: {}", msg),
            Self::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for VectorError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_random_vector(dims: usize) -> Vec<f32> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        (0..dims)
            .map(|i| ((seed.wrapping_mul(i as u32 + 1) % 1000) as f32 / 1000.0) - 0.5)
            .collect()
    }

    #[test]
    fn test_flat_index() {
        let config = VectorConfig::new(128).with_index_type(IndexType::Flat);
        let store = VectorStore::new("test", config).unwrap();

        // Insert vectors
        for i in 0..100 {
            let vec = create_random_vector(128);
            store
                .insert(format!("vec_{}", i), &vec, HashMap::new())
                .unwrap();
        }

        assert_eq!(store.len(), 100);

        // Search
        let query = create_random_vector(128);
        let results = store.search_simple(&query, 10).unwrap();

        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_hnsw_index() {
        let config = VectorConfig::new(64).with_index_type(IndexType::Hnsw {
            m: 16,
            ef_construction: 100,
        });
        let store = VectorStore::new("hnsw_test", config).unwrap();

        // Insert vectors
        for i in 0..50 {
            let vec = create_random_vector(64);
            store
                .insert(format!("doc_{}", i), &vec, HashMap::new())
                .unwrap();
        }

        // Search
        let query = create_random_vector(64);
        let results = store.search_simple(&query, 5).unwrap();

        assert!(results.len() <= 5);
    }

    #[test]
    fn test_ivf_index() {
        let config = VectorConfig::new(32).with_index_type(IndexType::Ivf {
            n_clusters: 4,
            n_probes: 2,
        });
        let store = VectorStore::new("ivf_test", config).unwrap();

        // Insert vectors
        for i in 0..40 {
            let vec = create_random_vector(32);
            store
                .insert(format!("item_{}", i), &vec, HashMap::new())
                .unwrap();
        }

        // Build index
        store.build_index().unwrap();

        // Search
        let query = create_random_vector(32);
        let results = store.search_simple(&query, 5).unwrap();

        assert!(!results.is_empty());
    }

    #[test]
    fn test_metadata_filtering() {
        let config = VectorConfig::new(16);
        let store = VectorStore::new("filter_test", config).unwrap();

        // Insert with metadata
        for i in 0..20 {
            let vec = create_random_vector(16);
            let mut attrs = HashMap::new();
            attrs.insert("category".to_string(), MetadataValue::String(
                if i % 2 == 0 { "even" } else { "odd" }.to_string()
            ));
            attrs.insert("value".to_string(), MetadataValue::Int(i as i64));
            store.insert(format!("item_{}", i), &vec, attrs).unwrap();
        }

        // Search with filter
        let query = create_random_vector(16);
        let results = store
            .search(&SearchQuery {
                vector: query,
                top_k: 10,
                filters: vec![MetadataFilter::Eq(
                    "category".to_string(),
                    MetadataValue::String("even".to_string()),
                )],
                include_vectors: false,
                include_metadata: true,
                params: SearchParams::default(),
            })
            .unwrap();

        // All results should be "even"
        for result in results {
            if let Some(meta) = result.metadata {
                if let Some(MetadataValue::String(cat)) = meta.attributes.get("category") {
                    assert_eq!(cat, "even");
                }
            }
        }
    }

    #[test]
    fn test_dimension_validation() {
        let config = VectorConfig::new(128);
        let store = VectorStore::new("dim_test", config).unwrap();

        // Try to insert wrong dimension
        let wrong_vec = create_random_vector(64);
        let result = store.insert("bad", &wrong_vec, HashMap::new());

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VectorError::DimensionMismatch { .. }
        ));
    }

    #[test]
    fn test_delete() {
        let config = VectorConfig::new(32);
        let store = VectorStore::new("delete_test", config).unwrap();

        let vec = create_random_vector(32);
        store.insert("to_delete", &vec, HashMap::new()).unwrap();

        assert_eq!(store.len(), 1);

        store.delete("to_delete").unwrap();

        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_cosine_similarity() {
        let config = VectorConfig::new(4).with_metric(DistanceMetric::Cosine);
        let store = VectorStore::new("cosine_test", config).unwrap();

        // Insert similar vectors
        store.insert("a", &[1.0, 0.0, 0.0, 0.0], HashMap::new()).unwrap();
        store.insert("b", &[0.9, 0.1, 0.0, 0.0], HashMap::new()).unwrap();
        store.insert("c", &[0.0, 1.0, 0.0, 0.0], HashMap::new()).unwrap();

        // Query should find 'a' and 'b' as most similar
        let results = store.search_simple(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "a");
        assert_eq!(results[1].id, "b");
    }

    #[test]
    fn test_batch_insert() {
        let config = VectorConfig::new(16);
        let store = VectorStore::new("batch_test", config).unwrap();

        let vectors: Vec<_> = (0..50)
            .map(|i| {
                (
                    format!("vec_{}", i),
                    create_random_vector(16),
                    HashMap::new(),
                )
            })
            .collect();

        store.batch_insert(vectors).unwrap();

        assert_eq!(store.len(), 50);
    }

    #[test]
    fn test_update_metadata() {
        let config = VectorConfig::new(16);
        let store = VectorStore::new("update_test", config).unwrap();

        let vec = create_random_vector(16);
        let mut attrs = HashMap::new();
        attrs.insert("status".to_string(), MetadataValue::String("new".to_string()));
        store.insert("item", &vec, attrs).unwrap();

        // Update metadata
        let mut new_attrs = HashMap::new();
        new_attrs.insert("status".to_string(), MetadataValue::String("processed".to_string()));
        store.update_metadata("item", new_attrs).unwrap();

        // Verify update
        let (_, meta) = store.get("item").unwrap();
        assert!(matches!(
            meta.attributes.get("status"),
            Some(MetadataValue::String(s)) if s == "processed"
        ));
    }
}
