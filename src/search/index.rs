// Search Index Management

use super::analyzer::{Analyzer, TextAnalyzer};
#[allow(unused_imports)]
use super::document::{Document, Field, FieldType, Schema};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Index name
    pub name: String,
    /// Number of shards
    pub num_shards: u32,
    /// Number of replicas
    pub num_replicas: u32,
    /// Refresh interval in milliseconds
    pub refresh_interval_ms: u64,
    /// Maximum documents per segment
    pub max_docs_per_segment: usize,
    /// Enable term positions
    pub positions: bool,
    /// Enable term vectors
    pub term_vectors: bool,
    /// Enable real-time indexing
    pub real_time: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            num_shards: 1,
            num_replicas: 0,
            refresh_interval_ms: 1000,
            max_docs_per_segment: 100000,
            positions: true,
            term_vectors: false,
            real_time: true,
        }
    }
}

/// Inverted index entry
#[derive(Debug, Clone)]
struct PostingsList {
    /// Document frequency
    doc_freq: u32,
    /// Postings: doc_id -> (term_freq, positions)
    postings: HashMap<String, (u32, Vec<u32>)>,
}

impl PostingsList {
    fn new() -> Self {
        Self {
            doc_freq: 0,
            postings: HashMap::new(),
        }
    }

    fn add_posting(&mut self, doc_id: &str, position: u32) {
        let entry = self.postings.entry(doc_id.to_string()).or_insert((0, Vec::new()));
        entry.0 += 1; // Increment term frequency
        entry.1.push(position); // Add position

        // Update doc frequency only for new documents
        if entry.0 == 1 {
            self.doc_freq += 1;
        }
    }

    fn remove_document(&mut self, doc_id: &str) {
        if self.postings.remove(doc_id).is_some() {
            self.doc_freq = self.doc_freq.saturating_sub(1);
        }
    }
}

/// Search index
pub struct SearchIndex {
    /// Configuration
    config: IndexConfig,
    /// Schema
    schema: Schema,
    /// Analyzer
    analyzer: Arc<dyn Analyzer>,
    /// Document store
    documents: Arc<RwLock<HashMap<String, Document>>>,
    /// Inverted index: field -> term -> postings
    inverted_index: Arc<RwLock<HashMap<String, HashMap<String, PostingsList>>>>,
    /// Field lengths: field -> doc_id -> length
    field_lengths: Arc<RwLock<HashMap<String, HashMap<String, u32>>>>,
    /// Numeric index: field -> value -> doc_ids
    numeric_index: Arc<RwLock<HashMap<String, BTreeMap<i64, HashSet<String>>>>>,
    /// Statistics
    stats: Arc<IndexStats>,
}

/// Index statistics
pub struct IndexStats {
    /// Total documents
    pub doc_count: AtomicU64,
    /// Total terms
    pub term_count: AtomicU64,
    /// Total indexed bytes
    pub indexed_bytes: AtomicU64,
    /// Index operations
    pub index_ops: AtomicU64,
    /// Search operations
    pub search_ops: AtomicU64,
    /// Delete operations
    pub delete_ops: AtomicU64,
}

impl Default for IndexStats {
    fn default() -> Self {
        Self {
            doc_count: AtomicU64::new(0),
            term_count: AtomicU64::new(0),
            indexed_bytes: AtomicU64::new(0),
            index_ops: AtomicU64::new(0),
            search_ops: AtomicU64::new(0),
            delete_ops: AtomicU64::new(0),
        }
    }
}

impl SearchIndex {
    /// Creates a new search index
    pub fn new(config: IndexConfig, schema: Schema) -> Self {
        Self {
            config,
            schema,
            analyzer: Arc::new(TextAnalyzer::default_analyzer()),
            documents: Arc::new(RwLock::new(HashMap::new())),
            inverted_index: Arc::new(RwLock::new(HashMap::new())),
            field_lengths: Arc::new(RwLock::new(HashMap::new())),
            numeric_index: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(IndexStats::default()),
        }
    }

    /// Creates with custom analyzer
    pub fn with_analyzer(mut self, analyzer: Arc<dyn Analyzer>) -> Self {
        self.analyzer = analyzer;
        self
    }

    /// Indexes a document
    pub async fn index(&self, doc: Document) -> Result<()> {
        let doc_id = doc.id.clone();
        let mut indexed_bytes = 0u64;

        // Remove old document if exists
        if self.documents.read().await.contains_key(&doc_id) {
            self.delete(&doc_id).await?;
        }

        // Index each field
        for field_def in &self.schema.fields {
            if !field_def.indexed {
                continue;
            }

            if let Some(value) = doc.fields.get(&field_def.name) {
                match field_def.field_type {
                    FieldType::Text => {
                        if let Some(text) = value.as_str() {
                            self.index_text_field(&doc_id, &field_def.name, text).await;
                            indexed_bytes += text.len() as u64;
                        }
                    }
                    FieldType::Keyword => {
                        if let Some(text) = value.as_str() {
                            self.index_keyword_field(&doc_id, &field_def.name, text).await;
                            indexed_bytes += text.len() as u64;
                        }
                    }
                    FieldType::Integer | FieldType::Long => {
                        if let Some(num) = value.as_i64() {
                            self.index_numeric_field(&doc_id, &field_def.name, num).await;
                        }
                    }
                    FieldType::Float | FieldType::Double => {
                        if let Some(num) = value.as_f64() {
                            // Store as scaled integer for BTree indexing
                            let scaled = (num * 1000000.0) as i64;
                            self.index_numeric_field(&doc_id, &field_def.name, scaled).await;
                        }
                    }
                    FieldType::Boolean => {
                        if let Some(b) = value.as_bool() {
                            let text = if b { "true" } else { "false" };
                            self.index_keyword_field(&doc_id, &field_def.name, text).await;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Store document
        {
            let mut docs = self.documents.write().await;
            docs.insert(doc_id, doc);
        }

        // Update stats
        self.stats.doc_count.fetch_add(1, Ordering::Relaxed);
        self.stats.indexed_bytes.fetch_add(indexed_bytes, Ordering::Relaxed);
        self.stats.index_ops.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Indexes a text field
    async fn index_text_field(&self, doc_id: &str, field: &str, text: &str) {
        let tokens = self.analyzer.analyze_for_index(text);
        let mut inverted = self.inverted_index.write().await;
        let mut lengths = self.field_lengths.write().await;

        let field_index = inverted.entry(field.to_string()).or_insert_with(HashMap::new);

        for (position, token) in tokens.iter().enumerate() {
            let postings = field_index.entry(token.clone()).or_insert_with(PostingsList::new);
            postings.add_posting(doc_id, position as u32);
        }

        // Store field length for BM25
        let field_lengths = lengths.entry(field.to_string()).or_insert_with(HashMap::new);
        field_lengths.insert(doc_id.to_string(), tokens.len() as u32);

        self.stats.term_count.fetch_add(tokens.len() as u64, Ordering::Relaxed);
    }

    /// Indexes a keyword field (no analysis)
    async fn index_keyword_field(&self, doc_id: &str, field: &str, value: &str) {
        let mut inverted = self.inverted_index.write().await;
        let field_index = inverted.entry(field.to_string()).or_insert_with(HashMap::new);

        let postings = field_index.entry(value.to_lowercase()).or_insert_with(PostingsList::new);
        postings.add_posting(doc_id, 0);
    }

    /// Indexes a numeric field
    async fn index_numeric_field(&self, doc_id: &str, field: &str, value: i64) {
        let mut numeric = self.numeric_index.write().await;
        let field_index = numeric.entry(field.to_string()).or_insert_with(BTreeMap::new);

        field_index
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert(doc_id.to_string());
    }

    /// Deletes a document
    pub async fn delete(&self, doc_id: &str) -> Result<bool> {
        // Remove from document store
        let doc = {
            let mut docs = self.documents.write().await;
            docs.remove(doc_id)
        };

        if doc.is_none() {
            return Ok(false);
        }

        // Remove from inverted index
        {
            let mut inverted = self.inverted_index.write().await;
            for field_index in inverted.values_mut() {
                for postings in field_index.values_mut() {
                    postings.remove_document(doc_id);
                }
            }
        }

        // Remove from field lengths
        {
            let mut lengths = self.field_lengths.write().await;
            for field_lengths in lengths.values_mut() {
                field_lengths.remove(doc_id);
            }
        }

        // Remove from numeric index
        {
            let mut numeric = self.numeric_index.write().await;
            for field_index in numeric.values_mut() {
                for doc_ids in field_index.values_mut() {
                    doc_ids.remove(doc_id);
                }
            }
        }

        self.stats.doc_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.delete_ops.fetch_add(1, Ordering::Relaxed);

        Ok(true)
    }

    /// Gets a document by ID
    pub async fn get(&self, doc_id: &str) -> Option<Document> {
        let docs = self.documents.read().await;
        docs.get(doc_id).cloned()
    }

    /// Searches for documents matching a term
    pub async fn search_term(&self, field: &str, term: &str) -> Vec<String> {
        let tokens = self.analyzer.analyze_for_query(term);
        if tokens.is_empty() {
            return Vec::new();
        }

        let inverted = self.inverted_index.read().await;
        let token = &tokens[0];

        if let Some(field_index) = inverted.get(field) {
            if let Some(postings) = field_index.get(token) {
                return postings.postings.keys().cloned().collect();
            }
        }

        Vec::new()
    }

    /// Searches with prefix matching
    pub async fn search_prefix(&self, field: &str, prefix: &str) -> Vec<String> {
        let tokens = self.analyzer.analyze_for_query(prefix);
        if tokens.is_empty() {
            return Vec::new();
        }

        let inverted = self.inverted_index.read().await;
        let prefix_token = &tokens[0];
        let mut results = HashSet::new();

        if let Some(field_index) = inverted.get(field) {
            for (term, postings) in field_index {
                if term.starts_with(prefix_token) {
                    results.extend(postings.postings.keys().cloned());
                }
            }
        }

        results.into_iter().collect()
    }

    /// Searches with wildcard matching
    pub async fn search_wildcard(&self, field: &str, pattern: &str) -> Vec<String> {
        let inverted = self.inverted_index.read().await;
        let mut results = HashSet::new();

        let pattern_lower = pattern.to_lowercase();

        if let Some(field_index) = inverted.get(field) {
            for (term, postings) in field_index {
                if wildcard_match(&pattern_lower, term) {
                    results.extend(postings.postings.keys().cloned());
                }
            }
        }

        results.into_iter().collect()
    }

    /// Searches with fuzzy matching
    pub async fn search_fuzzy(&self, field: &str, term: &str, max_edits: usize) -> Vec<String> {
        let tokens = self.analyzer.analyze_for_query(term);
        if tokens.is_empty() {
            return Vec::new();
        }

        let inverted = self.inverted_index.read().await;
        let query_token = &tokens[0];
        let mut results = HashSet::new();

        if let Some(field_index) = inverted.get(field) {
            for (term, postings) in field_index {
                if levenshtein_distance(query_token, term) <= max_edits {
                    results.extend(postings.postings.keys().cloned());
                }
            }
        }

        results.into_iter().collect()
    }

    /// Range search for numeric fields
    pub async fn search_range(&self, field: &str, min: Option<i64>, max: Option<i64>) -> Vec<String> {
        let numeric = self.numeric_index.read().await;
        let mut results = HashSet::new();

        if let Some(field_index) = numeric.get(field) {
            let range = match (min, max) {
                (Some(min), Some(max)) => field_index.range(min..=max),
                (Some(min), None) => field_index.range(min..),
                (None, Some(max)) => field_index.range(..=max),
                (None, None) => return field_index.values().flatten().cloned().collect(),
            };

            for (_, doc_ids) in range {
                results.extend(doc_ids.iter().cloned());
            }
        }

        results.into_iter().collect()
    }

    /// Gets posting list for a term
    pub async fn get_postings(&self, field: &str, term: &str) -> Option<Vec<(String, u32, Vec<u32>)>> {
        let tokens = self.analyzer.analyze_for_query(term);
        if tokens.is_empty() {
            return None;
        }

        let inverted = self.inverted_index.read().await;
        let token = &tokens[0];

        if let Some(field_index) = inverted.get(field) {
            if let Some(postings) = field_index.get(token) {
                let result: Vec<_> = postings.postings
                    .iter()
                    .map(|(doc_id, (freq, positions))| {
                        (doc_id.clone(), *freq, positions.clone())
                    })
                    .collect();
                return Some(result);
            }
        }

        None
    }

    /// Gets document frequency for a term
    pub async fn get_doc_freq(&self, field: &str, term: &str) -> u32 {
        let tokens = self.analyzer.analyze_for_query(term);
        if tokens.is_empty() {
            return 0;
        }

        let inverted = self.inverted_index.read().await;
        let token = &tokens[0];

        if let Some(field_index) = inverted.get(field) {
            if let Some(postings) = field_index.get(token) {
                return postings.doc_freq;
            }
        }

        0
    }

    /// Gets field length for a document
    pub async fn get_field_length(&self, field: &str, doc_id: &str) -> u32 {
        let lengths = self.field_lengths.read().await;

        if let Some(field_lengths) = lengths.get(field) {
            return *field_lengths.get(doc_id).unwrap_or(&0);
        }

        0
    }

    /// Gets average field length
    pub async fn get_avg_field_length(&self, field: &str) -> f32 {
        let lengths = self.field_lengths.read().await;

        if let Some(field_lengths) = lengths.get(field) {
            if field_lengths.is_empty() {
                return 0.0;
            }
            let total: u32 = field_lengths.values().sum();
            return total as f32 / field_lengths.len() as f32;
        }

        0.0
    }

    /// Gets total document count
    pub fn doc_count(&self) -> u64 {
        self.stats.doc_count.load(Ordering::Relaxed)
    }

    /// Gets index statistics
    pub fn stats(&self) -> IndexStatsSnapshot {
        IndexStatsSnapshot {
            doc_count: self.stats.doc_count.load(Ordering::Relaxed),
            term_count: self.stats.term_count.load(Ordering::Relaxed),
            indexed_bytes: self.stats.indexed_bytes.load(Ordering::Relaxed),
            index_ops: self.stats.index_ops.load(Ordering::Relaxed),
            search_ops: self.stats.search_ops.load(Ordering::Relaxed),
            delete_ops: self.stats.delete_ops.load(Ordering::Relaxed),
        }
    }

    /// Gets all documents
    pub async fn all_docs(&self) -> Vec<Document> {
        let docs = self.documents.read().await;
        docs.values().cloned().collect()
    }

    /// Gets document count
    pub async fn count(&self) -> usize {
        self.documents.read().await.len()
    }

    /// Gets schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Gets config
    pub fn config(&self) -> &IndexConfig {
        &self.config
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatsSnapshot {
    pub doc_count: u64,
    pub term_count: u64,
    pub indexed_bytes: u64,
    pub index_ops: u64,
    pub search_ops: u64,
    pub delete_ops: u64,
}

/// Simple wildcard matching
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                if pattern_chars.peek().is_none() {
                    return true;
                }
                let remaining: String = pattern_chars.collect();
                let remaining_text: String = text_chars.collect();
                for i in 0..=remaining_text.len() {
                    if wildcard_match(&remaining, &remaining_text[i..]) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

/// Levenshtein edit distance
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let m = s1_chars.len();
    let n = s2_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::new("test")
            .field(Field::keyword("id").required())
            .field(Field::text("title").with_boost(2.0))
            .field(Field::text("content"))
            .field(Field::integer("size"))
    }

    #[tokio::test]
    async fn test_index_and_search() {
        let config = IndexConfig::default();
        let schema = test_schema();
        let index = SearchIndex::new(config, schema);

        // Index documents
        let doc1 = Document::new("1")
            .field("title", "Hello World")
            .field("content", "This is a test document");

        let doc2 = Document::new("2")
            .field("title", "Goodbye World")
            .field("content", "Another test document here");

        index.index(doc1).await.unwrap();
        index.index(doc2).await.unwrap();

        assert_eq!(index.doc_count(), 2);

        // Search
        let results = index.search_term("title", "hello").await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "1");

        let results = index.search_term("title", "world").await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_prefix_search() {
        let config = IndexConfig::default();
        let schema = test_schema();
        let index = SearchIndex::new(config, schema);

        index.index(Document::new("1").field("title", "testing")).await.unwrap();
        index.index(Document::new("2").field("title", "tested")).await.unwrap();
        index.index(Document::new("3").field("title", "other")).await.unwrap();

        let results = index.search_prefix("title", "test").await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_fuzzy_search() {
        let config = IndexConfig::default();
        let schema = test_schema();
        let index = SearchIndex::new(config, schema);

        index.index(Document::new("1").field("title", "hello")).await.unwrap();
        index.index(Document::new("2").field("title", "hallo")).await.unwrap();
        index.index(Document::new("3").field("title", "world")).await.unwrap();

        let results = index.search_fuzzy("title", "helo", 1).await;
        assert_eq!(results.len(), 1); // Should match "hello"

        let results = index.search_fuzzy("title", "helo", 2).await;
        assert_eq!(results.len(), 2); // Should match "hello" and "hallo"
    }

    #[tokio::test]
    async fn test_delete() {
        let config = IndexConfig::default();
        let schema = test_schema();
        let index = SearchIndex::new(config, schema);

        index.index(Document::new("1").field("title", "test")).await.unwrap();
        assert_eq!(index.doc_count(), 1);

        index.delete("1").await.unwrap();
        assert_eq!(index.doc_count(), 0);

        let results = index.search_term("title", "test").await;
        assert!(results.is_empty());
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("hello", "helo"), 1);
        assert_eq!(levenshtein_distance("hello", "hallo"), 1);
        assert_eq!(levenshtein_distance("hello", "world"), 4);
    }

    #[test]
    fn test_wildcard() {
        assert!(wildcard_match("hel*", "hello"));
        assert!(wildcard_match("*lo", "hello"));
        assert!(wildcard_match("h*o", "hello"));
        assert!(wildcard_match("h?llo", "hello"));
        assert!(!wildcard_match("hel*", "goodbye"));
    }
}
