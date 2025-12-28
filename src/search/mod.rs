//! Full-Text Search for Strata
//!
//! Provides comprehensive search capabilities:
//! - Full-text indexing and search
//! - Metadata and tag search
//! - Fuzzy matching and wildcards
//! - Faceted search and aggregations
//! - Real-time index updates

pub mod analyzer;
pub mod document;
pub mod index;
pub mod query;
pub mod ranking;

pub use analyzer::{Analyzer, AnalyzerConfig, TextAnalyzer, TokenFilter};
pub use document::{Document, Field, FieldType, FieldValue};
pub use index::{SearchIndex, IndexConfig, IndexStats};
pub use query::{Query, QueryParser, SearchRequest, SearchResponse};
pub use ranking::{Ranker, RankingConfig, ScoredDocument};
