// Query Parsing and Execution for Full-Text Search

use super::document::Document;
use super::index::SearchIndex;
use super::ranking::{Ranker, RankingConfig, ScoredDocument};
use crate::error::Result;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Query types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Query {
    /// Match all documents
    MatchAll,

    /// Term query (exact match)
    Term {
        field: String,
        value: String,
    },

    /// Match query (analyzed)
    Match {
        field: String,
        query: String,
        #[serde(default)]
        operator: MatchOperator,
    },

    /// Multi-match query
    MultiMatch {
        fields: Vec<String>,
        query: String,
        #[serde(default)]
        operator: MatchOperator,
    },

    /// Phrase query
    Phrase {
        field: String,
        phrase: String,
        #[serde(default)]
        slop: u32,
    },

    /// Prefix query
    Prefix {
        field: String,
        prefix: String,
    },

    /// Wildcard query
    Wildcard {
        field: String,
        pattern: String,
    },

    /// Fuzzy query
    Fuzzy {
        field: String,
        value: String,
        #[serde(default = "default_fuzziness")]
        fuzziness: usize,
    },

    /// Range query
    Range {
        field: String,
        #[serde(default)]
        gte: Option<i64>,
        #[serde(default)]
        gt: Option<i64>,
        #[serde(default)]
        lte: Option<i64>,
        #[serde(default)]
        lt: Option<i64>,
    },

    /// Boolean query
    Bool {
        #[serde(default)]
        must: Vec<Query>,
        #[serde(default)]
        should: Vec<Query>,
        #[serde(default)]
        must_not: Vec<Query>,
        #[serde(default)]
        filter: Vec<Query>,
        #[serde(default = "default_minimum_should_match")]
        minimum_should_match: usize,
    },

    /// IDs query
    Ids {
        values: Vec<String>,
    },

    /// Exists query
    Exists {
        field: String,
    },

    /// Regex query
    Regex {
        field: String,
        pattern: String,
    },

    /// Query string query
    QueryString {
        query: String,
        #[serde(default)]
        default_field: Option<String>,
        #[serde(default)]
        default_operator: MatchOperator,
    },

    /// Boosted query
    Boosted {
        query: Box<Query>,
        boost: f32,
    },
}

fn default_fuzziness() -> usize {
    2
}

fn default_minimum_should_match() -> usize {
    1
}

/// Match operator
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchOperator {
    #[default]
    Or,
    And,
}

/// Search request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Query
    pub query: Query,
    /// Start offset
    #[serde(default)]
    pub from: usize,
    /// Maximum results
    #[serde(default = "default_size")]
    pub size: usize,
    /// Sort options
    #[serde(default)]
    pub sort: Vec<SortOption>,
    /// Fields to return
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Highlight configuration
    #[serde(default)]
    pub highlight: Option<HighlightConfig>,
    /// Aggregations
    #[serde(default)]
    pub aggregations: Option<Vec<Aggregation>>,
    /// Minimum score threshold
    #[serde(default)]
    pub min_score: Option<f32>,
    /// Explain scoring
    #[serde(default)]
    pub explain: bool,
}

fn default_size() -> usize {
    10
}

impl Default for SearchRequest {
    fn default() -> Self {
        Self {
            query: Query::MatchAll,
            from: 0,
            size: 10,
            sort: Vec::new(),
            fields: None,
            highlight: None,
            aggregations: None,
            min_score: None,
            explain: false,
        }
    }
}

/// Sort option
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOption {
    /// Field to sort by
    pub field: String,
    /// Sort order
    #[serde(default)]
    pub order: SortOrder,
}

/// Sort order
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Highlight configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightConfig {
    /// Fields to highlight
    pub fields: Vec<String>,
    /// Pre tag
    #[serde(default = "default_pre_tag")]
    pub pre_tag: String,
    /// Post tag
    #[serde(default = "default_post_tag")]
    pub post_tag: String,
    /// Fragment size
    #[serde(default = "default_fragment_size")]
    pub fragment_size: usize,
    /// Number of fragments
    #[serde(default = "default_num_fragments")]
    pub num_fragments: usize,
}

fn default_pre_tag() -> String {
    "<em>".to_string()
}

fn default_post_tag() -> String {
    "</em>".to_string()
}

fn default_fragment_size() -> usize {
    100
}

fn default_num_fragments() -> usize {
    3
}

/// Aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Aggregation {
    /// Terms aggregation
    Terms {
        name: String,
        field: String,
        #[serde(default = "default_agg_size")]
        size: usize,
    },
    /// Histogram aggregation
    Histogram {
        name: String,
        field: String,
        interval: i64,
    },
    /// Stats aggregation
    Stats {
        name: String,
        field: String,
    },
    /// Count aggregation
    Count {
        name: String,
        field: String,
    },
}

fn default_agg_size() -> usize {
    10
}

/// Search response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Total hits
    pub total: usize,
    /// Maximum score
    pub max_score: Option<f32>,
    /// Hits
    pub hits: Vec<ScoredDocument>,
    /// Aggregation results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<AggregationResults>,
    /// Time taken in milliseconds
    pub took_ms: u64,
}

/// Aggregation results
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregationResults {
    /// Results by aggregation name
    pub results: std::collections::HashMap<String, AggregationResult>,
}

/// Aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AggregationResult {
    /// Terms buckets
    Terms { buckets: Vec<TermBucket> },
    /// Histogram buckets
    Histogram { buckets: Vec<HistogramBucket> },
    /// Stats
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        avg: f64,
    },
    /// Count
    Count { value: u64 },
}

/// Term bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermBucket {
    pub key: String,
    pub doc_count: u64,
}

/// Histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub key: i64,
    pub doc_count: u64,
}

/// Query parser
pub struct QueryParser {
    default_field: String,
    default_operator: MatchOperator,
}

impl QueryParser {
    /// Creates a new query parser
    pub fn new() -> Self {
        Self {
            default_field: "content".to_string(),
            default_operator: MatchOperator::Or,
        }
    }

    /// Sets default field
    pub fn default_field(mut self, field: &str) -> Self {
        self.default_field = field.to_string();
        self
    }

    /// Sets default operator
    pub fn default_operator(mut self, op: MatchOperator) -> Self {
        self.default_operator = op;
        self
    }

    /// Parses a query string
    pub fn parse(&self, query_string: &str) -> Result<Query> {
        let query_string = query_string.trim();

        if query_string.is_empty() || query_string == "*" {
            return Ok(Query::MatchAll);
        }

        // Simple parsing for common patterns
        if let Some(query) = self.try_parse_field_query(query_string) {
            return Ok(query);
        }

        if let Some(query) = self.try_parse_boolean_query(query_string) {
            return Ok(query);
        }

        // Default: match query on default field
        Ok(Query::Match {
            field: self.default_field.clone(),
            query: query_string.to_string(),
            operator: self.default_operator,
        })
    }

    /// Tries to parse field:value format
    fn try_parse_field_query(&self, query: &str) -> Option<Query> {
        let parts: Vec<&str> = query.splitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }

        let field = parts[0].trim();
        let value = parts[1].trim().trim_matches('"');

        // Check for special patterns
        if value.contains('*') || value.contains('?') {
            return Some(Query::Wildcard {
                field: field.to_string(),
                pattern: value.to_string(),
            });
        }

        if value.starts_with('[') || value.starts_with('{') {
            if let Some(range) = self.parse_range(value) {
                return Some(Query::Range {
                    field: field.to_string(),
                    gte: range.0,
                    gt: None,
                    lte: range.1,
                    lt: None,
                });
            }
        }

        if value.starts_with('~') {
            let fuzziness: usize = value[1..].parse().unwrap_or(2);
            return Some(Query::Fuzzy {
                field: field.to_string(),
                value: field.to_string(),
                fuzziness,
            });
        }

        Some(Query::Term {
            field: field.to_string(),
            value: value.to_string(),
        })
    }

    /// Parses range notation like [1 TO 10]
    fn parse_range(&self, value: &str) -> Option<(Option<i64>, Option<i64>)> {
        let _inclusive = value.starts_with('[');
        let value = value.trim_matches(|c| c == '[' || c == ']' || c == '{' || c == '}');

        let parts: Vec<&str> = value.split(" TO ").collect();
        if parts.len() != 2 {
            return None;
        }

        let min = if parts[0] == "*" {
            None
        } else {
            parts[0].trim().parse().ok()
        };

        let max = if parts[1] == "*" {
            None
        } else {
            parts[1].trim().parse().ok()
        };

        Some((min, max))
    }

    /// Tries to parse boolean operators
    fn try_parse_boolean_query(&self, query: &str) -> Option<Query> {
        // Check for AND/OR operators
        if query.contains(" AND ") || query.contains(" OR ") || query.contains(" NOT ") {
            let mut must = Vec::new();
            let mut should = Vec::new();
            let mut must_not = Vec::new();

            let mut current = query;
            while !current.is_empty() {
                if let Some(idx) = current.find(" AND ") {
                    let part = &current[..idx].trim();
                    if !part.is_empty() {
                        must.push(Query::Match {
                            field: self.default_field.clone(),
                            query: part.to_string(),
                            operator: self.default_operator,
                        });
                    }
                    current = &current[idx + 5..];
                } else if let Some(idx) = current.find(" OR ") {
                    let part = &current[..idx].trim();
                    if !part.is_empty() {
                        should.push(Query::Match {
                            field: self.default_field.clone(),
                            query: part.to_string(),
                            operator: self.default_operator,
                        });
                    }
                    current = &current[idx + 4..];
                } else if let Some(idx) = current.find(" NOT ") {
                    let part = &current[..idx].trim();
                    if !part.is_empty() {
                        must.push(Query::Match {
                            field: self.default_field.clone(),
                            query: part.to_string(),
                            operator: self.default_operator,
                        });
                    }
                    current = &current[idx + 5..];
                    // Next part is must_not
                    must_not.push(Query::Match {
                        field: self.default_field.clone(),
                        query: current.trim().to_string(),
                        operator: self.default_operator,
                    });
                    break;
                } else {
                    // Last part
                    if !current.trim().is_empty() {
                        if !should.is_empty() {
                            should.push(Query::Match {
                                field: self.default_field.clone(),
                                query: current.trim().to_string(),
                                operator: self.default_operator,
                            });
                        } else {
                            must.push(Query::Match {
                                field: self.default_field.clone(),
                                query: current.trim().to_string(),
                                operator: self.default_operator,
                            });
                        }
                    }
                    break;
                }
            }

            if !must.is_empty() || !should.is_empty() || !must_not.is_empty() {
                return Some(Query::Bool {
                    must,
                    should,
                    must_not,
                    filter: Vec::new(),
                    minimum_should_match: 1,
                });
            }
        }

        None
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Query executor
pub struct QueryExecutor<'a> {
    index: &'a SearchIndex,
    ranker: Ranker,
}

impl<'a> QueryExecutor<'a> {
    /// Creates a new query executor
    pub fn new(index: &'a SearchIndex) -> Self {
        Self {
            index,
            ranker: Ranker::new(RankingConfig::default()),
        }
    }

    /// Creates with custom ranking config
    pub fn with_ranking(index: &'a SearchIndex, config: RankingConfig) -> Self {
        Self {
            index,
            ranker: Ranker::new(config),
        }
    }

    /// Executes a search request
    pub async fn execute(&self, request: &SearchRequest) -> Result<SearchResponse> {
        let start = std::time::Instant::now();

        // Execute query
        let doc_ids = self.execute_query(&request.query).await?;

        // Get documents
        let mut scored_docs = Vec::new();
        for doc_id in &doc_ids {
            if let Some(doc) = self.index.get(doc_id).await {
                // Calculate score
                let score = self.calculate_score(&request.query, &doc).await;

                // Apply min score filter
                if let Some(min_score) = request.min_score {
                    if score < min_score {
                        continue;
                    }
                }

                scored_docs.push(ScoredDocument {
                    doc,
                    score,
                    explanation: None,
                });
            }
        }

        // Sort results
        if request.sort.is_empty() {
            // Default: sort by score descending
            scored_docs.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            for sort in request.sort.iter().rev() {
                scored_docs.sort_by(|a, b| {
                    let a_val = a.doc.get_field(&sort.field);
                    let b_val = b.doc.get_field(&sort.field);
                    let cmp = compare_field_values(a_val, b_val);
                    match sort.order {
                        SortOrder::Asc => cmp,
                        SortOrder::Desc => cmp.reverse(),
                    }
                });
            }
        }

        let total = scored_docs.len();
        let max_score = scored_docs.first().map(|d| d.score);

        // Apply pagination
        let hits: Vec<_> = scored_docs
            .into_iter()
            .skip(request.from)
            .take(request.size)
            .collect();

        let took_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse {
            total,
            max_score,
            hits,
            aggregations: None,
            took_ms,
        })
    }

    /// Executes a query and returns matching document IDs
    fn execute_query<'b>(&'b self, query: &'b Query) -> BoxFuture<'b, Result<HashSet<String>>> {
        async move {
        match query {
            Query::MatchAll => {
                let docs = self.index.all_docs().await;
                Ok(docs.into_iter().map(|d| d.id).collect())
            }

            Query::Term { field, value } => {
                let results = self.index.search_term(field, value).await;
                Ok(results.into_iter().collect())
            }

            Query::Match { field, query, operator: _ } => {
                let results = self.index.search_term(field, query).await;
                Ok(results.into_iter().collect())
            }

            Query::MultiMatch { fields, query, operator } => {
                let mut all_results = HashSet::new();
                for field in fields {
                    let results = self.index.search_term(field, query).await;
                    match operator {
                        MatchOperator::Or => all_results.extend(results),
                        MatchOperator::And => {
                            let results_set: HashSet<_> = results.into_iter().collect();
                            if all_results.is_empty() {
                                all_results = results_set;
                            } else {
                                all_results = all_results.intersection(&results_set).cloned().collect();
                            }
                        }
                    }
                }
                Ok(all_results)
            }

            Query::Prefix { field, prefix } => {
                let results = self.index.search_prefix(field, prefix).await;
                Ok(results.into_iter().collect())
            }

            Query::Wildcard { field, pattern } => {
                let results = self.index.search_wildcard(field, pattern).await;
                Ok(results.into_iter().collect())
            }

            Query::Fuzzy { field, value, fuzziness } => {
                let results = self.index.search_fuzzy(field, value, *fuzziness).await;
                Ok(results.into_iter().collect())
            }

            Query::Range { field, gte, gt, lte, lt } => {
                let min = gte.or(*gt);
                let max = lte.or(*lt);
                let results = self.index.search_range(field, min, max).await;
                Ok(results.into_iter().collect())
            }

            Query::Bool { must, should, must_not, filter, minimum_should_match } => {
                // Start with all documents for must queries
                let mut result: Option<HashSet<String>> = None;

                // Process must clauses
                for q in must {
                    let matches = self.execute_query(q).await?;
                    result = Some(match result {
                        Some(r) => r.intersection(&matches).cloned().collect(),
                        None => matches,
                    });
                }

                // Process filter clauses (same as must but no scoring)
                for q in filter {
                    let matches = self.execute_query(q).await?;
                    result = Some(match result {
                        Some(r) => r.intersection(&matches).cloned().collect(),
                        None => matches,
                    });
                }

                // Process should clauses
                if !should.is_empty() {
                    let mut should_matches: Vec<HashSet<String>> = Vec::new();
                    for q in should {
                        should_matches.push(self.execute_query(q).await?);
                    }

                    // Union of should matches
                    let should_union: HashSet<_> = should_matches.iter().flatten().cloned().collect();

                    result = Some(match result {
                        Some(r) if *minimum_should_match > 0 => {
                            r.intersection(&should_union).cloned().collect()
                        }
                        Some(r) => r,
                        None => should_union,
                    });
                }

                // Process must_not clauses
                for q in must_not {
                    let excluded = self.execute_query(q).await?;
                    if let Some(ref mut r) = result {
                        *r = r.difference(&excluded).cloned().collect();
                    }
                }

                Ok(result.unwrap_or_default())
            }

            Query::Ids { values } => {
                let mut result = HashSet::new();
                for id in values {
                    if self.index.get(id).await.is_some() {
                        result.insert(id.clone());
                    }
                }
                Ok(result)
            }

            Query::Exists { field } => {
                let docs = self.index.all_docs().await;
                Ok(docs
                    .into_iter()
                    .filter(|d| d.has_field(field))
                    .map(|d| d.id)
                    .collect())
            }

            Query::Phrase { field, phrase, slop: _ } => {
                // Simplified: treat as match for now
                let results = self.index.search_term(field, phrase).await;
                Ok(results.into_iter().collect())
            }

            Query::Regex { field, pattern } => {
                // Use wildcard as approximation
                let results = self.index.search_wildcard(field, pattern).await;
                Ok(results.into_iter().collect())
            }

            Query::QueryString { query, default_field, default_operator } => {
                let parser = QueryParser::new()
                    .default_field(default_field.as_deref().unwrap_or("content"))
                    .default_operator(*default_operator);
                let parsed = parser.parse(query)?;
                self.execute_query(&parsed).await
            }

            Query::Boosted { query, boost: _ } => {
                // Execute inner query, boost is applied during scoring
                self.execute_query(query).await
            }
        }
        }.boxed()
    }

    /// Calculates score for a document
    async fn calculate_score(&self, query: &Query, doc: &Document) -> f32 {
        match query {
            Query::MatchAll => 1.0,

            Query::Term { field, value } | Query::Match { field, query: value, .. } => {
                let doc_freq = self.index.get_doc_freq(field, value).await as u64;
                let doc_count = self.index.doc_count();
                let field_length = self.index.get_field_length(field, &doc.id).await as u64;
                let avg_length = self.index.get_avg_field_length(field).await as f64;

                self.ranker.calculate_bm25(1, doc_freq, doc_count, field_length, avg_length)
            }

            Query::Bool { must, should, .. } => {
                let mut score = 0.0;
                for q in must {
                    score += Box::pin(self.calculate_score(q, doc)).await;
                }
                for q in should {
                    score += Box::pin(self.calculate_score(q, doc)).await * 0.5;
                }
                score
            }

            Query::Boosted { query, boost } => {
                Box::pin(self.calculate_score(query, doc)).await * boost
            }

            _ => 1.0,
        }
    }
}

/// Compares field values for sorting
fn compare_field_values(
    a: Option<&super::document::FieldValue>,
    b: Option<&super::document::FieldValue>,
) -> std::cmp::Ordering {
    use super::document::FieldValue;

    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(a), Some(b)) => match (a, b) {
            (FieldValue::Text(a), FieldValue::Text(b)) => a.cmp(b),
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a.cmp(b),
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_parser_simple() {
        let parser = QueryParser::new();

        let query = parser.parse("hello world").unwrap();
        assert!(matches!(query, Query::Match { .. }));

        let query = parser.parse("*").unwrap();
        assert!(matches!(query, Query::MatchAll));
    }

    #[test]
    fn test_query_parser_field() {
        let parser = QueryParser::new();

        let query = parser.parse("title:hello").unwrap();
        match query {
            Query::Term { field, value } => {
                assert_eq!(field, "title");
                assert_eq!(value, "hello");
            }
            _ => panic!("Expected Term query"),
        }
    }

    #[test]
    fn test_query_parser_wildcard() {
        let parser = QueryParser::new();

        let query = parser.parse("title:hel*").unwrap();
        assert!(matches!(query, Query::Wildcard { .. }));
    }

    #[test]
    fn test_query_parser_boolean() {
        let parser = QueryParser::new();

        let query = parser.parse("hello AND world").unwrap();
        match query {
            Query::Bool { must, .. } => {
                assert_eq!(must.len(), 2);
            }
            _ => panic!("Expected Bool query"),
        }
    }

    #[test]
    fn test_search_request_default() {
        let request = SearchRequest::default();
        assert_eq!(request.from, 0);
        assert_eq!(request.size, 10);
        assert!(request.sort.is_empty());
    }
}
