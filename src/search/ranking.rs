// Search Result Ranking and Scoring

use super::document::Document;
use serde::{Deserialize, Serialize};

/// Ranking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingConfig {
    /// BM25 k1 parameter (term saturation)
    pub bm25_k1: f32,
    /// BM25 b parameter (length normalization)
    pub bm25_b: f32,
    /// Enable TF-IDF fallback
    pub use_tfidf: bool,
    /// Boost for exact matches
    pub exact_match_boost: f32,
    /// Boost for phrase matches
    pub phrase_match_boost: f32,
    /// Enable field length normalization
    pub length_normalization: bool,
    /// Minimum score threshold
    pub min_score: f32,
}

impl Default for RankingConfig {
    fn default() -> Self {
        Self {
            bm25_k1: 1.2,
            bm25_b: 0.75,
            use_tfidf: false,
            exact_match_boost: 2.0,
            phrase_match_boost: 1.5,
            length_normalization: true,
            min_score: 0.0,
        }
    }
}

/// Scored document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredDocument {
    /// The document
    pub doc: Document,
    /// Relevance score
    pub score: f32,
    /// Score explanation (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explanation: Option<ScoreExplanation>,
}

/// Score explanation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreExplanation {
    /// Description
    pub description: String,
    /// Score value
    pub value: f32,
    /// Child explanations
    pub details: Vec<ScoreExplanation>,
}

impl ScoreExplanation {
    /// Creates a new explanation
    pub fn new(description: &str, value: f32) -> Self {
        Self {
            description: description.to_string(),
            value,
            details: Vec::new(),
        }
    }

    /// Adds a child explanation
    pub fn with_detail(mut self, detail: ScoreExplanation) -> Self {
        self.details.push(detail);
        self
    }
}

/// Ranker for calculating document scores
pub struct Ranker {
    config: RankingConfig,
}

impl Ranker {
    /// Creates a new ranker
    pub fn new(config: RankingConfig) -> Self {
        Self { config }
    }

    /// Calculates BM25 score
    pub fn calculate_bm25(
        &self,
        term_freq: u32,
        doc_freq: u64,
        doc_count: u64,
        field_length: u64,
        avg_field_length: f64,
    ) -> f32 {
        if doc_count == 0 || doc_freq == 0 {
            return 0.0;
        }

        let k1 = self.config.bm25_k1 as f64;
        let b = self.config.bm25_b as f64;

        // IDF component
        let idf = ((doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5) + 1.0).ln();

        // TF component with length normalization
        let tf = term_freq as f64;
        let length_norm = if self.config.length_normalization && avg_field_length > 0.0 {
            1.0 - b + b * (field_length as f64 / avg_field_length)
        } else {
            1.0
        };

        let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * length_norm);

        (idf * tf_norm) as f32
    }

    /// Calculates BM25 score with explanation
    pub fn calculate_bm25_with_explanation(
        &self,
        term: &str,
        term_freq: u32,
        doc_freq: u64,
        doc_count: u64,
        field_length: u64,
        avg_field_length: f64,
    ) -> (f32, ScoreExplanation) {
        let score = self.calculate_bm25(term_freq, doc_freq, doc_count, field_length, avg_field_length);

        let k1 = self.config.bm25_k1;
        let b = self.config.bm25_b;

        let idf = ((doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5) + 1.0).ln() as f32;

        let explanation = ScoreExplanation::new(&format!("BM25 score for term '{}'", term), score)
            .with_detail(ScoreExplanation::new(
                &format!("IDF (log((N - df + 0.5) / (df + 0.5) + 1)) = {:.4}", idf),
                idf,
            ))
            .with_detail(ScoreExplanation::new(
                &format!("tf = {}", term_freq),
                term_freq as f32,
            ))
            .with_detail(ScoreExplanation::new(
                &format!("doc_freq = {}", doc_freq),
                doc_freq as f32,
            ))
            .with_detail(ScoreExplanation::new(
                &format!("k1 = {}, b = {}", k1, b),
                0.0,
            ))
            .with_detail(ScoreExplanation::new(
                &format!("field_length = {}, avg = {:.2}", field_length, avg_field_length),
                0.0,
            ));

        (score, explanation)
    }

    /// Calculates TF-IDF score
    pub fn calculate_tfidf(
        &self,
        term_freq: u32,
        doc_freq: u64,
        doc_count: u64,
    ) -> f32 {
        if doc_count == 0 || doc_freq == 0 {
            return 0.0;
        }

        let tf = (1.0 + (term_freq as f64).ln()) as f32;
        let idf = (doc_count as f64 / doc_freq as f64).ln() as f32;

        tf * idf
    }

    /// Calculates combined score for multiple terms
    pub fn combine_term_scores(&self, scores: &[f32]) -> f32 {
        if scores.is_empty() {
            return 0.0;
        }

        // Sum all term scores
        scores.iter().sum()
    }

    /// Applies field boost
    pub fn apply_field_boost(&self, score: f32, boost: f32) -> f32 {
        score * boost
    }

    /// Applies exact match boost
    pub fn apply_exact_match_boost(&self, score: f32) -> f32 {
        score * self.config.exact_match_boost
    }

    /// Applies phrase match boost
    pub fn apply_phrase_match_boost(&self, score: f32) -> f32 {
        score * self.config.phrase_match_boost
    }

    /// Normalizes scores to 0-1 range
    pub fn normalize_scores(scores: &mut [f32]) {
        if scores.is_empty() {
            return;
        }

        let max_score = scores.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        if max_score > 0.0 {
            for score in scores {
                *score /= max_score;
            }
        }
    }

    /// Calculates cosine similarity
    pub fn cosine_similarity(vec1: &[f32], vec2: &[f32]) -> f32 {
        if vec1.len() != vec2.len() || vec1.is_empty() {
            return 0.0;
        }

        let dot_product: f32 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let magnitude1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let magnitude2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if magnitude1 == 0.0 || magnitude2 == 0.0 {
            return 0.0;
        }

        dot_product / (magnitude1 * magnitude2)
    }

    /// Calculates Jaccard similarity
    pub fn jaccard_similarity(set1: &[String], set2: &[String]) -> f32 {
        use std::collections::HashSet;

        let set1: HashSet<_> = set1.iter().collect();
        let set2: HashSet<_> = set2.iter().collect();

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union == 0 {
            return 0.0;
        }

        intersection as f32 / union as f32
    }

    /// Ranks documents by score
    pub fn rank_documents(&self, mut docs: Vec<ScoredDocument>) -> Vec<ScoredDocument> {
        // Sort by score descending
        docs.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        // Filter by min score
        if self.config.min_score > 0.0 {
            docs.retain(|d| d.score >= self.config.min_score);
        }

        docs
    }
}

/// Score combiner for multi-field queries
pub struct ScoreCombiner {
    mode: CombineMode,
    weights: Vec<f32>,
}

/// Score combination mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CombineMode {
    /// Sum of scores
    Sum,
    /// Maximum score
    Max,
    /// Average score
    Avg,
    /// Weighted average
    WeightedAvg,
    /// Multiply scores
    Multiply,
}

impl ScoreCombiner {
    /// Creates a new combiner
    pub fn new(mode: CombineMode) -> Self {
        Self {
            mode,
            weights: Vec::new(),
        }
    }

    /// Sets weights for weighted average
    pub fn with_weights(mut self, weights: Vec<f32>) -> Self {
        self.weights = weights;
        self
    }

    /// Combines multiple scores
    pub fn combine(&self, scores: &[f32]) -> f32 {
        if scores.is_empty() {
            return 0.0;
        }

        match self.mode {
            CombineMode::Sum => scores.iter().sum(),

            CombineMode::Max => scores.iter().cloned().fold(f32::NEG_INFINITY, f32::max),

            CombineMode::Avg => scores.iter().sum::<f32>() / scores.len() as f32,

            CombineMode::WeightedAvg => {
                if self.weights.len() != scores.len() {
                    return scores.iter().sum::<f32>() / scores.len() as f32;
                }
                let weighted_sum: f32 = scores.iter().zip(&self.weights).map(|(s, w)| s * w).sum();
                let weight_sum: f32 = self.weights.iter().sum();
                if weight_sum > 0.0 {
                    weighted_sum / weight_sum
                } else {
                    0.0
                }
            }

            CombineMode::Multiply => {
                scores.iter().fold(1.0, |acc, s| acc * s)
            }
        }
    }
}

/// Function score modifiers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionScore {
    /// Weight multiplier
    Weight { weight: f32 },

    /// Field value boost
    FieldValue {
        field: String,
        factor: f32,
        modifier: Modifier,
        missing: f32,
    },

    /// Decay function (gaussian, linear, exponential)
    Decay {
        field: String,
        origin: f64,
        scale: f64,
        decay: f32,
        function: DecayFunction,
    },

    /// Random score
    Random { seed: u64 },

    /// Script score
    Script { source: String },
}

/// Score modifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Modifier {
    None,
    Log,
    Log1p,
    Log2p,
    Ln,
    Ln1p,
    Ln2p,
    Square,
    Sqrt,
    Reciprocal,
}

impl Modifier {
    /// Applies modifier to value
    pub fn apply(&self, value: f64) -> f64 {
        match self {
            Modifier::None => value,
            Modifier::Log => value.log10(),
            Modifier::Log1p => (value + 1.0).log10(),
            Modifier::Log2p => (value + 2.0).log10(),
            Modifier::Ln => value.ln(),
            Modifier::Ln1p => (value + 1.0).ln(),
            Modifier::Ln2p => (value + 2.0).ln(),
            Modifier::Square => value * value,
            Modifier::Sqrt => value.sqrt(),
            Modifier::Reciprocal => 1.0 / value,
        }
    }
}

/// Decay function type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecayFunction {
    Linear,
    Exponential,
    Gaussian,
}

impl DecayFunction {
    /// Calculates decay value
    pub fn calculate(&self, distance: f64, scale: f64, decay: f64) -> f64 {
        let normalized = distance / scale;

        match self {
            DecayFunction::Linear => {
                (1.0 - normalized).max(0.0)
            }
            DecayFunction::Exponential => {
                decay.powf(normalized)
            }
            DecayFunction::Gaussian => {
                (-0.5 * normalized * normalized / decay.ln().abs()).exp()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25() {
        let ranker = Ranker::new(RankingConfig::default());

        // Higher term frequency should yield higher score
        let score1 = ranker.calculate_bm25(1, 10, 1000, 100, 150.0);
        let score2 = ranker.calculate_bm25(5, 10, 1000, 100, 150.0);
        assert!(score2 > score1);

        // Rarer terms should yield higher score
        let score_common = ranker.calculate_bm25(1, 500, 1000, 100, 150.0);
        let score_rare = ranker.calculate_bm25(1, 10, 1000, 100, 150.0);
        assert!(score_rare > score_common);
    }

    #[test]
    fn test_tfidf() {
        let ranker = Ranker::new(RankingConfig::default());

        let score = ranker.calculate_tfidf(5, 100, 10000);
        assert!(score > 0.0);

        // Rarer terms should have higher IDF
        let score_common = ranker.calculate_tfidf(1, 5000, 10000);
        let score_rare = ranker.calculate_tfidf(1, 100, 10000);
        assert!(score_rare > score_common);
    }

    #[test]
    fn test_cosine_similarity() {
        let vec1 = vec![1.0, 2.0, 3.0];
        let vec2 = vec![1.0, 2.0, 3.0];
        let sim = Ranker::cosine_similarity(&vec1, &vec2);
        assert!((sim - 1.0).abs() < 0.001);

        let vec3 = vec![-1.0, -2.0, -3.0];
        let sim = Ranker::cosine_similarity(&vec1, &vec3);
        assert!((sim - (-1.0)).abs() < 0.001);
    }

    #[test]
    fn test_jaccard_similarity() {
        let set1 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let set2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let sim = Ranker::jaccard_similarity(&set1, &set2);
        assert_eq!(sim, 1.0);

        let set3 = vec!["a".to_string(), "d".to_string(), "e".to_string()];
        let sim = Ranker::jaccard_similarity(&set1, &set3);
        assert!((sim - 0.2).abs() < 0.001); // 1 common / 5 total
    }

    #[test]
    fn test_score_combiner() {
        let scores = vec![1.0, 2.0, 3.0];

        assert_eq!(ScoreCombiner::new(CombineMode::Sum).combine(&scores), 6.0);
        assert_eq!(ScoreCombiner::new(CombineMode::Max).combine(&scores), 3.0);
        assert_eq!(ScoreCombiner::new(CombineMode::Avg).combine(&scores), 2.0);
        assert_eq!(ScoreCombiner::new(CombineMode::Multiply).combine(&scores), 6.0);

        let combiner = ScoreCombiner::new(CombineMode::WeightedAvg)
            .with_weights(vec![1.0, 2.0, 1.0]);
        let weighted = combiner.combine(&scores);
        assert!((weighted - 2.0).abs() < 0.001); // (1*1 + 2*2 + 3*1) / 4 = 8/4 = 2
    }

    #[test]
    fn test_decay_functions() {
        // Linear decay
        let value = DecayFunction::Linear.calculate(0.0, 10.0, 0.5);
        assert_eq!(value, 1.0);

        let value = DecayFunction::Linear.calculate(10.0, 10.0, 0.5);
        assert_eq!(value, 0.0);

        // Exponential decay
        let value = DecayFunction::Exponential.calculate(0.0, 10.0, 0.5);
        assert_eq!(value, 1.0);
    }

    #[test]
    fn test_modifier() {
        assert_eq!(Modifier::None.apply(10.0), 10.0);
        assert!((Modifier::Sqrt.apply(16.0) - 4.0).abs() < 0.001);
        assert!((Modifier::Square.apply(4.0) - 16.0).abs() < 0.001);
        assert!((Modifier::Log.apply(100.0) - 2.0).abs() < 0.001);
    }
}
