// Text Analysis for Full-Text Search

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Analyzer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Tokenizer type
    pub tokenizer: TokenizerType,
    /// Token filters
    pub filters: Vec<TokenFilter>,
    /// Minimum token length
    pub min_token_length: usize,
    /// Maximum token length
    pub max_token_length: usize,
    /// Stop words
    pub stop_words: HashSet<String>,
    /// Enable stemming
    pub stemming: bool,
    /// Language for stemming
    pub language: String,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            tokenizer: TokenizerType::Standard,
            filters: vec![
                TokenFilter::Lowercase,
                TokenFilter::StopWords,
                TokenFilter::Stemmer,
            ],
            min_token_length: 2,
            max_token_length: 100,
            stop_words: default_stop_words(),
            stemming: true,
            language: "english".to_string(),
        }
    }
}

/// Tokenizer types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenizerType {
    /// Standard word tokenizer
    Standard,
    /// Whitespace tokenizer
    Whitespace,
    /// N-gram tokenizer
    Ngram,
    /// Edge n-gram tokenizer
    EdgeNgram,
    /// Character tokenizer
    Character,
    /// Path hierarchy tokenizer
    PathHierarchy,
    /// Keyword (no tokenization)
    Keyword,
}

/// Token filter types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenFilter {
    /// Convert to lowercase
    Lowercase,
    /// Convert to uppercase
    Uppercase,
    /// Remove stop words
    StopWords,
    /// Apply stemming
    Stemmer,
    /// Trim whitespace
    Trim,
    /// ASCII folding
    AsciiFolding,
    /// Remove duplicates
    Deduplicate,
    /// Synonym expansion
    Synonyms(Vec<(String, Vec<String>)>),
    /// Minimum length filter
    MinLength(usize),
    /// Maximum length filter
    MaxLength(usize),
    /// N-gram filter
    Ngram { min: usize, max: usize },
    /// Edge n-gram filter
    EdgeNgram { min: usize, max: usize },
}

/// A token produced by analysis
#[derive(Debug, Clone)]
pub struct Token {
    /// Token text
    pub text: String,
    /// Position in original text
    pub position: usize,
    /// Start offset in original text
    pub start_offset: usize,
    /// End offset in original text
    pub end_offset: usize,
}

/// Analyzer trait
pub trait Analyzer: Send + Sync {
    /// Analyzes text into tokens
    fn analyze(&self, text: &str) -> Vec<Token>;

    /// Analyzes for indexing
    fn analyze_for_index(&self, text: &str) -> Vec<String> {
        self.analyze(text).into_iter().map(|t| t.text).collect()
    }

    /// Analyzes for query
    fn analyze_for_query(&self, text: &str) -> Vec<String> {
        self.analyze(text).into_iter().map(|t| t.text).collect()
    }
}

/// Standard text analyzer
pub struct TextAnalyzer {
    config: AnalyzerConfig,
}

impl TextAnalyzer {
    /// Creates a new text analyzer
    pub fn new(config: AnalyzerConfig) -> Self {
        Self { config }
    }

    /// Creates with default configuration
    pub fn default_analyzer() -> Self {
        Self::new(AnalyzerConfig::default())
    }

    /// Creates a simple analyzer (whitespace + lowercase)
    pub fn simple() -> Self {
        Self::new(AnalyzerConfig {
            tokenizer: TokenizerType::Whitespace,
            filters: vec![TokenFilter::Lowercase, TokenFilter::Trim],
            stemming: false,
            ..Default::default()
        })
    }

    /// Creates a keyword analyzer (no tokenization)
    pub fn keyword() -> Self {
        Self::new(AnalyzerConfig {
            tokenizer: TokenizerType::Keyword,
            filters: vec![TokenFilter::Lowercase],
            stemming: false,
            ..Default::default()
        })
    }

    /// Tokenizes text based on tokenizer type
    fn tokenize(&self, text: &str) -> Vec<Token> {
        match self.config.tokenizer {
            TokenizerType::Standard => self.standard_tokenize(text),
            TokenizerType::Whitespace => self.whitespace_tokenize(text),
            TokenizerType::Ngram => self.ngram_tokenize(text, 2, 3),
            TokenizerType::EdgeNgram => self.edge_ngram_tokenize(text, 1, 10),
            TokenizerType::Character => self.character_tokenize(text),
            TokenizerType::PathHierarchy => self.path_hierarchy_tokenize(text),
            TokenizerType::Keyword => self.keyword_tokenize(text),
        }
    }

    fn standard_tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0;
        let mut current_word = String::new();
        let mut word_start = 0;

        for (i, c) in text.char_indices() {
            if c.is_alphanumeric() || c == '_' {
                if current_word.is_empty() {
                    word_start = i;
                }
                current_word.push(c);
            } else if !current_word.is_empty() {
                tokens.push(Token {
                    text: current_word.clone(),
                    position,
                    start_offset: word_start,
                    end_offset: i,
                });
                position += 1;
                current_word.clear();
            }
        }

        // Handle last word
        if !current_word.is_empty() {
            tokens.push(Token {
                text: current_word,
                position,
                start_offset: word_start,
                end_offset: text.len(),
            });
        }

        tokens
    }

    fn whitespace_tokenize(&self, text: &str) -> Vec<Token> {
        text.split_whitespace()
            .enumerate()
            .map(|(pos, word)| {
                let start = text.find(word).unwrap_or(0);
                Token {
                    text: word.to_string(),
                    position: pos,
                    start_offset: start,
                    end_offset: start + word.len(),
                }
            })
            .collect()
    }

    fn ngram_tokenize(&self, text: &str, min: usize, max: usize) -> Vec<Token> {
        let chars: Vec<char> = text.chars().collect();
        let mut tokens = Vec::new();
        let mut position = 0;

        for n in min..=max {
            for i in 0..=chars.len().saturating_sub(n) {
                let ngram: String = chars[i..i + n].iter().collect();
                tokens.push(Token {
                    text: ngram,
                    position,
                    start_offset: i,
                    end_offset: i + n,
                });
                position += 1;
            }
        }

        tokens
    }

    fn edge_ngram_tokenize(&self, text: &str, min: usize, max: usize) -> Vec<Token> {
        let chars: Vec<char> = text.chars().collect();
        let mut tokens = Vec::new();

        let actual_max = max.min(chars.len());
        for n in min..=actual_max {
            let ngram: String = chars[..n].iter().collect();
            tokens.push(Token {
                text: ngram,
                position: n - min,
                start_offset: 0,
                end_offset: n,
            });
        }

        tokens
    }

    fn character_tokenize(&self, text: &str) -> Vec<Token> {
        text.char_indices()
            .enumerate()
            .map(|(pos, (offset, c))| Token {
                text: c.to_string(),
                position: pos,
                start_offset: offset,
                end_offset: offset + c.len_utf8(),
            })
            .collect()
    }

    fn path_hierarchy_tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let parts: Vec<&str> = text.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_path = String::new();

        for (pos, part) in parts.iter().enumerate() {
            if !current_path.is_empty() {
                current_path.push('/');
            }
            current_path.push_str(part);

            tokens.push(Token {
                text: current_path.clone(),
                position: pos,
                start_offset: 0,
                end_offset: current_path.len(),
            });
        }

        tokens
    }

    fn keyword_tokenize(&self, text: &str) -> Vec<Token> {
        vec![Token {
            text: text.to_string(),
            position: 0,
            start_offset: 0,
            end_offset: text.len(),
        }]
    }

    /// Applies token filters
    fn apply_filters(&self, tokens: Vec<Token>) -> Vec<Token> {
        let mut result = tokens;

        for filter in &self.config.filters {
            result = self.apply_filter(result, filter);
        }

        // Apply length filters
        result = result
            .into_iter()
            .filter(|t| {
                t.text.len() >= self.config.min_token_length
                    && t.text.len() <= self.config.max_token_length
            })
            .collect();

        result
    }

    fn apply_filter(&self, tokens: Vec<Token>, filter: &TokenFilter) -> Vec<Token> {
        match filter {
            TokenFilter::Lowercase => tokens
                .into_iter()
                .map(|mut t| {
                    t.text = t.text.to_lowercase();
                    t
                })
                .collect(),

            TokenFilter::Uppercase => tokens
                .into_iter()
                .map(|mut t| {
                    t.text = t.text.to_uppercase();
                    t
                })
                .collect(),

            TokenFilter::StopWords => tokens
                .into_iter()
                .filter(|t| !self.config.stop_words.contains(&t.text.to_lowercase()))
                .collect(),

            TokenFilter::Stemmer => {
                if self.config.stemming {
                    tokens
                        .into_iter()
                        .map(|mut t| {
                            t.text = stem_word(&t.text, &self.config.language);
                            t
                        })
                        .collect()
                } else {
                    tokens
                }
            }

            TokenFilter::Trim => tokens
                .into_iter()
                .map(|mut t| {
                    t.text = t.text.trim().to_string();
                    t
                })
                .filter(|t| !t.text.is_empty())
                .collect(),

            TokenFilter::AsciiFolding => tokens
                .into_iter()
                .map(|mut t| {
                    t.text = fold_to_ascii(&t.text);
                    t
                })
                .collect(),

            TokenFilter::Deduplicate => {
                let mut seen = HashSet::new();
                tokens
                    .into_iter()
                    .filter(|t| seen.insert(t.text.clone()))
                    .collect()
            }

            TokenFilter::Synonyms(synonyms) => {
                let mut result = Vec::new();
                for token in tokens {
                    result.push(token.clone());
                    for (word, syns) in synonyms {
                        if token.text.eq_ignore_ascii_case(word) {
                            for syn in syns {
                                result.push(Token {
                                    text: syn.clone(),
                                    position: token.position,
                                    start_offset: token.start_offset,
                                    end_offset: token.end_offset,
                                });
                            }
                        }
                    }
                }
                result
            }

            TokenFilter::MinLength(min) => {
                tokens.into_iter().filter(|t| t.text.len() >= *min).collect()
            }

            TokenFilter::MaxLength(max) => {
                tokens.into_iter().filter(|t| t.text.len() <= *max).collect()
            }

            TokenFilter::Ngram { min, max } => {
                let mut result = Vec::new();
                for token in tokens {
                    let chars: Vec<char> = token.text.chars().collect();
                    for n in *min..=*max {
                        for i in 0..=chars.len().saturating_sub(n) {
                            let ngram: String = chars[i..i + n].iter().collect();
                            result.push(Token {
                                text: ngram,
                                position: token.position,
                                start_offset: token.start_offset + i,
                                end_offset: token.start_offset + i + n,
                            });
                        }
                    }
                }
                result
            }

            TokenFilter::EdgeNgram { min, max } => {
                let mut result = Vec::new();
                for token in tokens {
                    let chars: Vec<char> = token.text.chars().collect();
                    let actual_max = (*max).min(chars.len());
                    for n in *min..=actual_max {
                        let ngram: String = chars[..n].iter().collect();
                        result.push(Token {
                            text: ngram,
                            position: token.position,
                            start_offset: token.start_offset,
                            end_offset: token.start_offset + n,
                        });
                    }
                }
                result
            }
        }
    }
}

impl Analyzer for TextAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let tokens = self.tokenize(text);
        self.apply_filters(tokens)
    }
}

/// Default English stop words
fn default_stop_words() -> HashSet<String> {
    [
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Simple Porter-like stemmer
fn stem_word(word: &str, _language: &str) -> String {
    let mut result = word.to_lowercase();

    // Simple suffix removal rules
    let suffixes = ["ing", "ed", "ly", "ness", "ment", "tion", "sion", "able", "ible", "er", "est", "ful", "less", "ous", "ive", "ize", "ise"];

    for suffix in &suffixes {
        if result.len() > suffix.len() + 2 && result.ends_with(suffix) {
            result = result[..result.len() - suffix.len()].to_string();
            break;
        }
    }

    // Remove plural 's' if word is long enough
    if result.len() > 3 && result.ends_with('s') && !result.ends_with("ss") {
        result.pop();
    }

    result
}

/// ASCII folding (remove accents)
fn fold_to_ascii(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' => 'a',
            'é' | 'è' | 'ê' | 'ë' => 'e',
            'í' | 'ì' | 'î' | 'ï' => 'i',
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' => 'o',
            'ú' | 'ù' | 'û' | 'ü' => 'u',
            'ñ' => 'n',
            'ç' => 'c',
            'ß' => 's',
            _ => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_tokenize() {
        let analyzer = TextAnalyzer::default_analyzer();
        let tokens = analyzer.analyze("Hello World!");

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }

    #[test]
    fn test_stop_words() {
        let analyzer = TextAnalyzer::default_analyzer();
        let tokens = analyzer.analyze("the quick brown fox");

        // "the" should be removed as stop word
        assert!(!tokens.iter().any(|t| t.text == "the"));
        assert!(tokens.iter().any(|t| t.text == "quick"));
    }

    #[test]
    fn test_stemming() {
        let analyzer = TextAnalyzer::default_analyzer();
        let tokens = analyzer.analyze("running jumped testing");

        // Should stem to root forms
        assert!(tokens.iter().any(|t| t.text == "runn" || t.text == "run"));
    }

    #[test]
    fn test_path_hierarchy() {
        let config = AnalyzerConfig {
            tokenizer: TokenizerType::PathHierarchy,
            filters: vec![],
            stemming: false,
            ..Default::default()
        };
        let analyzer = TextAnalyzer::new(config);
        let tokens = analyzer.analyze("/a/b/c");

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "a");
        assert_eq!(tokens[1].text, "a/b");
        assert_eq!(tokens[2].text, "a/b/c");
    }

    #[test]
    fn test_ngram() {
        let config = AnalyzerConfig {
            tokenizer: TokenizerType::Ngram,
            filters: vec![],
            min_token_length: 1,
            stemming: false,
            ..Default::default()
        };
        let analyzer = TextAnalyzer::new(config);
        let tokens = analyzer.analyze("abc");

        // 2-grams: "ab", "bc"; 3-grams: "abc"
        assert!(tokens.iter().any(|t| t.text == "ab"));
        assert!(tokens.iter().any(|t| t.text == "bc"));
        assert!(tokens.iter().any(|t| t.text == "abc"));
    }
}
