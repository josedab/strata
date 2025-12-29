// Tiering policies and rules

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Storage tier types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierType {
    /// Hot tier - NVMe/SSD, fastest access
    #[default]
    Hot,
    /// Warm tier - HDD, balanced cost/performance
    Warm,
    /// Cold tier - Object storage, infrequent access
    Cold,
    /// Archive tier - Tape/Glacier, long-term retention
    Archive,
}

impl TierType {
    /// Returns the relative cost per GB (1.0 = baseline)
    pub fn cost_factor(&self) -> f64 {
        match self {
            TierType::Hot => 10.0,
            TierType::Warm => 3.0,
            TierType::Cold => 1.0,
            TierType::Archive => 0.3,
        }
    }

    /// Returns the relative latency (1.0 = fastest)
    pub fn latency_factor(&self) -> f64 {
        match self {
            TierType::Hot => 1.0,
            TierType::Warm => 5.0,
            TierType::Cold => 50.0,
            TierType::Archive => 1000.0, // May require retrieval
        }
    }

    /// Returns the relative throughput (1.0 = highest)
    pub fn throughput_factor(&self) -> f64 {
        match self {
            TierType::Hot => 1.0,
            TierType::Warm => 0.5,
            TierType::Cold => 0.1,
            TierType::Archive => 0.01,
        }
    }

    /// Next tier in demotion order
    pub fn demote(&self) -> Option<TierType> {
        match self {
            TierType::Hot => Some(TierType::Warm),
            TierType::Warm => Some(TierType::Cold),
            TierType::Cold => Some(TierType::Archive),
            TierType::Archive => None,
        }
    }

    /// Previous tier in promotion order
    pub fn promote(&self) -> Option<TierType> {
        match self {
            TierType::Archive => Some(TierType::Cold),
            TierType::Cold => Some(TierType::Warm),
            TierType::Warm => Some(TierType::Hot),
            TierType::Hot => None,
        }
    }
}

/// Tiering policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Policy name
    pub name: String,
    /// Policy description
    pub description: Option<String>,
    /// Is policy enabled
    pub enabled: bool,
    /// Rules to evaluate (in order)
    pub rules: Vec<TieringRule>,
    /// Default tier for new data
    pub default_tier: TierType,
    /// Minimum data age before tiering (prevents thrashing)
    #[serde(with = "crate::config::humantime_serde")]
    pub min_age: Duration,
    /// How often to evaluate tiering decisions
    #[serde(with = "crate::config::humantime_serde")]
    pub evaluation_interval: Duration,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            description: Some("Default tiering policy".to_string()),
            enabled: true,
            rules: vec![
                TieringRule::age_based("archive-old", TierType::Archive, Duration::from_secs(365 * 24 * 3600)),
                TieringRule::age_based("cold-old", TierType::Cold, Duration::from_secs(90 * 24 * 3600)),
                TieringRule::age_based("warm-old", TierType::Warm, Duration::from_secs(30 * 24 * 3600)),
            ],
            default_tier: TierType::Hot,
            min_age: Duration::from_secs(24 * 3600), // 1 day
            evaluation_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

impl TieringPolicy {
    /// Creates a new policy
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Creates a cost-optimized policy (aggressive tiering)
    pub fn cost_optimized() -> Self {
        Self {
            name: "cost-optimized".to_string(),
            description: Some("Aggressive tiering to minimize storage costs".to_string()),
            enabled: true,
            rules: vec![
                TieringRule::age_based("archive-old", TierType::Archive, Duration::from_secs(90 * 24 * 3600)),
                TieringRule::age_based("cold-old", TierType::Cold, Duration::from_secs(30 * 24 * 3600)),
                TieringRule::age_based("warm-old", TierType::Warm, Duration::from_secs(7 * 24 * 3600)),
                TieringRule::access_based("cold-unused", TierType::Cold, 0, Duration::from_secs(14 * 24 * 3600)),
            ],
            default_tier: TierType::Warm,
            min_age: Duration::from_secs(6 * 3600),
            evaluation_interval: Duration::from_secs(1800),
        }
    }

    /// Creates a performance-optimized policy (keep data hot)
    pub fn performance_optimized() -> Self {
        Self {
            name: "performance-optimized".to_string(),
            description: Some("Keep frequently accessed data on fast storage".to_string()),
            enabled: true,
            rules: vec![
                TieringRule::access_based("promote-active", TierType::Hot, 10, Duration::from_secs(24 * 3600)),
                TieringRule::age_based("archive-old", TierType::Archive, Duration::from_secs(365 * 24 * 3600)),
                TieringRule::age_based("cold-old", TierType::Cold, Duration::from_secs(180 * 24 * 3600)),
            ],
            default_tier: TierType::Hot,
            min_age: Duration::from_secs(7 * 24 * 3600),
            evaluation_interval: Duration::from_secs(7200),
        }
    }

    /// Creates a compliance-focused policy (retain in archive)
    pub fn compliance() -> Self {
        Self {
            name: "compliance".to_string(),
            description: Some("Long-term retention for compliance".to_string()),
            enabled: true,
            rules: vec![
                TieringRule::age_based("archive-compliance", TierType::Archive, Duration::from_secs(30 * 24 * 3600)),
            ],
            default_tier: TierType::Warm,
            min_age: Duration::from_secs(24 * 3600),
            evaluation_interval: Duration::from_secs(86400),
        }
    }

    /// Adds a rule to the policy
    pub fn with_rule(mut self, rule: TieringRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Sets the default tier
    pub fn with_default_tier(mut self, tier: TierType) -> Self {
        self.default_tier = tier;
        self
    }

    /// Evaluates which tier data should be in
    pub fn evaluate(&self, stats: &DataStats) -> TierEvaluation {
        if !self.enabled {
            return TierEvaluation {
                recommended_tier: stats.current_tier,
                reason: "Policy disabled".to_string(),
                matched_rule: None,
                confidence: 1.0,
            };
        }

        // Check minimum age
        if stats.age < self.min_age {
            return TierEvaluation {
                recommended_tier: stats.current_tier,
                reason: format!("Data too new (age: {:?}, min: {:?})", stats.age, self.min_age),
                matched_rule: None,
                confidence: 1.0,
            };
        }

        // Evaluate rules in order
        for rule in &self.rules {
            if let Some(tier) = rule.evaluate(stats) {
                return TierEvaluation {
                    recommended_tier: tier,
                    reason: rule.description.clone().unwrap_or_else(|| rule.name.clone()),
                    matched_rule: Some(rule.name.clone()),
                    confidence: rule.confidence,
                };
            }
        }

        // Default tier
        TierEvaluation {
            recommended_tier: self.default_tier,
            reason: "No rules matched, using default".to_string(),
            matched_rule: None,
            confidence: 0.5,
        }
    }
}

/// Individual tiering rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringRule {
    /// Rule name
    pub name: String,
    /// Rule description
    pub description: Option<String>,
    /// Target tier
    pub target_tier: TierType,
    /// Rule condition
    pub condition: RuleCondition,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
    /// Rule priority (lower = higher priority)
    pub priority: i32,
}

impl TieringRule {
    /// Creates an age-based rule
    pub fn age_based(name: impl Into<String>, tier: TierType, age: Duration) -> Self {
        Self {
            name: name.into(),
            description: Some(format!("Move to {:?} after {:?}", tier, age)),
            target_tier: tier,
            condition: RuleCondition::Age { min_age: age },
            confidence: 0.9,
            priority: 0,
        }
    }

    /// Creates an access-based rule
    pub fn access_based(name: impl Into<String>, tier: TierType, min_accesses: u64, window: Duration) -> Self {
        Self {
            name: name.into(),
            description: Some(format!("Move to {:?} with {} accesses in {:?}", tier, min_accesses, window)),
            target_tier: tier,
            condition: RuleCondition::AccessCount { min_count: min_accesses, window },
            confidence: 0.8,
            priority: 0,
        }
    }

    /// Creates a size-based rule
    pub fn size_based(name: impl Into<String>, tier: TierType, min_size: u64) -> Self {
        Self {
            name: name.into(),
            description: Some(format!("Move objects >= {} bytes to {:?}", min_size, tier)),
            target_tier: tier,
            condition: RuleCondition::Size { min_size },
            confidence: 0.7,
            priority: 0,
        }
    }

    /// Creates a pattern-based rule
    pub fn pattern_based(name: impl Into<String>, tier: TierType, pattern: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            target_tier: tier,
            condition: RuleCondition::PathPattern { pattern: pattern.into() },
            confidence: 1.0,
            priority: -10, // Higher priority
        }
    }

    /// Creates a composite rule with multiple conditions (all must match)
    pub fn composite(name: impl Into<String>, tier: TierType, conditions: Vec<RuleCondition>) -> Self {
        Self {
            name: name.into(),
            description: None,
            target_tier: tier,
            condition: RuleCondition::And(conditions),
            confidence: 0.85,
            priority: 0,
        }
    }

    /// Evaluates the rule against data stats
    pub fn evaluate(&self, stats: &DataStats) -> Option<TierType> {
        if self.condition.matches(stats) {
            Some(self.target_tier)
        } else {
            None
        }
    }
}

/// Rule conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleCondition {
    /// Based on data age
    Age { min_age: Duration },
    /// Based on access count in time window
    AccessCount { min_count: u64, window: Duration },
    /// Based on last access time
    LastAccess { not_accessed_for: Duration },
    /// Based on object size
    Size { min_size: u64 },
    /// Based on path pattern (glob)
    PathPattern { pattern: String },
    /// Based on content type
    ContentType { types: Vec<String> },
    /// Based on custom metadata tag
    Tag { key: String, value: Option<String> },
    /// All conditions must match
    And(Vec<RuleCondition>),
    /// Any condition must match
    Or(Vec<RuleCondition>),
    /// Negate condition
    Not(Box<RuleCondition>),
}

impl RuleCondition {
    /// Evaluates whether the condition matches
    pub fn matches(&self, stats: &DataStats) -> bool {
        match self {
            RuleCondition::Age { min_age } => stats.age >= *min_age,
            RuleCondition::AccessCount { min_count, window } => {
                let accesses = stats.access_count_in_window(*window);
                if *min_count == 0 {
                    accesses == 0
                } else {
                    accesses >= *min_count
                }
            }
            RuleCondition::LastAccess { not_accessed_for } => {
                stats.time_since_last_access >= *not_accessed_for
            }
            RuleCondition::Size { min_size } => stats.size >= *min_size,
            RuleCondition::PathPattern { pattern } => {
                glob_match(pattern, &stats.path)
            }
            RuleCondition::ContentType { types } => {
                stats.content_type.as_ref()
                    .map(|ct| types.iter().any(|t| ct.contains(t)))
                    .unwrap_or(false)
            }
            RuleCondition::Tag { key, value } => {
                stats.tags.get(key)
                    .map(|v| value.as_ref().map(|expected| v == expected).unwrap_or(true))
                    .unwrap_or(false)
            }
            RuleCondition::And(conditions) => {
                conditions.iter().all(|c| c.matches(stats))
            }
            RuleCondition::Or(conditions) => {
                conditions.iter().any(|c| c.matches(stats))
            }
            RuleCondition::Not(condition) => !condition.matches(stats),
        }
    }
}

/// Data statistics for tiering decisions
#[derive(Debug, Clone)]
pub struct DataStats {
    /// Object/file path
    pub path: String,
    /// Object size in bytes
    pub size: u64,
    /// Current storage tier
    pub current_tier: TierType,
    /// Data age (since creation)
    pub age: Duration,
    /// Time since last access
    pub time_since_last_access: Duration,
    /// Access history (timestamps)
    pub access_history: Vec<std::time::SystemTime>,
    /// Content type (MIME)
    pub content_type: Option<String>,
    /// Object tags/metadata
    pub tags: HashMap<String, String>,
    /// Number of versions
    pub version_count: u32,
    /// Is data compressed
    pub is_compressed: bool,
    /// Is data encrypted
    pub is_encrypted: bool,
}

impl DataStats {
    /// Counts accesses within a time window
    pub fn access_count_in_window(&self, window: Duration) -> u64 {
        let now = std::time::SystemTime::now();
        let cutoff = now - window;
        self.access_history.iter()
            .filter(|&t| *t >= cutoff)
            .count() as u64
    }

    /// Calculates access frequency (accesses per day)
    pub fn access_frequency(&self) -> f64 {
        if self.access_history.is_empty() {
            return 0.0;
        }
        let days = self.age.as_secs_f64() / 86400.0;
        if days < 0.001 {
            return 0.0;
        }
        self.access_history.len() as f64 / days
    }
}

/// Result of tier evaluation
#[derive(Debug, Clone)]
pub struct TierEvaluation {
    /// Recommended storage tier
    pub recommended_tier: TierType,
    /// Reason for recommendation
    pub reason: String,
    /// Which rule matched (if any)
    pub matched_rule: Option<String>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
}

impl TierEvaluation {
    /// Whether a tier change is recommended
    pub fn should_migrate(&self, current_tier: TierType) -> bool {
        self.recommended_tier != current_tier && self.confidence >= 0.5
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, path: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let path_parts: Vec<&str> = path.split('/').collect();

    glob_match_parts(&pattern_parts, &path_parts)
}

fn glob_match_parts(pattern: &[&str], path: &[&str]) -> bool {
    if pattern.is_empty() {
        return path.is_empty();
    }

    if pattern[0] == "**" {
        // Match any number of path segments
        for i in 0..=path.len() {
            if glob_match_parts(&pattern[1..], &path[i..]) {
                return true;
            }
        }
        return false;
    }

    if path.is_empty() {
        return false;
    }

    if glob_match_segment(pattern[0], path[0]) {
        glob_match_parts(&pattern[1..], &path[1..])
    } else {
        false
    }
}

fn glob_match_segment(pattern: &str, segment: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let pattern_chars: Vec<char> = pattern.chars().collect();
    let segment_chars: Vec<char> = segment.chars().collect();

    glob_match_chars(&pattern_chars, &segment_chars)
}

fn glob_match_chars(pattern: &[char], text: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        '*' => {
            // Match zero or more characters
            for i in 0..=text.len() {
                if glob_match_chars(&pattern[1..], &text[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            // Match exactly one character
            !text.is_empty() && glob_match_chars(&pattern[1..], &text[1..])
        }
        c => {
            // Match literal character
            !text.is_empty() && text[0] == c && glob_match_chars(&pattern[1..], &text[1..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_cost_factors() {
        assert!(TierType::Hot.cost_factor() > TierType::Warm.cost_factor());
        assert!(TierType::Warm.cost_factor() > TierType::Cold.cost_factor());
        assert!(TierType::Cold.cost_factor() > TierType::Archive.cost_factor());
    }

    #[test]
    fn test_tier_promotion_demotion() {
        assert_eq!(TierType::Hot.demote(), Some(TierType::Warm));
        assert_eq!(TierType::Archive.demote(), None);
        assert_eq!(TierType::Cold.promote(), Some(TierType::Warm));
        assert_eq!(TierType::Hot.promote(), None);
    }

    #[test]
    fn test_glob_matching() {
        assert!(glob_match("*.txt", "file.txt"));
        assert!(!glob_match("*.txt", "file.log"));
        assert!(glob_match("logs/**/*.log", "logs/2024/01/app.log"));
        assert!(glob_match("data/temp/*", "data/temp/file.tmp"));
    }

    #[test]
    fn test_age_rule() {
        let rule = TieringRule::age_based("test", TierType::Cold, Duration::from_secs(30 * 24 * 3600));

        let stats = DataStats {
            path: "/test/file.txt".to_string(),
            size: 1024,
            current_tier: TierType::Hot,
            age: Duration::from_secs(60 * 24 * 3600), // 60 days
            time_since_last_access: Duration::from_secs(30 * 24 * 3600),
            access_history: vec![],
            content_type: None,
            tags: HashMap::new(),
            version_count: 1,
            is_compressed: false,
            is_encrypted: false,
        };

        assert_eq!(rule.evaluate(&stats), Some(TierType::Cold));
    }

    #[test]
    fn test_policy_evaluation() {
        let policy = TieringPolicy::default();

        let stats = DataStats {
            path: "/test/old-file.txt".to_string(),
            size: 1024,
            current_tier: TierType::Hot,
            age: Duration::from_secs(400 * 24 * 3600), // > 1 year
            time_since_last_access: Duration::from_secs(300 * 24 * 3600),
            access_history: vec![],
            content_type: None,
            tags: HashMap::new(),
            version_count: 1,
            is_compressed: false,
            is_encrypted: false,
        };

        let eval = policy.evaluate(&stats);
        assert_eq!(eval.recommended_tier, TierType::Archive);
        assert!(eval.confidence > 0.5);
    }
}
