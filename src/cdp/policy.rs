// Retention policies for continuous data protection

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Type of retention policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyType {
    /// Time-based retention (keep for X duration)
    TimeBased,
    /// Count-based retention (keep last N versions)
    CountBased,
    /// Size-based retention (keep until total size exceeds X)
    SizeBased,
    /// Hybrid (combination of policies)
    Hybrid,
    /// Never delete (compliance/legal hold)
    Indefinite,
}

/// Retention policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Policy type
    pub policy_type: PolicyType,
    /// Retention duration (for time-based)
    pub retention_duration: Option<Duration>,
    /// Maximum versions to keep (for count-based)
    pub max_versions: Option<u32>,
    /// Maximum total size in bytes (for size-based)
    pub max_size_bytes: Option<u64>,
    /// Whether to keep at least one version always
    pub keep_minimum_one: bool,
    /// Priority (higher = more important)
    pub priority: u32,
    /// Scope: bucket pattern (glob)
    pub bucket_pattern: Option<String>,
    /// Scope: key prefix
    pub key_prefix: Option<String>,
    /// Tags for filtering
    pub tags: HashMap<String, String>,
    /// Created at
    pub created_at: SystemTime,
    /// Enabled
    pub enabled: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Default Policy".to_string(),
            policy_type: PolicyType::TimeBased,
            retention_duration: Some(Duration::from_secs(30 * 24 * 3600)), // 30 days
            max_versions: None,
            max_size_bytes: None,
            keep_minimum_one: true,
            priority: 100,
            bucket_pattern: None,
            key_prefix: None,
            tags: HashMap::new(),
            created_at: SystemTime::now(),
            enabled: true,
        }
    }
}

impl RetentionPolicy {
    /// Creates a new time-based policy
    pub fn time_based(name: &str, duration: Duration) -> Self {
        Self {
            name: name.to_string(),
            policy_type: PolicyType::TimeBased,
            retention_duration: Some(duration),
            ..Default::default()
        }
    }

    /// Creates a new count-based policy
    pub fn count_based(name: &str, max_versions: u32) -> Self {
        Self {
            name: name.to_string(),
            policy_type: PolicyType::CountBased,
            max_versions: Some(max_versions),
            retention_duration: None,
            ..Default::default()
        }
    }

    /// Creates a new size-based policy
    pub fn size_based(name: &str, max_size_bytes: u64) -> Self {
        Self {
            name: name.to_string(),
            policy_type: PolicyType::SizeBased,
            max_size_bytes: Some(max_size_bytes),
            retention_duration: None,
            ..Default::default()
        }
    }

    /// Creates an indefinite retention policy
    pub fn indefinite(name: &str) -> Self {
        Self {
            name: name.to_string(),
            policy_type: PolicyType::Indefinite,
            retention_duration: None,
            max_versions: None,
            max_size_bytes: None,
            ..Default::default()
        }
    }

    /// Sets the bucket pattern
    pub fn with_bucket_pattern(mut self, pattern: &str) -> Self {
        self.bucket_pattern = Some(pattern.to_string());
        self
    }

    /// Sets the key prefix
    pub fn with_key_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = Some(prefix.to_string());
        self
    }

    /// Sets priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Checks if this policy applies to a given bucket/key
    pub fn applies_to(&self, bucket: &str, key: &str) -> bool {
        // Check bucket pattern
        if let Some(ref pattern) = self.bucket_pattern {
            if !Self::glob_match(pattern, bucket) {
                return false;
            }
        }

        // Check key prefix
        if let Some(ref prefix) = self.key_prefix {
            if !key.starts_with(prefix) {
                return false;
            }
        }

        true
    }

    /// Simple glob matching (supports * and ?)
    fn glob_match(pattern: &str, text: &str) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut text_chars = text.chars().peekable();

        while let Some(p) = pattern_chars.next() {
            match p {
                '*' => {
                    // Match zero or more characters
                    if pattern_chars.peek().is_none() {
                        return true;
                    }
                    while text_chars.peek().is_some() {
                        let remaining_pattern: String = pattern_chars.clone().collect();
                        let remaining_text: String = text_chars.clone().collect();
                        if Self::glob_match(&remaining_pattern, &remaining_text) {
                            return true;
                        }
                        text_chars.next();
                    }
                    return false;
                }
                '?' => {
                    // Match exactly one character
                    if text_chars.next().is_none() {
                        return false;
                    }
                }
                c => {
                    // Match literal character
                    if text_chars.next() != Some(c) {
                        return false;
                    }
                }
            }
        }

        text_chars.peek().is_none()
    }

    /// Evaluates if a version should be retained based on this policy
    pub fn should_retain(&self, version_info: &VersionInfo, all_versions: &[VersionInfo]) -> bool {
        if !self.enabled {
            return true; // Disabled policies retain everything
        }

        match self.policy_type {
            PolicyType::Indefinite => true,

            PolicyType::TimeBased => {
                if let Some(duration) = self.retention_duration {
                    let cutoff = SystemTime::now() - duration;
                    version_info.created_at >= cutoff
                } else {
                    true
                }
            }

            PolicyType::CountBased => {
                if let Some(max) = self.max_versions {
                    // Find this version's position
                    let mut sorted = all_versions.to_vec();
                    sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));

                    if let Some(pos) = sorted.iter().position(|v| v.version_id == version_info.version_id) {
                        pos < max as usize
                    } else {
                        true
                    }
                } else {
                    true
                }
            }

            PolicyType::SizeBased => {
                if let Some(max_size) = self.max_size_bytes {
                    let mut sorted = all_versions.to_vec();
                    sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));

                    let mut total_size = 0u64;
                    for v in &sorted {
                        total_size += v.size;
                        if v.version_id == version_info.version_id {
                            return total_size <= max_size;
                        }
                    }
                    true
                } else {
                    true
                }
            }

            PolicyType::Hybrid => {
                // For hybrid, we check all applicable constraints
                let time_ok = self.retention_duration.map_or(true, |d| {
                    let cutoff = SystemTime::now() - d;
                    version_info.created_at >= cutoff
                });

                let count_ok = self.max_versions.map_or(true, |max| {
                    let mut sorted = all_versions.to_vec();
                    sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    sorted
                        .iter()
                        .position(|v| v.version_id == version_info.version_id)
                        .map(|pos| pos < max as usize)
                        .unwrap_or(true)
                });

                time_ok && count_ok
            }
        }
    }
}

/// Version information for retention evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Version ID
    pub version_id: String,
    /// Created at
    pub created_at: SystemTime,
    /// Size in bytes
    pub size: u64,
    /// Whether this is the current version
    pub is_current: bool,
}

/// Policy evaluation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEvaluation {
    /// Applied policy ID
    pub policy_id: String,
    /// Whether to retain
    pub retain: bool,
    /// Reason for decision
    pub reason: String,
    /// Expiration time (if not retained)
    pub expires_at: Option<SystemTime>,
}

/// Policy manager for CDP retention
pub struct PolicyManager {
    /// Registered policies
    policies: Arc<RwLock<Vec<RetentionPolicy>>>,
    /// Default policy
    default_policy: RetentionPolicy,
}

impl PolicyManager {
    /// Creates a new policy manager
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(Vec::new())),
            default_policy: RetentionPolicy::default(),
        }
    }

    /// Creates with a custom default policy
    pub fn with_default(default: RetentionPolicy) -> Self {
        Self {
            policies: Arc::new(RwLock::new(Vec::new())),
            default_policy: default,
        }
    }

    /// Adds a policy
    pub async fn add_policy(&self, policy: RetentionPolicy) {
        self.policies.write().await.push(policy);
    }

    /// Gets a policy by ID
    pub async fn get_policy(&self, id: &str) -> Option<RetentionPolicy> {
        self.policies
            .read()
            .await
            .iter()
            .find(|p| p.id == id)
            .cloned()
    }

    /// Updates a policy
    pub async fn update_policy(&self, policy: RetentionPolicy) -> bool {
        let mut policies = self.policies.write().await;
        if let Some(existing) = policies.iter_mut().find(|p| p.id == policy.id) {
            *existing = policy;
            true
        } else {
            false
        }
    }

    /// Deletes a policy
    pub async fn delete_policy(&self, id: &str) -> bool {
        let mut policies = self.policies.write().await;
        if let Some(pos) = policies.iter().position(|p| p.id == id) {
            policies.remove(pos);
            true
        } else {
            false
        }
    }

    /// Lists all policies
    pub async fn list_policies(&self) -> Vec<RetentionPolicy> {
        self.policies.read().await.clone()
    }

    /// Finds the applicable policy for a bucket/key
    pub async fn find_policy(&self, bucket: &str, key: &str) -> RetentionPolicy {
        let policies = self.policies.read().await;

        // Find all applicable policies
        let mut applicable: Vec<&RetentionPolicy> = policies
            .iter()
            .filter(|p| p.enabled && p.applies_to(bucket, key))
            .collect();

        // Sort by priority (highest first)
        applicable.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Return highest priority or default
        applicable.first().cloned().cloned().unwrap_or_else(|| self.default_policy.clone())
    }

    /// Evaluates retention for a version
    pub async fn evaluate(
        &self,
        bucket: &str,
        key: &str,
        version: &VersionInfo,
        all_versions: &[VersionInfo],
    ) -> PolicyEvaluation {
        let policy = self.find_policy(bucket, key).await;

        let retain = policy.should_retain(version, all_versions);

        let reason = if retain {
            format!("Retained by policy '{}' ({})", policy.name, policy.id)
        } else {
            format!("Expired per policy '{}' ({})", policy.name, policy.id)
        };

        let expires_at = if !retain {
            None
        } else if let Some(duration) = policy.retention_duration {
            Some(version.created_at + duration)
        } else {
            None
        };

        PolicyEvaluation {
            policy_id: policy.id,
            retain,
            reason,
            expires_at,
        }
    }

    /// Gets versions that should be cleaned up
    pub async fn get_expired_versions(
        &self,
        bucket: &str,
        key: &str,
        versions: &[VersionInfo],
    ) -> Vec<String> {
        let policy = self.find_policy(bucket, key).await;

        versions
            .iter()
            .filter(|v| !policy.should_retain(v, versions))
            .map(|v| v.version_id.clone())
            .collect()
    }
}

impl Default for PolicyManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Predefined policy templates
pub struct PolicyTemplates;

impl PolicyTemplates {
    /// Standard 30-day retention
    pub fn standard_30_day() -> RetentionPolicy {
        RetentionPolicy::time_based("Standard 30 Day", Duration::from_secs(30 * 24 * 3600))
    }

    /// Keep last 10 versions
    pub fn keep_last_10() -> RetentionPolicy {
        RetentionPolicy::count_based("Keep Last 10", 10)
    }

    /// Long-term archive (1 year)
    pub fn long_term_archive() -> RetentionPolicy {
        RetentionPolicy::time_based("Long-term Archive", Duration::from_secs(365 * 24 * 3600))
    }

    /// Compliance retention (7 years)
    pub fn compliance_7_year() -> RetentionPolicy {
        RetentionPolicy::time_based("Compliance 7-Year", Duration::from_secs(7 * 365 * 24 * 3600))
            .with_priority(1000)
    }

    /// Legal hold (indefinite)
    pub fn legal_hold() -> RetentionPolicy {
        RetentionPolicy::indefinite("Legal Hold").with_priority(10000)
    }

    /// Development (short retention)
    pub fn development() -> RetentionPolicy {
        RetentionPolicy::time_based("Development", Duration::from_secs(7 * 24 * 3600))
            .with_priority(10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = RetentionPolicy::time_based("Test", Duration::from_secs(3600));

        assert_eq!(policy.name, "Test");
        assert_eq!(policy.policy_type, PolicyType::TimeBased);
        assert_eq!(policy.retention_duration, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn test_policy_applies_to() {
        let policy = RetentionPolicy::default()
            .with_bucket_pattern("logs-*")
            .with_key_prefix("app/");

        assert!(policy.applies_to("logs-2024", "app/server.log"));
        assert!(!policy.applies_to("data-2024", "app/server.log"));
        assert!(!policy.applies_to("logs-2024", "other/file.txt"));
    }

    #[test]
    fn test_count_based_retention() {
        let policy = RetentionPolicy::count_based("Keep 3", 3);

        let versions: Vec<VersionInfo> = (0..5)
            .map(|i| VersionInfo {
                version_id: format!("v{}", i),
                created_at: SystemTime::now() - Duration::from_secs(i * 3600),
                size: 1000,
                is_current: i == 0,
            })
            .collect();

        // First 3 versions should be retained
        assert!(policy.should_retain(&versions[0], &versions));
        assert!(policy.should_retain(&versions[1], &versions));
        assert!(policy.should_retain(&versions[2], &versions));

        // Older versions should not
        assert!(!policy.should_retain(&versions[3], &versions));
        assert!(!policy.should_retain(&versions[4], &versions));
    }

    #[test]
    fn test_glob_matching() {
        assert!(RetentionPolicy::glob_match("*", "anything"));
        assert!(RetentionPolicy::glob_match("logs-*", "logs-2024"));
        assert!(!RetentionPolicy::glob_match("logs-*", "data-2024"));
        assert!(RetentionPolicy::glob_match("app-?", "app-1"));
        assert!(!RetentionPolicy::glob_match("app-?", "app-12"));
    }

    #[tokio::test]
    async fn test_policy_manager() {
        let manager = PolicyManager::new();

        let policy = RetentionPolicy::time_based("Custom", Duration::from_secs(86400))
            .with_bucket_pattern("important-*");

        manager.add_policy(policy.clone()).await;

        let found = manager.find_policy("important-data", "file.txt").await;
        assert_eq!(found.name, "Custom");

        // Non-matching bucket gets default
        let default = manager.find_policy("other-bucket", "file.txt").await;
        assert_eq!(default.name, "Default Policy");
    }

    #[test]
    fn test_policy_templates() {
        let standard = PolicyTemplates::standard_30_day();
        assert_eq!(
            standard.retention_duration,
            Some(Duration::from_secs(30 * 24 * 3600))
        );

        let legal = PolicyTemplates::legal_hold();
        assert_eq!(legal.policy_type, PolicyType::Indefinite);
    }
}
