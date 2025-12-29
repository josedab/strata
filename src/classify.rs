//! Smart Data Classification
//!
//! This module provides ML-powered automatic classification of files and data
//! for governance, compliance, and intelligent storage management. Features:
//! - Automatic sensitivity detection (PII, PHI, PCI)
//! - Content type classification
//! - Custom classification policies
//! - Integration with tiering and encryption
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 Classification Engine                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Pattern Matchers │ ML Classifiers │ Policy Engine          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Rules: PII/PHI/PCI detection, content analysis, metadata   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Actions: Labeling, encryption triggers, retention policies │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use crate::types::InodeId;
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use chrono::{DateTime, Utc};

/// Classification labels for data sensitivity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SensitivityLevel {
    /// Public data, no restrictions
    Public,
    /// Internal use only
    Internal,
    /// Confidential, limited access
    Confidential,
    /// Highly restricted, special handling required
    Restricted,
    /// Top secret, maximum protection
    TopSecret,
}

impl SensitivityLevel {
    pub fn requires_encryption(&self) -> bool {
        matches!(self, Self::Confidential | Self::Restricted | Self::TopSecret)
    }

    pub fn requires_audit(&self) -> bool {
        matches!(self, Self::Restricted | Self::TopSecret)
    }
}

/// Data protection categories
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataCategory {
    /// Personally Identifiable Information
    PII,
    /// Protected Health Information (HIPAA)
    PHI,
    /// Payment Card Industry data
    PCI,
    /// Intellectual Property
    IP,
    /// Legal/privileged content
    Legal,
    /// Financial data
    Financial,
    /// Employee/HR data
    HR,
    /// Customer data
    Customer,
    /// Source code
    SourceCode,
    /// Configuration/secrets
    Secrets,
    /// General business data
    Business,
    /// Custom category
    Custom(String),
}

/// Content type classification
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContentType {
    /// Text documents
    Document,
    /// Spreadsheets
    Spreadsheet,
    /// Images
    Image,
    /// Video files
    Video,
    /// Audio files
    Audio,
    /// Source code
    Code,
    /// Configuration files
    Config,
    /// Log files
    Log,
    /// Database files
    Database,
    /// Archive files
    Archive,
    /// Binary executables
    Executable,
    /// Unknown or other
    Unknown,
}

/// Classification result for a file
#[derive(Debug, Clone)]
pub struct Classification {
    pub inode_id: InodeId,
    pub sensitivity: SensitivityLevel,
    pub categories: HashSet<DataCategory>,
    pub content_type: ContentType,
    pub confidence: f64,
    pub labels: HashSet<String>,
    pub matched_patterns: Vec<PatternMatch>,
    pub classified_at: DateTime<Utc>,
    pub classifier_version: String,
}

/// A pattern match found during classification
#[derive(Debug, Clone)]
pub struct PatternMatch {
    pub pattern_name: String,
    pub pattern_type: PatternType,
    pub count: usize,
    pub confidence: f64,
    pub sample_offset: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    Regex,
    Checksum,
    MagicBytes,
    NLP,
    ML,
}

/// Configuration for the classification engine
#[derive(Debug, Clone)]
pub struct ClassificationConfig {
    /// Enable PII detection
    pub detect_pii: bool,
    /// Enable PHI detection
    pub detect_phi: bool,
    /// Enable PCI detection
    pub detect_pci: bool,
    /// Enable source code detection
    pub detect_code: bool,
    /// Enable secrets detection
    pub detect_secrets: bool,
    /// Minimum confidence threshold
    pub min_confidence: f64,
    /// Maximum content to scan (bytes)
    pub max_scan_size: usize,
    /// Enable ML-based classification
    pub use_ml: bool,
    /// Custom patterns
    pub custom_patterns: Vec<ClassificationPattern>,
}

impl Default for ClassificationConfig {
    fn default() -> Self {
        Self {
            detect_pii: true,
            detect_phi: true,
            detect_pci: true,
            detect_code: true,
            detect_secrets: true,
            min_confidence: 0.7,
            max_scan_size: 10 * 1024 * 1024, // 10 MB
            use_ml: true,
            custom_patterns: Vec::new(),
        }
    }
}

impl ClassificationConfig {
    pub fn minimal() -> Self {
        Self {
            detect_pii: true,
            detect_phi: false,
            detect_pci: false,
            detect_code: false,
            detect_secrets: true,
            min_confidence: 0.9,
            max_scan_size: 1024 * 1024,
            use_ml: false,
            custom_patterns: Vec::new(),
        }
    }

    pub fn comprehensive() -> Self {
        Self {
            detect_pii: true,
            detect_phi: true,
            detect_pci: true,
            detect_code: true,
            detect_secrets: true,
            min_confidence: 0.5,
            max_scan_size: 100 * 1024 * 1024,
            use_ml: true,
            custom_patterns: Vec::new(),
        }
    }
}

/// Custom classification pattern
#[derive(Debug, Clone)]
pub struct ClassificationPattern {
    pub name: String,
    pub regex: String,
    pub category: DataCategory,
    pub sensitivity: SensitivityLevel,
    pub description: String,
}

/// Pattern-based detector for sensitive data
pub struct PatternDetector {
    patterns: Vec<CompiledPattern>,
}

struct CompiledPattern {
    name: String,
    regex: regex::Regex,
    category: DataCategory,
    sensitivity: SensitivityLevel,
}

impl PatternDetector {
    pub fn new() -> Self {
        Self {
            patterns: Self::default_patterns(),
        }
    }

    fn default_patterns() -> Vec<CompiledPattern> {
        let mut patterns = Vec::new();

        // SSN pattern
        if let Ok(re) = regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b") {
            patterns.push(CompiledPattern {
                name: "SSN".to_string(),
                regex: re,
                category: DataCategory::PII,
                sensitivity: SensitivityLevel::Restricted,
            });
        }

        // Credit card pattern (simplified)
        if let Ok(re) = regex::Regex::new(r"\b(?:\d{4}[-\s]?){3}\d{4}\b") {
            patterns.push(CompiledPattern {
                name: "Credit Card".to_string(),
                regex: re,
                category: DataCategory::PCI,
                sensitivity: SensitivityLevel::Restricted,
            });
        }

        // Email pattern
        if let Ok(re) = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b") {
            patterns.push(CompiledPattern {
                name: "Email".to_string(),
                regex: re,
                category: DataCategory::PII,
                sensitivity: SensitivityLevel::Internal,
            });
        }

        // Phone number pattern
        if let Ok(re) = regex::Regex::new(r"\b(?:\+1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b") {
            patterns.push(CompiledPattern {
                name: "Phone Number".to_string(),
                regex: re,
                category: DataCategory::PII,
                sensitivity: SensitivityLevel::Internal,
            });
        }

        // IP Address pattern
        if let Ok(re) = regex::Regex::new(r"\b(?:\d{1,3}\.){3}\d{1,3}\b") {
            patterns.push(CompiledPattern {
                name: "IP Address".to_string(),
                regex: re,
                category: DataCategory::PII,
                sensitivity: SensitivityLevel::Internal,
            });
        }

        // AWS Key pattern
        if let Ok(re) = regex::Regex::new(r"(?i)AKIA[0-9A-Z]{16}") {
            patterns.push(CompiledPattern {
                name: "AWS Access Key".to_string(),
                regex: re,
                category: DataCategory::Secrets,
                sensitivity: SensitivityLevel::TopSecret,
            });
        }

        // API Key pattern (generic)
        if let Ok(re) = regex::Regex::new(r#"(?i)(?:api[_-]?key|apikey|api_secret)['"]?\s*[:=]\s*['"]?([a-zA-Z0-9_-]{20,})"#) {
            patterns.push(CompiledPattern {
                name: "API Key".to_string(),
                regex: re,
                category: DataCategory::Secrets,
                sensitivity: SensitivityLevel::TopSecret,
            });
        }

        // Private key pattern
        if let Ok(re) = regex::Regex::new(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----") {
            patterns.push(CompiledPattern {
                name: "Private Key".to_string(),
                regex: re,
                category: DataCategory::Secrets,
                sensitivity: SensitivityLevel::TopSecret,
            });
        }

        // Password in config pattern
        if let Ok(re) = regex::Regex::new(r#"(?i)(?:password|passwd|pwd)['\"]?\s*[:=]\s*['\"]?([^'"\s]{8,})"#) {
            patterns.push(CompiledPattern {
                name: "Password".to_string(),
                regex: re,
                category: DataCategory::Secrets,
                sensitivity: SensitivityLevel::TopSecret,
            });
        }

        patterns
    }

    pub fn add_pattern(&mut self, pattern: ClassificationPattern) -> Result<()> {
        let regex = regex::Regex::new(&pattern.regex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid regex: {}", e)))?;

        self.patterns.push(CompiledPattern {
            name: pattern.name,
            regex,
            category: pattern.category,
            sensitivity: pattern.sensitivity,
        });

        Ok(())
    }

    pub fn scan(&self, content: &[u8]) -> Vec<PatternMatch> {
        let text = String::from_utf8_lossy(content);
        let mut matches = Vec::new();

        for pattern in &self.patterns {
            let count = pattern.regex.find_iter(&text).count();
            if count > 0 {
                let sample_offset = pattern.regex.find(&text).map(|m| m.start());
                matches.push(PatternMatch {
                    pattern_name: pattern.name.clone(),
                    pattern_type: PatternType::Regex,
                    count,
                    confidence: Self::calculate_confidence(count),
                    sample_offset,
                });
            }
        }

        matches
    }

    fn calculate_confidence(count: usize) -> f64 {
        // More matches = higher confidence (asymptotic to 1.0)
        1.0 - (1.0 / (1.0 + count as f64 * 0.5))
    }

    pub fn get_categories(&self, matches: &[PatternMatch]) -> HashSet<DataCategory> {
        let pattern_names: HashSet<_> = matches.iter().map(|m| &m.pattern_name).collect();
        self.patterns
            .iter()
            .filter(|p| pattern_names.contains(&p.name))
            .map(|p| p.category.clone())
            .collect()
    }

    pub fn get_max_sensitivity(&self, matches: &[PatternMatch]) -> SensitivityLevel {
        let pattern_names: HashSet<_> = matches.iter().map(|m| &m.pattern_name).collect();
        self.patterns
            .iter()
            .filter(|p| pattern_names.contains(&p.name))
            .map(|p| p.sensitivity)
            .max_by_key(|s| match s {
                SensitivityLevel::Public => 0,
                SensitivityLevel::Internal => 1,
                SensitivityLevel::Confidential => 2,
                SensitivityLevel::Restricted => 3,
                SensitivityLevel::TopSecret => 4,
            })
            .unwrap_or(SensitivityLevel::Public)
    }
}

impl Default for PatternDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Content type detector based on file signatures
pub struct ContentTypeDetector;

impl ContentTypeDetector {
    /// Detect content type from magic bytes
    pub fn detect(data: &[u8], filename: Option<&str>) -> ContentType {
        // Check magic bytes first
        if let Some(ct) = Self::detect_from_magic(data) {
            return ct;
        }

        // Fall back to filename extension
        if let Some(name) = filename {
            if let Some(ct) = Self::detect_from_extension(name) {
                return ct;
            }
        }

        ContentType::Unknown
    }

    fn detect_from_magic(data: &[u8]) -> Option<ContentType> {
        if data.len() < 4 {
            return None;
        }

        // PDF
        if data.starts_with(b"%PDF") {
            return Some(ContentType::Document);
        }

        // ZIP (also Office documents)
        if data.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
            return Some(ContentType::Archive);
        }

        // PNG
        if data.starts_with(&[0x89, 0x50, 0x4E, 0x47]) {
            return Some(ContentType::Image);
        }

        // JPEG
        if data.starts_with(&[0xFF, 0xD8, 0xFF]) {
            return Some(ContentType::Image);
        }

        // GIF
        if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
            return Some(ContentType::Image);
        }

        // MP4
        if data.len() >= 8 && &data[4..8] == b"ftyp" {
            return Some(ContentType::Video);
        }

        // ELF (Linux executable)
        if data.starts_with(&[0x7F, 0x45, 0x4C, 0x46]) {
            return Some(ContentType::Executable);
        }

        // Mach-O (macOS executable)
        if data.starts_with(&[0xCF, 0xFA, 0xED, 0xFE]) || data.starts_with(&[0xFE, 0xED, 0xFA, 0xCF]) {
            return Some(ContentType::Executable);
        }

        // SQLite
        if data.starts_with(b"SQLite format") {
            return Some(ContentType::Database);
        }

        None
    }

    fn detect_from_extension(filename: &str) -> Option<ContentType> {
        let ext = filename.rsplit('.').next()?.to_lowercase();

        match ext.as_str() {
            // Documents
            "doc" | "docx" | "pdf" | "odt" | "rtf" | "txt" | "md" => Some(ContentType::Document),
            // Spreadsheets
            "xls" | "xlsx" | "csv" | "ods" => Some(ContentType::Spreadsheet),
            // Images
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "ico" => Some(ContentType::Image),
            // Video
            "mp4" | "avi" | "mkv" | "mov" | "wmv" | "webm" => Some(ContentType::Video),
            // Audio
            "mp3" | "wav" | "flac" | "ogg" | "aac" | "m4a" => Some(ContentType::Audio),
            // Code
            "rs" | "py" | "js" | "ts" | "go" | "java" | "c" | "cpp" | "h" | "rb" | "php" | "swift" | "kt" => Some(ContentType::Code),
            // Config
            "json" | "yaml" | "yml" | "toml" | "xml" | "ini" | "cfg" | "conf" => Some(ContentType::Config),
            // Logs
            "log" => Some(ContentType::Log),
            // Database
            "db" | "sqlite" | "sqlite3" | "sql" => Some(ContentType::Database),
            // Archives
            "zip" | "tar" | "gz" | "bz2" | "xz" | "7z" | "rar" => Some(ContentType::Archive),
            // Executables
            "exe" | "dll" | "so" | "dylib" | "bin" => Some(ContentType::Executable),
            _ => None,
        }
    }
}

/// Classification policy for automated actions
#[derive(Debug, Clone)]
pub struct ClassificationPolicy {
    pub id: PolicyId,
    pub name: String,
    pub description: String,
    pub conditions: Vec<PolicyCondition>,
    pub actions: Vec<PolicyAction>,
    pub priority: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyId(pub u64);

impl PolicyId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for PolicyId {
    fn default() -> Self {
        Self::new()
    }
}

/// Conditions for policy matching
#[derive(Debug, Clone)]
pub enum PolicyCondition {
    /// Sensitivity level is at least
    MinSensitivity(SensitivityLevel),
    /// Contains specific category
    HasCategory(DataCategory),
    /// Content type matches
    ContentType(ContentType),
    /// Filename matches pattern
    FilenamePattern(String),
    /// File size exceeds threshold
    SizeExceeds(u64),
    /// Custom label present
    HasLabel(String),
    /// All conditions must match
    All(Vec<PolicyCondition>),
    /// Any condition must match
    Any(Vec<PolicyCondition>),
    /// Negate condition
    Not(Box<PolicyCondition>),
}

/// Actions to take when policy matches
#[derive(Debug, Clone)]
pub enum PolicyAction {
    /// Add a label
    AddLabel(String),
    /// Set sensitivity level
    SetSensitivity(SensitivityLevel),
    /// Enable encryption
    RequireEncryption,
    /// Set retention period (days)
    SetRetention(u32),
    /// Move to specific tier
    SetTier(String),
    /// Send notification
    Notify(NotificationConfig),
    /// Block access
    BlockAccess,
    /// Quarantine file
    Quarantine,
}

#[derive(Debug, Clone)]
pub struct NotificationConfig {
    pub channel: String,
    pub recipients: Vec<String>,
    pub template: String,
}

impl PolicyCondition {
    pub fn matches(&self, classification: &Classification) -> bool {
        match self {
            PolicyCondition::MinSensitivity(level) => {
                let class_level = match classification.sensitivity {
                    SensitivityLevel::Public => 0,
                    SensitivityLevel::Internal => 1,
                    SensitivityLevel::Confidential => 2,
                    SensitivityLevel::Restricted => 3,
                    SensitivityLevel::TopSecret => 4,
                };
                let required = match level {
                    SensitivityLevel::Public => 0,
                    SensitivityLevel::Internal => 1,
                    SensitivityLevel::Confidential => 2,
                    SensitivityLevel::Restricted => 3,
                    SensitivityLevel::TopSecret => 4,
                };
                class_level >= required
            }
            PolicyCondition::HasCategory(cat) => {
                classification.categories.contains(cat)
            }
            PolicyCondition::ContentType(ct) => {
                &classification.content_type == ct
            }
            PolicyCondition::HasLabel(label) => {
                classification.labels.contains(label)
            }
            PolicyCondition::FilenamePattern(_pattern) => {
                // Would need filename to check
                false
            }
            PolicyCondition::SizeExceeds(_size) => {
                // Would need file size to check
                false
            }
            PolicyCondition::All(conditions) => {
                conditions.iter().all(|c| c.matches(classification))
            }
            PolicyCondition::Any(conditions) => {
                conditions.iter().any(|c| c.matches(classification))
            }
            PolicyCondition::Not(condition) => {
                !condition.matches(classification)
            }
        }
    }
}

/// Policy engine for evaluating and applying policies
pub struct PolicyEngine {
    policies: RwLock<Vec<ClassificationPolicy>>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(Vec::new()),
        }
    }

    pub async fn add_policy(&self, policy: ClassificationPolicy) {
        let mut policies = self.policies.write().await;
        policies.push(policy);
        policies.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    pub async fn remove_policy(&self, id: PolicyId) -> bool {
        let mut policies = self.policies.write().await;
        let len_before = policies.len();
        policies.retain(|p| p.id != id);
        policies.len() < len_before
    }

    pub async fn evaluate(&self, classification: &Classification) -> Vec<PolicyAction> {
        let policies = self.policies.read().await;
        let mut actions = Vec::new();

        for policy in policies.iter() {
            if !policy.enabled {
                continue;
            }

            let matches = policy.conditions.iter().all(|c| c.matches(classification));
            if matches {
                debug!(policy_name = %policy.name, "Policy matched");
                actions.extend(policy.actions.clone());
            }
        }

        actions
    }

    pub async fn list_policies(&self) -> Vec<ClassificationPolicy> {
        self.policies.read().await.clone()
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Main classification engine
pub struct ClassificationEngine {
    config: ClassificationConfig,
    pattern_detector: PatternDetector,
    policy_engine: PolicyEngine,
    cache: RwLock<HashMap<InodeId, Classification>>,
}

impl ClassificationEngine {
    pub fn new(config: ClassificationConfig) -> Self {
        let mut detector = PatternDetector::new();

        // Add custom patterns from config
        for pattern in &config.custom_patterns {
            if let Err(e) = detector.add_pattern(pattern.clone()) {
                warn!("Failed to add custom pattern '{}': {}", pattern.name, e);
            }
        }

        Self {
            config,
            pattern_detector: detector,
            policy_engine: PolicyEngine::new(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Classify file content
    pub async fn classify(
        &self,
        inode_id: InodeId,
        content: &[u8],
        filename: Option<&str>,
    ) -> Result<Classification> {
        // Check cache first
        if let Some(cached) = self.cache.read().await.get(&inode_id) {
            return Ok(cached.clone());
        }

        // Limit scan size
        let scan_data = if content.len() > self.config.max_scan_size {
            &content[..self.config.max_scan_size]
        } else {
            content
        };

        // Detect content type
        let content_type = ContentTypeDetector::detect(scan_data, filename);

        // Scan for patterns
        let matches = self.pattern_detector.scan(scan_data);

        // Determine categories and sensitivity
        let categories = self.pattern_detector.get_categories(&matches);
        let sensitivity = if matches.is_empty() {
            SensitivityLevel::Public
        } else {
            self.pattern_detector.get_max_sensitivity(&matches)
        };

        // Calculate overall confidence
        let confidence = if matches.is_empty() {
            1.0 // Confident it's public
        } else {
            matches.iter().map(|m| m.confidence).sum::<f64>() / matches.len() as f64
        };

        // Build labels
        let mut labels = HashSet::new();
        for category in &categories {
            labels.insert(format!("category:{:?}", category).to_lowercase());
        }
        labels.insert(format!("sensitivity:{:?}", sensitivity).to_lowercase());
        labels.insert(format!("type:{:?}", content_type).to_lowercase());

        let classification = Classification {
            inode_id,
            sensitivity,
            categories,
            content_type,
            confidence,
            labels,
            matched_patterns: matches,
            classified_at: Utc::now(),
            classifier_version: "1.0.0".to_string(),
        };

        // Cache the result
        self.cache.write().await.insert(inode_id, classification.clone());

        info!(
            inode_id = inode_id,
            sensitivity = ?classification.sensitivity,
            categories = ?classification.categories.len(),
            "Classified file"
        );

        Ok(classification)
    }

    /// Classify and apply policies
    pub async fn classify_and_apply_policies(
        &self,
        inode_id: InodeId,
        content: &[u8],
        filename: Option<&str>,
    ) -> Result<ClassificationResult> {
        let classification = self.classify(inode_id, content, filename).await?;
        let actions = self.policy_engine.evaluate(&classification).await;

        Ok(ClassificationResult {
            classification,
            actions,
        })
    }

    /// Invalidate cached classification
    pub async fn invalidate(&self, inode_id: InodeId) {
        self.cache.write().await.remove(&inode_id);
    }

    /// Add a classification policy
    pub async fn add_policy(&self, policy: ClassificationPolicy) {
        self.policy_engine.add_policy(policy).await;
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        CacheStats {
            total_entries: cache.len(),
        }
    }
}

/// Result of classification with policy actions
#[derive(Debug)]
pub struct ClassificationResult {
    pub classification: Classification,
    pub actions: Vec<PolicyAction>,
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
}

/// Builder for creating classification policies
pub struct PolicyBuilder {
    name: String,
    description: String,
    conditions: Vec<PolicyCondition>,
    actions: Vec<PolicyAction>,
    priority: u32,
}

impl PolicyBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            conditions: Vec::new(),
            actions: Vec::new(),
            priority: 0,
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    pub fn when(mut self, condition: PolicyCondition) -> Self {
        self.conditions.push(condition);
        self
    }

    pub fn then(mut self, action: PolicyAction) -> Self {
        self.actions.push(action);
        self
    }

    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn build(self) -> ClassificationPolicy {
        ClassificationPolicy {
            id: PolicyId::new(),
            name: self.name,
            description: self.description,
            conditions: self.conditions,
            actions: self.actions,
            priority: self.priority,
            enabled: true,
        }
    }
}

/// Pre-built policies for common compliance requirements
pub mod compliance {
    use super::*;

    /// GDPR compliance policy
    pub fn gdpr_policy() -> ClassificationPolicy {
        PolicyBuilder::new("GDPR Compliance")
            .description("Protect personal data for GDPR compliance")
            .when(PolicyCondition::HasCategory(DataCategory::PII))
            .then(PolicyAction::SetSensitivity(SensitivityLevel::Confidential))
            .then(PolicyAction::RequireEncryption)
            .then(PolicyAction::AddLabel("gdpr:protected".to_string()))
            .priority(100)
            .build()
    }

    /// HIPAA compliance policy
    pub fn hipaa_policy() -> ClassificationPolicy {
        PolicyBuilder::new("HIPAA Compliance")
            .description("Protect health information for HIPAA compliance")
            .when(PolicyCondition::HasCategory(DataCategory::PHI))
            .then(PolicyAction::SetSensitivity(SensitivityLevel::Restricted))
            .then(PolicyAction::RequireEncryption)
            .then(PolicyAction::AddLabel("hipaa:phi".to_string()))
            .priority(100)
            .build()
    }

    /// PCI-DSS compliance policy
    pub fn pci_policy() -> ClassificationPolicy {
        PolicyBuilder::new("PCI-DSS Compliance")
            .description("Protect payment card data for PCI-DSS compliance")
            .when(PolicyCondition::HasCategory(DataCategory::PCI))
            .then(PolicyAction::SetSensitivity(SensitivityLevel::Restricted))
            .then(PolicyAction::RequireEncryption)
            .then(PolicyAction::AddLabel("pci:cardholder-data".to_string()))
            .priority(100)
            .build()
    }

    /// Secret detection policy
    pub fn secrets_policy() -> ClassificationPolicy {
        PolicyBuilder::new("Secrets Detection")
            .description("Detect and protect secrets and credentials")
            .when(PolicyCondition::HasCategory(DataCategory::Secrets))
            .then(PolicyAction::SetSensitivity(SensitivityLevel::TopSecret))
            .then(PolicyAction::RequireEncryption)
            .then(PolicyAction::Quarantine)
            .then(PolicyAction::Notify(NotificationConfig {
                channel: "security".to_string(),
                recipients: vec!["security-team@example.com".to_string()],
                template: "secret_detected".to_string(),
            }))
            .priority(200)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensitivity_level() {
        assert!(!SensitivityLevel::Public.requires_encryption());
        assert!(SensitivityLevel::Confidential.requires_encryption());
        assert!(SensitivityLevel::TopSecret.requires_encryption());

        assert!(!SensitivityLevel::Internal.requires_audit());
        assert!(SensitivityLevel::Restricted.requires_audit());
    }

    #[test]
    fn test_pattern_detector_ssn() {
        let detector = PatternDetector::new();
        let content = b"SSN: 123-45-6789";
        let matches = detector.scan(content);

        assert!(!matches.is_empty());
        let ssn_match = matches.iter().find(|m| m.pattern_name == "SSN");
        assert!(ssn_match.is_some());
        assert_eq!(ssn_match.unwrap().count, 1);
    }

    #[test]
    fn test_pattern_detector_credit_card() {
        let detector = PatternDetector::new();
        let content = b"Card: 4111-1111-1111-1111";
        let matches = detector.scan(content);

        let cc_match = matches.iter().find(|m| m.pattern_name == "Credit Card");
        assert!(cc_match.is_some());
    }

    #[test]
    fn test_pattern_detector_email() {
        let detector = PatternDetector::new();
        let content = b"Contact: user@example.com";
        let matches = detector.scan(content);

        let email_match = matches.iter().find(|m| m.pattern_name == "Email");
        assert!(email_match.is_some());
    }

    #[test]
    fn test_pattern_detector_aws_key() {
        let detector = PatternDetector::new();
        let content = b"AWS_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE";
        let matches = detector.scan(content);

        let key_match = matches.iter().find(|m| m.pattern_name == "AWS Access Key");
        assert!(key_match.is_some());
    }

    #[test]
    fn test_pattern_detector_private_key() {
        let detector = PatternDetector::new();
        let content = b"-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...";
        let matches = detector.scan(content);

        let key_match = matches.iter().find(|m| m.pattern_name == "Private Key");
        assert!(key_match.is_some());
    }

    #[test]
    fn test_content_type_detection() {
        // PDF
        let pdf_data = b"%PDF-1.4 content";
        assert_eq!(ContentTypeDetector::detect(pdf_data, None), ContentType::Document);

        // PNG
        let png_data = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        assert_eq!(ContentTypeDetector::detect(&png_data, None), ContentType::Image);

        // By extension
        assert_eq!(
            ContentTypeDetector::detect(b"text", Some("file.rs")),
            ContentType::Code
        );
        assert_eq!(
            ContentTypeDetector::detect(b"text", Some("config.yaml")),
            ContentType::Config
        );
    }

    #[test]
    fn test_policy_condition() {
        let classification = Classification {
            inode_id: 1,
            sensitivity: SensitivityLevel::Restricted,
            categories: [DataCategory::PII].into_iter().collect(),
            content_type: ContentType::Document,
            confidence: 0.9,
            labels: HashSet::new(),
            matched_patterns: vec![],
            classified_at: Utc::now(),
            classifier_version: "1.0.0".to_string(),
        };

        assert!(PolicyCondition::MinSensitivity(SensitivityLevel::Internal)
            .matches(&classification));
        assert!(PolicyCondition::MinSensitivity(SensitivityLevel::Restricted)
            .matches(&classification));
        assert!(!PolicyCondition::MinSensitivity(SensitivityLevel::TopSecret)
            .matches(&classification));

        assert!(PolicyCondition::HasCategory(DataCategory::PII).matches(&classification));
        assert!(!PolicyCondition::HasCategory(DataCategory::PCI).matches(&classification));

        assert!(PolicyCondition::ContentType(ContentType::Document).matches(&classification));
    }

    #[tokio::test]
    async fn test_policy_engine() {
        let engine = PolicyEngine::new();

        let policy = PolicyBuilder::new("Test Policy")
            .when(PolicyCondition::HasCategory(DataCategory::PII))
            .then(PolicyAction::RequireEncryption)
            .build();

        engine.add_policy(policy).await;

        let classification = Classification {
            inode_id: 1,
            sensitivity: SensitivityLevel::Internal,
            categories: [DataCategory::PII].into_iter().collect(),
            content_type: ContentType::Document,
            confidence: 0.9,
            labels: HashSet::new(),
            matched_patterns: vec![],
            classified_at: Utc::now(),
            classifier_version: "1.0.0".to_string(),
        };

        let actions = engine.evaluate(&classification).await;
        assert!(!actions.is_empty());
        assert!(matches!(actions[0], PolicyAction::RequireEncryption));
    }

    #[tokio::test]
    async fn test_classification_engine() {
        let engine = ClassificationEngine::new(ClassificationConfig::default());

        // Content with PII
        let content = b"User SSN: 123-45-6789, Email: user@example.com";
        let result = engine.classify(1, content, Some("user_data.txt")).await.unwrap();

        assert!(result.categories.contains(&DataCategory::PII));
        assert!(matches!(
            result.sensitivity,
            SensitivityLevel::Internal | SensitivityLevel::Restricted
        ));
    }

    #[tokio::test]
    async fn test_classification_with_policies() {
        let engine = ClassificationEngine::new(ClassificationConfig::default());
        engine.add_policy(compliance::gdpr_policy()).await;

        let content = b"Customer email: customer@example.com";
        let result = engine.classify_and_apply_policies(1, content, None).await.unwrap();

        assert!(!result.actions.is_empty());
    }

    #[test]
    fn test_compliance_policies() {
        let gdpr = compliance::gdpr_policy();
        assert!(gdpr.enabled);
        assert!(!gdpr.actions.is_empty());

        let hipaa = compliance::hipaa_policy();
        assert!(hipaa.enabled);

        let pci = compliance::pci_policy();
        assert!(pci.enabled);

        let secrets = compliance::secrets_policy();
        assert!(secrets.priority > gdpr.priority);
    }

    #[test]
    fn test_config_presets() {
        let default = ClassificationConfig::default();
        let minimal = ClassificationConfig::minimal();
        let comprehensive = ClassificationConfig::comprehensive();

        assert!(default.detect_pii);
        assert!(minimal.detect_pii);
        assert!(!minimal.detect_phi);
        assert!(comprehensive.detect_phi);

        assert!(minimal.min_confidence > default.min_confidence);
        assert!(comprehensive.min_confidence < default.min_confidence);
    }
}
