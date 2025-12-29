// Asset Discovery for Data Lineage

use super::catalog::{Asset, AssetType, Classification, ClassificationLevel, DataCategory};
use super::schema::{Schema, SchemaType};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Enable automatic scanning
    pub auto_scan: bool,
    /// Scan interval in seconds
    pub scan_interval_secs: u64,
    /// Maximum depth for directory traversal
    pub max_depth: usize,
    /// File patterns to include
    pub include_patterns: Vec<String>,
    /// File patterns to exclude
    pub exclude_patterns: Vec<String>,
    /// Enable schema inference
    pub infer_schema: bool,
    /// Sample size for schema inference
    pub sample_size: usize,
    /// Enable PII detection
    pub detect_pii: bool,
    /// Classification rules
    pub classification_rules: Vec<ClassificationRule>,
    /// Tag rules
    pub tag_rules: Vec<TagRule>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            auto_scan: true,
            scan_interval_secs: 3600,
            max_depth: 10,
            include_patterns: vec!["**/*".to_string()],
            exclude_patterns: vec![
                "**/node_modules/**".to_string(),
                "**/.git/**".to_string(),
                "**/.*".to_string(),
            ],
            infer_schema: true,
            sample_size: 1000,
            detect_pii: true,
            classification_rules: default_classification_rules(),
            tag_rules: default_tag_rules(),
        }
    }
}

/// Classification rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationRule {
    /// Rule name
    pub name: String,
    /// Pattern type
    pub pattern_type: PatternType,
    /// Pattern
    pub pattern: String,
    /// Classification to apply
    pub level: ClassificationLevel,
    /// Categories to apply
    pub categories: Vec<DataCategory>,
}

/// Tag rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagRule {
    /// Rule name
    pub name: String,
    /// Pattern type
    pub pattern_type: PatternType,
    /// Pattern
    pub pattern: String,
    /// Tags to apply
    pub tags: Vec<String>,
}

/// Pattern type for matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PatternType {
    /// Regex pattern
    Regex,
    /// Glob pattern
    Glob,
    /// Contains substring
    Contains,
    /// Exact match
    Exact,
    /// Column name pattern
    ColumnName,
    /// File extension
    Extension,
    /// Path pattern
    Path,
}

/// Discovery result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryResult {
    /// Discovered assets
    pub assets: Vec<Asset>,
    /// Inferred schemas
    pub schemas: Vec<InferredSchema>,
    /// Classifications applied
    pub classifications: Vec<ClassificationResult>,
    /// Statistics
    pub stats: DiscoveryStats,
    /// Errors encountered
    pub errors: Vec<String>,
}

/// Inferred schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchema {
    /// Asset ID
    pub asset_id: String,
    /// Inferred schema
    pub schema: Schema,
    /// Confidence score (0-1)
    pub confidence: f32,
    /// Sample count used
    pub sample_count: usize,
}

/// Classification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    /// Asset ID
    pub asset_id: String,
    /// Field name (if column-level)
    pub field_name: Option<String>,
    /// Rule that matched
    pub rule_name: String,
    /// Classification
    pub classification: Classification,
    /// Confidence score
    pub confidence: f32,
}

/// Discovery statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryStats {
    /// Total files scanned
    pub files_scanned: u64,
    /// Total bytes scanned
    pub bytes_scanned: u64,
    /// Assets discovered
    pub assets_discovered: u64,
    /// Schemas inferred
    pub schemas_inferred: u64,
    /// PII detected
    pub pii_detected: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Asset discovery engine
pub struct Discovery {
    /// Configuration
    config: DiscoveryConfig,
    /// Running scans
    running_scans: Arc<RwLock<HashMap<String, ScanStatus>>>,
}

/// Scan status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanStatus {
    /// Scan ID
    pub id: String,
    /// Status
    pub status: ScanState,
    /// Progress (0-100)
    pub progress: u8,
    /// Started timestamp
    pub started_at: u64,
    /// Completed timestamp
    pub completed_at: Option<u64>,
    /// Current path being scanned
    pub current_path: Option<String>,
    /// Stats so far
    pub stats: DiscoveryStats,
}

/// Scan state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScanState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl Discovery {
    /// Creates a new discovery engine
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            running_scans: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Discovers assets at a path
    pub async fn discover(&self, path: &str) -> Result<DiscoveryResult> {
        let start = std::time::Instant::now();
        let mut result = DiscoveryResult {
            assets: Vec::new(),
            schemas: Vec::new(),
            classifications: Vec::new(),
            stats: DiscoveryStats::default(),
            errors: Vec::new(),
        };

        // Simulate discovery (in real implementation, would scan filesystem)
        let files = self.scan_path(path)?;

        for file_info in files {
            result.stats.files_scanned += 1;
            result.stats.bytes_scanned += file_info.size;

            // Create asset
            let asset = self.create_asset(&file_info);
            let asset_id = asset.id.clone();

            // Infer schema if enabled
            if self.config.infer_schema && self.is_structured_file(&file_info) {
                match self.infer_schema(&file_info) {
                    Ok(schema) => {
                        result.schemas.push(InferredSchema {
                            asset_id: asset_id.clone(),
                            schema,
                            confidence: 0.9,
                            sample_count: self.config.sample_size,
                        });
                        result.stats.schemas_inferred += 1;
                    }
                    Err(e) => {
                        result.errors.push(format!("Schema inference failed for {}: {}", file_info.path, e));
                    }
                }
            }

            // Apply classification rules
            if self.config.detect_pii {
                let classifications = self.classify_asset(&asset, &result.schemas);
                if !classifications.is_empty() {
                    result.stats.pii_detected += classifications.len() as u64;
                    result.classifications.extend(classifications);
                }
            }

            result.assets.push(asset);
            result.stats.assets_discovered += 1;
        }

        result.stats.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Scans a path (simulated)
    fn scan_path(&self, _path: &str) -> Result<Vec<FileInfo>> {
        // In real implementation, would use std::fs
        // For now, return empty or simulated data
        Ok(Vec::new())
    }

    /// Creates an asset from file info
    fn create_asset(&self, file_info: &FileInfo) -> Asset {
        let asset_type = self.infer_asset_type(file_info);
        let id = uuid::Uuid::new_v4().to_string();

        let mut asset = Asset::new(&id, asset_type, &file_info.name)
            .with_location(&file_info.path);

        asset.size_bytes = Some(file_info.size);
        asset.format = file_info.extension.clone();

        // Apply tag rules
        for rule in &self.config.tag_rules {
            if self.matches_pattern(&rule.pattern_type, &rule.pattern, &file_info.path) {
                for tag in &rule.tags {
                    if !asset.tags.contains(tag) {
                        asset.tags.push(tag.clone());
                    }
                }
            }
        }

        asset
    }

    /// Infers asset type from file info
    fn infer_asset_type(&self, file_info: &FileInfo) -> AssetType {
        match file_info.extension.as_deref() {
            Some("csv") | Some("tsv") => AssetType::Table,
            Some("parquet") | Some("orc") | Some("avro") => AssetType::Table,
            Some("json") | Some("jsonl") | Some("ndjson") => AssetType::Dataset,
            Some("sql") => AssetType::View,
            Some("py") | Some("ipynb") => AssetType::Pipeline,
            Some("pkl") | Some("h5") | Some("onnx") | Some("pt") => AssetType::Model,
            _ if file_info.is_directory => AssetType::Directory,
            _ => AssetType::File,
        }
    }

    /// Checks if file is structured
    fn is_structured_file(&self, file_info: &FileInfo) -> bool {
        matches!(
            file_info.extension.as_deref(),
            Some("csv") | Some("tsv") | Some("parquet") | Some("json") | Some("avro") | Some("orc")
        )
    }

    /// Infers schema from file
    fn infer_schema(&self, file_info: &FileInfo) -> Result<Schema> {
        let schema_type = match file_info.extension.as_deref() {
            Some("parquet") => SchemaType::Parquet,
            Some("avro") => SchemaType::Avro,
            Some("json") => SchemaType::Json,
            _ => SchemaType::Custom,
        };

        // In real implementation, would read file and infer schema
        // For now, return placeholder
        Ok(Schema::new(&file_info.name, schema_type))
    }

    /// Classifies an asset
    fn classify_asset(&self, asset: &Asset, schemas: &[InferredSchema]) -> Vec<ClassificationResult> {
        let mut results = Vec::new();

        for rule in &self.config.classification_rules {
            // Check path-based rules
            if self.matches_pattern(&rule.pattern_type, &rule.pattern, &asset.location) {
                results.push(ClassificationResult {
                    asset_id: asset.id.clone(),
                    field_name: None,
                    rule_name: rule.name.clone(),
                    classification: Classification {
                        level: rule.level,
                        categories: rule.categories.clone(),
                        compliance: Vec::new(),
                        retention_days: None,
                    },
                    confidence: 0.8,
                });
            }

            // Check column-level rules
            if rule.pattern_type == PatternType::ColumnName {
                if let Some(schema_info) = schemas.iter().find(|s| s.asset_id == asset.id) {
                    for field in &schema_info.schema.fields {
                        if self.matches_pattern(&PatternType::Regex, &rule.pattern, &field.name) {
                            results.push(ClassificationResult {
                                asset_id: asset.id.clone(),
                                field_name: Some(field.name.clone()),
                                rule_name: rule.name.clone(),
                                classification: Classification {
                                    level: rule.level,
                                    categories: rule.categories.clone(),
                                    compliance: Vec::new(),
                                    retention_days: None,
                                },
                                confidence: 0.9,
                            });
                        }
                    }
                }
            }
        }

        results
    }

    /// Matches a pattern
    fn matches_pattern(&self, pattern_type: &PatternType, pattern: &str, text: &str) -> bool {
        match pattern_type {
            PatternType::Exact => text == pattern,
            PatternType::Contains => text.contains(pattern),
            PatternType::Regex => {
                regex::Regex::new(pattern)
                    .map(|re| re.is_match(text))
                    .unwrap_or(false)
            }
            PatternType::Glob => glob_match(pattern, text),
            PatternType::Extension => {
                text.rsplit('.').next() == Some(pattern)
            }
            PatternType::Path => text.starts_with(pattern) || glob_match(pattern, text),
            PatternType::ColumnName => {
                regex::Regex::new(&format!("(?i){}", pattern))
                    .map(|re| re.is_match(text))
                    .unwrap_or(false)
            }
        }
    }

    /// Starts an async scan
    pub async fn start_scan(&self, path: &str) -> String {
        let scan_id = uuid::Uuid::new_v4().to_string();

        let status = ScanStatus {
            id: scan_id.clone(),
            status: ScanState::Pending,
            progress: 0,
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            completed_at: None,
            current_path: Some(path.to_string()),
            stats: DiscoveryStats::default(),
        };

        let mut scans = self.running_scans.write().await;
        scans.insert(scan_id.clone(), status);

        scan_id
    }

    /// Gets scan status
    pub async fn get_scan_status(&self, scan_id: &str) -> Option<ScanStatus> {
        let scans = self.running_scans.read().await;
        scans.get(scan_id).cloned()
    }

    /// Cancels a scan
    pub async fn cancel_scan(&self, scan_id: &str) -> bool {
        let mut scans = self.running_scans.write().await;
        if let Some(scan) = scans.get_mut(scan_id) {
            scan.status = ScanState::Cancelled;
            return true;
        }
        false
    }

    /// Lists all scans
    pub async fn list_scans(&self) -> Vec<ScanStatus> {
        let scans = self.running_scans.read().await;
        scans.values().cloned().collect()
    }
}

/// File info for discovery
#[derive(Debug, Clone)]
struct FileInfo {
    /// File path
    path: String,
    /// File name
    name: String,
    /// File extension
    extension: Option<String>,
    /// File size in bytes
    size: u64,
    /// Is directory
    is_directory: bool,
    /// Modified timestamp
    modified_at: u64,
}

/// Simple glob matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    glob_match_impl(&pattern_chars, &text_chars)
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        '*' => {
            if pattern.len() > 1 && pattern[1] == '*' {
                // ** matches any path
                for i in 0..=text.len() {
                    if glob_match_impl(&pattern[2..], &text[i..]) {
                        return true;
                    }
                }
                return false;
            }
            // * matches anything except /
            for i in 0..=text.len() {
                if i > 0 && text[i - 1] == '/' {
                    break;
                }
                if glob_match_impl(&pattern[1..], &text[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if text.is_empty() || text[0] == '/' {
                false
            } else {
                glob_match_impl(&pattern[1..], &text[1..])
            }
        }
        c => {
            if text.is_empty() || text[0] != c {
                false
            } else {
                glob_match_impl(&pattern[1..], &text[1..])
            }
        }
    }
}

/// Default classification rules
fn default_classification_rules() -> Vec<ClassificationRule> {
    vec![
        ClassificationRule {
            name: "email_detection".to_string(),
            pattern_type: PatternType::ColumnName,
            pattern: r"(?i)(email|e_mail|e-mail|mail_address)".to_string(),
            level: ClassificationLevel::Confidential,
            categories: vec![DataCategory::Pii],
        },
        ClassificationRule {
            name: "ssn_detection".to_string(),
            pattern_type: PatternType::ColumnName,
            pattern: r"(?i)(ssn|social_security|national_id)".to_string(),
            level: ClassificationLevel::Restricted,
            categories: vec![DataCategory::Pii],
        },
        ClassificationRule {
            name: "credit_card_detection".to_string(),
            pattern_type: PatternType::ColumnName,
            pattern: r"(?i)(credit_card|card_number|ccn|pan)".to_string(),
            level: ClassificationLevel::Restricted,
            categories: vec![DataCategory::Pci],
        },
        ClassificationRule {
            name: "password_detection".to_string(),
            pattern_type: PatternType::ColumnName,
            pattern: r"(?i)(password|passwd|secret|token|api_key)".to_string(),
            level: ClassificationLevel::TopSecret,
            categories: vec![DataCategory::Credentials],
        },
        ClassificationRule {
            name: "health_detection".to_string(),
            pattern_type: PatternType::ColumnName,
            pattern: r"(?i)(diagnosis|medical|health|patient)".to_string(),
            level: ClassificationLevel::Restricted,
            categories: vec![DataCategory::Phi],
        },
    ]
}

/// Default tag rules
fn default_tag_rules() -> Vec<TagRule> {
    vec![
        TagRule {
            name: "production_data".to_string(),
            pattern_type: PatternType::Path,
            pattern: "**/prod/**".to_string(),
            tags: vec!["production".to_string(), "critical".to_string()],
        },
        TagRule {
            name: "staging_data".to_string(),
            pattern_type: PatternType::Path,
            pattern: "**/staging/**".to_string(),
            tags: vec!["staging".to_string()],
        },
        TagRule {
            name: "test_data".to_string(),
            pattern_type: PatternType::Path,
            pattern: "**/test/**".to_string(),
            tags: vec!["test".to_string()],
        },
        TagRule {
            name: "raw_data".to_string(),
            pattern_type: PatternType::Path,
            pattern: "**/raw/**".to_string(),
            tags: vec!["raw".to_string(), "source".to_string()],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*.csv", "data.csv"));
        assert!(!glob_match("*.csv", "data.json"));
        assert!(glob_match("data/*.csv", "data/file.csv"));
        assert!(glob_match("**/*.csv", "a/b/c/file.csv"));
        assert!(glob_match("file?.csv", "file1.csv"));
    }

    #[test]
    fn test_pattern_matching() {
        let discovery = Discovery::new(DiscoveryConfig::default());

        assert!(discovery.matches_pattern(&PatternType::Exact, "test", "test"));
        assert!(!discovery.matches_pattern(&PatternType::Exact, "test", "testing"));

        assert!(discovery.matches_pattern(&PatternType::Contains, "email", "user_email_address"));

        assert!(discovery.matches_pattern(&PatternType::Extension, "csv", "data.csv"));
    }

    #[test]
    fn test_classification_rules() {
        let rules = default_classification_rules();
        assert!(!rules.is_empty());

        let email_rule = rules.iter().find(|r| r.name == "email_detection").unwrap();
        assert_eq!(email_rule.level, ClassificationLevel::Confidential);
        assert!(email_rule.categories.contains(&DataCategory::Pii));
    }

    #[tokio::test]
    async fn test_discovery_scan() {
        let discovery = Discovery::new(DiscoveryConfig::default());

        // Start scan
        let scan_id = discovery.start_scan("/data").await;
        assert!(!scan_id.is_empty());

        // Check status
        let status = discovery.get_scan_status(&scan_id).await.unwrap();
        assert_eq!(status.status, ScanState::Pending);

        // Cancel scan
        assert!(discovery.cancel_scan(&scan_id).await);

        let status = discovery.get_scan_status(&scan_id).await.unwrap();
        assert_eq!(status.status, ScanState::Cancelled);
    }
}
