//! S3 bucket lifecycle configuration support.
//!
//! Implements lifecycle rules for automatic object expiration and transitions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Maximum number of lifecycle rules per bucket.
pub const MAX_LIFECYCLE_RULES: usize = 1000;

/// Lifecycle configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfiguration {
    /// List of lifecycle rules.
    pub rules: Vec<LifecycleRule>,
}

impl Default for LifecycleConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleConfiguration {
    /// Create an empty lifecycle configuration.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a lifecycle rule.
    pub fn add_rule(&mut self, rule: LifecycleRule) -> Result<(), LifecycleError> {
        if self.rules.len() >= MAX_LIFECYCLE_RULES {
            return Err(LifecycleError::TooManyRules);
        }

        // Check for duplicate IDs
        if let Some(ref id) = rule.id {
            if self.rules.iter().any(|r| r.id.as_ref() == Some(id)) {
                return Err(LifecycleError::DuplicateId(id.clone()));
            }
        }

        self.rules.push(rule);
        Ok(())
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.rules.len() > MAX_LIFECYCLE_RULES {
            return Err(LifecycleError::TooManyRules);
        }

        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }
}

/// A single lifecycle rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// Unique identifier for this rule.
    pub id: Option<String>,

    /// Rule status (Enabled or Disabled).
    pub status: RuleStatus,

    /// Filter for objects this rule applies to.
    pub filter: LifecycleFilter,

    /// Expiration settings.
    pub expiration: Option<Expiration>,

    /// Transition settings.
    pub transitions: Vec<Transition>,

    /// Noncurrent version expiration.
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,

    /// Abort incomplete multipart upload.
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

impl Default for LifecycleRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleRule {
    /// Create a new lifecycle rule.
    pub fn new() -> Self {
        Self {
            id: None,
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: None,
            transitions: Vec::new(),
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: None,
        }
    }

    /// Set the rule ID.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the filter prefix.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.filter.prefix = Some(prefix.into());
        self
    }

    /// Set expiration days.
    pub fn with_expiration_days(mut self, days: u32) -> Self {
        self.expiration = Some(Expiration { days: Some(days), date: None, expired_object_delete_marker: None });
        self
    }

    /// Add a transition.
    pub fn with_transition(mut self, days: u32, storage_class: StorageClass) -> Self {
        self.transitions.push(Transition { days: Some(days), date: None, storage_class });
        self
    }

    /// Validate the rule.
    pub fn validate(&self) -> Result<(), LifecycleError> {
        // Must have at least one action
        if self.expiration.is_none()
            && self.transitions.is_empty()
            && self.noncurrent_version_expiration.is_none()
            && self.abort_incomplete_multipart_upload.is_none()
        {
            return Err(LifecycleError::NoAction);
        }

        // Validate expiration
        if let Some(ref exp) = self.expiration {
            if exp.days.is_none() && exp.date.is_none() && exp.expired_object_delete_marker.is_none() {
                return Err(LifecycleError::InvalidExpiration);
            }
        }

        // Validate transitions - must have days or date
        for transition in &self.transitions {
            if transition.days.is_none() && transition.date.is_none() {
                return Err(LifecycleError::InvalidTransition);
            }
        }

        Ok(())
    }
}

/// Rule status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum RuleStatus {
    Enabled,
    Disabled,
}

impl RuleStatus {
    /// Parse status from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Disabled => "Disabled",
        }
    }
}

/// Filter for lifecycle rules.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleFilter {
    /// Object key prefix.
    pub prefix: Option<String>,
    /// Object tags.
    pub tags: Vec<Tag>,
    /// Minimum object size in bytes.
    pub object_size_greater_than: Option<u64>,
    /// Maximum object size in bytes.
    pub object_size_less_than: Option<u64>,
}

/// Object tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// Expiration settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expiration {
    /// Number of days after creation to expire.
    pub days: Option<u32>,
    /// Specific date to expire.
    pub date: Option<String>,
    /// Whether to delete expired delete markers.
    pub expired_object_delete_marker: Option<bool>,
}

/// Transition settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transition {
    /// Number of days after creation to transition.
    pub days: Option<u32>,
    /// Specific date to transition.
    pub date: Option<String>,
    /// Target storage class.
    pub storage_class: StorageClass,
}

/// Storage class for transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageClass {
    Standard,
    StandardIa,
    OnezoneIa,
    IntelligentTiering,
    Glacier,
    GlacierIr,
    DeepArchive,
}

impl StorageClass {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "STANDARD" => Some(Self::Standard),
            "STANDARD_IA" => Some(Self::StandardIa),
            "ONEZONE_IA" => Some(Self::OnezoneIa),
            "INTELLIGENT_TIERING" => Some(Self::IntelligentTiering),
            "GLACIER" => Some(Self::Glacier),
            "GLACIER_IR" => Some(Self::GlacierIr),
            "DEEP_ARCHIVE" => Some(Self::DeepArchive),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::StandardIa => "STANDARD_IA",
            Self::OnezoneIa => "ONEZONE_IA",
            Self::IntelligentTiering => "INTELLIGENT_TIERING",
            Self::Glacier => "GLACIER",
            Self::GlacierIr => "GLACIER_IR",
            Self::DeepArchive => "DEEP_ARCHIVE",
        }
    }
}

/// Noncurrent version expiration settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Number of days after becoming noncurrent.
    pub noncurrent_days: u32,
    /// Number of newer versions to retain.
    pub newer_noncurrent_versions: Option<u32>,
}

/// Abort incomplete multipart upload settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortIncompleteMultipartUpload {
    /// Days after initiation to abort.
    pub days_after_initiation: u32,
}

/// Lifecycle-related errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LifecycleError {
    #[error("Too many lifecycle rules (max {MAX_LIFECYCLE_RULES})")]
    TooManyRules,

    #[error("Duplicate rule ID: {0}")]
    DuplicateId(String),

    #[error("Rule must have at least one action")]
    NoAction,

    #[error("Invalid expiration configuration")]
    InvalidExpiration,

    #[error("Invalid transition configuration")]
    InvalidTransition,

    #[error("Lifecycle configuration not found")]
    NotFound,

    #[error("Invalid XML: {0}")]
    InvalidXml(String),
}

/// Lifecycle configuration store.
#[derive(Clone)]
pub struct LifecycleStore {
    /// Map of bucket name to lifecycle configuration.
    configs: Arc<RwLock<HashMap<String, LifecycleConfiguration>>>,
}

impl Default for LifecycleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleStore {
    /// Create a new lifecycle store.
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get lifecycle configuration for a bucket.
    pub fn get(&self, bucket: &str) -> Option<LifecycleConfiguration> {
        self.configs.read().ok()?.get(bucket).cloned()
    }

    /// Set lifecycle configuration for a bucket.
    pub fn set(&self, bucket: &str, config: LifecycleConfiguration) -> Result<(), LifecycleError> {
        config.validate()?;
        if let Ok(mut configs) = self.configs.write() {
            configs.insert(bucket.to_string(), config);
        }
        Ok(())
    }

    /// Remove lifecycle configuration for a bucket.
    pub fn remove(&self, bucket: &str) -> bool {
        if let Ok(mut configs) = self.configs.write() {
            return configs.remove(bucket).is_some();
        }
        false
    }
}

/// State for lifecycle operations.
#[derive(Clone)]
pub struct LifecycleState {
    /// Lifecycle configuration store.
    pub store: LifecycleStore,
}

impl Default for LifecycleState {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleState {
    /// Create new lifecycle state.
    pub fn new() -> Self {
        Self {
            store: LifecycleStore::new(),
        }
    }
}

/// Parse lifecycle configuration from XML.
pub fn parse_lifecycle_xml(xml: &str) -> Result<LifecycleConfiguration, LifecycleError> {
    let mut config = LifecycleConfiguration::new();

    // Find all Rule blocks
    let mut pos = 0;
    while let Some(rule_start) = xml[pos..].find("<Rule>") {
        let rule_start = pos + rule_start;
        let rule_end = xml[rule_start..]
            .find("</Rule>")
            .map(|i| rule_start + i + 7)
            .ok_or_else(|| LifecycleError::InvalidXml("Missing </Rule>".to_string()))?;

        let rule_xml = &xml[rule_start..rule_end];
        let rule = parse_lifecycle_rule(rule_xml)?;
        config.add_rule(rule)?;

        pos = rule_end;
    }

    if config.rules.is_empty() {
        return Err(LifecycleError::InvalidXml("No rules found".to_string()));
    }

    Ok(config)
}

/// Parse a single lifecycle rule from XML.
fn parse_lifecycle_rule(xml: &str) -> Result<LifecycleRule, LifecycleError> {
    let mut rule = LifecycleRule::new();

    // Parse ID
    if let Some(id) = extract_xml_value(xml, "ID") {
        rule.id = Some(id);
    }

    // Parse Status
    if let Some(status_str) = extract_xml_value(xml, "Status") {
        rule.status = RuleStatus::from_str(&status_str)
            .ok_or_else(|| LifecycleError::InvalidXml(format!("Invalid status: {}", status_str)))?;
    }

    // Parse Filter/Prefix
    if let Some(prefix) = extract_xml_value(xml, "Prefix") {
        rule.filter.prefix = Some(prefix);
    }

    // Parse Expiration
    if let Some(exp_start) = xml.find("<Expiration>") {
        if let Some(exp_end) = xml[exp_start..].find("</Expiration>") {
            let exp_xml = &xml[exp_start..exp_start + exp_end + 13];
            let mut expiration = Expiration { days: None, date: None, expired_object_delete_marker: None };

            if let Some(days_str) = extract_xml_value(exp_xml, "Days") {
                expiration.days = days_str.parse().ok();
            }
            if let Some(date) = extract_xml_value(exp_xml, "Date") {
                expiration.date = Some(date);
            }

            rule.expiration = Some(expiration);
        }
    }

    // Parse Transitions
    let mut t_pos = 0;
    while let Some(t_start) = xml[t_pos..].find("<Transition>") {
        let t_start = t_pos + t_start;
        if let Some(t_end) = xml[t_start..].find("</Transition>") {
            let t_xml = &xml[t_start..t_start + t_end + 13];

            let days = extract_xml_value(t_xml, "Days").and_then(|s| s.parse().ok());
            let date = extract_xml_value(t_xml, "Date");
            let storage_class = extract_xml_value(t_xml, "StorageClass")
                .and_then(|s| StorageClass::from_str(&s))
                .unwrap_or(StorageClass::Glacier);

            rule.transitions.push(Transition { days, date, storage_class });
            t_pos = t_start + t_end + 13;
        } else {
            break;
        }
    }

    // Parse AbortIncompleteMultipartUpload
    if let Some(abort_start) = xml.find("<AbortIncompleteMultipartUpload>") {
        if let Some(abort_end) = xml[abort_start..].find("</AbortIncompleteMultipartUpload>") {
            let abort_xml = &xml[abort_start..abort_start + abort_end + 33];
            if let Some(days_str) = extract_xml_value(abort_xml, "DaysAfterInitiation") {
                if let Ok(days) = days_str.parse() {
                    rule.abort_incomplete_multipart_upload = Some(AbortIncompleteMultipartUpload {
                        days_after_initiation: days,
                    });
                }
            }
        }
    }

    rule.validate()?;
    Ok(rule)
}

/// Extract a single XML element value.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml[start..].find(&end_tag)?;

    Some(xml[start..start + end].trim().to_string())
}

/// Generate XML for lifecycle configuration.
pub fn lifecycle_to_xml(config: &LifecycleConfiguration) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for rule in &config.rules {
        xml.push_str("  <Rule>\n");

        if let Some(ref id) = rule.id {
            xml.push_str(&format!("    <ID>{}</ID>\n", xml_escape(id)));
        }

        xml.push_str(&format!("    <Status>{}</Status>\n", rule.status.as_str()));

        // Filter
        xml.push_str("    <Filter>\n");
        if let Some(ref prefix) = rule.filter.prefix {
            xml.push_str(&format!("      <Prefix>{}</Prefix>\n", xml_escape(prefix)));
        }
        xml.push_str("    </Filter>\n");

        // Expiration
        if let Some(ref exp) = rule.expiration {
            xml.push_str("    <Expiration>\n");
            if let Some(days) = exp.days {
                xml.push_str(&format!("      <Days>{}</Days>\n", days));
            }
            if let Some(ref date) = exp.date {
                xml.push_str(&format!("      <Date>{}</Date>\n", xml_escape(date)));
            }
            xml.push_str("    </Expiration>\n");
        }

        // Transitions
        for transition in &rule.transitions {
            xml.push_str("    <Transition>\n");
            if let Some(days) = transition.days {
                xml.push_str(&format!("      <Days>{}</Days>\n", days));
            }
            if let Some(ref date) = transition.date {
                xml.push_str(&format!("      <Date>{}</Date>\n", xml_escape(date)));
            }
            xml.push_str(&format!("      <StorageClass>{}</StorageClass>\n", transition.storage_class.as_str()));
            xml.push_str("    </Transition>\n");
        }

        // AbortIncompleteMultipartUpload
        if let Some(ref abort) = rule.abort_incomplete_multipart_upload {
            xml.push_str("    <AbortIncompleteMultipartUpload>\n");
            xml.push_str(&format!("      <DaysAfterInitiation>{}</DaysAfterInitiation>\n", abort.days_after_initiation));
            xml.push_str("    </AbortIncompleteMultipartUpload>\n");
        }

        xml.push_str("  </Rule>\n");
    }

    xml.push_str("</LifecycleConfiguration>");
    xml
}

/// Escape special XML characters.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_status() {
        assert_eq!(RuleStatus::from_str("Enabled"), Some(RuleStatus::Enabled));
        assert_eq!(RuleStatus::from_str("disabled"), Some(RuleStatus::Disabled));
        assert_eq!(RuleStatus::from_str("invalid"), None);
    }

    #[test]
    fn test_storage_class() {
        assert_eq!(StorageClass::from_str("GLACIER"), Some(StorageClass::Glacier));
        assert_eq!(StorageClass::from_str("STANDARD_IA"), Some(StorageClass::StandardIa));
        assert_eq!(StorageClass::from_str("invalid"), None);
    }

    #[test]
    fn test_lifecycle_rule_builder() {
        let rule = LifecycleRule::new()
            .with_id("my-rule")
            .with_prefix("logs/")
            .with_expiration_days(30)
            .with_transition(7, StorageClass::StandardIa);

        assert_eq!(rule.id, Some("my-rule".to_string()));
        assert_eq!(rule.filter.prefix, Some("logs/".to_string()));
        assert!(rule.expiration.is_some());
        assert_eq!(rule.transitions.len(), 1);
        assert!(rule.validate().is_ok());
    }

    #[test]
    fn test_lifecycle_configuration() {
        let mut config = LifecycleConfiguration::new();

        let rule = LifecycleRule::new()
            .with_id("rule1")
            .with_expiration_days(30);

        assert!(config.add_rule(rule).is_ok());
        assert_eq!(config.rules.len(), 1);
    }

    #[test]
    fn test_lifecycle_store() {
        let store = LifecycleStore::new();

        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule::new().with_expiration_days(30)).unwrap();

        assert!(store.set("test-bucket", config).is_ok());
        assert!(store.get("test-bucket").is_some());

        assert!(store.remove("test-bucket"));
        assert!(store.get("test-bucket").is_none());
    }

    #[test]
    fn test_parse_lifecycle_xml() {
        let xml = r#"
        <LifecycleConfiguration>
          <Rule>
            <ID>my-rule</ID>
            <Status>Enabled</Status>
            <Prefix>logs/</Prefix>
            <Expiration>
              <Days>30</Days>
            </Expiration>
          </Rule>
        </LifecycleConfiguration>
        "#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);

        let rule = &config.rules[0];
        assert_eq!(rule.id, Some("my-rule".to_string()));
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert_eq!(rule.expiration.as_ref().unwrap().days, Some(30));
    }

    #[test]
    fn test_lifecycle_to_xml() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(
            LifecycleRule::new()
                .with_id("rule1")
                .with_prefix("logs/")
                .with_expiration_days(30)
        ).unwrap();

        let xml = lifecycle_to_xml(&config);
        assert!(xml.contains("<ID>rule1</ID>"));
        assert!(xml.contains("<Prefix>logs/</Prefix>"));
        assert!(xml.contains("<Days>30</Days>"));
    }

    #[test]
    fn test_rule_validation() {
        // Rule without action should fail
        let rule = LifecycleRule::new();
        assert!(matches!(rule.validate(), Err(LifecycleError::NoAction)));

        // Rule with expiration should pass
        let rule = LifecycleRule::new().with_expiration_days(30);
        assert!(rule.validate().is_ok());
    }
}
