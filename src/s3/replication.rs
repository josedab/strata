//! S3 bucket replication support.
//!
//! Bucket replication enables automatic, asynchronous copying of objects across
//! buckets. This module provides configuration management for replication rules.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Replication configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationConfiguration {
    /// IAM role ARN for replication (for AWS compatibility).
    pub role: String,
    /// List of replication rules.
    pub rules: Vec<ReplicationRule>,
}

impl Default for ReplicationConfiguration {
    fn default() -> Self {
        Self {
            role: String::new(),
            rules: Vec::new(),
        }
    }
}

/// A single replication rule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationRule {
    /// Unique identifier for the rule.
    pub id: Option<String>,
    /// Priority of the rule (higher number = higher priority).
    pub priority: Option<i32>,
    /// Whether the rule is enabled.
    pub status: ReplicationStatus,
    /// Filter to identify objects to replicate.
    pub filter: Option<ReplicationFilter>,
    /// Destination for replicated objects.
    pub destination: ReplicationDestination,
    /// Whether to replicate delete markers.
    pub delete_marker_replication: Option<DeleteMarkerReplication>,
    /// Source selection criteria.
    pub source_selection_criteria: Option<SourceSelectionCriteria>,
    /// Existing object replication settings.
    pub existing_object_replication: Option<ExistingObjectReplication>,
}

/// Status of a replication rule.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicationStatus {
    /// Rule is enabled.
    Enabled,
    /// Rule is disabled.
    Disabled,
}

impl std::fmt::Display for ReplicationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationStatus::Enabled => write!(f, "Enabled"),
            ReplicationStatus::Disabled => write!(f, "Disabled"),
        }
    }
}

/// Filter for selecting objects to replicate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationFilter {
    /// Object key name prefix.
    pub prefix: Option<String>,
    /// Tag filter.
    pub tag: Option<Tag>,
    /// AND filter combining multiple conditions.
    pub and: Option<ReplicationFilterAnd>,
}

/// Combined AND filter for multiple conditions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationFilterAnd {
    /// Key prefix.
    pub prefix: Option<String>,
    /// Object tags.
    pub tags: Vec<Tag>,
}

/// A key-value tag pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tag {
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Destination configuration for replicated objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationDestination {
    /// Destination bucket ARN.
    pub bucket: String,
    /// Account ID of the destination bucket owner.
    pub account: Option<String>,
    /// Storage class for replicated objects.
    pub storage_class: Option<String>,
    /// Access control translation settings.
    pub access_control_translation: Option<AccessControlTranslation>,
    /// Encryption configuration.
    pub encryption_configuration: Option<EncryptionConfiguration>,
    /// Replication time control.
    pub replication_time: Option<ReplicationTime>,
    /// Metrics for replication.
    pub metrics: Option<Metrics>,
}

/// Access control translation settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccessControlTranslation {
    /// The override owner.
    pub owner: String,
}

/// Encryption configuration for destination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EncryptionConfiguration {
    /// KMS key ID for destination encryption.
    pub replica_kms_key_id: Option<String>,
}

/// Replication time control settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationTime {
    /// Whether RTC is enabled.
    pub status: ReplicationStatus,
    /// Time threshold.
    pub time: ReplicationTimeValue,
}

/// Time value for replication time control.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationTimeValue {
    /// Minutes for the time threshold.
    pub minutes: i32,
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metrics {
    /// Whether metrics are enabled.
    pub status: ReplicationStatus,
    /// Event threshold.
    pub event_threshold: Option<ReplicationTimeValue>,
}

/// Delete marker replication settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteMarkerReplication {
    /// Whether to replicate delete markers.
    pub status: ReplicationStatus,
}

/// Source selection criteria for encrypted objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceSelectionCriteria {
    /// SSE-KMS encrypted object settings.
    pub sse_kms_encrypted_objects: Option<SseKmsEncryptedObjects>,
    /// Replica modifications settings.
    pub replica_modifications: Option<ReplicaModifications>,
}

/// SSE-KMS encrypted objects settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SseKmsEncryptedObjects {
    /// Whether to replicate SSE-KMS encrypted objects.
    pub status: ReplicationStatus,
}

/// Replica modifications settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicaModifications {
    /// Whether to replicate modifications to replicas.
    pub status: ReplicationStatus,
}

/// Existing object replication settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExistingObjectReplication {
    /// Whether to replicate existing objects.
    pub status: ReplicationStatus,
}

/// Error types for replication operations.
#[derive(Debug, Clone)]
pub enum ReplicationError {
    /// Invalid configuration.
    InvalidConfiguration(String),
    /// Rule not found.
    RuleNotFound(String),
    /// Configuration not found.
    ConfigurationNotFound,
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationError::InvalidConfiguration(msg) => {
                write!(f, "Invalid replication configuration: {}", msg)
            }
            ReplicationError::RuleNotFound(id) => {
                write!(f, "Replication rule not found: {}", id)
            }
            ReplicationError::ConfigurationNotFound => {
                write!(f, "Replication configuration not found")
            }
        }
    }
}

impl std::error::Error for ReplicationError {}

/// Storage for replication configurations.
#[derive(Debug, Default)]
pub struct ReplicationStore {
    /// Bucket replication configurations.
    configs: HashMap<String, ReplicationConfiguration>,
}

impl ReplicationStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get replication configuration for a bucket.
    pub fn get(&self, bucket: &str) -> Option<&ReplicationConfiguration> {
        self.configs.get(bucket)
    }

    /// Set replication configuration for a bucket.
    pub fn set(
        &mut self,
        bucket: String,
        config: ReplicationConfiguration,
    ) -> Result<(), ReplicationError> {
        // Validate configuration
        if config.rules.is_empty() {
            return Err(ReplicationError::InvalidConfiguration(
                "At least one rule is required".to_string(),
            ));
        }

        for rule in &config.rules {
            if rule.destination.bucket.is_empty() {
                return Err(ReplicationError::InvalidConfiguration(
                    "Destination bucket is required".to_string(),
                ));
            }
        }

        self.configs.insert(bucket, config);
        Ok(())
    }

    /// Remove replication configuration for a bucket.
    pub fn remove(&mut self, bucket: &str) -> bool {
        self.configs.remove(bucket).is_some()
    }

    /// Check if a bucket has replication enabled.
    pub fn is_enabled(&self, bucket: &str) -> bool {
        self.configs
            .get(bucket)
            .map(|c| c.rules.iter().any(|r| r.status == ReplicationStatus::Enabled))
            .unwrap_or(false)
    }

    /// Get enabled rules for a bucket.
    pub fn get_enabled_rules(&self, bucket: &str) -> Vec<&ReplicationRule> {
        self.configs
            .get(bucket)
            .map(|c| {
                c.rules
                    .iter()
                    .filter(|r| r.status == ReplicationStatus::Enabled)
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Thread-safe wrapper for ReplicationStore.
#[derive(Debug, Clone, Default)]
pub struct ReplicationState {
    store: Arc<RwLock<ReplicationStore>>,
}

impl ReplicationState {
    /// Create a new state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the inner store for reading.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, ReplicationStore> {
        self.store.read().unwrap()
    }

    /// Get the inner store for writing.
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, ReplicationStore> {
        self.store.write().unwrap()
    }
}

/// Parse replication configuration from XML.
pub fn parse_replication_xml(xml: &str) -> Result<ReplicationConfiguration, ReplicationError> {
    let mut config = ReplicationConfiguration::default();

    // Extract role
    if let Some(role) = extract_xml_value(xml, "Role") {
        config.role = role;
    }

    // Parse rules
    let mut rules = Vec::new();
    let mut search_start = 0;

    while let Some(rule_start) = xml[search_start..].find("<Rule>") {
        let abs_start = search_start + rule_start;
        if let Some(rule_end) = xml[abs_start..].find("</Rule>") {
            let rule_xml = &xml[abs_start..abs_start + rule_end + 7];
            if let Ok(rule) = parse_rule_xml(rule_xml) {
                rules.push(rule);
            }
            search_start = abs_start + rule_end + 7;
        } else {
            break;
        }
    }

    config.rules = rules;
    Ok(config)
}

/// Parse a single replication rule from XML.
fn parse_rule_xml(xml: &str) -> Result<ReplicationRule, ReplicationError> {
    let id = extract_xml_value(xml, "ID");
    let priority = extract_xml_value(xml, "Priority").and_then(|s| s.parse().ok());

    let status = if xml.contains("<Status>Enabled</Status>") {
        ReplicationStatus::Enabled
    } else {
        ReplicationStatus::Disabled
    };

    // Parse filter
    let filter = if xml.contains("<Filter>") {
        let prefix = extract_nested_value(xml, "Filter", "Prefix");
        Some(ReplicationFilter {
            prefix,
            tag: None,
            and: None,
        })
    } else {
        None
    };

    // Parse destination
    let dest_bucket = extract_nested_value(xml, "Destination", "Bucket")
        .ok_or_else(|| ReplicationError::InvalidConfiguration("Missing destination bucket".to_string()))?;

    let storage_class = extract_nested_value(xml, "Destination", "StorageClass");

    let destination = ReplicationDestination {
        bucket: dest_bucket,
        account: None,
        storage_class,
        access_control_translation: None,
        encryption_configuration: None,
        replication_time: None,
        metrics: None,
    };

    // Parse delete marker replication
    let delete_marker_replication = if xml.contains("<DeleteMarkerReplication>") {
        let status = if xml.contains("<DeleteMarkerReplication>") && xml.contains("<Status>Enabled</Status>") {
            ReplicationStatus::Enabled
        } else {
            ReplicationStatus::Disabled
        };
        Some(DeleteMarkerReplication { status })
    } else {
        None
    };

    Ok(ReplicationRule {
        id,
        priority,
        status,
        filter,
        destination,
        delete_marker_replication,
        source_selection_criteria: None,
        existing_object_replication: None,
    })
}

/// Generate XML for replication configuration.
pub fn replication_to_xml(config: &ReplicationConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );

    if !config.role.is_empty() {
        xml.push_str(&format!("\n    <Role>{}</Role>", config.role));
    }

    for rule in &config.rules {
        xml.push_str("\n    <Rule>");

        if let Some(ref id) = rule.id {
            xml.push_str(&format!("\n        <ID>{}</ID>", id));
        }

        if let Some(priority) = rule.priority {
            xml.push_str(&format!("\n        <Priority>{}</Priority>", priority));
        }

        xml.push_str(&format!("\n        <Status>{}</Status>", rule.status));

        if let Some(ref filter) = rule.filter {
            xml.push_str("\n        <Filter>");
            if let Some(ref prefix) = filter.prefix {
                xml.push_str(&format!("\n            <Prefix>{}</Prefix>", prefix));
            }
            xml.push_str("\n        </Filter>");
        }

        xml.push_str("\n        <Destination>");
        xml.push_str(&format!(
            "\n            <Bucket>{}</Bucket>",
            rule.destination.bucket
        ));
        if let Some(ref storage_class) = rule.destination.storage_class {
            xml.push_str(&format!(
                "\n            <StorageClass>{}</StorageClass>",
                storage_class
            ));
        }
        xml.push_str("\n        </Destination>");

        if let Some(ref dmr) = rule.delete_marker_replication {
            xml.push_str(&format!(
                "\n        <DeleteMarkerReplication>\n            <Status>{}</Status>\n        </DeleteMarkerReplication>",
                dmr.status
            ));
        }

        xml.push_str("\n    </Rule>");
    }

    xml.push_str("\n</ReplicationConfiguration>");
    xml
}

/// Helper to extract a value from simple XML.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml.find(&end_tag)?;

    if start < end {
        Some(xml[start..end].trim().to_string())
    } else {
        None
    }
}

/// Helper to extract a nested value from XML.
fn extract_nested_value(xml: &str, parent: &str, child: &str) -> Option<String> {
    let parent_start = format!("<{}>", parent);
    let parent_end = format!("</{}>", parent);

    let start = xml.find(&parent_start)? + parent_start.len();
    let end = xml.find(&parent_end)?;

    if start < end {
        let parent_content = &xml[start..end];
        extract_xml_value(parent_content, child)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_status_display() {
        assert_eq!(format!("{}", ReplicationStatus::Enabled), "Enabled");
        assert_eq!(format!("{}", ReplicationStatus::Disabled), "Disabled");
    }

    #[test]
    fn test_replication_store_basic() {
        let mut store = ReplicationStore::new();

        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                priority: Some(1),
                status: ReplicationStatus::Enabled,
                filter: Some(ReplicationFilter {
                    prefix: Some("logs/".to_string()),
                    tag: None,
                    and: None,
                }),
                destination: ReplicationDestination {
                    bucket: "arn:aws:s3:::dest-bucket".to_string(),
                    account: None,
                    storage_class: Some("STANDARD".to_string()),
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: None,
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };

        store.set("test-bucket".to_string(), config).unwrap();
        assert!(store.is_enabled("test-bucket"));
        assert_eq!(store.get_enabled_rules("test-bucket").len(), 1);
    }

    #[test]
    fn test_replication_store_empty_rules() {
        let mut store = ReplicationStore::new();

        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![],
        };

        let result = store.set("test-bucket".to_string(), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_replication_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ReplicationConfiguration>
    <Role>arn:aws:iam::123456789:role/replication</Role>
    <Rule>
        <ID>rule1</ID>
        <Priority>1</Priority>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>logs/</Prefix>
        </Filter>
        <Destination>
            <Bucket>arn:aws:s3:::dest-bucket</Bucket>
            <StorageClass>STANDARD</StorageClass>
        </Destination>
    </Rule>
</ReplicationConfiguration>"#;

        let config = parse_replication_xml(xml).unwrap();
        assert_eq!(config.role, "arn:aws:iam::123456789:role/replication");
        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].id, Some("rule1".to_string()));
        assert_eq!(config.rules[0].status, ReplicationStatus::Enabled);
    }

    #[test]
    fn test_replication_to_xml() {
        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                priority: Some(1),
                status: ReplicationStatus::Enabled,
                filter: Some(ReplicationFilter {
                    prefix: Some("logs/".to_string()),
                    tag: None,
                    and: None,
                }),
                destination: ReplicationDestination {
                    bucket: "arn:aws:s3:::dest-bucket".to_string(),
                    account: None,
                    storage_class: Some("STANDARD".to_string()),
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: None,
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };

        let xml = replication_to_xml(&config);
        assert!(xml.contains("<Role>arn:aws:iam::123456789:role/replication</Role>"));
        assert!(xml.contains("<ID>rule1</ID>"));
        assert!(xml.contains("<Status>Enabled</Status>"));
        assert!(xml.contains("<Bucket>arn:aws:s3:::dest-bucket</Bucket>"));
    }

    #[test]
    fn test_replication_store_remove() {
        let mut store = ReplicationStore::new();

        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                priority: Some(1),
                status: ReplicationStatus::Enabled,
                filter: None,
                destination: ReplicationDestination {
                    bucket: "dest-bucket".to_string(),
                    account: None,
                    storage_class: None,
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: None,
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };

        store.set("test-bucket".to_string(), config).unwrap();
        assert!(store.is_enabled("test-bucket"));

        assert!(store.remove("test-bucket"));
        assert!(!store.is_enabled("test-bucket"));
    }

    #[test]
    fn test_replication_state_thread_safe() {
        let state = ReplicationState::new();
        let state_clone = state.clone();

        std::thread::spawn(move || {
            let mut store = state_clone.write();
            let config = ReplicationConfiguration {
                role: "test-role".to_string(),
                rules: vec![ReplicationRule {
                    id: Some("rule1".to_string()),
                    priority: Some(1),
                    status: ReplicationStatus::Enabled,
                    filter: None,
                    destination: ReplicationDestination {
                        bucket: "dest-bucket".to_string(),
                        account: None,
                        storage_class: None,
                        access_control_translation: None,
                        encryption_configuration: None,
                        replication_time: None,
                        metrics: None,
                    },
                    delete_marker_replication: None,
                    source_selection_criteria: None,
                    existing_object_replication: None,
                }],
            };
            store.set("bucket1".to_string(), config).unwrap();
        })
        .join()
        .unwrap();

        let store = state.read();
        assert!(store.is_enabled("bucket1"));
    }

    #[test]
    fn test_disabled_rules_not_returned() {
        let mut store = ReplicationStore::new();

        let config = ReplicationConfiguration {
            role: "test-role".to_string(),
            rules: vec![
                ReplicationRule {
                    id: Some("enabled-rule".to_string()),
                    priority: Some(1),
                    status: ReplicationStatus::Enabled,
                    filter: None,
                    destination: ReplicationDestination {
                        bucket: "dest-bucket".to_string(),
                        account: None,
                        storage_class: None,
                        access_control_translation: None,
                        encryption_configuration: None,
                        replication_time: None,
                        metrics: None,
                    },
                    delete_marker_replication: None,
                    source_selection_criteria: None,
                    existing_object_replication: None,
                },
                ReplicationRule {
                    id: Some("disabled-rule".to_string()),
                    priority: Some(2),
                    status: ReplicationStatus::Disabled,
                    filter: None,
                    destination: ReplicationDestination {
                        bucket: "dest-bucket-2".to_string(),
                        account: None,
                        storage_class: None,
                        access_control_translation: None,
                        encryption_configuration: None,
                        replication_time: None,
                        metrics: None,
                    },
                    delete_marker_replication: None,
                    source_selection_criteria: None,
                    existing_object_replication: None,
                },
            ],
        };

        store.set("test-bucket".to_string(), config).unwrap();
        let enabled_rules = store.get_enabled_rules("test-bucket");
        assert_eq!(enabled_rules.len(), 1);
        assert_eq!(enabled_rules[0].id, Some("enabled-rule".to_string()));
    }
}
