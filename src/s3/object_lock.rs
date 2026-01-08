//! S3 Object Lock support for WORM (Write Once Read Many) compliance.
//!
//! Object Lock helps prevent objects from being deleted or overwritten for a fixed
//! amount of time or indefinitely. It can be used to meet regulatory requirements
//! or add an extra layer of protection against object changes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Object Lock configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObjectLockConfiguration {
    /// Whether object lock is enabled for the bucket.
    pub enabled: bool,
    /// Default retention settings applied to new objects.
    pub default_retention: Option<DefaultRetention>,
}

impl Default for ObjectLockConfiguration {
    fn default() -> Self {
        Self {
            enabled: false,
            default_retention: None,
        }
    }
}

/// Default retention settings for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DefaultRetention {
    /// The retention mode.
    pub mode: RetentionMode,
    /// Number of days for retention (mutually exclusive with years).
    pub days: Option<u32>,
    /// Number of years for retention (mutually exclusive with days).
    pub years: Option<u32>,
}

/// Retention mode for object lock.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetentionMode {
    /// Objects can be deleted by users with special permissions before retention expires.
    Governance,
    /// Objects cannot be deleted by any user until retention expires.
    Compliance,
}

impl std::fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionMode::Governance => write!(f, "GOVERNANCE"),
            RetentionMode::Compliance => write!(f, "COMPLIANCE"),
        }
    }
}

/// Object-level retention settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObjectRetention {
    /// The retention mode.
    pub mode: RetentionMode,
    /// The date until which the object is retained.
    pub retain_until_date: DateTime<Utc>,
}

/// Legal hold status for an object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObjectLegalHold {
    /// Whether legal hold is enabled.
    pub status: LegalHoldStatus,
}

/// Legal hold status values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LegalHoldStatus {
    /// Legal hold is active.
    On,
    /// Legal hold is not active.
    Off,
}

impl std::fmt::Display for LegalHoldStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LegalHoldStatus::On => write!(f, "ON"),
            LegalHoldStatus::Off => write!(f, "OFF"),
        }
    }
}

/// Key for object-level lock settings.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ObjectKey {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// Error types for object lock operations.
#[derive(Debug, Clone)]
pub enum ObjectLockError {
    /// Object lock is not enabled on the bucket.
    NotEnabled,
    /// Invalid retention period.
    InvalidRetention(String),
    /// Object is locked and cannot be modified.
    ObjectLocked(String),
    /// Invalid configuration.
    InvalidConfiguration(String),
    /// Cannot shorten retention in compliance mode.
    RetentionPeriodTooShort,
}

impl std::fmt::Display for ObjectLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectLockError::NotEnabled => write!(f, "Object Lock is not enabled for this bucket"),
            ObjectLockError::InvalidRetention(msg) => write!(f, "Invalid retention: {}", msg),
            ObjectLockError::ObjectLocked(msg) => write!(f, "Object is locked: {}", msg),
            ObjectLockError::InvalidConfiguration(msg) => {
                write!(f, "Invalid configuration: {}", msg)
            }
            ObjectLockError::RetentionPeriodTooShort => {
                write!(f, "Cannot shorten retention period in COMPLIANCE mode")
            }
        }
    }
}

impl std::error::Error for ObjectLockError {}

/// Storage for object lock configurations.
#[derive(Debug, Default)]
pub struct ObjectLockStore {
    /// Bucket-level object lock configurations.
    bucket_configs: HashMap<String, ObjectLockConfiguration>,
    /// Object-level retention settings.
    object_retentions: HashMap<ObjectKey, ObjectRetention>,
    /// Object-level legal holds.
    object_legal_holds: HashMap<ObjectKey, ObjectLegalHold>,
}

impl ObjectLockStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get bucket object lock configuration.
    pub fn get_bucket_config(&self, bucket: &str) -> Option<&ObjectLockConfiguration> {
        self.bucket_configs.get(bucket)
    }

    /// Set bucket object lock configuration.
    pub fn set_bucket_config(
        &mut self,
        bucket: String,
        config: ObjectLockConfiguration,
    ) -> Result<(), ObjectLockError> {
        // Validate configuration
        if let Some(ref retention) = config.default_retention {
            if retention.days.is_some() && retention.years.is_some() {
                return Err(ObjectLockError::InvalidConfiguration(
                    "Cannot specify both days and years".to_string(),
                ));
            }
            if retention.days.is_none() && retention.years.is_none() {
                return Err(ObjectLockError::InvalidConfiguration(
                    "Must specify either days or years".to_string(),
                ));
            }
        }
        self.bucket_configs.insert(bucket, config);
        Ok(())
    }

    /// Check if object lock is enabled for a bucket.
    pub fn is_enabled(&self, bucket: &str) -> bool {
        self.bucket_configs
            .get(bucket)
            .map(|c| c.enabled)
            .unwrap_or(false)
    }

    /// Get object retention.
    pub fn get_retention(&self, key: &ObjectKey) -> Option<&ObjectRetention> {
        self.object_retentions.get(key)
    }

    /// Set object retention.
    pub fn set_retention(
        &mut self,
        key: ObjectKey,
        retention: ObjectRetention,
    ) -> Result<(), ObjectLockError> {
        // Check if there's existing retention in COMPLIANCE mode
        if let Some(existing) = self.object_retentions.get(&key) {
            if existing.mode == RetentionMode::Compliance {
                // Cannot shorten retention period
                if retention.retain_until_date < existing.retain_until_date {
                    return Err(ObjectLockError::RetentionPeriodTooShort);
                }
            }
        }

        // Validate retention date is in the future
        if retention.retain_until_date <= Utc::now() {
            return Err(ObjectLockError::InvalidRetention(
                "Retain until date must be in the future".to_string(),
            ));
        }

        self.object_retentions.insert(key, retention);
        Ok(())
    }

    /// Get object legal hold.
    pub fn get_legal_hold(&self, key: &ObjectKey) -> Option<&ObjectLegalHold> {
        self.object_legal_holds.get(key)
    }

    /// Set object legal hold.
    pub fn set_legal_hold(&mut self, key: ObjectKey, hold: ObjectLegalHold) {
        self.object_legal_holds.insert(key, hold);
    }

    /// Check if an object can be deleted.
    pub fn can_delete(&self, key: &ObjectKey) -> Result<(), ObjectLockError> {
        // Check legal hold
        if let Some(hold) = self.object_legal_holds.get(key) {
            if hold.status == LegalHoldStatus::On {
                return Err(ObjectLockError::ObjectLocked(
                    "Object has legal hold enabled".to_string(),
                ));
            }
        }

        // Check retention
        if let Some(retention) = self.object_retentions.get(key) {
            if retention.retain_until_date > Utc::now() {
                return Err(ObjectLockError::ObjectLocked(format!(
                    "Object is retained until {}",
                    retention.retain_until_date
                )));
            }
        }

        Ok(())
    }

    /// Check if an object can be overwritten.
    pub fn can_overwrite(&self, key: &ObjectKey) -> Result<(), ObjectLockError> {
        // Same rules as delete for WORM compliance
        self.can_delete(key)
    }
}

/// Thread-safe wrapper for ObjectLockStore.
#[derive(Debug, Clone, Default)]
pub struct ObjectLockState {
    store: Arc<RwLock<ObjectLockStore>>,
}

impl ObjectLockState {
    /// Create a new state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the inner store for reading.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, ObjectLockStore> {
        self.store.read().unwrap()
    }

    /// Get the inner store for writing.
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, ObjectLockStore> {
        self.store.write().unwrap()
    }
}

/// Parse object lock configuration from XML.
pub fn parse_object_lock_xml(xml: &str) -> Result<ObjectLockConfiguration, ObjectLockError> {
    let mut config = ObjectLockConfiguration::default();

    // Check if enabled
    if xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>") {
        config.enabled = true;
    }

    // Parse default retention if present
    if xml.contains("<DefaultRetention>") {
        let mode = if xml.contains("<Mode>GOVERNANCE</Mode>") {
            RetentionMode::Governance
        } else if xml.contains("<Mode>COMPLIANCE</Mode>") {
            RetentionMode::Compliance
        } else {
            return Err(ObjectLockError::InvalidConfiguration(
                "Invalid or missing retention mode".to_string(),
            ));
        };

        let days = extract_xml_value(xml, "Days").and_then(|s| s.parse().ok());
        let years = extract_xml_value(xml, "Years").and_then(|s| s.parse().ok());

        if days.is_none() && years.is_none() {
            return Err(ObjectLockError::InvalidConfiguration(
                "Must specify Days or Years for default retention".to_string(),
            ));
        }

        config.default_retention = Some(DefaultRetention { mode, days, years });
    }

    Ok(config)
}

/// Parse object retention from XML.
pub fn parse_retention_xml(xml: &str) -> Result<ObjectRetention, ObjectLockError> {
    let mode = if xml.contains("<Mode>GOVERNANCE</Mode>") {
        RetentionMode::Governance
    } else if xml.contains("<Mode>COMPLIANCE</Mode>") {
        RetentionMode::Compliance
    } else {
        return Err(ObjectLockError::InvalidRetention(
            "Invalid or missing retention mode".to_string(),
        ));
    };

    let retain_until_str = extract_xml_value(xml, "RetainUntilDate").ok_or_else(|| {
        ObjectLockError::InvalidRetention("Missing RetainUntilDate".to_string())
    })?;

    let retain_until_date = DateTime::parse_from_rfc3339(&retain_until_str)
        .map_err(|e| ObjectLockError::InvalidRetention(format!("Invalid date format: {}", e)))?
        .with_timezone(&Utc);

    Ok(ObjectRetention {
        mode,
        retain_until_date,
    })
}

/// Parse legal hold from XML.
pub fn parse_legal_hold_xml(xml: &str) -> Result<ObjectLegalHold, ObjectLockError> {
    let status = if xml.contains("<Status>ON</Status>") {
        LegalHoldStatus::On
    } else if xml.contains("<Status>OFF</Status>") {
        LegalHoldStatus::Off
    } else {
        return Err(ObjectLockError::InvalidConfiguration(
            "Invalid or missing legal hold status".to_string(),
        ));
    };

    Ok(ObjectLegalHold { status })
}

/// Generate XML for object lock configuration.
pub fn object_lock_to_xml(config: &ObjectLockConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );

    if config.enabled {
        xml.push_str("\n    <ObjectLockEnabled>Enabled</ObjectLockEnabled>");
    }

    if let Some(ref retention) = config.default_retention {
        xml.push_str("\n    <Rule>\n        <DefaultRetention>");
        xml.push_str(&format!("\n            <Mode>{}</Mode>", retention.mode));

        if let Some(days) = retention.days {
            xml.push_str(&format!("\n            <Days>{}</Days>", days));
        }
        if let Some(years) = retention.years {
            xml.push_str(&format!("\n            <Years>{}</Years>", years));
        }

        xml.push_str("\n        </DefaultRetention>\n    </Rule>");
    }

    xml.push_str("\n</ObjectLockConfiguration>");
    xml
}

/// Generate XML for object retention.
pub fn retention_to_xml(retention: &ObjectRetention) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Mode>{}</Mode>
    <RetainUntilDate>{}</RetainUntilDate>
</Retention>"#,
        retention.mode,
        retention.retain_until_date.to_rfc3339()
    )
}

/// Generate XML for legal hold.
pub fn legal_hold_to_xml(hold: &ObjectLegalHold) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Status>{}</Status>
</LegalHold>"#,
        hold.status
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_lock_configuration_default() {
        let config = ObjectLockConfiguration::default();
        assert!(!config.enabled);
        assert!(config.default_retention.is_none());
    }

    #[test]
    fn test_parse_object_lock_xml_enabled() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ObjectLockConfiguration>
                <ObjectLockEnabled>Enabled</ObjectLockEnabled>
            </ObjectLockConfiguration>"#;

        let config = parse_object_lock_xml(xml).unwrap();
        assert!(config.enabled);
        assert!(config.default_retention.is_none());
    }

    #[test]
    fn test_parse_object_lock_xml_with_retention() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ObjectLockConfiguration>
                <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                <Rule>
                    <DefaultRetention>
                        <Mode>COMPLIANCE</Mode>
                        <Days>30</Days>
                    </DefaultRetention>
                </Rule>
            </ObjectLockConfiguration>"#;

        let config = parse_object_lock_xml(xml).unwrap();
        assert!(config.enabled);
        let retention = config.default_retention.unwrap();
        assert_eq!(retention.mode, RetentionMode::Compliance);
        assert_eq!(retention.days, Some(30));
        assert!(retention.years.is_none());
    }

    #[test]
    fn test_parse_retention_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Retention>
                <Mode>GOVERNANCE</Mode>
                <RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
            </Retention>"#;

        let retention = parse_retention_xml(xml).unwrap();
        assert_eq!(retention.mode, RetentionMode::Governance);
    }

    #[test]
    fn test_parse_legal_hold_xml() {
        let xml = r#"<LegalHold><Status>ON</Status></LegalHold>"#;
        let hold = parse_legal_hold_xml(xml).unwrap();
        assert_eq!(hold.status, LegalHoldStatus::On);
    }

    #[test]
    fn test_object_lock_to_xml() {
        let config = ObjectLockConfiguration {
            enabled: true,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::Governance,
                days: Some(90),
                years: None,
            }),
        };

        let xml = object_lock_to_xml(&config);
        assert!(xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<Days>90</Days>"));
    }

    #[test]
    fn test_can_delete_with_legal_hold() {
        let mut store = ObjectLockStore::new();
        let key = ObjectKey {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            version_id: None,
        };

        store.set_legal_hold(
            key.clone(),
            ObjectLegalHold {
                status: LegalHoldStatus::On,
            },
        );

        assert!(store.can_delete(&key).is_err());
    }

    #[test]
    fn test_can_delete_with_retention() {
        let mut store = ObjectLockStore::new();
        let key = ObjectKey {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            version_id: None,
        };

        // Set retention to future date
        let future_date = Utc::now() + chrono::Duration::days(30);
        store
            .set_retention(
                key.clone(),
                ObjectRetention {
                    mode: RetentionMode::Compliance,
                    retain_until_date: future_date,
                },
            )
            .unwrap();

        assert!(store.can_delete(&key).is_err());
    }

    #[test]
    fn test_cannot_shorten_compliance_retention() {
        let mut store = ObjectLockStore::new();
        let key = ObjectKey {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            version_id: None,
        };

        // Set initial retention
        let future_date = Utc::now() + chrono::Duration::days(60);
        store
            .set_retention(
                key.clone(),
                ObjectRetention {
                    mode: RetentionMode::Compliance,
                    retain_until_date: future_date,
                },
            )
            .unwrap();

        // Try to shorten retention
        let shorter_date = Utc::now() + chrono::Duration::days(30);
        let result = store.set_retention(
            key,
            ObjectRetention {
                mode: RetentionMode::Compliance,
                retain_until_date: shorter_date,
            },
        );

        assert!(matches!(
            result,
            Err(ObjectLockError::RetentionPeriodTooShort)
        ));
    }

    #[test]
    fn test_object_lock_state_thread_safe() {
        let state = ObjectLockState::new();
        let state_clone = state.clone();

        std::thread::spawn(move || {
            let mut store = state_clone.write();
            store
                .set_bucket_config(
                    "bucket1".to_string(),
                    ObjectLockConfiguration {
                        enabled: true,
                        default_retention: None,
                    },
                )
                .unwrap();
        })
        .join()
        .unwrap();

        let store = state.read();
        assert!(store.is_enabled("bucket1"));
    }
}
