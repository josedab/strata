//! S3 object versioning support.
//!
//! Implements bucket versioning configuration and version ID management.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Versioning status for a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum VersioningStatus {
    /// Versioning is enabled.
    Enabled,
    /// Versioning is suspended (new versions not created, but existing preserved).
    Suspended,
}

impl VersioningStatus {
    /// Parse versioning status from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "enabled" => Some(Self::Enabled),
            "suspended" => Some(Self::Suspended),
            _ => None,
        }
    }

    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Suspended => "Suspended",
        }
    }
}

/// Versioning configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersioningConfiguration {
    /// Current versioning status (None means versioning was never enabled).
    pub status: Option<VersioningStatus>,
    /// MFA delete setting (optional, for enhanced security).
    pub mfa_delete: Option<bool>,
}

impl Default for VersioningConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl VersioningConfiguration {
    /// Create a new versioning configuration (versioning not enabled).
    pub fn new() -> Self {
        Self {
            status: None,
            mfa_delete: None,
        }
    }

    /// Check if versioning is enabled.
    pub fn is_enabled(&self) -> bool {
        matches!(self.status, Some(VersioningStatus::Enabled))
    }

    /// Check if versioning is suspended.
    pub fn is_suspended(&self) -> bool {
        matches!(self.status, Some(VersioningStatus::Suspended))
    }

    /// Enable versioning.
    pub fn enable(&mut self) {
        self.status = Some(VersioningStatus::Enabled);
    }

    /// Suspend versioning.
    pub fn suspend(&mut self) {
        self.status = Some(VersioningStatus::Suspended);
    }
}

/// Version information for an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    /// Unique version identifier.
    pub version_id: String,
    /// Object key.
    pub key: String,
    /// Whether this is the latest version.
    pub is_latest: bool,
    /// Last modified timestamp.
    pub last_modified: DateTime<Utc>,
    /// ETag of this version.
    pub etag: String,
    /// Size in bytes.
    pub size: u64,
    /// Storage class.
    pub storage_class: String,
    /// Owner information (simplified).
    pub owner_id: String,
}

/// Delete marker for versioned objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMarker {
    /// Unique version identifier.
    pub version_id: String,
    /// Object key.
    pub key: String,
    /// Whether this is the latest version.
    pub is_latest: bool,
    /// When the delete marker was created.
    pub last_modified: DateTime<Utc>,
    /// Owner ID.
    pub owner_id: String,
}

/// Version ID generator.
#[derive(Clone)]
pub struct VersionIdGenerator;

impl Default for VersionIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionIdGenerator {
    /// Create a new version ID generator.
    pub fn new() -> Self {
        Self
    }

    /// Generate a new version ID.
    ///
    /// Version IDs are URL-safe base64-encoded UUIDs.
    pub fn generate(&self) -> String {
        let uuid = Uuid::new_v4();
        base64_url_encode(uuid.as_bytes())
    }

    /// Special version ID for null versions (unversioned objects).
    pub fn null_version() -> &'static str {
        "null"
    }
}

/// URL-safe base64 encoding for version IDs.
fn base64_url_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    URL_SAFE_NO_PAD.encode(data)
}

/// Versioning configuration store.
#[derive(Clone)]
pub struct VersioningStore {
    /// Map of bucket name to versioning configuration.
    configs: Arc<RwLock<HashMap<String, VersioningConfiguration>>>,
}

impl Default for VersioningStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VersioningStore {
    /// Create a new versioning store.
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get versioning configuration for a bucket.
    pub fn get(&self, bucket: &str) -> VersioningConfiguration {
        self.configs
            .read()
            .ok()
            .and_then(|configs| configs.get(bucket).cloned())
            .unwrap_or_default()
    }

    /// Set versioning configuration for a bucket.
    pub fn set(&self, bucket: &str, config: VersioningConfiguration) {
        if let Ok(mut configs) = self.configs.write() {
            configs.insert(bucket.to_string(), config);
        }
    }

    /// Check if versioning is enabled for a bucket.
    pub fn is_enabled(&self, bucket: &str) -> bool {
        self.get(bucket).is_enabled()
    }

    /// Enable versioning for a bucket.
    pub fn enable(&self, bucket: &str) {
        if let Ok(mut configs) = self.configs.write() {
            let config = configs.entry(bucket.to_string()).or_default();
            config.enable();
        }
    }

    /// Suspend versioning for a bucket.
    pub fn suspend(&self, bucket: &str) {
        if let Ok(mut configs) = self.configs.write() {
            let config = configs.entry(bucket.to_string()).or_default();
            config.suspend();
        }
    }
}

/// State for versioning operations.
#[derive(Clone)]
pub struct VersioningState {
    /// Versioning configuration store.
    pub store: VersioningStore,
    /// Version ID generator.
    pub generator: VersionIdGenerator,
}

impl Default for VersioningState {
    fn default() -> Self {
        Self::new()
    }
}

impl VersioningState {
    /// Create new versioning state.
    pub fn new() -> Self {
        Self {
            store: VersioningStore::new(),
            generator: VersionIdGenerator::new(),
        }
    }

    /// Generate a version ID if versioning is enabled for the bucket.
    pub fn generate_version_id(&self, bucket: &str) -> String {
        if self.store.is_enabled(bucket) {
            self.generator.generate()
        } else {
            VersionIdGenerator::null_version().to_string()
        }
    }
}

/// Parse versioning configuration from XML.
pub fn parse_versioning_xml(xml: &str) -> Result<VersioningConfiguration, VersioningError> {
    // Format:
    // <VersioningConfiguration>
    //   <Status>Enabled|Suspended</Status>
    //   <MfaDelete>Enabled|Disabled</MfaDelete>
    // </VersioningConfiguration>

    let mut config = VersioningConfiguration::new();

    // Parse Status
    if let Some(status_start) = xml.find("<Status>") {
        let value_start = status_start + "<Status>".len();
        if let Some(status_end) = xml[value_start..].find("</Status>") {
            let status_str = xml[value_start..value_start + status_end].trim();
            config.status = VersioningStatus::from_str(status_str);
            if config.status.is_none() && !status_str.is_empty() {
                return Err(VersioningError::InvalidStatus(status_str.to_string()));
            }
        }
    }

    // Parse MfaDelete (optional)
    if let Some(mfa_start) = xml.find("<MfaDelete>") {
        let value_start = mfa_start + "<MfaDelete>".len();
        if let Some(mfa_end) = xml[value_start..].find("</MfaDelete>") {
            let mfa_str = xml[value_start..value_start + mfa_end].trim().to_lowercase();
            config.mfa_delete = match mfa_str.as_str() {
                "enabled" => Some(true),
                "disabled" => Some(false),
                _ => None,
            };
        }
    }

    Ok(config)
}

/// Generate XML for versioning configuration.
pub fn versioning_to_xml(config: &VersioningConfiguration) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    if let Some(status) = &config.status {
        xml.push_str(&format!("  <Status>{}</Status>\n", status.as_str()));
    }

    if let Some(mfa_delete) = config.mfa_delete {
        let mfa_str = if mfa_delete { "Enabled" } else { "Disabled" };
        xml.push_str(&format!("  <MfaDelete>{}</MfaDelete>\n", mfa_str));
    }

    xml.push_str("</VersioningConfiguration>");
    xml
}

/// Versioning-related errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum VersioningError {
    #[error("Invalid versioning status: {0}")]
    InvalidStatus(String),

    #[error("Invalid XML format")]
    InvalidXml,

    #[error("MFA authentication required")]
    MfaRequired,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioning_status() {
        assert_eq!(
            VersioningStatus::from_str("Enabled"),
            Some(VersioningStatus::Enabled)
        );
        assert_eq!(
            VersioningStatus::from_str("suspended"),
            Some(VersioningStatus::Suspended)
        );
        assert_eq!(VersioningStatus::from_str("invalid"), None);
    }

    #[test]
    fn test_versioning_configuration() {
        let mut config = VersioningConfiguration::new();
        assert!(!config.is_enabled());
        assert!(!config.is_suspended());

        config.enable();
        assert!(config.is_enabled());
        assert!(!config.is_suspended());

        config.suspend();
        assert!(!config.is_enabled());
        assert!(config.is_suspended());
    }

    #[test]
    fn test_version_id_generator() {
        let generator = VersionIdGenerator::new();
        let id1 = generator.generate();
        let id2 = generator.generate();

        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
        assert_eq!(VersionIdGenerator::null_version(), "null");
    }

    #[test]
    fn test_versioning_store() {
        let store = VersioningStore::new();

        assert!(!store.is_enabled("test-bucket"));

        store.enable("test-bucket");
        assert!(store.is_enabled("test-bucket"));

        store.suspend("test-bucket");
        assert!(!store.is_enabled("test-bucket"));
        assert!(store.get("test-bucket").is_suspended());
    }

    #[test]
    fn test_versioning_state() {
        let state = VersioningState::new();

        // Not enabled - should return "null"
        let version_id = state.generate_version_id("bucket1");
        assert_eq!(version_id, "null");

        // Enable versioning
        state.store.enable("bucket1");
        let version_id = state.generate_version_id("bucket1");
        assert_ne!(version_id, "null");
    }

    #[test]
    fn test_parse_versioning_xml() {
        let xml = r#"
        <VersioningConfiguration>
          <Status>Enabled</Status>
        </VersioningConfiguration>
        "#;

        let config = parse_versioning_xml(xml).unwrap();
        assert!(config.is_enabled());

        let xml = r#"
        <VersioningConfiguration>
          <Status>Suspended</Status>
          <MfaDelete>Enabled</MfaDelete>
        </VersioningConfiguration>
        "#;

        let config = parse_versioning_xml(xml).unwrap();
        assert!(config.is_suspended());
        assert_eq!(config.mfa_delete, Some(true));
    }

    #[test]
    fn test_versioning_to_xml() {
        let mut config = VersioningConfiguration::new();
        config.enable();

        let xml = versioning_to_xml(&config);
        assert!(xml.contains("<Status>Enabled</Status>"));

        config.mfa_delete = Some(false);
        let xml = versioning_to_xml(&config);
        assert!(xml.contains("<MfaDelete>Disabled</MfaDelete>"));
    }
}
