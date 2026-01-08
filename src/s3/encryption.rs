//! S3 server-side encryption (SSE) support.
//!
//! Implements bucket default encryption configuration and per-object encryption.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Server-side encryption configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfiguration {
    /// Default encryption rules.
    pub rules: Vec<EncryptionRule>,
}

impl Default for EncryptionConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionConfiguration {
    /// Create a new encryption configuration.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Create a configuration with SSE-S3 (AES256) default encryption.
    pub fn with_sse_s3() -> Self {
        Self {
            rules: vec![EncryptionRule {
                apply_server_side_encryption_by_default: DefaultEncryption {
                    sse_algorithm: SseAlgorithm::Aes256,
                    kms_master_key_id: None,
                },
                bucket_key_enabled: None,
            }],
        }
    }

    /// Create a configuration with SSE-KMS default encryption.
    pub fn with_sse_kms(key_id: impl Into<String>) -> Self {
        Self {
            rules: vec![EncryptionRule {
                apply_server_side_encryption_by_default: DefaultEncryption {
                    sse_algorithm: SseAlgorithm::AwsKms,
                    kms_master_key_id: Some(key_id.into()),
                },
                bucket_key_enabled: Some(true),
            }],
        }
    }

    /// Get the default SSE algorithm.
    pub fn default_algorithm(&self) -> Option<&SseAlgorithm> {
        self.rules
            .first()
            .map(|r| &r.apply_server_side_encryption_by_default.sse_algorithm)
    }

    /// Check if bucket key is enabled.
    pub fn bucket_key_enabled(&self) -> bool {
        self.rules
            .first()
            .and_then(|r| r.bucket_key_enabled)
            .unwrap_or(false)
    }
}

/// A single encryption rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionRule {
    /// Default encryption settings.
    pub apply_server_side_encryption_by_default: DefaultEncryption,
    /// Whether to use bucket key for SSE-KMS.
    pub bucket_key_enabled: Option<bool>,
}

/// Default encryption settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultEncryption {
    /// SSE algorithm (AES256 or aws:kms).
    pub sse_algorithm: SseAlgorithm,
    /// KMS master key ID (only for SSE-KMS).
    pub kms_master_key_id: Option<String>,
}

/// Server-side encryption algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    /// AES-256 encryption (SSE-S3).
    #[serde(rename = "AES256")]
    Aes256,
    /// KMS encryption (SSE-KMS).
    #[serde(rename = "aws:kms")]
    AwsKms,
    /// KMS with DSSE (Dual-layer SSE-KMS).
    #[serde(rename = "aws:kms:dsse")]
    AwsKmsDsse,
}

impl SseAlgorithm {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "AES256" => Some(Self::Aes256),
            "aws:kms" => Some(Self::AwsKms),
            "aws:kms:dsse" => Some(Self::AwsKmsDsse),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aes256 => "AES256",
            Self::AwsKms => "aws:kms",
            Self::AwsKmsDsse => "aws:kms:dsse",
        }
    }
}

/// Per-object encryption context.
#[derive(Debug, Clone, Default)]
pub struct ObjectEncryption {
    /// SSE algorithm used.
    pub algorithm: Option<SseAlgorithm>,
    /// KMS key ID (for SSE-KMS).
    pub kms_key_id: Option<String>,
    /// Customer-provided encryption key hash (for SSE-C).
    pub customer_key_md5: Option<String>,
    /// Encryption context for SSE-KMS.
    pub encryption_context: Option<String>,
    /// Whether bucket key was used.
    pub bucket_key_enabled: bool,
}

impl ObjectEncryption {
    /// Create SSE-S3 encryption context.
    pub fn sse_s3() -> Self {
        Self {
            algorithm: Some(SseAlgorithm::Aes256),
            ..Default::default()
        }
    }

    /// Create SSE-KMS encryption context.
    pub fn sse_kms(key_id: impl Into<String>) -> Self {
        Self {
            algorithm: Some(SseAlgorithm::AwsKms),
            kms_key_id: Some(key_id.into()),
            ..Default::default()
        }
    }

    /// Check if object is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.algorithm.is_some()
    }
}

/// Encryption-related errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum EncryptionError {
    #[error("Invalid SSE algorithm")]
    InvalidAlgorithm,

    #[error("Missing KMS key ID for SSE-KMS")]
    MissingKmsKeyId,

    #[error("Encryption configuration not found")]
    NotFound,

    #[error("Invalid XML: {0}")]
    InvalidXml(String),

    #[error("SSE-C not supported")]
    SseCNotSupported,
}

/// Encryption configuration store.
#[derive(Clone)]
pub struct EncryptionStore {
    /// Map of bucket name to encryption configuration.
    configs: Arc<RwLock<HashMap<String, EncryptionConfiguration>>>,
}

impl Default for EncryptionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionStore {
    /// Create a new encryption store.
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get encryption configuration for a bucket.
    pub fn get(&self, bucket: &str) -> Option<EncryptionConfiguration> {
        self.configs.read().ok()?.get(bucket).cloned()
    }

    /// Set encryption configuration for a bucket.
    pub fn set(&self, bucket: &str, config: EncryptionConfiguration) {
        if let Ok(mut configs) = self.configs.write() {
            configs.insert(bucket.to_string(), config);
        }
    }

    /// Remove encryption configuration for a bucket.
    pub fn remove(&self, bucket: &str) -> bool {
        if let Ok(mut configs) = self.configs.write() {
            return configs.remove(bucket).is_some();
        }
        false
    }
}

/// State for encryption operations.
#[derive(Clone)]
pub struct EncryptionState {
    /// Encryption configuration store.
    pub store: EncryptionStore,
}

impl Default for EncryptionState {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionState {
    /// Create new encryption state.
    pub fn new() -> Self {
        Self {
            store: EncryptionStore::new(),
        }
    }

    /// Get the effective encryption for an object.
    ///
    /// Uses object-level encryption if specified, otherwise bucket default.
    pub fn effective_encryption(
        &self,
        bucket: &str,
        object_encryption: Option<ObjectEncryption>,
    ) -> Option<ObjectEncryption> {
        // Object-level encryption takes precedence
        if let Some(enc) = object_encryption {
            if enc.is_encrypted() {
                return Some(enc);
            }
        }

        // Fall back to bucket default
        self.store.get(bucket).and_then(|config| {
            config.rules.first().map(|rule| {
                let mut enc = ObjectEncryption::default();
                enc.algorithm = Some(rule.apply_server_side_encryption_by_default.sse_algorithm);
                enc.kms_key_id = rule
                    .apply_server_side_encryption_by_default
                    .kms_master_key_id
                    .clone();
                enc.bucket_key_enabled = rule.bucket_key_enabled.unwrap_or(false);
                enc
            })
        })
    }
}

/// Parse encryption configuration from XML.
pub fn parse_encryption_xml(xml: &str) -> Result<EncryptionConfiguration, EncryptionError> {
    // Format:
    // <ServerSideEncryptionConfiguration>
    //   <Rule>
    //     <ApplyServerSideEncryptionByDefault>
    //       <SSEAlgorithm>AES256|aws:kms</SSEAlgorithm>
    //       <KMSMasterKeyID>key-id</KMSMasterKeyID>
    //     </ApplyServerSideEncryptionByDefault>
    //     <BucketKeyEnabled>true|false</BucketKeyEnabled>
    //   </Rule>
    // </ServerSideEncryptionConfiguration>

    let mut config = EncryptionConfiguration::new();

    // Find Rule blocks
    let mut pos = 0;
    while let Some(rule_start) = xml[pos..].find("<Rule>") {
        let rule_start = pos + rule_start;
        let rule_end = xml[rule_start..]
            .find("</Rule>")
            .map(|i| rule_start + i + 7)
            .ok_or_else(|| EncryptionError::InvalidXml("Missing </Rule>".to_string()))?;

        let rule_xml = &xml[rule_start..rule_end];
        let rule = parse_encryption_rule(rule_xml)?;
        config.rules.push(rule);

        pos = rule_end;
    }

    if config.rules.is_empty() {
        return Err(EncryptionError::InvalidXml("No rules found".to_string()));
    }

    Ok(config)
}

/// Parse a single encryption rule from XML.
fn parse_encryption_rule(xml: &str) -> Result<EncryptionRule, EncryptionError> {
    // Parse SSE algorithm
    let algorithm = extract_xml_value(xml, "SSEAlgorithm")
        .and_then(|s| SseAlgorithm::from_str(&s))
        .ok_or(EncryptionError::InvalidAlgorithm)?;

    // Parse KMS key ID (optional, required for SSE-KMS)
    let kms_key_id = extract_xml_value(xml, "KMSMasterKeyID");

    // For SSE-KMS, key ID is optional (uses default key if not specified)

    // Parse bucket key enabled
    let bucket_key_enabled = extract_xml_value(xml, "BucketKeyEnabled")
        .map(|s| s.to_lowercase() == "true");

    Ok(EncryptionRule {
        apply_server_side_encryption_by_default: DefaultEncryption {
            sse_algorithm: algorithm,
            kms_master_key_id: kms_key_id,
        },
        bucket_key_enabled,
    })
}

/// Extract a single XML element value.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml[start..].find(&end_tag)?;

    Some(xml[start..start + end].trim().to_string())
}

/// Generate XML for encryption configuration.
pub fn encryption_to_xml(config: &EncryptionConfiguration) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for rule in &config.rules {
        xml.push_str("  <Rule>\n");
        xml.push_str("    <ApplyServerSideEncryptionByDefault>\n");
        xml.push_str(&format!(
            "      <SSEAlgorithm>{}</SSEAlgorithm>\n",
            rule.apply_server_side_encryption_by_default.sse_algorithm.as_str()
        ));

        if let Some(ref key_id) = rule.apply_server_side_encryption_by_default.kms_master_key_id {
            xml.push_str(&format!(
                "      <KMSMasterKeyID>{}</KMSMasterKeyID>\n",
                xml_escape(key_id)
            ));
        }

        xml.push_str("    </ApplyServerSideEncryptionByDefault>\n");

        if let Some(enabled) = rule.bucket_key_enabled {
            xml.push_str(&format!(
                "    <BucketKeyEnabled>{}</BucketKeyEnabled>\n",
                enabled
            ));
        }

        xml.push_str("  </Rule>\n");
    }

    xml.push_str("</ServerSideEncryptionConfiguration>");
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

/// Extract SSE headers from request.
pub fn extract_sse_headers(headers: &axum::http::HeaderMap) -> Option<ObjectEncryption> {
    // Check for SSE-S3
    if let Some(algo) = headers.get("x-amz-server-side-encryption") {
        let algo_str = algo.to_str().ok()?;
        let algorithm = SseAlgorithm::from_str(algo_str)?;

        let kms_key_id = headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let encryption_context = headers
            .get("x-amz-server-side-encryption-context")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let bucket_key_enabled = headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or(false);

        return Some(ObjectEncryption {
            algorithm: Some(algorithm),
            kms_key_id,
            customer_key_md5: None,
            encryption_context,
            bucket_key_enabled,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sse_algorithm() {
        assert_eq!(SseAlgorithm::from_str("AES256"), Some(SseAlgorithm::Aes256));
        assert_eq!(SseAlgorithm::from_str("aws:kms"), Some(SseAlgorithm::AwsKms));
        assert_eq!(SseAlgorithm::from_str("invalid"), None);

        assert_eq!(SseAlgorithm::Aes256.as_str(), "AES256");
        assert_eq!(SseAlgorithm::AwsKms.as_str(), "aws:kms");
    }

    #[test]
    fn test_encryption_configuration_builders() {
        let config = EncryptionConfiguration::with_sse_s3();
        assert_eq!(config.default_algorithm(), Some(&SseAlgorithm::Aes256));
        assert!(!config.bucket_key_enabled());

        let config = EncryptionConfiguration::with_sse_kms("my-key-id");
        assert_eq!(config.default_algorithm(), Some(&SseAlgorithm::AwsKms));
        assert!(config.bucket_key_enabled());
    }

    #[test]
    fn test_object_encryption() {
        let enc = ObjectEncryption::sse_s3();
        assert!(enc.is_encrypted());
        assert_eq!(enc.algorithm, Some(SseAlgorithm::Aes256));

        let enc = ObjectEncryption::sse_kms("key-123");
        assert!(enc.is_encrypted());
        assert_eq!(enc.kms_key_id, Some("key-123".to_string()));
    }

    #[test]
    fn test_encryption_store() {
        let store = EncryptionStore::new();

        let config = EncryptionConfiguration::with_sse_s3();
        store.set("test-bucket", config);

        let retrieved = store.get("test-bucket").unwrap();
        assert_eq!(retrieved.default_algorithm(), Some(&SseAlgorithm::Aes256));

        assert!(store.remove("test-bucket"));
        assert!(store.get("test-bucket").is_none());
    }

    #[test]
    fn test_encryption_state_effective() {
        let state = EncryptionState::new();

        // No encryption configured - should return None
        assert!(state.effective_encryption("bucket1", None).is_none());

        // Set bucket default
        state.store.set("bucket1", EncryptionConfiguration::with_sse_s3());

        // Should use bucket default
        let enc = state.effective_encryption("bucket1", None).unwrap();
        assert_eq!(enc.algorithm, Some(SseAlgorithm::Aes256));

        // Object-level encryption should override
        let obj_enc = ObjectEncryption::sse_kms("custom-key");
        let enc = state.effective_encryption("bucket1", Some(obj_enc)).unwrap();
        assert_eq!(enc.algorithm, Some(SseAlgorithm::AwsKms));
        assert_eq!(enc.kms_key_id, Some("custom-key".to_string()));
    }

    #[test]
    fn test_parse_encryption_xml() {
        let xml = r#"
        <ServerSideEncryptionConfiguration>
          <Rule>
            <ApplyServerSideEncryptionByDefault>
              <SSEAlgorithm>AES256</SSEAlgorithm>
            </ApplyServerSideEncryptionByDefault>
          </Rule>
        </ServerSideEncryptionConfiguration>
        "#;

        let config = parse_encryption_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert_eq!(
            config.rules[0].apply_server_side_encryption_by_default.sse_algorithm,
            SseAlgorithm::Aes256
        );
    }

    #[test]
    fn test_parse_encryption_xml_kms() {
        let xml = r#"
        <ServerSideEncryptionConfiguration>
          <Rule>
            <ApplyServerSideEncryptionByDefault>
              <SSEAlgorithm>aws:kms</SSEAlgorithm>
              <KMSMasterKeyID>arn:aws:kms:us-east-1:123456789:key/12345</KMSMasterKeyID>
            </ApplyServerSideEncryptionByDefault>
            <BucketKeyEnabled>true</BucketKeyEnabled>
          </Rule>
        </ServerSideEncryptionConfiguration>
        "#;

        let config = parse_encryption_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);

        let rule = &config.rules[0];
        assert_eq!(
            rule.apply_server_side_encryption_by_default.sse_algorithm,
            SseAlgorithm::AwsKms
        );
        assert!(rule.apply_server_side_encryption_by_default.kms_master_key_id.is_some());
        assert_eq!(rule.bucket_key_enabled, Some(true));
    }

    #[test]
    fn test_encryption_to_xml() {
        let config = EncryptionConfiguration::with_sse_s3();
        let xml = encryption_to_xml(&config);

        assert!(xml.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"));
    }

    #[test]
    fn test_encryption_to_xml_kms() {
        let config = EncryptionConfiguration::with_sse_kms("my-key");
        let xml = encryption_to_xml(&config);

        assert!(xml.contains("<SSEAlgorithm>aws:kms</SSEAlgorithm>"));
        assert!(xml.contains("<KMSMasterKeyID>my-key</KMSMasterKeyID>"));
        assert!(xml.contains("<BucketKeyEnabled>true</BucketKeyEnabled>"));
    }
}
