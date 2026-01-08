//! S3 credential storage and management.
//!
//! Provides storage and lookup for S3 access keys used in AWS Signature V4 authentication.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::SystemTime;

/// S3 credential errors.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("Access key not found: {0}")]
    NotFound(String),
    #[error("Access key disabled: {0}")]
    Disabled(String),
    #[error("Invalid credential format")]
    InvalidFormat,
}

/// An S3 access key with associated metadata.
#[derive(Debug, Clone)]
pub struct S3AccessKey {
    /// The access key ID (public identifier).
    pub access_key_id: String,
    /// The secret access key (used for signing).
    pub secret_key: String,
    /// Associated user ID for internal mapping.
    pub user_id: String,
    /// Optional display name.
    pub display_name: Option<String>,
    /// When the key was created.
    pub created_at: SystemTime,
    /// Whether the key is enabled.
    pub enabled: bool,
}

impl S3AccessKey {
    /// Create a new S3 access key.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_key: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_key: secret_key.into(),
            user_id: user_id.into(),
            display_name: None,
            created_at: SystemTime::now(),
            enabled: true,
        }
    }

    /// Set the display name.
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    /// Disable this key.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Enable this key.
    pub fn enable(&mut self) {
        self.enabled = true;
    }
}

/// S3 credential store for managing access keys.
#[derive(Debug)]
pub struct S3CredentialStore {
    /// Map of access_key_id -> S3AccessKey.
    credentials: RwLock<HashMap<String, S3AccessKey>>,
}

impl S3CredentialStore {
    /// Create an empty credential store.
    pub fn new() -> Self {
        Self {
            credentials: RwLock::new(HashMap::new()),
        }
    }

    /// Create a credential store from a list of access keys.
    pub fn from_keys(keys: Vec<S3AccessKey>) -> Self {
        let mut map = HashMap::new();
        for key in keys {
            map.insert(key.access_key_id.clone(), key);
        }
        Self {
            credentials: RwLock::new(map),
        }
    }

    /// Add an access key to the store.
    pub fn add_key(&self, key: S3AccessKey) {
        let mut creds = self.credentials.write().unwrap_or_else(|e| e.into_inner());
        creds.insert(key.access_key_id.clone(), key);
    }

    /// Remove an access key from the store.
    pub fn remove_key(&self, access_key_id: &str) -> Option<S3AccessKey> {
        let mut creds = self.credentials.write().unwrap_or_else(|e| e.into_inner());
        creds.remove(access_key_id)
    }

    /// Get an access key by ID.
    pub fn get_key(&self, access_key_id: &str) -> Result<S3AccessKey, CredentialError> {
        let creds = self.credentials.read().unwrap_or_else(|e| e.into_inner());
        match creds.get(access_key_id) {
            Some(key) if key.enabled => Ok(key.clone()),
            Some(_) => Err(CredentialError::Disabled(access_key_id.to_string())),
            None => Err(CredentialError::NotFound(access_key_id.to_string())),
        }
    }

    /// Get the secret key for an access key ID.
    pub fn get_secret(&self, access_key_id: &str) -> Result<String, CredentialError> {
        self.get_key(access_key_id).map(|k| k.secret_key)
    }

    /// Check if an access key exists and is enabled.
    pub fn is_valid(&self, access_key_id: &str) -> bool {
        self.get_key(access_key_id).is_ok()
    }

    /// List all access key IDs (not secrets).
    pub fn list_key_ids(&self) -> Vec<String> {
        let creds = self.credentials.read().unwrap_or_else(|e| e.into_inner());
        creds.keys().cloned().collect()
    }

    /// Get the number of stored credentials.
    pub fn len(&self) -> usize {
        let creds = self.credentials.read().unwrap_or_else(|e| e.into_inner());
        creds.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for S3CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for S3CredentialStore {
    fn clone(&self) -> Self {
        let creds = self.credentials.read().unwrap_or_else(|e| e.into_inner());
        Self {
            credentials: RwLock::new(creds.clone()),
        }
    }
}

/// Configuration for S3 authentication.
#[derive(Debug, Clone)]
pub struct S3AuthConfig {
    /// Whether to allow anonymous (unsigned) requests.
    pub allow_anonymous: bool,
    /// Static credentials loaded from config.
    pub static_credentials: Vec<StaticCredential>,
}

impl S3AuthConfig {
    /// Create auth config that allows anonymous access.
    pub fn anonymous() -> Self {
        Self {
            allow_anonymous: true,
            static_credentials: vec![],
        }
    }

    /// Create auth config requiring authentication.
    pub fn authenticated() -> Self {
        Self {
            allow_anonymous: false,
            static_credentials: vec![],
        }
    }

    /// Add static credentials.
    pub fn with_credentials(mut self, credentials: Vec<StaticCredential>) -> Self {
        self.static_credentials = credentials;
        self
    }

    /// Build the credential store from this config.
    pub fn build_store(&self) -> S3CredentialStore {
        let keys: Vec<S3AccessKey> = self
            .static_credentials
            .iter()
            .map(|c| {
                S3AccessKey::new(&c.access_key_id, &c.secret_key, &c.user_id)
                    .with_display_name(c.display_name.clone().unwrap_or_default())
            })
            .collect();
        S3CredentialStore::from_keys(keys)
    }
}

impl Default for S3AuthConfig {
    fn default() -> Self {
        // Default to anonymous for backwards compatibility
        Self::anonymous()
    }
}

/// A static credential entry from configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StaticCredential {
    /// The access key ID.
    pub access_key_id: String,
    /// The secret access key.
    pub secret_key: String,
    /// Associated user ID.
    pub user_id: String,
    /// Optional display name.
    pub display_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_key_creation() {
        let key = S3AccessKey::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "user123");

        assert_eq!(key.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(key.user_id, "user123");
        assert!(key.enabled);
    }

    #[test]
    fn test_credential_store_basic() {
        let store = S3CredentialStore::new();
        assert!(store.is_empty());

        let key = S3AccessKey::new("AKID1", "secret1", "user1");
        store.add_key(key);

        assert_eq!(store.len(), 1);
        assert!(store.is_valid("AKID1"));
        assert!(!store.is_valid("AKID2"));
    }

    #[test]
    fn test_credential_store_get_secret() {
        let store = S3CredentialStore::new();
        store.add_key(S3AccessKey::new("AKID1", "secret1", "user1"));

        let secret = store.get_secret("AKID1");
        assert!(secret.is_ok());
        assert_eq!(secret.unwrap(), "secret1");

        let missing = store.get_secret("AKID_MISSING");
        assert!(matches!(missing, Err(CredentialError::NotFound(_))));
    }

    #[test]
    fn test_disabled_key() {
        let store = S3CredentialStore::new();
        let mut key = S3AccessKey::new("AKID1", "secret1", "user1");
        key.disable();
        store.add_key(key);

        let result = store.get_key("AKID1");
        assert!(matches!(result, Err(CredentialError::Disabled(_))));
    }

    #[test]
    fn test_auth_config_build_store() {
        let config = S3AuthConfig::authenticated().with_credentials(vec![
            StaticCredential {
                access_key_id: "AKID1".to_string(),
                secret_key: "secret1".to_string(),
                user_id: "user1".to_string(),
                display_name: Some("Test Key".to_string()),
            },
        ]);

        let store = config.build_store();
        assert_eq!(store.len(), 1);
        assert!(store.is_valid("AKID1"));
    }
}
