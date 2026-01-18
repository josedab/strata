//! Key Management Service (KMS) integration for encryption key management.
//!
//! This module provides an abstract interface for different KMS providers,
//! enabling secure key storage, retrieval, and rotation.

use crate::error::{Result, StrataError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Encryption key with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKey {
    /// Unique key identifier.
    pub key_id: String,
    /// Key material (encrypted if from external KMS).
    pub key_material: Vec<u8>,
    /// Key algorithm.
    pub algorithm: KeyAlgorithm,
    /// Key state.
    pub state: KeyState,
    /// Creation timestamp.
    pub created_at: SystemTime,
    /// Last rotation timestamp.
    pub rotated_at: Option<SystemTime>,
    /// Expiration timestamp.
    pub expires_at: Option<SystemTime>,
    /// Metadata.
    pub metadata: HashMap<String, String>,
}

/// Encryption key algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyAlgorithm {
    /// AES-256-GCM.
    Aes256Gcm,
    /// AES-128-GCM.
    Aes128Gcm,
    /// ChaCha20-Poly1305.
    ChaCha20Poly1305,
}

impl KeyAlgorithm {
    /// Get the key size in bytes.
    pub fn key_size(&self) -> usize {
        match self {
            KeyAlgorithm::Aes256Gcm => 32,
            KeyAlgorithm::Aes128Gcm => 16,
            KeyAlgorithm::ChaCha20Poly1305 => 32,
        }
    }
}

/// Key state in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyState {
    /// Key is active and can be used.
    Active,
    /// Key is disabled but can be re-enabled.
    Disabled,
    /// Key is pending deletion.
    PendingDeletion,
    /// Key exists but can only decrypt (for rotation).
    DecryptOnly,
}

/// Abstract KMS provider trait.
#[async_trait]
pub trait KmsProvider: Send + Sync {
    /// Get the provider name.
    fn name(&self) -> &str;

    /// Create a new encryption key.
    async fn create_key(
        &self,
        key_id: &str,
        algorithm: KeyAlgorithm,
        metadata: HashMap<String, String>,
    ) -> Result<EncryptionKey>;

    /// Get an existing key.
    async fn get_key(&self, key_id: &str) -> Result<EncryptionKey>;

    /// List all keys.
    async fn list_keys(&self) -> Result<Vec<String>>;

    /// Rotate a key (creates new version).
    async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey>;

    /// Disable a key.
    async fn disable_key(&self, key_id: &str) -> Result<()>;

    /// Enable a disabled key.
    async fn enable_key(&self, key_id: &str) -> Result<()>;

    /// Schedule key deletion.
    async fn schedule_deletion(&self, key_id: &str, delay: Duration) -> Result<()>;

    /// Encrypt data using a key.
    async fn encrypt(&self, key_id: &str, plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt data using a key.
    async fn decrypt(&self, key_id: &str, ciphertext: &[u8]) -> Result<Vec<u8>>;

    /// Generate a data encryption key (DEK) protected by the master key.
    async fn generate_data_key(&self, key_id: &str) -> Result<DataKeyPair>;
}

/// A data encryption key pair (plaintext + encrypted).
#[derive(Debug, Clone)]
pub struct DataKeyPair {
    /// Plaintext DEK for immediate use.
    pub plaintext_key: Vec<u8>,
    /// Encrypted DEK for storage.
    pub encrypted_key: Vec<u8>,
}

/// Local KMS provider for development and testing.
pub struct LocalKmsProvider {
    /// Stored keys.
    keys: Arc<RwLock<HashMap<String, EncryptionKey>>>,
    /// Master key for encrypting stored keys (reserved for future use).
    #[allow(dead_code)]
    master_key: [u8; 32],
}

impl LocalKmsProvider {
    /// Create a new local KMS provider.
    pub fn new(master_key: [u8; 32]) -> Self {
        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            master_key,
        }
    }

    /// Create with a random master key (for testing).
    pub fn new_random() -> Self {
        use rand::RngCore;
        let mut master_key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut master_key);
        Self::new(master_key)
    }

    /// Generate random key material.
    fn generate_key_material(size: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut key = vec![0u8; size];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }
}

#[async_trait]
impl KmsProvider for LocalKmsProvider {
    fn name(&self) -> &str {
        "local"
    }

    async fn create_key(
        &self,
        key_id: &str,
        algorithm: KeyAlgorithm,
        metadata: HashMap<String, String>,
    ) -> Result<EncryptionKey> {
        let mut keys = self.keys.write().await;

        if keys.contains_key(key_id) {
            return Err(StrataError::Internal(format!(
                "Key {} already exists",
                key_id
            )));
        }

        let key = EncryptionKey {
            key_id: key_id.to_string(),
            key_material: Self::generate_key_material(algorithm.key_size()),
            algorithm,
            state: KeyState::Active,
            created_at: SystemTime::now(),
            rotated_at: None,
            expires_at: None,
            metadata,
        };

        keys.insert(key_id.to_string(), key.clone());

        info!(key_id, algorithm = ?algorithm, "Created new encryption key");

        Ok(key)
    }

    async fn get_key(&self, key_id: &str) -> Result<EncryptionKey> {
        let keys = self.keys.read().await;

        keys.get(key_id)
            .cloned()
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let keys = self.keys.read().await;
        Ok(keys.keys().cloned().collect())
    }

    async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey> {
        let mut keys = self.keys.write().await;

        let key = keys
            .get_mut(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        // Generate new key material
        key.key_material = Self::generate_key_material(key.algorithm.key_size());
        key.rotated_at = Some(SystemTime::now());

        info!(key_id, "Rotated encryption key");

        Ok(key.clone())
    }

    async fn disable_key(&self, key_id: &str) -> Result<()> {
        let mut keys = self.keys.write().await;

        let key = keys
            .get_mut(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        key.state = KeyState::Disabled;

        warn!(key_id, "Disabled encryption key");

        Ok(())
    }

    async fn enable_key(&self, key_id: &str) -> Result<()> {
        let mut keys = self.keys.write().await;

        let key = keys
            .get_mut(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        if key.state == KeyState::PendingDeletion {
            return Err(StrataError::Internal(
                "Cannot enable key pending deletion".to_string(),
            ));
        }

        key.state = KeyState::Active;

        info!(key_id, "Enabled encryption key");

        Ok(())
    }

    async fn schedule_deletion(&self, key_id: &str, delay: Duration) -> Result<()> {
        let mut keys = self.keys.write().await;

        let key = keys
            .get_mut(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        key.state = KeyState::PendingDeletion;
        key.expires_at = Some(SystemTime::now() + delay);

        warn!(
            key_id,
            delay_secs = delay.as_secs(),
            "Scheduled key for deletion"
        );

        Ok(())
    }

    async fn encrypt(&self, key_id: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
        use ring::aead::{self, Aad, BoundKey, Nonce, NonceSequence, SealingKey, UnboundKey, NONCE_LEN};
        use ring::rand::{SecureRandom, SystemRandom};

        let keys = self.keys.read().await;
        let key = keys
            .get(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        if key.state != KeyState::Active {
            return Err(StrataError::Internal("Key is not active".to_string()));
        }

        // Generate random nonce
        let rng = SystemRandom::new();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rng.fill(&mut nonce_bytes)
            .map_err(|_| StrataError::Internal("Failed to generate nonce".to_string()))?;

        // Create cipher
        let unbound_key = UnboundKey::new(&aead::AES_256_GCM, &key.key_material)
            .map_err(|_| StrataError::Internal("Invalid key".to_string()))?;

        struct SingleNonce([u8; NONCE_LEN]);
        impl NonceSequence for SingleNonce {
            fn advance(&mut self) -> std::result::Result<Nonce, ring::error::Unspecified> {
                Nonce::try_assume_unique_for_key(&self.0)
            }
        }

        let mut sealing_key = SealingKey::new(unbound_key, SingleNonce(nonce_bytes));

        // Encrypt in place
        let mut in_out = plaintext.to_vec();
        sealing_key
            .seal_in_place_append_tag(Aad::empty(), &mut in_out)
            .map_err(|_| StrataError::Internal("Encryption failed".to_string()))?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend(in_out);

        debug!(
            key_id,
            plaintext_len = plaintext.len(),
            ciphertext_len = result.len(),
            "Encrypted data"
        );

        Ok(result)
    }

    async fn decrypt(&self, key_id: &str, ciphertext: &[u8]) -> Result<Vec<u8>> {
        use ring::aead::{self, Aad, BoundKey, Nonce, NonceSequence, OpeningKey, UnboundKey, NONCE_LEN};

        if ciphertext.len() < NONCE_LEN + 16 {
            // nonce + tag minimum
            return Err(StrataError::Internal("Ciphertext too short".to_string()));
        }

        let keys = self.keys.read().await;
        let key = keys
            .get(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        if key.state == KeyState::PendingDeletion {
            return Err(StrataError::Internal(
                "Key is pending deletion".to_string(),
            ));
        }

        // Extract nonce and ciphertext
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&ciphertext[..NONCE_LEN]);
        let actual_ciphertext = &ciphertext[NONCE_LEN..];

        // Create cipher
        let unbound_key = UnboundKey::new(&aead::AES_256_GCM, &key.key_material)
            .map_err(|_| StrataError::Internal("Invalid key".to_string()))?;

        struct SingleNonce([u8; NONCE_LEN]);
        impl NonceSequence for SingleNonce {
            fn advance(&mut self) -> std::result::Result<Nonce, ring::error::Unspecified> {
                Nonce::try_assume_unique_for_key(&self.0)
            }
        }

        let mut opening_key = OpeningKey::new(unbound_key, SingleNonce(nonce_bytes));

        // Decrypt in place
        let mut in_out = actual_ciphertext.to_vec();
        let plaintext = opening_key
            .open_in_place(Aad::empty(), &mut in_out)
            .map_err(|_| StrataError::Internal("Decryption failed".to_string()))?;

        debug!(
            key_id,
            ciphertext_len = ciphertext.len(),
            plaintext_len = plaintext.len(),
            "Decrypted data"
        );

        Ok(plaintext.to_vec())
    }

    async fn generate_data_key(&self, key_id: &str) -> Result<DataKeyPair> {
        let keys = self.keys.read().await;
        let master_key = keys
            .get(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key {} not found", key_id)))?;

        if master_key.state != KeyState::Active {
            return Err(StrataError::Internal("Key is not active".to_string()));
        }

        // Generate DEK
        let plaintext_key = Self::generate_key_material(32);

        // Encrypt DEK with master key
        drop(keys); // Release lock before calling encrypt
        let encrypted_key = self.encrypt(key_id, &plaintext_key).await?;

        Ok(DataKeyPair {
            plaintext_key,
            encrypted_key,
        })
    }
}

/// Per-tenant key management for multi-tenant encryption.
pub struct TenantKeyManager {
    /// KMS provider.
    kms: Arc<dyn KmsProvider>,
    /// Cached tenant keys.
    cache: Arc<RwLock<HashMap<String, EncryptionKey>>>,
    /// Master key ID for tenant key encryption (reserved for envelope encryption).
    #[allow(dead_code)]
    master_key_id: String,
}

impl TenantKeyManager {
    /// Create a new tenant key manager.
    pub fn new(kms: Arc<dyn KmsProvider>, master_key_id: String) -> Self {
        Self {
            kms,
            cache: Arc::new(RwLock::new(HashMap::new())),
            master_key_id,
        }
    }

    /// Get or create a key for a tenant.
    pub async fn get_tenant_key(&self, tenant_id: &str) -> Result<EncryptionKey> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(key) = cache.get(tenant_id) {
                return Ok(key.clone());
            }
        }

        // Try to get from KMS
        let key_id = self.tenant_key_id(tenant_id);
        let key = match self.kms.get_key(&key_id).await {
            Ok(key) => key,
            Err(_) => {
                // Create new tenant key
                let mut metadata = HashMap::new();
                metadata.insert("tenant_id".to_string(), tenant_id.to_string());
                metadata.insert("key_type".to_string(), "tenant".to_string());

                self.kms
                    .create_key(&key_id, KeyAlgorithm::Aes256Gcm, metadata)
                    .await?
            }
        };

        // Cache the key
        {
            let mut cache = self.cache.write().await;
            cache.insert(tenant_id.to_string(), key.clone());
        }

        Ok(key)
    }

    /// Rotate a tenant's key.
    pub async fn rotate_tenant_key(&self, tenant_id: &str) -> Result<EncryptionKey> {
        let key_id = self.tenant_key_id(tenant_id);
        let new_key = self.kms.rotate_key(&key_id).await?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(tenant_id.to_string(), new_key.clone());
        }

        info!(tenant_id, "Rotated tenant encryption key");

        Ok(new_key)
    }

    /// Disable a tenant's key.
    pub async fn disable_tenant_key(&self, tenant_id: &str) -> Result<()> {
        let key_id = self.tenant_key_id(tenant_id);
        self.kms.disable_key(&key_id).await?;

        // Remove from cache
        {
            let mut cache = self.cache.write().await;
            cache.remove(tenant_id);
        }

        warn!(tenant_id, "Disabled tenant encryption key");

        Ok(())
    }

    /// Clear the key cache.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Generate key ID for a tenant.
    fn tenant_key_id(&self, tenant_id: &str) -> String {
        format!("tenant-{}", tenant_id)
    }
}

/// KMS statistics.
#[derive(Debug, Default, Clone)]
pub struct KmsStats {
    /// Total encryption operations.
    pub encryptions: u64,
    /// Total decryption operations.
    pub decryptions: u64,
    /// Key creations.
    pub keys_created: u64,
    /// Key rotations.
    pub keys_rotated: u64,
    /// Cache hits.
    pub cache_hits: u64,
    /// Cache misses.
    pub cache_misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_kms_create_key() {
        let kms = LocalKmsProvider::new_random();

        let key = kms
            .create_key("test-key", KeyAlgorithm::Aes256Gcm, HashMap::new())
            .await
            .unwrap();

        assert_eq!(key.key_id, "test-key");
        assert_eq!(key.algorithm, KeyAlgorithm::Aes256Gcm);
        assert_eq!(key.state, KeyState::Active);
    }

    #[tokio::test]
    async fn test_local_kms_encrypt_decrypt() {
        let kms = LocalKmsProvider::new_random();

        kms.create_key("test-key", KeyAlgorithm::Aes256Gcm, HashMap::new())
            .await
            .unwrap();

        let plaintext = b"Hello, World!";
        let ciphertext = kms.encrypt("test-key", plaintext).await.unwrap();

        assert_ne!(&ciphertext[12..], plaintext);

        let decrypted = kms.decrypt("test-key", &ciphertext).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_local_kms_rotate_key() {
        let kms = LocalKmsProvider::new_random();

        let original = kms
            .create_key("test-key", KeyAlgorithm::Aes256Gcm, HashMap::new())
            .await
            .unwrap();

        let rotated = kms.rotate_key("test-key").await.unwrap();

        assert_ne!(original.key_material, rotated.key_material);
        assert!(rotated.rotated_at.is_some());
    }

    #[tokio::test]
    async fn test_tenant_key_manager() {
        let kms = Arc::new(LocalKmsProvider::new_random());

        // Create master key
        kms.create_key("master", KeyAlgorithm::Aes256Gcm, HashMap::new())
            .await
            .unwrap();

        let manager = TenantKeyManager::new(kms, "master".to_string());

        let key = manager.get_tenant_key("tenant1").await.unwrap();
        assert!(key.key_id.contains("tenant1"));

        // Should get cached key
        let key2 = manager.get_tenant_key("tenant1").await.unwrap();
        assert_eq!(key.key_id, key2.key_id);
    }
}
