//! Encryption at rest for Strata.
//!
//! Provides AES-256-GCM encryption for data stored on disk.

use crate::error::{Result, StrataError};
use ring::aead::{self, Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Encryption algorithm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (recommended).
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305.
    ChaCha20Poly1305,
    /// No encryption.
    None,
}


impl EncryptionAlgorithm {
    /// Get the key length for this algorithm.
    pub fn key_length(&self) -> usize {
        match self {
            EncryptionAlgorithm::Aes256Gcm => 32,
            EncryptionAlgorithm::ChaCha20Poly1305 => 32,
            EncryptionAlgorithm::None => 0,
        }
    }

    /// Get the nonce length for this algorithm.
    pub fn nonce_length(&self) -> usize {
        match self {
            EncryptionAlgorithm::Aes256Gcm => 12,
            EncryptionAlgorithm::ChaCha20Poly1305 => 12,
            EncryptionAlgorithm::None => 0,
        }
    }

    /// Get the tag length for this algorithm.
    pub fn tag_length(&self) -> usize {
        match self {
            EncryptionAlgorithm::Aes256Gcm => 16,
            EncryptionAlgorithm::ChaCha20Poly1305 => 16,
            EncryptionAlgorithm::None => 0,
        }
    }
}

/// Encryption configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Whether encryption is enabled.
    pub enabled: bool,
    /// Encryption algorithm.
    pub algorithm: EncryptionAlgorithm,
    /// Key rotation interval in seconds (0 = no rotation).
    pub key_rotation_interval: u64,
    /// Master key ID.
    pub master_key_id: Option<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_rotation_interval: 86400 * 30, // 30 days
            master_key_id: None,
        }
    }
}

impl EncryptionConfig {
    /// Enable encryption with defaults.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Disable encryption.
    pub fn disabled() -> Self {
        Self::default()
    }
}

/// Encryption key with metadata.
#[derive(Clone)]
pub struct EncryptionKey {
    /// Key ID.
    pub id: String,
    /// Raw key bytes.
    key_bytes: Vec<u8>,
    /// Algorithm this key is for.
    pub algorithm: EncryptionAlgorithm,
    /// When the key was created.
    pub created_at: u64,
    /// Whether this key is active for encryption.
    pub active: bool,
}

impl EncryptionKey {
    /// Generate a new random key.
    pub fn generate(id: impl Into<String>, algorithm: EncryptionAlgorithm) -> Result<Self> {
        let rng = SystemRandom::new();
        let mut key_bytes = vec![0u8; algorithm.key_length()];
        rng.fill(&mut key_bytes)
            .map_err(|_| StrataError::Internal("Failed to generate random key".to_string()))?;

        Ok(Self {
            id: id.into(),
            key_bytes,
            algorithm,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            active: true,
        })
    }

    /// Create from existing key bytes.
    pub fn from_bytes(
        id: impl Into<String>,
        bytes: Vec<u8>,
        algorithm: EncryptionAlgorithm,
    ) -> Result<Self> {
        if bytes.len() != algorithm.key_length() {
            return Err(StrataError::InvalidData(format!(
                "Key length {} doesn't match algorithm requirement {}",
                bytes.len(),
                algorithm.key_length()
            )));
        }

        Ok(Self {
            id: id.into(),
            key_bytes: bytes,
            algorithm,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            active: true,
        })
    }

    /// Get the key bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.key_bytes
    }
}

/// Nonce sequence for AEAD operations.
struct CounterNonceSequence {
    counter: u64,
    nonce_bytes: [u8; 12],
}

impl CounterNonceSequence {
    fn new(initial_nonce: &[u8]) -> Self {
        let mut nonce_bytes = [0u8; 12];
        let len = initial_nonce.len().min(12);
        nonce_bytes[..len].copy_from_slice(&initial_nonce[..len]);
        Self {
            counter: 0,
            nonce_bytes,
        }
    }
}

impl NonceSequence for CounterNonceSequence {
    fn advance(&mut self) -> std::result::Result<Nonce, ring::error::Unspecified> {
        // XOR counter into the last 8 bytes
        let counter_bytes = self.counter.to_be_bytes();
        for i in 0..8 {
            self.nonce_bytes[4 + i] ^= counter_bytes[i];
        }
        self.counter += 1;
        Nonce::try_assume_unique_for_key(&self.nonce_bytes)
    }
}

/// Encryptor for encrypting data.
pub struct Encryptor {
    key: Arc<EncryptionKey>,
    rng: SystemRandom,
}

impl Encryptor {
    /// Create a new encryptor with the given key.
    pub fn new(key: Arc<EncryptionKey>) -> Self {
        Self {
            key,
            rng: SystemRandom::new(),
        }
    }

    /// Encrypt data and return ciphertext with nonce prepended.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        self.encrypt_with_aad(plaintext, &[])
    }

    /// Encrypt data with additional authenticated data.
    pub fn encrypt_with_aad(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        match self.key.algorithm {
            EncryptionAlgorithm::None => Ok(plaintext.to_vec()),
            EncryptionAlgorithm::Aes256Gcm => self.encrypt_aes_gcm(plaintext, aad),
            EncryptionAlgorithm::ChaCha20Poly1305 => self.encrypt_chacha(plaintext, aad),
        }
    }

    fn encrypt_aes_gcm(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let algorithm = &aead::AES_256_GCM;

        // Generate random nonce
        let mut nonce = [0u8; 12];
        self.rng
            .fill(&mut nonce)
            .map_err(|_| StrataError::Internal("Failed to generate nonce".to_string()))?;

        let unbound_key = UnboundKey::new(algorithm, self.key.bytes())
            .map_err(|_| StrataError::Internal("Invalid encryption key".to_string()))?;

        let nonce_sequence = CounterNonceSequence::new(&nonce);
        let mut sealing_key = SealingKey::new(unbound_key, nonce_sequence);

        // Allocate buffer for ciphertext + tag
        let mut in_out = plaintext.to_vec();
        in_out.reserve(algorithm.tag_len());

        sealing_key
            .seal_in_place_append_tag(Aad::from(aad), &mut in_out)
            .map_err(|_| StrataError::Internal("Encryption failed".to_string()))?;

        // Prepend nonce to ciphertext
        let mut result = Vec::with_capacity(nonce.len() + in_out.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&in_out);

        Ok(result)
    }

    fn encrypt_chacha(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let algorithm = &aead::CHACHA20_POLY1305;

        // Generate random nonce
        let mut nonce = [0u8; 12];
        self.rng
            .fill(&mut nonce)
            .map_err(|_| StrataError::Internal("Failed to generate nonce".to_string()))?;

        let unbound_key = UnboundKey::new(algorithm, self.key.bytes())
            .map_err(|_| StrataError::Internal("Invalid encryption key".to_string()))?;

        let nonce_sequence = CounterNonceSequence::new(&nonce);
        let mut sealing_key = SealingKey::new(unbound_key, nonce_sequence);

        let mut in_out = plaintext.to_vec();
        in_out.reserve(algorithm.tag_len());

        sealing_key
            .seal_in_place_append_tag(Aad::from(aad), &mut in_out)
            .map_err(|_| StrataError::Internal("Encryption failed".to_string()))?;

        let mut result = Vec::with_capacity(nonce.len() + in_out.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&in_out);

        Ok(result)
    }

    /// Get the key ID used by this encryptor.
    pub fn key_id(&self) -> &str {
        &self.key.id
    }
}

/// Decryptor for decrypting data.
pub struct Decryptor {
    key: Arc<EncryptionKey>,
}

impl Decryptor {
    /// Create a new decryptor with the given key.
    pub fn new(key: Arc<EncryptionKey>) -> Self {
        Self { key }
    }

    /// Decrypt data (expects nonce prepended to ciphertext).
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.decrypt_with_aad(ciphertext, &[])
    }

    /// Decrypt data with additional authenticated data.
    pub fn decrypt_with_aad(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        match self.key.algorithm {
            EncryptionAlgorithm::None => Ok(ciphertext.to_vec()),
            EncryptionAlgorithm::Aes256Gcm => self.decrypt_aes_gcm(ciphertext, aad),
            EncryptionAlgorithm::ChaCha20Poly1305 => self.decrypt_chacha(ciphertext, aad),
        }
    }

    fn decrypt_aes_gcm(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let algorithm = &aead::AES_256_GCM;
        let nonce_len = 12;

        if ciphertext.len() < nonce_len + algorithm.tag_len() {
            return Err(StrataError::InvalidData(
                "Ciphertext too short (AES)".to_string(),
            ));
        }

        let (nonce, encrypted) = ciphertext.split_at(nonce_len);

        let unbound_key = UnboundKey::new(algorithm, self.key.bytes())
            .map_err(|_| StrataError::Internal("Invalid decryption key".to_string()))?;

        let nonce_sequence = CounterNonceSequence::new(nonce);
        let mut opening_key = OpeningKey::new(unbound_key, nonce_sequence);

        let mut in_out = encrypted.to_vec();

        let plaintext = opening_key
            .open_in_place(Aad::from(aad), &mut in_out)
            .map_err(|_| StrataError::Internal("Decryption failed".to_string()))?;

        Ok(plaintext.to_vec())
    }

    fn decrypt_chacha(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let algorithm = &aead::CHACHA20_POLY1305;
        let nonce_len = 12;

        if ciphertext.len() < nonce_len + algorithm.tag_len() {
            return Err(StrataError::InvalidData(
                "Ciphertext too short (ChaCha)".to_string(),
            ));
        }

        let (nonce, encrypted) = ciphertext.split_at(nonce_len);

        let unbound_key = UnboundKey::new(algorithm, self.key.bytes())
            .map_err(|_| StrataError::Internal("Invalid decryption key".to_string()))?;

        let nonce_sequence = CounterNonceSequence::new(nonce);
        let mut opening_key = OpeningKey::new(unbound_key, nonce_sequence);

        let mut in_out = encrypted.to_vec();

        let plaintext = opening_key
            .open_in_place(Aad::from(aad), &mut in_out)
            .map_err(|_| StrataError::Internal("Decryption failed".to_string()))?;

        Ok(plaintext.to_vec())
    }

    /// Get the key ID used by this decryptor.
    pub fn key_id(&self) -> &str {
        &self.key.id
    }
}

/// Key manager for managing encryption keys.
pub struct KeyManager {
    /// Configuration.
    config: EncryptionConfig,
    /// All keys indexed by ID.
    keys: RwLock<HashMap<String, Arc<EncryptionKey>>>,
    /// Currently active key for encryption.
    active_key: RwLock<Option<Arc<EncryptionKey>>>,
}

impl KeyManager {
    /// Create a new key manager.
    pub fn new(config: EncryptionConfig) -> Self {
        Self {
            config,
            keys: RwLock::new(HashMap::new()),
            active_key: RwLock::new(None),
        }
    }

    /// Initialize with a new random key.
    pub async fn initialize(&self) -> Result<String> {
        let key = EncryptionKey::generate("key-0", self.config.algorithm)?;
        let key_id = key.id.clone();
        let key = Arc::new(key);

        let mut keys = self.keys.write().await;
        keys.insert(key_id.clone(), Arc::clone(&key));

        let mut active = self.active_key.write().await;
        *active = Some(key);

        Ok(key_id)
    }

    /// Load a key from bytes.
    pub async fn load_key(&self, id: &str, bytes: Vec<u8>) -> Result<()> {
        let key = EncryptionKey::from_bytes(id, bytes, self.config.algorithm)?;
        let key = Arc::new(key);

        let mut keys = self.keys.write().await;
        keys.insert(id.to_string(), Arc::clone(&key));

        // If no active key, make this one active
        let mut active = self.active_key.write().await;
        if active.is_none() {
            *active = Some(key);
        }

        Ok(())
    }

    /// Set the active key for encryption.
    pub async fn set_active_key(&self, key_id: &str) -> Result<()> {
        let keys = self.keys.read().await;
        let key = keys
            .get(key_id)
            .ok_or_else(|| StrataError::NotFound(format!("Key not found: {}", key_id)))?;

        let mut active = self.active_key.write().await;
        *active = Some(Arc::clone(key));
        Ok(())
    }

    /// Get a key by ID.
    pub async fn get_key(&self, key_id: &str) -> Option<Arc<EncryptionKey>> {
        let keys = self.keys.read().await;
        keys.get(key_id).cloned()
    }

    /// Get the active key.
    pub async fn active_key(&self) -> Option<Arc<EncryptionKey>> {
        let active = self.active_key.read().await;
        active.clone()
    }

    /// Create an encryptor with the active key.
    pub async fn encryptor(&self) -> Result<Encryptor> {
        let key = self
            .active_key()
            .await
            .ok_or_else(|| StrataError::Internal("No active encryption key".to_string()))?;
        Ok(Encryptor::new(key))
    }

    /// Create a decryptor for a specific key.
    pub async fn decryptor(&self, key_id: &str) -> Result<Decryptor> {
        let key = self
            .get_key(key_id)
            .await
            .ok_or_else(|| StrataError::NotFound(format!("Key not found: {}", key_id)))?;
        Ok(Decryptor::new(key))
    }

    /// Rotate the active key (generate new key).
    pub async fn rotate_key(&self) -> Result<String> {
        let new_id = format!(
            "key-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let key = EncryptionKey::generate(&new_id, self.config.algorithm)?;
        let key = Arc::new(key);

        let mut keys = self.keys.write().await;
        keys.insert(new_id.clone(), Arc::clone(&key));

        let mut active = self.active_key.write().await;
        if let Some(old_key) = active.as_ref() {
            // Mark old key as inactive (it can still decrypt)
            let old_id = old_key.id.clone();
            if let Some(old) = keys.get_mut(&old_id) {
                if let Some(old_mut) = Arc::get_mut(old) {
                    old_mut.active = false;
                }
            }
        }
        *active = Some(key);

        Ok(new_id)
    }

    /// List all key IDs.
    pub async fn list_keys(&self) -> Vec<String> {
        let keys = self.keys.read().await;
        keys.keys().cloned().collect()
    }

    /// Is encryption enabled?
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Encrypted data wrapper with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    /// Key ID used for encryption.
    pub key_id: String,
    /// Algorithm used.
    pub algorithm: EncryptionAlgorithm,
    /// Encrypted bytes (nonce + ciphertext + tag).
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl EncryptedData {
    /// Encrypt data and wrap with metadata.
    pub fn encrypt(encryptor: &Encryptor, plaintext: &[u8]) -> Result<Self> {
        let data = encryptor.encrypt(plaintext)?;
        Ok(Self {
            key_id: encryptor.key_id().to_string(),
            algorithm: encryptor.key.algorithm,
            data,
        })
    }

    /// Decrypt the data.
    pub fn decrypt(&self, _key_manager: &KeyManager) -> Result<Vec<u8>> {
        // This is sync, but key_manager methods are async
        // In real use, caller should get the decryptor first
        Err(StrataError::Internal(
            "Use async decrypt_async instead".to_string(),
        ))
    }

    /// Decrypt the data asynchronously.
    pub async fn decrypt_async(&self, key_manager: &KeyManager) -> Result<Vec<u8>> {
        let decryptor = key_manager.decryptor(&self.key_id).await?;
        decryptor.decrypt(&self.data)
    }
}

/// File encryption helper for encrypting files at rest.
pub struct FileEncryption {
    key_manager: Arc<KeyManager>,
}

impl FileEncryption {
    /// Create a new file encryption helper.
    pub fn new(key_manager: Arc<KeyManager>) -> Self {
        Self { key_manager }
    }

    /// Encrypt a file.
    pub async fn encrypt_file(&self, input: &Path, output: &Path) -> Result<()> {
        let plaintext = tokio::fs::read(input).await?;

        let encryptor = self.key_manager.encryptor().await?;
        let ciphertext = encryptor.encrypt(&plaintext)?;

        tokio::fs::write(output, ciphertext).await?;

        Ok(())
    }

    /// Decrypt a file.
    pub async fn decrypt_file(&self, input: &Path, output: &Path, key_id: &str) -> Result<()> {
        let ciphertext = tokio::fs::read(input).await?;

        let decryptor = self.key_manager.decryptor(key_id).await?;
        let plaintext = decryptor.decrypt(&ciphertext)?;

        tokio::fs::write(output, plaintext).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_properties() {
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.key_length(), 32);
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.nonce_length(), 12);
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.tag_length(), 16);

        assert_eq!(EncryptionAlgorithm::ChaCha20Poly1305.key_length(), 32);
        assert_eq!(EncryptionAlgorithm::None.key_length(), 0);
    }

    #[test]
    fn test_key_generation() {
        let key = EncryptionKey::generate("test-key", EncryptionAlgorithm::Aes256Gcm).unwrap();
        assert_eq!(key.id, "test-key");
        assert_eq!(key.bytes().len(), 32);
        assert!(key.active);
    }

    #[test]
    fn test_key_from_bytes() {
        let bytes = vec![0u8; 32];
        let key =
            EncryptionKey::from_bytes("test", bytes.clone(), EncryptionAlgorithm::Aes256Gcm)
                .unwrap();
        assert_eq!(key.bytes(), &bytes[..]);
    }

    #[test]
    fn test_key_from_bytes_wrong_length() {
        let bytes = vec![0u8; 16]; // Wrong length
        let result = EncryptionKey::from_bytes("test", bytes, EncryptionAlgorithm::Aes256Gcm);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_decrypt_aes256gcm() {
        let key = Arc::new(
            EncryptionKey::generate("test", EncryptionAlgorithm::Aes256Gcm).unwrap(),
        );

        let encryptor = Encryptor::new(Arc::clone(&key));
        let decryptor = Decryptor::new(key);

        let plaintext = b"Hello, World!";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();

        // Ciphertext should be different from plaintext
        assert_ne!(&ciphertext[12..], plaintext);

        let decrypted = decryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_decrypt_chacha20() {
        let key = Arc::new(
            EncryptionKey::generate("test", EncryptionAlgorithm::ChaCha20Poly1305).unwrap(),
        );

        let encryptor = Encryptor::new(Arc::clone(&key));
        let decryptor = Decryptor::new(key);

        let plaintext = b"Hello, ChaCha!";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();

        let decrypted = decryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_decrypt_with_aad() {
        let key = Arc::new(
            EncryptionKey::generate("test", EncryptionAlgorithm::Aes256Gcm).unwrap(),
        );

        let encryptor = Encryptor::new(Arc::clone(&key));
        let decryptor = Decryptor::new(key);

        let plaintext = b"Sensitive data";
        let aad = b"file-id-12345";

        let ciphertext = encryptor.encrypt_with_aad(plaintext, aad).unwrap();
        let decrypted = decryptor.decrypt_with_aad(&ciphertext, aad).unwrap();
        assert_eq!(decrypted, plaintext);

        // Wrong AAD should fail
        let result = decryptor.decrypt_with_aad(&ciphertext, b"wrong-aad");
        assert!(result.is_err());
    }

    #[test]
    fn test_no_encryption() {
        let key = Arc::new(
            EncryptionKey::from_bytes("test", vec![], EncryptionAlgorithm::None).unwrap(),
        );

        let encryptor = Encryptor::new(Arc::clone(&key));
        let decryptor = Decryptor::new(key);

        let plaintext = b"Unencrypted data";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        assert_eq!(ciphertext, plaintext); // Should be unchanged

        let decrypted = decryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_ciphertexts() {
        let key = Arc::new(
            EncryptionKey::generate("test", EncryptionAlgorithm::Aes256Gcm).unwrap(),
        );

        let encryptor = Encryptor::new(key);
        let plaintext = b"Same plaintext";

        let ct1 = encryptor.encrypt(plaintext).unwrap();
        let ct2 = encryptor.encrypt(plaintext).unwrap();

        // Different encryptions should produce different ciphertexts (random nonce)
        assert_ne!(ct1, ct2);
    }

    #[tokio::test]
    async fn test_key_manager_initialize() {
        let config = EncryptionConfig::enabled();
        let manager = KeyManager::new(config);

        let key_id = manager.initialize().await.unwrap();
        assert!(!key_id.is_empty());

        let key = manager.active_key().await;
        assert!(key.is_some());
    }

    #[tokio::test]
    async fn test_key_manager_load_key() {
        let config = EncryptionConfig::enabled();
        let manager = KeyManager::new(config);

        let key_bytes = vec![1u8; 32];
        manager.load_key("external-key", key_bytes).await.unwrap();

        let key = manager.get_key("external-key").await;
        assert!(key.is_some());
    }

    #[tokio::test]
    async fn test_key_manager_rotate() {
        let config = EncryptionConfig::enabled();
        let manager = KeyManager::new(config);

        let initial_id = manager.initialize().await.unwrap();
        let new_id = manager.rotate_key().await.unwrap();

        assert_ne!(initial_id, new_id);

        // Both keys should exist
        assert!(manager.get_key(&initial_id).await.is_some());
        assert!(manager.get_key(&new_id).await.is_some());

        // New key should be active
        let active = manager.active_key().await.unwrap();
        assert_eq!(active.id, new_id);
    }

    #[tokio::test]
    async fn test_key_manager_encryptor_decryptor() {
        let config = EncryptionConfig::enabled();
        let manager = KeyManager::new(config);
        manager.initialize().await.unwrap();

        let encryptor = manager.encryptor().await.unwrap();
        let plaintext = b"Test message";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();

        let decryptor = manager.decryptor(encryptor.key_id()).await.unwrap();
        let decrypted = decryptor.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypted_data() {
        let config = EncryptionConfig::enabled();
        let manager = KeyManager::new(config);
        manager.initialize().await.unwrap();

        let encryptor = manager.encryptor().await.unwrap();
        let plaintext = b"Important data";

        let encrypted = EncryptedData::encrypt(&encryptor, plaintext).unwrap();
        assert!(!encrypted.data.is_empty());

        let decrypted = encrypted.decrypt_async(&manager).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_config_presets() {
        let enabled = EncryptionConfig::enabled();
        assert!(enabled.enabled);
        assert_eq!(enabled.algorithm, EncryptionAlgorithm::Aes256Gcm);

        let disabled = EncryptionConfig::disabled();
        assert!(!disabled.enabled);
    }
}
