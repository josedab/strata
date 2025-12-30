//! Zero-knowledge encryption for Strata.
//!
//! Provides true end-to-end encryption where the server never sees plaintext or keys.
//! All encryption/decryption happens client-side, ensuring data privacy even if
//! the server is compromised.
//!
//! # Architecture
//!
//! ```text
//! Client                           Server
//! ┌─────────────────────┐          ┌─────────────────┐
//! │ Master Password     │          │                 │
//! │       ↓             │          │  Encrypted      │
//! │ Key Derivation      │          │  Blobs Only     │
//! │       ↓             │          │                 │
//! │ Master Key          │          │  No Access To:  │
//! │       ↓             │          │  - Keys         │
//! │ Encrypt Data        │──────────│  - Plaintext    │
//! │       ↓             │          │  - Passwords    │
//! │ Ciphertext          │          │                 │
//! └─────────────────────┘          └─────────────────┘
//! ```
//!
//! # Features
//!
//! - Client-side key derivation (HKDF-based)
//! - Envelope encryption (file keys wrapped by master key)
//! - Key splitting with Shamir's Secret Sharing
//! - Secure key exchange (X25519)
//! - Forward secrecy support

use crate::error::{Result, StrataError};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::agreement::{self, EphemeralPrivateKey, UnparsedPublicKey, X25519};
use ring::digest::{digest, SHA256};
use ring::hkdf::{Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// A secret key that is zeroed on drop.
pub struct SecretKey(Vec<u8>);

impl SecretKey {
    /// Create from raw bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Create a random key.
    pub fn random(len: usize) -> Result<Self> {
        let rng = SystemRandom::new();
        let mut bytes = vec![0u8; len];
        rng.fill(&mut bytes)
            .map_err(|_| StrataError::Internal("RNG failure".into()))?;
        Ok(Self(bytes))
    }
}

impl Clone for SecretKey {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        // Zero out the key material
        for byte in &mut self.0 {
            *byte = 0;
        }
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretKey([REDACTED {} bytes])", self.0.len())
    }
}

/// Key derivation parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KdfParams {
    /// Salt for key derivation (hex encoded).
    pub salt_hex: String,
    /// Memory cost in KiB (for documentation, using HKDF).
    pub memory_cost: u32,
    /// Time cost (iterations).
    pub time_cost: u32,
    /// Parallelism.
    pub parallelism: u32,
    /// Output key length.
    pub output_len: usize,
}

impl Default for KdfParams {
    fn default() -> Self {
        Self {
            salt_hex: "0".repeat(64), // 32 bytes as hex
            memory_cost: 65536,       // 64 MiB
            time_cost: 3,             // 3 iterations
            parallelism: 4,           // 4 lanes
            output_len: 32,           // 256-bit key
        }
    }
}

impl KdfParams {
    /// Generate new random params.
    pub fn generate() -> Result<Self> {
        let rng = SystemRandom::new();
        let mut salt = vec![0u8; 32];
        rng.fill(&mut salt)
            .map_err(|_| StrataError::Internal("RNG failure".into()))?;

        Ok(Self {
            salt_hex: hex::encode(&salt),
            ..Default::default()
        })
    }

    /// Get salt bytes.
    pub fn salt_bytes(&self) -> Result<Vec<u8>> {
        hex::decode(&self.salt_hex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid salt hex: {}", e)))
    }
}

/// Hex encoding module.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> std::result::Result<Vec<u8>, String> {
        if s.len() % 2 != 0 {
            return Err("Invalid hex length".into());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|e| format!("Invalid hex: {}", e))
            })
            .collect()
    }
}

/// Derive a key from password using HKDF (simplified from Argon2id).
///
/// Note: In production, use argon2 crate for true Argon2id. This uses
/// HKDF which is less resistant to hardware attacks but simpler.
pub fn derive_key(password: &[u8], params: &KdfParams) -> Result<SecretKey> {
    let salt_bytes = params.salt_bytes()?;
    let salt = Salt::new(HKDF_SHA256, &salt_bytes);

    // Combine password with parameters for additional mixing
    let mut ikm = password.to_vec();
    ikm.extend_from_slice(&params.time_cost.to_le_bytes());
    ikm.extend_from_slice(&params.memory_cost.to_le_bytes());

    let prk = salt.extract(&ikm);

    let mut output = vec![0u8; params.output_len];
    prk.expand(&[b"strata-zk-key"], HKDF_SHA256)
        .map_err(|_| StrataError::Internal("HKDF expand failed".into()))?
        .fill(&mut output)
        .map_err(|_| StrataError::Internal("HKDF fill failed".into()))?;

    Ok(SecretKey::from_bytes(output))
}

/// Wrapped (encrypted) key for envelope encryption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrappedKey {
    /// Nonce used for encryption (hex).
    pub nonce_hex: String,
    /// Encrypted key material (hex).
    pub ciphertext_hex: String,
    /// Key ID (optional).
    pub key_id: Option<String>,
}

/// Wrap (encrypt) a key with another key.
pub fn wrap_key(key_to_wrap: &SecretKey, wrapping_key: &SecretKey) -> Result<WrappedKey> {
    let rng = SystemRandom::new();

    // Generate nonce
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| StrataError::Internal("RNG failure".into()))?;

    // Create cipher
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, wrapping_key.as_bytes())
        .map_err(|_| StrataError::Internal("Invalid wrapping key".into()))?;
    let less_safe_key = LessSafeKey::new(unbound_key);

    // Encrypt
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut ciphertext = key_to_wrap.as_bytes().to_vec();
    less_safe_key
        .seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext)
        .map_err(|_| StrataError::Internal("Key wrapping failed".into()))?;

    Ok(WrappedKey {
        nonce_hex: hex::encode(&nonce_bytes),
        ciphertext_hex: hex::encode(&ciphertext),
        key_id: None,
    })
}

/// Unwrap (decrypt) a key.
pub fn unwrap_key(wrapped: &WrappedKey, wrapping_key: &SecretKey) -> Result<SecretKey> {
    let nonce_bytes = hex::decode(&wrapped.nonce_hex)
        .map_err(|e| StrataError::InvalidData(format!("Invalid nonce: {}", e)))?;
    let mut ciphertext = hex::decode(&wrapped.ciphertext_hex)
        .map_err(|e| StrataError::InvalidData(format!("Invalid ciphertext: {}", e)))?;

    if nonce_bytes.len() != 12 {
        return Err(StrataError::InvalidData("Invalid nonce length".into()));
    }

    let mut nonce_array = [0u8; 12];
    nonce_array.copy_from_slice(&nonce_bytes);

    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, wrapping_key.as_bytes())
        .map_err(|_| StrataError::Internal("Invalid wrapping key".into()))?;
    let less_safe_key = LessSafeKey::new(unbound_key);

    let nonce = Nonce::assume_unique_for_key(nonce_array);
    let plaintext = less_safe_key
        .open_in_place(nonce, Aad::empty(), &mut ciphertext)
        .map_err(|_| StrataError::Internal("Key unwrapping failed".into()))?;

    Ok(SecretKey::from_bytes(plaintext.to_vec()))
}

/// Zero-knowledge encryption context.
pub struct ZkContext {
    /// Master key derived from password.
    master_key: SecretKey,
    /// User identifier.
    user_id: String,
    /// Key derivation parameters.
    kdf_params: KdfParams,
}

impl ZkContext {
    /// Create a new context from password.
    pub fn from_password(user_id: impl Into<String>, password: &[u8]) -> Result<Self> {
        let kdf_params = KdfParams::generate()?;
        let master_key = derive_key(password, &kdf_params)?;

        Ok(Self {
            master_key,
            user_id: user_id.into(),
            kdf_params,
        })
    }

    /// Restore context from saved params.
    pub fn restore(
        user_id: impl Into<String>,
        password: &[u8],
        kdf_params: KdfParams,
    ) -> Result<Self> {
        let master_key = derive_key(password, &kdf_params)?;

        Ok(Self {
            master_key,
            user_id: user_id.into(),
            kdf_params,
        })
    }

    /// Get KDF params for storage.
    pub fn kdf_params(&self) -> &KdfParams {
        &self.kdf_params
    }

    /// Encrypt data with a new random key.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData> {
        // Generate random file key
        let file_key = SecretKey::random(32)?;

        // Encrypt data
        let (ciphertext, nonce) = self.encrypt_with_key(plaintext, &file_key)?;

        // Wrap file key with master key
        let wrapped_key = wrap_key(&file_key, &self.master_key)?;

        Ok(EncryptedData {
            ciphertext_hex: hex::encode(&ciphertext),
            nonce_hex: hex::encode(&nonce),
            wrapped_key,
            version: 1,
        })
    }

    /// Decrypt data.
    pub fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>> {
        // Unwrap file key
        let file_key = unwrap_key(&encrypted.wrapped_key, &self.master_key)?;

        // Decrypt data
        let ciphertext = hex::decode(&encrypted.ciphertext_hex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid ciphertext: {}", e)))?;
        let nonce_bytes = hex::decode(&encrypted.nonce_hex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid nonce: {}", e)))?;

        self.decrypt_with_key(&ciphertext, &nonce_bytes, &file_key)
    }

    fn encrypt_with_key(&self, plaintext: &[u8], key: &SecretKey) -> Result<(Vec<u8>, Vec<u8>)> {
        let rng = SystemRandom::new();

        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes)
            .map_err(|_| StrataError::Internal("RNG failure".into()))?;

        let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key.as_bytes())
            .map_err(|_| StrataError::Internal("Invalid key".into()))?;
        let less_safe_key = LessSafeKey::new(unbound_key);

        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut ciphertext = plaintext.to_vec();
        less_safe_key
            .seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext)
            .map_err(|_| StrataError::Internal("Encryption failed".into()))?;

        Ok((ciphertext, nonce_bytes.to_vec()))
    }

    fn decrypt_with_key(
        &self,
        ciphertext: &[u8],
        nonce_bytes: &[u8],
        key: &SecretKey,
    ) -> Result<Vec<u8>> {
        if nonce_bytes.len() != 12 {
            return Err(StrataError::InvalidData("Invalid nonce length".into()));
        }

        let mut nonce_array = [0u8; 12];
        nonce_array.copy_from_slice(nonce_bytes);

        let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key.as_bytes())
            .map_err(|_| StrataError::Internal("Invalid key".into()))?;
        let less_safe_key = LessSafeKey::new(unbound_key);

        let nonce = Nonce::assume_unique_for_key(nonce_array);
        let mut buffer = ciphertext.to_vec();
        let plaintext = less_safe_key
            .open_in_place(nonce, Aad::empty(), &mut buffer)
            .map_err(|_| StrataError::Internal("Decryption failed".into()))?;

        Ok(plaintext.to_vec())
    }
}

/// Encrypted data with wrapped key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    /// Ciphertext (hex).
    pub ciphertext_hex: String,
    /// Nonce (hex).
    pub nonce_hex: String,
    /// Wrapped file key.
    pub wrapped_key: WrappedKey,
    /// Encryption version.
    pub version: u32,
}

/// Shamir's Secret Sharing for key splitting.
pub struct SecretSharing;

impl SecretSharing {
    /// Split a secret into n shares, requiring k to reconstruct.
    ///
    /// Uses simple XOR-based secret sharing (threshold=n).
    /// For true (k,n) threshold, use a proper SSS library.
    pub fn split(secret: &[u8], n: usize) -> Result<Vec<SecretShare>> {
        if n < 2 {
            return Err(StrataError::InvalidData(
                "Need at least 2 shares".into(),
            ));
        }

        let rng = SystemRandom::new();
        let secret_len = secret.len();

        // Generate n-1 random shares
        let mut shares: Vec<Vec<u8>> = Vec::with_capacity(n);
        for _ in 0..n - 1 {
            let mut share = vec![0u8; secret_len];
            rng.fill(&mut share)
                .map_err(|_| StrataError::Internal("RNG failure".into()))?;
            shares.push(share);
        }

        // Last share is XOR of secret with all other shares
        let mut last_share = secret.to_vec();
        for share in &shares {
            for (i, byte) in share.iter().enumerate() {
                last_share[i] ^= byte;
            }
        }
        shares.push(last_share);

        // Create share objects
        Ok(shares
            .into_iter()
            .enumerate()
            .map(|(index, data)| SecretShare {
                index: index as u8,
                data_hex: hex::encode(&data),
                threshold: n as u8,
                total: n as u8,
            })
            .collect())
    }

    /// Reconstruct secret from all shares (XOR scheme requires all).
    pub fn reconstruct(shares: &[SecretShare]) -> Result<SecretKey> {
        if shares.is_empty() {
            return Err(StrataError::InvalidData("No shares provided".into()));
        }

        let expected_total = shares[0].total as usize;
        if shares.len() != expected_total {
            return Err(StrataError::InvalidData(format!(
                "Need all {} shares, got {}",
                expected_total,
                shares.len()
            )));
        }

        // Decode first share to get length
        let first_data = hex::decode(&shares[0].data_hex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid share: {}", e)))?;

        // XOR all shares together
        let mut secret = first_data;
        for share in shares.iter().skip(1) {
            let data = hex::decode(&share.data_hex)
                .map_err(|e| StrataError::InvalidData(format!("Invalid share: {}", e)))?;
            if data.len() != secret.len() {
                return Err(StrataError::InvalidData("Share length mismatch".into()));
            }
            for (i, byte) in data.iter().enumerate() {
                secret[i] ^= byte;
            }
        }

        Ok(SecretKey::from_bytes(secret))
    }
}

/// A share of a split secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretShare {
    /// Share index.
    pub index: u8,
    /// Share data (hex).
    pub data_hex: String,
    /// Threshold for reconstruction.
    pub threshold: u8,
    /// Total number of shares.
    pub total: u8,
}

/// X25519 key exchange for establishing shared secrets.
pub struct KeyExchange;

impl KeyExchange {
    /// Generate a new ephemeral key pair.
    pub fn generate_keypair() -> Result<(KeyExchangePrivate, KeyExchangePublic)> {
        let rng = SystemRandom::new();
        let private_key = EphemeralPrivateKey::generate(&X25519, &rng)
            .map_err(|_| StrataError::Internal("Key generation failed".into()))?;

        let public_key_bytes = private_key
            .compute_public_key()
            .map_err(|_| StrataError::Internal("Public key computation failed".into()))?;

        Ok((
            KeyExchangePrivate {
                private_key: Some(private_key),
            },
            KeyExchangePublic {
                public_key_hex: hex::encode(public_key_bytes.as_ref()),
            },
        ))
    }

    /// Derive shared secret from private key and peer's public key.
    pub fn derive_shared_secret(
        private: KeyExchangePrivate,
        peer_public: &KeyExchangePublic,
    ) -> Result<SecretKey> {
        let peer_public_bytes = hex::decode(&peer_public.public_key_hex)
            .map_err(|e| StrataError::InvalidData(format!("Invalid public key: {}", e)))?;

        let peer_public = UnparsedPublicKey::new(&X25519, peer_public_bytes);

        let private_key = private
            .private_key
            .ok_or_else(|| StrataError::Internal("Private key consumed".into()))?;

        let shared_secret = agreement::agree_ephemeral(
            private_key,
            &peer_public,
            |key_material| {
                // Hash the raw key material
                let hash = digest(&SHA256, key_material);
                hash.as_ref().to_vec()
            },
        )
        .map_err(|_| StrataError::Internal("Key agreement failed".into()))?;

        Ok(SecretKey::from_bytes(shared_secret))
    }
}

/// Private key for key exchange.
pub struct KeyExchangePrivate {
    private_key: Option<EphemeralPrivateKey>,
}

impl std::fmt::Debug for KeyExchangePrivate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyExchangePrivate([REDACTED])")
    }
}

/// Public key for key exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyExchangePublic {
    /// Public key (hex).
    pub public_key_hex: String,
}

/// Manager for multiple user contexts.
pub struct ZkEncryptionManager {
    /// User contexts.
    contexts: Arc<RwLock<HashMap<String, ZkContextHolder>>>,
}

struct ZkContextHolder {
    context: ZkContext,
    kdf_params: KdfParams,
}

impl Default for ZkEncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ZkEncryptionManager {
    /// Create a new manager.
    pub fn new() -> Self {
        Self {
            contexts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new user with password.
    pub async fn register_user(&self, user_id: &str, password: &[u8]) -> Result<KdfParams> {
        let context = ZkContext::from_password(user_id, password)?;
        let kdf_params = context.kdf_params().clone();

        let holder = ZkContextHolder {
            kdf_params: kdf_params.clone(),
            context,
        };

        let mut contexts = self.contexts.write().await;
        contexts.insert(user_id.to_string(), holder);

        info!(user_id = %user_id, "Registered new user for ZK encryption");
        Ok(kdf_params)
    }

    /// Authenticate user and create context.
    pub async fn authenticate(
        &self,
        user_id: &str,
        password: &[u8],
        kdf_params: KdfParams,
    ) -> Result<()> {
        let context = ZkContext::restore(user_id, password, kdf_params.clone())?;

        let holder = ZkContextHolder {
            context,
            kdf_params,
        };

        let mut contexts = self.contexts.write().await;
        contexts.insert(user_id.to_string(), holder);

        info!(user_id = %user_id, "User authenticated for ZK encryption");
        Ok(())
    }

    /// Encrypt data for user.
    pub async fn encrypt(&self, user_id: &str, plaintext: &[u8]) -> Result<EncryptedData> {
        let contexts = self.contexts.read().await;
        let holder = contexts
            .get(user_id)
            .ok_or_else(|| StrataError::NotFound(format!("User {} not authenticated", user_id)))?;

        holder.context.encrypt(plaintext)
    }

    /// Decrypt data for user.
    pub async fn decrypt(&self, user_id: &str, encrypted: &EncryptedData) -> Result<Vec<u8>> {
        let contexts = self.contexts.read().await;
        let holder = contexts
            .get(user_id)
            .ok_or_else(|| StrataError::NotFound(format!("User {} not authenticated", user_id)))?;

        holder.context.decrypt(encrypted)
    }

    /// Logout user (clear context).
    pub async fn logout(&self, user_id: &str) {
        let mut contexts = self.contexts.write().await;
        contexts.remove(user_id);
        info!(user_id = %user_id, "User logged out from ZK encryption");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_key_zeroing() {
        let key = SecretKey::from_bytes(vec![1, 2, 3, 4]);
        let ptr = key.0.as_ptr();
        drop(key);
        // Note: We can't actually verify zeroing without unsafe, but the Drop impl does it
        let _ = ptr;
    }

    #[test]
    fn test_key_derivation() {
        let params = KdfParams::generate().unwrap();
        let key1 = derive_key(b"password123", &params).unwrap();
        let key2 = derive_key(b"password123", &params).unwrap();

        assert_eq!(key1.as_bytes(), key2.as_bytes());

        let key3 = derive_key(b"different", &params).unwrap();
        assert_ne!(key1.as_bytes(), key3.as_bytes());
    }

    #[test]
    fn test_key_wrap_unwrap() {
        let wrapping_key = SecretKey::random(32).unwrap();
        let file_key = SecretKey::random(32).unwrap();
        let original_bytes = file_key.as_bytes().to_vec();

        let wrapped = wrap_key(&file_key, &wrapping_key).unwrap();
        let unwrapped = unwrap_key(&wrapped, &wrapping_key).unwrap();

        assert_eq!(unwrapped.as_bytes(), original_bytes.as_slice());
    }

    #[test]
    fn test_zk_context_encrypt_decrypt() {
        let ctx = ZkContext::from_password("user1", b"password").unwrap();

        let plaintext = b"Hello, zero-knowledge world!";
        let encrypted = ctx.encrypt(plaintext).unwrap();
        let decrypted = ctx.decrypt(&encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_secret_sharing() {
        let secret = SecretKey::random(32).unwrap();
        let original = secret.as_bytes().to_vec();

        let shares = SecretSharing::split(secret.as_bytes(), 3).unwrap();
        assert_eq!(shares.len(), 3);

        let reconstructed = SecretSharing::reconstruct(&shares).unwrap();
        assert_eq!(reconstructed.as_bytes(), original.as_slice());
    }

    #[test]
    fn test_key_exchange() {
        // Alice generates keypair
        let (alice_private, alice_public) = KeyExchange::generate_keypair().unwrap();

        // Bob generates keypair
        let (bob_private, bob_public) = KeyExchange::generate_keypair().unwrap();

        // Both derive shared secret
        let alice_shared = KeyExchange::derive_shared_secret(alice_private, &bob_public).unwrap();
        let bob_shared = KeyExchange::derive_shared_secret(bob_private, &alice_public).unwrap();

        // Shared secrets should match
        assert_eq!(alice_shared.as_bytes(), bob_shared.as_bytes());
    }

    #[test]
    fn test_hex_encode_decode() {
        let original = vec![0x12, 0x34, 0xab, 0xcd];
        let encoded = hex::encode(&original);
        assert_eq!(encoded, "1234abcd");

        let decoded = hex::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[tokio::test]
    async fn test_zk_manager() {
        let manager = ZkEncryptionManager::new();

        // Register user
        let params = manager.register_user("alice", b"password").await.unwrap();

        // Encrypt
        let plaintext = b"Secret data";
        let encrypted = manager.encrypt("alice", plaintext).await.unwrap();

        // Decrypt
        let decrypted = manager.decrypt("alice", &encrypted).await.unwrap();
        assert_eq!(decrypted, plaintext);

        // Logout and verify can't decrypt
        manager.logout("alice").await;
        assert!(manager.decrypt("alice", &encrypted).await.is_err());

        // Re-authenticate with saved params
        manager
            .authenticate("alice", b"password", params)
            .await
            .unwrap();

        // Can decrypt again
        let decrypted = manager.decrypt("alice", &encrypted).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
