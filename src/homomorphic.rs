//! Homomorphic Encryption
//!
//! This module provides homomorphic encryption capabilities for performing
//! computations on encrypted data without decryption. Features:
//! - Partial homomorphic encryption for specific operations
//! - Searchable encryption for encrypted queries
//! - Secure computation on encrypted metadata
//! - Privacy-preserving analytics
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │               Homomorphic Encryption Engine                  │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Schemes: Paillier │ ElGamal │ BGV/BFV Lite                 │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Operations: Add │ Multiply │ Compare │ Search              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Key Management │ Noise Budgeting │ Batching                │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! Note: This is a simplified educational implementation. For production use,
//! consider using established libraries like Microsoft SEAL or OpenFHE.

use crate::error::{Result, StrataError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Primes for modular arithmetic (sized to avoid u128 overflow)
/// Note: Using small primes so n^2 * n^2 fits in u128 without overflow.
/// In production, you would use a big integer library (like num-bigint).
const PRIME_P: u128 = 251; // small prime
const PRIME_Q: u128 = 241; // small prime
const MODULUS_N: u128 = PRIME_P * PRIME_Q; // 60491, n^2 = ~3.6 billion

/// Homomorphic encryption scheme types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionScheme {
    /// Additive homomorphic (supports addition on ciphertexts)
    Paillier,
    /// Multiplicative homomorphic (supports multiplication)
    ElGamal,
    /// Somewhat homomorphic (limited additions and multiplications)
    BFVLite,
    /// Order-preserving (supports comparisons)
    OrderPreserving,
    /// Searchable encryption
    Searchable,
}

/// Configuration for homomorphic encryption
#[derive(Debug, Clone)]
pub struct HomomorphicConfig {
    pub scheme: EncryptionScheme,
    pub security_bits: u32,
    pub noise_budget: u32,
    pub batch_size: usize,
}

impl Default for HomomorphicConfig {
    fn default() -> Self {
        Self {
            scheme: EncryptionScheme::Paillier,
            security_bits: 128,
            noise_budget: 100,
            batch_size: 4096,
        }
    }
}

/// Public key for homomorphic encryption
#[derive(Debug, Clone)]
pub struct PublicKey {
    pub scheme: EncryptionScheme,
    pub n: u128,           // Modulus
    pub g: u128,           // Generator
    pub key_id: KeyId,
}

/// Secret key for homomorphic encryption
#[derive(Clone)]
pub struct SecretKey {
    pub scheme: EncryptionScheme,
    pub lambda: u128,      // Lambda = lcm(p-1, q-1) for Paillier
    pub mu: u128,          // Mu for decryption
    pub key_id: KeyId,
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretKey")
            .field("scheme", &self.scheme)
            .field("key_id", &self.key_id)
            .field("lambda", &"[REDACTED]")
            .finish()
    }
}

/// Unique key identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId(pub u64);

impl KeyId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for KeyId {
    fn default() -> Self {
        Self::new()
    }
}

/// Encrypted value (ciphertext)
#[derive(Debug, Clone)]
pub struct Ciphertext {
    pub scheme: EncryptionScheme,
    pub value: u128,
    pub key_id: KeyId,
    pub noise_level: u32,
}

impl Ciphertext {
    /// Check if we can still perform operations
    pub fn can_operate(&self, budget: u32) -> bool {
        self.noise_level < budget
    }
}

/// Key pair generation
pub fn generate_keypair(config: &HomomorphicConfig) -> (PublicKey, SecretKey) {
    let key_id = KeyId::new();

    match config.scheme {
        EncryptionScheme::Paillier => {
            // Simplified Paillier key generation
            let n = MODULUS_N;
            let _n_squared = n.wrapping_mul(n);
            let g = n + 1; // Simple generator for Paillier

            // lambda = lcm(p-1, q-1)
            let lambda = lcm(PRIME_P - 1, PRIME_Q - 1);

            // For g = n+1, L(g^lambda mod n^2) = lambda
            // mu = lambda^(-1) mod n
            let mu = mod_inverse(lambda, n).expect("lambda should be invertible mod n");

            let public = PublicKey {
                scheme: EncryptionScheme::Paillier,
                n,
                g,
                key_id,
            };

            let secret = SecretKey {
                scheme: EncryptionScheme::Paillier,
                lambda,
                mu,
                key_id,
            };

            (public, secret)
        }
        _ => {
            // Default to Paillier for other schemes in this simplified impl
            let n = MODULUS_N;
            let g = n + 1;
            let lambda = lcm(PRIME_P - 1, PRIME_Q - 1);
            let mu = mod_inverse(lambda, n).expect("lambda should be invertible mod n");

            (
                PublicKey { scheme: config.scheme, n, g, key_id },
                SecretKey { scheme: config.scheme, lambda, mu, key_id },
            )
        }
    }
}

/// Greatest common divisor
fn gcd(a: u128, b: u128) -> u128 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// Least common multiple
fn lcm(a: u128, b: u128) -> u128 {
    (a / gcd(a, b)) * b
}

/// Modular exponentiation
fn mod_pow(base: u128, exp: u128, modulus: u128) -> u128 {
    if modulus == 1 {
        return 0;
    }

    let mut result: u128 = 1;
    let mut base = base % modulus;
    let mut exp = exp;

    while exp > 0 {
        if exp % 2 == 1 {
            result = (result * base) % modulus;
        }
        exp /= 2;
        base = (base * base) % modulus;
    }

    result
}

/// Extended Euclidean algorithm to find modular inverse
fn mod_inverse(a: u128, m: u128) -> Option<u128> {
    let (mut old_r, mut r) = (a as i128, m as i128);
    let (mut old_s, mut s) = (1i128, 0i128);

    while r != 0 {
        let quotient = old_r / r;
        (old_r, r) = (r, old_r - quotient * r);
        (old_s, s) = (s, old_s - quotient * s);
    }

    if old_r != 1 {
        return None; // No inverse exists
    }

    // Make sure result is positive
    let result = if old_s < 0 {
        (old_s + m as i128) as u128
    } else {
        old_s as u128
    };

    Some(result)
}

/// Paillier encryption context
pub struct PaillierContext {
    pub public_key: PublicKey,
    secret_key: Option<SecretKey>,
    n_squared: u128,
}

impl PaillierContext {
    pub fn new(public_key: PublicKey, secret_key: Option<SecretKey>) -> Self {
        let n_squared = public_key.n.wrapping_mul(public_key.n);
        Self {
            public_key,
            secret_key,
            n_squared,
        }
    }

    pub fn from_keypair(public: PublicKey, secret: SecretKey) -> Self {
        let n_squared = public.n.wrapping_mul(public.n);
        Self {
            public_key: public,
            secret_key: Some(secret),
            n_squared,
        }
    }

    /// Encrypt a plaintext value
    pub fn encrypt(&self, plaintext: u64) -> Ciphertext {
        let m = plaintext as u128;
        let n = self.public_key.n;

        // For simplicity, we use a deterministic encryption
        // Real Paillier uses random r: c = g^m * r^n mod n^2
        let g_m = mod_pow(self.public_key.g, m, self.n_squared);

        // Using a pseudo-random component
        let r: u128 = rand::random::<u64>() as u128 % n;
        let r_n = mod_pow(r, n, self.n_squared);

        let ciphertext = (g_m * r_n) % self.n_squared;

        Ciphertext {
            scheme: EncryptionScheme::Paillier,
            value: ciphertext,
            key_id: self.public_key.key_id,
            noise_level: 0,
        }
    }

    /// Decrypt a ciphertext
    pub fn decrypt(&self, ciphertext: &Ciphertext) -> Result<u64> {
        let secret = self.secret_key.as_ref()
            .ok_or_else(|| StrataError::InvalidData("No secret key available".into()))?;

        if ciphertext.key_id != self.public_key.key_id {
            return Err(StrataError::InvalidData("Key mismatch".into()));
        }

        let n = self.public_key.n;

        // L(c^lambda mod n^2) * mu mod n
        let c_lambda = mod_pow(ciphertext.value, secret.lambda, self.n_squared);

        // L(x) = (x - 1) / n
        let l_value = (c_lambda - 1) / n;

        // Simplified decryption
        let plaintext = (l_value * secret.mu) % n;

        Ok(plaintext as u64)
    }

    /// Add two ciphertexts (homomorphic addition)
    pub fn add(&self, a: &Ciphertext, b: &Ciphertext) -> Result<Ciphertext> {
        if a.key_id != b.key_id || a.key_id != self.public_key.key_id {
            return Err(StrataError::InvalidData("Key mismatch".into()));
        }

        // E(a) * E(b) mod n^2 = E(a + b)
        let result = (a.value * b.value) % self.n_squared;

        Ok(Ciphertext {
            scheme: EncryptionScheme::Paillier,
            value: result,
            key_id: self.public_key.key_id,
            noise_level: a.noise_level.max(b.noise_level) + 1,
        })
    }

    /// Add a plaintext to a ciphertext
    pub fn add_plain(&self, encrypted: &Ciphertext, plaintext: u64) -> Result<Ciphertext> {
        if encrypted.key_id != self.public_key.key_id {
            return Err(StrataError::InvalidData("Key mismatch".into()));
        }

        // E(a) * g^b mod n^2 = E(a + b)
        let g_m = mod_pow(self.public_key.g, plaintext as u128, self.n_squared);
        let result = (encrypted.value * g_m) % self.n_squared;

        Ok(Ciphertext {
            scheme: EncryptionScheme::Paillier,
            value: result,
            key_id: self.public_key.key_id,
            noise_level: encrypted.noise_level + 1,
        })
    }

    /// Multiply ciphertext by a plaintext (scalar multiplication)
    pub fn multiply_plain(&self, encrypted: &Ciphertext, scalar: u64) -> Result<Ciphertext> {
        if encrypted.key_id != self.public_key.key_id {
            return Err(StrataError::InvalidData("Key mismatch".into()));
        }

        // E(a)^k mod n^2 = E(a * k)
        let result = mod_pow(encrypted.value, scalar as u128, self.n_squared);

        Ok(Ciphertext {
            scheme: EncryptionScheme::Paillier,
            value: result,
            key_id: self.public_key.key_id,
            noise_level: encrypted.noise_level + 2,
        })
    }
}

/// Order-preserving encryption for range queries
pub struct OrderPreservingContext {
    key: [u8; 32],
    key_id: KeyId,
}

impl OrderPreservingContext {
    pub fn new() -> Self {
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = rand::random();
        }
        Self {
            key,
            key_id: KeyId::new(),
        }
    }

    /// Encrypt preserving order (simplified)
    pub fn encrypt(&self, value: u64) -> u64 {
        // Simple order-preserving transformation using addition only
        // Real OPE uses more complex algorithms like mOPE or BCLO
        let key_sum: u64 = self.key.iter().map(|&b| b as u64).sum();
        value.wrapping_add(key_sum)
    }

    /// Decrypt
    pub fn decrypt(&self, ciphertext: u64) -> u64 {
        let key_sum: u64 = self.key.iter().map(|&b| b as u64).sum();
        ciphertext.wrapping_sub(key_sum)
    }

    /// Compare two encrypted values
    pub fn compare(&self, a: u64, b: u64) -> std::cmp::Ordering {
        // Since order is preserved, direct comparison works
        a.cmp(&b)
    }
}

impl Default for OrderPreservingContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Searchable encryption for encrypted keyword search
pub struct SearchableContext {
    key: [u8; 32],
    key_id: KeyId,
}

impl SearchableContext {
    pub fn new() -> Self {
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = rand::random();
        }
        Self {
            key,
            key_id: KeyId::new(),
        }
    }

    /// Create a searchable token for a keyword
    pub fn create_token(&self, keyword: &str) -> SearchToken {
        let hash = self.hash_keyword(keyword);
        SearchToken {
            hash,
            key_id: self.key_id,
        }
    }

    /// Encrypt a document with searchable tokens
    pub fn encrypt_document(&self, keywords: &[&str]) -> EncryptedDocument {
        let tokens: Vec<_> = keywords.iter()
            .map(|k| self.create_token(k))
            .collect();

        EncryptedDocument {
            tokens,
            key_id: self.key_id,
        }
    }

    /// Search for a keyword in encrypted documents
    pub fn search(&self, token: &SearchToken, documents: &[EncryptedDocument]) -> Vec<usize> {
        documents.iter()
            .enumerate()
            .filter(|(_, doc)| {
                doc.key_id == token.key_id &&
                doc.tokens.iter().any(|t| t.hash == token.hash)
            })
            .map(|(i, _)| i)
            .collect()
    }

    fn hash_keyword(&self, keyword: &str) -> u64 {
        // Simple keyed hash (in practice, use HMAC or similar)
        let mut hash: u64 = 0;
        for (i, byte) in keyword.bytes().enumerate() {
            hash = hash.wrapping_add(
                (byte as u64).wrapping_mul(self.key[i % 32] as u64)
            );
            hash = hash.rotate_left(7);
        }
        hash
    }
}

impl Default for SearchableContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Search token for encrypted search
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchToken {
    pub hash: u64,
    pub key_id: KeyId,
}

/// Document with searchable tokens
#[derive(Debug, Clone)]
pub struct EncryptedDocument {
    pub tokens: Vec<SearchToken>,
    pub key_id: KeyId,
}

/// Encrypted aggregation for privacy-preserving analytics
pub struct EncryptedAggregator {
    context: Arc<PaillierContext>,
    running_sum: RwLock<Option<Ciphertext>>,
    count: RwLock<u64>,
}

impl EncryptedAggregator {
    pub fn new(context: Arc<PaillierContext>) -> Self {
        Self {
            context,
            running_sum: RwLock::new(None),
            count: RwLock::new(0),
        }
    }

    /// Add an encrypted value to the aggregation
    pub async fn add(&self, value: &Ciphertext) -> Result<()> {
        let mut sum = self.running_sum.write().await;
        let mut count = self.count.write().await;

        *sum = match sum.take() {
            Some(current) => Some(self.context.add(&current, value)?),
            None => Some(value.clone()),
        };
        *count += 1;

        Ok(())
    }

    /// Get the encrypted sum
    pub async fn sum(&self) -> Option<Ciphertext> {
        self.running_sum.read().await.clone()
    }

    /// Get the count (not encrypted, just for averaging)
    pub async fn count(&self) -> u64 {
        *self.count.read().await
    }

    /// Reset the aggregator
    pub async fn reset(&self) {
        *self.running_sum.write().await = None;
        *self.count.write().await = 0;
    }
}

/// Homomorphic encryption manager
pub struct HomomorphicManager {
    config: HomomorphicConfig,
    paillier_contexts: RwLock<HashMap<KeyId, Arc<PaillierContext>>>,
    ope_contexts: RwLock<HashMap<KeyId, Arc<OrderPreservingContext>>>,
    searchable_contexts: RwLock<HashMap<KeyId, Arc<SearchableContext>>>,
}

impl HomomorphicManager {
    pub fn new(config: HomomorphicConfig) -> Self {
        Self {
            config,
            paillier_contexts: RwLock::new(HashMap::new()),
            ope_contexts: RwLock::new(HashMap::new()),
            searchable_contexts: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new Paillier encryption context
    pub async fn create_paillier_context(&self) -> Arc<PaillierContext> {
        let (public, secret) = generate_keypair(&self.config);
        let context = Arc::new(PaillierContext::from_keypair(public, secret));

        self.paillier_contexts.write().await
            .insert(context.public_key.key_id, context.clone());

        info!(key_id = ?context.public_key.key_id, "Created Paillier context");
        context
    }

    /// Create a new order-preserving encryption context
    pub async fn create_ope_context(&self) -> Arc<OrderPreservingContext> {
        let context = Arc::new(OrderPreservingContext::new());
        self.ope_contexts.write().await
            .insert(context.key_id, context.clone());

        info!(key_id = ?context.key_id, "Created OPE context");
        context
    }

    /// Create a new searchable encryption context
    pub async fn create_searchable_context(&self) -> Arc<SearchableContext> {
        let context = Arc::new(SearchableContext::new());
        self.searchable_contexts.write().await
            .insert(context.key_id, context.clone());

        info!(key_id = ?context.key_id, "Created searchable context");
        context
    }

    /// Get a Paillier context by key ID
    pub async fn get_paillier(&self, key_id: KeyId) -> Option<Arc<PaillierContext>> {
        self.paillier_contexts.read().await.get(&key_id).cloned()
    }

    /// Get an OPE context by key ID
    pub async fn get_ope(&self, key_id: KeyId) -> Option<Arc<OrderPreservingContext>> {
        self.ope_contexts.read().await.get(&key_id).cloned()
    }

    /// Get a searchable context by key ID
    pub async fn get_searchable(&self, key_id: KeyId) -> Option<Arc<SearchableContext>> {
        self.searchable_contexts.read().await.get(&key_id).cloned()
    }
}

/// Privacy-preserving computation result
#[derive(Debug, Clone)]
pub struct ComputationResult {
    pub operation: String,
    pub encrypted_result: Ciphertext,
    pub noise_level: u32,
    pub operations_performed: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);

        assert_eq!(public.key_id, secret.key_id);
        assert_eq!(public.scheme, EncryptionScheme::Paillier);
    }

    #[test]
    fn test_paillier_encrypt_decrypt() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);
        let context = PaillierContext::from_keypair(public, secret);

        let plaintext = 42u64;
        let ciphertext = context.encrypt(plaintext);
        let decrypted = context.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_paillier_homomorphic_addition() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);
        let context = PaillierContext::from_keypair(public, secret);

        let a = 10u64;
        let b = 20u64;

        let enc_a = context.encrypt(a);
        let enc_b = context.encrypt(b);

        let enc_sum = context.add(&enc_a, &enc_b).unwrap();
        let sum = context.decrypt(&enc_sum).unwrap();

        assert_eq!(sum, a + b);
    }

    #[test]
    fn test_paillier_add_plaintext() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);
        let context = PaillierContext::from_keypair(public, secret);

        let a = 15u64;
        let b = 7u64;

        let enc_a = context.encrypt(a);
        let enc_sum = context.add_plain(&enc_a, b).unwrap();
        let sum = context.decrypt(&enc_sum).unwrap();

        assert_eq!(sum, a + b);
    }

    #[test]
    fn test_paillier_scalar_multiply() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);
        let context = PaillierContext::from_keypair(public, secret);

        let a = 5u64;
        let k = 4u64;

        let enc_a = context.encrypt(a);
        let enc_product = context.multiply_plain(&enc_a, k).unwrap();
        let product = context.decrypt(&enc_product).unwrap();

        assert_eq!(product, a * k);
    }

    #[test]
    fn test_order_preserving() {
        let ope = OrderPreservingContext::new();

        let values = [10u64, 50, 30, 80, 5];
        let encrypted: Vec<_> = values.iter().map(|&v| ope.encrypt(v)).collect();

        // Check order is preserved
        for i in 0..values.len() {
            for j in 0..values.len() {
                assert_eq!(
                    ope.compare(encrypted[i], encrypted[j]),
                    values[i].cmp(&values[j])
                );
            }
        }

        // Check decryption
        for (i, &enc) in encrypted.iter().enumerate() {
            assert_eq!(ope.decrypt(enc), values[i]);
        }
    }

    #[test]
    fn test_searchable_encryption() {
        let se = SearchableContext::new();

        let doc1 = se.encrypt_document(&["hello", "world", "rust"]);
        let doc2 = se.encrypt_document(&["hello", "crypto"]);
        let doc3 = se.encrypt_document(&["rust", "programming"]);

        let documents = vec![doc1, doc2, doc3];

        // Search for "hello"
        let token = se.create_token("hello");
        let results = se.search(&token, &documents);
        assert_eq!(results, vec![0, 1]);

        // Search for "rust"
        let token = se.create_token("rust");
        let results = se.search(&token, &documents);
        assert_eq!(results, vec![0, 2]);

        // Search for non-existent keyword
        let token = se.create_token("python");
        let results = se.search(&token, &documents);
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_encrypted_aggregator() {
        let config = HomomorphicConfig::default();
        let (public, secret) = generate_keypair(&config);
        let context = Arc::new(PaillierContext::from_keypair(public, secret));

        let aggregator = EncryptedAggregator::new(context.clone());

        let values = [10u64, 20, 30, 40];
        for &v in &values {
            let encrypted = context.encrypt(v);
            aggregator.add(&encrypted).await.unwrap();
        }

        assert_eq!(aggregator.count().await, 4);

        let sum = aggregator.sum().await.unwrap();
        let decrypted_sum = context.decrypt(&sum).unwrap();
        let expected: u64 = values.iter().sum();
        assert_eq!(decrypted_sum, expected);
    }

    #[tokio::test]
    async fn test_homomorphic_manager() {
        let config = HomomorphicConfig::default();
        let manager = HomomorphicManager::new(config);

        let paillier = manager.create_paillier_context().await;
        let ope = manager.create_ope_context().await;
        let se = manager.create_searchable_context().await;

        // Retrieve contexts
        assert!(manager.get_paillier(paillier.public_key.key_id).await.is_some());
        assert!(manager.get_ope(ope.key_id).await.is_some());
        assert!(manager.get_searchable(se.key_id).await.is_some());

        // Non-existent key
        assert!(manager.get_paillier(KeyId::new()).await.is_none());
    }

    #[test]
    fn test_noise_level_tracking() {
        let config = HomomorphicConfig {
            noise_budget: 10,
            ..Default::default()
        };
        let (public, secret) = generate_keypair(&config);
        let context = PaillierContext::from_keypair(public, secret);

        let enc = context.encrypt(5);
        assert_eq!(enc.noise_level, 0);
        assert!(enc.can_operate(10));

        let enc2 = context.add_plain(&enc, 3).unwrap();
        assert_eq!(enc2.noise_level, 1);

        let enc3 = context.multiply_plain(&enc2, 2).unwrap();
        assert_eq!(enc3.noise_level, 3);
        assert!(enc3.can_operate(10));
    }

    #[test]
    fn test_gcd_lcm() {
        assert_eq!(gcd(48, 18), 6);
        assert_eq!(gcd(100, 25), 25);
        assert_eq!(lcm(4, 6), 12);
        assert_eq!(lcm(3, 5), 15);
    }

    #[test]
    fn test_mod_pow() {
        assert_eq!(mod_pow(2, 10, 1000), 24);
        assert_eq!(mod_pow(3, 4, 100), 81);
        assert_eq!(mod_pow(7, 3, 13), 5);
    }
}
