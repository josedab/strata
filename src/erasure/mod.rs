//! Erasure coding implementation using Reed-Solomon.
//!
//! This module provides erasure coding functionality for data durability.
//! It uses Reed-Solomon coding to split data into data shards and parity shards,
//! allowing reconstruction of the original data even when some shards are lost.

use crate::error::{Result, StrataError};
use crate::types::ErasureCodingConfig;
use reed_solomon_erasure::galois_8::ReedSolomon;
use tracing::debug;

/// Encoder/decoder for erasure coding.
pub struct ErasureCoder {
    config: ErasureCodingConfig,
    encoder: ReedSolomon,
}

impl ErasureCoder {
    /// Create a new erasure coder with the given configuration.
    pub fn new(config: ErasureCodingConfig) -> Result<Self> {
        let encoder = ReedSolomon::new(config.data_shards, config.parity_shards)
            .map_err(|e| StrataError::Internal(format!("Failed to create encoder: {}", e)))?;

        Ok(Self { config, encoder })
    }

    /// Get the configuration.
    pub fn config(&self) -> &ErasureCodingConfig {
        &self.config
    }

    /// Encode data into shards (data + parity).
    ///
    /// Returns a vector of shards, where the first `data_shards` are data
    /// and the remaining `parity_shards` are parity.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let shard_size = self.calculate_shard_size(data.len());

        // Create data shards
        let mut shards: Vec<Vec<u8>> = (0..self.config.data_shards)
            .map(|i| {
                let start = i * shard_size;
                let end = ((i + 1) * shard_size).min(data.len());

                let mut shard = if start < data.len() {
                    data.get(start..end).unwrap_or(&[]).to_vec()
                } else {
                    Vec::new()
                };

                // Pad to shard_size
                shard.resize(shard_size, 0);
                shard
            })
            .collect();

        // Add empty parity shards
        for _ in 0..self.config.parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        // Encode parity
        self.encoder
            .encode(&mut shards)
            .map_err(|e| StrataError::Internal(format!("Encoding failed: {}", e)))?;

        debug!(
            data_len = data.len(),
            shard_size,
            num_shards = shards.len(),
            "Encoded data into shards"
        );

        Ok(shards)
    }

    /// Decode shards back into original data.
    ///
    /// The `shards` vector should have `data_shards + parity_shards` elements,
    /// where `None` represents a missing shard.
    pub fn decode(&self, shards: &mut [Option<Vec<u8>>], original_size: usize) -> Result<Vec<u8>> {
        // Check we have enough shards
        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < self.config.data_shards {
            return Err(StrataError::InsufficientShards {
                available,
                required: self.config.data_shards,
            });
        }

        // Verify shard count
        if shards.len() != self.config.total_shards() {
            return Err(StrataError::Internal(format!(
                "Expected {} shards, got {}",
                self.config.total_shards(),
                shards.len()
            )));
        }

        // Get shard size from available shard
        let shard_size = shards
            .iter()
            .find_map(|s| s.as_ref())
            .map(|s| s.len())
            .ok_or_else(|| StrataError::InsufficientShards {
                available: 0,
                required: self.config.data_shards,
            })?;

        // Reconstruct missing shards
        self.encoder
            .reconstruct(shards)
            .map_err(|e| StrataError::Internal(format!("Reconstruction failed: {}", e)))?;

        // Combine data shards
        let mut result = Vec::with_capacity(self.config.data_shards * shard_size);
        for shard in shards.iter().take(self.config.data_shards) {
            if let Some(data) = shard {
                result.extend_from_slice(data);
            } else {
                return Err(StrataError::Internal("Reconstruction incomplete".into()));
            }
        }

        // Truncate to original size
        result.truncate(original_size);

        debug!(
            original_size,
            reconstructed_size = result.len(),
            "Decoded shards to data"
        );

        Ok(result)
    }

    /// Verify that shards are consistent.
    /// Only works when all shards are present.
    pub fn verify(&self, shards: &[Vec<u8>]) -> Result<bool> {
        self.encoder
            .verify(shards)
            .map_err(|e| StrataError::Internal(format!("Verification failed: {}", e)))
    }

    /// Check if we can verify (all shards present).
    pub fn can_verify(&self, shards: &[Option<Vec<u8>>]) -> bool {
        shards.iter().all(|s| s.is_some())
    }

    /// Calculate the shard size for a given data length.
    fn calculate_shard_size(&self, data_len: usize) -> usize {
        (data_len + self.config.data_shards - 1) / self.config.data_shards
    }

    /// Calculate the storage overhead for a given data length.
    pub fn storage_size(&self, data_len: usize) -> usize {
        let shard_size = self.calculate_shard_size(data_len);
        shard_size * self.config.total_shards()
    }
}

/// Shard with metadata for storage.
#[derive(Debug, Clone)]
pub struct Shard {
    /// Shard index (0 to total_shards - 1).
    pub index: usize,
    /// Shard data.
    pub data: Vec<u8>,
    /// Whether this is a parity shard.
    pub is_parity: bool,
    /// CRC32 checksum of the shard data.
    pub checksum: u32,
}

impl Shard {
    pub fn new(index: usize, data: Vec<u8>, data_shards: usize) -> Self {
        let checksum = crc32fast::hash(&data);
        Self {
            index,
            data,
            is_parity: index >= data_shards,
            checksum,
        }
    }

    /// Verify the shard checksum.
    pub fn verify_checksum(&self) -> bool {
        crc32fast::hash(&self.data) == self.checksum
    }
}

/// Encode data into shards with metadata.
pub fn encode_with_metadata(
    data: &[u8],
    config: ErasureCodingConfig,
) -> Result<(Vec<Shard>, usize)> {
    let coder = ErasureCoder::new(config)?;
    let shard_data = coder.encode(data)?;

    let shards: Vec<Shard> = shard_data
        .into_iter()
        .enumerate()
        .map(|(i, data)| Shard::new(i, data, config.data_shards))
        .collect();

    Ok((shards, data.len()))
}

/// Decode shards with metadata back to original data.
pub fn decode_with_metadata(
    shards: &[Option<Shard>],
    original_size: usize,
    config: ErasureCodingConfig,
) -> Result<Vec<u8>> {
    // Verify checksums for available shards
    for shard in shards.iter().flatten() {
        if !shard.verify_checksum() {
            return Err(StrataError::ShardCorruption { index: shard.index });
        }
    }

    // Convert to data vectors
    let mut shard_data: Vec<Option<Vec<u8>>> = shards
        .iter()
        .map(|s| s.as_ref().map(|s| s.data.clone()))
        .collect();

    let coder = ErasureCoder::new(config)?;
    coder.decode(&mut shard_data, original_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        let data = b"Hello, World! This is a test of erasure coding.";
        let shards = coder.encode(data).unwrap();

        assert_eq!(shards.len(), 6); // 4 data + 2 parity

        // Decode with all shards
        let mut shard_opts: Vec<_> = shards.into_iter().map(Some).collect();
        let decoded = coder.decode(&mut shard_opts, data.len()).unwrap();

        assert_eq!(&decoded, data);
    }

    #[test]
    fn test_decode_with_missing_shards() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        let data = b"Test data for erasure coding reconstruction.";
        let shards = coder.encode(data).unwrap();

        // Remove 2 shards (should still be able to reconstruct)
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[1] = None;
        shard_opts[4] = None;

        let decoded = coder.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(&decoded, data);
    }

    #[test]
    fn test_decode_too_many_missing() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        let data = b"Test data";
        let shards = coder.encode(data).unwrap();

        // Remove 3 shards (too many - only 2 parity shards)
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[0] = None;
        shard_opts[2] = None;
        shard_opts[4] = None;

        let result = coder.decode(&mut shard_opts, data.len());
        assert!(result.is_err());
    }

    #[test]
    fn test_verify() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        let data = b"Verify test data";
        let shards = coder.encode(data).unwrap();

        assert!(coder.verify(&shards).unwrap());
    }

    #[test]
    fn test_shard_metadata() {
        let config = ErasureCodingConfig::DEFAULT;
        let (shards, original_size) = encode_with_metadata(b"Test metadata", config).unwrap();

        assert_eq!(shards.len(), 6);
        assert_eq!(original_size, 13);

        // Data shards
        assert!(!shards[0].is_parity);
        assert!(!shards[3].is_parity);

        // Parity shards
        assert!(shards[4].is_parity);
        assert!(shards[5].is_parity);

        // Verify checksums
        for shard in &shards {
            assert!(shard.verify_checksum());
        }
    }

    #[test]
    fn test_small_cluster_config() {
        let config = ErasureCodingConfig::SMALL_CLUSTER;
        let coder = ErasureCoder::new(config).unwrap();

        let data = b"Small cluster test";
        let shards = coder.encode(data).unwrap();

        assert_eq!(shards.len(), 3); // 2 data + 1 parity

        // Remove 1 shard
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[0] = None;

        let decoded = coder.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(&decoded, data);
    }

    #[test]
    fn test_large_data() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        // 1MB of data
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let shards = coder.encode(&data).unwrap();

        let mut shard_opts: Vec<_> = shards.into_iter().map(Some).collect();
        shard_opts[2] = None; // Remove one shard

        let decoded = coder.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_storage_overhead() {
        let config = ErasureCodingConfig::DEFAULT;
        let coder = ErasureCoder::new(config).unwrap();

        // For 4+2 config, overhead should be 1.5x
        let data_len = 1000;
        let storage_len = coder.storage_size(data_len);

        // Shard size is ceil(1000/4) = 250, total = 250 * 6 = 1500
        assert_eq!(storage_len, 1500);
    }
}
