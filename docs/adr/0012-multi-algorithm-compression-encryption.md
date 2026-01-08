# ADR-0012: Multi-Algorithm Compression and Encryption

## Status

**Accepted**

## Context

Storage systems must balance competing concerns:
- **Storage efficiency**: Compression reduces bytes stored
- **Security**: Encryption protects data at rest
- **Performance**: Both operations consume CPU
- **Compatibility**: Different workloads have different optimal algorithms

No single algorithm is optimal for all cases:
- LZ4: Fast but lower compression ratio
- Zstd: Balanced speed and ratio
- Snappy: Google's fast compression, widely supported
- AES-256-GCM: Standard encryption, hardware acceleration on modern CPUs
- ChaCha20-Poly1305: Software-efficient, good for systems without AES-NI

We needed a flexible approach that:
- Supports multiple algorithms
- Allows per-chunk algorithm selection
- Maintains compatibility across versions
- Enables future algorithm additions

## Decision

We implement **algorithm selection as enums with versioned format tags**:

### Compression

```rust
// src/compression.rs
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,
}

pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub level: Option<i32>,  // Algorithm-specific compression level
}

impl CompressionConfig {
    /// Fast compression for latency-sensitive workloads
    pub fn fast() -> Self {
        Self { algorithm: CompressionAlgorithm::Lz4, level: None }
    }

    /// Balanced compression for general use
    pub fn balanced() -> Self {
        Self { algorithm: CompressionAlgorithm::Zstd, level: Some(3) }
    }

    /// Maximum compression for archival
    pub fn maximum() -> Self {
        Self { algorithm: CompressionAlgorithm::Zstd, level: Some(19) }
    }
}
```

### Encryption

```rust
// src/encryption.rs
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum EncryptionAlgorithm {
    None = 0,
    Aes256Gcm = 1,
    ChaCha20Poly1305 = 2,
}

pub struct EncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_rotation_interval: Duration,
}

impl EncryptionConfig {
    /// Standard encryption with AES (hardware accelerated)
    pub fn standard() -> Self {
        Self {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_rotation_interval: Duration::from_secs(30 * 24 * 3600), // 30 days
        }
    }

    /// Software-optimized encryption (no AES-NI required)
    pub fn software() -> Self {
        Self {
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            key_rotation_interval: Duration::from_secs(30 * 24 * 3600),
        }
    }
}
```

### Chunk Header Format

Each chunk includes a header identifying its format:

```rust
pub struct ChunkHeader {
    /// Magic bytes for format identification
    pub magic: [u8; 4],  // "STRA"
    /// Format version
    pub version: u8,
    /// Compression algorithm used
    pub compression: CompressionAlgorithm,
    /// Encryption algorithm used
    pub encryption: EncryptionAlgorithm,
    /// Original (uncompressed) size
    pub original_size: u64,
    /// Compressed size (before encryption)
    pub compressed_size: u64,
    /// Checksum of original data
    pub checksum: [u8; 32],
}

impl ChunkHeader {
    pub fn serialize(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.compression as u8;
        buf[6] = self.encryption as u8;
        // ... rest of serialization
        buf
    }
}
```

### Processing Pipeline

Data flows through compression then encryption:

```
Write Path:
  Original Data
      │
      ▼
  ┌──────────────┐
  │  Compress    │  (based on chunk.compression)
  │  (optional)  │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │   Encrypt    │  (based on chunk.encryption)
  │  (optional)  │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │  Add Header  │
  └──────┬───────┘
         │
         ▼
  Stored Chunk

Read Path: (reverse order)
  Stored Chunk → Parse Header → Decrypt → Decompress → Original Data
```

### Implementation

```rust
pub async fn write_chunk(
    &self,
    data: &[u8],
    compression: CompressionConfig,
    encryption: EncryptionConfig,
) -> Result<ChunkId> {
    // Compress
    let compressed = match compression.algorithm {
        CompressionAlgorithm::None => data.to_vec(),
        CompressionAlgorithm::Lz4 => lz4::compress(data)?,
        CompressionAlgorithm::Zstd => {
            let level = compression.level.unwrap_or(3);
            zstd::encode_all(data, level)?
        }
        CompressionAlgorithm::Snappy => snap::raw::Encoder::new().compress_vec(data)?,
    };

    // Encrypt
    let encrypted = match encryption.algorithm {
        EncryptionAlgorithm::None => compressed,
        EncryptionAlgorithm::Aes256Gcm => {
            let key = self.key_manager.current_key()?;
            aes_gcm_encrypt(&key, &compressed)?
        }
        EncryptionAlgorithm::ChaCha20Poly1305 => {
            let key = self.key_manager.current_key()?;
            chacha20_encrypt(&key, &compressed)?
        }
    };

    // Create header
    let header = ChunkHeader {
        magic: *b"STRA",
        version: 1,
        compression: compression.algorithm,
        encryption: encryption.algorithm,
        original_size: data.len() as u64,
        compressed_size: compressed.len() as u64,
        checksum: sha256(data),
    };

    // Store
    let chunk_id = ChunkId::new();
    self.storage.put(chunk_id, &header.serialize(), &encrypted).await?;

    Ok(chunk_id)
}
```

## Consequences

### Positive

- **Workload optimization**: Choose algorithm per use case
- **Future-proof**: Add algorithms via new enum variants
- **Backward compatible**: Header identifies format for reading old data
- **Security flexibility**: Use hardware acceleration where available
- **Storage efficiency**: Tune compression vs. CPU tradeoff

### Negative

- **Configuration complexity**: More options to understand
- **Mixed clusters**: All nodes must support all algorithms
- **Testing burden**: Must test all algorithm combinations
- **Key management**: Encryption requires secure key storage

### Algorithm Selection Guide

**Compression:**
| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| None | N/A | 1.0x | Already compressed data (images, video) |
| LZ4 | ~500 MB/s | ~2.0x | Latency-sensitive workloads |
| Zstd (level 3) | ~300 MB/s | ~2.8x | General purpose (default) |
| Zstd (level 19) | ~10 MB/s | ~3.5x | Archival, cold storage |
| Snappy | ~400 MB/s | ~1.7x | Compatibility with other systems |

**Encryption:**
| Algorithm | Hardware Accel | Performance | Use Case |
|-----------|---------------|-------------|----------|
| None | N/A | N/A | Development, trusted networks |
| AES-256-GCM | AES-NI | ~5 GB/s | Default for modern CPUs |
| ChaCha20 | None needed | ~1 GB/s | ARM, older CPUs, VMs |

### Key Rotation

Encryption keys rotate periodically:

```rust
pub struct KeyManager {
    current_key: Key,
    current_key_id: u64,
    key_created_at: SystemTime,
    rotation_interval: Duration,
    old_keys: HashMap<u64, Key>,  // For reading old data
}

impl KeyManager {
    pub fn current_key(&mut self) -> Result<(u64, &Key)> {
        if self.key_created_at.elapsed()? > self.rotation_interval {
            self.rotate_key()?;
        }
        Ok((self.current_key_id, &self.current_key))
    }
}
```

### Operational Implications

- **Algorithm support**: Ensure all algorithms available on all nodes
- **Key management**: Secure storage for encryption keys
- **Monitoring**: Track compression ratios, encryption overhead
- **Migration**: Plan for algorithm changes in long-lived clusters

## References

- [LZ4](https://lz4.github.io/lz4/) - Extremely fast compression
- [Zstandard](https://facebook.github.io/zstd/) - Facebook's compression algorithm
- [AES-GCM](https://en.wikipedia.org/wiki/Galois/Counter_Mode) - Authenticated encryption
- `src/compression.rs` - Compression implementation
- `src/encryption.rs` - Encryption implementation
- `src/data/chunk_storage.rs` - Chunk storage with header handling
