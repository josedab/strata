//! Compression module for Strata.
//!
//! Provides compression utilities for data storage and transfer.

use crate::error::{Result, StrataError};
use std::io::{Read, Write};

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    /// No compression.
    #[default]
    None = 0,
    /// LZ4 compression (fast, moderate ratio).
    Lz4 = 1,
    /// Zstd compression (balanced speed/ratio).
    Zstd = 2,
    /// Snappy compression (very fast, lower ratio).
    Snappy = 3,
}

impl CompressionAlgorithm {
    /// Get algorithm from byte value.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            3 => Some(Self::Snappy),
            _ => None,
        }
    }

    /// Convert to byte value.
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }

    /// Get human-readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Snappy => "snappy",
        }
    }
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::str::FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            "snappy" => Ok(Self::Snappy),
            _ => Err(format!("Unknown compression algorithm: {}", s)),
        }
    }
}

/// Compression configuration.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Algorithm to use.
    pub algorithm: CompressionAlgorithm,
    /// Compression level (algorithm-specific).
    pub level: i32,
    /// Minimum size to compress (bytes).
    pub min_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::None,
            level: 3,
            min_size: 512,
        }
    }
}

impl CompressionConfig {
    /// Fast compression (prioritize speed).
    pub fn fast() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Lz4,
            level: 1,
            min_size: 256,
        }
    }

    /// Balanced compression (default for most use cases).
    pub fn balanced() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 512,
        }
    }

    /// Best compression (prioritize ratio).
    pub fn best() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: 9,
            min_size: 256,
        }
    }
}

/// Compressed data with metadata header.
#[derive(Debug, Clone)]
pub struct CompressedData {
    /// Original (uncompressed) size.
    pub original_size: u64,
    /// Compressed size.
    pub compressed_size: u64,
    /// Compression algorithm used.
    pub algorithm: CompressionAlgorithm,
    /// Compressed data bytes.
    pub data: Vec<u8>,
}

impl CompressedData {
    /// Create from uncompressed data.
    pub fn new(data: Vec<u8>, algorithm: CompressionAlgorithm) -> Self {
        Self {
            original_size: data.len() as u64,
            compressed_size: data.len() as u64,
            algorithm,
            data,
        }
    }

    /// Get compression ratio.
    pub fn ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 1.0;
        }
        self.original_size as f64 / self.compressed_size as f64
    }

    /// Check if data was actually compressed.
    pub fn is_compressed(&self) -> bool {
        self.algorithm != CompressionAlgorithm::None && self.compressed_size < self.original_size
    }

    /// Serialize to bytes with header.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(17 + self.data.len());
        result.extend_from_slice(&self.original_size.to_le_bytes());
        result.extend_from_slice(&self.compressed_size.to_le_bytes());
        result.push(self.algorithm.as_byte());
        result.extend_from_slice(&self.data);
        result
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 17 {
            return Err(StrataError::InvalidData(
                "Compressed data too short".to_string(),
            ));
        }

        let original_size = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .expect("length checked above; slice is exactly 8 bytes"),
        );
        let compressed_size = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .expect("length checked above; slice is exactly 8 bytes"),
        );
        let algorithm = CompressionAlgorithm::from_byte(bytes[16]).ok_or_else(|| {
            StrataError::InvalidData("Unknown compression algorithm".to_string())
        })?;

        if bytes.len() < 17 + compressed_size as usize {
            return Err(StrataError::InvalidData(
                "Compressed data truncated".to_string(),
            ));
        }

        let data = bytes[17..17 + compressed_size as usize].to_vec();

        Ok(Self {
            original_size,
            compressed_size,
            algorithm,
            data,
        })
    }
}

/// Compressor for data.
pub struct Compressor {
    config: CompressionConfig,
}

impl Compressor {
    /// Create a new compressor.
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }

    /// Compress data.
    pub fn compress(&self, data: &[u8]) -> Result<CompressedData> {
        // Skip compression for small data
        if data.len() < self.config.min_size {
            return Ok(CompressedData::new(data.to_vec(), CompressionAlgorithm::None));
        }

        match self.config.algorithm {
            CompressionAlgorithm::None => {
                Ok(CompressedData::new(data.to_vec(), CompressionAlgorithm::None))
            }
            CompressionAlgorithm::Lz4 => self.compress_lz4(data),
            CompressionAlgorithm::Zstd => self.compress_zstd(data),
            CompressionAlgorithm::Snappy => self.compress_snappy(data),
        }
    }

    /// Decompress data.
    pub fn decompress(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        match compressed.algorithm {
            CompressionAlgorithm::None => Ok(compressed.data.clone()),
            CompressionAlgorithm::Lz4 => self.decompress_lz4(compressed),
            CompressionAlgorithm::Zstd => self.decompress_zstd(compressed),
            CompressionAlgorithm::Snappy => self.decompress_snappy(compressed),
        }
    }

    // Simple LZ4-like compression (simplified RLE + LZ)
    fn compress_lz4(&self, data: &[u8]) -> Result<CompressedData> {
        let compressed = simple_compress(data);

        // If compression didn't help, store uncompressed
        if compressed.len() >= data.len() {
            return Ok(CompressedData::new(data.to_vec(), CompressionAlgorithm::None));
        }

        Ok(CompressedData {
            original_size: data.len() as u64,
            compressed_size: compressed.len() as u64,
            algorithm: CompressionAlgorithm::Lz4,
            data: compressed,
        })
    }

    fn decompress_lz4(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        simple_decompress(&compressed.data, compressed.original_size as usize)
    }

    // Zstd compression (using simple implementation)
    fn compress_zstd(&self, data: &[u8]) -> Result<CompressedData> {
        let compressed = simple_compress(data);

        if compressed.len() >= data.len() {
            return Ok(CompressedData::new(data.to_vec(), CompressionAlgorithm::None));
        }

        Ok(CompressedData {
            original_size: data.len() as u64,
            compressed_size: compressed.len() as u64,
            algorithm: CompressionAlgorithm::Zstd,
            data: compressed,
        })
    }

    fn decompress_zstd(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        simple_decompress(&compressed.data, compressed.original_size as usize)
    }

    // Snappy compression
    fn compress_snappy(&self, data: &[u8]) -> Result<CompressedData> {
        let compressed = simple_compress(data);

        if compressed.len() >= data.len() {
            return Ok(CompressedData::new(data.to_vec(), CompressionAlgorithm::None));
        }

        Ok(CompressedData {
            original_size: data.len() as u64,
            compressed_size: compressed.len() as u64,
            algorithm: CompressionAlgorithm::Snappy,
            data: compressed,
        })
    }

    fn decompress_snappy(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        simple_decompress(&compressed.data, compressed.original_size as usize)
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new(CompressionConfig::default())
    }
}

// Simple run-length encoding + dictionary compression
// This is a simplified implementation; production would use actual LZ4/Zstd/Snappy
fn simple_compress(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return vec![];
    }

    let mut result = Vec::new();
    let mut i = 0;

    while i < data.len() {
        // Look for runs of repeated bytes
        let byte = data[i];
        let mut run_len = 1;

        while i + run_len < data.len() && data[i + run_len] == byte && run_len < 255 {
            run_len += 1;
        }

        if run_len >= 4 {
            // Encode as run: 0x00, byte, length
            result.push(0x00);
            result.push(byte);
            result.push(run_len as u8);
            i += run_len;
        } else {
            // Literal byte (escape 0x00 as 0x00 0x00 0x01)
            if byte == 0x00 {
                result.push(0x00);
                result.push(0x00);
                result.push(0x01);
            } else {
                result.push(byte);
            }
            i += 1;
        }
    }

    result
}

fn simple_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(expected_size);
    let mut i = 0;

    while i < data.len() {
        if data[i] == 0x00 {
            if i + 2 >= data.len() {
                return Err(StrataError::InvalidData(
                    "Truncated compressed data".to_string(),
                ));
            }

            let byte = data[i + 1];
            let count = data[i + 2] as usize;

            if byte == 0x00 && count == 1 {
                // Escaped literal 0x00
                result.push(0x00);
            } else {
                // Run of bytes
                for _ in 0..count {
                    result.push(byte);
                }
            }
            i += 3;
        } else {
            result.push(data[i]);
            i += 1;
        }
    }

    if result.len() != expected_size {
        return Err(StrataError::InvalidData(format!(
            "Decompressed size mismatch: expected {}, got {}",
            expected_size,
            result.len()
        )));
    }

    Ok(result)
}

/// Streaming compressor.
pub struct StreamCompressor {
    algorithm: CompressionAlgorithm,
    buffer: Vec<u8>,
}

impl StreamCompressor {
    /// Create a new stream compressor.
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            buffer: Vec::new(),
        }
    }

    /// Write data to the compressor.
    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Finish compression and return compressed data.
    pub fn finish(self) -> Result<CompressedData> {
        let compressor = Compressor::new(CompressionConfig {
            algorithm: self.algorithm,
            level: 3,
            min_size: 0,
        });
        compressor.compress(&self.buffer)
    }
}

impl Write for StreamCompressor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Streaming decompressor.
pub struct StreamDecompressor {
    data: Vec<u8>,
    position: usize,
}

impl StreamDecompressor {
    /// Create a new stream decompressor.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, position: 0 }
    }
}

impl Read for StreamDecompressor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.data.len() - self.position;
        let to_read = buf.len().min(remaining);

        if to_read == 0 {
            return Ok(0);
        }

        buf[..to_read].copy_from_slice(&self.data[self.position..self.position + to_read]);
        self.position += to_read;

        Ok(to_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_algorithm_from_byte() {
        assert_eq!(
            CompressionAlgorithm::from_byte(0),
            Some(CompressionAlgorithm::None)
        );
        assert_eq!(
            CompressionAlgorithm::from_byte(1),
            Some(CompressionAlgorithm::Lz4)
        );
        assert_eq!(
            CompressionAlgorithm::from_byte(2),
            Some(CompressionAlgorithm::Zstd)
        );
        assert_eq!(CompressionAlgorithm::from_byte(255), None);
    }

    #[test]
    fn test_compression_algorithm_parse() {
        assert_eq!(
            "lz4".parse::<CompressionAlgorithm>().unwrap(),
            CompressionAlgorithm::Lz4
        );
        assert_eq!(
            "ZSTD".parse::<CompressionAlgorithm>().unwrap(),
            CompressionAlgorithm::Zstd
        );
        assert!("invalid".parse::<CompressionAlgorithm>().is_err());
    }

    #[test]
    fn test_compress_decompress_none() {
        let compressor = Compressor::new(CompressionConfig::default());
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed.algorithm, CompressionAlgorithm::None);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_with_runs() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            level: 3,
            min_size: 0,
        };
        let compressor = Compressor::new(config);

        // Data with repeated bytes
        let data: Vec<u8> = vec![0u8; 100]
            .into_iter()
            .chain(vec![1u8; 100])
            .chain(vec![2u8; 100])
            .collect();

        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compressed_data_serialization() {
        let data = vec![1, 2, 3, 4, 5];
        let original = CompressedData {
            original_size: 1000,
            compressed_size: data.len() as u64,
            algorithm: CompressionAlgorithm::Lz4,
            data,
        };

        let bytes = original.to_bytes();
        let restored = CompressedData::from_bytes(&bytes).unwrap();

        assert_eq!(restored.original_size, 1000);
        assert_eq!(restored.compressed_size, 5);
        assert_eq!(restored.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(restored.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_compression_ratio() {
        let compressed = CompressedData {
            original_size: 1000,
            compressed_size: 250,
            algorithm: CompressionAlgorithm::Zstd,
            data: vec![],
        };

        assert!((compressed.ratio() - 4.0).abs() < 0.01);
    }

    #[test]
    fn test_stream_compressor() {
        let mut compressor = StreamCompressor::new(CompressionAlgorithm::Lz4);
        compressor.write(b"Hello, ");
        compressor.write(b"World!");

        let compressed = compressor.finish().unwrap();
        assert!(compressed.original_size > 0);
    }

    #[test]
    fn test_config_presets() {
        let fast = CompressionConfig::fast();
        assert_eq!(fast.algorithm, CompressionAlgorithm::Lz4);

        let balanced = CompressionConfig::balanced();
        assert_eq!(balanced.algorithm, CompressionAlgorithm::Zstd);

        let best = CompressionConfig::best();
        assert_eq!(best.level, 9);
    }
}
