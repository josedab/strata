//! Integration tests for erasure coding.

use strata::erasure::ErasureCoder;
use strata::types::ErasureCodingConfig;

#[test]
fn test_erasure_small_cluster_roundtrip() {
    let config = ErasureCodingConfig::SMALL_CLUSTER; // 2+1
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Hello, World! This is test data for erasure coding.";

    // Encode
    let shards = coder.encode(original_data).expect("Failed to encode");
    assert_eq!(shards.len(), 3); // 2 data + 1 parity

    // Decode with all shards present
    let mut all_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    let decoded = coder
        .decode(&mut all_shards, original_data.len())
        .expect("Failed to decode");
    assert_eq!(&decoded[..], original_data);
}

#[test]
fn test_erasure_default_cluster_roundtrip() {
    let config = ErasureCodingConfig::DEFAULT; // 4+2
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Default cluster test with more redundancy for better fault tolerance.";

    // Encode
    let shards = coder.encode(original_data).expect("Failed to encode");
    assert_eq!(shards.len(), 6); // 4 data + 2 parity

    // Decode with all shards
    let mut all_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    let decoded = coder
        .decode(&mut all_shards, original_data.len())
        .expect("Failed to decode");
    assert_eq!(&decoded[..], original_data);
}

#[test]
fn test_erasure_large_cluster_roundtrip() {
    let config = ErasureCodingConfig::LARGE_CLUSTER; // 8+4
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Large cluster configuration provides excellent fault tolerance.";

    // Encode
    let shards = coder.encode(original_data).expect("Failed to encode");
    assert_eq!(shards.len(), 12); // 8 data + 4 parity

    // Decode with all shards
    let mut all_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    let decoded = coder
        .decode(&mut all_shards, original_data.len())
        .expect("Failed to decode");
    assert_eq!(&decoded[..], original_data);
}

#[test]
fn test_erasure_recovery_one_missing() {
    let config = ErasureCodingConfig::SMALL_CLUSTER; // 2+1
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Testing recovery with missing shard.";
    let shards = coder.encode(original_data).expect("Failed to encode");

    // Remove one shard (simulating failure)
    let mut partial_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    partial_shards[0] = None; // Remove first shard

    // Should still decode successfully
    let decoded = coder
        .decode(&mut partial_shards, original_data.len())
        .expect("Failed to decode with missing shard");
    assert_eq!(&decoded[..], original_data);
}

#[test]
fn test_erasure_recovery_max_missing() {
    let config = ErasureCodingConfig::DEFAULT; // 4+2
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Testing recovery with maximum missing shards (2 parity).";
    let shards = coder.encode(original_data).expect("Failed to encode");

    // Remove 2 shards (max recoverable)
    let mut partial_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    partial_shards[0] = None;
    partial_shards[2] = None;

    // Should still decode successfully
    let decoded = coder
        .decode(&mut partial_shards, original_data.len())
        .expect("Failed to decode with max missing shards");
    assert_eq!(&decoded[..], original_data);
}

#[test]
fn test_erasure_too_many_missing_fails() {
    let config = ErasureCodingConfig::SMALL_CLUSTER; // 2+1
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"This should fail with too many missing.";
    let shards = coder.encode(original_data).expect("Failed to encode");

    // Remove 2 shards (more than 1 parity can recover)
    let mut partial_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    partial_shards[0] = None;
    partial_shards[1] = None;

    // Should fail to decode
    let result = coder.decode(&mut partial_shards, original_data.len());
    assert!(result.is_err());
}

#[test]
fn test_erasure_large_data() {
    let config = ErasureCodingConfig::DEFAULT;
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    // Create 1MB of test data
    let original_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

    let shards = coder.encode(&original_data).expect("Failed to encode large data");

    // Remove one shard
    let mut partial_shards: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    partial_shards[3] = None;

    let decoded = coder
        .decode(&mut partial_shards, original_data.len())
        .expect("Failed to decode large data");
    assert_eq!(&decoded[..], &original_data[..]);
}

#[test]
fn test_erasure_verify_valid() {
    let config = ErasureCodingConfig::SMALL_CLUSTER;
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let data = b"Verification test data.";
    let shards = coder.encode(data).expect("Failed to encode");

    // All shards should verify
    assert!(coder.verify(&shards).expect("Failed to verify"));
}

#[test]
fn test_erasure_verify_corrupted() {
    let config = ErasureCodingConfig::SMALL_CLUSTER;
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let data = b"Corruption test data.";
    let mut shards = coder.encode(data).expect("Failed to encode");

    // Corrupt one shard
    if !shards[0].is_empty() {
        shards[0][0] ^= 0xFF;
    }

    // Verification should fail
    assert!(!coder.verify(&shards).expect("Failed to verify"));
}

#[test]
fn test_erasure_empty_data() {
    let config = ErasureCodingConfig::SMALL_CLUSTER;
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    // Empty data cannot be encoded (Reed-Solomon requires non-empty shards)
    let data = b"";
    let result = coder.encode(data);
    assert!(result.is_err());
}

#[test]
fn test_erasure_storage_overhead() {
    // Verify storage overhead calculations
    let small = ErasureCodingConfig::SMALL_CLUSTER;
    assert!((small.storage_overhead() - 1.5).abs() < 0.01); // 3/2 = 1.5x

    let default = ErasureCodingConfig::DEFAULT;
    assert!((default.storage_overhead() - 1.5).abs() < 0.01); // 6/4 = 1.5x

    let large = ErasureCodingConfig::LARGE_CLUSTER;
    assert!((large.storage_overhead() - 1.5).abs() < 0.01); // 12/8 = 1.5x
}

#[test]
fn test_erasure_cost_optimized() {
    let config = ErasureCodingConfig::COST_OPTIMIZED; // 10+4
    let coder = ErasureCoder::new(config).expect("Failed to create coder");

    let original_data = b"Cost optimized storage with lower overhead.";

    let shards = coder.encode(original_data).expect("Failed to encode");
    assert_eq!(shards.len(), 14); // 10 data + 4 parity

    // Can recover from 4 missing shards
    let mut partial: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.clone())).collect();
    partial[0] = None;
    partial[1] = None;
    partial[5] = None;
    partial[10] = None;

    let decoded = coder
        .decode(&mut partial, original_data.len())
        .expect("Failed to decode");
    assert_eq!(&decoded[..], original_data);

    // Storage overhead should be 1.4x
    assert!((config.storage_overhead() - 1.4).abs() < 0.01);
}
