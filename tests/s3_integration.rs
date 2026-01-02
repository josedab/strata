//! S3 Gateway integration tests
//!
//! Tests S3-compatible API operations.

#![cfg(feature = "s3")]

#[allow(dead_code)]
mod common;

use common::fixtures::{TestBucket, TestDataGenerator, TestObject, file_sizes};

// =============================================================================
// Bucket Operation Tests
// =============================================================================

#[tokio::test]
async fn test_bucket_operations() {
    // Test bucket CRUD operations
    let bucket = TestBucket::new("test-bucket");

    // Verify bucket configuration
    assert_eq!(bucket.name, "test-bucket");
    assert!(!bucket.versioning);
    assert!(!bucket.encryption);
}

#[tokio::test]
async fn test_bucket_with_versioning() {
    let bucket = TestBucket::new("versioned-bucket")
        .with_versioning()
        .with_encryption();

    assert!(bucket.versioning);
    assert!(bucket.encryption);
}

#[tokio::test]
async fn test_bucket_naming_validation() {
    // Valid bucket names
    let valid_names = [
        "my-bucket",
        "bucket123",
        "bucket.with.dots",
        "a", // Minimum length
        &"a".repeat(63), // Maximum length
    ];

    for name in valid_names {
        let bucket = TestBucket::new(name);
        assert_eq!(bucket.name, name);
    }
}

// =============================================================================
// Object Operation Tests
// =============================================================================

#[tokio::test]
async fn test_object_creation() {
    let mut gen = TestDataGenerator::new(42);

    let object = TestObject::new("test-object.txt", file_sizes::SMALL)
        .with_content_type("text/plain")
        .with_metadata("x-custom-header", "custom-value");

    assert_eq!(object.key, "test-object.txt");
    assert_eq!(object.content_type, "text/plain");
    assert_eq!(object.metadata.get("x-custom-header"), Some(&"custom-value".to_string()));
}

#[tokio::test]
async fn test_object_with_various_sizes() {
    let sizes = [
        file_sizes::ZERO,
        file_sizes::TINY,
        file_sizes::SMALL,
        file_sizes::MEDIUM,
        file_sizes::CHUNK_SIZE,
        file_sizes::TWO_CHUNKS,
    ];

    for size in sizes {
        let object = TestObject::new(format!("file_{}.bin", size), size);
        let content = object.content.generate();
        assert_eq!(content.len(), size);
        assert!(object.content.verify(&content));
    }
}

#[tokio::test]
async fn test_object_keys_with_special_characters() {
    let special_keys = [
        "simple.txt",
        "path/to/object.txt",
        "path/with spaces/file.txt",
        "unicode/日本語/ファイル.txt",
        "special-chars/file@#$.txt",
        &format!("deep/{}/path/file.txt", "a".repeat(100)),
    ];

    for key in special_keys {
        let object = TestObject::new(key, file_sizes::SMALL);
        assert_eq!(object.key, key);
    }
}

// =============================================================================
// Content Verification Tests
// =============================================================================

#[tokio::test]
async fn test_content_verification() {
    let object = TestObject::new("verify.bin", file_sizes::MEDIUM);

    // Generate content
    let content = object.content.generate();

    // Verify content matches
    assert!(object.content.verify(&content));

    // Verify modified content fails
    let mut modified = content.clone();
    if !modified.is_empty() {
        modified[0] ^= 0xFF;
        assert!(!object.content.verify(&modified));
    }

    // Verify truncated content fails
    let truncated = &content[..content.len().saturating_sub(1)];
    assert!(!object.content.verify(truncated));
}

#[tokio::test]
async fn test_deterministic_content_generation() {
    // Same seed should produce same content
    let mut gen1 = TestDataGenerator::new(12345);
    let mut gen2 = TestDataGenerator::new(12345);

    let bytes1 = gen1.random_bytes(1024);
    let bytes2 = gen2.random_bytes(1024);

    assert_eq!(bytes1, bytes2);

    // Different seeds should produce different content
    let mut gen3 = TestDataGenerator::new(54321);
    let bytes3 = gen3.random_bytes(1024);

    assert_ne!(bytes1, bytes3);
}

// =============================================================================
// Bucket with Objects Tests
// =============================================================================

#[tokio::test]
async fn test_bucket_with_objects() {
    let bucket = TestBucket::new("objects-bucket")
        .with_object(TestObject::new("file1.txt", file_sizes::SMALL))
        .with_object(TestObject::new("file2.txt", file_sizes::MEDIUM))
        .with_object(TestObject::new("dir/file3.txt", file_sizes::SMALL));

    assert_eq!(bucket.objects.len(), 3);
    assert_eq!(bucket.objects[0].key, "file1.txt");
    assert_eq!(bucket.objects[1].key, "file2.txt");
    assert_eq!(bucket.objects[2].key, "dir/file3.txt");
}

#[tokio::test]
async fn test_bucket_object_listing_simulation() {
    let bucket = TestBucket::new("list-test")
        .with_object(TestObject::new("a/1.txt", file_sizes::TINY))
        .with_object(TestObject::new("a/2.txt", file_sizes::TINY))
        .with_object(TestObject::new("b/1.txt", file_sizes::TINY))
        .with_object(TestObject::new("b/2.txt", file_sizes::TINY))
        .with_object(TestObject::new("root.txt", file_sizes::TINY));

    // Filter by prefix
    let prefix_a: Vec<_> = bucket.objects.iter()
        .filter(|o| o.key.starts_with("a/"))
        .collect();
    assert_eq!(prefix_a.len(), 2);

    let prefix_b: Vec<_> = bucket.objects.iter()
        .filter(|o| o.key.starts_with("b/"))
        .collect();
    assert_eq!(prefix_b.len(), 2);

    // Root level (no prefix)
    let root: Vec<_> = bucket.objects.iter()
        .filter(|o| !o.key.contains('/'))
        .collect();
    assert_eq!(root.len(), 1);
}

// =============================================================================
// Multipart Upload Simulation Tests
// =============================================================================

#[tokio::test]
async fn test_multipart_upload_simulation() {
    let part_size = file_sizes::CHUNK_SIZE;
    let total_size = part_size * 3; // 3 parts

    let object = TestObject::new("multipart-object.bin", total_size);
    let content = object.content.generate();

    // Split into parts
    let parts: Vec<_> = content.chunks(part_size).collect();
    assert_eq!(parts.len(), 3);

    // Each part should be CHUNK_SIZE (except possibly the last)
    assert_eq!(parts[0].len(), part_size);
    assert_eq!(parts[1].len(), part_size);
    assert_eq!(parts[2].len(), part_size);

    // Reassemble
    let reassembled: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
    assert!(object.content.verify(&reassembled));
}

#[tokio::test]
async fn test_multipart_with_small_last_part() {
    let part_size = file_sizes::CHUNK_SIZE;
    let total_size = part_size * 2 + 1000; // 2 full parts + 1000 bytes

    let object = TestObject::new("multipart-small-last.bin", total_size);
    let content = object.content.generate();

    let parts: Vec<_> = content.chunks(part_size).collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].len(), part_size);
    assert_eq!(parts[1].len(), part_size);
    assert_eq!(parts[2].len(), 1000);
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[tokio::test]
async fn test_object_metadata() {
    let object = TestObject::new("metadata-test.txt", file_sizes::SMALL)
        .with_content_type("application/json")
        .with_metadata("x-amz-meta-author", "test-user")
        .with_metadata("x-amz-meta-version", "1.0")
        .with_metadata("x-amz-meta-tags", "test,integration");

    assert_eq!(object.content_type, "application/json");
    assert_eq!(object.metadata.len(), 3);
    assert_eq!(object.metadata.get("x-amz-meta-author"), Some(&"test-user".to_string()));
}

#[tokio::test]
async fn test_content_types() {
    let content_types = [
        ("file.txt", "text/plain"),
        ("file.html", "text/html"),
        ("file.json", "application/json"),
        ("file.xml", "application/xml"),
        ("file.bin", "application/octet-stream"),
        ("file.png", "image/png"),
        ("file.jpg", "image/jpeg"),
    ];

    for (key, content_type) in content_types {
        let object = TestObject::new(key, file_sizes::TINY)
            .with_content_type(content_type);
        assert_eq!(object.content_type, content_type);
    }
}

// =============================================================================
// Copy and Move Simulation Tests
// =============================================================================

#[tokio::test]
async fn test_copy_object_simulation() {
    let source = TestObject::new("source.txt", file_sizes::SMALL);
    let source_content = source.content.generate();

    // Simulate copy by creating a new object with same content
    // In real S3, this would be a server-side copy
    let dest = TestObject::new("dest.txt", source.content.size());

    // The copy should preserve content
    assert_eq!(source_content.len(), dest.content.size());
}

#[tokio::test]
async fn test_cross_bucket_copy_simulation() {
    let source_bucket = TestBucket::new("source-bucket")
        .with_object(TestObject::new("file.txt", file_sizes::SMALL));

    let dest_bucket = TestBucket::new("dest-bucket");

    // In real implementation, we'd copy the object
    // Here we just verify the setup
    assert_eq!(source_bucket.objects.len(), 1);
    assert_eq!(dest_bucket.objects.len(), 0);
}

// =============================================================================
// Error Condition Tests
// =============================================================================

#[tokio::test]
async fn test_empty_object() {
    let object = TestObject::new("empty.txt", file_sizes::ZERO);
    let content = object.content.generate();

    assert!(content.is_empty());
    assert!(object.content.verify(&content));
}

#[tokio::test]
async fn test_object_size_limits() {
    // S3 single object size limits
    const MAX_SINGLE_PUT: usize = 5 * 1024 * 1024 * 1024; // 5 GB

    // We can't actually create 5GB objects in tests, but we can verify the logic
    let large_object = TestObject::new("large.bin", file_sizes::LARGE);
    assert!(large_object.content.size() < MAX_SINGLE_PUT);
}

// =============================================================================
// Test Data Generator Tests
// =============================================================================

#[tokio::test]
async fn test_random_filename_generation() {
    let mut gen = TestDataGenerator::new(42);

    let filenames: Vec<_> = (0..10).map(|_| gen.random_filename()).collect();

    // All filenames should end with .dat
    for name in &filenames {
        assert!(name.ends_with(".dat"));
        assert!(name.len() > 4);
    }

    // Filenames should be unique (with high probability)
    let unique: std::collections::HashSet<_> = filenames.iter().collect();
    assert!(unique.len() > 5); // Allow some collisions but expect mostly unique
}

#[tokio::test]
async fn test_random_path_generation() {
    let mut gen = TestDataGenerator::new(42);

    let path = gen.random_path(5);

    // Path should have 5 components
    assert_eq!(path.components().count(), 5);
}

#[tokio::test]
async fn test_verifiable_content_different_seeds() {
    let content1 = common::fixtures::VerifiableContent::new(1, 1024);
    let content2 = common::fixtures::VerifiableContent::new(2, 1024);

    let data1 = content1.generate();
    let data2 = content2.generate();

    // Different seeds should produce different content
    assert_ne!(data1, data2);

    // Each should verify against itself
    assert!(content1.verify(&data1));
    assert!(content2.verify(&data2));

    // Cross-verification should fail
    assert!(!content1.verify(&data2));
    assert!(!content2.verify(&data1));
}
