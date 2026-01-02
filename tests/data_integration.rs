//! Integration tests for data server operations.

#[allow(dead_code)]
mod common;

use common::TestEnv;
use strata::data::ChunkStorage;
use strata::types::ChunkId;

#[test]
fn test_chunk_storage_write_read() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10) // 10MB cache
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;
    let data = b"Test shard data for storage";

    // Write shard
    storage
        .write_shard(chunk_id, shard_idx, data)
        .expect("Failed to write shard");

    // Read shard back
    let read_data = storage
        .read_shard(chunk_id, shard_idx)
        .expect("Failed to read shard");

    assert_eq!(read_data, data);
}

#[test]
fn test_chunk_storage_multiple_shards() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let total_shards = 3; // Small cluster config

    // Write multiple shards for the same chunk
    for shard_idx in 0..total_shards {
        let data = format!("Shard {} data", shard_idx);
        storage
            .write_shard(chunk_id, shard_idx, data.as_bytes())
            .expect("Failed to write shard");
    }

    // Read all shards back
    for shard_idx in 0..total_shards {
        let expected = format!("Shard {} data", shard_idx);
        let read_data = storage
            .read_shard(chunk_id, shard_idx)
            .expect("Failed to read shard");
        assert_eq!(read_data, expected.as_bytes());
    }
}

#[test]
fn test_chunk_storage_delete() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;
    let data = b"Data to be deleted";

    // Write and verify
    storage.write_shard(chunk_id, shard_idx, data).unwrap();
    assert!(storage.read_shard(chunk_id, shard_idx).is_ok());

    // Delete
    storage.delete_shard(chunk_id, shard_idx).unwrap();

    // Should not exist anymore
    assert!(storage.read_shard(chunk_id, shard_idx).is_err());
}

#[test]
fn test_chunk_storage_list_shards() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();

    // Write several shards
    for shard_idx in [0, 2, 4] {
        storage
            .write_shard(chunk_id, shard_idx, b"test")
            .expect("Failed to write");
    }

    // List shards
    let shards = storage.list_shards(chunk_id).expect("Failed to list shards");
    assert_eq!(shards.len(), 3);
    assert!(shards.contains(&0));
    assert!(shards.contains(&2));
    assert!(shards.contains(&4));
}

#[test]
fn test_chunk_storage_shard_exists() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;

    // Initially should not exist
    assert!(!storage.shard_exists(chunk_id, shard_idx));

    // Write shard
    storage.write_shard(chunk_id, shard_idx, b"test").unwrap();

    // Now should exist
    assert!(storage.shard_exists(chunk_id, shard_idx));

    // Delete shard
    storage.delete_shard(chunk_id, shard_idx).unwrap();

    // Should not exist again
    assert!(!storage.shard_exists(chunk_id, shard_idx));
}

#[test]
fn test_chunk_storage_large_shard() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 100) // 100MB cache
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;

    // Create 1MB of data
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

    storage.write_shard(chunk_id, shard_idx, &data).unwrap();

    let read_data = storage.read_shard(chunk_id, shard_idx).unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn test_chunk_storage_scrub_shard() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;

    // Write shard
    storage
        .write_shard(chunk_id, shard_idx, b"test data for scrubbing")
        .expect("Failed to write");

    // Scrub should pass (no corruption)
    let valid = storage
        .scrub_shard(chunk_id, shard_idx)
        .expect("Scrub failed");
    assert!(valid);
}

#[test]
fn test_chunk_storage_persistence() {
    let env = TestEnv::new();
    let chunk_id = ChunkId::new();
    let shard_idx = 0;
    let data = b"Persistent data";

    // Write with first storage instance
    {
        let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
            .expect("Failed to create storage");
        storage.write_shard(chunk_id, shard_idx, data).unwrap();
    }

    // Read with new storage instance
    {
        let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
            .expect("Failed to create storage");
        let read_data = storage.read_shard(chunk_id, shard_idx).unwrap();
        assert_eq!(read_data, data);
    }
}

#[test]
fn test_chunk_storage_calculate_usage() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    // Initial usage should be zero or minimal
    let initial_usage = storage.calculate_usage().unwrap();

    // Write some data
    let chunk_id = ChunkId::new();
    let data = vec![0u8; 1024 * 100]; // 100KB
    storage.write_shard(chunk_id, 0, &data).unwrap();

    // Usage should have increased
    let new_usage = storage.calculate_usage().unwrap();
    assert!(new_usage > initial_usage);
}

#[test]
fn test_chunk_storage_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let env = TestEnv::new();
    let storage = Arc::new(
        ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10).expect("Failed to create storage"),
    );

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let storage = Arc::clone(&storage);
            thread::spawn(move || {
                let chunk_id = ChunkId::new();
                let data = format!("Thread {} data", i);

                for shard_idx in 0..3 {
                    storage
                        .write_shard(chunk_id, shard_idx, data.as_bytes())
                        .expect("Failed to write");

                    let read = storage.read_shard(chunk_id, shard_idx).expect("Failed to read");
                    assert_eq!(read, data.as_bytes());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_chunk_storage_get_shard_meta() {
    let env = TestEnv::new();
    let storage = ChunkStorage::new(&env.data_dir, 1024 * 1024 * 10)
        .expect("Failed to create storage");

    let chunk_id = ChunkId::new();
    let shard_idx = 0;
    let data = b"Metadata test data";

    // Initially no metadata
    let meta = storage.get_shard_meta(chunk_id, shard_idx).unwrap();
    assert!(meta.is_none());

    // Write shard
    storage.write_shard(chunk_id, shard_idx, data).unwrap();

    // Now should have metadata
    let meta = storage.get_shard_meta(chunk_id, shard_idx).unwrap();
    assert!(meta.is_some());
    let meta = meta.unwrap();
    assert_eq!(meta.size, data.len() as u64);
}
