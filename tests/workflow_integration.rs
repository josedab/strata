//! End-to-end workflow integration tests
//!
//! Tests complete user workflows from file creation to retrieval.

#[allow(dead_code)]
mod common;

use strata::metadata::{MetadataOp, MetadataStateMachine};
use strata::raft::StateMachine;
use strata::types::{ChunkId, FileType, InodeId};

const ROOT_INODE: InodeId = 1;

/// Helper to extract created inode ID from OpResult
fn get_created_inode(result: &strata::metadata::OpResult) -> InodeId {
    match result {
        strata::metadata::OpResult::Created { inode } => *inode,
        _ => panic!("Expected Created result, got {:?}", result),
    }
}

// =============================================================================
// File Lifecycle Tests
// =============================================================================

#[test]
fn test_complete_file_lifecycle() {
    let mut sm = MetadataStateMachine::new();

    // 1. Create a file
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "document.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let file_inode = get_created_inode(&result);

    // 2. Write data (add chunks)
    let chunk1 = ChunkId::new();
    let chunk2 = ChunkId::new();

    sm.apply_op(MetadataOp::AddChunk {
        inode: file_inode,
        chunk_id: chunk1,
    });
    sm.apply_op(MetadataOp::AddChunk {
        inode: file_inode,
        chunk_id: chunk2,
    });

    // 3. Update file size
    sm.apply_op(MetadataOp::SetAttr {
        inode: file_inode,
        mode: None,
        uid: None,
        gid: None,
        size: Some(8 * 1024 * 1024), // 8 MB
        atime: None,
        mtime: None,
    });

    // 4. Verify file state
    let file = sm.get_inode(file_inode).unwrap();
    assert_eq!(file.chunks.len(), 2);
    assert_eq!(file.size, 8 * 1024 * 1024);

    // 5. Rename file
    sm.apply_op(MetadataOp::Rename {
        src_parent: ROOT_INODE,
        src_name: "document.txt".to_string(),
        dst_parent: ROOT_INODE,
        dst_name: "renamed_document.txt".to_string(),
    });

    // 6. Verify rename
    assert!(sm.lookup(ROOT_INODE, "document.txt").is_none());
    assert!(sm.lookup(ROOT_INODE, "renamed_document.txt").is_some());

    // 7. Delete file
    sm.apply_op(MetadataOp::Delete {
        parent: ROOT_INODE,
        name: "renamed_document.txt".to_string(),
    });

    // 8. Verify deletion
    assert!(sm.lookup(ROOT_INODE, "renamed_document.txt").is_none());
}

// =============================================================================
// Directory Hierarchy Tests
// =============================================================================

#[test]
fn test_deep_directory_hierarchy() {
    let mut sm = MetadataStateMachine::new();

    // Create a deep directory structure: /a/b/c/d/e
    let mut current_parent = ROOT_INODE;
    let dirs = ["a", "b", "c", "d", "e"];
    let mut dir_inodes = Vec::new();

    for name in &dirs {
        let result = sm.apply_op(MetadataOp::CreateDirectory {
            parent: current_parent,
            name: name.to_string(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        });
        let dir_inode = get_created_inode(&result);
        dir_inodes.push(dir_inode);
        current_parent = dir_inode;
    }

    // Create a file in the deepest directory
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: current_parent,
        name: "deep_file.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());

    // Verify we can look up through the hierarchy
    let mut lookup_parent = ROOT_INODE;
    for (i, name) in dirs.iter().enumerate() {
        let dir = sm.lookup(lookup_parent, name);
        assert!(dir.is_some(), "Failed to find directory {}", name);
        assert_eq!(dir.as_ref().unwrap().id, dir_inodes[i]);
        assert_eq!(dir.unwrap().file_type, FileType::Directory);
        lookup_parent = dir_inodes[i];
    }

    // Verify the file exists
    let file = sm.lookup(current_parent, "deep_file.txt");
    assert!(file.is_some());
    assert_eq!(file.unwrap().file_type, FileType::RegularFile);
}

#[test]
fn test_directory_with_many_entries() {
    let mut sm = MetadataStateMachine::new();

    // Create a directory with many files
    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "large_dir".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let dir_inode = get_created_inode(&result);

    let num_files = 100;
    let mut file_inodes = Vec::new();

    for i in 0..num_files {
        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: dir_inode,
            name: format!("file_{:04}.txt", i),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        file_inodes.push(get_created_inode(&result));
    }

    // List directory and verify all files
    let entries = sm.readdir(dir_inode).expect("Failed to read directory");
    let user_entries: Vec<_> = entries
        .iter()
        .filter(|(name, _, _)| name != "." && name != "..")
        .collect();

    assert_eq!(user_entries.len(), num_files);

    // Verify each file can be looked up
    for i in 0..num_files {
        let name = format!("file_{:04}.txt", i);
        let file = sm.lookup(dir_inode, &name);
        assert!(file.is_some(), "Failed to find {}", name);
        assert_eq!(file.unwrap().id, file_inodes[i]);
    }
}

// =============================================================================
// Hard Link and Symlink Tests
// =============================================================================

#[test]
fn test_hard_link_workflow() {
    let mut sm = MetadataStateMachine::new();

    // Create original file
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "original.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Add some data
    let chunk = ChunkId::new();
    sm.apply_op(MetadataOp::AddChunk {
        inode: file_inode,
        chunk_id: chunk,
    });

    // Create multiple hard links
    let link_names = ["link1.txt", "link2.txt", "link3.txt"];
    for name in &link_names {
        let result = sm.apply_op(MetadataOp::Link {
            parent: ROOT_INODE,
            name: name.to_string(),
            inode: file_inode,
        });
        assert!(result.is_success());
    }

    // All links should point to the same inode
    let original = sm.lookup(ROOT_INODE, "original.txt").unwrap();
    assert_eq!(original.nlink, 4); // original + 3 links

    for name in &link_names {
        let link = sm.lookup(ROOT_INODE, name).unwrap();
        assert_eq!(link.id, file_inode);
        assert_eq!(link.chunks.len(), 1);
        assert_eq!(link.chunks[0], chunk);
    }

    // Delete one link
    sm.apply_op(MetadataOp::Delete {
        parent: ROOT_INODE,
        name: "link1.txt".to_string(),
    });

    let remaining = sm.lookup(ROOT_INODE, "original.txt").unwrap();
    assert_eq!(remaining.nlink, 3);
}

#[test]
fn test_symlink_workflow() {
    let mut sm = MetadataStateMachine::new();

    // Create target file
    sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "target.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });

    // Create symlink
    let result = sm.apply_op(MetadataOp::CreateSymlink {
        parent: ROOT_INODE,
        name: "symlink".to_string(),
        target: "target.txt".to_string(),
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let symlink_inode = get_created_inode(&result);

    // Verify symlink properties
    let symlink = sm.lookup(ROOT_INODE, "symlink").unwrap();
    assert_eq!(symlink.id, symlink_inode);
    assert_eq!(symlink.file_type, FileType::Symlink);

    // Create symlink to directory
    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "target_dir".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());

    let result = sm.apply_op(MetadataOp::CreateSymlink {
        parent: ROOT_INODE,
        name: "dir_link".to_string(),
        target: "target_dir".to_string(),
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
}

// =============================================================================
// Move/Rename Operations
// =============================================================================

#[test]
fn test_cross_directory_move() {
    let mut sm = MetadataStateMachine::new();

    // Create source and destination directories
    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "src".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let src_dir = get_created_inode(&result);

    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "dst".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let dst_dir = get_created_inode(&result);

    // Create file in source directory
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: src_dir,
        name: "moveme.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Add chunk to file
    let chunk = ChunkId::new();
    sm.apply_op(MetadataOp::AddChunk {
        inode: file_inode,
        chunk_id: chunk,
    });

    // Move file to destination
    let result = sm.apply_op(MetadataOp::Rename {
        src_parent: src_dir,
        src_name: "moveme.txt".to_string(),
        dst_parent: dst_dir,
        dst_name: "moved.txt".to_string(),
    });
    assert!(result.is_success());

    // Verify file is gone from source
    assert!(sm.lookup(src_dir, "moveme.txt").is_none());

    // Verify file exists in destination with same data
    let moved = sm.lookup(dst_dir, "moved.txt");
    assert!(moved.is_some());
    let moved = moved.unwrap();
    assert_eq!(moved.id, file_inode);
    assert_eq!(moved.chunks.len(), 1);
    assert_eq!(moved.chunks[0], chunk);
}

#[test]
fn test_rename_overwrite() {
    let mut sm = MetadataStateMachine::new();

    // Create two files
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "source.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let source_inode = get_created_inode(&result);

    sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "target.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });

    // Add distinct chunk to source
    let source_chunk = ChunkId::new();
    sm.apply_op(MetadataOp::AddChunk {
        inode: source_inode,
        chunk_id: source_chunk,
    });

    // Rename source to target (overwrite)
    let result = sm.apply_op(MetadataOp::Rename {
        src_parent: ROOT_INODE,
        src_name: "source.txt".to_string(),
        dst_parent: ROOT_INODE,
        dst_name: "target.txt".to_string(),
    });
    assert!(result.is_success());

    // Source should be gone
    assert!(sm.lookup(ROOT_INODE, "source.txt").is_none());

    // Target should have source's inode and data
    let target = sm.lookup(ROOT_INODE, "target.txt").unwrap();
    assert_eq!(target.id, source_inode);
    assert_eq!(target.chunks.len(), 1);
    assert_eq!(target.chunks[0], source_chunk);
}

// =============================================================================
// Snapshot and Restore Tests
// =============================================================================

#[test]
fn test_snapshot_complex_state() {
    let mut sm = MetadataStateMachine::new();

    // Create a complex directory structure
    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "projects".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let projects = get_created_inode(&result);

    // Create files with chunks
    for i in 0..5 {
        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: projects,
            name: format!("file_{}.dat", i),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let inode = get_created_inode(&result);

        // Add chunks
        for _ in 0..3 {
            sm.apply_op(MetadataOp::AddChunk {
                inode,
                chunk_id: ChunkId::new(),
            });
        }
    }

    // Create symlink
    sm.apply_op(MetadataOp::CreateSymlink {
        parent: ROOT_INODE,
        name: "proj_link".to_string(),
        target: "projects".to_string(),
        uid: 1000,
        gid: 1000,
    });

    // Take snapshot
    let snapshot = sm.snapshot();

    // Restore to new state machine
    let mut sm2 = MetadataStateMachine::new();
    sm2.restore(&snapshot).expect("Failed to restore");

    // Verify structure matches
    assert!(sm2.lookup(ROOT_INODE, "projects").is_some());
    assert!(sm2.lookup(ROOT_INODE, "proj_link").is_some());

    let restored_projects = sm2.lookup(ROOT_INODE, "projects").unwrap();
    assert_eq!(restored_projects.id, projects);

    for i in 0..5 {
        let name = format!("file_{}.dat", i);
        let original = sm.lookup(projects, &name).unwrap();
        let restored = sm2.lookup(projects, &name).unwrap();

        assert_eq!(original.id, restored.id);
        assert_eq!(original.chunks.len(), restored.chunks.len());
    }
}

// =============================================================================
// Data Server Registration Tests
// =============================================================================

#[test]
fn test_data_server_registration_workflow() {
    let mut sm = MetadataStateMachine::new();

    // Register multiple data servers
    for i in 1..=5 {
        let result = sm.apply_op(MetadataOp::RegisterDataServer {
            server_id: i,
            address: format!("192.168.1.{}:9000", i),
            capacity: 1_000_000_000_000, // 1 TB
        });
        assert!(result.is_success());
    }

    // Verify all servers registered
    let servers = sm.get_data_servers();
    assert_eq!(servers.len(), 5);

    // Update status with varying usage
    for i in 1..=5 {
        let used = (i as u64) * 100_000_000_000; // 100 GB per server
        sm.apply_op(MetadataOp::UpdateDataServerStatus {
            server_id: i,
            used,
            online: true,
        });
    }

    // Deregister one server
    sm.apply_op(MetadataOp::DeregisterDataServer { server_id: 3 });

    let remaining = sm.get_data_servers();
    assert_eq!(remaining.len(), 4);
    assert!(!remaining.contains_key(&3));
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

#[test]
fn test_operations_on_nonexistent() {
    let mut sm = MetadataStateMachine::new();

    // Delete nonexistent file
    let result = sm.apply_op(MetadataOp::Delete {
        parent: ROOT_INODE,
        name: "nonexistent.txt".to_string(),
    });
    assert!(!result.is_success());

    // Rename nonexistent file
    let result = sm.apply_op(MetadataOp::Rename {
        src_parent: ROOT_INODE,
        src_name: "nonexistent.txt".to_string(),
        dst_parent: ROOT_INODE,
        dst_name: "still_nonexistent.txt".to_string(),
    });
    assert!(!result.is_success());

    // Link to nonexistent inode
    let result = sm.apply_op(MetadataOp::Link {
        parent: ROOT_INODE,
        name: "bad_link.txt".to_string(),
        inode: 99999,
    });
    assert!(!result.is_success());
}

#[test]
fn test_duplicate_creation() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "exists.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });

    // Try to create another file with same name
    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "exists.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    assert!(!result.is_success());

    // Same for directory
    sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "dir".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });

    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "dir".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    assert!(!result.is_success());
}

#[test]
fn test_special_characters_in_names() {
    let mut sm = MetadataStateMachine::new();

    let special_names = [
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
        "UPPERCASE.TXT",
        "MixedCase.Txt",
        "unicode_ファイル.txt",
        "emoji_📄.txt",
    ];

    for name in &special_names {
        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: ROOT_INODE,
            name: name.to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        assert!(result.is_success(), "Failed to create file: {}", name);

        let lookup = sm.lookup(ROOT_INODE, name);
        assert!(lookup.is_some(), "Failed to look up file: {}", name);
    }
}

// =============================================================================
// Performance-oriented Tests
// =============================================================================

#[test]
fn test_large_chunk_count() {
    let mut sm = MetadataStateMachine::new();

    let result = sm.apply_op(MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "huge_file.bin".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Add many chunks (simulating a very large file)
    let chunk_count = 1000;
    for _ in 0..chunk_count {
        sm.apply_op(MetadataOp::AddChunk {
            inode: file_inode,
            chunk_id: ChunkId::new(),
        });
    }

    let file = sm.get_inode(file_inode).unwrap();
    assert_eq!(file.chunks.len(), chunk_count);
}

#[test]
fn test_wide_directory() {
    let mut sm = MetadataStateMachine::new();

    let result = sm.apply_op(MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "wide".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let dir_inode = get_created_inode(&result);

    // Create many entries
    let entry_count = 500;
    for i in 0..entry_count {
        if i % 2 == 0 {
            sm.apply_op(MetadataOp::CreateFile {
                parent: dir_inode,
                name: format!("entry_{:05}", i),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            });
        } else {
            sm.apply_op(MetadataOp::CreateDirectory {
                parent: dir_inode,
                name: format!("entry_{:05}", i),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            });
        }
    }

    let entries = sm.readdir(dir_inode).expect("Failed to read directory");
    let user_entries: Vec<_> = entries
        .iter()
        .filter(|(name, _, _)| name != "." && name != "..")
        .collect();

    assert_eq!(user_entries.len(), entry_count);
}
