//! Integration tests for metadata operations.

#[allow(dead_code)]
mod common;

use strata::metadata::{MetadataOp, MetadataStateMachine, OpResult};
use strata::raft::StateMachine;
use strata::types::{FileType, InodeId};

const ROOT_INODE: InodeId = 1;

/// Helper to extract created inode ID from OpResult
fn get_created_inode(result: &OpResult) -> InodeId {
    match result {
        OpResult::Created { inode } => *inode,
        _ => panic!("Expected Created result, got {:?}", result),
    }
}

#[test]
fn test_state_machine_file_operations() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "test.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let file_inode = get_created_inode(&result);

    assert!(file_inode > ROOT_INODE);

    // Lookup the file
    let inode = sm.lookup(ROOT_INODE, "test.txt");
    assert!(inode.is_some());
    let inode = inode.unwrap();
    assert_eq!(inode.id, file_inode);
    assert_eq!(inode.file_type, FileType::RegularFile);
    // Mode includes file type bits (S_IFREG)
    assert_eq!(inode.mode & 0o777, 0o644);

    // Create a directory
    let result = sm.apply_op(&MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "subdir".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let dir_inode = get_created_inode(&result);

    // Create a file in the subdirectory
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: dir_inode,
        name: "nested.txt".to_string(),
        mode: 0o600,
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let nested_file = get_created_inode(&result);

    // Verify nested lookup
    let nested = sm.lookup(dir_inode, "nested.txt");
    assert!(nested.is_some());
    assert_eq!(nested.unwrap().id, nested_file);
}

#[test]
fn test_state_machine_directory_listing() {
    let mut sm = MetadataStateMachine::new();

    // Create multiple files
    sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "file1.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "file2.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    sm.apply_op(&MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "dir1".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });

    // List directory
    let entries = sm.readdir(ROOT_INODE).expect("Failed to read directory");

    // Filter out . and .. entries for comparison
    let user_entries: Vec<_> = entries
        .iter()
        .filter(|(name, _, _)| name != "." && name != "..")
        .collect();

    // Should have 3 user entries
    assert_eq!(user_entries.len(), 3);

    let names: Vec<&str> = user_entries.iter().map(|(name, _, _)| name.as_str()).collect();
    assert!(names.contains(&"file1.txt"));
    assert!(names.contains(&"file2.txt"));
    assert!(names.contains(&"dir1"));
}

#[test]
fn test_state_machine_rename() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "original.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Create a target directory
    let result = sm.apply_op(&MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "target".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });
    let target_dir = get_created_inode(&result);

    // Rename to the same directory
    let result = sm.apply_op(&MetadataOp::Rename {
        src_parent: ROOT_INODE,
        src_name: "original.txt".to_string(),
        dst_parent: ROOT_INODE,
        dst_name: "renamed.txt".to_string(),
    });
    assert!(result.is_success());

    // Old name should not exist
    assert!(sm.lookup(ROOT_INODE, "original.txt").is_none());

    // New name should exist with same inode
    let renamed = sm.lookup(ROOT_INODE, "renamed.txt");
    assert!(renamed.is_some());
    assert_eq!(renamed.unwrap().id, file_inode);

    // Move to different directory
    let result = sm.apply_op(&MetadataOp::Rename {
        src_parent: ROOT_INODE,
        src_name: "renamed.txt".to_string(),
        dst_parent: target_dir,
        dst_name: "moved.txt".to_string(),
    });
    assert!(result.is_success());

    assert!(sm.lookup(ROOT_INODE, "renamed.txt").is_none());
    assert!(sm.lookup(target_dir, "moved.txt").is_some());
}

#[test]
fn test_state_machine_delete() {
    let mut sm = MetadataStateMachine::new();

    // Create files
    sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "to_delete.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "to_keep.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });

    // Delete one file
    let result = sm.apply_op(&MetadataOp::Delete {
        parent: ROOT_INODE,
        name: "to_delete.txt".to_string(),
    });
    assert!(result.is_success());

    // Verify deletion
    assert!(sm.lookup(ROOT_INODE, "to_delete.txt").is_none());
    assert!(sm.lookup(ROOT_INODE, "to_keep.txt").is_some());
}

#[test]
fn test_state_machine_hard_link() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "original.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Create a hard link
    let result = sm.apply_op(&MetadataOp::Link {
        parent: ROOT_INODE,
        name: "link.txt".to_string(),
        inode: file_inode,
    });
    assert!(result.is_success());

    // Both names should resolve to the same inode
    let original = sm.lookup(ROOT_INODE, "original.txt").unwrap();
    let link = sm.lookup(ROOT_INODE, "link.txt").unwrap();

    assert_eq!(original.id, link.id);
    assert_eq!(original.nlink, 2);
}

#[test]
fn test_state_machine_symlink() {
    let mut sm = MetadataStateMachine::new();

    // Create a symlink
    let result = sm.apply_op(&MetadataOp::CreateSymlink {
        parent: ROOT_INODE,
        name: "mylink".to_string(),
        target: "/path/to/target".to_string(),
        uid: 1000,
        gid: 1000,
    });
    assert!(result.is_success());
    let symlink_inode = get_created_inode(&result);

    // Lookup should return the symlink
    let inode = sm.lookup(ROOT_INODE, "mylink").unwrap();
    assert_eq!(inode.id, symlink_inode);
    assert_eq!(inode.file_type, FileType::Symlink);
}

#[test]
fn test_state_machine_snapshot_restore() {
    let mut sm = MetadataStateMachine::new();

    // Create some state
    sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "file1.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    sm.apply_op(&MetadataOp::CreateDirectory {
        parent: ROOT_INODE,
        name: "dir1".to_string(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    });

    // Take a snapshot
    let snapshot = sm.snapshot();
    assert!(!snapshot.is_empty());

    // Create a new state machine and restore
    let mut sm2 = MetadataStateMachine::new();
    sm2.restore(&snapshot).expect("Failed to restore snapshot");

    // Verify state was restored
    assert!(sm2.lookup(ROOT_INODE, "file1.txt").is_some());
    assert!(sm2.lookup(ROOT_INODE, "dir1").is_some());
}

#[test]
fn test_state_machine_chunk_operations() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "chunked.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Get the file and verify no chunks initially
    let file = sm.get_inode(file_inode).unwrap();
    assert!(file.chunks.is_empty());

    // Add a chunk
    let chunk_id = strata::types::ChunkId::new();
    let result = sm.apply_op(&MetadataOp::AddChunk {
        inode: file_inode,
        chunk_id,
    });
    assert!(result.is_success());

    // Verify chunk was added
    let file = sm.get_inode(file_inode).unwrap();
    assert_eq!(file.chunks.len(), 1);
    assert_eq!(file.chunks[0], chunk_id);
}

#[test]
fn test_state_machine_setattr() {
    let mut sm = MetadataStateMachine::new();

    // Create a file
    let result = sm.apply_op(&MetadataOp::CreateFile {
        parent: ROOT_INODE,
        name: "attrs.txt".to_string(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    });
    let file_inode = get_created_inode(&result);

    // Update attributes
    let result = sm.apply_op(&MetadataOp::SetAttr {
        inode: file_inode,
        mode: Some(0o755),
        uid: Some(500),
        gid: Some(500),
        size: None,
        atime: None,
        mtime: None,
    });
    assert!(result.is_success());

    // Verify attributes were updated (mode includes file type bits)
    let file = sm.get_inode(file_inode).unwrap();
    assert_eq!(file.mode & 0o777, 0o755);
    assert_eq!(file.uid, 500);
    assert_eq!(file.gid, 500);
}

#[test]
fn test_state_machine_data_server_registration() {
    let mut sm = MetadataStateMachine::new();

    // Register a data server
    let result = sm.apply_op(&MetadataOp::RegisterDataServer {
        server_id: 1,
        address: "127.0.0.1:9001".to_string(),
        capacity: 1_000_000_000,
    });
    assert!(result.is_success());

    // Verify server is registered
    let servers = sm.get_data_servers();
    assert_eq!(servers.len(), 1);
    assert!(servers.contains_key(&1));

    // Update status
    let result = sm.apply_op(&MetadataOp::UpdateDataServerStatus {
        server_id: 1,
        used: 500_000_000,
        online: true,
    });
    assert!(result.is_success());

    // Deregister
    let result = sm.apply_op(&MetadataOp::DeregisterDataServer { server_id: 1 });
    assert!(result.is_success());

    let servers = sm.get_data_servers();
    assert!(servers.is_empty());
}
