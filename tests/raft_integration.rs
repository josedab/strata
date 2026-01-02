//! Raft consensus integration tests.
//!
//! Tests for Raft log, storage, state machine, and message handling.

#[allow(dead_code)]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, RwLock};

use strata::error::{Result, StrataError};
use strata::raft::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, RaftCommand, RaftConfig, RaftLog,
    RaftNode, RaftRpc, RaftStorage, RequestVoteRequest, RequestVoteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, StateMachine,
};
use strata::types::NodeId;

// =============================================================================
// Test State Machine
// =============================================================================

/// Simple counter state machine for testing.
#[derive(Debug, Clone, Default)]
struct CounterStateMachine {
    counter: i64,
    applied_commands: Vec<Vec<u8>>,
}

impl StateMachine for CounterStateMachine {
    type Result = i64;

    fn apply(&mut self, command: &[u8]) -> Self::Result {
        self.applied_commands.push(command.to_vec());
        // Simple increment/decrement protocol
        if !command.is_empty() {
            match command[0] {
                b'+' => self.counter += 1,
                b'-' => self.counter -= 1,
                _ => {}
            }
        }
        self.counter
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&(self.counter, self.applied_commands.clone()))
            .unwrap_or_default()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        let (counter, commands): (i64, Vec<Vec<u8>>) = bincode::deserialize(snapshot)
            .map_err(|e| StrataError::Internal(format!("Failed to restore snapshot: {}", e)))?;
        self.counter = counter;
        self.applied_commands = commands;
        Ok(())
    }
}

// =============================================================================
// Simple Mock RPC for basic tests
// =============================================================================

/// Simple RPC that returns canned responses for basic testing.
struct SimpleTestRpc;

#[async_trait::async_trait]
impl RaftRpc for SimpleTestRpc {
    async fn request_vote(
        &self,
        _target: NodeId,
        _request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        // Always grant vote for testing
        Ok(RequestVoteResponse {
            term: 1,
            vote_granted: true,
        })
    }

    async fn append_entries(
        &self,
        _target: NodeId,
        _request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        // Always succeed for testing
        Ok(AppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 0,
            conflict_index: None,
            conflict_term: None,
        })
    }

    async fn install_snapshot(
        &self,
        _target: NodeId,
        _request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        Ok(InstallSnapshotResponse { term: 1 })
    }
}

// =============================================================================
// Raft Log Tests
// =============================================================================

#[test]
fn test_raft_log_empty() {
    let log = RaftLog::new();
    assert!(log.is_empty());
    assert_eq!(log.last_index(), 0);
    assert_eq!(log.last_term(), 0);
    assert_eq!(log.first_index(), 1);
}

#[test]
fn test_raft_log_append() {
    let mut log = RaftLog::new();

    // Append entries
    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(1, 2, vec![2])).unwrap();
    log.append(LogEntry::new(2, 3, vec![3])).unwrap();

    assert_eq!(log.len(), 3);
    assert_eq!(log.last_index(), 3);
    assert_eq!(log.last_term(), 2);
}

#[test]
fn test_raft_log_get_entry() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![10])).unwrap();
    log.append(LogEntry::new(2, 2, vec![20])).unwrap();

    // Get existing entries
    let entry1 = log.get(1).unwrap();
    assert_eq!(entry1.term, 1);
    assert_eq!(entry1.data_bytes(), &[10]);

    let entry2 = log.get(2).unwrap();
    assert_eq!(entry2.term, 2);
    assert_eq!(entry2.data_bytes(), &[20]);

    // Non-existent entries
    assert!(log.get(0).is_none());
    assert!(log.get(3).is_none());
}

#[test]
fn test_raft_log_truncate() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(1, 2, vec![2])).unwrap();
    log.append(LogEntry::new(1, 3, vec![3])).unwrap();

    log.truncate_from(2);

    assert_eq!(log.len(), 1);
    assert_eq!(log.last_index(), 1);
    assert!(log.get(2).is_none());
}

#[test]
fn test_raft_log_matches() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(2, 2, vec![2])).unwrap();

    // Should match at index 0 (before log)
    assert!(log.matches(0, 0));

    // Should match at existing entries
    assert!(log.matches(1, 1));
    assert!(log.matches(2, 2));

    // Should not match with wrong term
    assert!(!log.matches(2, 1));
    assert!(!log.matches(1, 2));

    // Should not match beyond log
    assert!(!log.matches(3, 2));
}

#[test]
fn test_raft_log_entries_from() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(1, 2, vec![2])).unwrap();
    log.append(LogEntry::new(2, 3, vec![3])).unwrap();

    // Get all entries
    let entries = log.entries_from(1);
    assert_eq!(entries.len(), 3);

    // Get from middle
    let entries = log.entries_from(2);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].index, 2);

    // Get from end
    let entries = log.entries_from(3);
    assert_eq!(entries.len(), 1);

    // Get beyond end
    let entries = log.entries_from(4);
    assert!(entries.is_empty());
}

#[test]
fn test_raft_log_is_up_to_date() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(2, 2, vec![2])).unwrap();

    // Higher term is always more up-to-date
    assert!(log.is_up_to_date(1, 3));

    // Same term, higher index
    assert!(log.is_up_to_date(3, 2));

    // Same term, same index
    assert!(log.is_up_to_date(2, 2));

    // Lower term is never up-to-date
    assert!(!log.is_up_to_date(100, 1));
}

#[test]
fn test_raft_log_compact() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();
    log.append(LogEntry::new(1, 2, vec![2])).unwrap();
    log.append(LogEntry::new(2, 3, vec![3])).unwrap();
    log.append(LogEntry::new(2, 4, vec![4])).unwrap();

    // Compact up to index 2
    log.compact(2, 1);

    assert_eq!(log.first_index(), 3);
    assert_eq!(log.len(), 2);
    assert!(log.get(2).is_none());
    assert!(log.get(3).is_some());
}

// =============================================================================
// Raft Storage Tests
// =============================================================================

#[test]
fn test_raft_storage_persistent_state() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::open(temp_dir.path().join("raft")).unwrap();

    // Initially empty
    assert!(storage.load_persistent_state().unwrap().is_none());

    // Save state
    let state = strata::raft::PersistentState {
        current_term: 5,
        voted_for: Some(3),
    };
    storage.save_persistent_state(&state).unwrap();

    // Load state
    let loaded = storage.load_persistent_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 5);
    assert_eq!(loaded.voted_for, Some(3));
}

#[test]
fn test_raft_storage_log_entries() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::open(temp_dir.path().join("raft")).unwrap();

    let entries = vec![
        LogEntry::new(1, 1, vec![1, 2, 3]),
        LogEntry::new(1, 2, vec![4, 5, 6]),
        LogEntry::new(2, 3, vec![7, 8, 9]),
    ];

    storage.append_log_entries(&entries).unwrap();

    // Load single entry
    let entry = storage.load_log_entry(2).unwrap().unwrap();
    assert_eq!(entry.term, 1);
    assert_eq!(entry.data_bytes(), &[4, 5, 6]);

    // Load all entries
    let loaded = storage.load_log_entries_from(1).unwrap();
    assert_eq!(loaded.len(), 3);

    // Get bounds
    let bounds = storage.get_log_bounds().unwrap().unwrap();
    assert_eq!(bounds, (1, 3));
}

#[test]
fn test_raft_storage_truncate() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::open(temp_dir.path().join("raft")).unwrap();

    let entries = vec![
        LogEntry::new(1, 1, vec![1]),
        LogEntry::new(1, 2, vec![2]),
        LogEntry::new(1, 3, vec![3]),
    ];

    storage.append_log_entries(&entries).unwrap();
    storage.truncate_log_from(2).unwrap();

    let loaded = storage.load_log_entries_from(1).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].index, 1);
}

#[test]
fn test_raft_storage_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::open(temp_dir.path().join("raft")).unwrap();

    let snapshot_data = b"test snapshot data".to_vec();
    storage.save_snapshot(&snapshot_data, 10, 5).unwrap();

    let (loaded_data, meta) = storage.load_snapshot().unwrap().unwrap();
    assert_eq!(loaded_data, snapshot_data);
    assert_eq!(meta.last_index, 10);
    assert_eq!(meta.last_term, 5);
}

#[test]
fn test_raft_storage_compact_log() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::open(temp_dir.path().join("raft")).unwrap();

    let entries = vec![
        LogEntry::new(1, 1, vec![1]),
        LogEntry::new(1, 2, vec![2]),
        LogEntry::new(1, 3, vec![3]),
        LogEntry::new(1, 4, vec![4]),
        LogEntry::new(1, 5, vec![5]),
    ];

    storage.append_log_entries(&entries).unwrap();
    storage.compact_log(3).unwrap();

    // Entries 1-3 should be removed
    assert!(storage.load_log_entry(1).unwrap().is_none());
    assert!(storage.load_log_entry(2).unwrap().is_none());
    assert!(storage.load_log_entry(3).unwrap().is_none());

    // Entries 4-5 should remain
    assert!(storage.load_log_entry(4).unwrap().is_some());
    assert!(storage.load_log_entry(5).unwrap().is_some());
}

// =============================================================================
// State Machine Tests
// =============================================================================

#[test]
fn test_state_machine_apply() {
    let mut sm = CounterStateMachine::default();

    assert_eq!(sm.counter, 0);

    sm.apply(b"+");
    assert_eq!(sm.counter, 1);

    sm.apply(b"+");
    assert_eq!(sm.counter, 2);

    sm.apply(b"-");
    assert_eq!(sm.counter, 1);
}

#[test]
fn test_state_machine_snapshot_restore() {
    let mut sm = CounterStateMachine::default();

    // Apply some commands
    sm.apply(b"+");
    sm.apply(b"+");
    sm.apply(b"-");
    assert_eq!(sm.counter, 1);
    assert_eq!(sm.applied_commands.len(), 3);

    // Create snapshot
    let snapshot = sm.snapshot();

    // Restore to new state machine
    let mut sm2 = CounterStateMachine::default();
    sm2.restore(&snapshot).unwrap();

    assert_eq!(sm2.counter, 1);
    assert_eq!(sm2.applied_commands.len(), 3);
}

// =============================================================================
// LogEntry Arc Sharing Tests
// =============================================================================

#[test]
fn test_log_entry_arc_cloning() {
    use std::sync::Arc;

    // Create entry with large data
    let data = vec![0u8; 1024 * 1024]; // 1MB
    let entry = LogEntry::new(1, 1, data);

    // Clone should be cheap (Arc)
    let cloned = entry.clone();

    // Both should reference the same underlying data
    assert!(Arc::ptr_eq(&entry.data, &cloned.data));
}

#[test]
fn test_log_entry_with_arc_data() {
    use std::sync::Arc;

    let data = Arc::new(vec![1, 2, 3, 4, 5]);
    let entry = LogEntry::with_arc_data(1, 1, data.clone());

    // Should use the same Arc
    assert!(Arc::ptr_eq(&entry.data, &data));
}

#[test]
fn test_log_entry_serialization() {
    let entry = LogEntry::new(5, 100, vec![1, 2, 3, 4, 5]);

    // Serialize and deserialize
    let bytes = bincode::serialize(&entry).unwrap();
    let deserialized: LogEntry = bincode::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.term, 5);
    assert_eq!(deserialized.index, 100);
    assert_eq!(deserialized.data_bytes(), &[1, 2, 3, 4, 5]);
}

// =============================================================================
// RaftNode Creation Tests
// =============================================================================

#[tokio::test]
async fn test_raft_node_creation() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("raft");

    let config = RaftConfig {
        node_id: 1,
        peers: HashMap::new(),
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        max_entries_per_append: 100,
        snapshot_threshold: 10000,
    };

    let state_machine = CounterStateMachine::default();
    let rpc = Arc::new(SimpleTestRpc);

    let result = RaftNode::new(config, storage_path, state_machine, rpc);
    assert!(result.is_ok());

    let (node, _rx) = result.unwrap();

    // Get command sender
    let sender = node.command_sender();

    // Should be able to send IsLeader command
    let (tx, rx) = oneshot::channel();
    sender
        .send(RaftCommand::IsLeader { response: tx })
        .await
        .unwrap();

    // Without running the node, this will timeout, but sender should work
    tokio::time::timeout(Duration::from_millis(100), rx)
        .await
        .ok();
}

#[tokio::test]
async fn test_raft_node_with_peers() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("raft");

    let mut peers = HashMap::new();
    peers.insert(2, "127.0.0.1:5002".to_string());
    peers.insert(3, "127.0.0.1:5003".to_string());

    let config = RaftConfig {
        node_id: 1,
        peers,
        ..RaftConfig::default()
    };

    let state_machine = CounterStateMachine::default();
    let rpc = Arc::new(SimpleTestRpc);

    let result = RaftNode::new(config, storage_path, state_machine, rpc);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_raft_node_loads_existing_state() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("raft");

    // First, create storage with state
    {
        let storage = RaftStorage::open(&storage_path).unwrap();
        let state = strata::raft::PersistentState {
            current_term: 10,
            voted_for: Some(2),
        };
        storage.save_persistent_state(&state).unwrap();

        let entries = vec![
            LogEntry::new(10, 1, vec![1]),
            LogEntry::new(10, 2, vec![2]),
        ];
        storage.append_log_entries(&entries).unwrap();
    }

    // Now create node - it should load existing state
    let config = RaftConfig {
        node_id: 1,
        peers: HashMap::new(),
        ..RaftConfig::default()
    };

    let state_machine = CounterStateMachine::default();
    let rpc = Arc::new(SimpleTestRpc);

    let result = RaftNode::new(config, storage_path, state_machine, rpc);
    assert!(result.is_ok());
}

// =============================================================================
// RPC Message Tests
// =============================================================================

#[test]
fn test_request_vote_request_serialization() {
    let request = RequestVoteRequest {
        term: 5,
        candidate_id: 2,
        last_log_index: 100,
        last_log_term: 4,
    };

    let bytes = bincode::serialize(&request).unwrap();
    let deserialized: RequestVoteRequest = bincode::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.term, 5);
    assert_eq!(deserialized.candidate_id, 2);
    assert_eq!(deserialized.last_log_index, 100);
    assert_eq!(deserialized.last_log_term, 4);
}

#[test]
fn test_append_entries_request_serialization() {
    let entries = vec![
        LogEntry::new(5, 101, vec![1, 2, 3]),
        LogEntry::new(5, 102, vec![4, 5, 6]),
    ];

    let request = AppendEntriesRequest {
        term: 5,
        leader_id: 1,
        prev_log_index: 100,
        prev_log_term: 4,
        entries,
        leader_commit: 99,
    };

    let bytes = bincode::serialize(&request).unwrap();
    let deserialized: AppendEntriesRequest = bincode::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.term, 5);
    assert_eq!(deserialized.leader_id, 1);
    assert_eq!(deserialized.entries.len(), 2);
    assert_eq!(deserialized.entries[0].data_bytes(), &[1, 2, 3]);
}

#[test]
fn test_append_entries_response_serialization() {
    let response = AppendEntriesResponse {
        term: 5,
        success: true,
        match_index: 102,
        conflict_index: Some(50),
        conflict_term: Some(3),
    };

    let bytes = bincode::serialize(&response).unwrap();
    let deserialized: AppendEntriesResponse = bincode::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.term, 5);
    assert!(deserialized.success);
    assert_eq!(deserialized.match_index, 102);
    assert_eq!(deserialized.conflict_index, Some(50));
    assert_eq!(deserialized.conflict_term, Some(3));
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_log_empty_entry() {
    let mut log = RaftLog::new();

    // Empty data should work
    log.append(LogEntry::new(1, 1, vec![])).unwrap();

    let entry = log.get(1).unwrap();
    assert!(entry.data_bytes().is_empty());
}

#[test]
fn test_log_large_entry() {
    let mut log = RaftLog::new();

    // Large data should work
    let large_data = vec![0u8; 1024 * 1024]; // 1MB
    log.append(LogEntry::new(1, 1, large_data.clone())).unwrap();

    let entry = log.get(1).unwrap();
    assert_eq!(entry.data_bytes().len(), 1024 * 1024);
}

#[test]
fn test_storage_persistence_across_reopens() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("raft");

    // Write data
    {
        let storage = RaftStorage::open(&path).unwrap();
        let state = strata::raft::PersistentState {
            current_term: 42,
            voted_for: Some(7),
        };
        storage.save_persistent_state(&state).unwrap();
    }

    // Reopen and verify
    {
        let storage = RaftStorage::open(&path).unwrap();
        let loaded = storage.load_persistent_state().unwrap().unwrap();
        assert_eq!(loaded.current_term, 42);
        assert_eq!(loaded.voted_for, Some(7));
    }
}

#[test]
fn test_log_term_at_boundary() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();

    // term_at(0) should return 0 (before log starts)
    assert_eq!(log.term_at(0), Some(0));

    // term_at(1) should return the entry's term
    assert_eq!(log.term_at(1), Some(1));

    // term_at(2) should return None (beyond log)
    assert_eq!(log.term_at(2), None);
}

#[test]
fn test_log_entries_range() {
    let mut log = RaftLog::new();

    for i in 1..=10 {
        log.append(LogEntry::new(1, i, vec![i as u8])).unwrap();
    }

    // Get range [3, 7]
    let entries = log.entries_range(3, 7);
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].index, 3);
    assert_eq!(entries[4].index, 7);

    // Empty range
    let entries = log.entries_range(10, 5);
    assert!(entries.is_empty());
}

#[test]
fn test_state_machine_empty_command() {
    let mut sm = CounterStateMachine::default();

    // Empty command should not change counter
    sm.apply(&[]);
    assert_eq!(sm.counter, 0);

    // But should still be recorded
    assert_eq!(sm.applied_commands.len(), 1);
}

#[test]
fn test_log_create_entries() {
    let log = RaftLog::new();

    let commands = vec![vec![1], vec![2], vec![3]];
    let entries = log.create_entries(5, commands);

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].term, 5);
    assert_eq!(entries[0].index, 1);
    assert_eq!(entries[1].index, 2);
    assert_eq!(entries[2].index, 3);
}

#[test]
fn test_log_sequential_index_enforcement() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(1, 1, vec![1])).unwrap();

    // Trying to append non-sequential index should fail
    let result = log.append(LogEntry::new(1, 3, vec![3]));
    assert!(result.is_err());

    // Sequential should work
    let result = log.append(LogEntry::new(1, 2, vec![2]));
    assert!(result.is_ok());
}
