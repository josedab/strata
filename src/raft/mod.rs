//! Raft consensus implementation for Strata metadata cluster.
//!
//! This module implements the Raft consensus algorithm for maintaining
//! consistent metadata across the cluster. It handles leader election,
//! log replication, and state machine management.

// Deny unsafe code patterns in this critical consensus module.
// unwrap() calls can cause panics that break consensus.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod log;
mod node;
pub mod rpc;
mod state;
mod storage;

pub use log::{LogEntry, RaftLog};
pub use node::{RaftNode, RaftConfig, RaftCommand};
pub use rpc::{
    RaftMessage, RaftRpc, RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse,
};
pub use state::{RaftState, NodeState, PersistentState};
pub use storage::RaftStorage;

use crate::types::NodeId;
use serde::{Deserialize, Serialize};

/// Raft operation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResult {
    Success,
    LeaderRedirect { leader_id: Option<NodeId> },
    Error(String),
}

/// Trait for state machines that can be driven by Raft.
pub trait StateMachine: Send + Sync {
    /// The result type for operations.
    type Result: Send + Clone;

    /// Apply a committed command to the state machine.
    fn apply(&mut self, command: &[u8]) -> Self::Result;

    /// Create a snapshot of the current state.
    fn snapshot(&self) -> Vec<u8>;

    /// Restore state from a snapshot.
    fn restore(&mut self, snapshot: &[u8]) -> crate::Result<()>;
}
