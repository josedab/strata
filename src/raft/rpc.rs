//! Raft RPC message definitions.

use super::LogEntry;
use crate::types::{LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};

/// Raft RPC messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Request vote from other nodes during election.
    RequestVote(RequestVoteRequest),
    /// Response to RequestVote.
    RequestVoteResponse(RequestVoteResponse),
    /// Append entries (heartbeat or log replication).
    AppendEntries(AppendEntriesRequest),
    /// Response to AppendEntries.
    AppendEntriesResponse(AppendEntriesResponse),
    /// Install snapshot for slow followers.
    InstallSnapshot(InstallSnapshotRequest),
    /// Response to InstallSnapshot.
    InstallSnapshotResponse(InstallSnapshotResponse),
}

/// RequestVote RPC arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Candidate's term.
    pub term: Term,
    /// Candidate requesting vote.
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry.
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: Term,
    /// True if candidate received vote.
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: Term,
    /// Leader's ID so followers can redirect clients.
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: LogIndex,
    /// Term of prev_log_index entry.
    pub prev_log_term: Term,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: Term,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
    /// The index of the last entry that was replicated (for optimization).
    pub match_index: LogIndex,
    /// Hint for the leader about where to retry (for optimization).
    pub conflict_index: Option<LogIndex>,
    /// Term of the conflicting entry (for optimization).
    pub conflict_term: Option<Term>,
}

/// InstallSnapshot RPC arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Leader's term.
    pub term: Term,
    /// Leader's ID.
    pub leader_id: NodeId,
    /// The snapshot replaces all entries up through and including this index.
    pub last_included_index: LogIndex,
    /// Term of last_included_index.
    pub last_included_term: Term,
    /// Byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// Raw bytes of the snapshot chunk.
    pub data: Vec<u8>,
    /// True if this is the last chunk.
    pub done: bool,
}

/// InstallSnapshot RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// Current term, for leader to update itself.
    pub term: Term,
}

/// Trait for Raft RPC transport.
#[async_trait::async_trait]
pub trait RaftRpc: Send + Sync {
    /// Send RequestVote to a peer.
    async fn request_vote(
        &self,
        target: NodeId,
        request: RequestVoteRequest,
    ) -> crate::Result<RequestVoteResponse>;

    /// Send AppendEntries to a peer.
    async fn append_entries(
        &self,
        target: NodeId,
        request: AppendEntriesRequest,
    ) -> crate::Result<AppendEntriesResponse>;

    /// Send InstallSnapshot to a peer.
    async fn install_snapshot(
        &self,
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> crate::Result<InstallSnapshotResponse>;
}

/// In-memory RPC implementation for testing.
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    type ResponseHandler = Box<dyn Fn(RaftMessage) -> RaftMessage + Send + Sync>;

    pub struct MockRpc {
        handlers: Arc<Mutex<HashMap<NodeId, ResponseHandler>>>,
    }

    impl MockRpc {
        pub fn new() -> Self {
            Self {
                handlers: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub async fn register_handler<F>(&self, node_id: NodeId, handler: F)
        where
            F: Fn(RaftMessage) -> RaftMessage + Send + Sync + 'static,
        {
            self.handlers.lock().await.insert(node_id, Box::new(handler));
        }
    }

    #[async_trait::async_trait]
    impl RaftRpc for MockRpc {
        async fn request_vote(
            &self,
            target: NodeId,
            request: RequestVoteRequest,
        ) -> crate::Result<RequestVoteResponse> {
            let handlers = self.handlers.lock().await;
            let handler = handlers.get(&target).ok_or_else(|| {
                crate::StrataError::NodeNotFound(target)
            })?;

            match handler(RaftMessage::RequestVote(request)) {
                RaftMessage::RequestVoteResponse(resp) => Ok(resp),
                _ => Err(crate::StrataError::Internal("Unexpected response".into())),
            }
        }

        async fn append_entries(
            &self,
            target: NodeId,
            request: AppendEntriesRequest,
        ) -> crate::Result<AppendEntriesResponse> {
            let handlers = self.handlers.lock().await;
            let handler = handlers.get(&target).ok_or_else(|| {
                crate::StrataError::NodeNotFound(target)
            })?;

            match handler(RaftMessage::AppendEntries(request)) {
                RaftMessage::AppendEntriesResponse(resp) => Ok(resp),
                _ => Err(crate::StrataError::Internal("Unexpected response".into())),
            }
        }

        async fn install_snapshot(
            &self,
            target: NodeId,
            request: InstallSnapshotRequest,
        ) -> crate::Result<InstallSnapshotResponse> {
            let handlers = self.handlers.lock().await;
            let handler = handlers.get(&target).ok_or_else(|| {
                crate::StrataError::NodeNotFound(target)
            })?;

            match handler(RaftMessage::InstallSnapshot(request)) {
                RaftMessage::InstallSnapshotResponse(resp) => Ok(resp),
                _ => Err(crate::StrataError::Internal("Unexpected response".into())),
            }
        }
    }
}
