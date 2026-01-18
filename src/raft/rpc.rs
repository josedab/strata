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
    /// Transfer leadership to another node.
    TransferLeadership(TransferLeadershipRequest),
    /// Response to TransferLeadership.
    TransferLeadershipResponse(TransferLeadershipResponse),
    /// Timeout now - force immediate election.
    TimeoutNow(TimeoutNowRequest),
    /// Response to TimeoutNow.
    TimeoutNowResponse(TimeoutNowResponse),
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
    /// Byte offset for the next expected chunk (for streaming).
    pub next_offset: u64,
    /// Whether the snapshot was fully received and applied.
    pub done: bool,
}

/// TransferLeadership RPC arguments.
/// Used to gracefully transfer leadership to another node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferLeadershipRequest {
    /// Current leader's term.
    pub term: Term,
    /// Current leader's ID.
    pub leader_id: NodeId,
    /// Target node to transfer leadership to.
    pub target_id: NodeId,
}

/// TransferLeadership RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferLeadershipResponse {
    /// Current term.
    pub term: Term,
    /// Whether the transfer was initiated successfully.
    pub success: bool,
    /// Error message if transfer failed.
    pub error: Option<String>,
}

/// TimeoutNow RPC arguments.
/// Sent by leader to trigger immediate election on target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutNowRequest {
    /// Leader's term.
    pub term: Term,
    /// Leader's ID.
    pub leader_id: NodeId,
}

/// TimeoutNow RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutNowResponse {
    /// Current term.
    pub term: Term,
}

/// Membership change type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChangeType {
    /// Add a new node to the cluster.
    AddNode,
    /// Remove a node from the cluster.
    RemoveNode,
    /// Add a non-voting learner node.
    AddLearner,
    /// Promote a learner to voting member.
    PromoteLearner,
}

/// Membership change entry stored in the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChange {
    /// Type of membership change.
    pub change_type: MembershipChangeType,
    /// Node ID being changed.
    pub node_id: NodeId,
    /// Node address for AddNode/AddLearner operations.
    pub node_addr: Option<String>,
}

/// ReadIndex RPC for linearizable reads.
/// Leader confirms it's still the leader before serving a read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadIndexRequest {
    /// Client-provided read request ID for correlation.
    pub request_id: u64,
}

/// ReadIndex RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadIndexResponse {
    /// Current term.
    pub term: Term,
    /// Whether the node is still leader.
    pub success: bool,
    /// Read index - the commit index at which the read can be served.
    pub read_index: LogIndex,
    /// Request ID for correlation.
    pub request_id: u64,
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

    /// Send TimeoutNow to trigger immediate election.
    async fn timeout_now(
        &self,
        target: NodeId,
        request: TimeoutNowRequest,
    ) -> crate::Result<TimeoutNowResponse>;

    /// Request a read index for linearizable reads.
    async fn read_index(
        &self,
        target: NodeId,
        request: ReadIndexRequest,
    ) -> crate::Result<ReadIndexResponse>;
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

        async fn timeout_now(
            &self,
            target: NodeId,
            request: TimeoutNowRequest,
        ) -> crate::Result<TimeoutNowResponse> {
            let handlers = self.handlers.lock().await;
            let handler = handlers.get(&target).ok_or_else(|| {
                crate::StrataError::NodeNotFound(target)
            })?;

            match handler(RaftMessage::TimeoutNow(request)) {
                RaftMessage::TimeoutNowResponse(resp) => Ok(resp),
                _ => Err(crate::StrataError::Internal("Unexpected response".into())),
            }
        }

        async fn read_index(
            &self,
            _target: NodeId,
            request: ReadIndexRequest,
        ) -> crate::Result<ReadIndexResponse> {
            // Mock implementation - return success with commit index 0
            Ok(ReadIndexResponse {
                term: 0,
                success: true,
                read_index: 0,
                request_id: request.request_id,
            })
        }
    }
}
