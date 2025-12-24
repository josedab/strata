//! Raft node state management.

use crate::types::{LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The state of a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Follower state - passive, responds to RPCs.
    Follower,
    /// Candidate state - actively seeking election.
    Candidate,
    /// Leader state - handling client requests and replication.
    Leader,
}

impl NodeState {
    pub fn is_leader(&self) -> bool {
        matches!(self, NodeState::Leader)
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, NodeState::Follower)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, NodeState::Candidate)
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Follower => write!(f, "Follower"),
            NodeState::Candidate => write!(f, "Candidate"),
            NodeState::Leader => write!(f, "Leader"),
        }
    }
}

/// Persistent state that must survive restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// Current term (latest term server has seen).
    pub current_term: Term,
    /// CandidateId that received vote in current term (or None).
    pub voted_for: Option<NodeId>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

/// Volatile state for all servers.
#[derive(Debug, Clone)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of highest log entry applied to state machine.
    pub last_applied: LogIndex,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

impl Default for VolatileState {
    fn default() -> Self {
        Self::new()
    }
}

/// Volatile state for leaders only.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each server, index of the next log entry to send.
    pub next_index: HashMap<NodeId, LogIndex>,
    /// For each server, index of highest log entry known to be replicated.
    pub match_index: HashMap<NodeId, LogIndex>,
}

impl LeaderState {
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for &peer in peers {
            // Initialize nextIndex to leader's last log index + 1
            next_index.insert(peer, last_log_index + 1);
            // Initialize matchIndex to 0
            match_index.insert(peer, 0);
        }

        Self {
            next_index,
            match_index,
        }
    }

    /// Update match_index and potentially next_index after successful replication.
    pub fn update_match(&mut self, peer: NodeId, match_index: LogIndex) {
        self.match_index.insert(peer, match_index);
        self.next_index.insert(peer, match_index + 1);
    }

    /// Decrement next_index after replication failure.
    pub fn decrement_next(&mut self, peer: NodeId) {
        if let Some(next) = self.next_index.get_mut(&peer) {
            *next = next.saturating_sub(1).max(1);
        }
    }
}

/// Complete Raft state for a node.
#[derive(Debug)]
pub struct RaftState {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current node state (Follower/Candidate/Leader).
    pub state: NodeState,
    /// Current leader ID (if known).
    pub leader_id: Option<NodeId>,
    /// Persistent state.
    pub persistent: PersistentState,
    /// Volatile state.
    pub volatile: VolatileState,
    /// Leader-specific state (only valid when state == Leader).
    pub leader: Option<LeaderState>,
    /// Peer node IDs.
    pub peers: Vec<NodeId>,
}

impl RaftState {
    pub fn new(node_id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            node_id,
            state: NodeState::Follower,
            leader_id: None,
            persistent: PersistentState::new(),
            volatile: VolatileState::new(),
            leader: None,
            peers,
        }
    }

    /// Transition to follower state.
    pub fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        self.state = NodeState::Follower;
        self.persistent.current_term = term;
        self.leader_id = leader_id;
        self.leader = None;

        // Reset voted_for if term changed
        if self.persistent.current_term < term {
            self.persistent.voted_for = None;
        }

        tracing::info!(
            node_id = self.node_id,
            term = term,
            leader = ?leader_id,
            "Became follower"
        );
    }

    /// Transition to candidate state.
    pub fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.persistent.current_term += 1;
        self.persistent.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.leader = None;

        tracing::info!(
            node_id = self.node_id,
            term = self.persistent.current_term,
            "Became candidate"
        );
    }

    /// Transition to leader state.
    pub fn become_leader(&mut self, last_log_index: LogIndex) {
        self.state = NodeState::Leader;
        self.leader_id = Some(self.node_id);
        self.leader = Some(LeaderState::new(&self.peers, last_log_index));

        tracing::info!(
            node_id = self.node_id,
            term = self.persistent.current_term,
            "Became leader"
        );
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    /// Get the current term.
    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    /// Get the quorum size for this cluster.
    pub fn quorum_size(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    /// Update term if we see a higher term.
    /// Returns true if term was updated.
    pub fn maybe_update_term(&mut self, term: Term) -> bool {
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            true
        } else {
            false
        }
    }

    /// Calculate the new commit index based on match indices.
    pub fn calculate_commit_index(&self, last_log_index: LogIndex) -> LogIndex {
        if !self.is_leader() {
            return self.volatile.commit_index;
        }

        let leader_state = match &self.leader {
            Some(l) => l,
            None => return self.volatile.commit_index,
        };

        // Find the highest index that a majority of servers have
        let mut indices: Vec<LogIndex> = leader_state.match_index.values().copied().collect();
        indices.push(last_log_index); // Include leader's own index

        indices.sort_unstable();
        indices.reverse();

        // The index at position quorum_size - 1 is the highest index
        // that at least quorum_size servers have
        let quorum_idx = self.quorum_size() - 1;
        if quorum_idx < indices.len() {
            indices[quorum_idx].max(self.volatile.commit_index)
        } else {
            self.volatile.commit_index
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let state = RaftState::new(1, vec![2, 3, 4]);
        assert!(state.state.is_follower());
        assert_eq!(state.current_term(), 0);
        assert!(state.leader_id.is_none());
    }

    #[test]
    fn test_become_candidate() {
        let mut state = RaftState::new(1, vec![2, 3]);
        state.become_candidate();

        assert!(state.state.is_candidate());
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.persistent.voted_for, Some(1));
    }

    #[test]
    fn test_become_leader() {
        let mut state = RaftState::new(1, vec![2, 3]);
        state.become_candidate();
        state.become_leader(5);

        assert!(state.state.is_leader());
        assert_eq!(state.leader_id, Some(1));
        assert!(state.leader.is_some());

        let leader = state.leader.as_ref().unwrap();
        assert_eq!(leader.next_index.get(&2), Some(&6));
        assert_eq!(leader.match_index.get(&2), Some(&0));
    }

    #[test]
    fn test_quorum_size() {
        // 3 node cluster (1 + 2 peers)
        let state = RaftState::new(1, vec![2, 3]);
        assert_eq!(state.quorum_size(), 2);

        // 5 node cluster (1 + 4 peers)
        let state = RaftState::new(1, vec![2, 3, 4, 5]);
        assert_eq!(state.quorum_size(), 3);
    }

    #[test]
    fn test_calculate_commit_index() {
        let mut state = RaftState::new(1, vec![2, 3, 4, 5]);
        state.become_candidate();
        state.become_leader(10);

        // Update match indices
        let leader = state.leader.as_mut().unwrap();
        leader.match_index.insert(2, 8);
        leader.match_index.insert(3, 7);
        leader.match_index.insert(4, 9);
        leader.match_index.insert(5, 6);

        // Leader has index 10, so indices are [10, 9, 8, 7, 6]
        // Quorum is 3, so third highest (index 2) = 8
        let commit_index = state.calculate_commit_index(10);
        assert_eq!(commit_index, 8);
    }
}
