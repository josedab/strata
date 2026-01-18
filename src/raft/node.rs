//! Raft node implementation.

use super::rpc::*;
use super::state::*;
use super::{LogEntry, RaftLog, RaftStorage, StateMachine};
use crate::error::{Result, StrataError};
use crate::types::{LogIndex, NodeId, Term};
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, timeout, Instant};
use tracing::{debug, error, info, warn};

/// Raft configuration.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// Peer node IDs and addresses.
    pub peers: HashMap<NodeId, String>,
    /// Minimum election timeout.
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Maximum entries per AppendEntries RPC.
    pub max_entries_per_append: usize,
    /// Snapshot threshold (log entries before snapshot).
    pub snapshot_threshold: usize,
    /// Snapshot chunk size for streaming (default 1MB).
    pub snapshot_chunk_size: usize,
    /// Leadership transfer timeout.
    pub transfer_leader_timeout: Duration,
    /// Enable pre-vote protocol to prevent disruptions.
    pub pre_vote: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: HashMap::new(),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            max_entries_per_append: 100,
            snapshot_threshold: 10000,
            snapshot_chunk_size: 1024 * 1024, // 1MB
            transfer_leader_timeout: Duration::from_secs(5),
            pre_vote: true,
        }
    }
}

/// Command for the Raft node.
pub enum RaftCommand {
    /// Propose a new command to be replicated.
    Propose {
        data: Vec<u8>,
        response: oneshot::Sender<Result<LogIndex>>,
    },
    /// Handle incoming RequestVote RPC.
    RequestVote {
        request: RequestVoteRequest,
        response: oneshot::Sender<RequestVoteResponse>,
    },
    /// Handle incoming AppendEntries RPC.
    AppendEntries {
        request: AppendEntriesRequest,
        response: oneshot::Sender<AppendEntriesResponse>,
    },
    /// Handle incoming InstallSnapshot RPC.
    InstallSnapshot {
        request: InstallSnapshotRequest,
        response: oneshot::Sender<InstallSnapshotResponse>,
    },
    /// Handle incoming TimeoutNow RPC for leader transfer.
    TimeoutNow {
        request: TimeoutNowRequest,
        response: oneshot::Sender<TimeoutNowResponse>,
    },
    /// Transfer leadership to another node.
    TransferLeadership {
        target_id: NodeId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Add a node to the cluster.
    AddNode {
        node_id: NodeId,
        address: String,
        response: oneshot::Sender<Result<()>>,
    },
    /// Remove a node from the cluster.
    RemoveNode {
        node_id: NodeId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Request a linearizable read.
    ReadIndex {
        response: oneshot::Sender<Result<LogIndex>>,
    },
    /// Check if this node is the leader.
    IsLeader {
        response: oneshot::Sender<bool>,
    },
    /// Get current leader ID.
    GetLeader {
        response: oneshot::Sender<Option<NodeId>>,
    },
    /// Shutdown the node.
    Shutdown,
}

/// State for pending snapshot installation.
#[derive(Debug)]
struct PendingSnapshot {
    /// Accumulated snapshot data.
    data: Vec<u8>,
    /// Expected total size.
    last_included_index: LogIndex,
    /// Term of the last included entry.
    last_included_term: Term,
    /// Next expected offset.
    next_offset: u64,
}

/// State for leadership transfer.
#[derive(Debug)]
struct LeaderTransferState {
    /// Target node for leadership transfer.
    target_id: NodeId,
    /// When the transfer started.
    started_at: Instant,
}

/// The Raft node, managing consensus for a replicated state machine.
pub struct RaftNode<S: StateMachine> {
    config: RaftConfig,
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<RaftLog>>,
    storage: Arc<RaftStorage>,
    state_machine: Arc<RwLock<S>>,
    rpc: Arc<dyn RaftRpc>,
    command_tx: mpsc::Sender<RaftCommand>,
    /// Pending snapshot being received.
    pending_snapshot: Arc<RwLock<Option<PendingSnapshot>>>,
    /// Active leadership transfer.
    leader_transfer: Arc<RwLock<Option<LeaderTransferState>>>,
    /// Pending linearizable read requests.
    pending_reads: Arc<RwLock<Vec<(u64, LogIndex, oneshot::Sender<Result<LogIndex>>)>>>,
    /// Next read request ID.
    next_read_id: Arc<std::sync::atomic::AtomicU64>,
}

impl<S: StateMachine + 'static> RaftNode<S> {
    /// Create a new Raft node.
    pub fn new<P: AsRef<Path>>(
        config: RaftConfig,
        storage_path: P,
        state_machine: S,
        rpc: Arc<dyn RaftRpc>,
    ) -> Result<(Self, mpsc::Receiver<RaftCommand>)> {
        let storage = Arc::new(RaftStorage::open(storage_path)?);

        // Load persistent state
        let peers: Vec<NodeId> = config.peers.keys().copied().collect();
        let mut raft_state = RaftState::new(config.node_id, peers);

        if let Some(persistent) = storage.load_persistent_state()? {
            raft_state.persistent = persistent;
        }

        // Load log entries
        let mut log = RaftLog::new();
        let mut state_machine = state_machine;
        if let Some((snapshot_data, meta)) = storage.load_snapshot()? {
            // Restore from snapshot
            state_machine.restore(&snapshot_data)?;
            log.compact(meta.last_index, meta.last_term);
        }

        // Load remaining log entries
        let first_index = log.first_index();
        for entry in storage.load_log_entries_from(first_index)? {
            log.append(entry)?;
        }

        let (command_tx, command_rx) = mpsc::channel(1000);

        let node = Self {
            config,
            state: Arc::new(RwLock::new(raft_state)),
            log: Arc::new(RwLock::new(log)),
            storage,
            state_machine: Arc::new(RwLock::new(state_machine)),
            rpc,
            command_tx,
            pending_snapshot: Arc::new(RwLock::new(None)),
            leader_transfer: Arc::new(RwLock::new(None)),
            pending_reads: Arc::new(RwLock::new(Vec::new())),
            next_read_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        Ok((node, command_rx))
    }

    /// Get a command sender for this node.
    pub fn command_sender(&self) -> mpsc::Sender<RaftCommand> {
        self.command_tx.clone()
    }

    /// Run the Raft node event loop.
    pub async fn run(self, mut command_rx: mpsc::Receiver<RaftCommand>) {
        let mut election_deadline = self.reset_election_deadline();
        let mut heartbeat_interval = interval(self.config.heartbeat_interval);

        loop {
            let is_leader = self.state.read().is_leader();

            tokio::select! {
                // Handle incoming commands
                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        RaftCommand::Shutdown => {
                            info!("Raft node shutting down");
                            break;
                        }
                        RaftCommand::Propose { data, response } => {
                            let result = self.handle_propose(data).await;
                            let _ = response.send(result);
                        }
                        RaftCommand::RequestVote { request, response } => {
                            let result = self.handle_request_vote(request);
                            let _ = response.send(result);
                        }
                        RaftCommand::AppendEntries { request, response } => {
                            let result = self.handle_append_entries(request);
                            // Reset election deadline on valid AppendEntries
                            if result.success {
                                election_deadline = self.reset_election_deadline();
                            }
                            let _ = response.send(result);
                        }
                        RaftCommand::InstallSnapshot { request, response } => {
                            let result = self.handle_install_snapshot(request);
                            // Reset election deadline on valid InstallSnapshot
                            election_deadline = self.reset_election_deadline();
                            let _ = response.send(result);
                        }
                        RaftCommand::TimeoutNow { request, response } => {
                            let result = self.handle_timeout_now(request);
                            let _ = response.send(result);
                            // Trigger immediate election
                            election_deadline = Instant::now();
                        }
                        RaftCommand::TransferLeadership { target_id, response } => {
                            let result = self.handle_transfer_leadership(target_id).await;
                            let _ = response.send(result);
                        }
                        RaftCommand::AddNode { node_id, address, response } => {
                            let result = self.handle_add_node(node_id, address).await;
                            let _ = response.send(result);
                        }
                        RaftCommand::RemoveNode { node_id, response } => {
                            let result = self.handle_remove_node(node_id).await;
                            let _ = response.send(result);
                        }
                        RaftCommand::ReadIndex { response } => {
                            let result = self.handle_read_index().await;
                            let _ = response.send(result);
                        }
                        RaftCommand::IsLeader { response } => {
                            let _ = response.send(is_leader);
                        }
                        RaftCommand::GetLeader { response } => {
                            let leader = self.state.read().leader_id;
                            let _ = response.send(leader);
                        }
                    }
                }

                // Leader heartbeat
                _ = heartbeat_interval.tick(), if is_leader => {
                    self.send_heartbeats().await;
                }

                // Election timeout (non-leader only)
                _ = tokio::time::sleep_until(election_deadline), if !is_leader => {
                    self.start_election().await;
                    election_deadline = self.reset_election_deadline();
                }
            }

            // Apply committed entries
            self.apply_committed_entries();

            // Check if snapshot is needed
            self.maybe_snapshot();
        }
    }

    /// Propose a new command to be replicated.
    async fn handle_propose(&self, data: Vec<u8>) -> Result<LogIndex> {
        let (term, is_leader) = {
            let state = self.state.read();
            (state.current_term(), state.is_leader())
        };

        if !is_leader {
            let leader = self.state.read().leader_id;
            return Err(StrataError::NotLeader { leader });
        }

        // Append to local log
        let index = {
            let mut log = self.log.write();
            let index = log.last_index() + 1;
            let entry = LogEntry::new(term, index, data);
            log.append(entry.clone())?;

            // Persist the entry
            self.storage.append_log_entries(&[entry])?;
            index
        };

        // Replicate to followers
        self.replicate_to_all().await;

        Ok(index)
    }

    /// Handle RequestVote RPC.
    fn handle_request_vote(&self, request: RequestVoteRequest) -> RequestVoteResponse {
        let mut state = self.state.write();
        let log = self.log.read();

        // If request term is higher, update and become follower
        if request.term > state.current_term() {
            state.become_follower(request.term, None);
            self.persist_state(&state);
        }

        let vote_granted = if request.term < state.current_term() {
            // Stale term
            false
        } else if state.persistent.voted_for.is_some()
            && state.persistent.voted_for != Some(request.candidate_id)
        {
            // Already voted for someone else
            false
        } else if !log.is_up_to_date(request.last_log_index, request.last_log_term) {
            // Candidate's log is not up-to-date
            false
        } else {
            // Grant vote
            state.persistent.voted_for = Some(request.candidate_id);
            self.persist_state(&state);
            true
        };

        debug!(
            node_id = state.node_id,
            candidate = request.candidate_id,
            term = request.term,
            vote_granted,
            "Handled RequestVote"
        );

        RequestVoteResponse {
            term: state.current_term(),
            vote_granted,
        }
    }

    /// Handle AppendEntries RPC.
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut state = self.state.write();
        let mut log = self.log.write();

        // If request term is higher, update and become follower
        if request.term > state.current_term() {
            state.become_follower(request.term, Some(request.leader_id));
            self.persist_state(&state);
        }

        // Reject if term is stale
        if request.term < state.current_term() {
            return AppendEntriesResponse {
                term: state.current_term(),
                success: false,
                match_index: 0,
                conflict_index: None,
                conflict_term: None,
            };
        }

        // Update leader
        state.leader_id = Some(request.leader_id);

        // Check log consistency
        if !log.matches(request.prev_log_index, request.prev_log_term) {
            let conflict_term = log.term_at(request.prev_log_index);
            let conflict_index = if conflict_term.is_some() {
                // Find the first index with this term
                let mut idx = request.prev_log_index;
                while idx > log.first_index() && log.term_at(idx - 1) == conflict_term {
                    idx -= 1;
                }
                Some(idx)
            } else {
                // We don't have this index at all
                Some(log.last_index() + 1)
            };

            return AppendEntriesResponse {
                term: state.current_term(),
                success: false,
                match_index: 0,
                conflict_index,
                conflict_term,
            };
        }

        // Append new entries
        let mut new_entries = Vec::new();
        for entry in request.entries {
            if entry.index <= log.last_index() {
                // Check for conflicts
                if let Some(existing) = log.get(entry.index) {
                    if existing.term != entry.term {
                        // Conflict - truncate and append
                        log.truncate_from(entry.index);
                        if let Err(e) = self.storage.truncate_log_from(entry.index) {
                            tracing::error!(
                                error = %e,
                                index = entry.index,
                                "Failed to truncate storage log during conflict resolution - data inconsistency possible"
                            );
                            // Continue - in-memory log is already truncated
                        }
                        new_entries.push(entry);
                    }
                    // Otherwise skip (already have this entry)
                } else {
                    new_entries.push(entry);
                }
            } else {
                new_entries.push(entry);
            }
        }

        // Persist and append new entries
        if !new_entries.is_empty() {
            if let Err(e) = self.storage.append_log_entries(&new_entries) {
                tracing::error!(
                    error = %e,
                    entry_count = new_entries.len(),
                    "Failed to persist log entries to storage - returning failure to trigger retry"
                );
                // Return failure so leader will retry
                return AppendEntriesResponse {
                    term: state.current_term(),
                    success: false,
                    match_index: log.last_index(),
                    conflict_index: None,
                    conflict_term: None,
                };
            }
            for entry in new_entries {
                if let Err(e) = log.append(entry.clone()) {
                    tracing::error!(
                        error = %e,
                        index = entry.index,
                        term = entry.term,
                        "Failed to append entry to in-memory log - state may be inconsistent"
                    );
                    // This is a critical error - in-memory and storage are now potentially inconsistent
                }
            }
        }

        // Update commit index
        if request.leader_commit > state.volatile.commit_index {
            state.volatile.commit_index = request.leader_commit.min(log.last_index());
        }

        AppendEntriesResponse {
            term: state.current_term(),
            success: true,
            match_index: log.last_index(),
            conflict_index: None,
            conflict_term: None,
        }
    }

    /// Start a new election.
    async fn start_election(&self) {
        let (term, last_log_index, last_log_term) = {
            let mut state = self.state.write();
            let log = self.log.read();

            state.become_candidate();
            self.persist_state(&state);

            (state.current_term(), log.last_index(), log.last_term())
        };

        info!(node_id = self.config.node_id, term, "Starting election");

        let request = RequestVoteRequest {
            term,
            candidate_id: self.config.node_id,
            last_log_index,
            last_log_term,
        };

        // Count votes (including self-vote)
        let mut votes_received = 1;
        let quorum_size = {
            let state = self.state.read();
            state.quorum_size()
        };

        // Request votes from all peers in parallel
        let peers: Vec<_> = self.config.peers.keys().copied().collect();
        let mut vote_futures = Vec::new();

        for peer_id in peers {
            let rpc = Arc::clone(&self.rpc);
            let req = request.clone();
            vote_futures.push(async move {
                match timeout(
                    Duration::from_millis(100),
                    rpc.request_vote(peer_id, req),
                )
                .await
                {
                    Ok(Ok(response)) => Some((peer_id, response)),
                    _ => None,
                }
            });
        }

        let results = futures::future::join_all(vote_futures).await;

        for result in results {
            if let Some((peer_id, response)) = result {
                // Check if we're still a candidate for this term
                let should_become_leader;
                {
                    let mut state = self.state.write();
                    if !state.state.is_candidate() || state.current_term() != term {
                        return;
                    }

                    // Check for higher term
                    if response.term > state.current_term() {
                        state.become_follower(response.term, None);
                        self.persist_state(&state);
                        return;
                    }

                    should_become_leader = if response.vote_granted {
                        votes_received += 1;
                        debug!(
                            node_id = self.config.node_id,
                            voter = peer_id,
                            votes = votes_received,
                            "Received vote"
                        );

                        if votes_received >= quorum_size {
                            // Won the election!
                            let last_index = self.log.read().last_index();
                            state.become_leader(last_index);

                            info!(
                                node_id = self.config.node_id,
                                term,
                                "Won election, became leader"
                            );
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                } // state lock is dropped here

                if should_become_leader {
                    // Send initial heartbeats
                    self.send_heartbeats().await;
                    return;
                }
            }
        }
    }

    /// Send heartbeats to all followers.
    async fn send_heartbeats(&self) {
        self.replicate_to_all().await;
    }

    /// Replicate log entries to all followers.
    async fn replicate_to_all(&self) {
        let (term, commit_index, leader_state) = {
            let state = self.state.read();
            if !state.is_leader() {
                return;
            }
            (
                state.current_term(),
                state.volatile.commit_index,
                state.leader.clone(),
            )
        };

        let leader_state = match leader_state {
            Some(l) => l,
            None => return,
        };

        let peers: Vec<_> = self.config.peers.keys().copied().collect();
        let mut replication_futures = Vec::new();

        for peer_id in peers {
            let rpc = Arc::clone(&self.rpc);
            let log = Arc::clone(&self.log);
            let config = self.config.clone();

            let next_index = *leader_state.next_index.get(&peer_id).unwrap_or(&1);

            let (prev_log_index, prev_log_term, entries) = {
                let log = log.read();
                let prev_log_index = next_index.saturating_sub(1);
                let prev_log_term = log.term_at(prev_log_index).unwrap_or(0);

                let entries = log.entries_from_limit(next_index, config.max_entries_per_append);

                (prev_log_index, prev_log_term, entries)
            };

            let request = AppendEntriesRequest {
                term,
                leader_id: config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };

            replication_futures.push(async move {
                match timeout(Duration::from_millis(50), rpc.append_entries(peer_id, request)).await
                {
                    Ok(Ok(response)) => Some((peer_id, response)),
                    _ => None,
                }
            });
        }

        let results = futures::future::join_all(replication_futures).await;

        // Process results
        let mut state = self.state.write();
        if !state.is_leader() {
            return;
        }

        for result in results {
            if let Some((peer_id, response)) = result {
                // Check for higher term
                if response.term > state.current_term() {
                    state.become_follower(response.term, None);
                    self.persist_state(&state);
                    return;
                }

                if let Some(leader) = state.leader.as_mut() {
                    if response.success {
                        leader.update_match(peer_id, response.match_index);
                    } else {
                        // Decrement next_index and retry
                        if let Some(conflict_index) = response.conflict_index {
                            leader.next_index.insert(peer_id, conflict_index);
                        } else {
                            leader.decrement_next(peer_id);
                        }
                    }
                }
            }
        }

        // Update commit index
        let last_log_index = self.log.read().last_index();
        let new_commit = state.calculate_commit_index(last_log_index);
        if new_commit > state.volatile.commit_index {
            state.volatile.commit_index = new_commit;
            debug!(
                node_id = state.node_id,
                commit_index = new_commit,
                "Updated commit index"
            );
        }
    }

    /// Apply committed entries to the state machine.
    fn apply_committed_entries(&self) {
        let (commit_index, last_applied) = {
            let state = self.state.read();
            (state.volatile.commit_index, state.volatile.last_applied)
        };

        if commit_index <= last_applied {
            return;
        }

        let entries_to_apply: Vec<_> = {
            let log = self.log.read();
            log.entries_range(last_applied + 1, commit_index)
        };

        let mut state_machine = self.state_machine.write();
        for entry in entries_to_apply {
            state_machine.apply(&entry.data);
            self.state.write().volatile.last_applied = entry.index;
        }
    }

    /// Check if a snapshot should be taken.
    fn maybe_snapshot(&self) {
        let (last_applied, last_snapshot_index) = {
            let state = self.state.read();
            let log = self.log.read();
            (state.volatile.last_applied, log.first_index().saturating_sub(1))
        };

        if last_applied - last_snapshot_index < self.config.snapshot_threshold as u64 {
            return;
        }

        // Take snapshot
        let snapshot_data = self.state_machine.read().snapshot();
        let snapshot_term = self.log.read().term_at(last_applied).unwrap_or(0);

        if let Err(e) = self.storage.save_snapshot(&snapshot_data, last_applied, snapshot_term) {
            error!(error = %e, "Failed to save snapshot");
            return;
        }

        // Compact log
        {
            let mut log = self.log.write();
            log.compact(last_applied, snapshot_term);
        }

        if let Err(e) = self.storage.compact_log(last_applied) {
            error!(error = %e, "Failed to compact log");
        }

        info!(
            node_id = self.config.node_id,
            last_applied,
            "Created snapshot"
        );
    }

    /// Persist state to storage.
    fn persist_state(&self, state: &RaftState) {
        if let Err(e) = self.storage.save_persistent_state(&state.persistent) {
            error!(error = %e, "Failed to persist state");
        }
    }

    /// Generate a random election timeout.
    fn reset_election_deadline(&self) -> Instant {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(
            self.config.election_timeout_min..=self.config.election_timeout_max,
        );
        Instant::now() + timeout
    }

    /// Handle InstallSnapshot RPC for snapshot streaming.
    fn handle_install_snapshot(&self, request: InstallSnapshotRequest) -> InstallSnapshotResponse {
        let mut state = self.state.write();

        // Update term if necessary
        if request.term > state.current_term() {
            state.become_follower(request.term, Some(request.leader_id));
            self.persist_state(&state);
        }

        // Reject if stale term
        if request.term < state.current_term() {
            return InstallSnapshotResponse {
                term: state.current_term(),
                next_offset: 0,
                done: false,
            };
        }

        // Update leader
        state.leader_id = Some(request.leader_id);

        let mut pending = self.pending_snapshot.write();

        // Check if this is a new snapshot or continuation
        if request.offset == 0 {
            // Start new snapshot
            *pending = Some(PendingSnapshot {
                data: Vec::new(),
                last_included_index: request.last_included_index,
                last_included_term: request.last_included_term,
                next_offset: 0,
            });
        }

        let snapshot = match pending.as_mut() {
            Some(s) => s,
            None => {
                warn!("Received snapshot chunk without starting snapshot");
                return InstallSnapshotResponse {
                    term: state.current_term(),
                    next_offset: 0,
                    done: false,
                };
            }
        };

        // Check offset
        if request.offset != snapshot.next_offset {
            warn!(
                expected = snapshot.next_offset,
                received = request.offset,
                "Snapshot chunk offset mismatch"
            );
            return InstallSnapshotResponse {
                term: state.current_term(),
                next_offset: snapshot.next_offset,
                done: false,
            };
        }

        // Append chunk data
        snapshot.data.extend_from_slice(&request.data);
        snapshot.next_offset += request.data.len() as u64;

        // Extract values we need before potentially clearing pending
        let final_next_offset = snapshot.next_offset;
        let last_included_index = snapshot.last_included_index;
        let last_included_term = snapshot.last_included_term;

        if request.done {
            // Snapshot complete - apply it
            info!(
                index = last_included_index,
                term = last_included_term,
                size = snapshot.data.len(),
                "Received complete snapshot"
            );

            // Clone data before we clear pending
            let snapshot_data = snapshot.data.clone();

            // Save snapshot to storage
            if let Err(e) = self.storage.save_snapshot(
                &snapshot_data,
                last_included_index,
                last_included_term,
            ) {
                error!(error = %e, "Failed to save snapshot");
                *pending = None;
                return InstallSnapshotResponse {
                    term: state.current_term(),
                    next_offset: 0,
                    done: false,
                };
            }

            // Restore state machine
            {
                let mut sm = self.state_machine.write();
                if let Err(e) = sm.restore(&snapshot_data) {
                    error!(error = %e, "Failed to restore state machine from snapshot");
                    *pending = None;
                    return InstallSnapshotResponse {
                        term: state.current_term(),
                        next_offset: 0,
                        done: false,
                    };
                }
            }

            // Compact log
            {
                let mut log = self.log.write();
                log.compact(last_included_index, last_included_term);
            }

            // Update state
            state.volatile.commit_index = last_included_index;
            state.volatile.last_applied = last_included_index;

            *pending = None;

            return InstallSnapshotResponse {
                term: state.current_term(),
                next_offset: final_next_offset,
                done: true,
            };
        }

        InstallSnapshotResponse {
            term: state.current_term(),
            next_offset: final_next_offset,
            done: false,
        }
    }

    /// Handle TimeoutNow RPC for leader transfer.
    fn handle_timeout_now(&self, request: TimeoutNowRequest) -> TimeoutNowResponse {
        let state = self.state.read();

        // Verify term
        if request.term < state.current_term() {
            return TimeoutNowResponse {
                term: state.current_term(),
            };
        }

        info!(
            node_id = self.config.node_id,
            from_leader = request.leader_id,
            "Received TimeoutNow - starting immediate election"
        );

        TimeoutNowResponse {
            term: state.current_term(),
        }
    }

    /// Handle leadership transfer request.
    async fn handle_transfer_leadership(&self, target_id: NodeId) -> Result<()> {
        // Verify we're the leader
        let is_leader = self.state.read().is_leader();
        if !is_leader {
            return Err(StrataError::NotLeader {
                leader: self.state.read().leader_id,
            });
        }

        // Verify target is a known peer
        if !self.config.peers.contains_key(&target_id) {
            return Err(StrataError::NodeNotFound(target_id));
        }

        // Set transfer state
        {
            let mut transfer = self.leader_transfer.write();
            *transfer = Some(LeaderTransferState {
                target_id,
                started_at: Instant::now(),
            });
        }

        info!(
            node_id = self.config.node_id,
            target = target_id,
            "Initiating leadership transfer"
        );

        // Ensure target is caught up
        self.replicate_to_all().await;

        // Check if target is caught up
        let target_caught_up = {
            let state = self.state.read();
            let log = self.log.read();
            if let Some(leader) = &state.leader {
                let target_match = leader.match_index.get(&target_id).copied().unwrap_or(0);
                target_match >= log.last_index()
            } else {
                false
            }
        };

        if !target_caught_up {
            // Clear transfer state
            *self.leader_transfer.write() = None;
            return Err(StrataError::Internal(
                "Target node is not caught up".to_string(),
            ));
        }

        // Send TimeoutNow to target
        let term = self.state.read().current_term();
        let request = TimeoutNowRequest {
            term,
            leader_id: self.config.node_id,
        };

        match timeout(
            self.config.transfer_leader_timeout,
            self.rpc.timeout_now(target_id, request),
        )
        .await
        {
            Ok(Ok(_)) => {
                info!(
                    node_id = self.config.node_id,
                    target = target_id,
                    "Leadership transfer initiated successfully"
                );
                Ok(())
            }
            Ok(Err(e)) => {
                *self.leader_transfer.write() = None;
                Err(e)
            }
            Err(_) => {
                *self.leader_transfer.write() = None;
                Err(StrataError::TimeoutStr("Operation timed out".to_string()))
            }
        }
    }

    /// Handle adding a node to the cluster.
    async fn handle_add_node(&self, node_id: NodeId, address: String) -> Result<()> {
        // Verify we're the leader
        let is_leader = self.state.read().is_leader();
        if !is_leader {
            return Err(StrataError::NotLeader {
                leader: self.state.read().leader_id,
            });
        }

        // Check if node already exists
        if self.config.peers.contains_key(&node_id) || node_id == self.config.node_id {
            return Err(StrataError::AlreadyExists(format!(
                "Node {} already in cluster",
                node_id
            )));
        }

        // Create membership change entry
        let change = MembershipChange {
            change_type: MembershipChangeType::AddNode,
            node_id,
            node_addr: Some(address),
        };

        let data = bincode::serialize(&change)
            .map_err(|e| StrataError::Serialization(e.to_string()))?;

        // Propose the membership change through normal Raft
        self.handle_propose(data).await?;

        info!(
            node_id = self.config.node_id,
            new_node = node_id,
            "Proposed adding node to cluster"
        );

        Ok(())
    }

    /// Handle removing a node from the cluster.
    async fn handle_remove_node(&self, node_id: NodeId) -> Result<()> {
        // Verify we're the leader
        let is_leader = self.state.read().is_leader();
        if !is_leader {
            return Err(StrataError::NotLeader {
                leader: self.state.read().leader_id,
            });
        }

        // Check if node exists
        if !self.config.peers.contains_key(&node_id) && node_id != self.config.node_id {
            return Err(StrataError::NodeNotFound(node_id));
        }

        // Create membership change entry
        let change = MembershipChange {
            change_type: MembershipChangeType::RemoveNode,
            node_id,
            node_addr: None,
        };

        let data = bincode::serialize(&change)
            .map_err(|e| StrataError::Serialization(e.to_string()))?;

        // Propose the membership change through normal Raft
        self.handle_propose(data).await?;

        info!(
            node_id = self.config.node_id,
            removed_node = node_id,
            "Proposed removing node from cluster"
        );

        Ok(())
    }

    /// Handle linearizable read request.
    async fn handle_read_index(&self) -> Result<LogIndex> {
        // Verify we're the leader
        let (is_leader, commit_index) = {
            let state = self.state.read();
            (state.is_leader(), state.volatile.commit_index)
        };

        if !is_leader {
            return Err(StrataError::NotLeader {
                leader: self.state.read().leader_id,
            });
        }

        // For linearizable reads, we need to confirm leadership
        // by successfully sending heartbeats to a quorum
        self.replicate_to_all().await;

        // Check we're still the leader after replication
        let still_leader = self.state.read().is_leader();
        if !still_leader {
            return Err(StrataError::NotLeader {
                leader: self.state.read().leader_id,
            });
        }

        // Return the commit index - client should wait until
        // last_applied >= commit_index before serving the read
        Ok(commit_index)
    }

    /// Send snapshot to a follower that's too far behind.
    async fn send_snapshot_to_follower(&self, follower_id: NodeId) -> Result<()> {
        // Load snapshot from storage
        let (data, last_index, last_term) = match self.storage.load_snapshot()? {
            Some((data, meta)) => (data, meta.last_index, meta.last_term),
            None => {
                return Err(StrataError::Internal("No snapshot available".to_string()));
            }
        };

        let term = self.state.read().current_term();
        let chunk_size = self.config.snapshot_chunk_size;
        let mut offset = 0u64;

        info!(
            node_id = self.config.node_id,
            follower = follower_id,
            size = data.len(),
            "Starting snapshot streaming"
        );

        while offset < data.len() as u64 {
            let end = ((offset as usize) + chunk_size).min(data.len());
            let chunk = data[offset as usize..end].to_vec();
            let done = end >= data.len();

            let request = InstallSnapshotRequest {
                term,
                leader_id: self.config.node_id,
                last_included_index: last_index,
                last_included_term: last_term,
                offset,
                data: chunk,
                done,
            };

            match timeout(
                Duration::from_secs(10),
                self.rpc.install_snapshot(follower_id, request),
            )
            .await
            {
                Ok(Ok(response)) => {
                    if response.term > term {
                        // We're stale, become follower
                        let mut state = self.state.write();
                        state.become_follower(response.term, None);
                        self.persist_state(&state);
                        return Err(StrataError::NotLeader { leader: None });
                    }

                    if response.done {
                        info!(
                            node_id = self.config.node_id,
                            follower = follower_id,
                            "Snapshot streaming completed"
                        );
                        return Ok(());
                    }

                    offset = response.next_offset;
                }
                Ok(Err(e)) => {
                    error!(
                        error = %e,
                        follower = follower_id,
                        "Failed to send snapshot chunk"
                    );
                    return Err(e);
                }
                Err(_) => {
                    return Err(StrataError::TimeoutStr("Operation timed out".to_string()));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::rpc::mock::MockRpc;
    use tempfile::tempdir;

    struct TestStateMachine {
        applied: Vec<Vec<u8>>,
    }

    impl StateMachine for TestStateMachine {
        type Result = ();

        fn apply(&mut self, command: &[u8]) -> Self::Result {
            self.applied.push(command.to_vec());
        }

        fn snapshot(&self) -> Vec<u8> {
            bincode::serialize(&self.applied).unwrap()
        }

        fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
            self.applied = bincode::deserialize(snapshot)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_raft_node_creation() {
        let dir = tempdir().unwrap();
        let config = RaftConfig {
            node_id: 1,
            peers: [(2, "127.0.0.1:9001".to_string())].into_iter().collect(),
            ..Default::default()
        };

        let state_machine = TestStateMachine { applied: Vec::new() };
        let rpc = Arc::new(MockRpc::new());

        let (node, _rx) = RaftNode::new(config, dir.path(), state_machine, rpc).unwrap();

        assert!(!node.state.read().is_leader());
    }
}
