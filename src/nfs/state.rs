//! NFS state management for locks, opens, and delegations.
//!
//! NFSv4 requires server-side state tracking for:
//! - Open files (share reservations)
//! - Byte-range locks
//! - Delegations (client-side caching)
//!
//! This module provides the state management infrastructure.

use crate::types::InodeId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Client identifier (unique per client instance).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientId(pub u64);

impl ClientId {
    /// Create a new client ID.
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/// State owner identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateOwner {
    /// Client ID.
    pub client_id: ClientId,
    /// Owner identifier (opaque to server).
    pub owner: Vec<u8>,
}

impl StateOwner {
    /// Create a new state owner.
    pub fn new(client_id: ClientId, owner: Vec<u8>) -> Self {
        Self { client_id, owner }
    }
}

/// State ID for tracking open/lock/delegation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateId {
    /// Sequence ID (incremented on state changes).
    pub seqid: u32,
    /// Unique identifier.
    pub other: [u8; 12],
}

impl StateId {
    /// Create a new state ID.
    pub fn new(id: u64) -> Self {
        let mut other = [0u8; 12];
        other[0..8].copy_from_slice(&id.to_be_bytes());
        Self { seqid: 1, other }
    }

    /// Increment the sequence ID.
    pub fn increment(&mut self) {
        self.seqid = self.seqid.wrapping_add(1);
        if self.seqid == 0 {
            self.seqid = 1;
        }
    }

    /// Check if this is a special state ID.
    pub fn is_special(&self) -> bool {
        self.other == [0u8; 12] || self.other == [0xFFu8; 12]
    }
}

/// Open state flags.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct OpenStateFlags {
    /// Read access granted.
    pub read: bool,
    /// Write access granted.
    pub write: bool,
    /// Deny read to others.
    pub deny_read: bool,
    /// Deny write to others.
    pub deny_write: bool,
}

impl OpenStateFlags {
    /// Check if this open conflicts with another.
    pub fn conflicts_with(&self, other: &OpenStateFlags) -> bool {
        // Check if our access conflicts with their deny
        (self.read && other.deny_read)
            || (self.write && other.deny_write)
            // Check if their access conflicts with our deny
            || (other.read && self.deny_read)
            || (other.write && self.deny_write)
    }
}

/// Open state for a file.
#[derive(Debug, Clone)]
pub struct OpenState {
    /// State ID for this open.
    pub state_id: StateId,
    /// The state owner.
    pub owner: StateOwner,
    /// The opened file's inode.
    pub inode_id: InodeId,
    /// Open flags.
    pub flags: OpenStateFlags,
    /// Creation time.
    pub created_at: Instant,
    /// Last access time.
    pub last_access: Instant,
}

impl OpenState {
    /// Create a new open state.
    pub fn new(state_id: StateId, owner: StateOwner, inode_id: InodeId, flags: OpenStateFlags) -> Self {
        let now = Instant::now();
        Self {
            state_id,
            owner,
            inode_id,
            flags,
            created_at: now,
            last_access: now,
        }
    }

    /// Touch the state to update last access time.
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

/// Lock type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Read lock (shared).
    Read,
    /// Write lock (exclusive).
    Write,
}

/// Byte range for locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockRange {
    /// Start offset.
    pub offset: u64,
    /// Length (0 means to end of file).
    pub length: u64,
}

impl LockRange {
    /// Create a new lock range.
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    /// Check if two ranges overlap.
    pub fn overlaps(&self, other: &LockRange) -> bool {
        let self_end = if self.length == 0 {
            u64::MAX
        } else {
            self.offset.saturating_add(self.length)
        };
        let other_end = if other.length == 0 {
            u64::MAX
        } else {
            other.offset.saturating_add(other.length)
        };

        self.offset < other_end && other.offset < self_end
    }
}

/// Byte-range lock.
#[derive(Debug, Clone)]
pub struct Lock {
    /// State ID for this lock.
    pub state_id: StateId,
    /// The state owner.
    pub owner: StateOwner,
    /// The locked file's inode.
    pub inode_id: InodeId,
    /// Lock type.
    pub lock_type: LockType,
    /// Lock range.
    pub range: LockRange,
    /// Creation time.
    pub created_at: Instant,
}

impl Lock {
    /// Create a new lock.
    pub fn new(
        state_id: StateId,
        owner: StateOwner,
        inode_id: InodeId,
        lock_type: LockType,
        range: LockRange,
    ) -> Self {
        Self {
            state_id,
            owner,
            inode_id,
            lock_type,
            range,
            created_at: Instant::now(),
        }
    }

    /// Check if this lock conflicts with another.
    pub fn conflicts_with(&self, other: &Lock) -> bool {
        // Same owner doesn't conflict
        if self.owner == other.owner {
            return false;
        }

        // Different files don't conflict
        if self.inode_id != other.inode_id {
            return false;
        }

        // Ranges must overlap
        if !self.range.overlaps(&other.range) {
            return false;
        }

        // Read locks don't conflict with each other
        !(self.lock_type == LockType::Read && other.lock_type == LockType::Read)
    }
}

/// Delegation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DelegationType {
    /// No delegation.
    None,
    /// Read delegation.
    Read,
    /// Write delegation.
    Write,
}

/// File delegation.
#[derive(Debug, Clone)]
pub struct Delegation {
    /// State ID for this delegation.
    pub state_id: StateId,
    /// Client holding the delegation.
    pub client_id: ClientId,
    /// The delegated file's inode.
    pub inode_id: InodeId,
    /// Delegation type.
    pub delegation_type: DelegationType,
    /// Creation time.
    pub created_at: Instant,
    /// Whether recall has been requested.
    pub recall_requested: bool,
}

impl Delegation {
    /// Create a new delegation.
    pub fn new(
        state_id: StateId,
        client_id: ClientId,
        inode_id: InodeId,
        delegation_type: DelegationType,
    ) -> Self {
        Self {
            state_id,
            client_id,
            inode_id,
            delegation_type,
            created_at: Instant::now(),
            recall_requested: false,
        }
    }

    /// Request recall of this delegation.
    pub fn request_recall(&mut self) {
        self.recall_requested = true;
    }
}

/// Lock manager for byte-range locks.
pub struct LockManager {
    /// Locks indexed by inode.
    locks: RwLock<HashMap<InodeId, Vec<Lock>>>,
    /// State ID counter.
    state_counter: AtomicU64,
}

impl LockManager {
    /// Create a new lock manager.
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            state_counter: AtomicU64::new(1),
        }
    }

    /// Try to acquire a lock.
    pub fn lock(
        &self,
        owner: StateOwner,
        inode_id: InodeId,
        lock_type: LockType,
        range: LockRange,
    ) -> Result<Lock, LockConflict> {
        let state_id = StateId::new(self.state_counter.fetch_add(1, Ordering::SeqCst));
        let new_lock = Lock::new(state_id, owner, inode_id, lock_type, range);

        let mut locks = self.locks.write();
        let file_locks = locks.entry(inode_id).or_insert_with(Vec::new);

        // Check for conflicts
        for existing in file_locks.iter() {
            if new_lock.conflicts_with(existing) {
                return Err(LockConflict {
                    owner: existing.owner.clone(),
                    lock_type: existing.lock_type,
                    range: existing.range,
                });
            }
        }

        file_locks.push(new_lock.clone());
        debug!(inode = inode_id, "Acquired lock");
        Ok(new_lock)
    }

    /// Release a lock.
    pub fn unlock(&self, owner: &StateOwner, inode_id: InodeId, range: LockRange) -> bool {
        let mut locks = self.locks.write();
        if let Some(file_locks) = locks.get_mut(&inode_id) {
            let initial_len = file_locks.len();
            file_locks.retain(|lock| {
                !(lock.owner == *owner && lock.range == range)
            });
            let removed = file_locks.len() < initial_len;
            if removed {
                debug!(inode = inode_id, "Released lock");
            }
            removed
        } else {
            false
        }
    }

    /// Test if a lock can be acquired without actually acquiring it.
    pub fn test_lock(
        &self,
        owner: &StateOwner,
        inode_id: InodeId,
        lock_type: LockType,
        range: LockRange,
    ) -> Option<LockConflict> {
        let locks = self.locks.read();
        if let Some(file_locks) = locks.get(&inode_id) {
            for existing in file_locks.iter() {
                // Skip same owner
                if existing.owner == *owner {
                    continue;
                }

                // Check conflict
                if existing.inode_id == inode_id
                    && existing.range.overlaps(&range)
                    && !(lock_type == LockType::Read && existing.lock_type == LockType::Read)
                {
                    return Some(LockConflict {
                        owner: existing.owner.clone(),
                        lock_type: existing.lock_type,
                        range: existing.range,
                    });
                }
            }
        }
        None
    }

    /// Release all locks for an owner.
    pub fn release_all(&self, owner: &StateOwner) {
        let mut locks = self.locks.write();
        for file_locks in locks.values_mut() {
            file_locks.retain(|lock| lock.owner != *owner);
        }
    }

    /// Get lock count.
    pub fn lock_count(&self) -> usize {
        let locks = self.locks.read();
        locks.values().map(|v| v.len()).sum()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock conflict information.
#[derive(Debug, Clone)]
pub struct LockConflict {
    /// Owner of conflicting lock.
    pub owner: StateOwner,
    /// Type of conflicting lock.
    pub lock_type: LockType,
    /// Range of conflicting lock.
    pub range: LockRange,
}

/// State manager for opens, locks, and delegations.
pub struct StateManager {
    /// Open states indexed by state ID.
    opens: RwLock<HashMap<StateId, OpenState>>,
    /// Opens indexed by inode for conflict checking.
    opens_by_inode: RwLock<HashMap<InodeId, Vec<StateId>>>,
    /// Lock manager.
    pub lock_manager: LockManager,
    /// Delegations indexed by state ID.
    delegations: RwLock<HashMap<StateId, Delegation>>,
    /// Delegations indexed by inode.
    delegations_by_inode: RwLock<HashMap<InodeId, StateId>>,
    /// State ID counter.
    state_counter: AtomicU64,
    /// Lease duration.
    lease_duration: Duration,
    /// Client leases (client_id -> lease expiry).
    leases: RwLock<HashMap<ClientId, Instant>>,
}

impl StateManager {
    /// Create a new state manager.
    pub fn new(lease_duration_secs: u32) -> Self {
        Self {
            opens: RwLock::new(HashMap::new()),
            opens_by_inode: RwLock::new(HashMap::new()),
            lock_manager: LockManager::new(),
            delegations: RwLock::new(HashMap::new()),
            delegations_by_inode: RwLock::new(HashMap::new()),
            state_counter: AtomicU64::new(1),
            lease_duration: Duration::from_secs(lease_duration_secs as u64),
            leases: RwLock::new(HashMap::new()),
        }
    }

    /// Open a file.
    pub fn open(
        &self,
        owner: StateOwner,
        inode_id: InodeId,
        flags: OpenStateFlags,
    ) -> Result<OpenState, OpenConflict> {
        // Check for conflicts with existing opens
        {
            let opens = self.opens.read();
            let opens_by_inode = self.opens_by_inode.read();

            if let Some(state_ids) = opens_by_inode.get(&inode_id) {
                for state_id in state_ids {
                    if let Some(existing) = opens.get(state_id) {
                        if flags.conflicts_with(&existing.flags) {
                            return Err(OpenConflict {
                                owner: existing.owner.clone(),
                                flags: existing.flags,
                            });
                        }
                    }
                }
            }
        }

        let state_id = StateId::new(self.state_counter.fetch_add(1, Ordering::SeqCst));
        let open_state = OpenState::new(state_id, owner, inode_id, flags);

        let mut opens = self.opens.write();
        let mut opens_by_inode = self.opens_by_inode.write();

        opens.insert(state_id, open_state.clone());
        opens_by_inode
            .entry(inode_id)
            .or_insert_with(Vec::new)
            .push(state_id);

        debug!(inode = inode_id, "Opened file");
        Ok(open_state)
    }

    /// Close a file.
    pub fn close(&self, state_id: &StateId) -> bool {
        let mut opens = self.opens.write();
        if let Some(open_state) = opens.remove(state_id) {
            let mut opens_by_inode = self.opens_by_inode.write();
            if let Some(state_ids) = opens_by_inode.get_mut(&open_state.inode_id) {
                state_ids.retain(|id| id != state_id);
            }
            debug!(inode = open_state.inode_id, "Closed file");
            true
        } else {
            false
        }
    }

    /// Get an open state.
    pub fn get_open(&self, state_id: &StateId) -> Option<OpenState> {
        let opens = self.opens.read();
        opens.get(state_id).cloned()
    }

    /// Grant a delegation.
    pub fn grant_delegation(
        &self,
        client_id: ClientId,
        inode_id: InodeId,
        delegation_type: DelegationType,
    ) -> Option<Delegation> {
        // Check if delegation already exists
        {
            let delegations_by_inode = self.delegations_by_inode.read();
            if delegations_by_inode.contains_key(&inode_id) {
                return None;
            }
        }

        let state_id = StateId::new(self.state_counter.fetch_add(1, Ordering::SeqCst));
        let delegation = Delegation::new(state_id, client_id, inode_id, delegation_type);

        let mut delegations = self.delegations.write();
        let mut delegations_by_inode = self.delegations_by_inode.write();

        delegations.insert(state_id, delegation.clone());
        delegations_by_inode.insert(inode_id, state_id);

        info!(inode = inode_id, "Granted delegation");
        Some(delegation)
    }

    /// Return a delegation.
    pub fn return_delegation(&self, state_id: &StateId) -> bool {
        let mut delegations = self.delegations.write();
        if let Some(delegation) = delegations.remove(state_id) {
            let mut delegations_by_inode = self.delegations_by_inode.write();
            delegations_by_inode.remove(&delegation.inode_id);
            info!(inode = delegation.inode_id, "Returned delegation");
            true
        } else {
            false
        }
    }

    /// Recall a delegation (request client to return it).
    pub fn recall_delegation(&self, inode_id: InodeId) -> Option<Delegation> {
        let delegations_by_inode = self.delegations_by_inode.read();
        let state_id = delegations_by_inode.get(&inode_id)?;

        let mut delegations = self.delegations.write();
        if let Some(delegation) = delegations.get_mut(state_id) {
            delegation.request_recall();
            info!(inode = inode_id, "Requested delegation recall");
            return Some(delegation.clone());
        }
        None
    }

    /// Renew client lease.
    pub fn renew_lease(&self, client_id: ClientId) {
        let mut leases = self.leases.write();
        leases.insert(client_id, Instant::now() + self.lease_duration);
    }

    /// Check if client lease is valid.
    pub fn is_lease_valid(&self, client_id: &ClientId) -> bool {
        let leases = self.leases.read();
        leases
            .get(client_id)
            .map(|expiry| Instant::now() < *expiry)
            .unwrap_or(false)
    }

    /// Clean up expired state.
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut count = 0;

        // Clean up expired leases and their state
        let expired_clients: Vec<ClientId> = {
            let leases = self.leases.read();
            leases
                .iter()
                .filter(|(_, expiry)| now >= **expiry)
                .map(|(id, _)| *id)
                .collect()
        };

        for client_id in expired_clients {
            // Remove opens
            {
                let mut opens = self.opens.write();
                let mut opens_by_inode = self.opens_by_inode.write();

                let to_remove: Vec<StateId> = opens
                    .iter()
                    .filter(|(_, state)| state.owner.client_id == client_id)
                    .map(|(id, _)| *id)
                    .collect();

                for state_id in to_remove {
                    if let Some(open_state) = opens.remove(&state_id) {
                        if let Some(state_ids) = opens_by_inode.get_mut(&open_state.inode_id) {
                            state_ids.retain(|id| *id != state_id);
                        }
                        count += 1;
                    }
                }
            }

            // Remove delegations
            {
                let mut delegations = self.delegations.write();
                let mut delegations_by_inode = self.delegations_by_inode.write();

                let to_remove: Vec<StateId> = delegations
                    .iter()
                    .filter(|(_, deleg)| deleg.client_id == client_id)
                    .map(|(id, _)| *id)
                    .collect();

                for state_id in to_remove {
                    if let Some(deleg) = delegations.remove(&state_id) {
                        delegations_by_inode.remove(&deleg.inode_id);
                        count += 1;
                    }
                }
            }

            // Remove locks
            self.lock_manager.release_all(&StateOwner::new(client_id, vec![]));

            // Remove lease
            self.leases.write().remove(&client_id);
        }

        if count > 0 {
            warn!(count = count, "Cleaned up expired NFS state");
        }

        count
    }

    /// Get state statistics.
    pub fn stats(&self) -> StateStats {
        StateStats {
            open_count: self.opens.read().len(),
            lock_count: self.lock_manager.lock_count(),
            delegation_count: self.delegations.read().len(),
            lease_count: self.leases.read().len(),
        }
    }
}

/// Open conflict information.
#[derive(Debug, Clone)]
pub struct OpenConflict {
    /// Owner of conflicting open.
    pub owner: StateOwner,
    /// Flags of conflicting open.
    pub flags: OpenStateFlags,
}

/// State manager statistics.
#[derive(Debug, Clone)]
pub struct StateStats {
    /// Number of open files.
    pub open_count: usize,
    /// Number of locks.
    pub lock_count: usize,
    /// Number of delegations.
    pub delegation_count: usize,
    /// Number of active leases.
    pub lease_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_owner(n: u8) -> StateOwner {
        StateOwner::new(ClientId(n as u64), vec![n])
    }

    #[test]
    fn test_lock_range_overlap() {
        let range1 = LockRange::new(0, 100);
        let range2 = LockRange::new(50, 100);
        let range3 = LockRange::new(200, 50);

        assert!(range1.overlaps(&range2));
        assert!(!range1.overlaps(&range3));

        // Zero-length means to end of file
        let to_eof = LockRange::new(150, 0);
        assert!(to_eof.overlaps(&range3));
    }

    #[test]
    fn test_lock_manager() {
        let manager = LockManager::new();
        let owner1 = test_owner(1);
        let owner2 = test_owner(2);

        // Acquire read lock
        let lock1 = manager
            .lock(owner1.clone(), 100, LockType::Read, LockRange::new(0, 100))
            .unwrap();
        assert_eq!(lock1.inode_id, 100);

        // Same owner can acquire overlapping read lock
        let _lock2 = manager
            .lock(owner1.clone(), 100, LockType::Read, LockRange::new(50, 100))
            .unwrap();

        // Different owner can acquire non-overlapping lock
        let _lock3 = manager
            .lock(owner2.clone(), 100, LockType::Write, LockRange::new(200, 50))
            .unwrap();

        // Different owner cannot acquire overlapping write lock
        let result = manager.lock(owner2.clone(), 100, LockType::Write, LockRange::new(0, 50));
        assert!(result.is_err());
    }

    #[test]
    fn test_open_state_conflicts() {
        let flags1 = OpenStateFlags {
            read: true,
            write: false,
            deny_read: false,
            deny_write: true,
        };

        let flags2 = OpenStateFlags {
            read: false,
            write: true,
            deny_read: false,
            deny_write: false,
        };

        // flags1 denies write, flags2 wants write -> conflict
        assert!(flags1.conflicts_with(&flags2));
    }

    #[test]
    fn test_state_manager_opens() {
        let manager = StateManager::new(90);
        let owner = test_owner(1);

        // Open a file
        let flags = OpenStateFlags {
            read: true,
            write: false,
            deny_read: false,
            deny_write: false,
        };
        let open = manager.open(owner.clone(), 100, flags).unwrap();

        // Get the open state
        let retrieved = manager.get_open(&open.state_id).unwrap();
        assert_eq!(retrieved.inode_id, 100);

        // Close the file
        assert!(manager.close(&open.state_id));
        assert!(manager.get_open(&open.state_id).is_none());
    }

    #[test]
    fn test_delegation() {
        let manager = StateManager::new(90);
        let client = ClientId(1);

        // Grant delegation
        let deleg = manager
            .grant_delegation(client, 100, DelegationType::Read)
            .unwrap();
        assert_eq!(deleg.inode_id, 100);

        // Cannot grant another delegation for same file
        assert!(manager
            .grant_delegation(ClientId(2), 100, DelegationType::Read)
            .is_none());

        // Recall delegation
        let recalled = manager.recall_delegation(100).unwrap();
        assert!(recalled.recall_requested);

        // Return delegation
        assert!(manager.return_delegation(&deleg.state_id));
    }

    #[test]
    fn test_lease_management() {
        let manager = StateManager::new(90);
        let client = ClientId(1);

        // Initially no lease
        assert!(!manager.is_lease_valid(&client));

        // Renew lease
        manager.renew_lease(client);
        assert!(manager.is_lease_valid(&client));
    }
}
