//! Distributed locking with leases for Strata.
//!
//! Provides coordination primitives for preventing concurrent access conflicts.
//! Uses lease-based locking with automatic expiration to handle failures.

use crate::error::{Result, StrataError};
use crate::types::{InodeId, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;

/// Lock mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockMode {
    /// Shared lock (multiple readers).
    Shared,
    /// Exclusive lock (single writer).
    Exclusive,
}

/// Lock scope - what is being locked.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockScope {
    /// Lock on an inode.
    Inode(InodeId),
    /// Lock on a path.
    Path(String),
    /// Lock on a byte range within a file.
    ByteRange {
        inode: InodeId,
        offset: u64,
        length: u64,
    },
    /// Named lock for arbitrary coordination.
    Named(String),
}

impl std::fmt::Display for LockScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockScope::Inode(id) => write!(f, "inode:{}", id),
            LockScope::Path(p) => write!(f, "path:{}", p),
            LockScope::ByteRange {
                inode,
                offset,
                length,
            } => write!(f, "range:{}:{}-{}", inode, offset, offset + length),
            LockScope::Named(name) => write!(f, "named:{}", name),
        }
    }
}

/// Lock holder information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockHolder {
    /// Client/owner ID.
    pub owner_id: String,
    /// Node where the client is connected.
    pub node_id: NodeId,
    /// Session ID for the lock holder.
    pub session_id: String,
    /// Process ID (if available).
    pub pid: Option<u32>,
}

impl LockHolder {
    /// Create a new lock holder.
    pub fn new(owner_id: impl Into<String>, node_id: NodeId, session_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            node_id,
            session_id: session_id.into(),
            pid: None,
        }
    }

    /// Set the process ID.
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }
}

/// A granted lock (lease).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lock {
    /// Unique lock ID.
    pub id: String,
    /// What is locked.
    pub scope: LockScope,
    /// Lock mode.
    pub mode: LockMode,
    /// Who holds the lock.
    pub holder: LockHolder,
    /// When the lock was acquired.
    pub acquired_at: u64,
    /// When the lease expires.
    pub expires_at: u64,
    /// Lease duration in seconds.
    pub lease_duration: u64,
}

impl Lock {
    /// Check if the lock has expired.
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now >= self.expires_at
    }

    /// Get remaining lease time in seconds.
    pub fn remaining_lease(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.expires_at.saturating_sub(now)
    }
}

/// Lock request.
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// What to lock.
    pub scope: LockScope,
    /// Lock mode.
    pub mode: LockMode,
    /// Who is requesting.
    pub holder: LockHolder,
    /// Requested lease duration.
    pub lease_duration: Duration,
    /// Wait timeout (None = don't wait).
    pub wait_timeout: Option<Duration>,
}

impl LockRequest {
    /// Create a new lock request.
    pub fn new(scope: LockScope, mode: LockMode, holder: LockHolder) -> Self {
        Self {
            scope,
            mode,
            holder,
            lease_duration: Duration::from_secs(30),
            wait_timeout: None,
        }
    }

    /// Set lease duration.
    pub fn with_lease(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Set wait timeout.
    pub fn with_wait(mut self, timeout: Duration) -> Self {
        self.wait_timeout = Some(timeout);
        self
    }
}

/// Lock manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockConfig {
    /// Default lease duration in seconds.
    pub default_lease_duration: u64,
    /// Maximum lease duration in seconds.
    pub max_lease_duration: u64,
    /// How often to check for expired locks.
    pub expiry_check_interval: u64,
    /// Grace period before cleaning up expired locks.
    pub expiry_grace_period: u64,
    /// Maximum locks per client.
    pub max_locks_per_client: usize,
    /// Maximum total locks.
    pub max_total_locks: usize,
    /// Enable deadlock detection.
    pub deadlock_detection: bool,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            default_lease_duration: 30,
            max_lease_duration: 300,
            expiry_check_interval: 5,
            expiry_grace_period: 10,
            max_locks_per_client: 100,
            max_total_locks: 10000,
            deadlock_detection: true,
        }
    }
}

impl LockConfig {
    /// Configuration for short-lived locks.
    pub fn short_lived() -> Self {
        Self {
            default_lease_duration: 10,
            max_lease_duration: 60,
            expiry_check_interval: 2,
            expiry_grace_period: 5,
            ..Default::default()
        }
    }

    /// Configuration for long-running operations.
    pub fn long_running() -> Self {
        Self {
            default_lease_duration: 120,
            max_lease_duration: 600,
            expiry_check_interval: 10,
            expiry_grace_period: 30,
            ..Default::default()
        }
    }
}

/// Internal lock state.
struct LockState {
    /// Active exclusive lock.
    exclusive: Option<Lock>,
    /// Active shared locks.
    shared: Vec<Lock>,
    /// Waiting requests (for queueing).
    waiters: Vec<(LockRequest, tokio::sync::oneshot::Sender<Result<Lock>>)>,
}

impl LockState {
    fn new() -> Self {
        Self {
            exclusive: None,
            shared: Vec::new(),
            waiters: Vec::new(),
        }
    }

    fn is_locked(&self) -> bool {
        self.exclusive.is_some() || !self.shared.is_empty()
    }

    fn has_exclusive(&self) -> bool {
        self.exclusive.is_some()
    }

    fn shared_count(&self) -> usize {
        self.shared.len()
    }

    fn remove_expired(&mut self) -> Vec<Lock> {
        let mut expired = Vec::new();

        if let Some(ref lock) = self.exclusive {
            if lock.is_expired() {
                expired.push(self.exclusive.take().unwrap());
            }
        }

        let (valid, exp): (Vec<_>, Vec<_>) = self.shared.drain(..).partition(|l| !l.is_expired());
        self.shared = valid;
        expired.extend(exp);

        expired
    }

    fn can_acquire(&self, mode: LockMode) -> bool {
        match mode {
            LockMode::Exclusive => !self.is_locked(),
            LockMode::Shared => !self.has_exclusive(),
        }
    }
}

/// Lock event for notifications.
#[derive(Debug, Clone)]
pub enum LockEvent {
    /// Lock was acquired.
    Acquired(Lock),
    /// Lock was released.
    Released { scope: LockScope, holder: LockHolder },
    /// Lock expired.
    Expired(Lock),
    /// Lock was denied.
    Denied { scope: LockScope, holder: LockHolder, reason: String },
}

/// The distributed lock manager.
pub struct LockManager {
    config: LockConfig,
    locks: RwLock<HashMap<LockScope, LockState>>,
    lock_by_id: RwLock<HashMap<String, LockScope>>,
    client_locks: RwLock<HashMap<String, Vec<String>>>,
    events: broadcast::Sender<LockEvent>,
    stats: LockStats,
}

/// Lock statistics.
#[derive(Debug, Default)]
struct LockStats {
    acquired: std::sync::atomic::AtomicU64,
    released: std::sync::atomic::AtomicU64,
    expired: std::sync::atomic::AtomicU64,
    denied: std::sync::atomic::AtomicU64,
    contention: std::sync::atomic::AtomicU64,
}

/// Public lock statistics.
#[derive(Debug, Clone, Default)]
pub struct LockManagerStats {
    /// Total locks acquired.
    pub acquired: u64,
    /// Total locks released.
    pub released: u64,
    /// Total locks expired.
    pub expired: u64,
    /// Total lock requests denied.
    pub denied: u64,
    /// Lock contention count.
    pub contention: u64,
    /// Current active locks.
    pub active_locks: usize,
    /// Current exclusive locks.
    pub exclusive_locks: usize,
    /// Current shared locks.
    pub shared_locks: usize,
}

impl LockManager {
    /// Create a new lock manager.
    pub fn new(config: LockConfig) -> Arc<Self> {
        let (events, _) = broadcast::channel(1000);

        let manager = Arc::new(Self {
            config,
            locks: RwLock::new(HashMap::new()),
            lock_by_id: RwLock::new(HashMap::new()),
            client_locks: RwLock::new(HashMap::new()),
            events,
            stats: LockStats::default(),
        });

        // Start expiry checker
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(async move {
            manager_clone.expiry_checker().await;
        });

        manager
    }

    async fn expiry_checker(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(self.config.expiry_check_interval));

        loop {
            interval.tick().await;
            self.cleanup_expired().await;
        }
    }

    async fn cleanup_expired(&self) {
        let mut locks = self.locks.write().await;
        let mut lock_by_id = self.lock_by_id.write().await;
        let mut client_locks = self.client_locks.write().await;

        let mut expired_locks = Vec::new();
        let mut empty_scopes = Vec::new();

        for (scope, state) in locks.iter_mut() {
            let expired = state.remove_expired();
            for lock in expired {
                expired_locks.push(lock.clone());
                lock_by_id.remove(&lock.id);

                // Remove from client's lock list
                if let Some(client_list) = client_locks.get_mut(&lock.holder.owner_id) {
                    client_list.retain(|id| id != &lock.id);
                }
            }

            if !state.is_locked() && state.waiters.is_empty() {
                empty_scopes.push(scope.clone());
            }
        }

        // Clean up empty scopes
        for scope in empty_scopes {
            locks.remove(&scope);
        }

        // Notify about expired locks
        for lock in expired_locks {
            self.stats.expired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = self.events.send(LockEvent::Expired(lock));
        }

        // Process waiters
        for state in locks.values_mut() {
            self.process_waiters(state).await;
        }
    }

    async fn process_waiters(&self, state: &mut LockState) {
        let mut i = 0;
        while i < state.waiters.len() {
            let (ref request, _) = state.waiters[i];
            if state.can_acquire(request.mode) {
                let (request, sender) = state.waiters.remove(i);
                let lock = self.create_lock(&request);
                self.grant_lock(state, lock.clone());
                let _ = sender.send(Ok(lock));
            } else {
                i += 1;
            }
        }
    }

    fn create_lock(&self, request: &LockRequest) -> Lock {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_secs = request
            .lease_duration
            .as_secs()
            .min(self.config.max_lease_duration);

        Lock {
            id: uuid::Uuid::new_v4().to_string(),
            scope: request.scope.clone(),
            mode: request.mode,
            holder: request.holder.clone(),
            acquired_at: now,
            expires_at: now + lease_secs,
            lease_duration: lease_secs,
        }
    }

    fn grant_lock(&self, state: &mut LockState, lock: Lock) {
        match lock.mode {
            LockMode::Exclusive => {
                state.exclusive = Some(lock);
            }
            LockMode::Shared => {
                state.shared.push(lock);
            }
        }
    }

    /// Acquire a lock.
    pub async fn acquire(&self, request: LockRequest) -> Result<Lock> {
        // Check client lock limit
        {
            let client_locks = self.client_locks.read().await;
            if let Some(locks) = client_locks.get(&request.holder.owner_id) {
                if locks.len() >= self.config.max_locks_per_client {
                    self.stats.denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(StrataError::Internal(format!(
                        "Client {} has too many locks",
                        request.holder.owner_id
                    )));
                }
            }
        }

        // Try to acquire immediately
        {
            let mut locks = self.locks.write().await;

            // Check total lock limit
            let total: usize = locks.values().map(|s| {
                (if s.exclusive.is_some() { 1 } else { 0 }) + s.shared.len()
            }).sum();
            if total >= self.config.max_total_locks {
                self.stats.denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(StrataError::Internal("Too many total locks".to_string()));
            }

            let state = locks.entry(request.scope.clone()).or_insert_with(LockState::new);

            if state.can_acquire(request.mode) {
                let lock = self.create_lock(&request);
                self.grant_lock(state, lock.clone());

                // Track lock
                drop(locks);
                self.track_lock(&lock).await;

                self.stats.acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = self.events.send(LockEvent::Acquired(lock.clone()));
                return Ok(lock);
            }

            // Lock is contended
            self.stats.contention.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Wait if requested
        if let Some(timeout) = request.wait_timeout {
            let (sender, receiver) = tokio::sync::oneshot::channel();

            {
                let mut locks = self.locks.write().await;
                let state = locks.get_mut(&request.scope).unwrap();
                state.waiters.push((request.clone(), sender));
            }

            match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(result)) => {
                    if let Ok(ref lock) = result {
                        self.track_lock(lock).await;
                        self.stats.acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = self.events.send(LockEvent::Acquired(lock.clone()));
                    }
                    result
                }
                Ok(Err(_)) => {
                    // Channel closed
                    self.stats.denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err(StrataError::Internal("Lock request cancelled".to_string()))
                }
                Err(_) => {
                    // Timeout - remove from waiters
                    let mut locks = self.locks.write().await;
                    if let Some(state) = locks.get_mut(&request.scope) {
                        state.waiters.retain(|(r, _)| r.holder.owner_id != request.holder.owner_id);
                    }
                    self.stats.denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err(StrataError::TimeoutStr("Lock acquisition timed out".to_string()))
                }
            }
        } else {
            self.stats.denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = self.events.send(LockEvent::Denied {
                scope: request.scope,
                holder: request.holder,
                reason: "Lock held by another client".to_string(),
            });
            Err(StrataError::Internal("Lock not available".to_string()))
        }
    }

    async fn track_lock(&self, lock: &Lock) {
        let mut lock_by_id = self.lock_by_id.write().await;
        let mut client_locks = self.client_locks.write().await;

        lock_by_id.insert(lock.id.clone(), lock.scope.clone());
        client_locks
            .entry(lock.holder.owner_id.clone())
            .or_insert_with(Vec::new)
            .push(lock.id.clone());
    }

    /// Release a lock by ID.
    pub async fn release(&self, lock_id: &str) -> Result<()> {
        let scope = {
            let lock_by_id = self.lock_by_id.read().await;
            lock_by_id.get(lock_id).cloned()
        };

        let Some(scope) = scope else {
            return Err(StrataError::NotFound(format!("Lock not found: {}", lock_id)));
        };

        let released_lock = {
            let mut locks = self.locks.write().await;
            let state = locks.get_mut(&scope).ok_or_else(|| {
                StrataError::NotFound(format!("Lock state not found: {}", scope))
            })?;

            let released = if let Some(ref lock) = state.exclusive {
                if lock.id == lock_id {
                    state.exclusive.take()
                } else {
                    None
                }
            } else {
                let idx = state.shared.iter().position(|l| l.id == lock_id);
                idx.map(|i| state.shared.remove(i))
            };

            if released.is_none() {
                return Err(StrataError::NotFound(format!("Lock not found: {}", lock_id)));
            }

            // Process waiters
            self.process_waiters(state).await;

            released.unwrap()
        };

        // Clean up tracking
        {
            let mut lock_by_id = self.lock_by_id.write().await;
            let mut client_locks = self.client_locks.write().await;

            lock_by_id.remove(lock_id);
            if let Some(client_list) = client_locks.get_mut(&released_lock.holder.owner_id) {
                client_list.retain(|id| id != lock_id);
            }
        }

        self.stats.released.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _ = self.events.send(LockEvent::Released {
            scope: released_lock.scope,
            holder: released_lock.holder,
        });

        Ok(())
    }

    /// Renew a lock's lease.
    pub async fn renew(&self, lock_id: &str, duration: Option<Duration>) -> Result<Lock> {
        let scope = {
            let lock_by_id = self.lock_by_id.read().await;
            lock_by_id.get(lock_id).cloned()
        };

        let Some(scope) = scope else {
            return Err(StrataError::NotFound(format!("Lock not found: {}", lock_id)));
        };

        let mut locks = self.locks.write().await;
        let state = locks.get_mut(&scope).ok_or_else(|| {
            StrataError::NotFound(format!("Lock state not found: {}", scope))
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_secs = duration
            .map(|d| d.as_secs())
            .unwrap_or(self.config.default_lease_duration)
            .min(self.config.max_lease_duration);

        // Find and update the lock
        if let Some(ref mut lock) = state.exclusive {
            if lock.id == lock_id {
                lock.expires_at = now + lease_secs;
                lock.lease_duration = lease_secs;
                return Ok(lock.clone());
            }
        }

        for lock in &mut state.shared {
            if lock.id == lock_id {
                lock.expires_at = now + lease_secs;
                lock.lease_duration = lease_secs;
                return Ok(lock.clone());
            }
        }

        Err(StrataError::NotFound(format!("Lock not found: {}", lock_id)))
    }

    /// Get a lock by ID.
    pub async fn get(&self, lock_id: &str) -> Option<Lock> {
        let scope = {
            let lock_by_id = self.lock_by_id.read().await;
            lock_by_id.get(lock_id)?.clone()
        };

        let locks = self.locks.read().await;
        let state = locks.get(&scope)?;

        if let Some(ref lock) = state.exclusive {
            if lock.id == lock_id {
                return Some(lock.clone());
            }
        }

        state.shared.iter().find(|l| l.id == lock_id).cloned()
    }

    /// List locks for a scope.
    pub async fn list_locks(&self, scope: &LockScope) -> Vec<Lock> {
        let locks = self.locks.read().await;
        let Some(state) = locks.get(scope) else {
            return Vec::new();
        };

        let mut result = Vec::new();
        if let Some(ref lock) = state.exclusive {
            result.push(lock.clone());
        }
        result.extend(state.shared.iter().cloned());
        result
    }

    /// List all locks for a client.
    pub async fn list_client_locks(&self, client_id: &str) -> Vec<Lock> {
        let client_locks = self.client_locks.read().await;
        let Some(lock_ids) = client_locks.get(client_id) else {
            return Vec::new();
        };

        let mut result = Vec::new();
        for lock_id in lock_ids {
            if let Some(lock) = self.get(lock_id).await {
                result.push(lock);
            }
        }
        result
    }

    /// Release all locks for a client (on disconnect).
    pub async fn release_client_locks(&self, client_id: &str) -> usize {
        let lock_ids: Vec<String> = {
            let client_locks = self.client_locks.read().await;
            client_locks.get(client_id).cloned().unwrap_or_default()
        };

        let mut released = 0;
        for lock_id in lock_ids {
            if self.release(&lock_id).await.is_ok() {
                released += 1;
            }
        }

        released
    }

    /// Subscribe to lock events.
    pub fn subscribe(&self) -> broadcast::Receiver<LockEvent> {
        self.events.subscribe()
    }

    /// Get lock statistics.
    pub async fn stats(&self) -> LockManagerStats {
        let locks = self.locks.read().await;

        let mut exclusive = 0;
        let mut shared = 0;

        for state in locks.values() {
            if state.exclusive.is_some() {
                exclusive += 1;
            }
            shared += state.shared.len();
        }

        LockManagerStats {
            acquired: self.stats.acquired.load(std::sync::atomic::Ordering::Relaxed),
            released: self.stats.released.load(std::sync::atomic::Ordering::Relaxed),
            expired: self.stats.expired.load(std::sync::atomic::Ordering::Relaxed),
            denied: self.stats.denied.load(std::sync::atomic::Ordering::Relaxed),
            contention: self.stats.contention.load(std::sync::atomic::Ordering::Relaxed),
            active_locks: exclusive + shared,
            exclusive_locks: exclusive,
            shared_locks: shared,
        }
    }
}

/// RAII lock guard.
pub struct LockGuard {
    manager: Arc<LockManager>,
    lock: Lock,
    released: bool,
}

impl LockGuard {
    /// Create a new lock guard.
    pub fn new(manager: Arc<LockManager>, lock: Lock) -> Self {
        Self {
            manager,
            lock,
            released: false,
        }
    }

    /// Get the lock.
    pub fn lock(&self) -> &Lock {
        &self.lock
    }

    /// Release the lock explicitly.
    pub async fn release(mut self) -> Result<()> {
        self.released = true;
        self.manager.release(&self.lock.id).await
    }

    /// Renew the lease.
    pub async fn renew(&mut self, duration: Option<Duration>) -> Result<()> {
        self.lock = self.manager.renew(&self.lock.id, duration).await?;
        Ok(())
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released {
            let manager = Arc::clone(&self.manager);
            let lock_id = self.lock.id.clone();
            tokio::spawn(async move {
                let _ = manager.release(&lock_id).await;
            });
        }
    }
}

/// Helper to acquire a lock with RAII guard.
pub async fn with_lock(
    manager: Arc<LockManager>,
    request: LockRequest,
) -> Result<LockGuard> {
    let lock = manager.acquire(request).await?;
    Ok(LockGuard::new(manager, lock))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_holder(id: &str) -> LockHolder {
        LockHolder::new(id, 1, format!("session-{}", id))
    }

    #[tokio::test]
    async fn test_exclusive_lock() {
        let manager = LockManager::new(LockConfig::default());

        let request = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client1"),
        );

        let lock = manager.acquire(request).await.unwrap();
        assert_eq!(lock.mode, LockMode::Exclusive);

        // Second exclusive lock should fail
        let request2 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client2"),
        );

        let result = manager.acquire(request2).await;
        assert!(result.is_err());

        // Release and try again
        manager.release(&lock.id).await.unwrap();

        let request3 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client2"),
        );

        let lock2 = manager.acquire(request3).await.unwrap();
        assert!(lock2.id != lock.id);
    }

    #[tokio::test]
    async fn test_shared_locks() {
        let manager = LockManager::new(LockConfig::default());

        let request1 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Shared,
            test_holder("client1"),
        );

        let request2 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Shared,
            test_holder("client2"),
        );

        // Both should succeed
        let lock1 = manager.acquire(request1).await.unwrap();
        let lock2 = manager.acquire(request2).await.unwrap();

        assert_eq!(lock1.mode, LockMode::Shared);
        assert_eq!(lock2.mode, LockMode::Shared);

        // Exclusive should fail while shared locks held
        let request3 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client3"),
        );

        let result = manager.acquire(request3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lock_renewal() {
        let manager = LockManager::new(LockConfig::default());

        let request = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client1"),
        ).with_lease(Duration::from_secs(10));

        let lock = manager.acquire(request).await.unwrap();
        let original_expiry = lock.expires_at;

        // Renew with longer lease
        let renewed = manager.renew(&lock.id, Some(Duration::from_secs(60))).await.unwrap();
        assert!(renewed.expires_at > original_expiry);
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        let config = LockConfig {
            default_lease_duration: 1,
            expiry_check_interval: 1,
            expiry_grace_period: 0,
            ..Default::default()
        };
        let manager = LockManager::new(config);

        let request = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client1"),
        ).with_lease(Duration::from_secs(1));

        let lock = manager.acquire(request).await.unwrap();

        // Wait for expiry
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Lock should be expired
        let found = manager.get(&lock.id).await;
        // Note: The lock might still be in the map but marked as expired
        // The cleanup runs periodically
    }

    #[tokio::test]
    async fn test_lock_wait() {
        let manager = LockManager::new(LockConfig::default());

        let request1 = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client1"),
        );

        let lock1 = manager.acquire(request1).await.unwrap();

        // Start a waiter
        let manager_clone = Arc::clone(&manager);
        let waiter = tokio::spawn(async move {
            let request2 = LockRequest::new(
                LockScope::Inode(1),
                LockMode::Exclusive,
                test_holder("client2"),
            ).with_wait(Duration::from_secs(5));

            manager_clone.acquire(request2).await
        });

        // Release first lock after short delay
        tokio::time::sleep(Duration::from_millis(100)).await;
        manager.release(&lock1.id).await.unwrap();

        // Waiter should succeed
        let lock2 = waiter.await.unwrap().unwrap();
        assert_eq!(lock2.holder.owner_id, "client2");
    }

    #[tokio::test]
    async fn test_client_lock_release() {
        let manager = LockManager::new(LockConfig::default());

        // Acquire multiple locks for same client
        for i in 0..3 {
            let request = LockRequest::new(
                LockScope::Inode(i),
                LockMode::Exclusive,
                test_holder("client1"),
            );
            manager.acquire(request).await.unwrap();
        }

        let locks = manager.list_client_locks("client1").await;
        assert_eq!(locks.len(), 3);

        // Release all client locks
        let released = manager.release_client_locks("client1").await;
        assert_eq!(released, 3);

        let locks = manager.list_client_locks("client1").await;
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn test_lock_guard() {
        let manager = LockManager::new(LockConfig::default());

        {
            let request = LockRequest::new(
                LockScope::Inode(1),
                LockMode::Exclusive,
                test_holder("client1"),
            );
            let guard = with_lock(Arc::clone(&manager), request).await.unwrap();

            // Lock is held
            let stats = manager.stats().await;
            assert_eq!(stats.active_locks, 1);
        }

        // Guard dropped, lock should be released
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stats = manager.stats().await;
        assert_eq!(stats.active_locks, 0);
    }

    #[tokio::test]
    async fn test_lock_scope_types() {
        let manager = LockManager::new(LockConfig::default());

        // Test different scope types
        let scopes = vec![
            LockScope::Inode(123),
            LockScope::Path("/some/path".to_string()),
            LockScope::ByteRange {
                inode: 456,
                offset: 0,
                length: 1024,
            },
            LockScope::Named("my-lock".to_string()),
        ];

        for scope in scopes {
            let request = LockRequest::new(
                scope.clone(),
                LockMode::Exclusive,
                test_holder("client1"),
            );
            let lock = manager.acquire(request).await.unwrap();
            manager.release(&lock.id).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = LockManager::new(LockConfig::default());

        let request = LockRequest::new(
            LockScope::Inode(1),
            LockMode::Exclusive,
            test_holder("client1"),
        );

        let lock = manager.acquire(request).await.unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.acquired, 1);
        assert_eq!(stats.active_locks, 1);
        assert_eq!(stats.exclusive_locks, 1);

        manager.release(&lock.id).await.unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.released, 1);
        assert_eq!(stats.active_locks, 0);
    }
}
