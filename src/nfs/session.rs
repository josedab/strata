//! NFSv4.1 Session Management.
//!
//! Sessions provide exactly-once semantics for NFSv4.1 operations through
//! sequence IDs and slot-based request tracking.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Session ID (16 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NfsSessionId(pub [u8; 16]);

impl NfsSessionId {
    /// Generate a new random session ID.
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut bytes = [0u8; 16];

        // Use timestamp and random for uniqueness
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        bytes[0..8].copy_from_slice(&nanos.to_be_bytes());
        bytes[8..16].copy_from_slice(&rand_bytes());

        Self(bytes)
    }

    /// Create from bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl Default for NfsSessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate random bytes for session IDs.
fn rand_bytes() -> [u8; 8] {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    hasher.finish().to_be_bytes()
}

/// Slot sequence tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SlotSequence {
    /// Current sequence ID.
    pub sequence_id: u32,
    /// Last operation's sequence ID.
    pub last_sequence_id: u32,
    /// Whether the last reply is cached.
    pub cached: bool,
}

impl SlotSequence {
    /// Create a new slot sequence.
    pub fn new() -> Self {
        Self {
            sequence_id: 1,
            last_sequence_id: 0,
            cached: false,
        }
    }

    /// Check and advance the sequence ID.
    pub fn check_and_advance(&mut self, requested_seq: u32) -> SequenceResult {
        if requested_seq == self.sequence_id {
            // Expected sequence - advance
            self.last_sequence_id = self.sequence_id;
            self.sequence_id = self.sequence_id.wrapping_add(1);
            if self.sequence_id == 0 {
                self.sequence_id = 1;
            }
            self.cached = false;
            SequenceResult::Ok
        } else if requested_seq == self.last_sequence_id && self.cached {
            // Retry of last request - return cached result
            SequenceResult::CachedReply
        } else if requested_seq < self.sequence_id {
            // Old sequence - misordered
            SequenceResult::Misordered
        } else {
            // Future sequence - error
            SequenceResult::BadSequence
        }
    }

    /// Mark the current operation's reply as cached.
    pub fn cache_reply(&mut self) {
        self.cached = true;
    }
}

/// Result of sequence checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceResult {
    /// Sequence OK, proceed with operation.
    Ok,
    /// Return cached reply from previous operation.
    CachedReply,
    /// Sequence is misordered.
    Misordered,
    /// Bad sequence number.
    BadSequence,
}

/// Session slot for request tracking.
#[derive(Debug, Clone)]
pub struct SessionSlot {
    /// Slot ID.
    pub slot_id: u32,
    /// Sequence tracking.
    pub sequence: SlotSequence,
    /// Cached reply data.
    pub cached_reply: Option<Vec<u8>>,
    /// Last activity time.
    pub last_activity: Instant,
}

impl SessionSlot {
    /// Create a new session slot.
    pub fn new(slot_id: u32) -> Self {
        Self {
            slot_id,
            sequence: SlotSequence::new(),
            cached_reply: None,
            last_activity: Instant::now(),
        }
    }

    /// Process a sequence operation.
    pub fn process_sequence(&mut self, sequence_id: u32) -> SequenceResult {
        self.last_activity = Instant::now();
        self.sequence.check_and_advance(sequence_id)
    }

    /// Cache a reply for potential retransmission.
    pub fn cache_reply(&mut self, reply: Vec<u8>) {
        self.cached_reply = Some(reply);
        self.sequence.cache_reply();
    }

    /// Get the cached reply if available.
    pub fn get_cached_reply(&self) -> Option<&[u8]> {
        self.cached_reply.as_deref()
    }
}

/// NFS session state.
pub struct NfsSession {
    /// Session ID.
    pub id: NfsSessionId,
    /// Client ID.
    pub client_id: u64,
    /// Session slots for fore channel.
    slots: RwLock<HashMap<u32, SessionSlot>>,
    /// Maximum number of slots.
    max_slots: u32,
    /// Creation time.
    created_at: Instant,
    /// Last activity time.
    last_activity: RwLock<Instant>,
    /// Session flags.
    pub flags: SessionFlags,
}

/// Session flags.
#[derive(Debug, Clone, Copy, Default)]
pub struct SessionFlags {
    /// Persist session through server restart.
    pub persist: bool,
    /// Back channel required.
    pub back_channel: bool,
    /// RDMA supported.
    pub rdma: bool,
}

impl NfsSession {
    /// Create a new session.
    pub fn new(client_id: u64, max_slots: u32) -> Self {
        Self {
            id: NfsSessionId::new(),
            client_id,
            slots: RwLock::new(HashMap::new()),
            max_slots,
            created_at: Instant::now(),
            last_activity: RwLock::new(Instant::now()),
            flags: SessionFlags::default(),
        }
    }

    /// Get or create a slot.
    pub fn get_or_create_slot(&self, slot_id: u32) -> Option<SessionSlot> {
        if slot_id >= self.max_slots {
            return None;
        }

        let mut slots = self.slots.write();
        *self.last_activity.write() = Instant::now();

        Some(
            slots
                .entry(slot_id)
                .or_insert_with(|| SessionSlot::new(slot_id))
                .clone(),
        )
    }

    /// Update a slot after processing.
    pub fn update_slot(&self, slot: SessionSlot) {
        let mut slots = self.slots.write();
        slots.insert(slot.slot_id, slot);
    }

    /// Check if the session has expired.
    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_activity.read().elapsed() > timeout
    }

    /// Get session age.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get last activity time.
    pub fn last_activity(&self) -> Instant {
        *self.last_activity.read()
    }

    /// Touch the session to update last activity.
    pub fn touch(&self) {
        *self.last_activity.write() = Instant::now();
    }

    /// Get the number of active slots.
    pub fn active_slots(&self) -> usize {
        self.slots.read().len()
    }
}

/// Configuration for the session manager.
#[derive(Debug, Clone)]
pub struct SessionManagerConfig {
    /// Maximum number of sessions.
    pub max_sessions: usize,
    /// Session timeout in seconds.
    pub session_timeout_secs: u64,
    /// Maximum slots per session.
    pub max_slots_per_session: u32,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            session_timeout_secs: 300,
            max_slots_per_session: 64,
        }
    }
}

/// Session manager for tracking all active sessions.
pub struct SessionManager {
    /// Configuration.
    config: SessionManagerConfig,
    /// Active sessions indexed by session ID.
    sessions: RwLock<HashMap<NfsSessionId, Arc<NfsSession>>>,
    /// Client to session mapping.
    client_sessions: RwLock<HashMap<u64, Vec<NfsSessionId>>>,
}

impl SessionManager {
    /// Create a new session manager.
    pub fn new(config: SessionManagerConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            client_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new session for a client.
    pub fn create_session(&self, client_id: u64) -> Result<Arc<NfsSession>, SessionError> {
        let sessions = self.sessions.read();
        if sessions.len() >= self.config.max_sessions {
            return Err(SessionError::ResourceExhausted);
        }
        drop(sessions);

        let session = Arc::new(NfsSession::new(
            client_id,
            self.config.max_slots_per_session,
        ));

        let mut sessions = self.sessions.write();
        let mut client_sessions = self.client_sessions.write();

        sessions.insert(session.id, Arc::clone(&session));
        client_sessions
            .entry(client_id)
            .or_insert_with(Vec::new)
            .push(session.id);

        debug!(
            session_id = ?session.id,
            client_id = client_id,
            "Created new NFS session"
        );

        Ok(session)
    }

    /// Get an existing session.
    pub fn get_session(&self, session_id: &NfsSessionId) -> Option<Arc<NfsSession>> {
        let sessions = self.sessions.read();
        sessions.get(session_id).cloned()
    }

    /// Destroy a session.
    pub fn destroy_session(&self, session_id: &NfsSessionId) -> bool {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.remove(session_id) {
            let mut client_sessions = self.client_sessions.write();
            if let Some(client_sess) = client_sessions.get_mut(&session.client_id) {
                client_sess.retain(|id| id != session_id);
            }
            debug!(session_id = ?session_id, "Destroyed NFS session");
            true
        } else {
            false
        }
    }

    /// Get all sessions for a client.
    pub fn get_client_sessions(&self, client_id: u64) -> Vec<Arc<NfsSession>> {
        let client_sessions = self.client_sessions.read();
        let sessions = self.sessions.read();

        client_sessions
            .get(&client_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| sessions.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Clean up expired sessions.
    pub fn cleanup_expired(&self) -> usize {
        let timeout = Duration::from_secs(self.config.session_timeout_secs);
        let sessions = self.sessions.read();

        let expired: Vec<NfsSessionId> = sessions
            .iter()
            .filter(|(_, session)| session.is_expired(timeout))
            .map(|(id, _)| *id)
            .collect();

        drop(sessions);

        let mut count = 0;
        for session_id in expired {
            if self.destroy_session(&session_id) {
                count += 1;
            }
        }

        if count > 0 {
            warn!(count = count, "Cleaned up expired NFS sessions");
        }

        count
    }

    /// Get session statistics.
    pub fn stats(&self) -> SessionStats {
        let sessions = self.sessions.read();
        let client_sessions = self.client_sessions.read();

        let total_slots: usize = sessions.values().map(|s| s.active_slots()).sum();

        SessionStats {
            active_sessions: sessions.len(),
            active_clients: client_sessions.len(),
            total_slots,
            max_sessions: self.config.max_sessions,
        }
    }
}

/// Session manager statistics.
#[derive(Debug, Clone)]
pub struct SessionStats {
    /// Number of active sessions.
    pub active_sessions: usize,
    /// Number of clients with sessions.
    pub active_clients: usize,
    /// Total active slots across all sessions.
    pub total_slots: usize,
    /// Maximum allowed sessions.
    pub max_sessions: usize,
}

/// Session-related errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    /// Session not found.
    NotFound,
    /// Maximum sessions reached.
    ResourceExhausted,
    /// Session expired.
    Expired,
    /// Invalid slot.
    BadSlot,
    /// Sequence error.
    SequenceError(SequenceResult),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "Session not found"),
            Self::ResourceExhausted => write!(f, "Maximum sessions reached"),
            Self::Expired => write!(f, "Session expired"),
            Self::BadSlot => write!(f, "Invalid slot"),
            Self::SequenceError(result) => write!(f, "Sequence error: {:?}", result),
        }
    }
}

impl std::error::Error for SessionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_id_generation() {
        let id1 = NfsSessionId::new();
        let id2 = NfsSessionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_slot_sequence() {
        let mut seq = SlotSequence::new();
        assert_eq!(seq.sequence_id, 1);

        // Normal advance
        assert_eq!(seq.check_and_advance(1), SequenceResult::Ok);
        assert_eq!(seq.sequence_id, 2);

        // Retry with cached reply (last_sequence_id is 1, sequence_id is 2)
        seq.cache_reply();
        assert_eq!(seq.check_and_advance(1), SequenceResult::CachedReply);

        // Another retry should still return CachedReply (reply is still cached)
        assert_eq!(seq.check_and_advance(1), SequenceResult::CachedReply);

        // Advance with correct sequence
        assert_eq!(seq.check_and_advance(2), SequenceResult::Ok);

        // Now sequence_id is 3, last_sequence_id is 2
        // Old sequence (1) should be misordered since it's not the last one
        assert_eq!(seq.check_and_advance(1), SequenceResult::Misordered);

        // Future sequence
        assert_eq!(seq.check_and_advance(100), SequenceResult::BadSequence);
    }

    #[test]
    fn test_session_slot() {
        let mut slot = SessionSlot::new(0);
        assert_eq!(slot.process_sequence(1), SequenceResult::Ok);

        slot.cache_reply(vec![1, 2, 3]);
        assert_eq!(slot.get_cached_reply(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn test_session_manager() {
        let config = SessionManagerConfig {
            max_sessions: 10,
            session_timeout_secs: 300,
            max_slots_per_session: 8,
        };
        let manager = SessionManager::new(config);

        // Create session
        let session = manager.create_session(1).unwrap();
        assert_eq!(session.client_id, 1);

        // Get session
        let retrieved = manager.get_session(&session.id).unwrap();
        assert_eq!(retrieved.id, session.id);

        // Get client sessions
        let client_sessions = manager.get_client_sessions(1);
        assert_eq!(client_sessions.len(), 1);

        // Stats
        let stats = manager.stats();
        assert_eq!(stats.active_sessions, 1);
        assert_eq!(stats.active_clients, 1);

        // Destroy session
        assert!(manager.destroy_session(&session.id));
        assert!(manager.get_session(&session.id).is_none());
    }

    #[test]
    fn test_session_max_slots() {
        let session = NfsSession::new(1, 4);

        // Valid slot
        assert!(session.get_or_create_slot(0).is_some());
        assert!(session.get_or_create_slot(3).is_some());

        // Invalid slot
        assert!(session.get_or_create_slot(4).is_none());
        assert!(session.get_or_create_slot(100).is_none());
    }
}
