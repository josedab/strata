//! Real-Time Collaboration with CRDTs
//!
//! This module provides Conflict-free Replicated Data Types (CRDTs) for
//! real-time collaborative editing and metadata synchronization. Features:
//! - Automatic conflict resolution without coordination
//! - Support for concurrent edits from multiple users
//! - Eventually consistent distributed state
//! - Operational transformation for text editing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    CRDT Framework                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  G-Counter │ PN-Counter │ LWW-Register │ OR-Set            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  RGA Sequence │ LWW-Map │ MV-Register                       │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Sync Protocol │ Delta Encoding │ Causal Ordering           │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::types::NodeId;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Unique replica identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicaId(pub u64);

impl ReplicaId {
    pub fn new() -> Self {
        Self(rand::random())
    }

    pub fn from_node(node_id: NodeId) -> Self {
        Self(node_id)
    }
}

impl Default for ReplicaId {
    fn default() -> Self {
        Self::new()
    }
}

/// Lamport timestamp for causal ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LamportClock {
    pub time: u64,
    pub replica: ReplicaId,
}

impl LamportClock {
    pub fn new(replica: ReplicaId) -> Self {
        Self { time: 0, replica }
    }

    pub fn tick(&mut self) -> Self {
        self.time += 1;
        *self
    }

    pub fn update(&mut self, other: &Self) {
        self.time = self.time.max(other.time) + 1;
    }

    pub fn merge(&mut self, other: &Self) {
        self.time = self.time.max(other.time);
    }
}

/// Vector clock for tracking causality
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct VectorClock {
    clocks: BTreeMap<ReplicaId, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, replica: ReplicaId) -> u64 {
        let entry = self.clocks.entry(replica).or_insert(0);
        *entry += 1;
        *entry
    }

    pub fn get(&self, replica: &ReplicaId) -> u64 {
        *self.clocks.get(replica).unwrap_or(&0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (replica, &time) in &other.clocks {
            let entry = self.clocks.entry(*replica).or_insert(0);
            *entry = (*entry).max(time);
        }
    }

    /// Check if self happened before other
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let mut dominated = false;
        for (replica, &time) in &self.clocks {
            let other_time = other.get(replica);
            if time > other_time {
                return false;
            }
            if time < other_time {
                dominated = true;
            }
        }
        for (replica, &time) in &other.clocks {
            if !self.clocks.contains_key(replica) && time > 0 {
                dominated = true;
            }
        }
        dominated
    }

    /// Check if two clocks are concurrent (neither happened before the other)
    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self)
    }
}

/// Trait for CRDT types
pub trait Crdt: Clone + Send + Sync {
    /// The operation type for state-based CRDTs
    type Delta;

    /// Merge another CRDT state into this one
    fn merge(&mut self, other: &Self);

    /// Generate a delta for synchronization
    fn delta(&self, since: &VectorClock) -> Option<Self::Delta>;

    /// Apply a delta update
    fn apply_delta(&mut self, delta: Self::Delta);
}

/// G-Counter: Grow-only counter
#[derive(Debug, Clone, Default)]
pub struct GCounter {
    counts: BTreeMap<ReplicaId, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, replica: ReplicaId) {
        *self.counts.entry(replica).or_insert(0) += 1;
    }

    pub fn increment_by(&mut self, replica: ReplicaId, amount: u64) {
        *self.counts.entry(replica).or_insert(0) += amount;
    }

    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }
}

impl Crdt for GCounter {
    type Delta = BTreeMap<ReplicaId, u64>;

    fn merge(&mut self, other: &Self) {
        for (replica, &count) in &other.counts {
            let entry = self.counts.entry(*replica).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    fn delta(&self, _since: &VectorClock) -> Option<Self::Delta> {
        Some(self.counts.clone())
    }

    fn apply_delta(&mut self, delta: Self::Delta) {
        for (replica, count) in delta {
            let entry = self.counts.entry(replica).or_insert(0);
            *entry = (*entry).max(count);
        }
    }
}

/// PN-Counter: Positive-Negative counter (supports increment and decrement)
#[derive(Debug, Clone, Default)]
pub struct PNCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, replica: ReplicaId) {
        self.positive.increment(replica);
    }

    pub fn decrement(&mut self, replica: ReplicaId) {
        self.negative.increment(replica);
    }

    pub fn value(&self) -> i64 {
        self.positive.value() as i64 - self.negative.value() as i64
    }
}

impl Crdt for PNCounter {
    type Delta = (BTreeMap<ReplicaId, u64>, BTreeMap<ReplicaId, u64>);

    fn merge(&mut self, other: &Self) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }

    fn delta(&self, since: &VectorClock) -> Option<Self::Delta> {
        Some((
            self.positive.delta(since)?,
            self.negative.delta(since)?,
        ))
    }

    fn apply_delta(&mut self, delta: Self::Delta) {
        self.positive.apply_delta(delta.0);
        self.negative.apply_delta(delta.1);
    }
}

/// LWW-Register: Last-Writer-Wins Register
#[derive(Debug, Clone)]
pub struct LWWRegister<T: Clone + Send + Sync> {
    value: Option<T>,
    timestamp: LamportClock,
}

impl<T: Clone + Send + Sync> LWWRegister<T> {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            value: None,
            timestamp: LamportClock::new(replica),
        }
    }

    pub fn set(&mut self, value: T) {
        self.timestamp.tick();
        self.value = Some(value);
    }

    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn timestamp(&self) -> LamportClock {
        self.timestamp
    }
}

impl<T: Clone + Send + Sync> Crdt for LWWRegister<T> {
    type Delta = (Option<T>, LamportClock);

    fn merge(&mut self, other: &Self) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
        }
    }

    fn delta(&self, _since: &VectorClock) -> Option<Self::Delta> {
        Some((self.value.clone(), self.timestamp))
    }

    fn apply_delta(&mut self, delta: Self::Delta) {
        if delta.1 > self.timestamp {
            self.value = delta.0;
            self.timestamp = delta.1;
        }
    }
}

/// MV-Register: Multi-Value Register (keeps concurrent values)
#[derive(Debug, Clone)]
pub struct MVRegister<T: Clone + Eq + Hash + Send + Sync> {
    values: HashSet<(T, VectorClock)>,
    clock: VectorClock,
}

impl<T: Clone + Eq + Hash + Send + Sync> MVRegister<T> {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
            clock: VectorClock::new(),
        }
    }

    pub fn set(&mut self, replica: ReplicaId, value: T) {
        self.clock.increment(replica);
        // Remove values that are dominated by the new value
        self.values.retain(|(_, vc)| !vc.happened_before(&self.clock));
        self.values.insert((value, self.clock.clone()));
    }

    pub fn get(&self) -> Vec<&T> {
        self.values.iter().map(|(v, _)| v).collect()
    }
}

impl<T: Clone + Eq + Hash + Send + Sync> Default for MVRegister<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// OR-Set: Observed-Remove Set
#[derive(Debug, Clone)]
pub struct ORSet<T: Clone + Eq + Hash> {
    elements: HashMap<T, HashSet<(ReplicaId, u64)>>,
    removed: HashMap<T, HashSet<(ReplicaId, u64)>>,
    clock: VectorClock,
}

impl<T: Clone + Eq + Hash> ORSet<T> {
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
            removed: HashMap::new(),
            clock: VectorClock::new(),
        }
    }

    pub fn add(&mut self, replica: ReplicaId, value: T) {
        let seq = self.clock.increment(replica);
        self.elements
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert((replica, seq));
    }

    pub fn remove(&mut self, value: &T) {
        if let Some(tags) = self.elements.remove(value) {
            self.removed
                .entry(value.clone())
                .or_insert_with(HashSet::new)
                .extend(tags);
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.elements.contains_key(value)
    }

    pub fn values(&self) -> Vec<&T> {
        self.elements.keys().collect()
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl<T: Clone + Eq + Hash> Default for ORSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Eq + Hash + Send + Sync> Crdt for ORSet<T> {
    type Delta = (
        HashMap<T, HashSet<(ReplicaId, u64)>>,
        HashMap<T, HashSet<(ReplicaId, u64)>>,
    );

    fn merge(&mut self, other: &Self) {
        // Merge elements
        for (value, tags) in &other.elements {
            let entry = self.elements.entry(value.clone()).or_insert_with(HashSet::new);
            entry.extend(tags.iter().cloned());
        }

        // Merge removed
        for (value, tags) in &other.removed {
            let entry = self.removed.entry(value.clone()).or_insert_with(HashSet::new);
            entry.extend(tags.iter().cloned());
        }

        // Apply removals to elements
        for (value, removed_tags) in &self.removed {
            if let Some(element_tags) = self.elements.get_mut(value) {
                for tag in removed_tags {
                    element_tags.remove(tag);
                }
                if element_tags.is_empty() {
                    self.elements.remove(value);
                }
            }
        }

        self.clock.merge(&other.clock);
    }

    fn delta(&self, _since: &VectorClock) -> Option<Self::Delta> {
        Some((self.elements.clone(), self.removed.clone()))
    }

    fn apply_delta(&mut self, delta: Self::Delta) {
        for (value, tags) in delta.0 {
            let entry = self.elements.entry(value).or_insert_with(HashSet::new);
            entry.extend(tags);
        }
        for (value, tags) in delta.1 {
            let removed = self.removed.entry(value.clone()).or_insert_with(HashSet::new);
            removed.extend(tags.clone());
            if let Some(element_tags) = self.elements.get_mut(&value) {
                for tag in tags {
                    element_tags.remove(&tag);
                }
            }
        }
    }
}

/// LWW-Map: Last-Writer-Wins Map
#[derive(Debug, Clone)]
pub struct LWWMap<K: Clone + Eq + Hash, V: Clone + Send + Sync> {
    entries: HashMap<K, LWWRegister<V>>,
    replica: ReplicaId,
}

impl<K: Clone + Eq + Hash, V: Clone + Send + Sync> LWWMap<K, V> {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            entries: HashMap::new(),
            replica,
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        let entry = self.entries
            .entry(key)
            .or_insert_with(|| LWWRegister::new(self.replica));
        entry.set(value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).and_then(|r| r.get())
    }

    pub fn remove(&mut self, key: &K) {
        self.entries.remove(key);
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K: Clone + Eq + Hash + Send + Sync, V: Clone + Send + Sync> Crdt for LWWMap<K, V> {
    type Delta = HashMap<K, (Option<V>, LamportClock)>;

    fn merge(&mut self, other: &Self) {
        for (key, other_reg) in &other.entries {
            match self.entries.get_mut(key) {
                Some(reg) => reg.merge(other_reg),
                None => {
                    self.entries.insert(key.clone(), other_reg.clone());
                }
            }
        }
    }

    fn delta(&self, since: &VectorClock) -> Option<Self::Delta> {
        let delta: HashMap<_, _> = self.entries
            .iter()
            .filter_map(|(k, v)| v.delta(since).map(|d| (k.clone(), d)))
            .collect();
        if delta.is_empty() {
            None
        } else {
            Some(delta)
        }
    }

    fn apply_delta(&mut self, delta: Self::Delta) {
        for (key, d) in delta {
            match self.entries.get_mut(&key) {
                Some(reg) => reg.apply_delta(d),
                None => {
                    let mut reg = LWWRegister::new(self.replica);
                    reg.apply_delta(d);
                    self.entries.insert(key, reg);
                }
            }
        }
    }
}

/// RGA: Replicated Growable Array (for sequences/text)
#[derive(Debug, Clone)]
pub struct RGA<T: Clone + Send + Sync> {
    elements: BTreeMap<RGAId, RGAElement<T>>,
    replica: ReplicaId,
    clock: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RGAId {
    timestamp: u64,
    replica: ReplicaId,
}

#[derive(Debug, Clone)]
struct RGAElement<T> {
    value: T,
    tombstone: bool,
    after: Option<RGAId>,
}

impl<T: Clone + Send + Sync> RGA<T> {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            elements: BTreeMap::new(),
            replica,
            clock: 0,
        }
    }

    fn next_id(&mut self) -> RGAId {
        self.clock += 1;
        RGAId {
            timestamp: self.clock,
            replica: self.replica,
        }
    }

    /// Insert a value after a given position
    pub fn insert(&mut self, after: Option<RGAId>, value: T) -> RGAId {
        let id = self.next_id();
        self.elements.insert(id, RGAElement {
            value,
            tombstone: false,
            after,
        });
        id
    }

    /// Delete an element by marking it as a tombstone
    pub fn delete(&mut self, id: RGAId) -> bool {
        if let Some(elem) = self.elements.get_mut(&id) {
            elem.tombstone = true;
            true
        } else {
            false
        }
    }

    /// Get all visible elements in order
    pub fn to_vec(&self) -> Vec<(RGAId, &T)> {
        // Build the sequence from after links
        let mut result = Vec::new();
        let mut visited = HashSet::new();

        // Find elements with no predecessor (roots)
        let mut roots: Vec<_> = self.elements
            .iter()
            .filter(|(_, e)| e.after.is_none())
            .collect();
        roots.sort_by(|(a, _), (b, _)| b.cmp(a)); // Reverse order for stack

        let mut stack: Vec<RGAId> = roots.iter().map(|(id, _)| **id).collect();

        while let Some(id) = stack.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id);

            if let Some(elem) = self.elements.get(&id) {
                if !elem.tombstone {
                    result.push((id, &elem.value));
                }

                // Find elements that come after this one
                let mut followers: Vec<_> = self.elements
                    .iter()
                    .filter(|(_, e)| e.after == Some(id))
                    .collect();
                followers.sort_by(|(a, _), (b, _)| b.cmp(a));
                for (fid, _) in followers {
                    stack.push(*fid);
                }
            }
        }

        result
    }

    /// Get the length (excluding tombstones)
    pub fn len(&self) -> usize {
        self.elements.values().filter(|e| !e.tombstone).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Text CRDT built on RGA for collaborative text editing
#[derive(Debug, Clone)]
pub struct TextCRDT {
    rga: RGA<char>,
}

impl TextCRDT {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            rga: RGA::new(replica),
        }
    }

    /// Insert a character at a position
    pub fn insert(&mut self, index: usize, ch: char) -> RGAId {
        let elements = self.rga.to_vec();
        let after = if index == 0 {
            None
        } else if index <= elements.len() {
            Some(elements[index - 1].0)
        } else {
            elements.last().map(|(id, _)| *id)
        };
        self.rga.insert(after, ch)
    }

    /// Delete a character at a position
    pub fn delete(&mut self, index: usize) -> bool {
        let elements = self.rga.to_vec();
        if index < elements.len() {
            self.rga.delete(elements[index].0)
        } else {
            false
        }
    }

    /// Insert a string at a position
    pub fn insert_string(&mut self, index: usize, s: &str) -> Vec<RGAId> {
        let mut ids = Vec::new();
        let mut current_index = index;
        for ch in s.chars() {
            let id = self.insert(current_index, ch);
            ids.push(id);
            current_index += 1;
        }
        ids
    }

    /// Get the text as a string
    pub fn to_string(&self) -> String {
        self.rga.to_vec().into_iter().map(|(_, ch)| *ch).collect()
    }

    /// Get the length
    pub fn len(&self) -> usize {
        self.rga.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rga.is_empty()
    }
}

/// Document metadata CRDT for file attributes
#[derive(Debug, Clone)]
pub struct DocumentMetadata {
    pub name: LWWRegister<String>,
    pub tags: ORSet<String>,
    pub properties: LWWMap<String, String>,
    pub collaborators: ORSet<String>,
    pub version: GCounter,
}

impl DocumentMetadata {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            name: LWWRegister::new(replica),
            tags: ORSet::new(),
            properties: LWWMap::new(replica),
            collaborators: ORSet::new(),
            version: GCounter::new(),
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.name.set(name);
    }

    pub fn add_tag(&mut self, replica: ReplicaId, tag: String) {
        self.tags.add(replica, tag);
    }

    pub fn remove_tag(&mut self, tag: &String) {
        self.tags.remove(tag);
    }

    pub fn set_property(&mut self, key: String, value: String) {
        self.properties.set(key, value);
    }

    pub fn add_collaborator(&mut self, replica: ReplicaId, user: String) {
        self.collaborators.add(replica, user);
    }

    pub fn increment_version(&mut self, replica: ReplicaId) {
        self.version.increment(replica);
    }
}

/// Collaboration session managing CRDT state
pub struct CollaborationSession {
    replica: ReplicaId,
    documents: RwLock<HashMap<String, Arc<RwLock<DocumentMetadata>>>>,
    clock: RwLock<VectorClock>,
}

impl CollaborationSession {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            replica,
            documents: RwLock::new(HashMap::new()),
            clock: RwLock::new(VectorClock::new()),
        }
    }

    /// Create or get a document
    pub async fn get_or_create_document(&self, doc_id: &str) -> Arc<RwLock<DocumentMetadata>> {
        let mut docs = self.documents.write().await;
        Arc::clone(
            docs.entry(doc_id.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(DocumentMetadata::new(self.replica))))
        )
    }

    /// Get current vector clock
    pub async fn clock(&self) -> VectorClock {
        self.clock.read().await.clone()
    }

    /// Tick the clock for this replica
    pub async fn tick(&self) {
        self.clock.write().await.increment(self.replica);
    }

    /// Merge with another session's clock
    pub async fn sync_clock(&self, other: &VectorClock) {
        self.clock.write().await.merge(other);
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica
    }
}

/// Sync protocol for CRDT replication
pub struct SyncProtocol {
    local_replica: ReplicaId,
    known_clocks: RwLock<HashMap<ReplicaId, VectorClock>>,
}

impl SyncProtocol {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            local_replica: replica,
            known_clocks: RwLock::new(HashMap::new()),
        }
    }

    /// Update known state for a replica
    pub async fn update_known(&self, replica: ReplicaId, clock: VectorClock) {
        self.known_clocks.write().await.insert(replica, clock);
    }

    /// Get what we need to send to a replica
    pub async fn get_sync_needs(&self, replica: &ReplicaId) -> Option<VectorClock> {
        self.known_clocks.read().await.get(replica).cloned()
    }

    /// Check if we have new updates for a replica
    pub async fn has_updates_for(&self, replica: &ReplicaId, our_clock: &VectorClock) -> bool {
        if let Some(their_clock) = self.known_clocks.read().await.get(replica) {
            their_clock.happened_before(our_clock)
        } else {
            true // Never synced, always have updates
        }
    }
}

/// Collaborative editing operation
#[derive(Debug, Clone)]
pub enum EditOperation {
    Insert { position: usize, text: String },
    Delete { position: usize, length: usize },
    Replace { position: usize, length: usize, text: String },
}

/// Operation with metadata for synchronization
#[derive(Debug, Clone)]
pub struct TimestampedOperation {
    pub operation: EditOperation,
    pub timestamp: LamportClock,
    pub replica: ReplicaId,
    pub document_id: String,
}

/// Collaboration statistics
#[derive(Debug, Clone, Default)]
pub struct CollaborationStats {
    pub total_operations: u64,
    pub conflicts_resolved: u64,
    pub syncs_completed: u64,
    pub active_documents: usize,
    pub active_replicas: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_id() {
        let id1 = ReplicaId::new();
        let id2 = ReplicaId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_lamport_clock() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut c1 = LamportClock::new(r1);
        let mut c2 = LamportClock::new(r2);

        c1.tick();
        c1.tick();
        assert_eq!(c1.time, 2);

        c2.update(&c1);
        assert_eq!(c2.time, 3);
    }

    #[test]
    fn test_vector_clock() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();

        vc1.increment(r1);
        vc1.increment(r1);
        vc2.increment(r2);

        // Neither happened before the other
        assert!(vc1.concurrent_with(&vc2));

        // Merge
        vc1.merge(&vc2);
        assert_eq!(vc1.get(&r1), 2);
        assert_eq!(vc1.get(&r2), 1);
    }

    #[test]
    fn test_g_counter() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut c1 = GCounter::new();
        let mut c2 = GCounter::new();

        c1.increment(r1);
        c1.increment(r1);
        c2.increment(r2);
        c2.increment(r2);
        c2.increment(r2);

        c1.merge(&c2);
        assert_eq!(c1.value(), 5);
    }

    #[test]
    fn test_pn_counter() {
        let r1 = ReplicaId(1);

        let mut counter = PNCounter::new();
        counter.increment(r1);
        counter.increment(r1);
        counter.decrement(r1);

        assert_eq!(counter.value(), 1);
    }

    #[test]
    fn test_lww_register() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut reg1: LWWRegister<String> = LWWRegister::new(r1);
        let mut reg2: LWWRegister<String> = LWWRegister::new(r2);

        reg1.set("hello".to_string());
        reg2.set("world".to_string());

        // reg2 has higher timestamp, so it wins
        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"world".to_string()));
    }

    #[test]
    fn test_or_set() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut set1 = ORSet::new();
        let mut set2 = ORSet::new();

        set1.add(r1, "apple".to_string());
        set1.add(r1, "banana".to_string());
        set2.add(r2, "cherry".to_string());

        set1.merge(&set2);
        assert_eq!(set1.len(), 3);
        assert!(set1.contains(&"apple".to_string()));
        assert!(set1.contains(&"cherry".to_string()));

        // Remove from one replica
        set1.remove(&"banana".to_string());
        assert!(!set1.contains(&"banana".to_string()));
    }

    #[test]
    fn test_lww_map() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let mut map1: LWWMap<String, i32> = LWWMap::new(r1);
        let mut map2: LWWMap<String, i32> = LWWMap::new(r2);

        map1.set("key".to_string(), 1);
        map2.set("key".to_string(), 2);

        map1.merge(&map2);
        assert_eq!(map1.get(&"key".to_string()), Some(&2));
    }

    #[test]
    fn test_rga() {
        let r1 = ReplicaId(1);

        let mut rga = RGA::new(r1);

        let id1 = rga.insert(None, 'a');
        let id2 = rga.insert(Some(id1), 'b');
        let _id3 = rga.insert(Some(id2), 'c');

        let elements: String = rga.to_vec().into_iter().map(|(_, c)| *c).collect();
        assert_eq!(elements, "abc");
        assert_eq!(rga.len(), 3);

        // Delete middle element
        rga.delete(id2);
        let elements: String = rga.to_vec().into_iter().map(|(_, c)| *c).collect();
        assert_eq!(elements, "ac");
    }

    #[test]
    fn test_text_crdt() {
        let r1 = ReplicaId(1);

        let mut text = TextCRDT::new(r1);
        text.insert_string(0, "Hello");
        assert_eq!(text.to_string(), "Hello");

        text.insert_string(5, " World");
        assert_eq!(text.to_string(), "Hello World");

        text.delete(5); // Delete space
        assert_eq!(text.to_string(), "HelloWorld");
    }

    #[test]
    fn test_document_metadata() {
        let r1 = ReplicaId(1);

        let mut doc = DocumentMetadata::new(r1);

        doc.set_name("test.txt".to_string());
        doc.add_tag(r1, "important".to_string());
        doc.set_property("author".to_string(), "Alice".to_string());
        doc.add_collaborator(r1, "bob@example.com".to_string());
        doc.increment_version(r1);

        assert_eq!(doc.name.get(), Some(&"test.txt".to_string()));
        assert!(doc.tags.contains(&"important".to_string()));
        assert_eq!(doc.properties.get(&"author".to_string()), Some(&"Alice".to_string()));
        assert_eq!(doc.version.value(), 1);
    }

    #[tokio::test]
    async fn test_collaboration_session() {
        let r1 = ReplicaId(1);

        let session = CollaborationSession::new(r1);

        let doc = session.get_or_create_document("test.txt").await;
        {
            let mut doc = doc.write().await;
            doc.set_name("My Document".to_string());
        }

        session.tick().await;
        let clock = session.clock().await;
        assert_eq!(clock.get(&r1), 1);
    }

    #[tokio::test]
    async fn test_sync_protocol() {
        let r1 = ReplicaId(1);
        let r2 = ReplicaId(2);

        let protocol = SyncProtocol::new(r1);

        // Create a clock state for r2
        let mut their_clock = VectorClock::new();
        their_clock.increment(r2);

        protocol.update_known(r2, their_clock.clone()).await;

        // We have updates for r2 if we have never synced with them (true initially)
        let r3 = ReplicaId(3);
        assert!(protocol.has_updates_for(&r3, &their_clock).await);

        // Create our clock that includes r2's updates (dominating their clock)
        let mut our_clock = VectorClock::new();
        our_clock.increment(r2);
        our_clock.increment(r2); // We're ahead of their known state

        // Their clock (r2=1) happened before our clock (r2=2)
        assert!(protocol.has_updates_for(&r2, &our_clock).await);
    }
}
