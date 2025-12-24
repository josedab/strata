//! Raft log implementation.

use crate::error::{Result, StrataError};
use crate::types::{LogIndex, Term};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

/// A single entry in the Raft log.
///
/// Uses Arc<Vec<u8>> for the data field to enable O(1) cloning during
/// log replication, avoiding expensive data copies in the hot path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term when the entry was received.
    pub term: Term,
    /// The index of this entry in the log.
    pub index: LogIndex,
    /// The command data (Arc-wrapped for cheap cloning during replication).
    #[serde(with = "arc_bytes")]
    pub data: Arc<Vec<u8>>,
}

impl LogEntry {
    /// Create a new log entry.
    pub fn new(term: Term, index: LogIndex, data: Vec<u8>) -> Self {
        Self { term, index, data: Arc::new(data) }
    }

    /// Create a new log entry with pre-wrapped Arc data.
    /// Use this when data is already Arc-wrapped to avoid double wrapping.
    pub fn with_arc_data(term: Term, index: LogIndex, data: Arc<Vec<u8>>) -> Self {
        Self { term, index, data }
    }

    /// Get a reference to the data bytes.
    #[inline]
    pub fn data_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Serde helper module for Arc<Vec<u8>> serialization.
/// Serializes as raw bytes, deserializes into Arc-wrapped Vec.
mod arc_bytes {
    use serde::{Deserializer, Serializer};
    use std::sync::Arc;

    pub fn serialize<S>(data: &Arc<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(data.as_slice(), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Ok(Arc::new(bytes))
    }
}

/// The Raft log, storing all entries.
#[derive(Debug)]
pub struct RaftLog {
    /// Log entries (kept in memory, with persistence via RaftStorage).
    entries: VecDeque<LogEntry>,
    /// Index of the first entry in the log (after compaction).
    first_index: LogIndex,
    /// Term of the entry at first_index - 1 (for AppendEntries consistency check).
    snapshot_term: Term,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            first_index: 1,
            snapshot_term: 0,
        }
    }

    /// Get the index of the last log entry.
    pub fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            self.first_index.saturating_sub(1)
        } else {
            self.first_index + self.entries.len() as u64 - 1
        }
    }

    /// Get the term of the last log entry.
    pub fn last_term(&self) -> Term {
        self.entries.back().map(|e| e.term).unwrap_or(self.snapshot_term)
    }

    /// Get the first index in the log.
    pub fn first_index(&self) -> LogIndex {
        self.first_index
    }

    /// Get the number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append an entry to the log.
    pub fn append(&mut self, entry: LogEntry) -> Result<()> {
        let expected_index = self.last_index() + 1;
        if entry.index != expected_index {
            return Err(StrataError::RaftLog(format!(
                "Expected index {}, got {}",
                expected_index, entry.index
            )));
        }
        self.entries.push_back(entry);
        Ok(())
    }

    /// Get an entry by index.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index < self.first_index || index > self.last_index() {
            return None;
        }
        let offset = (index - self.first_index) as usize;
        self.entries.get(offset)
    }

    /// Get the term at a specific index.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.first_index - 1 {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Get entries starting from the given index.
    pub fn entries_from(&self, start_index: LogIndex) -> Vec<LogEntry> {
        if start_index > self.last_index() {
            return Vec::new();
        }
        let start = start_index.max(self.first_index);
        let offset = (start - self.first_index) as usize;
        self.entries.iter().skip(offset).cloned().collect()
    }

    /// Get entries in a range [start, end].
    pub fn entries_range(&self, start: LogIndex, end: LogIndex) -> Vec<LogEntry> {
        self.entries_from(start)
            .into_iter()
            .take_while(|e| e.index <= end)
            .collect()
    }

    /// Truncate the log from the given index (inclusive).
    /// Used when receiving conflicting entries from the leader.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if index < self.first_index {
            self.entries.clear();
            return;
        }
        let keep = (index - self.first_index) as usize;
        self.entries.truncate(keep);
    }

    /// Check if our log matches the leader's at the given index and term.
    pub fn matches(&self, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
        if prev_log_index == 0 {
            return true;
        }
        match self.term_at(prev_log_index) {
            Some(term) => term == prev_log_term,
            None => false,
        }
    }

    /// Compact the log up to the given index (for snapshotting).
    pub fn compact(&mut self, up_to_index: LogIndex, snapshot_term: Term) {
        if up_to_index < self.first_index {
            return;
        }

        let entries_to_remove = (up_to_index - self.first_index + 1) as usize;
        for _ in 0..entries_to_remove.min(self.entries.len()) {
            self.entries.pop_front();
        }

        self.first_index = up_to_index + 1;
        self.snapshot_term = snapshot_term;
    }

    /// Check if a candidate's log is at least as up-to-date as ours.
    /// Used for voting in leader election.
    pub fn is_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let our_last_term = self.last_term();
        let our_last_index = self.last_index();

        if last_log_term != our_last_term {
            last_log_term > our_last_term
        } else {
            last_log_index >= our_last_index
        }
    }

    /// Create entries from raw data.
    pub fn create_entries(&self, term: Term, commands: Vec<Vec<u8>>) -> Vec<LogEntry> {
        let start_index = self.last_index() + 1;
        commands
            .into_iter()
            .enumerate()
            .map(|(i, data)| LogEntry::new(term, start_index + i as u64, data))
            .collect()
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_log() {
        let log = RaftLog::new();
        assert!(log.is_empty());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    fn test_append_entries() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(1, 1, vec![1, 2, 3])).unwrap();
        log.append(LogEntry::new(1, 2, vec![4, 5, 6])).unwrap();
        log.append(LogEntry::new(2, 3, vec![7, 8, 9])).unwrap();

        assert_eq!(log.len(), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn test_get_entry() {
        let mut log = RaftLog::new();
        log.append(LogEntry::new(1, 1, vec![1])).unwrap();
        log.append(LogEntry::new(2, 2, vec![2])).unwrap();

        assert!(log.get(0).is_none());
        assert_eq!(log.get(1).unwrap().data_bytes(), &[1]);
        assert_eq!(log.get(2).unwrap().data_bytes(), &[2]);
        assert!(log.get(3).is_none());
    }

    #[test]
    fn test_truncate() {
        let mut log = RaftLog::new();
        log.append(LogEntry::new(1, 1, vec![1])).unwrap();
        log.append(LogEntry::new(1, 2, vec![2])).unwrap();
        log.append(LogEntry::new(1, 3, vec![3])).unwrap();

        log.truncate_from(2);
        assert_eq!(log.len(), 1);
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn test_matches() {
        let mut log = RaftLog::new();
        log.append(LogEntry::new(1, 1, vec![1])).unwrap();
        log.append(LogEntry::new(2, 2, vec![2])).unwrap();

        assert!(log.matches(0, 0));
        assert!(log.matches(1, 1));
        assert!(log.matches(2, 2));
        assert!(!log.matches(2, 1)); // Wrong term
        assert!(!log.matches(3, 2)); // Index too high
    }

    #[test]
    fn test_is_up_to_date() {
        let mut log = RaftLog::new();
        log.append(LogEntry::new(1, 1, vec![1])).unwrap();
        log.append(LogEntry::new(2, 2, vec![2])).unwrap();

        // Higher term is always more up-to-date
        assert!(log.is_up_to_date(1, 3));
        // Same term, higher index is more up-to-date
        assert!(log.is_up_to_date(3, 2));
        // Same term, same index is up-to-date
        assert!(log.is_up_to_date(2, 2));
        // Lower term is not up-to-date
        assert!(!log.is_up_to_date(3, 1));
    }

    #[test]
    fn test_compact() {
        let mut log = RaftLog::new();
        log.append(LogEntry::new(1, 1, vec![1])).unwrap();
        log.append(LogEntry::new(1, 2, vec![2])).unwrap();
        log.append(LogEntry::new(2, 3, vec![3])).unwrap();
        log.append(LogEntry::new(2, 4, vec![4])).unwrap();

        log.compact(2, 1);
        assert_eq!(log.first_index(), 3);
        assert_eq!(log.len(), 2);
        assert!(log.get(2).is_none());
        assert_eq!(log.get(3).unwrap().data_bytes(), &[3]);
    }
}
