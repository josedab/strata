//! Persistent storage for Raft state.

use super::{LogEntry, PersistentState};
use crate::error::{Result, StrataError};
use crate::types::LogIndex;
use rocksdb::{Options, DB};
use std::path::Path;

const PERSISTENT_STATE_KEY: &[u8] = b"raft_persistent_state";
const LOG_PREFIX: &[u8] = b"raft_log_";
const SNAPSHOT_KEY: &[u8] = b"raft_snapshot";
const SNAPSHOT_META_KEY: &[u8] = b"raft_snapshot_meta";

/// Persistent storage for Raft using RocksDB.
pub struct RaftStorage {
    db: DB,
}

impl RaftStorage {
    /// Open or create a Raft storage at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Load persistent state from storage.
    pub fn load_persistent_state(&self) -> Result<Option<PersistentState>> {
        match self.db.get(PERSISTENT_STATE_KEY)? {
            Some(data) => {
                let state: PersistentState = bincode::deserialize(&data)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Save persistent state to storage.
    pub fn save_persistent_state(&self, state: &PersistentState) -> Result<()> {
        let data = bincode::serialize(state)?;
        self.db.put(PERSISTENT_STATE_KEY, data)?;
        self.db.flush()?;
        Ok(())
    }

    /// Append log entries to storage.
    pub fn append_log_entries(&self, entries: &[LogEntry]) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();

        for entry in entries {
            let key = self.log_key(entry.index);
            let value = bincode::serialize(entry)?;
            batch.put(&key, value);
        }

        self.db.write(batch)?;
        Ok(())
    }

    /// Load a log entry by index.
    pub fn load_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry>> {
        let key = self.log_key(index);
        match self.db.get(&key)? {
            Some(data) => {
                let entry: LogEntry = bincode::deserialize(&data)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Load all log entries starting from the given index.
    pub fn load_log_entries_from(&self, start_index: LogIndex) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();
        let start_key = self.log_key(start_index);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(LOG_PREFIX) {
                break;
            }
            let entry: LogEntry = bincode::deserialize(&value)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Delete log entries from the given index onwards.
    pub fn truncate_log_from(&self, from_index: LogIndex) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let start_key = self.log_key(from_index);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(LOG_PREFIX) {
                break;
            }
            batch.delete(&key);
        }

        self.db.write(batch)?;
        Ok(())
    }

    /// Save a snapshot.
    pub fn save_snapshot(&self, data: &[u8], last_index: LogIndex, last_term: u64) -> Result<()> {
        let meta = SnapshotMeta {
            last_index,
            last_term,
        };
        let meta_data = bincode::serialize(&meta)?;

        let mut batch = rocksdb::WriteBatch::default();
        batch.put(SNAPSHOT_KEY, data);
        batch.put(SNAPSHOT_META_KEY, meta_data);
        self.db.write(batch)?;
        self.db.flush()?;

        Ok(())
    }

    /// Load snapshot data and metadata.
    pub fn load_snapshot(&self) -> Result<Option<(Vec<u8>, SnapshotMeta)>> {
        let meta_data = match self.db.get(SNAPSHOT_META_KEY)? {
            Some(d) => d,
            None => return Ok(None),
        };

        let snapshot_data = match self.db.get(SNAPSHOT_KEY)? {
            Some(d) => d,
            None => return Ok(None),
        };

        let meta: SnapshotMeta = bincode::deserialize(&meta_data)?;
        Ok(Some((snapshot_data.to_vec(), meta)))
    }

    /// Compact log entries up to the given index (for snapshot).
    pub fn compact_log(&self, up_to_index: LogIndex) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let start_key = self.log_key(1);
        let end_key = self.log_key(up_to_index + 1);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item?;
            if key.as_ref() >= end_key.as_slice() || !key.starts_with(LOG_PREFIX) {
                break;
            }
            batch.delete(&key);
        }

        self.db.write(batch)?;
        Ok(())
    }

    /// Get the first and last log indices stored.
    pub fn get_log_bounds(&self) -> Result<Option<(LogIndex, LogIndex)>> {
        let mut first: Option<LogIndex> = None;
        let mut last: Option<LogIndex> = None;

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            LOG_PREFIX,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(LOG_PREFIX) {
                break;
            }
            let index = self.parse_log_key(&key)?;
            if first.is_none() {
                first = Some(index);
            }
            last = Some(index);
        }

        match (first, last) {
            (Some(f), Some(l)) => Ok(Some((f, l))),
            _ => Ok(None),
        }
    }

    fn log_key(&self, index: LogIndex) -> Vec<u8> {
        let mut key = LOG_PREFIX.to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn parse_log_key(&self, key: &[u8]) -> Result<LogIndex> {
        if key.len() < LOG_PREFIX.len() + 8 {
            return Err(StrataError::Storage("Invalid log key".into()));
        }
        let index_bytes: [u8; 8] = key[LOG_PREFIX.len()..]
            .try_into()
            .map_err(|_| StrataError::Storage("Invalid log key".into()))?;
        Ok(LogIndex::from_be_bytes(index_bytes))
    }
}

/// Snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: u64,
}

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_persistent_state() {
        let dir = tempdir().unwrap();
        let storage = RaftStorage::open(dir.path()).unwrap();

        // Initially empty
        assert!(storage.load_persistent_state().unwrap().is_none());

        // Save state
        let state = PersistentState {
            current_term: 5,
            voted_for: Some(3),
        };
        storage.save_persistent_state(&state).unwrap();

        // Load state
        let loaded = storage.load_persistent_state().unwrap().unwrap();
        assert_eq!(loaded.current_term, 5);
        assert_eq!(loaded.voted_for, Some(3));
    }

    #[test]
    fn test_log_entries() {
        let dir = tempdir().unwrap();
        let storage = RaftStorage::open(dir.path()).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, vec![1, 2, 3]),
            LogEntry::new(1, 2, vec![4, 5, 6]),
            LogEntry::new(2, 3, vec![7, 8, 9]),
        ];

        storage.append_log_entries(&entries).unwrap();

        // Load single entry
        let entry = storage.load_log_entry(2).unwrap().unwrap();
        assert_eq!(entry.term, 1);
        assert_eq!(entry.data_bytes(), &[4, 5, 6]);

        // Load all entries
        let loaded = storage.load_log_entries_from(1).unwrap();
        assert_eq!(loaded.len(), 3);

        // Get bounds
        let bounds = storage.get_log_bounds().unwrap().unwrap();
        assert_eq!(bounds, (1, 3));
    }

    #[test]
    fn test_truncate_log() {
        let dir = tempdir().unwrap();
        let storage = RaftStorage::open(dir.path()).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(1, 2, vec![2]),
            LogEntry::new(1, 3, vec![3]),
        ];

        storage.append_log_entries(&entries).unwrap();
        storage.truncate_log_from(2).unwrap();

        let loaded = storage.load_log_entries_from(1).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].index, 1);
    }

    #[test]
    fn test_snapshot() {
        let dir = tempdir().unwrap();
        let storage = RaftStorage::open(dir.path()).unwrap();

        let snapshot_data = b"test snapshot data".to_vec();
        storage.save_snapshot(&snapshot_data, 10, 5).unwrap();

        let (loaded_data, meta) = storage.load_snapshot().unwrap().unwrap();
        assert_eq!(loaded_data, snapshot_data);
        assert_eq!(meta.last_index, 10);
        assert_eq!(meta.last_term, 5);
    }
}
