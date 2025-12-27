//! Continuous Data Protection (CDP)
//!
//! Provides real-time data change capture for point-in-time recovery:
//! - Change journal for all write operations
//! - Recovery to any point in time
//! - Configurable retention policies
//! - Efficient delta storage

pub mod journal;
pub mod recovery;
pub mod policy;
pub mod checkpoint;

pub use journal::{ChangeJournal, ChangeEntry, ChangeType, JournalConfig};
pub use recovery::{RecoveryManager, RecoveryPoint, RecoveryOptions};
pub use policy::{RetentionPolicy, PolicyType, PolicyManager};
pub use checkpoint::{Checkpoint, CheckpointManager, CheckpointConfig};
