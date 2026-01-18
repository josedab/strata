//! Metadata service for Strata.
//!
//! This module implements the metadata cluster using Raft consensus.
//! It manages the file system namespace, including inodes, directories,
//! and chunk location tracking.

// Deny unsafe code patterns in this critical metadata module.
// Panics in metadata operations can cause data inconsistency.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod batching;
mod operations;
mod server;
mod state_machine;

pub use batching::{
    BatchConfig, BatchStats, BatchableMetadataBackend, InMemoryMetadataBackend, MetadataBatcher,
};
pub use operations::{MetadataOp, OpResult};
pub use server::run_metadata_server;
pub use state_machine::MetadataStateMachine;
