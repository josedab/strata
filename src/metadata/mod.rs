//! Metadata service for Strata.
//!
//! This module implements the metadata cluster using Raft consensus.
//! It manages the file system namespace, including inodes, directories,
//! and chunk location tracking.

// Deny unsafe code patterns in this critical metadata module.
// Panics in metadata operations can cause data inconsistency.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod operations;
mod state_machine;
mod server;

pub use operations::{MetadataOp, OpResult};
pub use state_machine::MetadataStateMachine;
pub use server::run_metadata_server;
