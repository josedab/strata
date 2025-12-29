//! Edge Caching for Strata
//!
//! Provides geographically distributed caching for low-latency access:
//! - Edge cache nodes
//! - Content replication
//! - Cache invalidation
//! - CDN-style caching

pub mod node;
pub mod replication;
pub mod invalidation;

pub use node::{EdgeNode, EdgeConfig, CachePolicy};
pub use replication::{ReplicationManager, ReplicationConfig, ConsistencyLevel};
pub use invalidation::{InvalidationManager, InvalidationEvent, InvalidationStrategy};
