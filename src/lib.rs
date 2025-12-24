//! Strata - A distributed file system combining POSIX compatibility with S3 access.
//!
//! Strata is designed to provide enterprise-grade distributed storage that any operations
//! team can deploy and manage. It combines the reliability of proven distributed systems
//! with the simplicity that modern infrastructure demands.
//!
//! # Features
//!
//! - **Raft-based Metadata Cluster**: Consistent metadata management using Raft consensus.
//! - **Erasure Coding**: Reed-Solomon erasure coding for data durability.
//! - **POSIX Compatibility**: FUSE-based file system interface.
//! - **S3 Gateway**: S3-compatible API for cloud-native workloads.
//! - **Self-Healing**: Automatic recovery and rebalancing.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                         Strata                               │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Access Layer: FUSE Client | S3 Gateway | Native Client     │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Metadata Cluster: Raft Consensus | State Machine           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Data Servers: Chunk Storage | Erasure Coding | Caching     │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Cluster Management: Placement | Recovery | Rebalancing     │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```no_run
//! use strata::config::StrataConfig;
//!
//! #[tokio::main]
//! async fn main() -> strata::Result<()> {
//!     // Use development configuration
//!     let config = StrataConfig::development();
//!
//!     // Start the Strata server
//!     strata::run(config).await
//! }
//! ```

pub mod config;
pub mod error;
pub mod types;

pub mod audit;
pub mod auth;
pub mod backup;
pub mod cache;
pub mod cluster;
pub mod compression;
pub mod data;
pub mod encryption;
pub mod erasure;
pub mod events;
pub mod gc;
pub mod health;
pub mod lock;
pub mod metadata;
pub mod pool;
pub mod quota;
pub mod raft;
pub mod ratelimit;
pub mod resilience;
pub mod scrub;
pub mod shutdown;
pub mod snapshot;
pub mod tls;

// Next-gen features
pub mod classify;
pub mod compute;
pub mod crdt;
pub mod csi;
pub mod dedup;
pub mod ebpf;
pub mod federation;
pub mod graphql;
pub mod homomorphic;
pub mod lineage;
pub mod prefetch;
pub mod search;
pub mod tiering;
pub mod timetravel;
pub mod otel;
pub mod multitenancy;
pub mod cdc;
pub mod cdp;
pub mod worm;
pub mod zk_encryption;
pub mod selfheal;

// Ultra next-gen features
pub mod aiops;
pub mod benchmarks;
pub mod carbon;
pub mod chaos;
pub mod confidential;
pub mod global;
pub mod gpu;
pub mod intent;
pub mod iouring;
pub mod pmem;
pub mod rdma;
pub mod triggers;
pub mod vector;
pub mod edge;

#[cfg(feature = "fuse")]
pub mod fuse;

#[cfg(feature = "s3")]
pub mod s3;

pub mod cli;
pub mod client;
pub mod observability;

// Re-exports
pub use error::{Result, StrataError};
pub use types::*;

use config::StrataConfig;
use shutdown::{ShutdownCoordinator, SignalHandler, ShutdownManager, ServiceHandle};
use tracing::{info, error, warn};

/// Run the Strata server with the given configuration.
pub async fn run(config: StrataConfig) -> Result<()> {
    info!("Starting Strata node: {}", config.node.name);

    // Initialize observability
    observability::init(&config.observability)?;

    // Create storage directories
    std::fs::create_dir_all(&config.storage.data_dir)?;
    std::fs::create_dir_all(&config.storage.metadata_dir)?;

    // Create shutdown coordinator
    let coordinator = ShutdownCoordinator::new();
    let mut shutdown_manager = ShutdownManager::new(coordinator.clone());

    // Start services based on node role
    let mut handles = Vec::new();

    if config.node.role.is_metadata() {
        info!("Starting metadata service on {}", config.metadata.bind_addr);
        let metadata_config = config.metadata.clone();
        let storage_config = config.storage.clone();
        let node_id = config.node.id;

        let handle = tokio::spawn(async move {
            if let Err(e) = metadata::run_metadata_server(node_id, metadata_config, storage_config).await {
                error!("Metadata server error: {}", e);
            }
        });
        handles.push(("metadata", handle));
        shutdown_manager.register(ServiceHandle::simple("metadata"));
    }

    if config.node.role.is_data() {
        info!("Starting data service on {}", config.data.bind_addr);
        let data_config = config.data.clone();
        let storage_config = config.storage.clone();
        let node_id = config.node.id;

        let handle = tokio::spawn(async move {
            if let Err(e) = data::run_data_server(node_id, data_config, storage_config).await {
                error!("Data server error: {}", e);
            }
        });
        handles.push(("data", handle));
        shutdown_manager.register(ServiceHandle::simple("data"));
    }

    #[cfg(feature = "s3")]
    if config.s3.enabled {
        info!("Starting S3 gateway on {}", config.s3.bind_addr);
        let s3_config = config.s3.clone();
        let metadata_addr = config.metadata.bind_addr;

        let handle = tokio::spawn(async move {
            if let Err(e) = s3::run_s3_gateway(s3_config, metadata_addr).await {
                error!("S3 gateway error: {}", e);
            }
        });
        handles.push(("s3", handle));
        shutdown_manager.register(ServiceHandle::simple("s3"));
    }

    if config.observability.metrics_enabled {
        info!("Starting metrics server on {}", config.observability.metrics_addr);
        let obs_config = config.observability.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = observability::run_metrics_server(obs_config).await {
                error!("Metrics server error: {}", e);
            }
        });
        handles.push(("metrics", handle));
        shutdown_manager.register(ServiceHandle::simple("metrics"));
    }

    // Start signal handler in background
    let signal_coordinator = coordinator.clone();
    tokio::spawn(async move {
        SignalHandler::new(signal_coordinator).run().await;
    });

    // Wait for shutdown signal
    coordinator.wait_for_shutdown().await;

    info!("Shutting down Strata gracefully...");

    // Run the shutdown manager
    shutdown_manager.run().await;

    // Abort any remaining handles
    for (name, handle) in handles {
        if !handle.is_finished() {
            warn!(service = %name, "Force aborting service");
            handle.abort();
        }
    }

    info!("Strata shutdown complete");
    Ok(())
}
