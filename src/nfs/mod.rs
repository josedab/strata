//! NFS v4.1/4.2 Gateway for Strata
//!
//! This module provides an NFS-compatible gateway for accessing Strata storage,
//! allowing POSIX-compliant network file access using industry-standard NFS protocols.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       NFS Gateway                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Protocol Layer: NFSv4.1 RPC │ Session Management │ Compound Ops│
//! ├─────────────────────────────────────────────────────────────────┤
//! │  State Layer: State Manager │ Lock Manager │ Delegation Manager │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Backend Layer: Metadata Client │ Data Client │ Cache           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! ## NFSv4.1 Support
//! - Session-based operations
//! - Compound procedure calls
//! - State management (locks, delegations)
//! - Sequence operations for exactly-once semantics
//!
//! ## NFSv4.2 Extensions (Future)
//! - Server-side copy
//! - Space reservation
//! - Application I/O hints
//! - Sparse files
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::nfs::{NfsServer, NfsConfig};
//!
//! let config = NfsConfig::default();
//! let server = NfsServer::new(config, metadata_client, data_client).await?;
//! server.run().await?;
//! ```

mod compound;
mod error;
mod filehandle;
mod operations;
mod protocol;
mod server;
mod session;
mod state;
mod types;

pub use compound::{CompoundRequest, CompoundResponse, CompoundResult};
pub use error::{NfsError, NfsStatus};
pub use filehandle::{FileHandle, FileHandleGenerator};
pub use operations::NfsOperations;
pub use protocol::{NfsProtocol, RpcMessage, RpcReply, XdrCodec};
pub use server::{NfsServer, NfsServerConfig};
pub use session::{
    NfsSession, NfsSessionId, SessionManager, SessionManagerConfig, SessionSlot, SlotSequence,
};
pub use state::{
    ClientId, Delegation, DelegationType, Lock, LockManager, LockRange, LockType, OpenState,
    OpenStateFlags, StateId, StateManager, StateOwner,
};
pub use types::{
    Access4, Ace4, AceFlag4, AceMask4, AceType4, Attr4, AttrBitmap, ChangeInfo4, CreateHow4,
    FsLocations4, FsStatus4, FsVer4, Layoutiomode4, Layouttype4, NfsAcl4, NfsFh4, NfsFtype4,
    NfsLease4, NfsResop4, NfsTime4, OpenClaim4, OpenDelegationType4, OpenFlags4, ShareAccess4,
    ShareDeny4, Specdata4, Stateid4, Verifier4,
};

use crate::client::{DataClient, MetadataClient};
use crate::error::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// NFS gateway configuration.
#[derive(Debug, Clone)]
pub struct NfsConfig {
    /// Address to bind the NFS server.
    pub bind_addr: SocketAddr,
    /// Maximum number of concurrent sessions.
    pub max_sessions: usize,
    /// Session timeout in seconds.
    pub session_timeout_secs: u64,
    /// Lease duration in seconds.
    pub lease_duration_secs: u32,
    /// Maximum slots per session.
    pub max_slots_per_session: u32,
    /// Enable NFSv4.2 extensions.
    pub enable_v42_extensions: bool,
    /// Enable delegations.
    pub enable_delegations: bool,
    /// Maximum cache entries for file handles.
    pub max_filehandle_cache: usize,
}

impl Default for NfsConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:2049".parse().unwrap(),
            max_sessions: 1024,
            session_timeout_secs: 300,
            lease_duration_secs: 90,
            max_slots_per_session: 64,
            enable_v42_extensions: true,
            enable_delegations: true,
            max_filehandle_cache: 10000,
        }
    }
}

/// NFS gateway state shared across handlers.
pub struct NfsGatewayState {
    /// Configuration.
    pub config: NfsConfig,
    /// Metadata client.
    pub metadata: MetadataClient,
    /// Data client.
    pub data: DataClient,
    /// Session manager.
    pub session_manager: Arc<SessionManager>,
    /// State manager for locks and delegations.
    pub state_manager: Arc<StateManager>,
    /// File handle generator.
    pub filehandle_gen: Arc<FileHandleGenerator>,
}

impl NfsGatewayState {
    /// Create a new NFS gateway state.
    pub fn new(config: NfsConfig, metadata: MetadataClient, data: DataClient) -> Self {
        let session_config = SessionManagerConfig {
            max_sessions: config.max_sessions,
            session_timeout_secs: config.session_timeout_secs,
            max_slots_per_session: config.max_slots_per_session,
        };

        Self {
            session_manager: Arc::new(SessionManager::new(session_config)),
            state_manager: Arc::new(StateManager::new(config.lease_duration_secs)),
            filehandle_gen: Arc::new(FileHandleGenerator::new(config.max_filehandle_cache)),
            config,
            metadata,
            data,
        }
    }
}

/// Run the NFS gateway server.
pub async fn run_nfs_gateway(
    config: NfsConfig,
    metadata: MetadataClient,
    data: DataClient,
) -> Result<()> {
    let state = Arc::new(NfsGatewayState::new(config.clone(), metadata, data));
    let server = NfsServer::new(state.clone())?;

    info!(addr = %config.bind_addr, "Starting NFS gateway");
    server.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NfsConfig::default();
        assert_eq!(config.bind_addr.port(), 2049);
        assert_eq!(config.max_sessions, 1024);
        assert_eq!(config.lease_duration_secs, 90);
        assert!(config.enable_delegations);
    }
}
