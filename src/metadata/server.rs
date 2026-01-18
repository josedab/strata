//! Metadata server implementation.

use super::{MetadataOp, MetadataStateMachine};
use crate::config::{MetadataConfig, StorageConfig};
use crate::error::{Result, StrataError};
use crate::raft::{
    RaftCommand, RaftConfig, RaftNode, RaftRpc,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    TimeoutNowRequest, TimeoutNowResponse,
    ReadIndexRequest, ReadIndexResponse,
};
use crate::types::NodeId;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

/// Default connect timeout for Raft RPC operations.
const RAFT_RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Default request timeout for Raft RPC operations.
const RAFT_RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Metadata server state.
#[allow(dead_code)]
pub struct MetadataServer {
    node_id: NodeId,
    command_tx: mpsc::Sender<RaftCommand>,
    state_machine: Arc<RwLock<MetadataStateMachine>>,
}

impl MetadataServer {
    /// Create a new metadata server.
    pub fn new(
        node_id: NodeId,
        command_tx: mpsc::Sender<RaftCommand>,
        state_machine: Arc<RwLock<MetadataStateMachine>>,
    ) -> Self {
        Self {
            node_id,
            command_tx,
            state_machine,
        }
    }

    /// Check if this node is the leader.
    pub async fn is_leader(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.command_tx.send(RaftCommand::IsLeader { response: tx }).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Get the current leader ID.
    pub async fn get_leader(&self) -> Option<NodeId> {
        let (tx, rx) = oneshot::channel();
        if self.command_tx.send(RaftCommand::GetLeader { response: tx }).await.is_ok() {
            rx.await.ok().flatten()
        } else {
            None
        }
    }

    /// Apply a metadata operation through Raft.
    pub async fn apply(&self, op: MetadataOp) -> Result<crate::types::LogIndex> {
        let data = bincode::serialize(&op)?;

        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(RaftCommand::Propose { data, response: tx })
            .await
            .map_err(|_| StrataError::Internal("Raft channel closed".into()))?;

        rx.await.map_err(|_| StrataError::Internal("Response channel closed".into()))?
    }
}

/// Network-based RPC implementation.
pub struct NetworkRpc {
    peers: HashMap<NodeId, String>,
    client: reqwest::Client,
}

impl NetworkRpc {
    pub fn new(peers: HashMap<NodeId, String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(RAFT_RPC_CONNECT_TIMEOUT)
            .timeout(RAFT_RPC_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            peers,
            client,
        }
    }

    fn get_peer_url(&self, target: NodeId, endpoint: &str) -> Option<String> {
        self.peers.get(&target).map(|addr| format!("http://{}/{}", addr, endpoint))
    }
}

#[async_trait::async_trait]
impl RaftRpc for NetworkRpc {
    async fn request_vote(
        &self,
        target: NodeId,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        let url = self.get_peer_url(target, "raft/request_vote")
            .ok_or_else(|| StrataError::NodeNotFound(target))?;

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    async fn append_entries(
        &self,
        target: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let url = self.get_peer_url(target, "raft/append_entries")
            .ok_or_else(|| StrataError::NodeNotFound(target))?;

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let url = self.get_peer_url(target, "raft/install_snapshot")
            .ok_or_else(|| StrataError::NodeNotFound(target))?;

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    async fn timeout_now(
        &self,
        target: NodeId,
        request: TimeoutNowRequest,
    ) -> Result<TimeoutNowResponse> {
        let url = self.get_peer_url(target, "raft/timeout_now")
            .ok_or_else(|| StrataError::NodeNotFound(target))?;

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    async fn read_index(
        &self,
        target: NodeId,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse> {
        let url = self.get_peer_url(target, "raft/read_index")
            .ok_or_else(|| StrataError::NodeNotFound(target))?;

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }
}

/// Shared server state for Axum handlers.
#[derive(Clone)]
struct ServerState {
    command_tx: mpsc::Sender<RaftCommand>,
    state_machine: Arc<RwLock<MetadataStateMachine>>,
}

/// Run the metadata server.
pub async fn run_metadata_server(
    node_id: NodeId,
    config: MetadataConfig,
    storage_config: StorageConfig,
) -> Result<()> {
    info!(node_id, "Starting metadata server");

    // Parse peer addresses
    let peers: HashMap<NodeId, String> = config
        .raft_peers
        .iter()
        .filter_map(|peer| {
            let parts: Vec<&str> = peer.split('=').collect();
            if parts.len() == 2 {
                parts[0].parse::<NodeId>().ok().map(|id| (id, parts[1].to_string()))
            } else {
                None
            }
        })
        .collect();

    // Create Raft configuration
    let raft_config = RaftConfig {
        node_id,
        peers: peers.clone(),
        election_timeout_min: config.election_timeout_min,
        election_timeout_max: config.election_timeout_max,
        heartbeat_interval: config.heartbeat_interval,
        ..Default::default()
    };

    // Create state machine
    let state_machine = MetadataStateMachine::new();
    let sm_arc = Arc::new(RwLock::new(state_machine));

    // Create RPC handler
    let rpc = Arc::new(NetworkRpc::new(peers));

    // Create Raft node
    let storage_path = storage_config.metadata_dir.join("raft");
    let (raft_node, command_rx) = RaftNode::new(
        raft_config,
        storage_path,
        MetadataStateMachine::new(),
        rpc,
    )?;

    let command_tx = raft_node.command_sender();

    // Create server state
    let server_state = ServerState {
        command_tx: command_tx.clone(),
        state_machine: Arc::clone(&sm_arc),
    };

    // Start Raft node in background
    let raft_handle = tokio::spawn(async move {
        raft_node.run(command_rx).await;
    });

    // Build API router
    let app = Router::new()
        // Raft RPC endpoints
        .route("/raft/request_vote", post(handle_request_vote))
        .route("/raft/append_entries", post(handle_append_entries))
        // Metadata API endpoints
        .route("/health", get(health_check))
        .route("/metadata/lookup", post(handle_lookup))
        .route("/metadata/create_file", post(handle_create_file))
        .route("/metadata/create_directory", post(handle_create_directory))
        .route("/metadata/delete", post(handle_delete))
        .route("/metadata/rename", post(handle_rename))
        .route("/metadata/readdir", post(handle_readdir))
        .route("/metadata/getattr", post(handle_getattr))
        .route("/metadata/add_chunk", post(handle_add_chunk))
        .with_state(server_state);

    // Bind and serve
    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "Metadata server listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| StrataError::Network(e.to_string()))?;

    raft_handle.abort();
    Ok(())
}

// API Handlers

async fn health_check() -> &'static str {
    "OK"
}

async fn handle_request_vote(
    State(state): State<ServerState>,
    Json(request): Json<RequestVoteRequest>,
) -> Json<RequestVoteResponse> {
    let (tx, rx) = oneshot::channel();

    if state.command_tx.send(RaftCommand::RequestVote { request, response: tx }).await.is_err() {
        return Json(RequestVoteResponse {
            term: 0,
            vote_granted: false,
        });
    }

    Json(rx.await.unwrap_or(RequestVoteResponse {
        term: 0,
        vote_granted: false,
    }))
}

async fn handle_append_entries(
    State(state): State<ServerState>,
    Json(request): Json<AppendEntriesRequest>,
) -> Json<AppendEntriesResponse> {
    let (tx, rx) = oneshot::channel();

    if state.command_tx.send(RaftCommand::AppendEntries { request, response: tx }).await.is_err() {
        return Json(AppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
            conflict_index: None,
            conflict_term: None,
        });
    }

    Json(rx.await.unwrap_or(AppendEntriesResponse {
        term: 0,
        success: false,
        match_index: 0,
        conflict_index: None,
        conflict_term: None,
    }))
}

#[derive(serde::Deserialize)]
struct LookupRequest {
    parent: u64,
    name: String,
}

#[derive(serde::Serialize)]
struct LookupResponse {
    found: bool,
    inode: Option<crate::types::Inode>,
}

async fn handle_lookup(
    State(state): State<ServerState>,
    Json(request): Json<LookupRequest>,
) -> Json<LookupResponse> {
    let sm = state.state_machine.read();
    let inode = sm.lookup(request.parent, &request.name).cloned();

    Json(LookupResponse {
        found: inode.is_some(),
        inode,
    })
}

#[derive(serde::Deserialize)]
struct CreateFileRequest {
    parent: u64,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

#[derive(serde::Serialize)]
struct CreateResponse {
    success: bool,
    inode: Option<u64>,
    error: Option<String>,
}

async fn handle_create_file(
    State(state): State<ServerState>,
    Json(request): Json<CreateFileRequest>,
) -> Json<CreateResponse> {
    let op = MetadataOp::CreateFile {
        parent: request.parent,
        name: request.name,
        mode: request.mode,
        uid: request.uid,
        gid: request.gid,
    };

    let data = match bincode::serialize(&op) {
        Ok(d) => d,
        Err(e) => return Json(CreateResponse {
            success: false,
            inode: None,
            error: Some(e.to_string()),
        }),
    };

    let (tx, rx) = oneshot::channel();
    if state.command_tx.send(RaftCommand::Propose { data, response: tx }).await.is_err() {
        return Json(CreateResponse {
            success: false,
            inode: None,
            error: Some("Failed to send command".into()),
        });
    }

    match rx.await {
        Ok(Ok(_)) => Json(CreateResponse {
            success: true,
            inode: None, // Would need to query state machine
            error: None,
        }),
        Ok(Err(e)) => Json(CreateResponse {
            success: false,
            inode: None,
            error: Some(e.to_string()),
        }),
        Err(_) => Json(CreateResponse {
            success: false,
            inode: None,
            error: Some("Response channel closed".into()),
        }),
    }
}

#[derive(serde::Deserialize)]
struct CreateDirectoryRequest {
    parent: u64,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

async fn handle_create_directory(
    State(state): State<ServerState>,
    Json(request): Json<CreateDirectoryRequest>,
) -> Json<CreateResponse> {
    let op = MetadataOp::CreateDirectory {
        parent: request.parent,
        name: request.name,
        mode: request.mode,
        uid: request.uid,
        gid: request.gid,
    };

    let data = match bincode::serialize(&op) {
        Ok(d) => d,
        Err(e) => return Json(CreateResponse {
            success: false,
            inode: None,
            error: Some(e.to_string()),
        }),
    };

    let (tx, rx) = oneshot::channel();
    if state.command_tx.send(RaftCommand::Propose { data, response: tx }).await.is_err() {
        return Json(CreateResponse {
            success: false,
            inode: None,
            error: Some("Failed to send command".into()),
        });
    }

    match rx.await {
        Ok(Ok(_)) => Json(CreateResponse {
            success: true,
            inode: None,
            error: None,
        }),
        Ok(Err(e)) => Json(CreateResponse {
            success: false,
            inode: None,
            error: Some(e.to_string()),
        }),
        Err(_) => Json(CreateResponse {
            success: false,
            inode: None,
            error: Some("Response channel closed".into()),
        }),
    }
}

#[derive(serde::Deserialize)]
struct DeleteRequest {
    parent: u64,
    name: String,
}

#[derive(serde::Serialize)]
struct DeleteResponse {
    success: bool,
    error: Option<String>,
}

async fn handle_delete(
    State(state): State<ServerState>,
    Json(request): Json<DeleteRequest>,
) -> Json<DeleteResponse> {
    let op = MetadataOp::Delete {
        parent: request.parent,
        name: request.name,
    };

    let data = match bincode::serialize(&op) {
        Ok(d) => d,
        Err(e) => return Json(DeleteResponse {
            success: false,
            error: Some(e.to_string()),
        }),
    };

    let (tx, rx) = oneshot::channel();
    if state.command_tx.send(RaftCommand::Propose { data, response: tx }).await.is_err() {
        return Json(DeleteResponse {
            success: false,
            error: Some("Failed to send command".into()),
        });
    }

    match rx.await {
        Ok(Ok(_)) => Json(DeleteResponse { success: true, error: None }),
        Ok(Err(e)) => Json(DeleteResponse { success: false, error: Some(e.to_string()) }),
        Err(_) => Json(DeleteResponse { success: false, error: Some("Response channel closed".into()) }),
    }
}

#[derive(serde::Deserialize)]
struct RenameRequest {
    src_parent: u64,
    src_name: String,
    dst_parent: u64,
    dst_name: String,
}

async fn handle_rename(
    State(state): State<ServerState>,
    Json(request): Json<RenameRequest>,
) -> Json<DeleteResponse> {
    let op = MetadataOp::Rename {
        src_parent: request.src_parent,
        src_name: request.src_name,
        dst_parent: request.dst_parent,
        dst_name: request.dst_name,
    };

    let data = match bincode::serialize(&op) {
        Ok(d) => d,
        Err(e) => return Json(DeleteResponse {
            success: false,
            error: Some(e.to_string()),
        }),
    };

    let (tx, rx) = oneshot::channel();
    if state.command_tx.send(RaftCommand::Propose { data, response: tx }).await.is_err() {
        return Json(DeleteResponse {
            success: false,
            error: Some("Failed to send command".into()),
        });
    }

    match rx.await {
        Ok(Ok(_)) => Json(DeleteResponse { success: true, error: None }),
        Ok(Err(e)) => Json(DeleteResponse { success: false, error: Some(e.to_string()) }),
        Err(_) => Json(DeleteResponse { success: false, error: Some("Response channel closed".into()) }),
    }
}

#[derive(serde::Deserialize)]
struct ReaddirRequest {
    inode: u64,
}

#[derive(serde::Serialize)]
struct ReaddirResponse {
    success: bool,
    entries: Vec<DirEntryResponse>,
    error: Option<String>,
}

#[derive(serde::Serialize)]
struct DirEntryResponse {
    name: String,
    inode: u64,
    file_type: crate::types::FileType,
}

async fn handle_readdir(
    State(state): State<ServerState>,
    Json(request): Json<ReaddirRequest>,
) -> Json<ReaddirResponse> {
    let sm = state.state_machine.read();

    match sm.readdir(request.inode) {
        Ok(entries) => {
            let entries: Vec<DirEntryResponse> = entries
                .into_iter()
                .map(|(name, inode_id, file_type)| DirEntryResponse {
                    name,
                    inode: inode_id,
                    file_type,
                })
                .collect();
            Json(ReaddirResponse {
                success: true,
                entries,
                error: None,
            })
        }
        Err(e) => Json(ReaddirResponse {
            success: false,
            entries: Vec::new(),
            error: Some(format!("{:?}", e)),
        }),
    }
}

#[derive(serde::Deserialize)]
struct GetattrRequest {
    inode: u64,
}

#[derive(serde::Serialize)]
struct GetattrResponse {
    found: bool,
    inode: Option<crate::types::Inode>,
}

async fn handle_getattr(
    State(state): State<ServerState>,
    Json(request): Json<GetattrRequest>,
) -> Json<GetattrResponse> {
    let sm = state.state_machine.read();
    let inode = sm.get_inode(request.inode).cloned();

    Json(GetattrResponse {
        found: inode.is_some(),
        inode,
    })
}

#[derive(serde::Deserialize)]
struct AddChunkRequest {
    inode: u64,
    chunk_id: crate::types::ChunkId,
}

#[derive(serde::Serialize)]
struct AddChunkResponse {
    success: bool,
    error: Option<String>,
}

async fn handle_add_chunk(
    State(state): State<ServerState>,
    Json(request): Json<AddChunkRequest>,
) -> Json<AddChunkResponse> {
    let op = MetadataOp::AddChunk {
        inode: request.inode,
        chunk_id: request.chunk_id,
    };

    let data = match bincode::serialize(&op) {
        Ok(d) => d,
        Err(e) => return Json(AddChunkResponse {
            success: false,
            error: Some(e.to_string()),
        }),
    };

    let (tx, rx) = oneshot::channel();
    if state.command_tx.send(RaftCommand::Propose { data, response: tx }).await.is_err() {
        return Json(AddChunkResponse {
            success: false,
            error: Some("Failed to send command".into()),
        });
    }

    match rx.await {
        Ok(Ok(_)) => Json(AddChunkResponse { success: true, error: None }),
        Ok(Err(e)) => Json(AddChunkResponse { success: false, error: Some(e.to_string()) }),
        Err(_) => Json(AddChunkResponse { success: false, error: Some("Response channel closed".into()) }),
    }
}
