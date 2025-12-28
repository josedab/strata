// RDMA Connection and Queue Pair Management

use super::transport::{ConnectionState, RdmaOpType, WorkCompletion};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};

/// Queue Pair type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QpType {
    /// Reliable Connected
    RC,
    /// Unreliable Connected
    UC,
    /// Unreliable Datagram
    UD,
    /// Extended Reliable Connected (RDMA atomic)
    XRC,
    /// Raw IPv6
    RawIPv6,
    /// Raw Ethertype
    RawEthertype,
}

impl Default for QpType {
    fn default() -> Self {
        QpType::RC
    }
}

/// Queue Pair configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePairConfig {
    /// QP type
    pub qp_type: QpType,
    /// Send queue depth
    pub sq_depth: u32,
    /// Receive queue depth
    pub rq_depth: u32,
    /// Max SGEs for send
    pub max_send_sge: u32,
    /// Max SGEs for receive
    pub max_recv_sge: u32,
    /// Max inline data
    pub max_inline_data: u32,
    /// Use SRQ
    pub use_srq: bool,
}

impl Default for QueuePairConfig {
    fn default() -> Self {
        Self {
            qp_type: QpType::RC,
            sq_depth: 256,
            rq_depth: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
            use_srq: false,
        }
    }
}

/// Queue Pair information for connection establishment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QpInfo {
    /// Queue pair number
    pub qp_num: u32,
    /// Partition key
    pub pkey: u16,
    /// LID (Local ID)
    pub lid: u16,
    /// GID (Global ID)
    pub gid: [u8; 16],
    /// GID index
    pub gid_index: u8,
    /// Port number
    pub port_num: u8,
    /// MTU
    pub mtu: u8,
    /// PSN (Packet Sequence Number)
    pub psn: u32,
}

/// Queue Pair
pub struct QueuePair {
    /// Configuration
    config: QueuePairConfig,
    /// QP number
    qp_num: u32,
    /// Current state
    state: RwLock<ConnectionState>,
    /// Send queue
    send_queue: Arc<RwLock<VecDeque<PendingWork>>>,
    /// Receive queue
    recv_queue: Arc<RwLock<VecDeque<PendingWork>>>,
    /// Work request ID counter
    wr_id_counter: AtomicU64,
    /// Statistics
    stats: Arc<QpStats>,
    /// Completion callback
    completion_tx: mpsc::Sender<WorkCompletion>,
}

/// Pending work request
struct PendingWork {
    wr_id: u64,
    op_type: RdmaOpType,
    posted_at: Instant,
    completion: Option<oneshot::Sender<WorkCompletion>>,
}

/// QP statistics
#[derive(Debug, Default)]
pub struct QpStats {
    pub sends_posted: AtomicU64,
    pub sends_completed: AtomicU64,
    pub recvs_posted: AtomicU64,
    pub recvs_completed: AtomicU64,
    pub rdma_writes: AtomicU64,
    pub rdma_reads: AtomicU64,
    pub errors: AtomicU64,
}

impl QueuePair {
    /// Creates a new queue pair
    pub fn new(config: QueuePairConfig) -> Result<Self> {
        let (tx, _rx) = mpsc::channel(config.sq_depth as usize);

        Ok(Self {
            config,
            qp_num: rand::random(),
            state: RwLock::new(ConnectionState::Reset),
            send_queue: Arc::new(RwLock::new(VecDeque::new())),
            recv_queue: Arc::new(RwLock::new(VecDeque::new())),
            wr_id_counter: AtomicU64::new(1),
            stats: Arc::new(QpStats::default()),
            completion_tx: tx,
        })
    }

    /// Gets the QP number
    pub fn qp_num(&self) -> u32 {
        self.qp_num
    }

    /// Gets the current state
    pub async fn state(&self) -> ConnectionState {
        *self.state.read().await
    }

    /// Transitions the QP to a new state
    pub async fn modify_state(&self, new_state: ConnectionState) -> Result<()> {
        let current = *self.state.read().await;

        // Validate state transition
        let valid = match (current, new_state) {
            (ConnectionState::Reset, ConnectionState::Init) => true,
            (ConnectionState::Init, ConnectionState::ReadyToReceive) => true,
            (ConnectionState::ReadyToReceive, ConnectionState::ReadyToSend) => true,
            (ConnectionState::ReadyToSend, ConnectionState::Error) => true,
            (_, ConnectionState::Reset) => true,
            (_, ConnectionState::Error) => true,
            _ => false,
        };

        if !valid {
            return Err(StrataError::InvalidState(format!(
                "Invalid state transition: {:?} -> {:?}",
                current, new_state
            )));
        }

        *self.state.write().await = new_state;
        Ok(())
    }

    /// Posts a send work request
    pub async fn post_send(
        &self,
        _data: &[u8],
        _lkey: u32,
    ) -> Result<u64> {
        let state = self.state().await;
        if state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "QP not ready to send: {:?}",
                state
            )));
        }

        let wr_id = self.wr_id_counter.fetch_add(1, Ordering::Relaxed);

        let pending = PendingWork {
            wr_id,
            op_type: RdmaOpType::Send,
            posted_at: Instant::now(),
            completion: None,
        };

        self.send_queue.write().await.push_back(pending);
        self.stats.sends_posted.fetch_add(1, Ordering::Relaxed);

        Ok(wr_id)
    }

    /// Posts a receive work request
    pub async fn post_recv(&self, _addr: u64, _length: u32, _lkey: u32) -> Result<u64> {
        let wr_id = self.wr_id_counter.fetch_add(1, Ordering::Relaxed);

        let pending = PendingWork {
            wr_id,
            op_type: RdmaOpType::Recv,
            posted_at: Instant::now(),
            completion: None,
        };

        self.recv_queue.write().await.push_back(pending);
        self.stats.recvs_posted.fetch_add(1, Ordering::Relaxed);

        Ok(wr_id)
    }

    /// Posts an RDMA write work request
    pub async fn post_rdma_write(
        &self,
        _local_data: &[u8],
        _lkey: u32,
        _remote_addr: u64,
        _rkey: u32,
    ) -> Result<u64> {
        let state = self.state().await;
        if state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "QP not ready: {:?}",
                state
            )));
        }

        let wr_id = self.wr_id_counter.fetch_add(1, Ordering::Relaxed);

        let pending = PendingWork {
            wr_id,
            op_type: RdmaOpType::Write,
            posted_at: Instant::now(),
            completion: None,
        };

        self.send_queue.write().await.push_back(pending);
        self.stats.rdma_writes.fetch_add(1, Ordering::Relaxed);

        Ok(wr_id)
    }

    /// Posts an RDMA read work request
    pub async fn post_rdma_read(
        &self,
        _local_addr: u64,
        _local_length: u32,
        _lkey: u32,
        _remote_addr: u64,
        _rkey: u32,
    ) -> Result<u64> {
        let state = self.state().await;
        if state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "QP not ready: {:?}",
                state
            )));
        }

        let wr_id = self.wr_id_counter.fetch_add(1, Ordering::Relaxed);

        let pending = PendingWork {
            wr_id,
            op_type: RdmaOpType::Read,
            posted_at: Instant::now(),
            completion: None,
        };

        self.send_queue.write().await.push_back(pending);
        self.stats.rdma_reads.fetch_add(1, Ordering::Relaxed);

        Ok(wr_id)
    }

    /// Gets QP info for connection establishment
    pub fn get_qp_info(&self, gid: [u8; 16]) -> QpInfo {
        QpInfo {
            qp_num: self.qp_num,
            pkey: 0xFFFF, // Default partition key
            lid: 0,       // For RoCE, LID is not used
            gid,
            gid_index: 0,
            port_num: 1,
            mtu: 4, // MTU 4096
            psn: rand::random::<u32>() & 0xFFFFFF,
        }
    }

    /// Gets statistics
    pub fn stats(&self) -> QpStatsSnapshot {
        QpStatsSnapshot {
            sends_posted: self.stats.sends_posted.load(Ordering::Relaxed),
            sends_completed: self.stats.sends_completed.load(Ordering::Relaxed),
            recvs_posted: self.stats.recvs_posted.load(Ordering::Relaxed),
            recvs_completed: self.stats.recvs_completed.load(Ordering::Relaxed),
            rdma_writes: self.stats.rdma_writes.load(Ordering::Relaxed),
            rdma_reads: self.stats.rdma_reads.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of QP statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QpStatsSnapshot {
    pub sends_posted: u64,
    pub sends_completed: u64,
    pub recvs_posted: u64,
    pub recvs_completed: u64,
    pub rdma_writes: u64,
    pub rdma_reads: u64,
    pub errors: u64,
}

/// RDMA Connection wrapping a Queue Pair
pub struct RdmaConnection {
    /// Local address
    local_addr: SocketAddr,
    /// Remote address
    remote_addr: SocketAddr,
    /// Queue pair
    qp: QueuePair,
    /// Remote QP info
    remote_qp_info: RwLock<Option<QpInfo>>,
    /// Connection ID
    conn_id: u64,
    /// Created at
    created_at: Instant,
}

impl RdmaConnection {
    /// Creates a new RDMA connection
    pub fn new(local_addr: SocketAddr, remote_addr: SocketAddr) -> Result<Self> {
        let qp = QueuePair::new(QueuePairConfig::default())?;

        Ok(Self {
            local_addr,
            remote_addr,
            qp,
            remote_qp_info: RwLock::new(None),
            conn_id: rand::random(),
            created_at: Instant::now(),
        })
    }

    /// Gets the connection ID
    pub fn id(&self) -> u64 {
        self.conn_id
    }

    /// Gets the local address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Gets the remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Gets connection state
    pub async fn state(&self) -> ConnectionState {
        self.qp.state().await
    }

    /// Gets the QP
    pub fn qp(&self) -> &QueuePair {
        &self.qp
    }

    /// Establishes the connection with remote QP info
    pub async fn establish(&self, remote_info: QpInfo) -> Result<()> {
        // Store remote info
        *self.remote_qp_info.write().await = Some(remote_info);

        // Transition QP states
        self.qp.modify_state(ConnectionState::Init).await?;
        self.qp.modify_state(ConnectionState::ReadyToReceive).await?;
        self.qp.modify_state(ConnectionState::ReadyToSend).await?;

        tracing::info!(
            "Connection {} established with {}",
            self.conn_id,
            self.remote_addr
        );

        Ok(())
    }

    /// Sends data over the connection
    pub async fn send(&self, data: &[u8], lkey: u32) -> Result<u64> {
        self.qp.post_send(data, lkey).await
    }

    /// Performs an RDMA write
    pub async fn rdma_write(
        &self,
        local_data: &[u8],
        local_key: u32,
        remote_addr: u64,
        remote_key: u32,
    ) -> Result<u64> {
        self.qp.post_rdma_write(local_data, local_key, remote_addr, remote_key).await
    }

    /// Performs an RDMA read
    pub async fn rdma_read(
        &self,
        local_buffer: u64,
        local_length: u32,
        local_key: u32,
        remote_addr: u64,
        remote_key: u32,
    ) -> Result<u64> {
        self.qp.post_rdma_read(local_buffer, local_length, local_key, remote_addr, remote_key).await
    }

    /// Gets connection duration
    pub fn duration(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Closes the connection
    pub async fn close(&self) -> Result<()> {
        self.qp.modify_state(ConnectionState::Reset).await?;
        tracing::info!("Connection {} closed", self.conn_id);
        Ok(())
    }
}

/// Connection manager for multiple RDMA connections
pub struct ConnectionManager {
    /// Active connections by remote address
    connections: Arc<RwLock<std::collections::HashMap<SocketAddr, Arc<RdmaConnection>>>>,
    /// Connection limit
    max_connections: usize,
}

impl ConnectionManager {
    /// Creates a new connection manager
    pub fn new(max_connections: usize) -> Self {
        Self {
            connections: Arc::new(RwLock::new(std::collections::HashMap::new())),
            max_connections,
        }
    }

    /// Gets or creates a connection
    pub async fn get_or_create(
        &self,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) -> Result<Arc<RdmaConnection>> {
        let connections = self.connections.read().await;

        if let Some(conn) = connections.get(&remote_addr) {
            return Ok(conn.clone());
        }

        drop(connections);

        let mut connections = self.connections.write().await;

        // Double-check after acquiring write lock
        if let Some(conn) = connections.get(&remote_addr) {
            return Ok(conn.clone());
        }

        if connections.len() >= self.max_connections {
            return Err(StrataError::ResourceExhausted(
                "Maximum connections reached".to_string(),
            ));
        }

        let conn = Arc::new(RdmaConnection::new(local_addr, remote_addr)?);
        connections.insert(remote_addr, conn.clone());

        Ok(conn)
    }

    /// Removes a connection
    pub async fn remove(&self, remote_addr: &SocketAddr) -> Option<Arc<RdmaConnection>> {
        self.connections.write().await.remove(remote_addr)
    }

    /// Gets connection count
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Lists all connections
    pub async fn list_connections(&self) -> Vec<(SocketAddr, ConnectionState)> {
        let connections = self.connections.read().await;
        let mut result = Vec::new();

        for (addr, conn) in connections.iter() {
            result.push((*addr, conn.state().await));
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_pair_creation() {
        let qp = QueuePair::new(QueuePairConfig::default()).unwrap();

        assert_eq!(qp.state().await, ConnectionState::Reset);
    }

    #[tokio::test]
    async fn test_qp_state_transitions() {
        let qp = QueuePair::new(QueuePairConfig::default()).unwrap();

        qp.modify_state(ConnectionState::Init).await.unwrap();
        assert_eq!(qp.state().await, ConnectionState::Init);

        qp.modify_state(ConnectionState::ReadyToReceive).await.unwrap();
        assert_eq!(qp.state().await, ConnectionState::ReadyToReceive);

        qp.modify_state(ConnectionState::ReadyToSend).await.unwrap();
        assert_eq!(qp.state().await, ConnectionState::ReadyToSend);
    }

    #[tokio::test]
    async fn test_connection_establishment() {
        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let conn = RdmaConnection::new(local, remote).unwrap();

        let remote_info = QpInfo {
            qp_num: 12345,
            pkey: 0xFFFF,
            lid: 0,
            gid: [0; 16],
            gid_index: 0,
            port_num: 1,
            mtu: 4,
            psn: 0,
        };

        conn.establish(remote_info).await.unwrap();
        assert_eq!(conn.state().await, ConnectionState::ReadyToSend);
    }

    #[tokio::test]
    async fn test_connection_manager() {
        let manager = ConnectionManager::new(10);

        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let conn = manager.get_or_create(local, remote).await.unwrap();
        assert_eq!(manager.connection_count().await, 1);

        // Getting same connection again should return the same one
        let conn2 = manager.get_or_create(local, remote).await.unwrap();
        assert_eq!(conn.id(), conn2.id());
        assert_eq!(manager.connection_count().await, 1);
    }
}
