// RDMA Transport layer for high-performance data transfer

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

/// Transport type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportType {
    /// InfiniBand RDMA
    InfiniBand,
    /// RoCE v1 (RDMA over Converged Ethernet)
    RoCEv1,
    /// RoCE v2 (RDMA over Converged Ethernet v2)
    RoCEv2,
    /// iWARP (internet Wide Area RDMA Protocol)
    IWarp,
    /// TCP fallback (for testing/development)
    TcpFallback,
}

impl Default for TransportType {
    fn default() -> Self {
        TransportType::RoCEv2
    }
}

/// Transport configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Transport type to use
    pub transport_type: TransportType,
    /// Device name (e.g., "mlx5_0")
    pub device_name: Option<String>,
    /// Port number on the device
    pub device_port: u8,
    /// GID index for RoCE
    pub gid_index: u8,
    /// Maximum message size
    pub max_message_size: usize,
    /// Maximum inline data size
    pub max_inline_data: usize,
    /// Send queue depth
    pub send_queue_depth: u32,
    /// Receive queue depth
    pub recv_queue_depth: u32,
    /// Completion queue depth
    pub cq_depth: u32,
    /// Enable SRQ (Shared Receive Queue)
    pub use_srq: bool,
    /// SRQ depth
    pub srq_depth: u32,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Retry count
    pub retry_count: u8,
    /// RNR (Receiver Not Ready) retry count
    pub rnr_retry: u8,
    /// MTU size
    pub mtu: MtuSize,
    /// Enable adaptive routing
    pub adaptive_routing: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            transport_type: TransportType::RoCEv2,
            device_name: None,
            device_port: 1,
            gid_index: 0,
            max_message_size: 1024 * 1024, // 1 MB
            max_inline_data: 64,
            send_queue_depth: 256,
            recv_queue_depth: 256,
            cq_depth: 512,
            use_srq: true,
            srq_depth: 1024,
            connect_timeout: Duration::from_secs(5),
            retry_count: 7,
            rnr_retry: 7,
            mtu: MtuSize::Mtu4096,
            adaptive_routing: false,
        }
    }
}

/// MTU sizes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MtuSize {
    Mtu256 = 256,
    Mtu512 = 512,
    Mtu1024 = 1024,
    Mtu2048 = 2048,
    Mtu4096 = 4096,
}

impl Default for MtuSize {
    fn default() -> Self {
        MtuSize::Mtu4096
    }
}

/// RDMA operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaOpType {
    /// Send operation
    Send,
    /// Send with immediate
    SendImm,
    /// Receive
    Recv,
    /// RDMA Write
    Write,
    /// RDMA Write with immediate
    WriteImm,
    /// RDMA Read
    Read,
    /// Atomic Compare and Swap
    AtomicCas,
    /// Atomic Fetch and Add
    AtomicFetchAdd,
}

/// Work request for RDMA operations
#[derive(Debug, Clone)]
pub struct WorkRequest {
    /// Request ID
    pub id: u64,
    /// Operation type
    pub op_type: RdmaOpType,
    /// Local buffer address
    pub local_addr: u64,
    /// Local buffer length
    pub local_length: u32,
    /// Local memory region key
    pub local_key: u32,
    /// Remote address (for RDMA read/write)
    pub remote_addr: Option<u64>,
    /// Remote key (for RDMA read/write)
    pub remote_key: Option<u32>,
    /// Immediate data (for send_imm/write_imm)
    pub imm_data: Option<u32>,
    /// Signal completion
    pub signal_completion: bool,
}

/// Work completion
#[derive(Debug, Clone)]
pub struct WorkCompletion {
    /// Request ID
    pub id: u64,
    /// Status
    pub status: CompletionStatus,
    /// Operation type
    pub op_type: RdmaOpType,
    /// Bytes transferred
    pub bytes_transferred: u32,
    /// Immediate data (if received)
    pub imm_data: Option<u32>,
    /// Source QP (for receives)
    pub source_qp: Option<u32>,
}

/// Completion status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    Success,
    LocalLengthError,
    LocalQpOperationError,
    LocalProtectionError,
    WrFlushError,
    MemoryWindowBindError,
    BadResponseError,
    LocalAccessError,
    RemoteInvalidRequestError,
    RemoteAccessError,
    RemoteOperationError,
    TransportRetryExceeded,
    RnrRetryExceeded,
    FatalError,
    ResponseTimeoutError,
    GeneralError,
}

/// Device capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceCapabilities {
    /// Device name
    pub name: String,
    /// Firmware version
    pub firmware_version: String,
    /// Maximum QPs
    pub max_qp: u32,
    /// Maximum WRs per QP
    pub max_qp_wr: u32,
    /// Maximum SGEs per WR
    pub max_sge: u32,
    /// Maximum CQs
    pub max_cq: u32,
    /// Maximum CQEs per CQ
    pub max_cqe: u32,
    /// Maximum MRs
    pub max_mr: u32,
    /// Maximum memory region size
    pub max_mr_size: u64,
    /// Page size minimum
    pub page_size_cap: u64,
    /// Atomic capabilities
    pub atomic_cap: AtomicCapability,
    /// Maximum inline data
    pub max_inline_data: u32,
    /// Support for SRQ
    pub srq_support: bool,
}

/// Atomic operation capability
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AtomicCapability {
    None,
    HcaReply,
    GlobalReply,
}

/// RDMA Transport
pub struct RdmaTransport {
    /// Configuration
    config: TransportConfig,
    /// Device capabilities
    capabilities: Option<DeviceCapabilities>,
    /// Active connections
    connections: Arc<RwLock<HashMap<SocketAddr, Arc<TransportConnection>>>>,
    /// Completion event channel
    completion_tx: mpsc::Sender<WorkCompletion>,
    /// Statistics
    stats: Arc<RwLock<TransportStats>>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

/// Transport connection
struct TransportConnection {
    /// Remote address
    remote_addr: SocketAddr,
    /// Queue pair number
    qp_num: u32,
    /// Connection state
    state: RwLock<ConnectionState>,
    /// Send buffer
    send_buffer: Vec<u8>,
    /// Receive buffer
    recv_buffer: Vec<u8>,
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Reset,
    Init,
    ReadyToReceive,
    ReadyToSend,
    Error,
    Disconnected,
}

/// Transport statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransportStats {
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Operations completed
    pub ops_completed: u64,
    /// Operations failed
    pub ops_failed: u64,
    /// Average latency (microseconds)
    pub avg_latency_us: f64,
    /// Maximum latency (microseconds)
    pub max_latency_us: u64,
    /// Active connections
    pub active_connections: u32,
}

impl RdmaTransport {
    /// Creates a new RDMA transport
    pub fn new(config: TransportConfig) -> Result<Self> {
        let (completion_tx, _completion_rx) = mpsc::channel(config.cq_depth as usize);

        Ok(Self {
            config,
            capabilities: None,
            connections: Arc::new(RwLock::new(HashMap::new())),
            completion_tx,
            stats: Arc::new(RwLock::new(TransportStats::default())),
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// Initializes the transport
    pub async fn initialize(&mut self) -> Result<()> {
        // In a real implementation, this would:
        // 1. Open the RDMA device
        // 2. Query device capabilities
        // 3. Allocate protection domain
        // 4. Create completion queues
        // 5. Optionally create SRQ

        // Simulated device capabilities
        self.capabilities = Some(DeviceCapabilities {
            name: self.config.device_name.clone().unwrap_or_else(|| "mlx5_0".to_string()),
            firmware_version: "20.35.1012".to_string(),
            max_qp: 262144,
            max_qp_wr: 32768,
            max_sge: 30,
            max_cq: 16777216,
            max_cqe: 4194303,
            max_mr: 16777216,
            max_mr_size: 0xFFFFFFFFFFFFFFFF,
            page_size_cap: 4096,
            atomic_cap: AtomicCapability::GlobalReply,
            max_inline_data: 256,
            srq_support: true,
        });

        *self.running.write().await = true;

        tracing::info!(
            "RDMA transport initialized with {} device",
            self.capabilities.as_ref().unwrap().name
        );

        Ok(())
    }

    /// Gets device capabilities
    pub fn capabilities(&self) -> Option<&DeviceCapabilities> {
        self.capabilities.as_ref()
    }

    /// Connects to a remote endpoint
    pub async fn connect(&self, remote: SocketAddr) -> Result<()> {
        let mut connections = self.connections.write().await;

        if connections.contains_key(&remote) {
            return Err(StrataError::AlreadyExists(format!(
                "Connection to {} already exists",
                remote
            )));
        }

        // In a real implementation, this would:
        // 1. Create a queue pair
        // 2. Exchange QP information with remote
        // 3. Transition QP through states (INIT -> RTR -> RTS)

        let connection = TransportConnection {
            remote_addr: remote,
            qp_num: rand::random(),
            state: RwLock::new(ConnectionState::ReadyToSend),
            send_buffer: vec![0u8; self.config.max_message_size],
            recv_buffer: vec![0u8; self.config.max_message_size],
        };

        connections.insert(remote, Arc::new(connection));

        let mut stats = self.stats.write().await;
        stats.active_connections += 1;

        tracing::info!("Connected to {}", remote);
        Ok(())
    }

    /// Disconnects from a remote endpoint
    pub async fn disconnect(&self, remote: &SocketAddr) -> Result<()> {
        let mut connections = self.connections.write().await;

        if connections.remove(remote).is_some() {
            let mut stats = self.stats.write().await;
            stats.active_connections = stats.active_connections.saturating_sub(1);
            tracing::info!("Disconnected from {}", remote);
            Ok(())
        } else {
            Err(StrataError::NotFound(format!("Connection to {}", remote)))
        }
    }

    /// Posts a send operation
    pub async fn send(&self, remote: &SocketAddr, data: &[u8]) -> Result<u64> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(remote)
            .ok_or_else(|| StrataError::NotFound(format!("Connection to {}", remote)))?;

        let state = conn.state.read().await;
        if *state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "Connection not ready: {:?}",
                *state
            )));
        }

        // In real implementation, this would post a send WR to the QP
        let wr_id = rand::random();

        // Simulate completion
        let completion = WorkCompletion {
            id: wr_id,
            status: CompletionStatus::Success,
            op_type: RdmaOpType::Send,
            bytes_transferred: data.len() as u32,
            imm_data: None,
            source_qp: None,
        };

        let _ = self.completion_tx.send(completion).await;

        let mut stats = self.stats.write().await;
        stats.bytes_sent += data.len() as u64;
        stats.ops_completed += 1;

        Ok(wr_id)
    }

    /// Posts an RDMA write operation
    pub async fn rdma_write(
        &self,
        remote: &SocketAddr,
        local_data: &[u8],
        _remote_addr: u64,
        _remote_key: u32,
    ) -> Result<u64> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(remote)
            .ok_or_else(|| StrataError::NotFound(format!("Connection to {}", remote)))?;

        let state = conn.state.read().await;
        if *state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "Connection not ready: {:?}",
                *state
            )));
        }

        // RDMA write directly to remote memory
        let wr_id = rand::random();

        let completion = WorkCompletion {
            id: wr_id,
            status: CompletionStatus::Success,
            op_type: RdmaOpType::Write,
            bytes_transferred: local_data.len() as u32,
            imm_data: None,
            source_qp: None,
        };

        let _ = self.completion_tx.send(completion).await;

        let mut stats = self.stats.write().await;
        stats.bytes_sent += local_data.len() as u64;
        stats.ops_completed += 1;

        Ok(wr_id)
    }

    /// Posts an RDMA read operation
    pub async fn rdma_read(
        &self,
        remote: &SocketAddr,
        local_buffer: &mut [u8],
        _remote_addr: u64,
        _remote_key: u32,
    ) -> Result<u64> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(remote)
            .ok_or_else(|| StrataError::NotFound(format!("Connection to {}", remote)))?;

        let state = conn.state.read().await;
        if *state != ConnectionState::ReadyToSend {
            return Err(StrataError::InvalidState(format!(
                "Connection not ready: {:?}",
                *state
            )));
        }

        // RDMA read from remote memory
        let wr_id = rand::random();

        let completion = WorkCompletion {
            id: wr_id,
            status: CompletionStatus::Success,
            op_type: RdmaOpType::Read,
            bytes_transferred: local_buffer.len() as u32,
            imm_data: None,
            source_qp: None,
        };

        let _ = self.completion_tx.send(completion).await;

        let mut stats = self.stats.write().await;
        stats.bytes_received += local_buffer.len() as u64;
        stats.ops_completed += 1;

        Ok(wr_id)
    }

    /// Gets transport statistics
    pub async fn stats(&self) -> TransportStats {
        self.stats.read().await.clone()
    }

    /// Shuts down the transport
    pub async fn shutdown(&self) -> Result<()> {
        *self.running.write().await = false;

        // Disconnect all connections
        let connections: Vec<SocketAddr> = {
            let conns = self.connections.read().await;
            conns.keys().cloned().collect()
        };

        for addr in connections {
            let _ = self.disconnect(&addr).await;
        }

        tracing::info!("RDMA transport shut down");
        Ok(())
    }
}

/// Builder for RDMA transport
pub struct RdmaTransportBuilder {
    config: TransportConfig,
}

impl RdmaTransportBuilder {
    pub fn new() -> Self {
        Self {
            config: TransportConfig::default(),
        }
    }

    pub fn transport_type(mut self, t: TransportType) -> Self {
        self.config.transport_type = t;
        self
    }

    pub fn device(mut self, name: &str) -> Self {
        self.config.device_name = Some(name.to_string());
        self
    }

    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }

    pub fn queue_depth(mut self, send: u32, recv: u32) -> Self {
        self.config.send_queue_depth = send;
        self.config.recv_queue_depth = recv;
        self
    }

    pub fn build(self) -> Result<RdmaTransport> {
        RdmaTransport::new(self.config)
    }
}

impl Default for RdmaTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = RdmaTransportBuilder::new()
            .device("mlx5_0")
            .max_message_size(1024 * 1024)
            .build()
            .unwrap();

        assert_eq!(transport.config.device_name, Some("mlx5_0".to_string()));
    }

    #[tokio::test]
    async fn test_transport_initialization() {
        let mut transport = RdmaTransport::new(TransportConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        assert!(transport.capabilities().is_some());
    }

    #[tokio::test]
    async fn test_connect_disconnect() {
        let mut transport = RdmaTransport::new(TransportConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        transport.connect(addr).await.unwrap();

        let stats = transport.stats().await;
        assert_eq!(stats.active_connections, 1);

        transport.disconnect(&addr).await.unwrap();

        let stats = transport.stats().await;
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_completion_status() {
        let completion = WorkCompletion {
            id: 1,
            status: CompletionStatus::Success,
            op_type: RdmaOpType::Send,
            bytes_transferred: 1024,
            imm_data: None,
            source_qp: None,
        };

        assert_eq!(completion.status, CompletionStatus::Success);
    }
}
