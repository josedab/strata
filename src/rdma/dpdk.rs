// DPDK (Data Plane Development Kit) Integration

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// DPDK port configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpdkConfig {
    /// Port ID
    pub port_id: u16,
    /// Number of RX queues
    pub num_rx_queues: u16,
    /// Number of TX queues
    pub num_tx_queues: u16,
    /// RX ring size
    pub rx_ring_size: u16,
    /// TX ring size
    pub tx_ring_size: u16,
    /// Memory pool size
    pub mempool_size: u32,
    /// Memory pool cache size
    pub mempool_cache_size: u32,
    /// MTU size
    pub mtu: u16,
    /// Enable promiscuous mode
    pub promiscuous: bool,
    /// Enable RSS (Receive Side Scaling)
    pub rss_enabled: bool,
    /// RSS hash key
    pub rss_key: Option<Vec<u8>>,
    /// NUMA node
    pub numa_node: Option<u32>,
    /// Burst size for packet processing
    pub burst_size: u16,
}

impl Default for DpdkConfig {
    fn default() -> Self {
        Self {
            port_id: 0,
            num_rx_queues: 4,
            num_tx_queues: 4,
            rx_ring_size: 1024,
            tx_ring_size: 1024,
            mempool_size: 8192,
            mempool_cache_size: 256,
            mtu: 1500,
            promiscuous: false,
            rss_enabled: true,
            rss_key: None,
            numa_node: None,
            burst_size: 32,
        }
    }
}

/// DPDK port state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PortState {
    /// Port not initialized
    Uninitialized,
    /// Port configured but not started
    Configured,
    /// Port is running
    Running,
    /// Port is stopped
    Stopped,
    /// Port has error
    Error,
}

/// DPDK port statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortStats {
    /// Total packets received
    pub rx_packets: u64,
    /// Total packets transmitted
    pub tx_packets: u64,
    /// Total bytes received
    pub rx_bytes: u64,
    /// Total bytes transmitted
    pub tx_bytes: u64,
    /// Receive errors
    pub rx_errors: u64,
    /// Transmit errors
    pub tx_errors: u64,
    /// Receive missed packets
    pub rx_missed: u64,
    /// Receive no buffer errors
    pub rx_no_mbuf: u64,
}

impl Default for PortStats {
    fn default() -> Self {
        Self {
            rx_packets: 0,
            tx_packets: 0,
            rx_bytes: 0,
            tx_bytes: 0,
            rx_errors: 0,
            tx_errors: 0,
            rx_missed: 0,
            rx_no_mbuf: 0,
        }
    }
}

/// Packet buffer for DPDK operations
#[derive(Debug)]
pub struct PacketBuffer {
    /// Buffer ID
    pub id: u64,
    /// Packet data
    data: Vec<u8>,
    /// Data length
    data_len: usize,
    /// Packet offset
    data_off: usize,
    /// Buffer size
    buf_len: usize,
    /// Port ID
    pub port: u16,
    /// Queue ID
    pub queue_id: u16,
    /// Timestamp
    pub timestamp: u64,
    /// Packet flags
    pub flags: PacketFlags,
    /// Reference count
    refcnt: u16,
}

/// Packet flags
#[derive(Debug, Clone, Copy, Default)]
pub struct PacketFlags {
    /// IP checksum offload
    pub ip_cksum: bool,
    /// L4 checksum offload
    pub l4_cksum: bool,
    /// VLAN tag present
    pub vlan: bool,
    /// RSS hash computed
    pub rss_hash: bool,
    /// Packet is multicast
    pub multicast: bool,
    /// Packet is broadcast
    pub broadcast: bool,
}

impl PacketBuffer {
    /// Creates a new packet buffer
    pub fn new(size: usize) -> Self {
        Self {
            id: rand::random(),
            data: vec![0u8; size],
            data_len: 0,
            data_off: 128, // Headroom for prepending headers
            buf_len: size,
            port: 0,
            queue_id: 0,
            timestamp: 0,
            flags: PacketFlags::default(),
            refcnt: 1,
        }
    }

    /// Gets the packet data
    pub fn data(&self) -> &[u8] {
        &self.data[self.data_off..self.data_off + self.data_len]
    }

    /// Gets mutable packet data
    pub fn data_mut(&mut self) -> &mut [u8] {
        let end = self.data_off + self.data_len;
        &mut self.data[self.data_off..end]
    }

    /// Gets the data length
    pub fn len(&self) -> usize {
        self.data_len
    }

    /// Checks if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data_len == 0
    }

    /// Gets available headroom
    pub fn headroom(&self) -> usize {
        self.data_off
    }

    /// Gets available tailroom
    pub fn tailroom(&self) -> usize {
        self.buf_len - self.data_off - self.data_len
    }

    /// Prepends data to the packet
    pub fn prepend(&mut self, len: usize) -> Result<&mut [u8]> {
        if len > self.data_off {
            return Err(StrataError::InvalidOperation(
                "Not enough headroom".to_string(),
            ));
        }
        self.data_off -= len;
        self.data_len += len;
        Ok(&mut self.data[self.data_off..self.data_off + len])
    }

    /// Appends data to the packet
    pub fn append(&mut self, len: usize) -> Result<&mut [u8]> {
        if len > self.tailroom() {
            return Err(StrataError::InvalidOperation(
                "Not enough tailroom".to_string(),
            ));
        }
        let start = self.data_off + self.data_len;
        self.data_len += len;
        Ok(&mut self.data[start..start + len])
    }

    /// Trims from the front
    pub fn trim_front(&mut self, len: usize) -> Result<()> {
        if len > self.data_len {
            return Err(StrataError::InvalidOperation(
                "Trim length exceeds data length".to_string(),
            ));
        }
        self.data_off += len;
        self.data_len -= len;
        Ok(())
    }

    /// Trims from the back
    pub fn trim_back(&mut self, len: usize) -> Result<()> {
        if len > self.data_len {
            return Err(StrataError::InvalidOperation(
                "Trim length exceeds data length".to_string(),
            ));
        }
        self.data_len -= len;
        Ok(())
    }

    /// Copies data into the packet
    pub fn copy_from(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.tailroom() + self.data_len {
            return Err(StrataError::InvalidOperation(
                "Data too large for buffer".to_string(),
            ));
        }
        self.data_len = data.len();
        self.data[self.data_off..self.data_off + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Resets the buffer
    pub fn reset(&mut self) {
        self.data_len = 0;
        self.data_off = 128;
        self.flags = PacketFlags::default();
    }

    /// Increments reference count
    pub fn ref_inc(&mut self) {
        self.refcnt += 1;
    }

    /// Decrements reference count
    pub fn ref_dec(&mut self) -> u16 {
        self.refcnt = self.refcnt.saturating_sub(1);
        self.refcnt
    }
}

/// Packet buffer pool
pub struct PacketPool {
    /// Pool configuration
    config: PacketPoolConfig,
    /// Free buffers
    free: Arc<RwLock<VecDeque<PacketBuffer>>>,
    /// Statistics
    stats: Arc<PacketPoolStats>,
}

/// Packet pool configuration
#[derive(Debug, Clone)]
pub struct PacketPoolConfig {
    /// Number of buffers
    pub num_buffers: usize,
    /// Buffer size
    pub buffer_size: usize,
    /// Cache size per core
    pub cache_size: usize,
}

impl Default for PacketPoolConfig {
    fn default() -> Self {
        Self {
            num_buffers: 8192,
            buffer_size: 2048,
            cache_size: 256,
        }
    }
}

/// Packet pool statistics
pub struct PacketPoolStats {
    /// Allocations
    pub allocations: AtomicU64,
    /// Frees
    pub frees: AtomicU64,
    /// Allocation failures
    pub alloc_failures: AtomicU64,
}

impl PacketPool {
    /// Creates a new packet pool
    pub fn new(config: PacketPoolConfig) -> Self {
        let mut free = VecDeque::with_capacity(config.num_buffers);
        for _ in 0..config.num_buffers {
            free.push_back(PacketBuffer::new(config.buffer_size));
        }

        Self {
            config,
            free: Arc::new(RwLock::new(free)),
            stats: Arc::new(PacketPoolStats {
                allocations: AtomicU64::new(0),
                frees: AtomicU64::new(0),
                alloc_failures: AtomicU64::new(0),
            }),
        }
    }

    /// Allocates a buffer from the pool
    pub async fn alloc(&self) -> Option<PacketBuffer> {
        let mut free = self.free.write().await;
        match free.pop_front() {
            Some(mut buf) => {
                buf.reset();
                self.stats.allocations.fetch_add(1, Ordering::Relaxed);
                Some(buf)
            }
            None => {
                self.stats.alloc_failures.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Returns a buffer to the pool
    pub async fn free(&self, buf: PacketBuffer) {
        self.stats.frees.fetch_add(1, Ordering::Relaxed);
        let mut free = self.free.write().await;
        free.push_back(buf);
    }

    /// Gets pool statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.allocations.load(Ordering::Relaxed),
            self.stats.frees.load(Ordering::Relaxed),
            self.stats.alloc_failures.load(Ordering::Relaxed),
        )
    }

    /// Gets available buffer count
    pub async fn available(&self) -> usize {
        self.free.read().await.len()
    }
}

/// DPDK port abstraction
pub struct DpdkPort {
    /// Configuration
    config: DpdkConfig,
    /// Port state
    state: Arc<RwLock<PortState>>,
    /// Packet pool
    pool: Arc<PacketPool>,
    /// RX queues
    rx_queues: Arc<RwLock<Vec<RxQueue>>>,
    /// TX queues
    tx_queues: Arc<RwLock<Vec<TxQueue>>>,
    /// Port statistics
    stats: Arc<RwLock<PortStats>>,
    /// MAC address
    mac_addr: [u8; 6],
}

/// RX queue
pub struct RxQueue {
    /// Queue ID
    pub id: u16,
    /// Ring buffer
    ring: VecDeque<PacketBuffer>,
    /// Ring size
    ring_size: u16,
    /// Packets received
    rx_packets: u64,
    /// Bytes received
    rx_bytes: u64,
}

impl RxQueue {
    fn new(id: u16, ring_size: u16) -> Self {
        Self {
            id,
            ring: VecDeque::with_capacity(ring_size as usize),
            ring_size,
            rx_packets: 0,
            rx_bytes: 0,
        }
    }
}

/// TX queue
pub struct TxQueue {
    /// Queue ID
    pub id: u16,
    /// Ring buffer
    ring: VecDeque<PacketBuffer>,
    /// Ring size
    ring_size: u16,
    /// Packets transmitted
    tx_packets: u64,
    /// Bytes transmitted
    tx_bytes: u64,
}

impl TxQueue {
    fn new(id: u16, ring_size: u16) -> Self {
        Self {
            id,
            ring: VecDeque::with_capacity(ring_size as usize),
            ring_size,
            tx_packets: 0,
            tx_bytes: 0,
        }
    }
}

impl DpdkPort {
    /// Creates a new DPDK port
    pub fn new(config: DpdkConfig) -> Result<Self> {
        let pool_config = PacketPoolConfig {
            num_buffers: config.mempool_size as usize,
            buffer_size: config.mtu as usize + 128, // MTU + headroom
            cache_size: config.mempool_cache_size as usize,
        };

        let mut rx_queues = Vec::with_capacity(config.num_rx_queues as usize);
        for i in 0..config.num_rx_queues {
            rx_queues.push(RxQueue::new(i, config.rx_ring_size));
        }

        let mut tx_queues = Vec::with_capacity(config.num_tx_queues as usize);
        for i in 0..config.num_tx_queues {
            tx_queues.push(TxQueue::new(i, config.tx_ring_size));
        }

        Ok(Self {
            config,
            state: Arc::new(RwLock::new(PortState::Configured)),
            pool: Arc::new(PacketPool::new(pool_config)),
            rx_queues: Arc::new(RwLock::new(rx_queues)),
            tx_queues: Arc::new(RwLock::new(tx_queues)),
            stats: Arc::new(RwLock::new(PortStats::default())),
            mac_addr: [0; 6],
        })
    }

    /// Initializes the port (simulated)
    pub async fn init(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state != PortState::Configured {
            return Err(StrataError::InvalidOperation(
                "Port must be in Configured state to initialize".to_string(),
            ));
        }

        // Generate random MAC for simulation
        let mut mac = [0u8; 6];
        mac[0] = 0x02; // Locally administered
        for byte in mac.iter_mut().skip(1) {
            *byte = rand::random();
        }
        self.mac_addr = mac;

        *state = PortState::Configured;
        Ok(())
    }

    /// Starts the port
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state != PortState::Configured && *state != PortState::Stopped {
            return Err(StrataError::InvalidOperation(
                "Port must be Configured or Stopped to start".to_string(),
            ));
        }
        *state = PortState::Running;
        Ok(())
    }

    /// Stops the port
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state != PortState::Running {
            return Err(StrataError::InvalidOperation(
                "Port must be Running to stop".to_string(),
            ));
        }
        *state = PortState::Stopped;
        Ok(())
    }

    /// Receives a burst of packets
    pub async fn rx_burst(&self, queue_id: u16, max_pkts: u16) -> Result<Vec<PacketBuffer>> {
        let state = self.state.read().await;
        if *state != PortState::Running {
            return Err(StrataError::InvalidOperation("Port not running".to_string()));
        }
        drop(state);

        let mut rx_queues = self.rx_queues.write().await;
        let queue = rx_queues.get_mut(queue_id as usize).ok_or_else(|| {
            StrataError::InvalidOperation(format!("Invalid queue ID: {}", queue_id))
        })?;

        let mut packets = Vec::with_capacity(max_pkts as usize);
        let count = max_pkts.min(queue.ring.len() as u16);

        for _ in 0..count {
            if let Some(pkt) = queue.ring.pop_front() {
                queue.rx_bytes += pkt.len() as u64;
                queue.rx_packets += 1;
                packets.push(pkt);
            }
        }

        // Update port stats
        let mut stats = self.stats.write().await;
        stats.rx_packets += packets.len() as u64;
        stats.rx_bytes += packets.iter().map(|p| p.len() as u64).sum::<u64>();

        Ok(packets)
    }

    /// Transmits a burst of packets
    pub async fn tx_burst(&self, queue_id: u16, packets: Vec<PacketBuffer>) -> Result<u16> {
        let state = self.state.read().await;
        if *state != PortState::Running {
            return Err(StrataError::InvalidOperation("Port not running".to_string()));
        }
        drop(state);

        let mut tx_queues = self.tx_queues.write().await;
        let queue = tx_queues.get_mut(queue_id as usize).ok_or_else(|| {
            StrataError::InvalidOperation(format!("Invalid queue ID: {}", queue_id))
        })?;

        let mut sent = 0u16;
        let mut bytes = 0u64;

        for pkt in packets {
            if queue.ring.len() < queue.ring_size as usize {
                bytes += pkt.len() as u64;
                queue.ring.push_back(pkt);
                sent += 1;
            } else {
                break;
            }
        }

        queue.tx_packets += sent as u64;
        queue.tx_bytes += bytes;

        // Update port stats
        let mut stats = self.stats.write().await;
        stats.tx_packets += sent as u64;
        stats.tx_bytes += bytes;

        Ok(sent)
    }

    /// Injects packets into RX queue (for testing/simulation)
    pub async fn inject_rx(&self, queue_id: u16, packets: Vec<PacketBuffer>) -> Result<()> {
        let mut rx_queues = self.rx_queues.write().await;
        let queue = rx_queues.get_mut(queue_id as usize).ok_or_else(|| {
            StrataError::InvalidOperation(format!("Invalid queue ID: {}", queue_id))
        })?;

        for pkt in packets {
            if queue.ring.len() < queue.ring_size as usize {
                queue.ring.push_back(pkt);
            }
        }

        Ok(())
    }

    /// Allocates a packet buffer
    pub async fn alloc_buffer(&self) -> Option<PacketBuffer> {
        self.pool.alloc().await
    }

    /// Frees a packet buffer
    pub async fn free_buffer(&self, buf: PacketBuffer) {
        self.pool.free(buf).await;
    }

    /// Gets port statistics
    pub async fn stats(&self) -> PortStats {
        self.stats.read().await.clone()
    }

    /// Resets port statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = PortStats::default();
    }

    /// Gets the MAC address
    pub fn mac_addr(&self) -> &[u8; 6] {
        &self.mac_addr
    }

    /// Gets port state
    pub async fn state(&self) -> PortState {
        *self.state.read().await
    }

    /// Gets port configuration
    pub fn config(&self) -> &DpdkConfig {
        &self.config
    }

    /// Gets the packet pool
    pub fn pool(&self) -> Arc<PacketPool> {
        Arc::clone(&self.pool)
    }
}

/// Flow rule for hardware filtering
#[derive(Debug, Clone)]
pub struct FlowRule {
    /// Rule ID
    pub id: u64,
    /// Priority (higher = more priority)
    pub priority: u32,
    /// Match pattern
    pub pattern: FlowPattern,
    /// Actions to perform
    pub actions: Vec<FlowAction>,
}

/// Flow match pattern
#[derive(Debug, Clone)]
pub struct FlowPattern {
    /// Source MAC
    pub src_mac: Option<[u8; 6]>,
    /// Destination MAC
    pub dst_mac: Option<[u8; 6]>,
    /// EtherType
    pub ether_type: Option<u16>,
    /// Source IP
    pub src_ip: Option<u32>,
    /// Destination IP
    pub dst_ip: Option<u32>,
    /// IP protocol
    pub ip_proto: Option<u8>,
    /// Source port
    pub src_port: Option<u16>,
    /// Destination port
    pub dst_port: Option<u16>,
    /// VLAN ID
    pub vlan_id: Option<u16>,
}

impl Default for FlowPattern {
    fn default() -> Self {
        Self {
            src_mac: None,
            dst_mac: None,
            ether_type: None,
            src_ip: None,
            dst_ip: None,
            ip_proto: None,
            src_port: None,
            dst_port: None,
            vlan_id: None,
        }
    }
}

/// Flow action
#[derive(Debug, Clone)]
pub enum FlowAction {
    /// Queue to specific RX queue
    Queue(u16),
    /// Drop the packet
    Drop,
    /// Mark with value
    Mark(u32),
    /// RSS to queue group
    Rss(Vec<u16>),
    /// Passthru (no action)
    Passthru,
}

/// Flow manager for hardware flow rules
pub struct FlowManager {
    /// Port ID
    port_id: u16,
    /// Active rules
    rules: Arc<RwLock<Vec<FlowRule>>>,
    /// Next rule ID
    next_id: AtomicU64,
}

impl FlowManager {
    /// Creates a new flow manager
    pub fn new(port_id: u16) -> Self {
        Self {
            port_id,
            rules: Arc::new(RwLock::new(Vec::new())),
            next_id: AtomicU64::new(1),
        }
    }

    /// Adds a flow rule
    pub async fn add_rule(&self, pattern: FlowPattern, actions: Vec<FlowAction>, priority: u32) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let rule = FlowRule {
            id,
            priority,
            pattern,
            actions,
        };

        let mut rules = self.rules.write().await;
        rules.push(rule);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        Ok(id)
    }

    /// Removes a flow rule
    pub async fn remove_rule(&self, rule_id: u64) -> Result<()> {
        let mut rules = self.rules.write().await;
        let idx = rules.iter().position(|r| r.id == rule_id).ok_or_else(|| {
            StrataError::NotFound(format!("Flow rule {} not found", rule_id))
        })?;
        rules.remove(idx);
        Ok(())
    }

    /// Lists all flow rules
    pub async fn list_rules(&self) -> Vec<FlowRule> {
        self.rules.read().await.clone()
    }

    /// Matches a packet against rules
    pub async fn match_packet(&self, _packet: &PacketBuffer) -> Option<&[FlowAction]> {
        // In real implementation, this would parse packet headers
        // and match against flow patterns
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_buffer() {
        let mut buf = PacketBuffer::new(2048);

        assert_eq!(buf.headroom(), 128);
        assert!(buf.is_empty());

        let data = b"Hello DPDK";
        buf.copy_from(data).unwrap();

        assert_eq!(buf.len(), 10);
        assert_eq!(buf.data(), data);
    }

    #[test]
    fn test_packet_prepend_append() {
        let mut buf = PacketBuffer::new(2048);
        buf.copy_from(b"payload").unwrap();

        // Prepend header
        let header = buf.prepend(4).unwrap();
        header.copy_from_slice(b"HDR:");

        assert_eq!(buf.len(), 11);
        assert_eq!(&buf.data()[..4], b"HDR:");

        // Append trailer
        let trailer = buf.append(4).unwrap();
        trailer.copy_from_slice(b":END");

        assert_eq!(buf.len(), 15);
    }

    #[tokio::test]
    async fn test_packet_pool() {
        let config = PacketPoolConfig {
            num_buffers: 10,
            buffer_size: 1024,
            cache_size: 4,
        };
        let pool = PacketPool::new(config);

        assert_eq!(pool.available().await, 10);

        let buf = pool.alloc().await.unwrap();
        assert_eq!(pool.available().await, 9);

        pool.free(buf).await;
        assert_eq!(pool.available().await, 10);
    }

    #[tokio::test]
    async fn test_dpdk_port() {
        let config = DpdkConfig {
            port_id: 0,
            num_rx_queues: 2,
            num_tx_queues: 2,
            rx_ring_size: 64,
            tx_ring_size: 64,
            mempool_size: 256,
            ..Default::default()
        };

        let mut port = DpdkPort::new(config).unwrap();
        port.init().await.unwrap();
        port.start().await.unwrap();

        assert_eq!(port.state().await, PortState::Running);

        // Inject some test packets
        let mut packets = Vec::new();
        for _ in 0..5 {
            let mut buf = port.alloc_buffer().await.unwrap();
            buf.copy_from(b"test packet").unwrap();
            packets.push(buf);
        }
        port.inject_rx(0, packets).await.unwrap();

        // Receive packets
        let rx_pkts = port.rx_burst(0, 10).await.unwrap();
        assert_eq!(rx_pkts.len(), 5);

        let stats = port.stats().await;
        assert_eq!(stats.rx_packets, 5);

        port.stop().await.unwrap();
        assert_eq!(port.state().await, PortState::Stopped);
    }

    #[tokio::test]
    async fn test_flow_manager() {
        let manager = FlowManager::new(0);

        let pattern = FlowPattern {
            dst_port: Some(80),
            ip_proto: Some(6), // TCP
            ..Default::default()
        };

        let rule_id = manager.add_rule(
            pattern,
            vec![FlowAction::Queue(1)],
            100,
        ).await.unwrap();

        let rules = manager.list_rules().await;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id, rule_id);

        manager.remove_rule(rule_id).await.unwrap();
        assert!(manager.list_rules().await.is_empty());
    }
}
