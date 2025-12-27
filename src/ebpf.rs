//! eBPF Data Plane
//!
//! This module provides high-performance data plane operations using eBPF
//! (extended Berkeley Packet Filter). Features:
//! - Zero-copy I/O operations in kernel space
//! - Traffic classification and prioritization
//! - Real-time performance monitoring
//! - Network and storage observability
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    eBPF Data Plane                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Programs: XDP │ TC │ Tracepoints │ Kprobes │ Uprobes      │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Maps: Hash │ Array │ Ring Buffer │ LPM Trie               │
//! ├─────────────────────────────────────────────────────────────┤
//! │  User Space: Program Loader │ Map Access │ Perf Events     │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::info;
use chrono::{DateTime, Utc};

/// eBPF program types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProgramType {
    /// eXpress Data Path - fastest network processing
    XDP,
    /// Traffic Control - network scheduling
    TC,
    /// Socket filter
    SocketFilter,
    /// Kernel tracepoint
    Tracepoint,
    /// Kernel function probe
    Kprobe,
    /// User function probe
    Uprobe,
    /// Cgroup socket
    CgroupSock,
    /// Perf event
    PerfEvent,
    /// Raw tracepoint
    RawTracepoint,
    /// LSM (Linux Security Module) hook
    LSM,
}

/// eBPF map types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MapType {
    /// Key-value hash map
    Hash,
    /// Fixed-size array
    Array,
    /// Per-CPU hash map
    PerCpuHash,
    /// Per-CPU array
    PerCpuArray,
    /// LRU hash map
    LruHash,
    /// Ring buffer for events
    RingBuffer,
    /// Perf event array
    PerfEventArray,
    /// Longest prefix match trie
    LpmTrie,
    /// Stack trace
    StackTrace,
    /// Program array for tail calls
    ProgArray,
}

/// eBPF program metadata
#[derive(Debug, Clone)]
pub struct ProgramInfo {
    pub id: ProgramId,
    pub name: String,
    pub program_type: ProgramType,
    pub attach_point: String,
    pub loaded_at: DateTime<Utc>,
    pub instructions: u32,
    pub verified: bool,
}

/// Unique program identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProgramId(pub u32);

impl ProgramId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for ProgramId {
    fn default() -> Self {
        Self::new()
    }
}

/// eBPF map metadata
#[derive(Debug, Clone)]
pub struct MapInfo {
    pub id: MapId,
    pub name: String,
    pub map_type: MapType,
    pub key_size: u32,
    pub value_size: u32,
    pub max_entries: u32,
}

/// Unique map identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MapId(pub u32);

impl MapId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for MapId {
    fn default() -> Self {
        Self::new()
    }
}

/// Traffic classification result
#[derive(Debug, Clone)]
pub struct TrafficClass {
    pub class_id: u32,
    pub priority: u8,
    pub bandwidth_limit: Option<u64>,
    pub action: TrafficAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficAction {
    Allow,
    Drop,
    RateLimit,
    Mark(u32),
    Redirect(u32),
}

/// Network flow key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub src_ip: [u8; 16],
    pub dst_ip: [u8; 16],
    pub src_port: u16,
    pub dst_port: u16,
    pub protocol: u8,
}

impl FlowKey {
    pub fn tcp(src_ip: &[u8], dst_ip: &[u8], src_port: u16, dst_port: u16) -> Self {
        let mut src = [0u8; 16];
        let mut dst = [0u8; 16];
        src[..src_ip.len().min(16)].copy_from_slice(&src_ip[..src_ip.len().min(16)]);
        dst[..dst_ip.len().min(16)].copy_from_slice(&dst_ip[..dst_ip.len().min(16)]);
        Self {
            src_ip: src,
            dst_ip: dst,
            src_port,
            dst_port,
            protocol: 6, // TCP
        }
    }

    pub fn udp(src_ip: &[u8], dst_ip: &[u8], src_port: u16, dst_port: u16) -> Self {
        let mut key = Self::tcp(src_ip, dst_ip, src_port, dst_port);
        key.protocol = 17; // UDP
        key
    }
}

/// Flow statistics
#[derive(Debug, Clone, Default)]
pub struct FlowStats {
    pub packets: u64,
    pub bytes: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
}

/// I/O operation statistics
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub read_ops: u64,
    pub write_ops: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_latency_ns: u64,
    pub write_latency_ns: u64,
}

/// Performance event
#[derive(Debug, Clone)]
pub struct PerfEvent {
    pub timestamp: DateTime<Utc>,
    pub event_type: PerfEventType,
    pub cpu: u32,
    pub pid: u32,
    pub comm: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerfEventType {
    /// System call entry
    SyscallEnter,
    /// System call exit
    SyscallExit,
    /// Block I/O request
    BlockRqIssue,
    /// Block I/O complete
    BlockRqComplete,
    /// Network packet
    NetPacket,
    /// Memory allocation
    MemAlloc,
    /// Custom event
    Custom(u32),
}

/// Simulated eBPF map for userspace
pub struct EbpfMap<K, V> {
    info: MapInfo,
    data: RwLock<HashMap<K, V>>,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> EbpfMap<K, V> {
    pub fn new(name: &str, map_type: MapType, max_entries: u32) -> Self {
        Self {
            info: MapInfo {
                id: MapId::new(),
                name: name.to_string(),
                map_type,
                key_size: std::mem::size_of::<K>() as u32,
                value_size: std::mem::size_of::<V>() as u32,
                max_entries,
            },
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn lookup(&self, key: &K) -> Option<V> {
        self.data.read().await.get(key).cloned()
    }

    pub async fn update(&self, key: K, value: V) -> Result<()> {
        let mut data = self.data.write().await;
        if data.len() >= self.info.max_entries as usize && !data.contains_key(&key) {
            return Err(StrataError::InvalidData("Map full".into()));
        }
        data.insert(key, value);
        Ok(())
    }

    pub async fn delete(&self, key: &K) -> bool {
        self.data.write().await.remove(key).is_some()
    }

    pub async fn len(&self) -> usize {
        self.data.read().await.len()
    }

    pub fn info(&self) -> &MapInfo {
        &self.info
    }
}

/// Ring buffer for perf events
pub struct RingBuffer {
    capacity: usize,
    events: RwLock<VecDeque<PerfEvent>>,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            events: RwLock::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub async fn push(&self, event: PerfEvent) {
        let mut events = self.events.write().await;
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }

    pub async fn pop(&self) -> Option<PerfEvent> {
        self.events.write().await.pop_front()
    }

    pub async fn drain(&self, max: usize) -> Vec<PerfEvent> {
        let mut events = self.events.write().await;
        let count = max.min(events.len());
        events.drain(..count).collect()
    }

    pub async fn len(&self) -> usize {
        self.events.read().await.len()
    }
}

/// Traffic classifier using simulated XDP
pub struct TrafficClassifier {
    rules: RwLock<Vec<ClassificationRule>>,
    flow_stats: Arc<EbpfMap<FlowKey, FlowStats>>,
    default_action: TrafficAction,
}

#[derive(Debug, Clone)]
pub struct ClassificationRule {
    pub id: u32,
    pub priority: u32,
    pub matcher: FlowMatcher,
    pub action: TrafficAction,
    pub class_id: u32,
}

#[derive(Debug, Clone)]
pub enum FlowMatcher {
    /// Match source IP prefix
    SrcPrefix { prefix: [u8; 16], len: u8 },
    /// Match destination IP prefix
    DstPrefix { prefix: [u8; 16], len: u8 },
    /// Match port range
    PortRange { min: u16, max: u16 },
    /// Match protocol
    Protocol(u8),
    /// Match all
    Any,
    /// Combine matchers with AND
    And(Vec<FlowMatcher>),
    /// Combine matchers with OR
    Or(Vec<FlowMatcher>),
}

impl FlowMatcher {
    pub fn matches(&self, flow: &FlowKey) -> bool {
        match self {
            FlowMatcher::SrcPrefix { prefix, len } => {
                Self::prefix_match(&flow.src_ip, prefix, *len)
            }
            FlowMatcher::DstPrefix { prefix, len } => {
                Self::prefix_match(&flow.dst_ip, prefix, *len)
            }
            FlowMatcher::PortRange { min, max } => {
                (flow.src_port >= *min && flow.src_port <= *max)
                    || (flow.dst_port >= *min && flow.dst_port <= *max)
            }
            FlowMatcher::Protocol(proto) => flow.protocol == *proto,
            FlowMatcher::Any => true,
            FlowMatcher::And(matchers) => matchers.iter().all(|m| m.matches(flow)),
            FlowMatcher::Or(matchers) => matchers.iter().any(|m| m.matches(flow)),
        }
    }

    fn prefix_match(addr: &[u8; 16], prefix: &[u8; 16], len: u8) -> bool {
        let full_bytes = len / 8;
        let remaining_bits = len % 8;

        for i in 0..full_bytes as usize {
            if addr[i] != prefix[i] {
                return false;
            }
        }

        if remaining_bits > 0 {
            let mask = 0xFF << (8 - remaining_bits);
            let i = full_bytes as usize;
            if (addr[i] & mask) != (prefix[i] & mask) {
                return false;
            }
        }

        true
    }
}

impl TrafficClassifier {
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(Vec::new()),
            flow_stats: Arc::new(EbpfMap::new("flow_stats", MapType::LruHash, 65536)),
            default_action: TrafficAction::Allow,
        }
    }

    pub async fn add_rule(&self, rule: ClassificationRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    pub async fn remove_rule(&self, id: u32) -> bool {
        let mut rules = self.rules.write().await;
        let len = rules.len();
        rules.retain(|r| r.id != id);
        rules.len() < len
    }

    pub async fn classify(&self, flow: &FlowKey) -> TrafficClass {
        let rules = self.rules.read().await;

        for rule in rules.iter() {
            if rule.matcher.matches(flow) {
                return TrafficClass {
                    class_id: rule.class_id,
                    priority: (rule.priority % 256) as u8,
                    bandwidth_limit: None,
                    action: rule.action,
                };
            }
        }

        TrafficClass {
            class_id: 0,
            priority: 0,
            bandwidth_limit: None,
            action: self.default_action,
        }
    }

    pub async fn record_flow(&self, flow: &FlowKey, bytes: u64) {
        let mut stats = self.flow_stats.lookup(flow).await.unwrap_or_default();
        let now = Utc::now();

        stats.packets += 1;
        stats.bytes += bytes;
        if stats.first_seen.is_none() {
            stats.first_seen = Some(now);
        }
        stats.last_seen = Some(now);

        let _ = self.flow_stats.update(flow.clone(), stats).await;
    }

    pub async fn get_flow_stats(&self, flow: &FlowKey) -> Option<FlowStats> {
        self.flow_stats.lookup(flow).await
    }
}

impl Default for TrafficClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// I/O tracer for storage operations
pub struct IoTracer {
    io_stats: Arc<EbpfMap<u32, IoStats>>,
    events: RingBuffer,
    tracing_enabled: RwLock<bool>,
}

impl IoTracer {
    pub fn new() -> Self {
        Self {
            io_stats: Arc::new(EbpfMap::new("io_stats", MapType::PerCpuHash, 1024)),
            events: RingBuffer::new(10000),
            tracing_enabled: RwLock::new(true),
        }
    }

    pub async fn trace_read(&self, pid: u32, bytes: u64, latency_ns: u64) {
        if !*self.tracing_enabled.read().await {
            return;
        }

        let mut stats = self.io_stats.lookup(&pid).await.unwrap_or_default();
        stats.read_ops += 1;
        stats.read_bytes += bytes;
        stats.read_latency_ns += latency_ns;

        let _ = self.io_stats.update(pid, stats).await;

        self.events.push(PerfEvent {
            timestamp: Utc::now(),
            event_type: PerfEventType::BlockRqComplete,
            cpu: 0,
            pid,
            comm: String::new(),
            data: vec![],
        }).await;
    }

    pub async fn trace_write(&self, pid: u32, bytes: u64, latency_ns: u64) {
        if !*self.tracing_enabled.read().await {
            return;
        }

        let mut stats = self.io_stats.lookup(&pid).await.unwrap_or_default();
        stats.write_ops += 1;
        stats.write_bytes += bytes;
        stats.write_latency_ns += latency_ns;

        let _ = self.io_stats.update(pid, stats).await;

        self.events.push(PerfEvent {
            timestamp: Utc::now(),
            event_type: PerfEventType::BlockRqComplete,
            cpu: 0,
            pid,
            comm: String::new(),
            data: vec![],
        }).await;
    }

    pub async fn get_io_stats(&self, pid: u32) -> Option<IoStats> {
        self.io_stats.lookup(&pid).await
    }

    pub async fn get_events(&self, max: usize) -> Vec<PerfEvent> {
        self.events.drain(max).await
    }

    pub async fn enable_tracing(&self) {
        *self.tracing_enabled.write().await = true;
    }

    pub async fn disable_tracing(&self) {
        *self.tracing_enabled.write().await = false;
    }
}

impl Default for IoTracer {
    fn default() -> Self {
        Self::new()
    }
}

/// Rate limiter using token bucket
pub struct RateLimiter {
    limits: RwLock<HashMap<u32, TokenBucket>>,
    default_rate: u64,
}

struct TokenBucket {
    tokens: f64,
    rate: f64,
    capacity: f64,
    last_update: Instant,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            rate,
            capacity,
            last_update: Instant::now(),
        }
    }

    fn consume(&mut self, amount: f64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;

        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);

        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }
}

impl RateLimiter {
    pub fn new(default_rate: u64) -> Self {
        Self {
            limits: RwLock::new(HashMap::new()),
            default_rate,
        }
    }

    pub async fn set_limit(&self, class_id: u32, rate: u64, burst: u64) {
        self.limits.write().await.insert(
            class_id,
            TokenBucket::new(rate as f64, burst as f64),
        );
    }

    pub async fn allow(&self, class_id: u32, bytes: u64) -> bool {
        let mut limits = self.limits.write().await;
        let bucket = limits.entry(class_id).or_insert_with(|| {
            TokenBucket::new(self.default_rate as f64, self.default_rate as f64 * 10.0)
        });
        bucket.consume(bytes as f64)
    }

    pub async fn remove_limit(&self, class_id: u32) {
        self.limits.write().await.remove(&class_id);
    }
}

/// eBPF data plane controller
pub struct EbpfDataPlane {
    classifier: TrafficClassifier,
    io_tracer: IoTracer,
    rate_limiter: RateLimiter,
    programs: RwLock<HashMap<ProgramId, ProgramInfo>>,
}

impl EbpfDataPlane {
    pub fn new() -> Self {
        Self {
            classifier: TrafficClassifier::new(),
            io_tracer: IoTracer::new(),
            rate_limiter: RateLimiter::new(1_000_000_000), // 1 GB/s default
            programs: RwLock::new(HashMap::new()),
        }
    }

    pub fn classifier(&self) -> &TrafficClassifier {
        &self.classifier
    }

    pub fn io_tracer(&self) -> &IoTracer {
        &self.io_tracer
    }

    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    /// Load a simulated eBPF program
    pub async fn load_program(&self, name: &str, prog_type: ProgramType, attach: &str) -> ProgramId {
        let id = ProgramId::new();
        let info = ProgramInfo {
            id,
            name: name.to_string(),
            program_type: prog_type,
            attach_point: attach.to_string(),
            loaded_at: Utc::now(),
            instructions: 100,
            verified: true,
        };

        self.programs.write().await.insert(id, info);
        info!(program = name, "Loaded eBPF program");
        id
    }

    /// Unload a program
    pub async fn unload_program(&self, id: ProgramId) -> bool {
        self.programs.write().await.remove(&id).is_some()
    }

    /// List loaded programs
    pub async fn list_programs(&self) -> Vec<ProgramInfo> {
        self.programs.read().await.values().cloned().collect()
    }

    /// Process a packet through the data plane
    pub async fn process_packet(&self, flow: &FlowKey, bytes: u64) -> TrafficAction {
        // Classify the flow
        let class = self.classifier.classify(flow).await;

        // Check rate limit
        if !self.rate_limiter.allow(class.class_id, bytes).await {
            return TrafficAction::Drop;
        }

        // Record flow stats
        self.classifier.record_flow(flow, bytes).await;

        class.action
    }

    /// Process an I/O operation
    pub async fn process_io(&self, pid: u32, is_read: bool, bytes: u64, latency_ns: u64) {
        if is_read {
            self.io_tracer.trace_read(pid, bytes, latency_ns).await;
        } else {
            self.io_tracer.trace_write(pid, bytes, latency_ns).await;
        }
    }
}

impl Default for EbpfDataPlane {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics aggregator for eBPF data
pub struct MetricsAggregator {
    data_plane: Arc<EbpfDataPlane>,
    metrics: RwLock<AggregatedMetrics>,
}

#[derive(Debug, Clone, Default)]
pub struct AggregatedMetrics {
    pub total_packets: u64,
    pub total_bytes: u64,
    pub dropped_packets: u64,
    pub rate_limited_bytes: u64,
    pub io_read_ops: u64,
    pub io_write_ops: u64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub avg_read_latency_ns: u64,
    pub avg_write_latency_ns: u64,
    pub active_flows: u64,
}

impl MetricsAggregator {
    pub fn new(data_plane: Arc<EbpfDataPlane>) -> Self {
        Self {
            data_plane,
            metrics: RwLock::new(AggregatedMetrics::default()),
        }
    }

    pub async fn get_metrics(&self) -> AggregatedMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn update_packet_metrics(&self, bytes: u64, dropped: bool) {
        let mut metrics = self.metrics.write().await;
        metrics.total_packets += 1;
        metrics.total_bytes += bytes;
        if dropped {
            metrics.dropped_packets += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_id() {
        let id1 = ProgramId::new();
        let id2 = ProgramId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_flow_key() {
        let flow = FlowKey::tcp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 80);
        assert_eq!(flow.protocol, 6);
        assert_eq!(flow.src_port, 12345);
        assert_eq!(flow.dst_port, 80);

        let udp = FlowKey::udp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 53);
        assert_eq!(udp.protocol, 17);
    }

    #[test]
    fn test_flow_matcher() {
        let flow = FlowKey::tcp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 80);

        assert!(FlowMatcher::Any.matches(&flow));
        assert!(FlowMatcher::Protocol(6).matches(&flow));
        assert!(!FlowMatcher::Protocol(17).matches(&flow));
        assert!(FlowMatcher::PortRange { min: 80, max: 443 }.matches(&flow));
        assert!(!FlowMatcher::PortRange { min: 443, max: 8080 }.matches(&flow));
    }

    #[tokio::test]
    async fn test_ebpf_map() {
        let map: EbpfMap<u32, u64> = EbpfMap::new("test", MapType::Hash, 100);

        map.update(1, 100).await.unwrap();
        map.update(2, 200).await.unwrap();

        assert_eq!(map.lookup(&1).await, Some(100));
        assert_eq!(map.lookup(&2).await, Some(200));
        assert_eq!(map.lookup(&3).await, None);

        assert!(map.delete(&1).await);
        assert_eq!(map.lookup(&1).await, None);
    }

    #[tokio::test]
    async fn test_ring_buffer() {
        let rb = RingBuffer::new(3);

        for i in 0..5 {
            rb.push(PerfEvent {
                timestamp: Utc::now(),
                event_type: PerfEventType::Custom(i),
                cpu: 0,
                pid: i,
                comm: String::new(),
                data: vec![],
            }).await;
        }

        // Only last 3 should remain
        assert_eq!(rb.len().await, 3);

        let events = rb.drain(10).await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].pid, 2);
        assert_eq!(events[1].pid, 3);
        assert_eq!(events[2].pid, 4);
    }

    #[tokio::test]
    async fn test_traffic_classifier() {
        let classifier = TrafficClassifier::new();

        classifier.add_rule(ClassificationRule {
            id: 1,
            priority: 100,
            matcher: FlowMatcher::PortRange { min: 80, max: 80 },
            action: TrafficAction::Allow,
            class_id: 1,
        }).await;

        classifier.add_rule(ClassificationRule {
            id: 2,
            priority: 50,
            matcher: FlowMatcher::Protocol(17),
            action: TrafficAction::RateLimit,
            class_id: 2,
        }).await;

        let http_flow = FlowKey::tcp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 80);
        let class = classifier.classify(&http_flow).await;
        assert_eq!(class.class_id, 1);
        assert!(matches!(class.action, TrafficAction::Allow));

        let udp_flow = FlowKey::udp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 53);
        let class = classifier.classify(&udp_flow).await;
        assert_eq!(class.class_id, 2);
    }

    #[tokio::test]
    async fn test_io_tracer() {
        let tracer = IoTracer::new();

        tracer.trace_read(1234, 4096, 1000).await;
        tracer.trace_read(1234, 8192, 2000).await;
        tracer.trace_write(1234, 1024, 500).await;

        let stats = tracer.get_io_stats(1234).await.unwrap();
        assert_eq!(stats.read_ops, 2);
        assert_eq!(stats.write_ops, 1);
        assert_eq!(stats.read_bytes, 12288);
        assert_eq!(stats.write_bytes, 1024);
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(1000);
        limiter.set_limit(1, 100, 200).await;

        // First requests should succeed
        assert!(limiter.allow(1, 50).await);
        assert!(limiter.allow(1, 50).await);
        assert!(limiter.allow(1, 50).await);

        // Eventually should be rate limited
        let mut denied = false;
        for _ in 0..100 {
            if !limiter.allow(1, 50).await {
                denied = true;
                break;
            }
        }
        assert!(denied);
    }

    #[tokio::test]
    async fn test_ebpf_data_plane() {
        let data_plane = EbpfDataPlane::new();

        // Load a program
        let prog_id = data_plane.load_program("test_xdp", ProgramType::XDP, "eth0").await;
        let programs = data_plane.list_programs().await;
        assert_eq!(programs.len(), 1);

        // Add classification rule
        data_plane.classifier().add_rule(ClassificationRule {
            id: 1,
            priority: 100,
            matcher: FlowMatcher::Any,
            action: TrafficAction::Allow,
            class_id: 1,
        }).await;

        // Process a packet
        let flow = FlowKey::tcp(&[192, 168, 1, 1], &[10, 0, 0, 1], 12345, 80);
        let action = data_plane.process_packet(&flow, 1500).await;
        assert!(matches!(action, TrafficAction::Allow));

        // Process I/O
        data_plane.process_io(1234, true, 4096, 1000).await;
        let stats = data_plane.io_tracer().get_io_stats(1234).await.unwrap();
        assert_eq!(stats.read_ops, 1);

        // Unload program
        assert!(data_plane.unload_program(prog_id).await);
    }

    #[test]
    fn test_prefix_match() {
        let mut prefix = [0u8; 16];
        prefix[0] = 192;
        prefix[1] = 168;

        let mut addr1 = [0u8; 16];
        addr1[0] = 192;
        addr1[1] = 168;
        addr1[2] = 1;
        addr1[3] = 1;

        // /16 prefix match
        assert!(FlowMatcher::prefix_match(&addr1, &prefix, 16));

        // /24 would not match if prefix is only /16
        let mut prefix24 = prefix;
        prefix24[2] = 2;
        assert!(!FlowMatcher::prefix_match(&addr1, &prefix24, 24));
    }
}
