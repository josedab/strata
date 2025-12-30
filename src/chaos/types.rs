//! Core types for chaos engineering.

use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ============================================================================
// Fault Types
// ============================================================================

/// Types of faults that can be injected
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Fault {
    /// Network partition between nodes
    NetworkPartition {
        /// Nodes to partition
        nodes: Vec<NodeId>,
        /// Duration of partition
        duration: Duration,
        /// Whether partition is bidirectional
        bidirectional: bool,
    },

    /// Network latency injection
    NetworkLatency {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Latency to add
        latency: Duration,
        /// Jitter (random variation)
        jitter: Duration,
        /// Duration of fault
        duration: Duration,
    },

    /// Packet loss
    PacketLoss {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Loss percentage (0-100)
        loss_percent: u8,
        /// Duration of fault
        duration: Duration,
    },

    /// Bandwidth limitation
    BandwidthLimit {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Bandwidth limit in bytes per second
        bytes_per_second: u64,
        /// Duration of fault
        duration: Duration,
    },

    /// Disk failure simulation
    DiskFailure {
        /// Target node
        node: NodeId,
        /// Disk identifier
        disk_id: String,
        /// Failure mode
        mode: DiskFailureMode,
        /// Duration of fault
        duration: Duration,
    },

    /// Disk slowdown
    DiskSlowdown {
        /// Target node
        node: NodeId,
        /// Read latency to add
        read_latency: Duration,
        /// Write latency to add
        write_latency: Duration,
        /// Duration of fault
        duration: Duration,
    },

    /// Process crash
    ProcessCrash {
        /// Target node
        node: NodeId,
        /// Process to crash
        process: ProcessType,
        /// Whether to auto-restart
        auto_restart: bool,
        /// Restart delay
        restart_delay: Duration,
    },

    /// CPU stress
    CpuStress {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// CPU usage percentage (0-100)
        usage_percent: u8,
        /// Number of cores to stress
        cores: Option<u8>,
        /// Duration of fault
        duration: Duration,
    },

    /// Memory pressure
    MemoryPressure {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Memory to consume in MB
        consume_mb: usize,
        /// Duration of fault
        duration: Duration,
    },

    /// Clock skew
    ClockSkew {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Time drift
        drift: Duration,
        /// Direction (forward or backward)
        direction: ClockDirection,
        /// Duration of fault
        duration: Duration,
    },

    /// Data corruption (bit rot simulation)
    BitRot {
        /// Target node
        node: NodeId,
        /// Number of chunks to corrupt
        chunk_count: usize,
        /// Corruption mode
        mode: CorruptionMode,
    },

    /// DNS failure
    DnsFailure {
        /// Target nodes
        nodes: Vec<NodeId>,
        /// Domains to fail
        domains: Vec<String>,
        /// Duration of fault
        duration: Duration,
    },

    /// Full node failure
    NodeFailure {
        /// Target node
        node: NodeId,
        /// Duration of fault
        duration: Duration,
    },

    /// Cascading failure simulation
    CascadingFailure {
        /// Initial failure
        initial: Box<Fault>,
        /// Subsequent failures triggered by initial
        cascade: Vec<(Duration, Box<Fault>)>,
    },

    /// Custom fault via script
    CustomScript {
        /// Script path
        script: String,
        /// Arguments
        args: Vec<String>,
        /// Rollback script
        rollback_script: Option<String>,
        /// Duration
        duration: Duration,
    },
}

impl Fault {
    /// Get the duration of this fault
    pub fn duration(&self) -> Duration {
        match self {
            Fault::NetworkPartition { duration, .. } => *duration,
            Fault::NetworkLatency { duration, .. } => *duration,
            Fault::PacketLoss { duration, .. } => *duration,
            Fault::BandwidthLimit { duration, .. } => *duration,
            Fault::DiskFailure { duration, .. } => *duration,
            Fault::DiskSlowdown { duration, .. } => *duration,
            Fault::ProcessCrash { restart_delay, .. } => *restart_delay,
            Fault::CpuStress { duration, .. } => *duration,
            Fault::MemoryPressure { duration, .. } => *duration,
            Fault::ClockSkew { duration, .. } => *duration,
            Fault::BitRot { .. } => Duration::from_secs(0), // Instant
            Fault::DnsFailure { duration, .. } => *duration,
            Fault::NodeFailure { duration, .. } => *duration,
            Fault::CascadingFailure { initial, cascade } => {
                let mut total = initial.duration();
                for (delay, fault) in cascade {
                    total = total.max(*delay + fault.duration());
                }
                total
            }
            Fault::CustomScript { duration, .. } => *duration,
        }
    }

    /// Get affected nodes
    pub fn affected_nodes(&self) -> Vec<NodeId> {
        match self {
            Fault::NetworkPartition { nodes, .. } => nodes.clone(),
            Fault::NetworkLatency { nodes, .. } => nodes.clone(),
            Fault::PacketLoss { nodes, .. } => nodes.clone(),
            Fault::BandwidthLimit { nodes, .. } => nodes.clone(),
            Fault::DiskFailure { node, .. } => vec![*node],
            Fault::DiskSlowdown { node, .. } => vec![*node],
            Fault::ProcessCrash { node, .. } => vec![*node],
            Fault::CpuStress { nodes, .. } => nodes.clone(),
            Fault::MemoryPressure { nodes, .. } => nodes.clone(),
            Fault::ClockSkew { nodes, .. } => nodes.clone(),
            Fault::BitRot { node, .. } => vec![*node],
            Fault::DnsFailure { nodes, .. } => nodes.clone(),
            Fault::NodeFailure { node, .. } => vec![*node],
            Fault::CascadingFailure { initial, cascade } => {
                let mut nodes = initial.affected_nodes();
                for (_, fault) in cascade {
                    nodes.extend(fault.affected_nodes());
                }
                nodes.sort();
                nodes.dedup();
                nodes
            }
            Fault::CustomScript { .. } => Vec::new(),
        }
    }

    /// Check if this fault is destructive
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            Fault::BitRot { .. }
                | Fault::NodeFailure { .. }
                | Fault::ProcessCrash { auto_restart: false, .. }
        )
    }
}

/// Disk failure modes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiskFailureMode {
    /// Complete failure (no reads or writes)
    Complete,
    /// Read-only (writes fail)
    ReadOnly,
    /// Write errors only
    WriteErrors,
    /// Random I/O errors
    RandomErrors { error_rate_percent: u8 },
}

/// Process types that can be crashed
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProcessType {
    MetadataServer,
    DataServer,
    RaftNode,
    S3Gateway,
    FuseClient,
    AllServices,
}

/// Clock drift direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClockDirection {
    Forward,
    Backward,
}

/// Data corruption modes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CorruptionMode {
    /// Flip random bits
    BitFlip { bits_per_chunk: usize },
    /// Zero out data
    ZeroFill,
    /// Random data
    RandomFill,
    /// Truncate data
    Truncate { bytes: usize },
}

// ============================================================================
// Blast Radius
// ============================================================================

/// Blast radius controls the scope of chaos experiments
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BlastRadius {
    /// Affect a single node
    SingleNode,
    /// Affect a fixed number of nodes
    FixedCount(usize),
    /// Affect a percentage of nodes
    Percentage(u8),
    /// Affect an entire availability zone
    AvailabilityZone,
    /// Affect an entire region
    Region,
    /// Custom blast radius
    Custom,
}

impl BlastRadius {
    /// Calculate the maximum number of nodes affected
    pub fn max_nodes(&self, total_nodes: usize) -> usize {
        match self {
            BlastRadius::SingleNode => 1,
            BlastRadius::FixedCount(n) => *n,
            BlastRadius::Percentage(p) => (total_nodes * *p as usize) / 100,
            BlastRadius::AvailabilityZone => total_nodes / 3, // Assume 3 AZs
            BlastRadius::Region => total_nodes,
            BlastRadius::Custom => total_nodes,
        }
    }
}

// ============================================================================
// Steady State
// ============================================================================

/// Steady state definition for experiments
#[derive(Debug, Clone)]
pub struct SteadyState {
    /// Name of the steady state check
    pub name: String,
    /// Description
    pub description: String,
    /// Check type
    pub check: SteadyStateCheck,
    /// Tolerance for the check
    pub tolerance: SteadyStateTolerance,
}

/// Types of steady state checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SteadyStateCheck {
    /// Check cluster health
    ClusterHealth,
    /// Check that all nodes are reachable
    AllNodesReachable,
    /// Check read latency is below threshold
    ReadLatency { max_p99_ms: u64 },
    /// Check write latency is below threshold
    WriteLatency { max_p99_ms: u64 },
    /// Check throughput is above threshold
    Throughput { min_ops_per_second: u64 },
    /// Check error rate is below threshold
    ErrorRate { max_percent: f64 },
    /// Check data availability
    DataAvailability { min_percent: f64 },
    /// Check Raft leader exists
    RaftLeaderExists,
    /// Check quorum is maintained
    QuorumMaintained,
    /// Custom HTTP check
    HttpCheck { url: String, expected_status: u16 },
    /// Custom metric check
    MetricCheck {
        metric: String,
        operator: MetricOperator,
        value: f64,
    },
}

/// Metric comparison operators
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetricOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
}

/// Tolerance for steady state checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SteadyStateTolerance {
    /// Must pass exactly
    Exact,
    /// Allow some deviation
    Deviation { percent: f64 },
    /// Allow retries before failing
    Retries { count: u32, delay: Duration },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fault_duration() {
        let fault = Fault::NetworkPartition {
            nodes: vec![1, 2],
            duration: Duration::from_secs(30),
            bidirectional: true,
        };
        assert_eq!(fault.duration(), Duration::from_secs(30));
    }

    #[test]
    fn test_fault_affected_nodes() {
        let fault = Fault::NetworkLatency {
            nodes: vec![1, 2, 3],
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(60),
        };
        assert_eq!(fault.affected_nodes(), vec![1, 2, 3]);
    }

    #[test]
    fn test_fault_is_destructive() {
        let destructive = Fault::BitRot {
            node: 1,
            chunk_count: 10,
            mode: CorruptionMode::BitFlip { bits_per_chunk: 1 },
        };
        assert!(destructive.is_destructive());

        let non_destructive = Fault::NetworkLatency {
            nodes: vec![1],
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(60),
        };
        assert!(!non_destructive.is_destructive());
    }

    #[test]
    fn test_blast_radius_max_nodes() {
        assert_eq!(BlastRadius::SingleNode.max_nodes(10), 1);
        assert_eq!(BlastRadius::FixedCount(3).max_nodes(10), 3);
        assert_eq!(BlastRadius::Percentage(50).max_nodes(10), 5);
    }
}
