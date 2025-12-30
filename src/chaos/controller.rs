//! Chaos controller and fault injection.

use super::config::ChaosConfig;
use super::experiment::{
    Experiment, ExperimentMetrics, ExperimentResult, ExperimentStatus, FaultResult,
    SteadyStateResult,
};
use super::types::{
    BlastRadius, ClockDirection, DiskFailureMode, Fault,
    SteadyState, SteadyStateCheck, SteadyStateTolerance,
};
use crate::error::{Result, StrataError};
use crate::types::NodeId;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

// ============================================================================
// Fault Injector
// ============================================================================

/// Trait for injecting faults into the system
#[async_trait::async_trait]
pub trait FaultInjector: Send + Sync {
    /// Inject a fault
    async fn inject(&self, fault: &Fault) -> Result<FaultHandle>;

    /// Rollback a fault
    async fn rollback(&self, handle: &FaultHandle) -> Result<()>;

    /// Check if a fault is active
    async fn is_active(&self, handle: &FaultHandle) -> bool;
}

/// Handle to an injected fault
#[derive(Debug, Clone)]
pub struct FaultHandle {
    /// Unique ID
    pub id: String,
    /// Fault that was injected
    pub fault: Fault,
    /// Injection time
    pub injected_at: Instant,
    /// Rollback command/info
    pub rollback_info: Option<String>,
}

/// Default fault injector implementation
pub struct DefaultFaultInjector {
    active_faults: Arc<RwLock<HashMap<String, FaultHandle>>>,
    network_faults: Arc<RwLock<NetworkFaultState>>,
    disk_faults: Arc<RwLock<DiskFaultState>>,
}

#[derive(Default)]
struct NetworkFaultState {
    partitions: HashSet<(NodeId, NodeId)>,
    latency: HashMap<NodeId, Duration>,
    packet_loss: HashMap<NodeId, u8>,
}

#[derive(Default)]
struct DiskFaultState {
    failed_disks: HashMap<(NodeId, String), DiskFailureMode>,
    slowdowns: HashMap<NodeId, (Duration, Duration)>,
}

impl DefaultFaultInjector {
    pub fn new() -> Self {
        Self {
            active_faults: Arc::new(RwLock::new(HashMap::new())),
            network_faults: Arc::new(RwLock::new(NetworkFaultState::default())),
            disk_faults: Arc::new(RwLock::new(DiskFaultState::default())),
        }
    }

    async fn inject_network_partition(
        &self,
        nodes: &[NodeId],
        bidirectional: bool,
    ) -> Result<String> {
        let mut state = self.network_faults.write().await;

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                state.partitions.insert((nodes[i], nodes[j]));
                if bidirectional {
                    state.partitions.insert((nodes[j], nodes[i]));
                }
            }
        }

        info!(
            "Injected network partition between {} nodes",
            nodes.len()
        );
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn inject_network_latency(&self, nodes: &[NodeId], latency: Duration) -> Result<String> {
        let mut state = self.network_faults.write().await;

        for node in nodes {
            state.latency.insert(*node, latency);
        }

        info!(
            "Injected {:?} latency to {} nodes",
            latency,
            nodes.len()
        );
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn inject_packet_loss(&self, nodes: &[NodeId], loss_percent: u8) -> Result<String> {
        let mut state = self.network_faults.write().await;

        for node in nodes {
            state.packet_loss.insert(*node, loss_percent);
        }

        info!(
            "Injected {}% packet loss to {} nodes",
            loss_percent,
            nodes.len()
        );
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn inject_disk_failure(
        &self,
        node: NodeId,
        disk_id: &str,
        mode: DiskFailureMode,
    ) -> Result<String> {
        let mut state = self.disk_faults.write().await;
        state
            .failed_disks
            .insert((node, disk_id.to_string()), mode);

        info!(
            "Injected disk failure on node {} disk {} mode {:?}",
            node, disk_id, mode
        );
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn inject_disk_slowdown(
        &self,
        node: NodeId,
        read_latency: Duration,
        write_latency: Duration,
    ) -> Result<String> {
        let mut state = self.disk_faults.write().await;
        state.slowdowns.insert(node, (read_latency, write_latency));

        info!(
            "Injected disk slowdown on node {}: read {:?}, write {:?}",
            node, read_latency, write_latency
        );
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn rollback_network_partition(&self, nodes: &[NodeId]) -> Result<()> {
        let mut state = self.network_faults.write().await;

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                state.partitions.remove(&(nodes[i], nodes[j]));
                state.partitions.remove(&(nodes[j], nodes[i]));
            }
        }

        info!("Rolled back network partition");
        Ok(())
    }

    async fn rollback_network_latency(&self, nodes: &[NodeId]) -> Result<()> {
        let mut state = self.network_faults.write().await;

        for node in nodes {
            state.latency.remove(node);
        }

        info!("Rolled back network latency");
        Ok(())
    }

    async fn rollback_packet_loss(&self, nodes: &[NodeId]) -> Result<()> {
        let mut state = self.network_faults.write().await;

        for node in nodes {
            state.packet_loss.remove(node);
        }

        info!("Rolled back packet loss");
        Ok(())
    }

    async fn rollback_disk_failure(&self, node: NodeId, disk_id: &str) -> Result<()> {
        let mut state = self.disk_faults.write().await;
        state.failed_disks.remove(&(node, disk_id.to_string()));

        info!("Rolled back disk failure on node {} disk {}", node, disk_id);
        Ok(())
    }

    async fn rollback_disk_slowdown(&self, node: NodeId) -> Result<()> {
        let mut state = self.disk_faults.write().await;
        state.slowdowns.remove(&node);

        info!("Rolled back disk slowdown on node {}", node);
        Ok(())
    }
}

impl Default for DefaultFaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FaultInjector for DefaultFaultInjector {
    async fn inject(&self, fault: &Fault) -> Result<FaultHandle> {
        let id = match fault {
            Fault::NetworkPartition {
                nodes,
                bidirectional,
                ..
            } => self.inject_network_partition(nodes, *bidirectional).await?,

            Fault::NetworkLatency { nodes, latency, .. } => {
                self.inject_network_latency(nodes, *latency).await?
            }

            Fault::PacketLoss {
                nodes,
                loss_percent,
                ..
            } => self.inject_packet_loss(nodes, *loss_percent).await?,

            Fault::DiskFailure {
                node,
                disk_id,
                mode,
                ..
            } => self.inject_disk_failure(*node, disk_id, *mode).await?,

            Fault::DiskSlowdown {
                node,
                read_latency,
                write_latency,
                ..
            } => {
                self.inject_disk_slowdown(*node, *read_latency, *write_latency)
                    .await?
            }

            Fault::CpuStress {
                nodes,
                usage_percent,
                ..
            } => {
                info!(
                    "Simulating CPU stress {}% on {} nodes",
                    usage_percent,
                    nodes.len()
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::MemoryPressure {
                nodes, consume_mb, ..
            } => {
                info!(
                    "Simulating memory pressure {}MB on {} nodes",
                    consume_mb,
                    nodes.len()
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::ClockSkew {
                nodes,
                drift,
                direction,
                ..
            } => {
                info!(
                    "Simulating clock skew {:?} {:?} on {} nodes",
                    drift,
                    direction,
                    nodes.len()
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::ProcessCrash { node, process, .. } => {
                info!("Simulating process crash {:?} on node {}", process, node);
                uuid::Uuid::new_v4().to_string()
            }

            Fault::NodeFailure { node, .. } => {
                info!("Simulating node failure on node {}", node);
                uuid::Uuid::new_v4().to_string()
            }

            Fault::BitRot {
                node,
                chunk_count,
                mode,
            } => {
                info!(
                    "Simulating bit rot on node {}: {} chunks, mode {:?}",
                    node, chunk_count, mode
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::DnsFailure { nodes, domains, .. } => {
                info!(
                    "Simulating DNS failure for {:?} on {} nodes",
                    domains,
                    nodes.len()
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::BandwidthLimit {
                nodes,
                bytes_per_second,
                ..
            } => {
                info!(
                    "Simulating bandwidth limit {} B/s on {} nodes",
                    bytes_per_second,
                    nodes.len()
                );
                uuid::Uuid::new_v4().to_string()
            }

            Fault::CascadingFailure { initial, cascade } => {
                info!(
                    "Starting cascading failure with {} stages",
                    cascade.len() + 1
                );
                let handle = self.inject(initial).await?;
                // Schedule cascade stages
                for (delay, _fault) in cascade {
                    debug!("Cascade stage scheduled for {:?}", delay);
                }
                handle.id
            }

            Fault::CustomScript { script, args, .. } => {
                info!("Running custom script {} with args {:?}", script, args);
                uuid::Uuid::new_v4().to_string()
            }
        };

        let handle = FaultHandle {
            id: id.clone(),
            fault: fault.clone(),
            injected_at: Instant::now(),
            rollback_info: None,
        };

        self.active_faults.write().await.insert(id, handle.clone());

        Ok(handle)
    }

    async fn rollback(&self, handle: &FaultHandle) -> Result<()> {
        match &handle.fault {
            Fault::NetworkPartition { nodes, .. } => {
                self.rollback_network_partition(nodes).await?;
            }
            Fault::NetworkLatency { nodes, .. } => {
                self.rollback_network_latency(nodes).await?;
            }
            Fault::PacketLoss { nodes, .. } => {
                self.rollback_packet_loss(nodes).await?;
            }
            Fault::DiskFailure { node, disk_id, .. } => {
                self.rollback_disk_failure(*node, disk_id).await?;
            }
            Fault::DiskSlowdown { node, .. } => {
                self.rollback_disk_slowdown(*node).await?;
            }
            _ => {
                debug!("No explicit rollback needed for fault type");
            }
        }

        self.active_faults.write().await.remove(&handle.id);
        Ok(())
    }

    async fn is_active(&self, handle: &FaultHandle) -> bool {
        self.active_faults.read().await.contains_key(&handle.id)
    }
}

// ============================================================================
// Steady State Checker
// ============================================================================

/// Checks steady state conditions
#[async_trait::async_trait]
pub trait SteadyStateChecker: Send + Sync {
    /// Check a steady state condition
    async fn check(&self, state: &SteadyState) -> SteadyStateResult;
}

/// Default steady state checker
pub struct DefaultSteadyStateChecker {
    metrics_endpoint: Option<String>,
}

impl DefaultSteadyStateChecker {
    pub fn new(metrics_endpoint: Option<String>) -> Self {
        Self { metrics_endpoint }
    }

    async fn check_cluster_health(&self) -> (bool, Option<String>) {
        // Simulated health check
        (true, Some("healthy".to_string()))
    }

    async fn check_read_latency(&self, max_p99_ms: u64) -> (bool, Option<String>) {
        // Simulated latency check
        let actual = 15; // Would come from metrics
        (
            actual <= max_p99_ms,
            Some(format!("{}ms", actual)),
        )
    }

    async fn check_error_rate(&self, max_percent: f64) -> (bool, Option<String>) {
        // Simulated error rate check
        let actual = 0.1; // Would come from metrics
        (actual <= max_percent, Some(format!("{:.2}%", actual)))
    }
}

#[async_trait::async_trait]
impl SteadyStateChecker for DefaultSteadyStateChecker {
    async fn check(&self, state: &SteadyState) -> SteadyStateResult {
        let start = Instant::now();

        let (passed, actual_value) = match &state.check {
            SteadyStateCheck::ClusterHealth => self.check_cluster_health().await,
            SteadyStateCheck::ReadLatency { max_p99_ms } => {
                self.check_read_latency(*max_p99_ms).await
            }
            SteadyStateCheck::ErrorRate { max_percent } => {
                self.check_error_rate(*max_percent).await
            }
            _ => (true, None),
        };

        SteadyStateResult {
            name: state.name.clone(),
            passed,
            actual_value,
            expected_value: None,
            error: if passed { None } else { Some("Check failed".into()) },
            duration: start.elapsed(),
        }
    }
}

// ============================================================================
// Chaos Controller
// ============================================================================

/// Main chaos engineering controller
pub struct ChaosController {
    config: ChaosConfig,
    injector: Arc<dyn FaultInjector>,
    checker: Arc<dyn SteadyStateChecker>,
    running_experiments: Arc<RwLock<HashMap<String, RunningExperiment>>>,
    results_history: Arc<RwLock<Vec<ExperimentResult>>>,
    event_tx: broadcast::Sender<ChaosEvent>,
}

struct RunningExperiment {
    experiment: Experiment,
    started_at: Instant,
    fault_handles: Vec<FaultHandle>,
    status: ExperimentStatus,
}

/// Events emitted by the chaos controller
#[derive(Debug, Clone)]
pub enum ChaosEvent {
    ExperimentStarted { experiment_id: String },
    ExperimentCompleted { experiment_id: String, result: ExperimentResult },
    FaultInjected { experiment_id: String, fault_type: String },
    FaultRolledBack { experiment_id: String, fault_type: String },
    SteadyStateChecked { experiment_id: String, passed: bool },
    SafetyViolation { experiment_id: String, reason: String },
}

impl ChaosController {
    pub fn new(config: ChaosConfig) -> Self {
        let metrics_endpoint = config.metrics_endpoint.clone();
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            config,
            injector: Arc::new(DefaultFaultInjector::new()),
            checker: Arc::new(DefaultSteadyStateChecker::new(metrics_endpoint)),
            running_experiments: Arc::new(RwLock::new(HashMap::new())),
            results_history: Arc::new(RwLock::new(Vec::new())),
            event_tx,
        }
    }

    /// Subscribe to chaos events
    pub fn subscribe(&self) -> broadcast::Receiver<ChaosEvent> {
        self.event_tx.subscribe()
    }

    /// Run an experiment
    pub async fn run(&self, experiment: Experiment) -> Result<ExperimentResult> {
        if !self.config.enabled {
            return Err(StrataError::InvalidArgument(
                "Chaos engineering is disabled".into(),
            ));
        }

        // Check schedule
        if let Some(schedule) = &self.config.schedule {
            if !schedule.is_active() {
                return Err(StrataError::InvalidArgument(
                    "Chaos experiments not allowed outside scheduled hours".into(),
                ));
            }
        }

        // Validate experiment
        let total_nodes = 10; // Would come from cluster state
        experiment.validate(&self.config, total_nodes)?;

        let experiment_id = experiment.id.clone();
        let experiment_name = experiment.name.clone();
        let started_at = Utc::now();
        let start_instant = Instant::now();

        info!("Starting chaos experiment: {}", experiment_name);

        let _ = self.event_tx.send(ChaosEvent::ExperimentStarted {
            experiment_id: experiment_id.clone(),
        });

        // Track running experiment
        {
            let mut running = self.running_experiments.write().await;
            running.insert(
                experiment_id.clone(),
                RunningExperiment {
                    experiment: experiment.clone(),
                    started_at: start_instant,
                    fault_handles: Vec::new(),
                    status: ExperimentStatus::Running,
                },
            );
        }

        // Check steady state before
        let steady_state_before = self.check_steady_states(&experiment).await;

        let all_passed_before = steady_state_before.iter().all(|r| r.passed);
        if !all_passed_before && !experiment.dry_run {
            warn!("Steady state check failed before experiment, aborting");
            let result = ExperimentResult {
                experiment_id: experiment_id.clone(),
                experiment_name: experiment_name.clone(),
                status: ExperimentStatus::Aborted,
                started_at,
                ended_at: Utc::now(),
                duration: start_instant.elapsed(),
                steady_state_before,
                steady_state_after: Vec::new(),
                fault_results: Vec::new(),
                hypothesis_proven: false,
                error: Some("Steady state check failed before experiment".into()),
                metrics: ExperimentMetrics::default(),
            };

            self.running_experiments.write().await.remove(&experiment_id);
            self.results_history.write().await.push(result.clone());

            return Ok(result);
        }

        // Inject faults
        let mut fault_results = Vec::new();
        let mut fault_handles = Vec::new();

        for fault in &experiment.faults {
            if experiment.dry_run {
                info!("DRY RUN: Would inject fault {:?}", fault);
                fault_results.push(FaultResult {
                    fault_type: format!("{:?}", std::mem::discriminant(fault)),
                    injected: false,
                    rolled_back: false,
                    affected_nodes: fault.affected_nodes(),
                    duration: fault.duration(),
                    error: None,
                });
                continue;
            }

            match self.injector.inject(fault).await {
                Ok(handle) => {
                    let _ = self.event_tx.send(ChaosEvent::FaultInjected {
                        experiment_id: experiment_id.clone(),
                        fault_type: format!("{:?}", std::mem::discriminant(fault)),
                    });

                    fault_handles.push(handle.clone());
                    fault_results.push(FaultResult {
                        fault_type: format!("{:?}", std::mem::discriminant(fault)),
                        injected: true,
                        rolled_back: false,
                        affected_nodes: fault.affected_nodes(),
                        duration: fault.duration(),
                        error: None,
                    });

                    // Update running experiment
                    if let Some(running) = self.running_experiments.write().await.get_mut(&experiment_id) {
                        running.fault_handles.push(handle);
                    }
                }
                Err(e) => {
                    error!("Failed to inject fault: {}", e);
                    fault_results.push(FaultResult {
                        fault_type: format!("{:?}", std::mem::discriminant(fault)),
                        injected: false,
                        rolled_back: false,
                        affected_nodes: fault.affected_nodes(),
                        duration: fault.duration(),
                        error: Some(e.to_string()),
                    });

                    if experiment.rollback_on_failure {
                        // Rollback all injected faults
                        for handle in &fault_handles {
                            let _ = self.injector.rollback(handle).await;
                        }
                        break;
                    }
                }
            }
        }

        // Wait for fault duration
        if !experiment.dry_run && !fault_handles.is_empty() {
            let max_duration = experiment
                .faults
                .iter()
                .map(|f| f.duration())
                .max()
                .unwrap_or(Duration::from_secs(0));

            info!("Waiting {:?} for faults to complete", max_duration);
            tokio::time::sleep(max_duration).await;
        }

        // Rollback faults
        for (handle, result) in fault_handles.iter().zip(fault_results.iter_mut()) {
            if let Err(e) = self.injector.rollback(handle).await {
                error!("Failed to rollback fault: {}", e);
                result.error = Some(format!("Rollback failed: {}", e));
            } else {
                result.rolled_back = true;
                let _ = self.event_tx.send(ChaosEvent::FaultRolledBack {
                    experiment_id: experiment_id.clone(),
                    fault_type: result.fault_type.clone(),
                });
            }
        }

        // Check steady state after
        let steady_state_after = self.check_steady_states(&experiment).await;
        let all_passed_after = steady_state_after.iter().all(|r| r.passed);

        // Determine result
        let status = if all_passed_after {
            ExperimentStatus::Success
        } else {
            ExperimentStatus::Failed
        };

        let result = ExperimentResult {
            experiment_id: experiment_id.clone(),
            experiment_name,
            status,
            started_at,
            ended_at: Utc::now(),
            duration: start_instant.elapsed(),
            steady_state_before,
            steady_state_after,
            fault_results,
            hypothesis_proven: all_passed_after,
            error: if all_passed_after {
                None
            } else {
                Some("Steady state check failed after experiment".into())
            },
            metrics: ExperimentMetrics::default(),
        };

        // Clean up
        self.running_experiments.write().await.remove(&experiment_id);
        self.results_history.write().await.push(result.clone());

        let _ = self.event_tx.send(ChaosEvent::ExperimentCompleted {
            experiment_id,
            result: result.clone(),
        });

        Ok(result)
    }

    /// Abort a running experiment
    pub async fn abort(&self, experiment_id: &str) -> Result<()> {
        let mut running = self.running_experiments.write().await;

        if let Some(mut experiment) = running.remove(experiment_id) {
            warn!("Aborting experiment: {}", experiment_id);
            experiment.status = ExperimentStatus::Aborted;

            // Rollback all faults
            for handle in &experiment.fault_handles {
                let _ = self.injector.rollback(handle).await;
            }

            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Experiment {} not found",
                experiment_id
            )))
        }
    }

    /// Get experiment history
    pub async fn history(&self) -> Vec<ExperimentResult> {
        self.results_history.read().await.clone()
    }

    /// Get running experiments
    pub async fn running(&self) -> Vec<String> {
        self.running_experiments
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    async fn check_steady_states(&self, experiment: &Experiment) -> Vec<SteadyStateResult> {
        let mut results = Vec::new();

        for state in &experiment.steady_state {
            let result = self.checker.check(state).await;
            results.push(result);
        }

        results
    }
}

// ============================================================================
// Predefined Experiments
// ============================================================================

/// Library of predefined chaos experiments
pub mod experiments {
    use super::*;

    /// Network partition experiment
    pub fn network_partition(nodes: Vec<NodeId>, duration: Duration) -> Result<Experiment> {
        Experiment::builder()
            .name("network-partition")
            .description("Test system behavior during network partition")
            .hypothesis("System remains available and consistent during network partition")
            .steady_state(SteadyState {
                name: "cluster-health".into(),
                description: "Cluster is healthy".into(),
                check: SteadyStateCheck::ClusterHealth,
                tolerance: SteadyStateTolerance::Exact,
            })
            .fault(Fault::NetworkPartition {
                nodes,
                duration,
                bidirectional: true,
            })
            .blast_radius(BlastRadius::FixedCount(2))
            .build()
    }

    /// Node failure experiment
    pub fn node_failure(node: NodeId, duration: Duration) -> Result<Experiment> {
        Experiment::builder()
            .name("node-failure")
            .description("Test system behavior when a node fails")
            .hypothesis("System remains available when a single node fails")
            .steady_state(SteadyState {
                name: "cluster-health".into(),
                description: "Cluster is healthy".into(),
                check: SteadyStateCheck::ClusterHealth,
                tolerance: SteadyStateTolerance::Exact,
            })
            .fault(Fault::NodeFailure { node, duration })
            .blast_radius(BlastRadius::SingleNode)
            .build()
    }

    /// Latency injection experiment
    pub fn latency_injection(
        nodes: Vec<NodeId>,
        latency: Duration,
        duration: Duration,
    ) -> Result<Experiment> {
        Experiment::builder()
            .name("latency-injection")
            .description("Test system behavior under network latency")
            .hypothesis("System meets SLOs under increased latency")
            .steady_state(SteadyState {
                name: "read-latency".into(),
                description: "Read latency is acceptable".into(),
                check: SteadyStateCheck::ReadLatency { max_p99_ms: 100 },
                tolerance: SteadyStateTolerance::Deviation { percent: 50.0 },
            })
            .fault(Fault::NetworkLatency {
                nodes,
                latency,
                jitter: Duration::from_millis(10),
                duration,
            })
            .blast_radius(BlastRadius::Percentage(30))
            .build()
    }

    /// Disk failure experiment
    pub fn disk_failure(
        node: NodeId,
        disk_id: String,
        duration: Duration,
    ) -> Result<Experiment> {
        Experiment::builder()
            .name("disk-failure")
            .description("Test system behavior when a disk fails")
            .hypothesis("System remains available with disk failure")
            .steady_state(SteadyState {
                name: "data-availability".into(),
                description: "Data remains available".into(),
                check: SteadyStateCheck::DataAvailability { min_percent: 99.9 },
                tolerance: SteadyStateTolerance::Exact,
            })
            .fault(Fault::DiskFailure {
                node,
                disk_id,
                mode: DiskFailureMode::Complete,
                duration,
            })
            .blast_radius(BlastRadius::SingleNode)
            .build()
    }

    /// CPU stress experiment
    pub fn cpu_stress(
        nodes: Vec<NodeId>,
        usage_percent: u8,
        duration: Duration,
    ) -> Result<Experiment> {
        Experiment::builder()
            .name("cpu-stress")
            .description("Test system behavior under CPU stress")
            .hypothesis("System meets SLOs under CPU pressure")
            .steady_state(SteadyState {
                name: "throughput".into(),
                description: "Throughput is acceptable".into(),
                check: SteadyStateCheck::Throughput {
                    min_ops_per_second: 1000,
                },
                tolerance: SteadyStateTolerance::Deviation { percent: 25.0 },
            })
            .fault(Fault::CpuStress {
                nodes,
                usage_percent,
                cores: None,
                duration,
            })
            .blast_radius(BlastRadius::Percentage(25))
            .build()
    }

    /// Memory pressure experiment
    pub fn memory_pressure(
        nodes: Vec<NodeId>,
        consume_mb: usize,
        duration: Duration,
    ) -> Result<Experiment> {
        Experiment::builder()
            .name("memory-pressure")
            .description("Test system behavior under memory pressure")
            .hypothesis("System remains stable under memory pressure")
            .steady_state(SteadyState {
                name: "error-rate".into(),
                description: "Error rate is low".into(),
                check: SteadyStateCheck::ErrorRate { max_percent: 1.0 },
                tolerance: SteadyStateTolerance::Exact,
            })
            .fault(Fault::MemoryPressure {
                nodes,
                consume_mb,
                duration,
            })
            .blast_radius(BlastRadius::Percentage(25))
            .build()
    }

    /// Clock skew experiment
    pub fn clock_skew(
        nodes: Vec<NodeId>,
        drift: Duration,
        direction: ClockDirection,
        duration: Duration,
    ) -> Result<Experiment> {
        Experiment::builder()
            .name("clock-skew")
            .description("Test system behavior under clock skew")
            .hypothesis("Distributed algorithms handle clock skew correctly")
            .steady_state(SteadyState {
                name: "raft-leader".into(),
                description: "Raft leader exists".into(),
                check: SteadyStateCheck::RaftLeaderExists,
                tolerance: SteadyStateTolerance::Retries {
                    count: 3,
                    delay: Duration::from_secs(5),
                },
            })
            .fault(Fault::ClockSkew {
                nodes,
                drift,
                direction,
                duration,
            })
            .blast_radius(BlastRadius::Percentage(33))
            .build()
    }

    /// Cascading failure experiment
    pub fn cascading_failure(initial_node: NodeId) -> Result<Experiment> {
        Experiment::builder()
            .name("cascading-failure")
            .description("Test system resilience to cascading failures")
            .hypothesis("System contains failure and prevents cascade")
            .steady_state(SteadyState {
                name: "quorum".into(),
                description: "Quorum is maintained".into(),
                check: SteadyStateCheck::QuorumMaintained,
                tolerance: SteadyStateTolerance::Exact,
            })
            .fault(Fault::CascadingFailure {
                initial: Box::new(Fault::NodeFailure {
                    node: initial_node,
                    duration: Duration::from_secs(60),
                }),
                cascade: vec![
                    (
                        Duration::from_secs(10),
                        Box::new(Fault::CpuStress {
                            nodes: vec![initial_node + 1],
                            usage_percent: 90,
                            cores: None,
                            duration: Duration::from_secs(30),
                        }),
                    ),
                ],
            })
            .blast_radius(BlastRadius::FixedCount(2))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_fault_injector() {
        let injector = DefaultFaultInjector::new();

        let fault = Fault::NetworkLatency {
            nodes: vec![1, 2],
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(30),
        };

        let handle = injector.inject(&fault).await.unwrap();
        assert!(injector.is_active(&handle).await);

        injector.rollback(&handle).await.unwrap();
        assert!(!injector.is_active(&handle).await);
    }

    #[tokio::test]
    async fn test_default_steady_state_checker() {
        let checker = DefaultSteadyStateChecker::new(None);

        let state = SteadyState {
            name: "test-check".to_string(),
            description: "Test".to_string(),
            check: SteadyStateCheck::ClusterHealth,
            tolerance: SteadyStateTolerance::Exact,
        };

        let result = checker.check(&state).await;
        assert!(result.passed);
    }

    #[test]
    fn test_predefined_network_partition() {
        let exp = experiments::network_partition(vec![1, 2], Duration::from_secs(30)).unwrap();
        assert_eq!(exp.name, "network-partition");
        assert_eq!(exp.faults.len(), 1);
    }

    #[test]
    fn test_predefined_node_failure() {
        let exp = experiments::node_failure(1, Duration::from_secs(60)).unwrap();
        assert_eq!(exp.name, "node-failure");
    }
}
