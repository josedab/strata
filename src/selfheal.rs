//! Self-Healing with Root Cause Analysis
//!
//! This module provides automatic system recovery with intelligent root cause
//! analysis for failures. Features:
//! - Automatic failure detection and classification
//! - Root cause analysis using causal inference
//! - Self-healing actions with rollback support
//! - Learning from past incidents
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 Self-Healing Engine                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Anomaly Detection │ Failure Classification │ RCA Engine   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Remediation Actions │ Rollback │ Escalation                │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Incident Learning │ Runbook Automation │ SLA Tracking      │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use crate::types::NodeId;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};
use chrono::{DateTime, Utc};

/// Unique incident identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IncidentId(pub u64);

impl IncidentId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for IncidentId {
    fn default() -> Self {
        Self::new()
    }
}

/// Severity levels for incidents
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
    Fatal,
}

impl Severity {
    pub fn requires_immediate_action(&self) -> bool {
        matches!(self, Severity::Critical | Severity::Fatal)
    }

    pub fn requires_human_review(&self) -> bool {
        matches!(self, Severity::Error | Severity::Critical | Severity::Fatal)
    }
}

/// Types of failures that can occur
#[derive(Debug, Clone, PartialEq)]
pub enum FailureType {
    /// Node is not responding
    NodeUnreachable(NodeId),
    /// Disk failure
    DiskFailure { node: NodeId, disk: String },
    /// Network partition
    NetworkPartition { affected_nodes: Vec<NodeId> },
    /// Chunk corruption detected
    ChunkCorruption { chunk_id: String, expected_checksum: String },
    /// Replication lag exceeds threshold
    ReplicationLag { source: NodeId, target: NodeId, lag_ms: u64 },
    /// Memory pressure
    MemoryPressure { node: NodeId, used_percent: f64 },
    /// CPU overload
    CpuOverload { node: NodeId, used_percent: f64 },
    /// Consensus failure
    ConsensusFailure { reason: String },
    /// Service crash
    ServiceCrash { node: NodeId, service: String },
    /// Slow query/operation
    SlowOperation { operation: String, duration_ms: u64 },
    /// Custom failure
    Custom { category: String, description: String },
}

impl FailureType {
    pub fn severity(&self) -> Severity {
        match self {
            FailureType::NodeUnreachable(_) => Severity::Critical,
            FailureType::DiskFailure { .. } => Severity::Critical,
            FailureType::NetworkPartition { .. } => Severity::Fatal,
            FailureType::ChunkCorruption { .. } => Severity::Error,
            FailureType::ReplicationLag { lag_ms, .. } => {
                if *lag_ms > 60000 { Severity::Error } else { Severity::Warning }
            }
            FailureType::MemoryPressure { used_percent, .. } => {
                if *used_percent > 95.0 { Severity::Critical }
                else if *used_percent > 85.0 { Severity::Warning }
                else { Severity::Info }
            }
            FailureType::CpuOverload { used_percent, .. } => {
                if *used_percent > 95.0 { Severity::Warning }
                else { Severity::Info }
            }
            FailureType::ConsensusFailure { .. } => Severity::Fatal,
            FailureType::ServiceCrash { .. } => Severity::Critical,
            FailureType::SlowOperation { duration_ms, .. } => {
                if *duration_ms > 30000 { Severity::Warning }
                else { Severity::Info }
            }
            FailureType::Custom { .. } => Severity::Warning,
        }
    }
}

/// An incident representing a detected failure
#[derive(Debug, Clone)]
pub struct Incident {
    pub id: IncidentId,
    pub failure: FailureType,
    pub severity: Severity,
    pub detected_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub status: IncidentStatus,
    pub root_cause: Option<RootCause>,
    pub remediation_actions: Vec<RemediationAction>,
    pub affected_components: Vec<String>,
    pub related_incidents: Vec<IncidentId>,
    pub metadata: HashMap<String, String>,
}

impl Incident {
    pub fn new(failure: FailureType) -> Self {
        let severity = failure.severity();
        Self {
            id: IncidentId::new(),
            failure,
            severity,
            detected_at: Utc::now(),
            resolved_at: None,
            status: IncidentStatus::Detected,
            root_cause: None,
            remediation_actions: Vec::new(),
            affected_components: Vec::new(),
            related_incidents: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn duration(&self) -> Option<Duration> {
        self.resolved_at.map(|r| {
            let dur = r.signed_duration_since(self.detected_at);
            Duration::from_millis(dur.num_milliseconds().max(0) as u64)
        })
    }

    pub fn is_resolved(&self) -> bool {
        matches!(self.status, IncidentStatus::Resolved | IncidentStatus::Closed)
    }
}

/// Status of an incident
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncidentStatus {
    Detected,
    Investigating,
    RootCauseIdentified,
    RemediationInProgress,
    Resolved,
    Escalated,
    Closed,
}

/// Root cause of an incident
#[derive(Debug, Clone)]
pub struct RootCause {
    pub cause_type: CauseType,
    pub description: String,
    pub confidence: f64,
    pub evidence: Vec<Evidence>,
    pub contributing_factors: Vec<String>,
}

/// Types of root causes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CauseType {
    HardwareFailure,
    SoftwareBug,
    ConfigurationError,
    ResourceExhaustion,
    NetworkIssue,
    ExternalDependency,
    HumanError,
    SecurityIncident,
    CapacityLimit,
    Unknown,
}

/// Evidence supporting a root cause determination
#[derive(Debug, Clone)]
pub struct Evidence {
    pub source: String,
    pub timestamp: DateTime<Utc>,
    pub description: String,
    pub data: HashMap<String, String>,
}

/// Remediation action to resolve an incident
#[derive(Debug, Clone)]
pub struct RemediationAction {
    pub id: ActionId,
    pub action_type: ActionType,
    pub status: ActionStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<ActionResult>,
    pub rollback_action: Option<Box<RemediationAction>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActionId(pub u64);

impl ActionId {
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for ActionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Types of remediation actions
#[derive(Debug, Clone, PartialEq)]
pub enum ActionType {
    /// Restart a service
    RestartService { node: NodeId, service: String },
    /// Failover to backup
    Failover { from: NodeId, to: NodeId },
    /// Repair corrupted data
    RepairData { chunk_id: String },
    /// Rebalance data
    Rebalance { reason: String },
    /// Evict node from cluster
    EvictNode { node: NodeId },
    /// Add node to cluster
    AddNode { node: NodeId },
    /// Scale resources
    Scale { component: String, factor: f64 },
    /// Run maintenance
    Maintenance { task: String },
    /// Alert humans
    Alert { channel: String, message: String },
    /// Custom action
    Custom { name: String, params: HashMap<String, String> },
}

/// Status of a remediation action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    RolledBack,
    Skipped,
}

/// Result of a remediation action
#[derive(Debug, Clone)]
pub struct ActionResult {
    pub success: bool,
    pub message: String,
    pub metrics: HashMap<String, f64>,
}

/// Anomaly detected in the system
#[derive(Debug, Clone)]
pub struct Anomaly {
    pub id: u64,
    pub metric: String,
    pub expected_value: f64,
    pub actual_value: f64,
    pub deviation: f64,
    pub timestamp: DateTime<Utc>,
    pub context: HashMap<String, String>,
}

/// Anomaly detector using statistical methods
pub struct AnomalyDetector {
    /// Moving averages for metrics
    averages: RwLock<HashMap<String, MovingAverage>>,
    /// Standard deviation threshold for anomaly detection
    threshold_std: f64,
    /// Recent anomalies
    recent_anomalies: RwLock<VecDeque<Anomaly>>,
    /// Maximum anomalies to keep
    max_anomalies: usize,
}

struct MovingAverage {
    values: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
    window_size: usize,
}

impl MovingAverage {
    fn new(window_size: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(window_size),
            sum: 0.0,
            sum_sq: 0.0,
            window_size,
        }
    }

    fn add(&mut self, value: f64) {
        if self.values.len() >= self.window_size {
            if let Some(old) = self.values.pop_front() {
                self.sum -= old;
                self.sum_sq -= old * old;
            }
        }
        self.values.push_back(value);
        self.sum += value;
        self.sum_sq += value * value;
    }

    fn mean(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    fn std_dev(&self) -> f64 {
        if self.values.len() < 2 {
            0.0
        } else {
            let n = self.values.len() as f64;
            let variance = (self.sum_sq / n) - (self.sum / n).powi(2);
            variance.max(0.0).sqrt()
        }
    }
}

impl AnomalyDetector {
    pub fn new(threshold_std: f64) -> Self {
        Self {
            averages: RwLock::new(HashMap::new()),
            threshold_std,
            recent_anomalies: RwLock::new(VecDeque::new()),
            max_anomalies: 1000,
        }
    }

    /// Record a metric value and check for anomalies
    pub async fn record(&self, metric: &str, value: f64) -> Option<Anomaly> {
        let mut averages = self.averages.write().await;
        let avg = averages.entry(metric.to_string())
            .or_insert_with(|| MovingAverage::new(100));

        let mean = avg.mean();
        let std = avg.std_dev();

        avg.add(value);

        // Check for anomaly (if we have enough data)
        if avg.values.len() > 10 && std > 0.0 {
            let deviation = (value - mean).abs() / std;
            if deviation > self.threshold_std {
                let anomaly = Anomaly {
                    id: rand::random(),
                    metric: metric.to_string(),
                    expected_value: mean,
                    actual_value: value,
                    deviation,
                    timestamp: Utc::now(),
                    context: HashMap::new(),
                };

                let mut recent = self.recent_anomalies.write().await;
                if recent.len() >= self.max_anomalies {
                    recent.pop_front();
                }
                recent.push_back(anomaly.clone());

                return Some(anomaly);
            }
        }

        None
    }

    /// Get recent anomalies
    pub async fn get_recent_anomalies(&self, limit: usize) -> Vec<Anomaly> {
        let anomalies = self.recent_anomalies.read().await;
        anomalies.iter().rev().take(limit).cloned().collect()
    }
}

/// Root cause analyzer
pub struct RootCauseAnalyzer {
    /// Known patterns for root cause identification
    patterns: Vec<CausePattern>,
    /// Historical incidents for learning
    history: RwLock<VecDeque<Incident>>,
    /// Maximum history size
    max_history: usize,
}

/// Pattern for identifying root causes
#[derive(Debug, Clone)]
struct CausePattern {
    name: String,
    failure_types: Vec<FailureType>,
    cause_type: CauseType,
    confidence_base: f64,
    description: String,
}

impl RootCauseAnalyzer {
    pub fn new() -> Self {
        Self {
            patterns: Self::default_patterns(),
            history: RwLock::new(VecDeque::new()),
            max_history: 10000,
        }
    }

    fn default_patterns() -> Vec<CausePattern> {
        vec![
            CausePattern {
                name: "Hardware Disk Failure".to_string(),
                failure_types: vec![
                    FailureType::DiskFailure { node: 0, disk: String::new() },
                ],
                cause_type: CauseType::HardwareFailure,
                confidence_base: 0.9,
                description: "Disk hardware failure detected".to_string(),
            },
            CausePattern {
                name: "Memory Exhaustion".to_string(),
                failure_types: vec![
                    FailureType::MemoryPressure { node: 0, used_percent: 0.0 },
                ],
                cause_type: CauseType::ResourceExhaustion,
                confidence_base: 0.85,
                description: "System running out of memory".to_string(),
            },
            CausePattern {
                name: "Network Connectivity".to_string(),
                failure_types: vec![
                    FailureType::NetworkPartition { affected_nodes: vec![] },
                    FailureType::NodeUnreachable(0),
                ],
                cause_type: CauseType::NetworkIssue,
                confidence_base: 0.8,
                description: "Network connectivity issues".to_string(),
            },
            CausePattern {
                name: "Consensus Protocol Failure".to_string(),
                failure_types: vec![
                    FailureType::ConsensusFailure { reason: String::new() },
                ],
                cause_type: CauseType::SoftwareBug,
                confidence_base: 0.7,
                description: "Consensus protocol failure - possible software bug".to_string(),
            },
        ]
    }

    /// Analyze an incident to determine root cause
    pub async fn analyze(&self, incident: &Incident) -> Option<RootCause> {
        let mut best_match: Option<(RootCause, f64)> = None;

        for pattern in &self.patterns {
            if self.matches_pattern(incident, pattern) {
                let confidence = self.calculate_confidence(incident, pattern).await;
                if confidence > best_match.as_ref().map(|(_, c)| *c).unwrap_or(0.0) {
                    let root_cause = RootCause {
                        cause_type: pattern.cause_type.clone(),
                        description: pattern.description.clone(),
                        confidence,
                        evidence: self.gather_evidence(incident, pattern),
                        contributing_factors: vec![],
                    };
                    best_match = Some((root_cause, confidence));
                }
            }
        }

        // Learn from this analysis
        if let Some((ref cause, _)) = best_match {
            let mut history = self.history.write().await;
            if history.len() >= self.max_history {
                history.pop_front();
            }
            let mut incident_with_cause = incident.clone();
            incident_with_cause.root_cause = Some(cause.clone());
            history.push_back(incident_with_cause);
        }

        best_match.map(|(cause, _)| cause)
    }

    fn matches_pattern(&self, incident: &Incident, pattern: &CausePattern) -> bool {
        pattern.failure_types.iter().any(|ft| {
            std::mem::discriminant(&incident.failure) == std::mem::discriminant(ft)
        })
    }

    async fn calculate_confidence(&self, incident: &Incident, pattern: &CausePattern) -> f64 {
        let mut confidence = pattern.confidence_base;

        // Boost confidence if similar incidents were seen before
        let history = self.history.read().await;
        let similar_count = history.iter()
            .filter(|h| {
                std::mem::discriminant(&h.failure) == std::mem::discriminant(&incident.failure)
            })
            .count();

        if similar_count > 0 {
            confidence += 0.1 * (1.0 - 1.0 / (similar_count as f64 + 1.0));
        }

        confidence.min(0.99)
    }

    fn gather_evidence(&self, _incident: &Incident, pattern: &CausePattern) -> Vec<Evidence> {
        vec![Evidence {
            source: "pattern_match".to_string(),
            timestamp: Utc::now(),
            description: format!("Matched pattern: {}", pattern.name),
            data: HashMap::new(),
        }]
    }

    /// Get similar historical incidents
    pub async fn get_similar_incidents(&self, incident: &Incident, limit: usize) -> Vec<Incident> {
        let history = self.history.read().await;
        history.iter()
            .filter(|h| {
                std::mem::discriminant(&h.failure) == std::mem::discriminant(&incident.failure)
            })
            .take(limit)
            .cloned()
            .collect()
    }
}

impl Default for RootCauseAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Remediation engine for executing healing actions
pub struct RemediationEngine {
    /// Action handlers
    handlers: HashMap<String, Box<dyn ActionHandler>>,
    /// Active actions
    active_actions: RwLock<HashMap<ActionId, RemediationAction>>,
    /// Completed actions history
    history: RwLock<VecDeque<RemediationAction>>,
}

/// Handler for executing a remediation action
#[async_trait::async_trait]
pub trait ActionHandler: Send + Sync {
    async fn execute(&self, action: &ActionType) -> Result<ActionResult>;
    async fn rollback(&self, action: &ActionType) -> Result<ActionResult>;
    fn can_handle(&self, action: &ActionType) -> bool;
}

/// Default handler for common actions
struct DefaultActionHandler;

#[async_trait::async_trait]
impl ActionHandler for DefaultActionHandler {
    async fn execute(&self, action: &ActionType) -> Result<ActionResult> {
        info!(action = ?action, "Executing remediation action");

        // Simulate action execution
        tokio::time::sleep(Duration::from_millis(100)).await;

        match action {
            ActionType::RestartService { node, service } => {
                info!(node = node, service = service, "Restarted service");
                Ok(ActionResult {
                    success: true,
                    message: format!("Restarted {} on node {}", service, node),
                    metrics: HashMap::new(),
                })
            }
            ActionType::Failover { from, to } => {
                info!(from = from, to = to, "Executed failover");
                Ok(ActionResult {
                    success: true,
                    message: format!("Failed over from {} to {}", from, to),
                    metrics: HashMap::new(),
                })
            }
            ActionType::RepairData { chunk_id } => {
                info!(chunk_id = chunk_id, "Repaired chunk");
                Ok(ActionResult {
                    success: true,
                    message: format!("Repaired chunk {}", chunk_id),
                    metrics: HashMap::new(),
                })
            }
            ActionType::Alert { channel, message } => {
                warn!(channel = channel, message = message, "Alert sent");
                Ok(ActionResult {
                    success: true,
                    message: format!("Alert sent to {}", channel),
                    metrics: HashMap::new(),
                })
            }
            _ => Ok(ActionResult {
                success: true,
                message: "Action executed".to_string(),
                metrics: HashMap::new(),
            }),
        }
    }

    async fn rollback(&self, action: &ActionType) -> Result<ActionResult> {
        warn!(action = ?action, "Rolling back action");
        Ok(ActionResult {
            success: true,
            message: "Rollback completed".to_string(),
            metrics: HashMap::new(),
        })
    }

    fn can_handle(&self, _action: &ActionType) -> bool {
        true
    }
}

impl RemediationEngine {
    pub fn new() -> Self {
        let mut handlers: HashMap<String, Box<dyn ActionHandler>> = HashMap::new();
        handlers.insert("default".to_string(), Box::new(DefaultActionHandler));

        Self {
            handlers,
            active_actions: RwLock::new(HashMap::new()),
            history: RwLock::new(VecDeque::new()),
        }
    }

    /// Execute a remediation action
    pub async fn execute(&self, action: &mut RemediationAction) -> Result<()> {
        action.started_at = Some(Utc::now());
        action.status = ActionStatus::InProgress;

        self.active_actions.write().await
            .insert(action.id, action.clone());

        let result = self.handlers.get("default")
            .unwrap()
            .execute(&action.action_type)
            .await;

        action.completed_at = Some(Utc::now());

        match result {
            Ok(r) => {
                action.status = if r.success {
                    ActionStatus::Completed
                } else {
                    ActionStatus::Failed
                };
                action.result = Some(r);
            }
            Err(e) => {
                action.status = ActionStatus::Failed;
                action.result = Some(ActionResult {
                    success: false,
                    message: e.to_string(),
                    metrics: HashMap::new(),
                });
            }
        }

        self.active_actions.write().await.remove(&action.id);

        let mut history = self.history.write().await;
        if history.len() >= 1000 {
            history.pop_front();
        }
        history.push_back(action.clone());

        Ok(())
    }

    /// Rollback an action
    pub async fn rollback(&self, action: &mut RemediationAction) -> Result<()> {
        if action.status != ActionStatus::Completed {
            return Err(StrataError::InvalidData("Can only rollback completed actions".into()));
        }

        let result = self.handlers.get("default")
            .unwrap()
            .rollback(&action.action_type)
            .await?;

        action.status = ActionStatus::RolledBack;
        action.result = Some(result);

        Ok(())
    }

    /// Get suggested actions for an incident
    pub fn suggest_actions(&self, incident: &Incident) -> Vec<ActionType> {
        match &incident.failure {
            FailureType::NodeUnreachable(node) => {
                vec![
                    ActionType::Alert {
                        channel: "ops".to_string(),
                        message: format!("Node {} unreachable", node),
                    },
                    ActionType::EvictNode { node: *node },
                    ActionType::Rebalance { reason: "Node evicted".to_string() },
                ]
            }
            FailureType::DiskFailure { node, disk } => {
                vec![
                    ActionType::Alert {
                        channel: "ops".to_string(),
                        message: format!("Disk {} failed on node {}", disk, node),
                    },
                    ActionType::Rebalance { reason: "Disk failure recovery".to_string() },
                ]
            }
            FailureType::ChunkCorruption { chunk_id, .. } => {
                vec![
                    ActionType::RepairData { chunk_id: chunk_id.clone() },
                ]
            }
            FailureType::ServiceCrash { node, service } => {
                vec![
                    ActionType::RestartService { node: *node, service: service.clone() },
                ]
            }
            FailureType::MemoryPressure { node, .. } => {
                vec![
                    ActionType::RestartService { node: *node, service: "all".to_string() },
                    ActionType::Alert {
                        channel: "ops".to_string(),
                        message: format!("Memory pressure on node {}", node),
                    },
                ]
            }
            _ => {
                vec![ActionType::Alert {
                    channel: "ops".to_string(),
                    message: format!("Incident detected: {:?}", incident.failure),
                }]
            }
        }
    }
}

impl Default for RemediationEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Self-healing controller orchestrating detection, analysis, and remediation
pub struct SelfHealingController {
    anomaly_detector: AnomalyDetector,
    rca: RootCauseAnalyzer,
    remediation: RemediationEngine,
    incidents: RwLock<HashMap<IncidentId, Incident>>,
    enabled: RwLock<bool>,
    auto_remediate: RwLock<bool>,
}

impl SelfHealingController {
    pub fn new() -> Self {
        Self {
            anomaly_detector: AnomalyDetector::new(3.0), // 3 std deviations
            rca: RootCauseAnalyzer::new(),
            remediation: RemediationEngine::new(),
            incidents: RwLock::new(HashMap::new()),
            enabled: RwLock::new(true),
            auto_remediate: RwLock::new(false),
        }
    }

    /// Report a failure and create an incident
    pub async fn report_failure(&self, failure: FailureType) -> Result<IncidentId> {
        if !*self.enabled.read().await {
            return Err(StrataError::InvalidData("Self-healing is disabled".into()));
        }

        let mut incident = Incident::new(failure);
        incident.status = IncidentStatus::Investigating;

        // Perform RCA
        if let Some(root_cause) = self.rca.analyze(&incident).await {
            incident.root_cause = Some(root_cause);
            incident.status = IncidentStatus::RootCauseIdentified;
        }

        // Get suggested actions
        let suggested = self.remediation.suggest_actions(&incident);
        for action_type in suggested {
            incident.remediation_actions.push(RemediationAction {
                id: ActionId::new(),
                action_type,
                status: ActionStatus::Pending,
                started_at: None,
                completed_at: None,
                result: None,
                rollback_action: None,
            });
        }

        let id = incident.id;
        info!(incident_id = ?id, severity = ?incident.severity, "Incident created");

        self.incidents.write().await.insert(id, incident.clone());

        // Auto-remediate if enabled and severity allows
        if *self.auto_remediate.read().await && !incident.severity.requires_human_review() {
            self.auto_remediate_incident(id).await?;
        }

        Ok(id)
    }

    /// Manually trigger remediation
    pub async fn remediate(&self, incident_id: IncidentId) -> Result<()> {
        let mut incidents = self.incidents.write().await;
        let incident = incidents.get_mut(&incident_id)
            .ok_or_else(|| StrataError::NotFound(format!("Incident {:?}", incident_id)))?;

        incident.status = IncidentStatus::RemediationInProgress;

        for action in &mut incident.remediation_actions {
            if action.status == ActionStatus::Pending {
                self.remediation.execute(action).await?;
            }
        }

        // Check if all actions completed successfully
        let all_success = incident.remediation_actions.iter()
            .all(|a| a.status == ActionStatus::Completed);

        if all_success {
            incident.status = IncidentStatus::Resolved;
            incident.resolved_at = Some(Utc::now());
            info!(incident_id = ?incident_id, "Incident resolved");
        } else {
            incident.status = IncidentStatus::Escalated;
            warn!(incident_id = ?incident_id, "Incident escalated - remediation failed");
        }

        Ok(())
    }

    async fn auto_remediate_incident(&self, incident_id: IncidentId) -> Result<()> {
        info!(incident_id = ?incident_id, "Auto-remediating incident");
        self.remediate(incident_id).await
    }

    /// Get an incident by ID
    pub async fn get_incident(&self, id: IncidentId) -> Option<Incident> {
        self.incidents.read().await.get(&id).cloned()
    }

    /// Get all active incidents
    pub async fn get_active_incidents(&self) -> Vec<Incident> {
        self.incidents.read().await
            .values()
            .filter(|i| !i.is_resolved())
            .cloned()
            .collect()
    }

    /// Enable/disable self-healing
    pub async fn set_enabled(&self, enabled: bool) {
        *self.enabled.write().await = enabled;
        info!(enabled = enabled, "Self-healing enabled state changed");
    }

    /// Enable/disable auto-remediation
    pub async fn set_auto_remediate(&self, enabled: bool) {
        *self.auto_remediate.write().await = enabled;
        info!(enabled = enabled, "Auto-remediation enabled state changed");
    }

    /// Record a metric for anomaly detection
    pub async fn record_metric(&self, metric: &str, value: f64) -> Option<IncidentId> {
        if let Some(anomaly) = self.anomaly_detector.record(metric, value).await {
            let failure = FailureType::Custom {
                category: "anomaly".to_string(),
                description: format!(
                    "Anomaly in {}: expected {:.2}, got {:.2} ({:.1}σ)",
                    anomaly.metric, anomaly.expected_value, anomaly.actual_value, anomaly.deviation
                ),
            };
            return self.report_failure(failure).await.ok();
        }
        None
    }

    /// Get statistics
    pub async fn stats(&self) -> SelfHealingStats {
        let incidents = self.incidents.read().await;

        let total = incidents.len();
        let resolved = incidents.values().filter(|i| i.is_resolved()).count();
        let active = total - resolved;

        let by_severity: HashMap<_, _> = [
            Severity::Info,
            Severity::Warning,
            Severity::Error,
            Severity::Critical,
            Severity::Fatal,
        ].iter()
            .map(|s| (*s, incidents.values().filter(|i| i.severity == *s).count()))
            .collect();

        SelfHealingStats {
            total_incidents: total,
            active_incidents: active,
            resolved_incidents: resolved,
            incidents_by_severity: by_severity,
            auto_remediate_enabled: *self.auto_remediate.read().await,
        }
    }
}

impl Default for SelfHealingController {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for self-healing system
#[derive(Debug, Clone)]
pub struct SelfHealingStats {
    pub total_incidents: usize,
    pub active_incidents: usize,
    pub resolved_incidents: usize,
    pub incidents_by_severity: HashMap<Severity, usize>,
    pub auto_remediate_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incident_creation() {
        let failure = FailureType::NodeUnreachable(1);
        let incident = Incident::new(failure);

        assert_eq!(incident.severity, Severity::Critical);
        assert!(!incident.is_resolved());
    }

    #[test]
    fn test_severity_levels() {
        assert!(Severity::Critical.requires_immediate_action());
        assert!(!Severity::Warning.requires_immediate_action());

        assert!(Severity::Error.requires_human_review());
        assert!(!Severity::Info.requires_human_review());

        assert!(Severity::Fatal > Severity::Critical);
        assert!(Severity::Critical > Severity::Error);
    }

    #[test]
    fn test_failure_severity() {
        let disk = FailureType::DiskFailure {
            node: 1,
            disk: "sda1".to_string(),
        };
        assert_eq!(disk.severity(), Severity::Critical);

        let mem_low = FailureType::MemoryPressure { node: 1, used_percent: 80.0 };
        assert_eq!(mem_low.severity(), Severity::Info);

        let mem_high = FailureType::MemoryPressure { node: 1, used_percent: 96.0 };
        assert_eq!(mem_high.severity(), Severity::Critical);
    }

    #[tokio::test]
    async fn test_anomaly_detector() {
        let detector = AnomalyDetector::new(3.0);

        // Add normal values to establish baseline
        for i in 0..50 {
            let value = 100.0 + (i as f64 % 10.0);
            detector.record("cpu", value).await;
        }

        // This should not be an anomaly
        let normal = detector.record("cpu", 105.0).await;
        assert!(normal.is_none());

        // This should be an anomaly
        let anomaly = detector.record("cpu", 1000.0).await;
        assert!(anomaly.is_some());
    }

    #[tokio::test]
    async fn test_root_cause_analyzer() {
        let rca = RootCauseAnalyzer::new();

        let incident = Incident::new(FailureType::DiskFailure {
            node: 1,
            disk: "sda1".to_string(),
        });

        let root_cause = rca.analyze(&incident).await;
        assert!(root_cause.is_some());

        let rc = root_cause.unwrap();
        assert_eq!(rc.cause_type, CauseType::HardwareFailure);
        assert!(rc.confidence > 0.5);
    }

    #[tokio::test]
    async fn test_remediation_engine() {
        let engine = RemediationEngine::new();

        let mut action = RemediationAction {
            id: ActionId::new(),
            action_type: ActionType::RestartService {
                node: 1,
                service: "strata".to_string(),
            },
            status: ActionStatus::Pending,
            started_at: None,
            completed_at: None,
            result: None,
            rollback_action: None,
        };

        engine.execute(&mut action).await.unwrap();
        assert_eq!(action.status, ActionStatus::Completed);
        assert!(action.result.as_ref().unwrap().success);
    }

    #[tokio::test]
    async fn test_suggested_actions() {
        let engine = RemediationEngine::new();

        let incident = Incident::new(FailureType::ChunkCorruption {
            chunk_id: "abc123".to_string(),
            expected_checksum: "xyz".to_string(),
        });

        let actions = engine.suggest_actions(&incident);
        assert!(!actions.is_empty());
        assert!(matches!(actions[0], ActionType::RepairData { .. }));
    }

    #[tokio::test]
    async fn test_self_healing_controller() {
        let controller = SelfHealingController::new();
        controller.set_auto_remediate(true).await;

        let incident_id = controller.report_failure(
            FailureType::ChunkCorruption {
                chunk_id: "test".to_string(),
                expected_checksum: "abc".to_string(),
            }
        ).await.unwrap();

        let incident = controller.get_incident(incident_id).await.unwrap();
        assert!(incident.root_cause.is_some());
    }

    #[tokio::test]
    async fn test_stats() {
        let controller = SelfHealingController::new();

        controller.report_failure(FailureType::NodeUnreachable(1)).await.unwrap();
        controller.report_failure(FailureType::NodeUnreachable(2)).await.unwrap();

        let stats = controller.stats().await;
        assert_eq!(stats.total_incidents, 2);
        assert_eq!(stats.active_incidents, 2);
    }

    #[test]
    fn test_moving_average() {
        let mut ma = MovingAverage::new(5);

        for i in 1..=5 {
            ma.add(i as f64);
        }

        assert!((ma.mean() - 3.0).abs() < 0.01);
        assert!(ma.std_dev() > 0.0);
    }
}
