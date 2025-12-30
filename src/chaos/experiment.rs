//! Experiment definition and results.

use super::config::ChaosConfig;
use super::types::{BlastRadius, Fault, SteadyState};
use crate::error::{Result, StrataError};
use crate::types::NodeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A chaos experiment definition
#[derive(Debug, Clone)]
pub struct Experiment {
    /// Unique experiment ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Description of the experiment
    pub description: String,
    /// Hypothesis being tested
    pub hypothesis: String,
    /// Steady state definition (before and after)
    pub steady_state: Vec<SteadyState>,
    /// Faults to inject
    pub faults: Vec<Fault>,
    /// Blast radius constraint
    pub blast_radius: BlastRadius,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Rollback on failure
    pub rollback_on_failure: bool,
    /// Dry run (don't actually inject faults)
    pub dry_run: bool,
}

impl Experiment {
    /// Create a new experiment builder
    pub fn builder() -> ExperimentBuilder {
        ExperimentBuilder::new()
    }

    /// Validate the experiment configuration
    pub fn validate(&self, config: &ChaosConfig, total_nodes: usize) -> Result<()> {
        // Check blast radius
        let max_affected = self.blast_radius.max_nodes(total_nodes);
        let max_allowed = (total_nodes * config.max_blast_radius_percent as usize) / 100;

        if max_affected > max_allowed {
            return Err(StrataError::InvalidArgument(format!(
                "Blast radius {} exceeds maximum allowed {} ({}% of {} nodes)",
                max_affected, max_allowed, config.max_blast_radius_percent, total_nodes
            )));
        }

        // Check for destructive faults requiring approval
        if config.require_approval {
            for fault in &self.faults {
                if fault.is_destructive() {
                    return Err(StrataError::InvalidArgument(
                        "Destructive faults require approval".to_string(),
                    ));
                }
            }
        }

        // Check excluded nodes
        for fault in &self.faults {
            for node in fault.affected_nodes() {
                if config.excluded_nodes.contains(&node) {
                    return Err(StrataError::InvalidArgument(format!(
                        "Node {} is excluded from chaos experiments",
                        node
                    )));
                }
            }
        }

        // Check total duration
        let total_duration: Duration = self.faults.iter().map(|f| f.duration()).sum();
        if total_duration > config.safety_timeout {
            return Err(StrataError::InvalidArgument(format!(
                "Total fault duration {:?} exceeds safety timeout {:?}",
                total_duration, config.safety_timeout
            )));
        }

        Ok(())
    }
}

/// Builder for experiments
pub struct ExperimentBuilder {
    name: Option<String>,
    description: Option<String>,
    hypothesis: Option<String>,
    steady_state: Vec<SteadyState>,
    faults: Vec<Fault>,
    blast_radius: BlastRadius,
    tags: Vec<String>,
    rollback_on_failure: bool,
    dry_run: bool,
}

impl ExperimentBuilder {
    pub fn new() -> Self {
        Self {
            name: None,
            description: None,
            hypothesis: None,
            steady_state: Vec::new(),
            faults: Vec::new(),
            blast_radius: BlastRadius::SingleNode,
            tags: Vec::new(),
            rollback_on_failure: true,
            dry_run: false,
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn hypothesis(mut self, hypothesis: impl Into<String>) -> Self {
        self.hypothesis = Some(hypothesis.into());
        self
    }

    pub fn steady_state(mut self, check: SteadyState) -> Self {
        self.steady_state.push(check);
        self
    }

    pub fn fault(mut self, fault: Fault) -> Self {
        self.faults.push(fault);
        self
    }

    pub fn blast_radius(mut self, radius: BlastRadius) -> Self {
        self.blast_radius = radius;
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn rollback_on_failure(mut self, rollback: bool) -> Self {
        self.rollback_on_failure = rollback;
        self
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn build(self) -> Result<Experiment> {
        let name = self
            .name
            .ok_or_else(|| StrataError::InvalidArgument("Experiment name is required".into()))?;

        Ok(Experiment {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description: self.description.unwrap_or_default(),
            hypothesis: self
                .hypothesis
                .unwrap_or_else(|| "System remains available under fault conditions".into()),
            steady_state: self.steady_state,
            faults: self.faults,
            blast_radius: self.blast_radius,
            tags: self.tags,
            created_at: Utc::now(),
            rollback_on_failure: self.rollback_on_failure,
            dry_run: self.dry_run,
        })
    }
}

impl Default for ExperimentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Experiment Result
// ============================================================================

/// Result of running an experiment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResult {
    /// Experiment ID
    pub experiment_id: String,
    /// Experiment name
    pub experiment_name: String,
    /// Overall status
    pub status: ExperimentStatus,
    /// Start time
    pub started_at: DateTime<Utc>,
    /// End time
    pub ended_at: DateTime<Utc>,
    /// Duration
    pub duration: Duration,
    /// Steady state results (before)
    pub steady_state_before: Vec<SteadyStateResult>,
    /// Steady state results (after)
    pub steady_state_after: Vec<SteadyStateResult>,
    /// Fault injection results
    pub fault_results: Vec<FaultResult>,
    /// Whether hypothesis was proven
    pub hypothesis_proven: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Metrics collected during experiment
    pub metrics: ExperimentMetrics,
}

/// Status of an experiment
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExperimentStatus {
    /// Experiment is pending
    Pending,
    /// Experiment is running
    Running,
    /// Experiment completed successfully
    Success,
    /// Experiment failed
    Failed,
    /// Experiment was aborted
    Aborted,
    /// Experiment was rolled back
    RolledBack,
}

/// Result of a steady state check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SteadyStateResult {
    /// Check name
    pub name: String,
    /// Whether check passed
    pub passed: bool,
    /// Actual value
    pub actual_value: Option<String>,
    /// Expected value
    pub expected_value: Option<String>,
    /// Error message if failed
    pub error: Option<String>,
    /// Check duration
    pub duration: Duration,
}

/// Result of a fault injection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultResult {
    /// Fault type description
    pub fault_type: String,
    /// Whether injection succeeded
    pub injected: bool,
    /// Whether rollback succeeded
    pub rolled_back: bool,
    /// Affected nodes
    pub affected_nodes: Vec<NodeId>,
    /// Duration of fault
    pub duration: Duration,
    /// Error if failed
    pub error: Option<String>,
}

/// Metrics collected during an experiment
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExperimentMetrics {
    /// Read latency samples (p50, p99, max)
    pub read_latency_ms: Option<(f64, f64, f64)>,
    /// Write latency samples (p50, p99, max)
    pub write_latency_ms: Option<(f64, f64, f64)>,
    /// Throughput (ops/sec)
    pub throughput_ops: Option<f64>,
    /// Error count
    pub error_count: u64,
    /// Successful operations
    pub success_count: u64,
    /// Data availability percentage
    pub availability_percent: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_experiment_builder() {
        let exp = Experiment::builder()
            .name("test-experiment")
            .description("A test experiment")
            .hypothesis("The system should remain available")
            .tag("test")
            .build()
            .unwrap();

        assert_eq!(exp.name, "test-experiment");
        assert_eq!(exp.description, "A test experiment");
    }

    #[test]
    fn test_experiment_builder_requires_name() {
        let result = Experiment::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_experiment_status() {
        assert_eq!(ExperimentStatus::Pending, ExperimentStatus::Pending);
        assert_ne!(ExperimentStatus::Success, ExperimentStatus::Failed);
    }
}
