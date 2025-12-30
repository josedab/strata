//! Chaos Engineering Framework for Strata
//!
//! Provides fault injection capabilities for testing system resilience.
//! Inspired by Netflix's Chaos Monkey and other chaos engineering tools.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Chaos Controller                         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Experiment Engine  │  Fault Injector  │  Safety Monitor   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Network Faults │ Disk Faults │ Process Faults │ Time Faults│
//! ├─────────────────────────────────────────────────────────────┤
//! │  Steady State   │  Hypothesis  │  Blast Radius │  Rollback  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::chaos::{ChaosController, Experiment, Fault};
//!
//! let controller = ChaosController::new(config);
//!
//! // Define an experiment
//! let experiment = Experiment::builder()
//!     .name("network-partition-test")
//!     .hypothesis("System remains available during network partition")
//!     .steady_state(|ctx| async { ctx.check_cluster_health().await })
//!     .fault(Fault::NetworkPartition {
//!         nodes: vec![node1, node2],
//!         duration: Duration::from_secs(30),
//!     })
//!     .blast_radius(BlastRadius::SingleNode)
//!     .build();
//!
//! let result = controller.run(experiment).await?;
//! ```

// Submodules
mod config;
mod controller;
mod experiment;
mod types;

// Re-export configuration types
pub use config::{ChaosConfig, ChaosSchedule};

// Re-export core types
pub use types::{
    BlastRadius, ClockDirection, CorruptionMode, DiskFailureMode, Fault, MetricOperator,
    ProcessType, SteadyState, SteadyStateCheck, SteadyStateTolerance,
};

// Re-export experiment types
pub use experiment::{
    Experiment, ExperimentBuilder, ExperimentMetrics, ExperimentResult, ExperimentStatus,
    FaultResult, SteadyStateResult,
};

// Re-export controller types
pub use controller::{
    ChaosController, ChaosEvent, DefaultFaultInjector, DefaultSteadyStateChecker, FaultHandle,
    FaultInjector, SteadyStateChecker,
};

// Re-export predefined experiments
pub use controller::experiments;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_module_exports() {
        // Test that all key types are accessible
        let _config = ChaosConfig::default();
        let _schedule = ChaosSchedule::business_hours();

        let _fault = Fault::NetworkLatency {
            nodes: vec![1, 2],
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(30),
        };

        let _radius = BlastRadius::SingleNode;
        let _status = ExperimentStatus::Pending;
    }

    #[test]
    fn test_experiment_builder_accessible() {
        let exp = Experiment::builder()
            .name("test")
            .build()
            .unwrap();

        assert_eq!(exp.name, "test");
    }
}
