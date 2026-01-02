//! Chaos engineering integration tests.
//!
//! Tests for the chaos framework including fault injection,
//! experiment execution, and safety mechanisms.

#[allow(dead_code)]
mod common;

use std::time::Duration;

use strata::chaos::{
    BlastRadius, ChaosConfig, ChaosController, ChaosSchedule, ClockDirection, CorruptionMode,
    DiskFailureMode, Experiment, ExperimentStatus, Fault, MetricOperator, ProcessType,
    SteadyState, SteadyStateCheck, SteadyStateTolerance,
};

// =============================================================================
// Configuration Tests
// =============================================================================

#[test]
fn test_chaos_config_default() {
    let config = ChaosConfig::default();

    // Default should be safe
    assert!(!config.enabled);
    assert!(config.require_approval);
    assert_eq!(config.max_blast_radius_percent, 33);
}

#[test]
fn test_chaos_config_development() {
    let config = ChaosConfig::development();

    // Development is more permissive
    assert!(config.enabled);
    assert!(!config.require_approval);
    assert_eq!(config.max_blast_radius_percent, 50);
}

#[test]
fn test_chaos_config_production() {
    let config = ChaosConfig::production();

    // Production is conservative
    assert!(config.enabled);
    assert!(config.require_approval);
    assert_eq!(config.max_blast_radius_percent, 10);
    assert!(config.schedule.is_some());
}

#[test]
fn test_chaos_schedule_business_hours() {
    let schedule = ChaosSchedule::business_hours();

    // Should be weekdays only (Mon-Fri = 1-5)
    assert_eq!(schedule.days, vec![1, 2, 3, 4, 5]);
    assert_eq!(schedule.start_hour, 9);
    assert_eq!(schedule.end_hour, 17);
}

// =============================================================================
// Fault Types Tests
// =============================================================================

#[test]
fn test_fault_network_partition() {
    let fault = Fault::NetworkPartition {
        nodes: vec![1, 2],
        duration: Duration::from_secs(30),
        bidirectional: true,
    };

    assert_eq!(fault.duration(), Duration::from_secs(30));
    assert_eq!(fault.affected_nodes(), vec![1, 2]);
}

#[test]
fn test_fault_network_latency() {
    let fault = Fault::NetworkLatency {
        nodes: vec![1, 2, 3],
        latency: Duration::from_millis(100),
        jitter: Duration::from_millis(20),
        duration: Duration::from_secs(60),
    };

    assert_eq!(fault.duration(), Duration::from_secs(60));
    assert_eq!(fault.affected_nodes().len(), 3);
}

#[test]
fn test_fault_packet_loss() {
    let fault = Fault::PacketLoss {
        nodes: vec![1],
        loss_percent: 25,
        duration: Duration::from_secs(45),
    };

    assert_eq!(fault.duration(), Duration::from_secs(45));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_disk_failure() {
    let fault = Fault::DiskFailure {
        node: 1,
        disk_id: "sda1".to_string(),
        mode: DiskFailureMode::ReadOnly,
        duration: Duration::from_secs(120),
    };

    assert_eq!(fault.duration(), Duration::from_secs(120));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_process_crash() {
    let fault = Fault::ProcessCrash {
        node: 1,
        process: ProcessType::MetadataServer,
        auto_restart: true,
        restart_delay: Duration::from_secs(10),
    };

    // Duration equals restart delay for process crash
    assert_eq!(fault.duration(), Duration::from_secs(10));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_cpu_stress() {
    let fault = Fault::CpuStress {
        nodes: vec![1, 2],
        usage_percent: 80,
        cores: Some(4),
        duration: Duration::from_secs(60),
    };

    assert_eq!(fault.duration(), Duration::from_secs(60));
    assert_eq!(fault.affected_nodes().len(), 2);
}

#[test]
fn test_fault_memory_pressure() {
    let fault = Fault::MemoryPressure {
        nodes: vec![1],
        consume_mb: 1024,
        duration: Duration::from_secs(30),
    };

    assert_eq!(fault.duration(), Duration::from_secs(30));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_clock_skew() {
    let fault = Fault::ClockSkew {
        nodes: vec![1],
        drift: Duration::from_secs(300),
        direction: ClockDirection::Forward,
        duration: Duration::from_secs(60),
    };

    assert_eq!(fault.duration(), Duration::from_secs(60));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_bit_rot() {
    let fault = Fault::BitRot {
        node: 1,
        chunk_count: 10,
        mode: CorruptionMode::BitFlip { bits_per_chunk: 1 },
    };

    // BitRot is instant, no duration
    assert_eq!(fault.duration(), Duration::from_secs(0));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_node_failure() {
    let fault = Fault::NodeFailure {
        node: 1,
        duration: Duration::from_secs(300),
    };

    assert_eq!(fault.duration(), Duration::from_secs(300));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_cascading() {
    let initial = Fault::NodeFailure {
        node: 1,
        duration: Duration::from_secs(60),
    };

    let cascade1 = Fault::NodeFailure {
        node: 2,
        duration: Duration::from_secs(30),
    };

    let cascade2 = Fault::NodeFailure {
        node: 3,
        duration: Duration::from_secs(30),
    };

    let fault = Fault::CascadingFailure {
        initial: Box::new(initial),
        cascade: vec![
            (Duration::from_secs(10), Box::new(cascade1)),
            (Duration::from_secs(20), Box::new(cascade2)),
        ],
    };

    // Duration should be max of all cascaded faults
    assert!(fault.duration() >= Duration::from_secs(60));
}

#[test]
fn test_fault_is_destructive() {
    // BitRot is destructive
    let bit_rot = Fault::BitRot {
        node: 1,
        chunk_count: 10,
        mode: CorruptionMode::ZeroFill,
    };
    assert!(bit_rot.is_destructive());

    // Node failure is destructive
    let node_failure = Fault::NodeFailure {
        node: 1,
        duration: Duration::from_secs(60),
    };
    assert!(node_failure.is_destructive());

    // Process crash without auto-restart is destructive
    let crash_no_restart = Fault::ProcessCrash {
        node: 1,
        process: ProcessType::DataServer,
        auto_restart: false,
        restart_delay: Duration::from_secs(0),
    };
    assert!(crash_no_restart.is_destructive());

    // Process crash with auto-restart is not destructive
    let crash_with_restart = Fault::ProcessCrash {
        node: 1,
        process: ProcessType::DataServer,
        auto_restart: true,
        restart_delay: Duration::from_secs(10),
    };
    assert!(!crash_with_restart.is_destructive());

    // Network partition is not destructive
    let partition = Fault::NetworkPartition {
        nodes: vec![1, 2],
        duration: Duration::from_secs(30),
        bidirectional: true,
    };
    assert!(!partition.is_destructive());
}

#[test]
fn test_fault_bandwidth_limit() {
    let fault = Fault::BandwidthLimit {
        nodes: vec![1, 2],
        bytes_per_second: 1_000_000, // 1 MB/s
        duration: Duration::from_secs(60),
    };

    assert_eq!(fault.duration(), Duration::from_secs(60));
    assert_eq!(fault.affected_nodes().len(), 2);
}

#[test]
fn test_fault_disk_slowdown() {
    let fault = Fault::DiskSlowdown {
        node: 1,
        read_latency: Duration::from_millis(50),
        write_latency: Duration::from_millis(100),
        duration: Duration::from_secs(120),
    };

    assert_eq!(fault.duration(), Duration::from_secs(120));
    assert_eq!(fault.affected_nodes(), vec![1]);
}

#[test]
fn test_fault_dns_failure() {
    let fault = Fault::DnsFailure {
        nodes: vec![1, 2, 3],
        domains: vec!["example.com".to_string(), "test.local".to_string()],
        duration: Duration::from_secs(30),
    };

    assert_eq!(fault.duration(), Duration::from_secs(30));
    assert_eq!(fault.affected_nodes().len(), 3);
}

#[test]
fn test_fault_custom_script() {
    let fault = Fault::CustomScript {
        script: "/usr/local/bin/chaos-script.sh".to_string(),
        args: vec!["--target".to_string(), "node1".to_string()],
        rollback_script: Some("/usr/local/bin/chaos-rollback.sh".to_string()),
        duration: Duration::from_secs(60),
    };

    assert_eq!(fault.duration(), Duration::from_secs(60));
    // Custom script doesn't track affected nodes
    assert!(fault.affected_nodes().is_empty());
}

// =============================================================================
// Blast Radius Tests
// =============================================================================

#[test]
fn test_blast_radius_single_node() {
    let radius = BlastRadius::SingleNode;
    assert_eq!(radius.max_nodes(10), 1);
}

#[test]
fn test_blast_radius_percentage() {
    let radius = BlastRadius::Percentage(20);

    assert_eq!(radius.max_nodes(10), 2);
    assert_eq!(radius.max_nodes(100), 20);
}

#[test]
fn test_blast_radius_fixed_count() {
    let radius = BlastRadius::FixedCount(3);

    assert_eq!(radius.max_nodes(10), 3);
    assert_eq!(radius.max_nodes(100), 3);
}

#[test]
fn test_blast_radius_availability_zone() {
    let radius = BlastRadius::AvailabilityZone;

    // Assumes 3 AZs
    assert_eq!(radius.max_nodes(9), 3);
    assert_eq!(radius.max_nodes(12), 4);
}

#[test]
fn test_blast_radius_region() {
    let radius = BlastRadius::Region;

    // Region affects all nodes
    assert_eq!(radius.max_nodes(10), 10);
    assert_eq!(radius.max_nodes(100), 100);
}

// =============================================================================
// Experiment Builder Tests
// =============================================================================

#[test]
fn test_experiment_builder_basic() {
    let experiment = Experiment::builder()
        .name("test-experiment")
        .hypothesis("System remains available")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .blast_radius(BlastRadius::SingleNode)
        .build()
        .unwrap();

    assert_eq!(experiment.name, "test-experiment");
    assert_eq!(experiment.hypothesis, "System remains available");
    assert_eq!(experiment.faults.len(), 1);
}

#[test]
fn test_experiment_builder_with_steady_state() {
    let steady_state = SteadyState {
        name: "cluster-health".to_string(),
        description: "Cluster should be healthy".to_string(),
        check: SteadyStateCheck::ClusterHealth,
        tolerance: SteadyStateTolerance::Exact,
    };

    let experiment = Experiment::builder()
        .name("steady-state-test")
        .hypothesis("Maintains health")
        .steady_state(steady_state)
        .fault(Fault::NetworkLatency {
            nodes: vec![1],
            latency: Duration::from_millis(50),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(60),
        })
        .build()
        .unwrap();

    assert_eq!(experiment.steady_state.len(), 1);
}

#[test]
fn test_experiment_requires_name() {
    let result = Experiment::builder()
        .hypothesis("Should fail")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .build();

    // Building without a name should fail
    assert!(result.is_err());
}

#[test]
fn test_experiment_with_multiple_faults() {
    let experiment = Experiment::builder()
        .name("multi-fault")
        .hypothesis("Handles multiple faults")
        .fault(Fault::NetworkLatency {
            nodes: vec![1],
            latency: Duration::from_millis(100),
            jitter: Duration::from_millis(20),
            duration: Duration::from_secs(30),
        })
        .fault(Fault::PacketLoss {
            nodes: vec![2],
            loss_percent: 10,
            duration: Duration::from_secs(30),
        })
        .build()
        .unwrap();

    assert_eq!(experiment.faults.len(), 2);
}

#[test]
fn test_experiment_with_tags() {
    let experiment = Experiment::builder()
        .name("tagged-experiment")
        .tag("network")
        .tag("production")
        .tag("critical")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .build()
        .unwrap();

    assert_eq!(experiment.tags.len(), 3);
    assert!(experiment.tags.contains(&"network".to_string()));
    assert!(experiment.tags.contains(&"production".to_string()));
}

#[test]
fn test_experiment_dry_run() {
    let experiment = Experiment::builder()
        .name("dry-run-test")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .dry_run(true)
        .build()
        .unwrap();

    assert!(experiment.dry_run);
}

#[test]
fn test_experiment_rollback_on_failure() {
    let experiment = Experiment::builder()
        .name("rollback-test")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .rollback_on_failure(true)
        .build()
        .unwrap();

    assert!(experiment.rollback_on_failure);
}

// =============================================================================
// Steady State Tests
// =============================================================================

#[test]
fn test_steady_state_cluster_health() {
    let state = SteadyState {
        name: "cluster-health".to_string(),
        description: "Check cluster is healthy".to_string(),
        check: SteadyStateCheck::ClusterHealth,
        tolerance: SteadyStateTolerance::Exact,
    };

    assert_eq!(state.name, "cluster-health");
}

#[test]
fn test_steady_state_read_latency() {
    let state = SteadyState {
        name: "read-latency".to_string(),
        description: "Read latency under threshold".to_string(),
        check: SteadyStateCheck::ReadLatency { max_p99_ms: 100 },
        tolerance: SteadyStateTolerance::Deviation { percent: 10.0 },
    };

    assert_eq!(state.name, "read-latency");
}

#[test]
fn test_steady_state_write_latency() {
    let state = SteadyState {
        name: "write-latency".to_string(),
        description: "Write latency under threshold".to_string(),
        check: SteadyStateCheck::WriteLatency { max_p99_ms: 200 },
        tolerance: SteadyStateTolerance::Retries {
            count: 3,
            delay: Duration::from_secs(1),
        },
    };

    assert_eq!(state.name, "write-latency");
}

#[test]
fn test_steady_state_error_rate() {
    let state = SteadyState {
        name: "error-rate".to_string(),
        description: "Error rate below threshold".to_string(),
        check: SteadyStateCheck::ErrorRate { max_percent: 0.1 },
        tolerance: SteadyStateTolerance::Exact,
    };

    assert_eq!(state.name, "error-rate");
}

#[test]
fn test_steady_state_throughput() {
    let state = SteadyState {
        name: "throughput".to_string(),
        description: "Throughput above minimum".to_string(),
        check: SteadyStateCheck::Throughput {
            min_ops_per_second: 1000,
        },
        tolerance: SteadyStateTolerance::Deviation { percent: 5.0 },
    };

    assert_eq!(state.name, "throughput");
}

#[test]
fn test_steady_state_http_check() {
    let state = SteadyState {
        name: "http-health".to_string(),
        description: "HTTP health endpoint returns 200".to_string(),
        check: SteadyStateCheck::HttpCheck {
            url: "http://localhost:8080/health".to_string(),
            expected_status: 200,
        },
        tolerance: SteadyStateTolerance::Exact,
    };

    assert_eq!(state.name, "http-health");
}

#[test]
fn test_steady_state_metric_check() {
    let state = SteadyState {
        name: "metric-check".to_string(),
        description: "Custom metric check".to_string(),
        check: SteadyStateCheck::MetricCheck {
            metric: "request_latency_p99".to_string(),
            operator: MetricOperator::LessThan,
            value: 100.0,
        },
        tolerance: SteadyStateTolerance::Exact,
    };

    assert_eq!(state.name, "metric-check");
}

#[test]
fn test_steady_state_raft_leader() {
    let state = SteadyState {
        name: "raft-leader".to_string(),
        description: "Raft cluster has a leader".to_string(),
        check: SteadyStateCheck::RaftLeaderExists,
        tolerance: SteadyStateTolerance::Retries {
            count: 5,
            delay: Duration::from_millis(500),
        },
    };

    assert_eq!(state.name, "raft-leader");
}

#[test]
fn test_steady_state_quorum() {
    let state = SteadyState {
        name: "quorum".to_string(),
        description: "Quorum is maintained".to_string(),
        check: SteadyStateCheck::QuorumMaintained,
        tolerance: SteadyStateTolerance::Exact,
    };

    assert_eq!(state.name, "quorum");
}

// =============================================================================
// Chaos Controller Tests
// =============================================================================

#[test]
fn test_controller_creation() {
    let config = ChaosConfig::development();
    let _controller = ChaosController::new(config);
    // Controller created successfully
}

#[test]
fn test_controller_with_disabled_config() {
    let config = ChaosConfig::default(); // disabled by default
    let _controller = ChaosController::new(config);
    // Controller can be created even when disabled
}

// =============================================================================
// Experiment Validation Tests
// =============================================================================

#[test]
fn test_experiment_validation_blast_radius() {
    let config = ChaosConfig {
        enabled: true,
        max_blast_radius_percent: 10,
        require_approval: false,
        ..Default::default()
    };

    let experiment = Experiment::builder()
        .name("too-wide")
        .hypothesis("Affects too many nodes")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(30),
        })
        .blast_radius(BlastRadius::Region) // Affects all nodes
        .build()
        .unwrap();

    // Should fail validation - region affects 100% but limit is 10%
    let result = experiment.validate(&config, 10);
    assert!(result.is_err());
}

#[test]
fn test_experiment_validation_excluded_nodes() {
    let config = ChaosConfig {
        enabled: true,
        require_approval: false,
        excluded_nodes: vec![1, 2], // Protect nodes 1 and 2
        ..Default::default()
    };

    let experiment = Experiment::builder()
        .name("excluded-target")
        .hypothesis("Targets excluded node")
        .fault(Fault::NodeFailure {
            node: 1, // This is excluded
            duration: Duration::from_secs(30),
        })
        .build()
        .unwrap();

    let result = experiment.validate(&config, 5);
    assert!(result.is_err());
}

#[test]
fn test_experiment_validation_destructive_requires_approval() {
    let config = ChaosConfig {
        enabled: true,
        require_approval: true, // Require approval for destructive
        ..Default::default()
    };

    let experiment = Experiment::builder()
        .name("destructive-test")
        .hypothesis("Tests destructive fault")
        .fault(Fault::BitRot {
            node: 3,
            chunk_count: 5,
            mode: CorruptionMode::ZeroFill,
        })
        .build()
        .unwrap();

    let result = experiment.validate(&config, 10);
    assert!(result.is_err());
}

#[test]
fn test_experiment_validation_duration_exceeds_timeout() {
    let config = ChaosConfig {
        enabled: true,
        require_approval: false,
        safety_timeout: Duration::from_secs(60), // 1 minute max
        ..Default::default()
    };

    let experiment = Experiment::builder()
        .name("long-experiment")
        .hypothesis("Runs too long")
        .fault(Fault::NodeFailure {
            node: 1,
            duration: Duration::from_secs(120), // 2 minutes
        })
        .build()
        .unwrap();

    let result = experiment.validate(&config, 10);
    assert!(result.is_err());
}

#[test]
fn test_experiment_validation_passes() {
    let config = ChaosConfig {
        enabled: true,
        max_blast_radius_percent: 50,
        require_approval: false,
        safety_timeout: Duration::from_secs(120),
        excluded_nodes: vec![],
        ..Default::default()
    };

    let experiment = Experiment::builder()
        .name("valid-experiment")
        .hypothesis("Should pass validation")
        .fault(Fault::NetworkLatency {
            nodes: vec![1],
            latency: Duration::from_millis(50),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(30),
        })
        .blast_radius(BlastRadius::SingleNode)
        .build()
        .unwrap();

    let result = experiment.validate(&config, 10);
    assert!(result.is_ok());
}

// =============================================================================
// Experiment Status Tests
// =============================================================================

#[test]
fn test_experiment_status_variants() {
    let pending = ExperimentStatus::Pending;
    let running = ExperimentStatus::Running;
    let success = ExperimentStatus::Success;
    let failed = ExperimentStatus::Failed;
    let aborted = ExperimentStatus::Aborted;
    let rolled_back = ExperimentStatus::RolledBack;

    // All variants should be distinguishable
    assert_ne!(pending, running);
    assert_ne!(success, failed);
    assert_ne!(aborted, rolled_back);
}

#[test]
fn test_experiment_status_serialization() {
    let status = ExperimentStatus::Success;

    let json = serde_json::to_string(&status).unwrap();
    let deserialized: ExperimentStatus = serde_json::from_str(&json).unwrap();

    assert_eq!(status, deserialized);
}

// =============================================================================
// Serialization Tests
// =============================================================================

#[test]
fn test_fault_serialization() {
    let fault = Fault::NetworkPartition {
        nodes: vec![1, 2, 3],
        duration: Duration::from_secs(60),
        bidirectional: true,
    };

    // Should be serializable
    let json = serde_json::to_string(&fault).unwrap();
    let deserialized: Fault = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.duration(), fault.duration());
}

#[test]
fn test_config_serialization() {
    let config = ChaosConfig::production();

    let json = serde_json::to_string(&config).unwrap();
    let deserialized: ChaosConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.enabled, config.enabled);
    assert_eq!(
        deserialized.max_blast_radius_percent,
        config.max_blast_radius_percent
    );
}

#[test]
fn test_blast_radius_serialization() {
    let radii = vec![
        BlastRadius::SingleNode,
        BlastRadius::FixedCount(5),
        BlastRadius::Percentage(25),
        BlastRadius::AvailabilityZone,
        BlastRadius::Region,
    ];

    for radius in radii {
        let json = serde_json::to_string(&radius).unwrap();
        let deserialized: BlastRadius = serde_json::from_str(&json).unwrap();
        assert_eq!(radius, deserialized);
    }
}

// =============================================================================
// Process Type Tests
// =============================================================================

#[test]
fn test_process_types() {
    let types = vec![
        ProcessType::MetadataServer,
        ProcessType::DataServer,
        ProcessType::RaftNode,
        ProcessType::S3Gateway,
        ProcessType::FuseClient,
        ProcessType::AllServices,
    ];

    // All process types should be distinct
    for i in 0..types.len() {
        for j in (i + 1)..types.len() {
            assert_ne!(types[i], types[j]);
        }
    }
}

#[test]
fn test_process_type_serialization() {
    let process = ProcessType::MetadataServer;

    let json = serde_json::to_string(&process).unwrap();
    let deserialized: ProcessType = serde_json::from_str(&json).unwrap();

    assert_eq!(process, deserialized);
}

// =============================================================================
// Disk Failure Mode Tests
// =============================================================================

#[test]
fn test_disk_failure_modes() {
    let modes = vec![
        DiskFailureMode::Complete,
        DiskFailureMode::ReadOnly,
        DiskFailureMode::WriteErrors,
        DiskFailureMode::RandomErrors {
            error_rate_percent: 10,
        },
    ];

    // All modes should be distinct
    assert_ne!(modes[0], modes[1]);
    assert_ne!(modes[1], modes[2]);
}

#[test]
fn test_disk_failure_mode_serialization() {
    let mode = DiskFailureMode::RandomErrors {
        error_rate_percent: 25,
    };

    let json = serde_json::to_string(&mode).unwrap();
    let deserialized: DiskFailureMode = serde_json::from_str(&json).unwrap();

    assert_eq!(mode, deserialized);
}

// =============================================================================
// Clock Direction Tests
// =============================================================================

#[test]
fn test_clock_directions() {
    let forward = ClockDirection::Forward;
    let backward = ClockDirection::Backward;

    assert_ne!(forward, backward);
}

#[test]
fn test_clock_direction_serialization() {
    let direction = ClockDirection::Forward;

    let json = serde_json::to_string(&direction).unwrap();
    let deserialized: ClockDirection = serde_json::from_str(&json).unwrap();

    assert_eq!(direction, deserialized);
}

// =============================================================================
// Corruption Mode Tests
// =============================================================================

#[test]
fn test_corruption_modes() {
    let modes = vec![
        CorruptionMode::BitFlip { bits_per_chunk: 1 },
        CorruptionMode::ZeroFill,
        CorruptionMode::RandomFill,
        CorruptionMode::Truncate { bytes: 100 },
    ];

    // All modes should have a debug representation
    for mode in modes {
        assert!(!format!("{:?}", mode).is_empty());
    }
}

#[test]
fn test_corruption_mode_serialization() {
    let mode = CorruptionMode::BitFlip { bits_per_chunk: 5 };

    let json = serde_json::to_string(&mode).unwrap();
    let deserialized: CorruptionMode = serde_json::from_str(&json).unwrap();

    assert_eq!(mode, deserialized);
}

// =============================================================================
// Metric Operator Tests
// =============================================================================

#[test]
fn test_metric_operators() {
    let operators = vec![
        MetricOperator::LessThan,
        MetricOperator::LessThanOrEqual,
        MetricOperator::GreaterThan,
        MetricOperator::GreaterThanOrEqual,
        MetricOperator::Equal,
    ];

    // All operators should be distinct
    for i in 0..operators.len() {
        for j in (i + 1)..operators.len() {
            assert_ne!(operators[i], operators[j]);
        }
    }
}

// =============================================================================
// Tolerance Tests
// =============================================================================

#[test]
fn test_steady_state_tolerance_exact() {
    let tolerance = SteadyStateTolerance::Exact;
    assert!(!format!("{:?}", tolerance).is_empty());
}

#[test]
fn test_steady_state_tolerance_deviation() {
    let tolerance = SteadyStateTolerance::Deviation { percent: 5.0 };
    assert!(!format!("{:?}", tolerance).is_empty());
}

#[test]
fn test_steady_state_tolerance_retries() {
    let tolerance = SteadyStateTolerance::Retries {
        count: 3,
        delay: Duration::from_secs(1),
    };
    assert!(!format!("{:?}", tolerance).is_empty());
}

// =============================================================================
// Schedule Tests
// =============================================================================

#[test]
fn test_schedule_is_active_logic() {
    // Create a schedule that should be active
    let schedule = ChaosSchedule {
        days: (0..7).collect(), // All days
        start_hour: 0,
        end_hour: 24,
        timezone: "UTC".to_string(),
    };

    // Should always be active
    assert!(schedule.is_active());
}

#[test]
fn test_schedule_never_active() {
    // Create a schedule with no days
    let schedule = ChaosSchedule {
        days: vec![], // No days
        start_hour: 9,
        end_hour: 17,
        timezone: "UTC".to_string(),
    };

    // Should never be active
    assert!(!schedule.is_active());
}

// =============================================================================
// Complex Fault Scenarios
// =============================================================================

#[test]
fn test_complex_cascading_failure() {
    // Build a complex cascading failure scenario
    let initial = Fault::NetworkPartition {
        nodes: vec![1, 2],
        duration: Duration::from_secs(30),
        bidirectional: true,
    };

    let stage1 = Fault::DiskSlowdown {
        node: 1,
        read_latency: Duration::from_millis(100),
        write_latency: Duration::from_millis(200),
        duration: Duration::from_secs(60),
    };

    let stage2 = Fault::ProcessCrash {
        node: 2,
        process: ProcessType::DataServer,
        auto_restart: true,
        restart_delay: Duration::from_secs(15),
    };

    let cascade = Fault::CascadingFailure {
        initial: Box::new(initial),
        cascade: vec![
            (Duration::from_secs(10), Box::new(stage1)),
            (Duration::from_secs(20), Box::new(stage2)),
        ],
    };

    // Should capture all affected nodes
    let affected = cascade.affected_nodes();
    assert!(affected.contains(&1));
    assert!(affected.contains(&2));
}

#[test]
fn test_experiment_with_all_steady_state_types() {
    let experiment = Experiment::builder()
        .name("comprehensive-test")
        .hypothesis("System remains stable under stress")
        .steady_state(SteadyState {
            name: "cluster-health".to_string(),
            description: "Cluster should be healthy".to_string(),
            check: SteadyStateCheck::ClusterHealth,
            tolerance: SteadyStateTolerance::Exact,
        })
        .steady_state(SteadyState {
            name: "latency".to_string(),
            description: "Read latency under 100ms".to_string(),
            check: SteadyStateCheck::ReadLatency { max_p99_ms: 100 },
            tolerance: SteadyStateTolerance::Deviation { percent: 10.0 },
        })
        .steady_state(SteadyState {
            name: "errors".to_string(),
            description: "Error rate under 1%".to_string(),
            check: SteadyStateCheck::ErrorRate { max_percent: 1.0 },
            tolerance: SteadyStateTolerance::Exact,
        })
        .fault(Fault::NetworkLatency {
            nodes: vec![1, 2],
            latency: Duration::from_millis(50),
            jitter: Duration::from_millis(10),
            duration: Duration::from_secs(60),
        })
        .blast_radius(BlastRadius::FixedCount(2))
        .build()
        .unwrap();

    assert_eq!(experiment.steady_state.len(), 3);
    assert_eq!(experiment.faults.len(), 1);
}
