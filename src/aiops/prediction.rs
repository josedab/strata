//! Prediction types for AIOps.

use crate::types::NodeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Failure prediction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailurePrediction {
    /// Node ID
    pub node_id: NodeId,
    /// Probability of failure (0-1)
    pub probability: f64,
    /// Predicted time to failure
    pub time_to_failure: Option<Duration>,
    /// Failure type
    pub failure_type: FailureType,
    /// Contributing factors
    pub factors: Vec<(String, f64)>,
    /// Confidence score
    pub confidence: f64,
    /// Prediction timestamp
    pub predicted_at: DateTime<Utc>,
    /// Recommended actions
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureType {
    DiskFailure,
    NodeUnreachable,
    MemoryExhaustion,
    CpuOverload,
    NetworkDegradation,
    Unknown,
}

/// Capacity forecast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    /// Forecast for each time point
    pub forecasts: Vec<CapacityPoint>,
    /// Time when capacity will be exhausted (if predicted)
    pub exhaustion_time: Option<DateTime<Utc>>,
    /// Recommended capacity to add
    pub recommended_capacity_gb: Option<u64>,
    /// Confidence interval (lower, upper)
    pub confidence_interval: (f64, f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityPoint {
    pub timestamp: DateTime<Utc>,
    pub used_gb: f64,
    pub total_gb: f64,
    pub predicted_used_gb: f64,
}

/// Anomaly detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Anomaly ID
    pub id: String,
    /// Affected node
    pub node_id: NodeId,
    /// Anomaly type
    pub anomaly_type: AnomalyType,
    /// Severity (0-1)
    pub severity: f64,
    /// Description
    pub description: String,
    /// Affected metrics
    pub affected_metrics: Vec<String>,
    /// Detection time
    pub detected_at: DateTime<Utc>,
    /// Whether this is a new pattern
    pub is_new_pattern: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalyType {
    LatencySpike,
    ThroughputDrop,
    ErrorRateIncrease,
    ResourceExhaustion,
    UnusualPattern,
    CorrelatedFailure,
}
