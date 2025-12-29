//! AIOps - AI-Powered Operations for Self-Driving Storage
//!
//! Provides machine learning-powered autonomous operations including
//! failure prediction, capacity forecasting, auto-tuning, and anomaly detection.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      AIOps Engine                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Data Collector  │  Feature Extractor  │  Model Registry   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Failure Prediction │ Capacity Forecast │ Anomaly Detection │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Auto-Tuning  │  Workload Classification  │  Cost Optimizer │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Action Recommender  │  Policy Engine  │  Feedback Loop    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Failure Prediction**: Predict disk/node failures before they happen
//! - **Capacity Forecasting**: Predict storage needs based on trends
//! - **Auto-Tuning**: Automatically optimize system parameters
//! - **Anomaly Detection**: Detect unusual patterns in metrics
//! - **Workload Classification**: Identify workload patterns for optimization

// Submodules
mod anomaly;
mod capacity;
mod config;
mod engine;
mod failure_predictor;
mod metrics;
mod prediction;
mod tuner;
mod workload;

// Re-export configuration types
pub use config::AIOpsConfig;

// Re-export metrics types
pub use metrics::{FeatureExtractor, FeatureVector, MetricsSample, SmartData};
pub use metrics::{mean, std_dev, trend};

// Re-export prediction types
pub use prediction::{
    Anomaly, AnomalyType, CapacityForecast, CapacityPoint, FailurePrediction, FailureType,
};

// Re-export component types
pub use anomaly::AnomalyDetector;
pub use capacity::CapacityForecaster;
pub use failure_predictor::FailurePredictor;
pub use tuner::{AutoTuner, ExperimentStatus, TunableParameter, TuningExperiment};
pub use workload::{TemporalPattern, WorkloadCharacteristics, WorkloadClassifier, WorkloadPattern};

// Re-export engine types
pub use engine::{AIOpsEngine, AIOpsStats};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_module_exports() {
        // Test that all key types are accessible
        let _config = AIOpsConfig::default();
        let _classifier = WorkloadClassifier::new();
    }

    #[tokio::test]
    async fn test_engine_integration() {
        let config = AIOpsConfig::default();
        let engine = AIOpsEngine::new(config);

        let sample = MetricsSample {
            timestamp: Utc::now(),
            node_id: 1u64,
            cpu_percent: 50.0,
            memory_percent: 60.0,
            disk_percent: 70.0,
            network_in_bytes: 1000,
            network_out_bytes: 1000,
            iops: 1000,
            latency_ms: (1.0, 5.0, 10.0),
            error_rate: 0.1,
            queue_depth: 10,
            smart_data: None,
        };

        engine.process_sample(sample).await;
    }
}
