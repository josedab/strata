//! Main AIOps engine coordinating all ML operations.

use super::anomaly::AnomalyDetector;
use super::capacity::CapacityForecaster;
use super::config::AIOpsConfig;
use super::failure_predictor::FailurePredictor;
use super::metrics::MetricsSample;
use super::prediction::{CapacityForecast, FailurePrediction};
use super::tuner::AutoTuner;
use super::workload::{WorkloadCharacteristics, WorkloadClassifier, WorkloadPattern};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

/// Main AIOps engine coordinating all ML operations
pub struct AIOpsEngine {
    #[allow(dead_code)]
    config: AIOpsConfig,
    failure_predictor: Arc<RwLock<FailurePredictor>>,
    capacity_forecaster: Arc<CapacityForecaster>,
    anomaly_detector: Arc<AnomalyDetector>,
    auto_tuner: Arc<AutoTuner>,
    workload_classifier: Arc<WorkloadClassifier>,
    stats: AIOpsStats,
}

#[derive(Debug, Default)]
pub struct AIOpsStats {
    pub samples_processed: AtomicU64,
    pub predictions_made: AtomicU64,
    pub anomalies_detected: AtomicU64,
    pub experiments_run: AtomicU64,
}

impl AIOpsEngine {
    pub fn new(config: AIOpsConfig) -> Self {
        Self {
            failure_predictor: Arc::new(RwLock::new(FailurePredictor::new(config.clone()))),
            capacity_forecaster: Arc::new(CapacityForecaster::new(config.clone())),
            anomaly_detector: Arc::new(AnomalyDetector::new(config.clone())),
            auto_tuner: Arc::new(AutoTuner::new(config.clone())),
            workload_classifier: Arc::new(WorkloadClassifier::new()),
            stats: AIOpsStats::default(),
            config,
        }
    }

    /// Process a metrics sample
    pub async fn process_sample(&self, sample: MetricsSample) {
        self.stats.samples_processed.fetch_add(1, Ordering::Relaxed);

        // Feed to failure predictor
        self.failure_predictor.write().await.observe(sample.clone());

        // Update anomaly baselines
        let detector = &self.anomaly_detector;
        detector
            .update_baseline("cpu_percent", sample.cpu_percent)
            .await;
        detector
            .update_baseline("memory_percent", sample.memory_percent)
            .await;
        detector
            .update_baseline("latency_p99", sample.latency_ms.2)
            .await;
        detector
            .update_baseline("error_rate", sample.error_rate)
            .await;

        // Check for anomalies
        if let Some(anomaly) = detector
            .check(sample.node_id, "latency_p99", sample.latency_ms.2)
            .await
        {
            self.stats.anomalies_detected.fetch_add(1, Ordering::Relaxed);
            warn!(anomaly_id = %anomaly.id, "Anomaly detected: {}", anomaly.description);
        }
    }

    /// Get failure predictions for all nodes
    pub async fn get_failure_predictions(&self) -> Vec<FailurePrediction> {
        self.failure_predictor.read().await.all_predictions().await
    }

    /// Get capacity forecast
    pub async fn get_capacity_forecast(&self) -> CapacityForecast {
        self.capacity_forecaster.forecast().await
    }

    /// Get tuning suggestions
    pub async fn get_tuning_suggestions(
        &self,
        metrics: &HashMap<String, f64>,
    ) -> Vec<(String, f64, f64)> {
        let mut suggestions = Vec::new();

        for param in self.auto_tuner.parameters().await {
            if let Some(suggested) = self.auto_tuner.suggest(&param.name, metrics).await {
                suggestions.push((param.name, param.current_value, suggested));
            }
        }

        suggestions
    }

    /// Get workload classification
    pub fn classify_workload(
        &self,
        characteristics: &WorkloadCharacteristics,
    ) -> Option<&WorkloadPattern> {
        self.workload_classifier.classify(characteristics)
    }

    /// Get statistics
    pub fn stats(&self) -> &AIOpsStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_aiops_engine() {
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

        assert_eq!(engine.stats.samples_processed.load(Ordering::Relaxed), 1);
    }
}
