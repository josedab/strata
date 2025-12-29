//! Failure prediction using SMART data and metrics.

use super::config::AIOpsConfig;
use super::metrics::{FeatureExtractor, MetricsSample};
use super::prediction::{FailurePrediction, FailureType};
use crate::types::NodeId;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::RwLock;

/// Predicts node/disk failures using SMART data and metrics
pub struct FailurePredictor {
    config: AIOpsConfig,
    feature_extractor: FeatureExtractor,
    predictions: RwLock<HashMap<NodeId, FailurePrediction>>,
    // Model weights (simplified - in practice would use proper ML library)
    weights: Vec<f64>,
}

impl FailurePredictor {
    pub fn new(config: AIOpsConfig) -> Self {
        // Initialize with pre-trained weights
        // In practice, these would come from training on historical data
        let weights = vec![
            0.3,  // reallocated_sectors
            0.25, // pending_sectors
            0.2,  // uncorrectable_errors
            0.1,  // power_on_hours
            0.05, // temperature
            0.05, // error_rate
            0.05, // latency_trend
        ];

        Self {
            config,
            feature_extractor: FeatureExtractor::new(1000),
            predictions: RwLock::new(HashMap::new()),
            weights,
        }
    }

    /// Add metrics sample for analysis
    pub fn observe(&mut self, sample: MetricsSample) {
        self.feature_extractor.add_sample(sample);
    }

    /// Predict failure probability for a node
    pub async fn predict(&self, node_id: NodeId) -> Option<FailurePrediction> {
        let features = self.feature_extractor.extract(node_id)?;

        // Calculate failure probability using logistic regression
        let mut score = 0.0;
        let mut factors = Vec::new();

        // SMART-based prediction
        if let Some(idx) = features.names.iter().position(|n| n == "smart_reallocated") {
            let value = features.values[idx];
            let contribution = value * self.weights[0];
            score += contribution;
            if contribution > 0.1 {
                factors.push(("Reallocated sectors".to_string(), contribution));
            }
        }

        if let Some(idx) = features.names.iter().position(|n| n == "smart_pending") {
            let value = features.values[idx];
            let contribution = value * self.weights[1];
            score += contribution;
            if contribution > 0.1 {
                factors.push(("Pending sectors".to_string(), contribution));
            }
        }

        if let Some(idx) = features.names.iter().position(|n| n == "smart_uncorrectable") {
            let value = features.values[idx];
            let contribution = value * self.weights[2];
            score += contribution;
            if contribution > 0.1 {
                factors.push(("Uncorrectable errors".to_string(), contribution));
            }
        }

        // Normalize with sigmoid
        let probability = 1.0 / (1.0 + (-score).exp());

        // Determine failure type
        let failure_type = if factors.iter().any(|(n, _)| n.contains("sector")) {
            FailureType::DiskFailure
        } else {
            FailureType::Unknown
        };

        // Calculate time to failure (simplified)
        let time_to_failure = if probability > 0.5 {
            Some(Duration::from_secs(
                ((1.0 - probability) * 86400.0 * 30.0) as u64, // Up to 30 days
            ))
        } else {
            None
        };

        // Generate recommendations
        let mut recommendations = Vec::new();
        if probability > 0.7 {
            recommendations.push("Schedule immediate disk replacement".to_string());
            recommendations.push("Migrate data to healthy nodes".to_string());
        } else if probability > 0.5 {
            recommendations.push("Increase monitoring frequency".to_string());
            recommendations.push("Prepare replacement hardware".to_string());
        }

        let prediction = FailurePrediction {
            node_id,
            probability,
            time_to_failure,
            failure_type,
            factors,
            confidence: 0.85, // Would be calculated from model uncertainty
            predicted_at: Utc::now(),
            recommendations,
        };

        // Cache prediction
        self.predictions
            .write()
            .await
            .insert(node_id, prediction.clone());

        Some(prediction)
    }

    /// Get all current predictions
    pub async fn all_predictions(&self) -> Vec<FailurePrediction> {
        self.predictions.read().await.values().cloned().collect()
    }

    /// Get high-risk predictions
    pub async fn high_risk_predictions(&self) -> Vec<FailurePrediction> {
        self.predictions
            .read()
            .await
            .values()
            .filter(|p| p.probability > self.config.confidence_threshold)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aiops::metrics::SmartData;

    #[tokio::test]
    async fn test_failure_predictor() {
        let config = AIOpsConfig::default();
        let mut predictor = FailurePredictor::new(config);

        // Add samples with SMART data indicating issues
        for i in 0..100 {
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
                smart_data: Some(SmartData {
                    reallocated_sectors: i * 10,
                    pending_sectors: i * 5,
                    uncorrectable_errors: i,
                    power_on_hours: 10000,
                    temperature: 40,
                    seek_error_rate: 0,
                    spin_retry_count: 0,
                    command_timeout: 0,
                }),
            };
            predictor.observe(sample);
        }

        let prediction = predictor.predict(1u64).await;
        assert!(prediction.is_some());
    }
}
