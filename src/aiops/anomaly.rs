//! Anomaly detection in system metrics.

use super::config::AIOpsConfig;
use super::prediction::{Anomaly, AnomalyType};
use crate::types::NodeId;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Detects anomalies in system metrics
pub struct AnomalyDetector {
    config: AIOpsConfig,
    baselines: RwLock<HashMap<String, MetricBaseline>>,
    detected: RwLock<Vec<Anomaly>>,
}

struct MetricBaseline {
    mean: f64,
    std_dev: f64,
    min: f64,
    max: f64,
    sample_count: u64,
}

impl AnomalyDetector {
    pub fn new(config: AIOpsConfig) -> Self {
        Self {
            config,
            baselines: RwLock::new(HashMap::new()),
            detected: RwLock::new(Vec::new()),
        }
    }

    /// Update baseline for a metric
    pub async fn update_baseline(&self, metric: &str, value: f64) {
        let mut baselines = self.baselines.write().await;

        let baseline = baselines.entry(metric.to_string()).or_insert(MetricBaseline {
            mean: value,
            std_dev: 0.0,
            min: value,
            max: value,
            sample_count: 0,
        });

        // Welford's online algorithm for mean and variance
        baseline.sample_count += 1;
        let delta = value - baseline.mean;
        baseline.mean += delta / baseline.sample_count as f64;
        let delta2 = value - baseline.mean;
        let m2 = baseline.std_dev.powi(2) * (baseline.sample_count - 1) as f64;
        let new_m2 = m2 + delta * delta2;
        baseline.std_dev = (new_m2 / baseline.sample_count as f64).sqrt();

        baseline.min = baseline.min.min(value);
        baseline.max = baseline.max.max(value);
    }

    /// Check for anomaly
    pub async fn check(&self, node_id: NodeId, metric: &str, value: f64) -> Option<Anomaly> {
        let baselines = self.baselines.read().await;
        let baseline = baselines.get(metric)?;

        if baseline.sample_count < 100 {
            return None; // Not enough data
        }

        // Calculate z-score
        let z_score = if baseline.std_dev > 0.0 {
            (value - baseline.mean).abs() / baseline.std_dev
        } else {
            0.0
        };

        // Threshold based on sensitivity
        let threshold = 3.0 - (self.config.anomaly_sensitivity * 2.0);

        if z_score > threshold {
            let anomaly_type = match metric {
                m if m.contains("latency") => AnomalyType::LatencySpike,
                m if m.contains("throughput") || m.contains("iops") => AnomalyType::ThroughputDrop,
                m if m.contains("error") => AnomalyType::ErrorRateIncrease,
                m if m.contains("cpu") || m.contains("memory") || m.contains("disk") => {
                    AnomalyType::ResourceExhaustion
                }
                _ => AnomalyType::UnusualPattern,
            };

            let severity = (z_score / 10.0).min(1.0);

            let anomaly = Anomaly {
                id: uuid::Uuid::new_v4().to_string(),
                node_id,
                anomaly_type,
                severity,
                description: format!(
                    "{} is {} (z-score: {:.2}, expected: {:.2} ± {:.2})",
                    metric,
                    if value > baseline.mean {
                        "unusually high"
                    } else {
                        "unusually low"
                    },
                    z_score,
                    baseline.mean,
                    baseline.std_dev
                ),
                affected_metrics: vec![metric.to_string()],
                detected_at: Utc::now(),
                is_new_pattern: baseline.sample_count < 1000,
            };

            // Store detection
            self.detected.write().await.push(anomaly.clone());

            return Some(anomaly);
        }

        None
    }

    /// Get recent anomalies
    pub async fn recent_anomalies(&self, since: DateTime<Utc>) -> Vec<Anomaly> {
        self.detected
            .read()
            .await
            .iter()
            .filter(|a| a.detected_at >= since)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_anomaly_detector() {
        let config = AIOpsConfig::default();
        let detector = AnomalyDetector::new(config);

        // Build baseline
        for i in 0..200 {
            detector
                .update_baseline("latency", 10.0 + (i % 3) as f64)
                .await;
        }

        // Normal value - no anomaly
        let normal = detector.check(1u64, "latency", 11.0).await;
        assert!(normal.is_none());

        // Anomalous value
        let anomaly = detector.check(1u64, "latency", 100.0).await;
        assert!(anomaly.is_some());
    }
}
