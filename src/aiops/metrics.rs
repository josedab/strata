//! Metrics collection and feature extraction.

use crate::types::NodeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// System metrics sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSample {
    /// Sample timestamp
    pub timestamp: DateTime<Utc>,
    /// Node ID
    pub node_id: NodeId,
    /// CPU utilization (0-100)
    pub cpu_percent: f64,
    /// Memory utilization (0-100)
    pub memory_percent: f64,
    /// Disk utilization (0-100)
    pub disk_percent: f64,
    /// Network bytes in
    pub network_in_bytes: u64,
    /// Network bytes out
    pub network_out_bytes: u64,
    /// IOPS
    pub iops: u64,
    /// Latency percentiles (p50, p95, p99) in ms
    pub latency_ms: (f64, f64, f64),
    /// Error rate (0-100)
    pub error_rate: f64,
    /// Queue depth
    pub queue_depth: u64,
    /// SMART data (for disk prediction)
    pub smart_data: Option<SmartData>,
}

/// SMART disk data for failure prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartData {
    /// Reallocated sectors count
    pub reallocated_sectors: u64,
    /// Pending sectors count
    pub pending_sectors: u64,
    /// Uncorrectable errors
    pub uncorrectable_errors: u64,
    /// Power on hours
    pub power_on_hours: u64,
    /// Temperature in Celsius
    pub temperature: u8,
    /// Seek error rate
    pub seek_error_rate: u64,
    /// Spin retry count
    pub spin_retry_count: u64,
    /// Command timeout count
    pub command_timeout: u64,
}

/// Extracted features for ML models
#[derive(Debug, Clone)]
pub struct FeatureVector {
    /// Feature names
    pub names: Vec<String>,
    /// Feature values
    pub values: Vec<f64>,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Source node
    pub node_id: NodeId,
}

impl FeatureVector {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            names: Vec::new(),
            values: Vec::new(),
            timestamp: Utc::now(),
            node_id,
        }
    }

    pub fn add(&mut self, name: impl Into<String>, value: f64) {
        self.names.push(name.into());
        self.values.push(value);
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Feature extractor from raw metrics
pub struct FeatureExtractor {
    window_size: usize,
    history: HashMap<NodeId, VecDeque<MetricsSample>>,
}

impl FeatureExtractor {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            history: HashMap::new(),
        }
    }

    /// Add a metrics sample
    pub fn add_sample(&mut self, sample: MetricsSample) {
        let history = self.history.entry(sample.node_id).or_default();
        history.push_back(sample);

        while history.len() > self.window_size {
            history.pop_front();
        }
    }

    /// Extract features for a node
    pub fn extract(&self, node_id: NodeId) -> Option<FeatureVector> {
        let history = self.history.get(&node_id)?;
        if history.is_empty() {
            return None;
        }

        let mut features = FeatureVector::new(node_id);

        // Current values
        if let Some(latest) = history.back() {
            features.add("cpu_current", latest.cpu_percent);
            features.add("memory_current", latest.memory_percent);
            features.add("disk_current", latest.disk_percent);
            features.add("iops_current", latest.iops as f64);
            features.add("latency_p50", latest.latency_ms.0);
            features.add("latency_p95", latest.latency_ms.1);
            features.add("latency_p99", latest.latency_ms.2);
            features.add("error_rate", latest.error_rate);
            features.add("queue_depth", latest.queue_depth as f64);
        }

        // Statistical features
        let cpu_values: Vec<f64> = history.iter().map(|s| s.cpu_percent).collect();
        let mem_values: Vec<f64> = history.iter().map(|s| s.memory_percent).collect();
        let disk_values: Vec<f64> = history.iter().map(|s| s.disk_percent).collect();

        features.add("cpu_mean", mean(&cpu_values));
        features.add("cpu_std", std_dev(&cpu_values));
        features.add("cpu_trend", trend(&cpu_values));

        features.add("memory_mean", mean(&mem_values));
        features.add("memory_std", std_dev(&mem_values));
        features.add("memory_trend", trend(&mem_values));

        features.add("disk_mean", mean(&disk_values));
        features.add("disk_std", std_dev(&disk_values));
        features.add("disk_trend", trend(&disk_values));

        // SMART features (if available)
        if let Some(latest) = history.back() {
            if let Some(smart) = &latest.smart_data {
                features.add("smart_reallocated", smart.reallocated_sectors as f64);
                features.add("smart_pending", smart.pending_sectors as f64);
                features.add("smart_uncorrectable", smart.uncorrectable_errors as f64);
                features.add("smart_power_hours", smart.power_on_hours as f64);
                features.add("smart_temperature", smart.temperature as f64);
            }
        }

        Some(features)
    }
}

// Helper functions

pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

pub fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let m = mean(values);
    let variance = values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

pub fn trend(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }

    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = mean(values);

    let mut numerator = 0.0;
    let mut denominator = 0.0;

    for (i, v) in values.iter().enumerate() {
        let x = i as f64;
        numerator += (x - x_mean) * (v - y_mean);
        denominator += (x - x_mean).powi(2);
    }

    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_vector() {
        let mut features = FeatureVector::new(1u64);
        features.add("cpu", 50.0);
        features.add("memory", 60.0);

        assert_eq!(features.len(), 2);
        assert_eq!(features.names[0], "cpu");
        assert_eq!(features.values[0], 50.0);
    }

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1.0, 2.0, 3.0, 4.0, 5.0]), 3.0);
        assert_eq!(mean(&[]), 0.0);
    }

    #[test]
    fn test_std_dev() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = std_dev(&values);
        // Sample std dev = sqrt(32/7) ≈ 2.14
        assert!((sd - 2.14).abs() < 0.1);
    }

    #[test]
    fn test_trend() {
        // Increasing trend
        let increasing = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!(trend(&increasing) > 0.0);

        // Decreasing trend
        let decreasing = vec![5.0, 4.0, 3.0, 2.0, 1.0];
        assert!(trend(&decreasing) < 0.0);

        // Flat
        let flat = vec![3.0, 3.0, 3.0, 3.0, 3.0];
        assert!((trend(&flat)).abs() < 0.001);
    }
}
