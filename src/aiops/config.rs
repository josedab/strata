//! Configuration for AIOps.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// AIOps configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIOpsConfig {
    /// Enable AIOps features
    pub enabled: bool,
    /// Metrics collection interval
    pub collection_interval: Duration,
    /// Model update interval
    pub model_update_interval: Duration,
    /// History window for analysis
    pub history_window: Duration,
    /// Prediction horizon
    pub prediction_horizon: Duration,
    /// Confidence threshold for alerts
    pub confidence_threshold: f64,
    /// Enable auto-tuning
    pub auto_tuning_enabled: bool,
    /// Enable automatic actions
    pub auto_actions_enabled: bool,
    /// Anomaly sensitivity (0.0 - 1.0)
    pub anomaly_sensitivity: f64,
}

impl Default for AIOpsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(60),
            model_update_interval: Duration::from_secs(3600),
            history_window: Duration::from_secs(86400 * 7), // 7 days
            prediction_horizon: Duration::from_secs(86400), // 24 hours
            confidence_threshold: 0.8,
            auto_tuning_enabled: true,
            auto_actions_enabled: false, // Require approval by default
            anomaly_sensitivity: 0.7,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = AIOpsConfig::default();
        assert!(config.enabled);
        assert!(!config.auto_actions_enabled);
        assert_eq!(config.confidence_threshold, 0.8);
    }
}
