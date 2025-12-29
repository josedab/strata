//! Capacity forecasting using time series analysis.

use super::config::AIOpsConfig;
use super::metrics::{mean, std_dev};
use super::prediction::{CapacityForecast, CapacityPoint};
use chrono::Utc;
use std::collections::VecDeque;
use tokio::sync::RwLock;

/// Forecasts storage capacity using time series analysis
pub struct CapacityForecaster {
    config: AIOpsConfig,
    history: RwLock<VecDeque<CapacityPoint>>,
}

impl CapacityForecaster {
    pub fn new(config: AIOpsConfig) -> Self {
        Self {
            config,
            history: RwLock::new(VecDeque::new()),
        }
    }

    /// Record current capacity
    pub async fn record(&self, used_gb: f64, total_gb: f64) {
        let point = CapacityPoint {
            timestamp: Utc::now(),
            used_gb,
            total_gb,
            predicted_used_gb: used_gb,
        };

        let mut history = self.history.write().await;
        history.push_back(point);

        // Keep limited history
        let max_points = (self.config.history_window.as_secs() / 3600) as usize;
        while history.len() > max_points {
            history.pop_front();
        }
    }

    /// Generate capacity forecast
    pub async fn forecast(&self) -> CapacityForecast {
        let history = self.history.read().await;

        if history.len() < 2 {
            return CapacityForecast {
                forecasts: Vec::new(),
                exhaustion_time: None,
                recommended_capacity_gb: None,
                confidence_interval: (0.0, 0.0),
            };
        }

        // Simple linear regression for trend
        let _n = history.len() as f64;
        let x_values: Vec<f64> = (0..history.len()).map(|i| i as f64).collect();
        let y_values: Vec<f64> = history.iter().map(|p| p.used_gb).collect();

        let x_mean = mean(&x_values);
        let y_mean = mean(&y_values);

        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for i in 0..history.len() {
            let x_diff = x_values[i] - x_mean;
            let y_diff = y_values[i] - y_mean;
            numerator += x_diff * y_diff;
            denominator += x_diff * x_diff;
        }

        let slope = if denominator > 0.0 {
            numerator / denominator
        } else {
            0.0
        };
        let intercept = y_mean - slope * x_mean;

        // Generate forecasts
        let forecast_points = (self.config.prediction_horizon.as_secs() / 3600) as usize;
        let total_gb = history.back().map(|p| p.total_gb).unwrap_or(0.0);
        let start_idx = history.len();

        let mut forecasts = Vec::new();
        for i in 0..forecast_points {
            let x = (start_idx + i) as f64;
            let predicted = slope * x + intercept;

            forecasts.push(CapacityPoint {
                timestamp: Utc::now() + chrono::Duration::hours(i as i64),
                used_gb: history.back().map(|p| p.used_gb).unwrap_or(0.0),
                total_gb,
                predicted_used_gb: predicted.max(0.0),
            });
        }

        // Calculate exhaustion time
        let exhaustion_time = if slope > 0.0 && total_gb > 0.0 {
            let current = history.back().map(|p| p.used_gb).unwrap_or(0.0);
            let hours_to_full = (total_gb - current) / slope;
            if hours_to_full > 0.0 && hours_to_full < 8760.0 {
                // Within a year
                Some(Utc::now() + chrono::Duration::hours(hours_to_full as i64))
            } else {
                None
            }
        } else {
            None
        };

        // Recommended capacity
        let recommended_capacity_gb = if exhaustion_time.is_some() {
            Some((total_gb * 0.5) as u64) // Add 50% more capacity
        } else {
            None
        };

        // Confidence interval (simplified)
        let residuals: Vec<f64> = history
            .iter()
            .enumerate()
            .map(|(i, p)| p.used_gb - (slope * i as f64 + intercept))
            .collect();
        let residual_std = std_dev(&residuals);

        CapacityForecast {
            forecasts,
            exhaustion_time,
            recommended_capacity_gb,
            confidence_interval: (y_mean - 2.0 * residual_std, y_mean + 2.0 * residual_std),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_capacity_forecaster() {
        let config = AIOpsConfig::default();
        let forecaster = CapacityForecaster::new(config);

        // Add historical data with growth
        for i in 0..100 {
            forecaster.record(100.0 + i as f64, 1000.0).await;
        }

        let forecast = forecaster.forecast().await;
        assert!(!forecast.forecasts.is_empty());
    }
}
