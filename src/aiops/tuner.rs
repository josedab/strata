//! Automatic parameter tuning.

use super::config::AIOpsConfig;
use crate::error::{Result, StrataError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// A tunable system parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunableParameter {
    pub name: String,
    pub description: String,
    pub current_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub default_value: f64,
    pub unit: String,
    pub impact_metrics: Vec<String>,
}

/// A tuning experiment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningExperiment {
    pub id: String,
    pub parameter: String,
    pub old_value: f64,
    pub new_value: f64,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub baseline_metrics: HashMap<String, f64>,
    pub experiment_metrics: HashMap<String, f64>,
    pub improvement_percent: Option<f64>,
    pub status: ExperimentStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExperimentStatus {
    Running,
    Completed,
    RolledBack,
}

/// Automatically tunes system parameters
pub struct AutoTuner {
    #[allow(dead_code)]
    config: AIOpsConfig,
    parameters: RwLock<HashMap<String, TunableParameter>>,
    experiments: RwLock<Vec<TuningExperiment>>,
}

impl AutoTuner {
    pub fn new(config: AIOpsConfig) -> Self {
        let mut parameters = HashMap::new();

        // Define tunable parameters
        parameters.insert(
            "cache_size_mb".to_string(),
            TunableParameter {
                name: "cache_size_mb".to_string(),
                description: "Size of read cache in MB".to_string(),
                current_value: 1024.0,
                min_value: 256.0,
                max_value: 16384.0,
                default_value: 1024.0,
                unit: "MB".to_string(),
                impact_metrics: vec!["read_latency_p99".to_string(), "cache_hit_rate".to_string()],
            },
        );

        parameters.insert(
            "write_buffer_size_mb".to_string(),
            TunableParameter {
                name: "write_buffer_size_mb".to_string(),
                description: "Size of write buffer in MB".to_string(),
                current_value: 256.0,
                min_value: 64.0,
                max_value: 2048.0,
                default_value: 256.0,
                unit: "MB".to_string(),
                impact_metrics: vec!["write_latency_p99".to_string(), "write_throughput".to_string()],
            },
        );

        parameters.insert(
            "prefetch_size_kb".to_string(),
            TunableParameter {
                name: "prefetch_size_kb".to_string(),
                description: "Prefetch read size in KB".to_string(),
                current_value: 256.0,
                min_value: 0.0,
                max_value: 4096.0,
                default_value: 256.0,
                unit: "KB".to_string(),
                impact_metrics: vec![
                    "sequential_read_throughput".to_string(),
                    "prefetch_hit_rate".to_string(),
                ],
            },
        );

        parameters.insert(
            "compaction_threads".to_string(),
            TunableParameter {
                name: "compaction_threads".to_string(),
                description: "Number of compaction threads".to_string(),
                current_value: 4.0,
                min_value: 1.0,
                max_value: 16.0,
                default_value: 4.0,
                unit: "threads".to_string(),
                impact_metrics: vec!["compaction_throughput".to_string(), "cpu_utilization".to_string()],
            },
        );

        Self {
            config,
            parameters: RwLock::new(parameters),
            experiments: RwLock::new(Vec::new()),
        }
    }

    /// Get all tunable parameters
    pub async fn parameters(&self) -> Vec<TunableParameter> {
        self.parameters.read().await.values().cloned().collect()
    }

    /// Get a specific parameter
    pub async fn get_parameter(&self, name: &str) -> Option<TunableParameter> {
        self.parameters.read().await.get(name).cloned()
    }

    /// Suggest parameter value based on workload
    pub async fn suggest(&self, parameter: &str, metrics: &HashMap<String, f64>) -> Option<f64> {
        let params = self.parameters.read().await;
        let param = params.get(parameter)?;

        // Simple heuristic-based suggestions
        match parameter {
            "cache_size_mb" => {
                let hit_rate = metrics.get("cache_hit_rate").copied().unwrap_or(0.5);
                let memory_available = metrics.get("memory_available_mb").copied().unwrap_or(8192.0);

                if hit_rate < 0.7 && memory_available > param.current_value * 2.0 {
                    Some((param.current_value * 1.5).min(param.max_value))
                } else if hit_rate > 0.95 && param.current_value > param.min_value * 2.0 {
                    Some((param.current_value * 0.75).max(param.min_value))
                } else {
                    None
                }
            }
            "prefetch_size_kb" => {
                let sequential_ratio = metrics.get("sequential_read_ratio").copied().unwrap_or(0.3);

                if sequential_ratio > 0.7 {
                    Some((param.current_value * 2.0).min(param.max_value))
                } else if sequential_ratio < 0.2 {
                    Some((param.current_value * 0.5).max(param.min_value))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Start a tuning experiment
    pub async fn start_experiment(
        &self,
        parameter: &str,
        new_value: f64,
        baseline_metrics: HashMap<String, f64>,
    ) -> Result<String> {
        let mut params = self.parameters.write().await;
        let param = params
            .get_mut(parameter)
            .ok_or_else(|| StrataError::NotFound(format!("Parameter {} not found", parameter)))?;

        if new_value < param.min_value || new_value > param.max_value {
            return Err(StrataError::InvalidArgument(format!(
                "Value {} out of range [{}, {}]",
                new_value, param.min_value, param.max_value
            )));
        }

        let old_value = param.current_value;
        param.current_value = new_value;

        let experiment = TuningExperiment {
            id: uuid::Uuid::new_v4().to_string(),
            parameter: parameter.to_string(),
            old_value,
            new_value,
            started_at: Utc::now(),
            ended_at: None,
            baseline_metrics,
            experiment_metrics: HashMap::new(),
            improvement_percent: None,
            status: ExperimentStatus::Running,
        };

        let id = experiment.id.clone();
        self.experiments.write().await.push(experiment);

        info!(
            parameter = parameter,
            old = old_value,
            new = new_value,
            "Started tuning experiment"
        );

        Ok(id)
    }

    /// Complete an experiment with results
    pub async fn complete_experiment(
        &self,
        experiment_id: &str,
        experiment_metrics: HashMap<String, f64>,
    ) -> Result<f64> {
        let mut experiments = self.experiments.write().await;
        let experiment = experiments
            .iter_mut()
            .find(|e| e.id == experiment_id)
            .ok_or_else(|| StrataError::NotFound(format!("Experiment {} not found", experiment_id)))?;

        // Calculate improvement
        let mut total_improvement = 0.0;
        let mut count = 0;

        for (metric, new_value) in &experiment_metrics {
            if let Some(old_value) = experiment.baseline_metrics.get(metric) {
                if *old_value > 0.0 {
                    // For latency, lower is better; for throughput, higher is better
                    let improvement = if metric.contains("latency") {
                        (old_value - new_value) / old_value * 100.0
                    } else {
                        (new_value - old_value) / old_value * 100.0
                    };
                    total_improvement += improvement;
                    count += 1;
                }
            }
        }

        let avg_improvement = if count > 0 {
            total_improvement / count as f64
        } else {
            0.0
        };

        experiment.experiment_metrics = experiment_metrics;
        experiment.improvement_percent = Some(avg_improvement);
        experiment.ended_at = Some(Utc::now());
        experiment.status = ExperimentStatus::Completed;

        info!(
            experiment = experiment_id,
            improvement = avg_improvement,
            "Completed tuning experiment"
        );

        Ok(avg_improvement)
    }

    /// Rollback an experiment
    pub async fn rollback_experiment(&self, experiment_id: &str) -> Result<()> {
        let mut experiments = self.experiments.write().await;
        let experiment = experiments
            .iter_mut()
            .find(|e| e.id == experiment_id)
            .ok_or_else(|| StrataError::NotFound(format!("Experiment {} not found", experiment_id)))?;

        // Restore old value
        let mut params = self.parameters.write().await;
        if let Some(param) = params.get_mut(&experiment.parameter) {
            param.current_value = experiment.old_value;
        }

        experiment.status = ExperimentStatus::RolledBack;
        experiment.ended_at = Some(Utc::now());

        warn!(experiment = experiment_id, "Rolled back tuning experiment");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auto_tuner() {
        let config = AIOpsConfig::default();
        let tuner = AutoTuner::new(config);

        let params = tuner.parameters().await;
        assert!(!params.is_empty());

        // Start experiment
        let baseline = HashMap::from([("read_latency_p99".to_string(), 10.0)]);
        let exp_id = tuner
            .start_experiment("cache_size_mb", 2048.0, baseline)
            .await
            .unwrap();

        // Complete experiment
        let results = HashMap::from([("read_latency_p99".to_string(), 8.0)]);
        let improvement = tuner.complete_experiment(&exp_id, results).await.unwrap();
        assert!(improvement > 0.0);
    }
}
