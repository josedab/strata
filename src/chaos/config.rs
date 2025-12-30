//! Configuration for chaos engineering.

use crate::types::NodeId;
use chrono::{Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Chaos engineering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    /// Whether chaos testing is enabled
    pub enabled: bool,
    /// Maximum blast radius (percentage of cluster)
    pub max_blast_radius_percent: u8,
    /// Safety timeout for all experiments
    pub safety_timeout: Duration,
    /// Require manual approval for destructive faults
    pub require_approval: bool,
    /// Excluded nodes (never inject faults)
    pub excluded_nodes: Vec<NodeId>,
    /// Schedule for automated chaos runs
    pub schedule: Option<ChaosSchedule>,
    /// Metrics endpoint for steady state checks
    pub metrics_endpoint: Option<String>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for safety
            max_blast_radius_percent: 33,
            safety_timeout: Duration::from_secs(300),
            require_approval: true,
            excluded_nodes: Vec::new(),
            schedule: None,
            metrics_endpoint: None,
        }
    }
}

impl ChaosConfig {
    /// Development configuration (more permissive)
    pub fn development() -> Self {
        Self {
            enabled: true,
            max_blast_radius_percent: 50,
            safety_timeout: Duration::from_secs(60),
            require_approval: false,
            excluded_nodes: Vec::new(),
            schedule: None,
            metrics_endpoint: None,
        }
    }

    /// Production configuration (conservative)
    pub fn production() -> Self {
        Self {
            enabled: true,
            max_blast_radius_percent: 10,
            safety_timeout: Duration::from_secs(120),
            require_approval: true,
            excluded_nodes: Vec::new(),
            schedule: Some(ChaosSchedule::business_hours()),
            metrics_endpoint: Some("http://localhost:9090".to_string()),
        }
    }
}

/// Schedule for automated chaos runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosSchedule {
    /// Days of week (0 = Sunday, 6 = Saturday)
    pub days: Vec<u8>,
    /// Start hour (24-hour format)
    pub start_hour: u8,
    /// End hour (24-hour format)
    pub end_hour: u8,
    /// Timezone
    pub timezone: String,
}

impl ChaosSchedule {
    /// Business hours schedule (Mon-Fri, 9am-5pm)
    pub fn business_hours() -> Self {
        Self {
            days: vec![1, 2, 3, 4, 5],
            start_hour: 9,
            end_hour: 17,
            timezone: "UTC".to_string(),
        }
    }

    /// Check if current time is within schedule
    pub fn is_active(&self) -> bool {
        let now = Utc::now();
        let weekday = now.weekday().num_days_from_sunday() as u8;
        let hour = now.hour() as u8;

        self.days.contains(&weekday) && hour >= self.start_hour && hour < self.end_hour
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ChaosConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_blast_radius_percent, 33);
        assert!(config.require_approval);
    }

    #[test]
    fn test_config_development() {
        let config = ChaosConfig::development();
        assert!(config.enabled);
        assert!(!config.require_approval);
    }

    #[test]
    fn test_config_production() {
        let config = ChaosConfig::production();
        assert!(config.enabled);
        assert!(config.require_approval);
        assert!(config.schedule.is_some());
    }

    #[test]
    fn test_schedule_business_hours() {
        let schedule = ChaosSchedule::business_hours();
        assert_eq!(schedule.days, vec![1, 2, 3, 4, 5]);
        assert_eq!(schedule.start_hour, 9);
        assert_eq!(schedule.end_hour, 17);
    }
}
