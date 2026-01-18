//! Alerting framework for Strata.
//!
//! Provides rule-based alerting with configurable thresholds,
//! severities, and notification channels.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Alert severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational alert, no action required.
    Info,
    /// Warning alert, should be investigated.
    Warning,
    /// Critical alert, requires immediate attention.
    Critical,
    /// Emergency alert, system is down or severely impacted.
    Emergency,
}

impl AlertSeverity {
    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
            AlertSeverity::Emergency => "emergency",
        }
    }
}

/// Current state of an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertState {
    /// Alert condition is not met.
    Inactive,
    /// Alert condition is met but waiting for duration.
    Pending,
    /// Alert is firing.
    Firing,
    /// Alert was firing but is now resolved.
    Resolved,
}

/// Threshold comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThresholdOperator {
    /// Value is greater than threshold.
    GreaterThan,
    /// Value is greater than or equal to threshold.
    GreaterThanOrEqual,
    /// Value is less than threshold.
    LessThan,
    /// Value is less than or equal to threshold.
    LessThanOrEqual,
    /// Value equals threshold.
    Equal,
    /// Value does not equal threshold.
    NotEqual,
}

impl ThresholdOperator {
    /// Evaluate the condition.
    pub fn evaluate(&self, value: f64, threshold: f64) -> bool {
        match self {
            ThresholdOperator::GreaterThan => value > threshold,
            ThresholdOperator::GreaterThanOrEqual => value >= threshold,
            ThresholdOperator::LessThan => value < threshold,
            ThresholdOperator::LessThanOrEqual => value <= threshold,
            ThresholdOperator::Equal => (value - threshold).abs() < f64::EPSILON,
            ThresholdOperator::NotEqual => (value - threshold).abs() >= f64::EPSILON,
        }
    }
}

/// Alert threshold configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThreshold {
    /// The threshold value.
    pub value: f64,
    /// The comparison operator.
    pub operator: ThresholdOperator,
    /// How long the condition must be true before firing.
    pub for_duration: Duration,
}

impl AlertThreshold {
    /// Create a new threshold.
    pub fn new(value: f64, operator: ThresholdOperator) -> Self {
        Self {
            value,
            operator,
            for_duration: Duration::from_secs(0),
        }
    }

    /// Set the duration the condition must be true.
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.for_duration = duration;
        self
    }

    /// Check if the threshold is exceeded.
    pub fn is_exceeded(&self, value: f64) -> bool {
        self.operator.evaluate(value, self.value)
    }
}

/// An alert rule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Unique identifier for the rule.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Description of what this alert means.
    pub description: String,
    /// The metric to monitor.
    pub metric: String,
    /// Optional labels to filter the metric.
    pub labels: HashMap<String, String>,
    /// The threshold configuration.
    pub threshold: AlertThreshold,
    /// Alert severity.
    pub severity: AlertSeverity,
    /// Additional annotations for the alert.
    pub annotations: HashMap<String, String>,
    /// Whether this rule is enabled.
    pub enabled: bool,
}

impl AlertRule {
    /// Create a new alert rule.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        metric: impl Into<String>,
        threshold: AlertThreshold,
        severity: AlertSeverity,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            metric: metric.into(),
            labels: HashMap::new(),
            threshold,
            severity,
            annotations: HashMap::new(),
            enabled: true,
        }
    }

    /// Set the description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Add a label filter.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add an annotation.
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Disable the rule.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

/// An active alert instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// The rule that triggered this alert.
    pub rule_id: String,
    /// Alert name.
    pub name: String,
    /// Current state.
    pub state: AlertState,
    /// Severity level.
    pub severity: AlertSeverity,
    /// Current metric value.
    pub value: f64,
    /// Threshold that was exceeded.
    pub threshold: f64,
    /// When the alert started pending.
    pub started_at: Option<SystemTime>,
    /// When the alert started firing.
    pub fired_at: Option<SystemTime>,
    /// When the alert was resolved.
    pub resolved_at: Option<SystemTime>,
    /// Labels from the metric.
    pub labels: HashMap<String, String>,
    /// Annotations from the rule.
    pub annotations: HashMap<String, String>,
}

/// Alert notification channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// Log the alert.
    Log,
    /// Send to a webhook URL.
    Webhook { url: String, headers: HashMap<String, String> },
    /// Send via email.
    Email { to: Vec<String>, smtp_server: String },
    /// Send to Slack.
    Slack { webhook_url: String, channel: String },
    /// Send to PagerDuty.
    PagerDuty { routing_key: String },
}

/// Alert manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Evaluation interval.
    pub evaluation_interval: Duration,
    /// How long to wait before re-sending a firing alert.
    pub repeat_interval: Duration,
    /// How long to wait after resolution before clearing.
    pub resolve_timeout: Duration,
    /// Maximum alerts to keep in history.
    pub max_history: usize,
    /// Notification channels.
    pub channels: Vec<NotificationChannel>,
    /// Group alerts by these labels.
    pub group_by: Vec<String>,
    /// Inhibition rules (suppress alerts based on other alerts).
    pub inhibit_rules: Vec<InhibitRule>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            evaluation_interval: Duration::from_secs(15),
            repeat_interval: Duration::from_secs(3600),
            resolve_timeout: Duration::from_secs(300),
            max_history: 1000,
            channels: vec![NotificationChannel::Log],
            group_by: vec!["severity".to_string()],
            inhibit_rules: Vec::new(),
        }
    }
}

/// Rule for inhibiting alerts based on other alerts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InhibitRule {
    /// Source alert matcher (the alert that inhibits).
    pub source_match: HashMap<String, String>,
    /// Target alert matcher (the alert to inhibit).
    pub target_match: HashMap<String, String>,
    /// Labels that must match between source and target.
    pub equal: Vec<String>,
}

/// Alert manager for evaluating rules and sending notifications.
pub struct AlertManager {
    config: AlertConfig,
    /// Registered alert rules.
    rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    /// Current alert states.
    states: Arc<RwLock<HashMap<String, AlertState>>>,
    /// When each rule started pending.
    pending_since: Arc<RwLock<HashMap<String, Instant>>>,
    /// Active alerts.
    active_alerts: Arc<RwLock<HashMap<String, Alert>>>,
    /// Alert history.
    history: Arc<RwLock<Vec<Alert>>>,
    /// Metric value provider.
    metric_provider: Arc<dyn MetricProvider>,
    /// Notification sender.
    notification_tx: mpsc::Sender<Alert>,
}

/// Trait for providing metric values.
pub trait MetricProvider: Send + Sync {
    /// Get the current value of a metric.
    fn get_metric(&self, name: &str, labels: &HashMap<String, String>) -> Option<f64>;
}

/// Default metric provider that always returns None.
struct NoopMetricProvider;

impl MetricProvider for NoopMetricProvider {
    fn get_metric(&self, _name: &str, _labels: &HashMap<String, String>) -> Option<f64> {
        None
    }
}

impl AlertManager {
    /// Create a new alert manager.
    pub fn new(config: AlertConfig) -> Self {
        let (tx, rx) = mpsc::channel(100);

        let manager = Self {
            config: config.clone(),
            rules: Arc::new(RwLock::new(HashMap::new())),
            states: Arc::new(RwLock::new(HashMap::new())),
            pending_since: Arc::new(RwLock::new(HashMap::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            metric_provider: Arc::new(NoopMetricProvider),
            notification_tx: tx,
        };

        // Start notification handler
        let channels = config.channels.clone();
        tokio::spawn(Self::notification_handler(rx, channels));

        manager
    }

    /// Set a custom metric provider.
    pub fn with_metric_provider(mut self, provider: Arc<dyn MetricProvider>) -> Self {
        self.metric_provider = provider;
        self
    }

    /// Register an alert rule.
    pub async fn register_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        info!(rule_id = %rule.id, name = %rule.name, "Registered alert rule");
        rules.insert(rule.id.clone(), rule);
    }

    /// Remove an alert rule.
    pub async fn remove_rule(&self, rule_id: &str) {
        let mut rules = self.rules.write().await;
        if rules.remove(rule_id).is_some() {
            info!(rule_id, "Removed alert rule");
        }
    }

    /// Get all registered rules.
    pub async fn rules(&self) -> Vec<AlertRule> {
        self.rules.read().await.values().cloned().collect()
    }

    /// Get all active alerts.
    pub async fn active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.read().await.values().cloned().collect()
    }

    /// Get alert history.
    pub async fn history(&self) -> Vec<Alert> {
        self.history.read().await.clone()
    }

    /// Evaluate all alert rules.
    pub async fn evaluate(&self) {
        let rules = self.rules.read().await.clone();

        for (rule_id, rule) in rules {
            if !rule.enabled {
                continue;
            }

            self.evaluate_rule(&rule_id, &rule).await;
        }
    }

    /// Evaluate a single rule.
    async fn evaluate_rule(&self, rule_id: &str, rule: &AlertRule) {
        let value = match self.metric_provider.get_metric(&rule.metric, &rule.labels) {
            Some(v) => v,
            None => {
                debug!(rule_id, metric = %rule.metric, "Metric not found");
                return;
            }
        };

        let exceeded = rule.threshold.is_exceeded(value);
        let mut states = self.states.write().await;
        let current_state = states.get(rule_id).copied().unwrap_or(AlertState::Inactive);

        let new_state = match (current_state, exceeded) {
            (AlertState::Inactive, true) => {
                if rule.threshold.for_duration.is_zero() {
                    AlertState::Firing
                } else {
                    // Start pending
                    let mut pending = self.pending_since.write().await;
                    pending.insert(rule_id.to_string(), Instant::now());
                    AlertState::Pending
                }
            }
            (AlertState::Pending, true) => {
                let pending = self.pending_since.read().await;
                if let Some(since) = pending.get(rule_id) {
                    if since.elapsed() >= rule.threshold.for_duration {
                        AlertState::Firing
                    } else {
                        AlertState::Pending
                    }
                } else {
                    AlertState::Pending
                }
            }
            (AlertState::Pending, false) => {
                // Condition no longer met, clear pending
                let mut pending = self.pending_since.write().await;
                pending.remove(rule_id);
                AlertState::Inactive
            }
            (AlertState::Firing, true) => AlertState::Firing,
            (AlertState::Firing, false) => AlertState::Resolved,
            (AlertState::Resolved, true) => AlertState::Firing,
            (AlertState::Resolved, false) => AlertState::Inactive,
            (AlertState::Inactive, false) => AlertState::Inactive,
        };

        // Handle state transitions
        if new_state != current_state {
            states.insert(rule_id.to_string(), new_state);
            self.handle_state_transition(rule_id, rule, current_state, new_state, value)
                .await;
        }
    }

    /// Handle an alert state transition.
    async fn handle_state_transition(
        &self,
        rule_id: &str,
        rule: &AlertRule,
        _old_state: AlertState,
        new_state: AlertState,
        value: f64,
    ) {
        let now = SystemTime::now();

        match new_state {
            AlertState::Firing => {
                let alert = Alert {
                    rule_id: rule_id.to_string(),
                    name: rule.name.clone(),
                    state: AlertState::Firing,
                    severity: rule.severity,
                    value,
                    threshold: rule.threshold.value,
                    started_at: Some(now),
                    fired_at: Some(now),
                    resolved_at: None,
                    labels: rule.labels.clone(),
                    annotations: rule.annotations.clone(),
                };

                warn!(
                    rule_id,
                    name = %rule.name,
                    severity = %rule.severity.as_str(),
                    value,
                    threshold = rule.threshold.value,
                    "Alert firing"
                );

                self.active_alerts
                    .write()
                    .await
                    .insert(rule_id.to_string(), alert.clone());

                let _ = self.notification_tx.send(alert).await;
            }
            AlertState::Resolved => {
                let mut active = self.active_alerts.write().await;
                if let Some(mut alert) = active.remove(rule_id) {
                    alert.state = AlertState::Resolved;
                    alert.resolved_at = Some(now);

                    info!(
                        rule_id,
                        name = %rule.name,
                        "Alert resolved"
                    );

                    // Add to history
                    let mut history = self.history.write().await;
                    history.push(alert.clone());
                    if history.len() > self.config.max_history {
                        history.remove(0);
                    }

                    let _ = self.notification_tx.send(alert).await;
                }
            }
            _ => {}
        }
    }

    /// Background handler for sending notifications.
    async fn notification_handler(
        mut rx: mpsc::Receiver<Alert>,
        channels: Vec<NotificationChannel>,
    ) {
        while let Some(alert) = rx.recv().await {
            for channel in &channels {
                if let Err(e) = Self::send_notification(channel, &alert).await {
                    error!(channel = ?channel, error = %e, "Failed to send notification");
                }
            }
        }
    }

    /// Send a notification to a channel.
    async fn send_notification(
        channel: &NotificationChannel,
        alert: &Alert,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match channel {
            NotificationChannel::Log => {
                match alert.state {
                    AlertState::Firing => {
                        warn!(
                            rule_id = %alert.rule_id,
                            name = %alert.name,
                            severity = %alert.severity.as_str(),
                            value = %alert.value,
                            threshold = %alert.threshold,
                            "ALERT FIRING"
                        );
                    }
                    AlertState::Resolved => {
                        info!(
                            rule_id = %alert.rule_id,
                            name = %alert.name,
                            "ALERT RESOLVED"
                        );
                    }
                    _ => {}
                }
                Ok(())
            }
            NotificationChannel::Webhook { url, headers: _ } => {
                debug!(url, rule_id = %alert.rule_id, "Would send webhook notification");
                // In production, use reqwest to send HTTP POST
                Ok(())
            }
            NotificationChannel::Email { to, smtp_server: _ } => {
                debug!(to = ?to, rule_id = %alert.rule_id, "Would send email notification");
                Ok(())
            }
            NotificationChannel::Slack { webhook_url: _, channel } => {
                debug!(channel, rule_id = %alert.rule_id, "Would send Slack notification");
                Ok(())
            }
            NotificationChannel::PagerDuty { routing_key: _ } => {
                debug!(rule_id = %alert.rule_id, "Would send PagerDuty notification");
                Ok(())
            }
        }
    }

    /// Run the alert evaluation loop.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut interval = tokio::time::interval(self.config.evaluation_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.evaluate().await;
                }
                _ = shutdown.recv() => {
                    info!("Alert manager shutting down");
                    break;
                }
            }
        }
    }
}

/// Predefined alert rules for Strata.
pub mod predefined {
    use super::*;

    /// High read latency alert.
    pub fn high_read_latency(threshold_ms: f64) -> AlertRule {
        AlertRule::new(
            "strata_high_read_latency",
            "High Read Latency",
            "strata_data_read_duration_seconds",
            AlertThreshold::new(threshold_ms / 1000.0, ThresholdOperator::GreaterThan)
                .with_duration(Duration::from_secs(60)),
            AlertSeverity::Warning,
        )
        .with_description("Read latency is above acceptable threshold")
        .with_annotation("runbook", "https://docs.strata.io/runbooks/high-read-latency")
    }

    /// High write latency alert.
    pub fn high_write_latency(threshold_ms: f64) -> AlertRule {
        AlertRule::new(
            "strata_high_write_latency",
            "High Write Latency",
            "strata_data_write_duration_seconds",
            AlertThreshold::new(threshold_ms / 1000.0, ThresholdOperator::GreaterThan)
                .with_duration(Duration::from_secs(60)),
            AlertSeverity::Warning,
        )
        .with_description("Write latency is above acceptable threshold")
    }

    /// Cluster nodes down alert.
    pub fn cluster_nodes_down(min_online: usize) -> AlertRule {
        AlertRule::new(
            "strata_cluster_nodes_down",
            "Cluster Nodes Down",
            "strata_cluster_nodes_online",
            AlertThreshold::new(min_online as f64, ThresholdOperator::LessThan)
                .with_duration(Duration::from_secs(30)),
            AlertSeverity::Critical,
        )
        .with_description("Number of online cluster nodes is below minimum")
    }

    /// Raft leader missing alert.
    pub fn raft_no_leader() -> AlertRule {
        AlertRule::new(
            "strata_raft_no_leader",
            "Raft No Leader",
            "strata_raft_has_leader",
            AlertThreshold::new(1.0, ThresholdOperator::LessThan)
                .with_duration(Duration::from_secs(10)),
            AlertSeverity::Critical,
        )
        .with_description("Raft cluster has no leader")
    }

    /// High error rate alert.
    pub fn high_error_rate(threshold_percent: f64) -> AlertRule {
        AlertRule::new(
            "strata_high_error_rate",
            "High Error Rate",
            "strata_error_rate_percent",
            AlertThreshold::new(threshold_percent, ThresholdOperator::GreaterThan)
                .with_duration(Duration::from_secs(120)),
            AlertSeverity::Warning,
        )
        .with_description("Error rate is above acceptable threshold")
    }

    /// Disk space low alert.
    pub fn disk_space_low(threshold_percent: f64) -> AlertRule {
        AlertRule::new(
            "strata_disk_space_low",
            "Disk Space Low",
            "strata_cluster_used_percent",
            AlertThreshold::new(threshold_percent, ThresholdOperator::GreaterThan)
                .with_duration(Duration::from_secs(300)),
            AlertSeverity::Warning,
        )
        .with_description("Cluster disk usage is above threshold")
    }

    /// Disk space critical alert.
    pub fn disk_space_critical(threshold_percent: f64) -> AlertRule {
        AlertRule::new(
            "strata_disk_space_critical",
            "Disk Space Critical",
            "strata_cluster_used_percent",
            AlertThreshold::new(threshold_percent, ThresholdOperator::GreaterThan)
                .with_duration(Duration::from_secs(60)),
            AlertSeverity::Critical,
        )
        .with_description("Cluster disk usage is critically high")
    }

    /// Get all predefined rules with default thresholds.
    pub fn all_default_rules() -> Vec<AlertRule> {
        vec![
            high_read_latency(100.0),      // 100ms
            high_write_latency(200.0),     // 200ms
            cluster_nodes_down(2),         // Min 2 nodes
            raft_no_leader(),
            high_error_rate(5.0),          // 5%
            disk_space_low(80.0),          // 80%
            disk_space_critical(95.0),     // 95%
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold_operators() {
        assert!(ThresholdOperator::GreaterThan.evaluate(10.0, 5.0));
        assert!(!ThresholdOperator::GreaterThan.evaluate(5.0, 10.0));
        assert!(ThresholdOperator::LessThan.evaluate(5.0, 10.0));
        assert!(ThresholdOperator::Equal.evaluate(5.0, 5.0));
        assert!(ThresholdOperator::NotEqual.evaluate(5.0, 10.0));
    }

    #[test]
    fn test_alert_threshold() {
        let threshold = AlertThreshold::new(100.0, ThresholdOperator::GreaterThan)
            .with_duration(Duration::from_secs(60));

        assert!(threshold.is_exceeded(150.0));
        assert!(!threshold.is_exceeded(50.0));
        assert_eq!(threshold.for_duration, Duration::from_secs(60));
    }

    #[test]
    fn test_alert_rule_builder() {
        let rule = AlertRule::new(
            "test_rule",
            "Test Rule",
            "test_metric",
            AlertThreshold::new(10.0, ThresholdOperator::GreaterThan),
            AlertSeverity::Warning,
        )
        .with_description("A test rule")
        .with_label("env", "test")
        .with_annotation("runbook", "https://example.com");

        assert_eq!(rule.id, "test_rule");
        assert_eq!(rule.severity, AlertSeverity::Warning);
        assert_eq!(rule.labels.get("env"), Some(&"test".to_string()));
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Emergency > AlertSeverity::Critical);
        assert!(AlertSeverity::Critical > AlertSeverity::Warning);
        assert!(AlertSeverity::Warning > AlertSeverity::Info);
    }

    #[test]
    fn test_predefined_rules() {
        let rules = predefined::all_default_rules();
        assert!(!rules.is_empty());

        let read_latency = predefined::high_read_latency(50.0);
        assert_eq!(read_latency.severity, AlertSeverity::Warning);
    }

    #[tokio::test]
    async fn test_alert_manager_register_rule() {
        let manager = AlertManager::new(AlertConfig::default());

        let rule = AlertRule::new(
            "test",
            "Test",
            "metric",
            AlertThreshold::new(10.0, ThresholdOperator::GreaterThan),
            AlertSeverity::Warning,
        );

        manager.register_rule(rule).await;

        let rules = manager.rules().await;
        assert_eq!(rules.len(), 1);
    }
}
