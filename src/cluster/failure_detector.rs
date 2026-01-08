//! Failure detection with heartbeat monitoring.
//!
//! This module implements a phi-accrual failure detector for detecting
//! data server failures based on heartbeat patterns.

use crate::types::DataServerId;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Heartbeat record for a single server.
#[derive(Debug, Clone)]
struct HeartbeatRecord {
    /// Last heartbeat time.
    last_heartbeat: Instant,
    /// Heartbeat intervals for phi calculation (VecDeque for O(1) pop_front).
    intervals: VecDeque<Duration>,
    /// Maximum intervals to track.
    max_samples: usize,
}

impl HeartbeatRecord {
    fn new(max_samples: usize) -> Self {
        Self {
            last_heartbeat: Instant::now(),
            intervals: VecDeque::with_capacity(max_samples),
            max_samples,
        }
    }

    fn record_heartbeat(&mut self) {
        let now = Instant::now();
        let interval = now.duration_since(self.last_heartbeat);
        self.last_heartbeat = now;

        if self.intervals.len() >= self.max_samples {
            self.intervals.pop_front(); // O(1) instead of O(n) with Vec::remove(0)
        }
        self.intervals.push_back(interval);
    }

    fn mean_interval(&self) -> Duration {
        if self.intervals.is_empty() {
            return Duration::from_secs(1); // Default 1 second
        }
        let total: Duration = self.intervals.iter().sum();
        total / self.intervals.len() as u32
    }

    fn time_since_last(&self) -> Duration {
        self.last_heartbeat.elapsed()
    }

    /// Calculate phi value (accrual failure detector).
    /// Higher phi means more likely the node has failed.
    fn phi(&self) -> f64 {
        let elapsed = self.time_since_last().as_secs_f64();
        let mean = self.mean_interval().as_secs_f64();

        if mean <= 0.0 {
            return 0.0;
        }

        // Simple exponential distribution assumption
        // phi = -log10(probability of arrival)
        // P(T > t) = e^(-t/mean) for exponential
        elapsed / mean
    }
}

/// Configuration for the failure detector.
#[derive(Debug, Clone)]
pub struct FailureDetectorConfig {
    /// Phi threshold for marking a node as failed.
    pub phi_threshold: f64,
    /// Expected heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Maximum samples for phi calculation.
    pub max_samples: usize,
    /// Minimum heartbeats before trusting phi.
    pub min_samples: usize,
    /// Grace period for new nodes.
    pub grace_period: Duration,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            heartbeat_interval: Duration::from_secs(5),
            max_samples: 100,
            min_samples: 3,
            grace_period: Duration::from_secs(30),
        }
    }
}

/// Failure detector using phi-accrual algorithm.
pub struct FailureDetector {
    /// Configuration.
    config: FailureDetectorConfig,
    /// Heartbeat records per server.
    records: Arc<RwLock<HashMap<DataServerId, HeartbeatRecord>>>,
    /// Server registration times.
    registration_times: Arc<RwLock<HashMap<DataServerId, Instant>>>,
}

impl FailureDetector {
    /// Create a new failure detector.
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            config,
            records: Arc::new(RwLock::new(HashMap::new())),
            registration_times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new server.
    pub async fn register(&self, server_id: DataServerId) {
        let mut records = self.records.write().await;
        let mut reg_times = self.registration_times.write().await;

        records.insert(server_id, HeartbeatRecord::new(self.config.max_samples));
        reg_times.insert(server_id, Instant::now());

        info!(server_id, "Server registered with failure detector");
    }

    /// Unregister a server.
    pub async fn unregister(&self, server_id: DataServerId) {
        let mut records = self.records.write().await;
        let mut reg_times = self.registration_times.write().await;

        records.remove(&server_id);
        reg_times.remove(&server_id);

        info!(server_id, "Server unregistered from failure detector");
    }

    /// Record a heartbeat from a server.
    pub async fn heartbeat(&self, server_id: DataServerId) {
        let mut records = self.records.write().await;

        if let Some(record) = records.get_mut(&server_id) {
            record.record_heartbeat();
            debug!(server_id, "Heartbeat recorded");
        } else {
            // Auto-register if not known
            let mut record = HeartbeatRecord::new(self.config.max_samples);
            record.record_heartbeat();
            records.insert(server_id, record);

            drop(records);
            let mut reg_times = self.registration_times.write().await;
            reg_times.insert(server_id, Instant::now());

            info!(server_id, "Server auto-registered via heartbeat");
        }
    }

    /// Check if a server is considered alive.
    pub async fn is_alive(&self, server_id: DataServerId) -> bool {
        let records = self.records.read().await;
        let reg_times = self.registration_times.read().await;

        let record = match records.get(&server_id) {
            Some(r) => r,
            None => return false, // Unknown server
        };

        // Grace period for new servers
        if let Some(&reg_time) = reg_times.get(&server_id) {
            if reg_time.elapsed() < self.config.grace_period {
                return true;
            }
        }

        // Need minimum samples for reliable phi
        if record.intervals.len() < self.config.min_samples {
            return record.time_since_last() < self.config.heartbeat_interval * 3;
        }

        record.phi() < self.config.phi_threshold
    }

    /// Get the phi value for a server.
    pub async fn phi(&self, server_id: DataServerId) -> Option<f64> {
        let records = self.records.read().await;
        records.get(&server_id).map(|r| r.phi())
    }

    /// Get all servers considered failed.
    pub async fn failed_servers(&self) -> Vec<DataServerId> {
        let records = self.records.read().await;
        let reg_times = self.registration_times.read().await;

        let mut failed = Vec::new();

        for (&server_id, record) in records.iter() {
            // Check grace period
            if let Some(&reg_time) = reg_times.get(&server_id) {
                if reg_time.elapsed() < self.config.grace_period {
                    continue;
                }
            }

            if record.intervals.len() >= self.config.min_samples
                && record.phi() >= self.config.phi_threshold
            {
                failed.push(server_id);
            }
        }

        failed
    }

    /// Get all servers considered alive.
    pub async fn alive_servers(&self) -> Vec<DataServerId> {
        let server_ids: Vec<_> = {
            let records = self.records.read().await;
            records.keys().copied().collect()
        };

        let mut alive = Vec::new();
        for server_id in server_ids {
            if self.is_alive(server_id).await {
                alive.push(server_id);
            }
        }

        alive
    }

    /// Get status summary.
    pub async fn status(&self) -> FailureDetectorStatus {
        let records = self.records.read().await;
        let reg_times = self.registration_times.read().await;

        let mut alive = 0;
        let mut suspected = 0;
        let mut failed = 0;

        for (&server_id, record) in records.iter() {
            // Check grace period
            let in_grace = reg_times
                .get(&server_id)
                .map(|t| t.elapsed() < self.config.grace_period)
                .unwrap_or(false);

            if in_grace || record.intervals.len() < self.config.min_samples {
                alive += 1;
            } else {
                let phi = record.phi();
                if phi < self.config.phi_threshold {
                    alive += 1;
                } else if phi < self.config.phi_threshold * 1.5 {
                    suspected += 1;
                } else {
                    failed += 1;
                }
            }
        }

        FailureDetectorStatus {
            total_servers: records.len(),
            alive_count: alive,
            suspected_count: suspected,
            failed_count: failed,
        }
    }
}

/// Status summary from the failure detector.
#[derive(Debug, Clone)]
pub struct FailureDetectorStatus {
    /// Total registered servers.
    pub total_servers: usize,
    /// Servers considered alive.
    pub alive_count: usize,
    /// Servers with elevated phi (suspected).
    pub suspected_count: usize,
    /// Servers with phi above threshold (failed).
    pub failed_count: usize,
}

/// Heartbeat monitor that runs as a background task.
pub struct HeartbeatMonitor {
    /// Failure detector.
    detector: Arc<FailureDetector>,
    /// Check interval.
    check_interval: Duration,
    /// Callback for status changes.
    on_failure: Option<Box<dyn Fn(DataServerId) + Send + Sync>>,
}

impl HeartbeatMonitor {
    /// Create a new heartbeat monitor.
    pub fn new(detector: Arc<FailureDetector>, check_interval: Duration) -> Self {
        Self {
            detector,
            check_interval,
            on_failure: None,
        }
    }

    /// Set a callback for when a server is detected as failed.
    pub fn on_failure<F>(mut self, callback: F) -> Self
    where
        F: Fn(DataServerId) + Send + Sync + 'static,
    {
        self.on_failure = Some(Box::new(callback));
        self
    }

    /// Run the monitor as a background task.
    pub async fn run(self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
        let mut interval = tokio::time::interval(self.check_interval);
        let mut last_failed: Vec<DataServerId> = Vec::new();

        info!("Heartbeat monitor starting");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let failed = self.detector.failed_servers().await;

                    // Check for newly failed servers
                    for &server_id in &failed {
                        if !last_failed.contains(&server_id) {
                            warn!(server_id, "Server detected as failed");
                            if let Some(ref callback) = self.on_failure {
                                callback(server_id);
                            }
                        }
                    }

                    // Check for recovered servers
                    for &server_id in &last_failed {
                        if !failed.contains(&server_id) {
                            info!(server_id, "Server recovered");
                        }
                    }

                    last_failed = failed;

                    let status = self.detector.status().await;
                    debug!(
                        alive = status.alive_count,
                        suspected = status.suspected_count,
                        failed = status.failed_count,
                        "Heartbeat check completed"
                    );
                }
                _ = shutdown_rx.recv() => {
                    info!("Heartbeat monitor shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_server() {
        let detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.register(1).await;

        assert!(detector.is_alive(1).await);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let config = FailureDetectorConfig {
            grace_period: Duration::from_millis(10),
            min_samples: 1,
            ..Default::default()
        };
        let detector = FailureDetector::new(config);

        detector.register(1).await;
        detector.heartbeat(1).await;

        assert!(detector.is_alive(1).await);
    }

    #[tokio::test]
    async fn test_unregister() {
        let detector = FailureDetector::new(FailureDetectorConfig::default());

        detector.register(1).await;
        assert!(detector.is_alive(1).await);

        detector.unregister(1).await;
        assert!(!detector.is_alive(1).await);
    }

    #[tokio::test]
    async fn test_phi_calculation() {
        let config = FailureDetectorConfig {
            grace_period: Duration::from_millis(1),
            min_samples: 1,
            ..Default::default()
        };
        let detector = FailureDetector::new(config);

        detector.register(1).await;

        // Multiple heartbeats
        for _ in 0..5 {
            detector.heartbeat(1).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let phi = detector.phi(1).await;
        assert!(phi.is_some());
        assert!(phi.unwrap() >= 0.0);
    }

    #[tokio::test]
    async fn test_status() {
        let config = FailureDetectorConfig {
            grace_period: Duration::from_millis(1),
            ..Default::default()
        };
        let detector = FailureDetector::new(config);

        detector.register(1).await;
        detector.register(2).await;

        let status = detector.status().await;
        assert_eq!(status.total_servers, 2);
    }

    #[tokio::test]
    async fn test_auto_register() {
        let detector = FailureDetector::new(FailureDetectorConfig::default());

        // Heartbeat from unknown server should auto-register
        detector.heartbeat(42).await;

        assert!(detector.is_alive(42).await);
    }
}
