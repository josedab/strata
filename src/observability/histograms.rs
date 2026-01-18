//! Latency histogram metrics for Strata.
//!
//! Provides histogram-based latency tracking for various operation types,
//! enabling p50, p95, p99 percentile calculations.

use metrics::histogram;
use std::time::{Duration, Instant};

/// Default histogram buckets for latency measurements (in seconds).
/// Covers microseconds to seconds range for diverse operation types.
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0001,  // 100µs
    0.0005,  // 500µs
    0.001,   // 1ms
    0.005,   // 5ms
    0.01,    // 10ms
    0.025,   // 25ms
    0.05,    // 50ms
    0.1,     // 100ms
    0.25,    // 250ms
    0.5,     // 500ms
    1.0,     // 1s
    2.5,     // 2.5s
    5.0,     // 5s
    10.0,    // 10s
];

/// Latency statistics for a specific operation type.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    /// Total number of observations.
    pub count: u64,
    /// Sum of all latencies in seconds.
    pub sum: f64,
    /// Minimum latency observed.
    pub min: Option<f64>,
    /// Maximum latency observed.
    pub max: Option<f64>,
}

impl LatencyStats {
    /// Calculate average latency.
    pub fn avg(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }
}

/// Record metadata operation latency.
pub fn record_metadata_latency(op_type: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_metadata_op_duration_seconds",
        "operation" => op_type.to_string()
    )
    .record(seconds);
}

/// Record data read operation latency.
pub fn record_data_read_latency(duration: Duration, bytes: u64) {
    let seconds = duration.as_secs_f64();
    histogram!("strata_data_read_duration_seconds").record(seconds);

    // Also record throughput histogram (bytes per second)
    if seconds > 0.0 {
        let throughput = bytes as f64 / seconds;
        histogram!("strata_data_read_throughput_bytes_per_second").record(throughput);
    }
}

/// Record data write operation latency.
pub fn record_data_write_latency(duration: Duration, bytes: u64) {
    let seconds = duration.as_secs_f64();
    histogram!("strata_data_write_duration_seconds").record(seconds);

    // Also record throughput histogram (bytes per second)
    if seconds > 0.0 {
        let throughput = bytes as f64 / seconds;
        histogram!("strata_data_write_throughput_bytes_per_second").record(throughput);
    }
}

/// Record Raft operation latency.
pub fn record_raft_latency(op_type: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_raft_op_duration_seconds",
        "operation" => op_type.to_string()
    )
    .record(seconds);
}

/// Record S3 gateway request latency.
pub fn record_s3_latency(method: &str, status: u16, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_s3_request_duration_seconds",
        "method" => method.to_string(),
        "status" => status.to_string()
    )
    .record(seconds);
}

/// Record chunk storage operation latency.
pub fn record_chunk_latency(op_type: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_chunk_op_duration_seconds",
        "operation" => op_type.to_string()
    )
    .record(seconds);
}

/// Record cache operation latency.
pub fn record_cache_latency(op_type: &str, hit: bool, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_cache_op_duration_seconds",
        "operation" => op_type.to_string(),
        "hit" => hit.to_string()
    )
    .record(seconds);
}

/// Record erasure coding operation latency.
pub fn record_erasure_latency(op_type: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_erasure_op_duration_seconds",
        "operation" => op_type.to_string()
    )
    .record(seconds);
}

/// Record cluster coordination latency.
pub fn record_coordination_latency(op_type: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    histogram!(
        "strata_coordination_op_duration_seconds",
        "operation" => op_type.to_string()
    )
    .record(seconds);
}

/// A latency timer that automatically records when dropped.
pub struct LatencyTimer {
    start: Instant,
    metric_name: &'static str,
    labels: Vec<(&'static str, String)>,
}

impl LatencyTimer {
    /// Create a new latency timer.
    pub fn new(metric_name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric_name,
            labels: Vec::new(),
        }
    }

    /// Add a label to the timer.
    pub fn with_label(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.labels.push((key, value.into()));
        self
    }

    /// Get elapsed time without recording.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Manually record and consume the timer.
    pub fn record(self) {
        // Drop will handle recording
        drop(self);
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let seconds = self.start.elapsed().as_secs_f64();

        // Build the histogram with labels
        match self.labels.len() {
            0 => {
                histogram!(self.metric_name).record(seconds);
            }
            1 => {
                histogram!(
                    self.metric_name,
                    self.labels[0].0 => self.labels[0].1.clone()
                )
                .record(seconds);
            }
            2 => {
                histogram!(
                    self.metric_name,
                    self.labels[0].0 => self.labels[0].1.clone(),
                    self.labels[1].0 => self.labels[1].1.clone()
                )
                .record(seconds);
            }
            _ => {
                // For more labels, just use the first two
                histogram!(
                    self.metric_name,
                    self.labels[0].0 => self.labels[0].1.clone(),
                    self.labels[1].0 => self.labels[1].1.clone()
                )
                .record(seconds);
            }
        }
    }
}

/// Create a timer for metadata operations.
pub fn metadata_timer(op_type: &str) -> LatencyTimer {
    LatencyTimer::new("strata_metadata_op_duration_seconds")
        .with_label("operation", op_type)
}

/// Create a timer for data read operations.
pub fn data_read_timer() -> LatencyTimer {
    LatencyTimer::new("strata_data_read_duration_seconds")
}

/// Create a timer for data write operations.
pub fn data_write_timer() -> LatencyTimer {
    LatencyTimer::new("strata_data_write_duration_seconds")
}

/// Create a timer for Raft operations.
pub fn raft_timer(op_type: &str) -> LatencyTimer {
    LatencyTimer::new("strata_raft_op_duration_seconds")
        .with_label("operation", op_type)
}

/// Create a timer for S3 operations.
pub fn s3_timer(method: &str) -> LatencyTimer {
    LatencyTimer::new("strata_s3_request_duration_seconds")
        .with_label("method", method)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_stats_avg() {
        let mut stats = LatencyStats::default();
        assert!(stats.avg().is_none());

        stats.count = 10;
        stats.sum = 1.0;
        assert_eq!(stats.avg(), Some(0.1));
    }

    #[test]
    fn test_latency_timer() {
        let timer = LatencyTimer::new("test_metric")
            .with_label("op", "test");

        std::thread::sleep(Duration::from_millis(10));

        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(10));
    }

    #[test]
    fn test_record_metadata_latency() {
        // Just ensure it doesn't panic
        record_metadata_latency("lookup", Duration::from_millis(5));
        record_metadata_latency("create", Duration::from_millis(10));
    }

    #[test]
    fn test_record_data_latency() {
        record_data_read_latency(Duration::from_millis(50), 1024);
        record_data_write_latency(Duration::from_millis(100), 2048);
    }

    #[test]
    fn test_record_raft_latency() {
        record_raft_latency("append_entries", Duration::from_millis(5));
        record_raft_latency("request_vote", Duration::from_millis(2));
    }

    #[test]
    fn test_record_s3_latency() {
        record_s3_latency("GET", 200, Duration::from_millis(50));
        record_s3_latency("PUT", 201, Duration::from_millis(100));
    }
}
