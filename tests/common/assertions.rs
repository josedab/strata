// Custom test assertions and matchers for integration tests

use std::time::{Duration, Instant};

/// Assertion result for detailed error messages
#[derive(Debug)]
pub struct AssertionResult {
    pub passed: bool,
    pub message: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
}

impl AssertionResult {
    pub fn pass() -> Self {
        Self {
            passed: true,
            message: String::new(),
            expected: None,
            actual: None,
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            passed: false,
            message: message.into(),
            expected: None,
            actual: None,
        }
    }

    pub fn with_expected(mut self, expected: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self
    }

    pub fn with_actual(mut self, actual: impl Into<String>) -> Self {
        self.actual = Some(actual.into());
        self
    }

    pub fn assert(self) {
        if !self.passed {
            let mut msg = self.message;
            if let Some(expected) = self.expected {
                msg.push_str(&format!("\n  expected: {}", expected));
            }
            if let Some(actual) = self.actual {
                msg.push_str(&format!("\n  actual: {}", actual));
            }
            panic!("{}", msg);
        }
    }
}

/// Retry an assertion until it passes or times out
pub async fn assert_eventually<F, Fut>(
    f: F,
    timeout: Duration,
    interval: Duration,
) -> AssertionResult
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = AssertionResult>,
{
    let start = Instant::now();
    let mut last_result = AssertionResult::fail("No attempts made");

    while start.elapsed() < timeout {
        last_result = f().await;
        if last_result.passed {
            return last_result;
        }
        tokio::time::sleep(interval).await;
    }

    AssertionResult::fail(format!(
        "Assertion did not pass within {:?}: {}",
        timeout, last_result.message
    ))
    .with_expected(last_result.expected.unwrap_or_default())
    .with_actual(last_result.actual.unwrap_or_default())
}

/// Assert that a condition becomes true within a timeout
#[macro_export]
macro_rules! assert_eventually {
    ($cond:expr, $timeout_ms:expr) => {
        assert_eventually!(@ $cond, $timeout_ms, 100)
    };
    (@ $cond:expr, $timeout_ms:expr, $interval_ms:expr) => {{
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis($timeout_ms);
        let interval = std::time::Duration::from_millis($interval_ms);

        while start.elapsed() < timeout {
            if $cond {
                break;
            }
            tokio::time::sleep(interval).await;
        }

        assert!(
            $cond,
            "Condition did not become true within {:?}",
            timeout
        );
    }};
}

/// Assert that a value is within a range
#[macro_export]
macro_rules! assert_in_range {
    ($value:expr, $min:expr, $max:expr) => {
        assert!(
            $value >= $min && $value <= $max,
            "Expected {} to be in range [{}, {}]",
            $value,
            $min,
            $max
        );
    };
}

/// Assert that a duration is within acceptable bounds
#[macro_export]
macro_rules! assert_duration {
    ($duration:expr, $min_ms:expr, $max_ms:expr) => {
        let min = std::time::Duration::from_millis($min_ms);
        let max = std::time::Duration::from_millis($max_ms);
        assert!(
            $duration >= min && $duration <= max,
            "Expected duration {:?} to be between {:?} and {:?}",
            $duration,
            min,
            max
        );
    };
}

/// Assert file content matches
pub fn assert_content_matches(actual: &[u8], expected: &[u8]) -> AssertionResult {
    if actual.len() != expected.len() {
        return AssertionResult::fail("Content length mismatch")
            .with_expected(format!("{} bytes", expected.len()))
            .with_actual(format!("{} bytes", actual.len()));
    }

    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        if a != e {
            return AssertionResult::fail(format!("Content mismatch at byte {}", i))
                .with_expected(format!("0x{:02x}", e))
                .with_actual(format!("0x{:02x}", a));
        }
    }

    AssertionResult::pass()
}

/// Assert that an error matches expected type
pub fn assert_error_type<E: std::error::Error>(
    result: Result<(), E>,
    expected_substring: &str,
) -> AssertionResult {
    match result {
        Ok(()) => AssertionResult::fail("Expected error but got Ok"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains(expected_substring) {
                AssertionResult::pass()
            } else {
                AssertionResult::fail("Error message mismatch")
                    .with_expected(format!("contains '{}'", expected_substring))
                    .with_actual(msg)
            }
        }
    }
}

/// Metrics assertions
pub struct MetricsAssertions {
    pub request_count: u64,
    pub error_count: u64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
}

impl MetricsAssertions {
    pub fn assert_error_rate_below(&self, max_rate: f64) -> AssertionResult {
        let rate = if self.request_count > 0 {
            self.error_count as f64 / self.request_count as f64
        } else {
            0.0
        };

        if rate <= max_rate {
            AssertionResult::pass()
        } else {
            AssertionResult::fail("Error rate too high")
                .with_expected(format!("<= {:.2}%", max_rate * 100.0))
                .with_actual(format!("{:.2}%", rate * 100.0))
        }
    }

    pub fn assert_latency_p99_below(&self, max_ms: f64) -> AssertionResult {
        if self.latency_p99_ms <= max_ms {
            AssertionResult::pass()
        } else {
            AssertionResult::fail("P99 latency too high")
                .with_expected(format!("<= {:.2}ms", max_ms))
                .with_actual(format!("{:.2}ms", self.latency_p99_ms))
        }
    }
}

/// Throughput assertions
pub struct ThroughputAssertions {
    pub bytes_transferred: u64,
    pub duration: Duration,
}

impl ThroughputAssertions {
    pub fn bytes_per_second(&self) -> f64 {
        self.bytes_transferred as f64 / self.duration.as_secs_f64()
    }

    pub fn assert_min_throughput_mbps(&self, min_mbps: f64) -> AssertionResult {
        let mbps = self.bytes_per_second() / (1024.0 * 1024.0);
        if mbps >= min_mbps {
            AssertionResult::pass()
        } else {
            AssertionResult::fail("Throughput too low")
                .with_expected(format!(">= {:.2} MB/s", min_mbps))
                .with_actual(format!("{:.2} MB/s", mbps))
        }
    }
}

/// Consistency assertions
pub struct ConsistencyCheck {
    writes: Vec<(String, Vec<u8>)>,
    reads: Vec<(String, Vec<u8>)>,
}

impl ConsistencyCheck {
    pub fn new() -> Self {
        Self {
            writes: Vec::new(),
            reads: Vec::new(),
        }
    }

    pub fn record_write(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.writes.push((key.into(), value));
    }

    pub fn record_read(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.reads.push((key.into(), value));
    }

    pub fn verify_consistency(&self) -> AssertionResult {
        for (key, expected) in &self.writes {
            let read = self.reads.iter().find(|(k, _)| k == key);
            match read {
                Some((_, actual)) => {
                    if actual != expected {
                        return AssertionResult::fail(format!("Data mismatch for key '{}'", key))
                            .with_expected(format!("{} bytes", expected.len()))
                            .with_actual(format!("{} bytes", actual.len()));
                    }
                }
                None => {
                    return AssertionResult::fail(format!("Missing read for key '{}'", key));
                }
            }
        }
        AssertionResult::pass()
    }
}

impl Default for ConsistencyCheck {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_match() {
        let data = vec![1, 2, 3, 4, 5];
        assert!(assert_content_matches(&data, &data).passed);

        let different = vec![1, 2, 3, 4, 6];
        assert!(!assert_content_matches(&data, &different).passed);
    }

    #[test]
    fn test_metrics_assertions() {
        let metrics = MetricsAssertions {
            request_count: 1000,
            error_count: 5,
            latency_p50_ms: 10.0,
            latency_p99_ms: 50.0,
        };

        assert!(metrics.assert_error_rate_below(0.01).passed);
        assert!(!metrics.assert_error_rate_below(0.001).passed);
    }

    #[test]
    fn test_throughput_assertions() {
        let throughput = ThroughputAssertions {
            bytes_transferred: 100 * 1024 * 1024, // 100 MB
            duration: Duration::from_secs(1),
        };

        assert!(throughput.assert_min_throughput_mbps(50.0).passed);
        assert!(!throughput.assert_min_throughput_mbps(200.0).passed);
    }

    #[test]
    fn test_consistency_check() {
        let mut check = ConsistencyCheck::new();
        check.record_write("key1", vec![1, 2, 3]);
        check.record_write("key2", vec![4, 5, 6]);
        check.record_read("key1", vec![1, 2, 3]);
        check.record_read("key2", vec![4, 5, 6]);

        assert!(check.verify_consistency().passed);
    }
}
