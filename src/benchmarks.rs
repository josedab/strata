//! Comprehensive Benchmarking Suite
//!
//! Provides standardized benchmarks for evaluating Strata performance
//! across multiple dimensions: throughput, latency, scalability, and durability.
//!
//! # Features
//!
//! - Automated benchmark orchestration
//! - Multiple workload patterns
//! - Comparative analysis
//! - Report generation
//! - Regression detection
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::benchmarks::{BenchmarkSuite, WorkloadConfig};
//!
//! let suite = BenchmarkSuite::new()
//!     .with_workload(WorkloadConfig::sequential_write(1024 * 1024))
//!     .with_workload(WorkloadConfig::random_read(4096))
//!     .with_duration(Duration::from_secs(60));
//!
//! let results = suite.run().await?;
//! results.generate_report("benchmark_report.html")?;
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Benchmark suite orchestrator
pub struct BenchmarkSuite {
    /// Suite configuration
    config: SuiteConfig,
    /// Workloads to run
    workloads: Vec<WorkloadConfig>,
    /// Collected results
    results: Vec<BenchmarkResult>,
    /// Baseline for comparison
    baseline: Option<BaselineData>,
}

/// Suite configuration
#[derive(Debug, Clone)]
pub struct SuiteConfig {
    /// Number of parallel clients
    pub concurrency: usize,
    /// Warmup duration before measurement
    pub warmup: Duration,
    /// Measurement duration
    pub duration: Duration,
    /// Number of iterations for statistical significance
    pub iterations: usize,
    /// Enable detailed profiling
    pub profiling: bool,
    /// Output directory
    pub output_dir: String,
}

/// Workload configuration
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Workload name
    pub name: String,
    /// Workload type
    pub workload_type: WorkloadType,
    /// Object size range
    pub size_range: SizeRange,
    /// Access pattern
    pub pattern: AccessPattern,
    /// Read/write ratio (0.0 = all writes, 1.0 = all reads)
    pub read_ratio: f64,
    /// Target operations per second (0 = unlimited)
    pub target_ops: u64,
}

/// Workload types
#[derive(Debug, Clone)]
pub enum WorkloadType {
    /// Pure reads
    Read,
    /// Pure writes
    Write,
    /// Mixed read/write
    Mixed,
    /// Metadata operations
    Metadata,
    /// List operations
    List,
    /// Custom
    Custom(String),
}

/// Size range configuration
#[derive(Debug, Clone)]
pub struct SizeRange {
    pub min: u64,
    pub max: u64,
    pub distribution: SizeDistribution,
}

/// Size distribution
#[derive(Debug, Clone)]
pub enum SizeDistribution {
    Fixed(u64),
    Uniform,
    Normal { mean: f64, std_dev: f64 },
    Exponential { lambda: f64 },
    Pareto { alpha: f64 },
}

/// Access patterns
#[derive(Debug, Clone)]
pub enum AccessPattern {
    /// Sequential access
    Sequential,
    /// Random access
    Random,
    /// Hot/cold distribution
    HotCold { hot_ratio: f64, hot_percentage: f64 },
    /// Zipfian distribution
    Zipfian { skew: f64 },
    /// Temporal locality
    Temporal { window_size: usize },
}

/// Benchmark result
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Workload name
    pub workload: String,
    /// Total operations
    pub operations: u64,
    /// Total bytes transferred
    pub bytes: u64,
    /// Duration
    pub duration: Duration,
    /// Latency statistics
    pub latency: LatencyStats,
    /// Throughput statistics
    pub throughput: ThroughputStats,
    /// Error count
    pub errors: u64,
    /// Per-second breakdown
    pub timeseries: Vec<TimeseriesPoint>,
    /// Resource usage
    pub resources: ResourceUsage,
}

/// Latency statistics
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
    pub median_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub std_dev_us: f64,
}

/// Throughput statistics
#[derive(Debug, Clone, Default)]
pub struct ThroughputStats {
    pub ops_per_sec: f64,
    pub bytes_per_sec: f64,
    pub peak_ops_per_sec: f64,
    pub peak_bytes_per_sec: f64,
    pub sustained_ops_per_sec: f64,
}

/// Timeseries data point
#[derive(Debug, Clone)]
pub struct TimeseriesPoint {
    pub timestamp: Instant,
    pub operations: u64,
    pub bytes: u64,
    pub latency_mean_us: f64,
    pub errors: u64,
}

/// Resource usage during benchmark
#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    pub cpu_avg_pct: f64,
    pub cpu_max_pct: f64,
    pub memory_avg_mb: f64,
    pub memory_max_mb: f64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

/// Baseline data for comparison
#[derive(Debug, Clone)]
pub struct BaselineData {
    pub name: String,
    pub timestamp: String,
    pub results: Vec<BenchmarkResult>,
}

/// Benchmark runner for a single workload
pub struct BenchmarkRunner {
    config: WorkloadConfig,
    metrics: Arc<BenchmarkMetrics>,
    stop_signal: Arc<AtomicU64>,
}

/// Live metrics during benchmark
pub struct BenchmarkMetrics {
    operations: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    latencies: parking_lot::RwLock<Vec<u64>>,
}

/// Benchmark report
pub struct BenchmarkReport {
    /// Report title
    pub title: String,
    /// Results
    pub results: Vec<BenchmarkResult>,
    /// Comparison to baseline
    pub comparison: Option<ComparisonResult>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Comparison result
#[derive(Debug)]
pub struct ComparisonResult {
    pub baseline_name: String,
    pub differences: Vec<MetricDifference>,
    pub regression_detected: bool,
}

/// Difference in a metric
#[derive(Debug)]
pub struct MetricDifference {
    pub metric: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub difference_pct: f64,
    pub significance: Significance,
}

/// Statistical significance
#[derive(Debug)]
pub enum Significance {
    Significant,
    Marginal,
    Insignificant,
}

impl BenchmarkSuite {
    /// Create a new benchmark suite
    pub fn new() -> Self {
        Self {
            config: SuiteConfig::default(),
            workloads: Vec::new(),
            results: Vec::new(),
            baseline: None,
        }
    }

    /// Add a workload
    pub fn with_workload(mut self, config: WorkloadConfig) -> Self {
        self.workloads.push(config);
        self
    }

    /// Set concurrency
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.config.concurrency = concurrency;
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.config.duration = duration;
        self
    }

    /// Set warmup duration
    pub fn with_warmup(mut self, warmup: Duration) -> Self {
        self.config.warmup = warmup;
        self
    }

    /// Set iterations
    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.config.iterations = iterations;
        self
    }

    /// Set baseline for comparison
    pub fn with_baseline(mut self, baseline: BaselineData) -> Self {
        self.baseline = Some(baseline);
        self
    }

    /// Run all workloads
    pub fn run(&mut self) -> Vec<BenchmarkResult> {
        let mut all_results = Vec::new();

        for workload in &self.workloads {
            // Run multiple iterations
            let mut iteration_results = Vec::new();

            for i in 0..self.config.iterations {
                // Warmup phase
                if self.config.warmup > Duration::ZERO {
                    let warmup_runner = BenchmarkRunner::new(workload.clone());
                    warmup_runner.run(self.config.warmup, self.config.concurrency);
                }

                // Measurement phase
                let runner = BenchmarkRunner::new(workload.clone());
                let result = runner.run(self.config.duration, self.config.concurrency);
                iteration_results.push(result);

                // Small delay between iterations
                std::thread::sleep(Duration::from_millis(100));
                let _ = i; // suppress warning
            }

            // Aggregate results
            let aggregated = Self::aggregate_results(&iteration_results);
            all_results.push(aggregated);
        }

        self.results = all_results.clone();
        all_results
    }

    /// Generate HTML report
    pub fn generate_report(&self, output_path: &str) -> std::io::Result<()> {
        let report = self.build_report();
        let html = Self::render_html(&report);
        std::fs::write(output_path, html)
    }

    /// Generate JSON results
    pub fn export_json(&self, output_path: &str) -> std::io::Result<()> {
        // Simplified JSON export
        let json = format!("{:?}", self.results);
        std::fs::write(output_path, json)
    }

    fn aggregate_results(results: &[BenchmarkResult]) -> BenchmarkResult {
        if results.is_empty() {
            return BenchmarkResult::default();
        }

        let total_ops: u64 = results.iter().map(|r| r.operations).sum();
        let total_bytes: u64 = results.iter().map(|r| r.bytes).sum();
        let total_errors: u64 = results.iter().map(|r| r.errors).sum();
        let total_duration: Duration = results.iter().map(|r| r.duration).sum();
        let n = results.len() as f64;

        // Average latency stats
        let latency = LatencyStats {
            min_us: results.iter().map(|r| r.latency.min_us).min().unwrap_or(0),
            max_us: results.iter().map(|r| r.latency.max_us).max().unwrap_or(0),
            mean_us: results.iter().map(|r| r.latency.mean_us).sum::<f64>() / n,
            median_us: results.iter().map(|r| r.latency.median_us).sum::<u64>() / results.len() as u64,
            p50_us: results.iter().map(|r| r.latency.p50_us).sum::<u64>() / results.len() as u64,
            p90_us: results.iter().map(|r| r.latency.p90_us).sum::<u64>() / results.len() as u64,
            p95_us: results.iter().map(|r| r.latency.p95_us).sum::<u64>() / results.len() as u64,
            p99_us: results.iter().map(|r| r.latency.p99_us).sum::<u64>() / results.len() as u64,
            p999_us: results.iter().map(|r| r.latency.p999_us).sum::<u64>() / results.len() as u64,
            std_dev_us: results.iter().map(|r| r.latency.std_dev_us).sum::<f64>() / n,
        };

        let throughput = ThroughputStats {
            ops_per_sec: results.iter().map(|r| r.throughput.ops_per_sec).sum::<f64>() / n,
            bytes_per_sec: results.iter().map(|r| r.throughput.bytes_per_sec).sum::<f64>() / n,
            peak_ops_per_sec: results.iter().map(|r| r.throughput.peak_ops_per_sec).fold(0.0, f64::max),
            peak_bytes_per_sec: results.iter().map(|r| r.throughput.peak_bytes_per_sec).fold(0.0, f64::max),
            sustained_ops_per_sec: results.iter().map(|r| r.throughput.sustained_ops_per_sec).sum::<f64>() / n,
        };

        BenchmarkResult {
            workload: results[0].workload.clone(),
            operations: total_ops / results.len() as u64,
            bytes: total_bytes / results.len() as u64,
            duration: total_duration / results.len() as u32,
            latency,
            throughput,
            errors: total_errors / results.len() as u64,
            timeseries: Vec::new(),
            resources: ResourceUsage::default(),
        }
    }

    fn build_report(&self) -> BenchmarkReport {
        let comparison = self.baseline.as_ref().map(|baseline| {
            self.compare_to_baseline(baseline)
        });

        let recommendations = self.generate_recommendations();

        BenchmarkReport {
            title: "Strata Benchmark Report".to_string(),
            results: self.results.clone(),
            comparison,
            recommendations,
        }
    }

    fn compare_to_baseline(&self, baseline: &BaselineData) -> ComparisonResult {
        let mut differences = Vec::new();
        let mut regression_detected = false;

        for result in &self.results {
            if let Some(baseline_result) = baseline.results.iter().find(|r| r.workload == result.workload) {
                // Compare throughput
                let throughput_diff = (result.throughput.ops_per_sec - baseline_result.throughput.ops_per_sec)
                    / baseline_result.throughput.ops_per_sec * 100.0;

                let throughput_sig = if throughput_diff.abs() > 10.0 {
                    if throughput_diff < -10.0 {
                        regression_detected = true;
                    }
                    Significance::Significant
                } else if throughput_diff.abs() > 5.0 {
                    Significance::Marginal
                } else {
                    Significance::Insignificant
                };

                differences.push(MetricDifference {
                    metric: format!("{} throughput", result.workload),
                    baseline_value: baseline_result.throughput.ops_per_sec,
                    current_value: result.throughput.ops_per_sec,
                    difference_pct: throughput_diff,
                    significance: throughput_sig,
                });

                // Compare p99 latency
                let latency_diff = (result.latency.p99_us as f64 - baseline_result.latency.p99_us as f64)
                    / baseline_result.latency.p99_us as f64 * 100.0;

                let latency_sig = if latency_diff.abs() > 10.0 {
                    if latency_diff > 10.0 {
                        regression_detected = true;
                    }
                    Significance::Significant
                } else if latency_diff.abs() > 5.0 {
                    Significance::Marginal
                } else {
                    Significance::Insignificant
                };

                differences.push(MetricDifference {
                    metric: format!("{} p99 latency", result.workload),
                    baseline_value: baseline_result.latency.p99_us as f64,
                    current_value: result.latency.p99_us as f64,
                    difference_pct: latency_diff,
                    significance: latency_sig,
                });
            }
        }

        ComparisonResult {
            baseline_name: baseline.name.clone(),
            differences,
            regression_detected,
        }
    }

    fn generate_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        for result in &self.results {
            // High latency recommendations
            if result.latency.p99_us > 100_000 {
                recommendations.push(format!(
                    "{}: Consider enabling caching - p99 latency is {}ms",
                    result.workload,
                    result.latency.p99_us / 1000
                ));
            }

            // High error rate recommendations
            if result.errors > 0 {
                let error_rate = result.errors as f64 / result.operations as f64 * 100.0;
                if error_rate > 1.0 {
                    recommendations.push(format!(
                        "{}: Error rate is {:.2}% - investigate failures",
                        result.workload, error_rate
                    ));
                }
            }

            // Throughput recommendations
            if result.throughput.ops_per_sec < 1000.0 && result.latency.mean_us < 1000.0 {
                recommendations.push(format!(
                    "{}: Low throughput with low latency - consider increasing concurrency",
                    result.workload
                ));
            }
        }

        recommendations
    }

    fn render_html(report: &BenchmarkReport) -> String {
        let mut html = String::new();

        html.push_str("<!DOCTYPE html>\n<html>\n<head>\n");
        html.push_str("<title>Strata Benchmark Report</title>\n");
        html.push_str("<style>\n");
        html.push_str("body { font-family: Arial, sans-serif; margin: 40px; }\n");
        html.push_str("table { border-collapse: collapse; width: 100%; margin: 20px 0; }\n");
        html.push_str("th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }\n");
        html.push_str("th { background-color: #4CAF50; color: white; }\n");
        html.push_str("tr:nth-child(even) { background-color: #f2f2f2; }\n");
        html.push_str(".regression { color: red; font-weight: bold; }\n");
        html.push_str(".improvement { color: green; }\n");
        html.push_str("</style>\n</head>\n<body>\n");

        html.push_str(&format!("<h1>{}</h1>\n", report.title));

        // Results table
        html.push_str("<h2>Results</h2>\n");
        html.push_str("<table>\n");
        html.push_str("<tr><th>Workload</th><th>Operations</th><th>Throughput (ops/s)</th>");
        html.push_str("<th>p50 (us)</th><th>p99 (us)</th><th>Errors</th></tr>\n");

        for result in &report.results {
            html.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{:.2}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                result.workload,
                result.operations,
                result.throughput.ops_per_sec,
                result.latency.p50_us,
                result.latency.p99_us,
                result.errors
            ));
        }

        html.push_str("</table>\n");

        // Comparison section
        if let Some(comparison) = &report.comparison {
            html.push_str("<h2>Comparison to Baseline</h2>\n");
            html.push_str(&format!("<p>Comparing to: {}</p>\n", comparison.baseline_name));

            if comparison.regression_detected {
                html.push_str("<p class='regression'>REGRESSION DETECTED</p>\n");
            }

            html.push_str("<table>\n");
            html.push_str("<tr><th>Metric</th><th>Baseline</th><th>Current</th><th>Difference</th></tr>\n");

            for diff in &comparison.differences {
                let class = if diff.difference_pct < -5.0 {
                    "regression"
                } else if diff.difference_pct > 5.0 {
                    "improvement"
                } else {
                    ""
                };

                html.push_str(&format!(
                    "<tr><td>{}</td><td>{:.2}</td><td>{:.2}</td><td class='{}'>{:+.2}%</td></tr>\n",
                    diff.metric, diff.baseline_value, diff.current_value, class, diff.difference_pct
                ));
            }

            html.push_str("</table>\n");
        }

        // Recommendations
        if !report.recommendations.is_empty() {
            html.push_str("<h2>Recommendations</h2>\n<ul>\n");
            for rec in &report.recommendations {
                html.push_str(&format!("<li>{}</li>\n", rec));
            }
            html.push_str("</ul>\n");
        }

        html.push_str("</body>\n</html>");
        html
    }
}

impl Default for BenchmarkSuite {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SuiteConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            warmup: Duration::from_secs(5),
            duration: Duration::from_secs(30),
            iterations: 3,
            profiling: false,
            output_dir: "./benchmark_results".to_string(),
        }
    }
}

impl WorkloadConfig {
    /// Create a sequential write workload
    pub fn sequential_write(size: u64) -> Self {
        Self {
            name: format!("sequential_write_{}KB", size / 1024),
            workload_type: WorkloadType::Write,
            size_range: SizeRange {
                min: size,
                max: size,
                distribution: SizeDistribution::Fixed(size),
            },
            pattern: AccessPattern::Sequential,
            read_ratio: 0.0,
            target_ops: 0,
        }
    }

    /// Create a random read workload
    pub fn random_read(size: u64) -> Self {
        Self {
            name: format!("random_read_{}KB", size / 1024),
            workload_type: WorkloadType::Read,
            size_range: SizeRange {
                min: size,
                max: size,
                distribution: SizeDistribution::Fixed(size),
            },
            pattern: AccessPattern::Random,
            read_ratio: 1.0,
            target_ops: 0,
        }
    }

    /// Create a mixed workload
    pub fn mixed(size: u64, read_ratio: f64) -> Self {
        Self {
            name: format!("mixed_{}KB_{:.0}r", size / 1024, read_ratio * 100.0),
            workload_type: WorkloadType::Mixed,
            size_range: SizeRange {
                min: size,
                max: size,
                distribution: SizeDistribution::Fixed(size),
            },
            pattern: AccessPattern::Random,
            read_ratio,
            target_ops: 0,
        }
    }

    /// Create metadata workload
    pub fn metadata() -> Self {
        Self {
            name: "metadata_ops".to_string(),
            workload_type: WorkloadType::Metadata,
            size_range: SizeRange {
                min: 0,
                max: 0,
                distribution: SizeDistribution::Fixed(0),
            },
            pattern: AccessPattern::Random,
            read_ratio: 0.8,
            target_ops: 0,
        }
    }

    /// Create variable size workload
    pub fn variable_size(min: u64, max: u64) -> Self {
        Self {
            name: format!("variable_{}KB_{}KB", min / 1024, max / 1024),
            workload_type: WorkloadType::Mixed,
            size_range: SizeRange {
                min,
                max,
                distribution: SizeDistribution::Uniform,
            },
            pattern: AccessPattern::Random,
            read_ratio: 0.5,
            target_ops: 0,
        }
    }

    /// Set target ops/sec
    pub fn with_target_ops(mut self, ops: u64) -> Self {
        self.target_ops = ops;
        self
    }

    /// Set access pattern
    pub fn with_pattern(mut self, pattern: AccessPattern) -> Self {
        self.pattern = pattern;
        self
    }
}

impl BenchmarkRunner {
    /// Create a new runner
    pub fn new(config: WorkloadConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(BenchmarkMetrics::new()),
            stop_signal: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Run the benchmark
    pub fn run(&self, duration: Duration, concurrency: usize) -> BenchmarkResult {
        let start = Instant::now();
        let metrics = self.metrics.clone();
        let stop = self.stop_signal.clone();
        let config = self.config.clone();

        // Spawn worker threads
        let mut handles = Vec::new();
        for _ in 0..concurrency {
            let metrics = metrics.clone();
            let stop = stop.clone();
            let config = config.clone();

            let handle = std::thread::spawn(move || {
                Self::worker_loop(&config, &metrics, &stop);
            });
            handles.push(handle);
        }

        // Collect timeseries data
        let mut timeseries = Vec::new();
        let mut last_ops = 0u64;
        let mut last_bytes = 0u64;

        while start.elapsed() < duration {
            std::thread::sleep(Duration::from_secs(1));

            let current_ops = metrics.operations.load(Ordering::Relaxed);
            let current_bytes = metrics.bytes.load(Ordering::Relaxed);
            let current_errors = metrics.errors.load(Ordering::Relaxed);

            let ops_delta = current_ops - last_ops;
            let bytes_delta = current_bytes - last_bytes;

            let latency_sum = metrics.latency_sum_us.load(Ordering::Relaxed);
            let latency_count = metrics.latency_count.load(Ordering::Relaxed);
            let mean_latency = if latency_count > 0 {
                latency_sum as f64 / latency_count as f64
            } else {
                0.0
            };

            timeseries.push(TimeseriesPoint {
                timestamp: Instant::now(),
                operations: ops_delta,
                bytes: bytes_delta,
                latency_mean_us: mean_latency,
                errors: current_errors,
            });

            last_ops = current_ops;
            last_bytes = current_bytes;
        }

        // Signal stop
        stop.store(1, Ordering::Relaxed);

        // Wait for workers
        for handle in handles {
            let _ = handle.join();
        }

        // Collect results
        let elapsed = start.elapsed();
        let total_ops = metrics.operations.load(Ordering::Relaxed);
        let total_bytes = metrics.bytes.load(Ordering::Relaxed);
        let total_errors = metrics.errors.load(Ordering::Relaxed);

        // Calculate latency percentiles
        let latency = metrics.calculate_latency_stats();

        let throughput = ThroughputStats {
            ops_per_sec: total_ops as f64 / elapsed.as_secs_f64(),
            bytes_per_sec: total_bytes as f64 / elapsed.as_secs_f64(),
            peak_ops_per_sec: timeseries.iter().map(|t| t.operations as f64).fold(0.0, f64::max),
            peak_bytes_per_sec: timeseries.iter().map(|t| t.bytes as f64).fold(0.0, f64::max),
            sustained_ops_per_sec: total_ops as f64 / elapsed.as_secs_f64(),
        };

        BenchmarkResult {
            workload: self.config.name.clone(),
            operations: total_ops,
            bytes: total_bytes,
            duration: elapsed,
            latency,
            throughput,
            errors: total_errors,
            timeseries,
            resources: ResourceUsage::default(),
        }
    }

    fn worker_loop(config: &WorkloadConfig, metrics: &BenchmarkMetrics, stop: &AtomicU64) {
        let mut rng_state = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        while stop.load(Ordering::Relaxed) == 0 {
            let start = Instant::now();

            // Simulate operation based on config
            let size = match &config.size_range.distribution {
                SizeDistribution::Fixed(s) => *s,
                SizeDistribution::Uniform => {
                    rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    config.size_range.min + (rng_state % (config.size_range.max - config.size_range.min + 1))
                }
                _ => config.size_range.min,
            };

            // Simulate work
            let iterations = (size / 1024).max(1);
            let mut sum = 0u64;
            for i in 0..iterations {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);

            let latency_us = start.elapsed().as_micros() as u64;

            // Record metrics
            metrics.operations.fetch_add(1, Ordering::Relaxed);
            metrics.bytes.fetch_add(size, Ordering::Relaxed);
            metrics.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
            metrics.latency_count.fetch_add(1, Ordering::Relaxed);

            {
                let mut latencies = metrics.latencies.write();
                if latencies.len() < 100000 {
                    latencies.push(latency_us);
                }
            }

            // Rate limiting
            if config.target_ops > 0 {
                let target_interval = Duration::from_nanos(1_000_000_000 / config.target_ops);
                let elapsed = start.elapsed();
                if elapsed < target_interval {
                    std::thread::sleep(target_interval - elapsed);
                }
            }
        }
    }
}

impl BenchmarkMetrics {
    fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latencies: parking_lot::RwLock::new(Vec::new()),
        }
    }

    fn calculate_latency_stats(&self) -> LatencyStats {
        let mut latencies = self.latencies.write();

        if latencies.is_empty() {
            return LatencyStats::default();
        }

        latencies.sort_unstable();

        let len = latencies.len();
        let sum: u64 = latencies.iter().sum();
        let mean = sum as f64 / len as f64;

        // Calculate std dev
        let variance: f64 = latencies.iter()
            .map(|&l| (l as f64 - mean).powi(2))
            .sum::<f64>() / len as f64;
        let std_dev = variance.sqrt();

        LatencyStats {
            min_us: latencies[0],
            max_us: latencies[len - 1],
            mean_us: mean,
            median_us: latencies[(len - 1) / 2],
            p50_us: latencies[((len - 1) as f64 * 0.5) as usize],
            p90_us: latencies[((len - 1) as f64 * 0.9) as usize],
            p95_us: latencies[((len - 1) as f64 * 0.95) as usize],
            p99_us: latencies[((len - 1) as f64 * 0.99) as usize],
            p999_us: latencies[((len - 1) as f64 * 0.999) as usize],
            std_dev_us: std_dev,
        }
    }
}

impl Default for BenchmarkResult {
    fn default() -> Self {
        Self {
            workload: String::new(),
            operations: 0,
            bytes: 0,
            duration: Duration::ZERO,
            latency: LatencyStats::default(),
            throughput: ThroughputStats::default(),
            errors: 0,
            timeseries: Vec::new(),
            resources: ResourceUsage::default(),
        }
    }
}

/// Predefined benchmark suites
pub mod presets {
    use super::*;

    /// Quick smoke test
    pub fn smoke_test() -> BenchmarkSuite {
        BenchmarkSuite::new()
            .with_workload(WorkloadConfig::sequential_write(4096))
            .with_workload(WorkloadConfig::random_read(4096))
            .with_duration(Duration::from_secs(5))
            .with_iterations(1)
    }

    /// Standard benchmark
    pub fn standard() -> BenchmarkSuite {
        BenchmarkSuite::new()
            .with_workload(WorkloadConfig::sequential_write(4 * 1024))
            .with_workload(WorkloadConfig::sequential_write(1024 * 1024))
            .with_workload(WorkloadConfig::random_read(4 * 1024))
            .with_workload(WorkloadConfig::random_read(1024 * 1024))
            .with_workload(WorkloadConfig::mixed(4 * 1024, 0.7))
            .with_workload(WorkloadConfig::metadata())
            .with_duration(Duration::from_secs(30))
            .with_iterations(3)
    }

    /// Comprehensive benchmark
    pub fn comprehensive() -> BenchmarkSuite {
        BenchmarkSuite::new()
            // Write workloads
            .with_workload(WorkloadConfig::sequential_write(1024))
            .with_workload(WorkloadConfig::sequential_write(4 * 1024))
            .with_workload(WorkloadConfig::sequential_write(64 * 1024))
            .with_workload(WorkloadConfig::sequential_write(1024 * 1024))
            .with_workload(WorkloadConfig::sequential_write(16 * 1024 * 1024))
            // Read workloads
            .with_workload(WorkloadConfig::random_read(1024))
            .with_workload(WorkloadConfig::random_read(4 * 1024))
            .with_workload(WorkloadConfig::random_read(64 * 1024))
            .with_workload(WorkloadConfig::random_read(1024 * 1024))
            // Mixed workloads
            .with_workload(WorkloadConfig::mixed(4 * 1024, 0.5))
            .with_workload(WorkloadConfig::mixed(4 * 1024, 0.7))
            .with_workload(WorkloadConfig::mixed(4 * 1024, 0.9))
            // Metadata
            .with_workload(WorkloadConfig::metadata())
            // Variable size
            .with_workload(WorkloadConfig::variable_size(1024, 1024 * 1024))
            .with_duration(Duration::from_secs(60))
            .with_iterations(5)
    }

    /// Stress test
    pub fn stress() -> BenchmarkSuite {
        BenchmarkSuite::new()
            .with_workload(WorkloadConfig::mixed(4 * 1024, 0.5))
            .with_workload(WorkloadConfig::variable_size(1024, 16 * 1024 * 1024))
            .with_concurrency(128)
            .with_duration(Duration::from_secs(300))
            .with_iterations(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_config() {
        let config = WorkloadConfig::sequential_write(4096);
        assert_eq!(config.read_ratio, 0.0);

        let config = WorkloadConfig::random_read(4096);
        assert_eq!(config.read_ratio, 1.0);

        let config = WorkloadConfig::mixed(4096, 0.7);
        assert_eq!(config.read_ratio, 0.7);
    }

    #[test]
    fn test_benchmark_runner() {
        let config = WorkloadConfig::sequential_write(1024);
        let runner = BenchmarkRunner::new(config);

        let result = runner.run(Duration::from_millis(100), 2);

        assert!(result.operations > 0);
        assert!(result.throughput.ops_per_sec > 0.0);
    }

    #[test]
    fn test_latency_stats() {
        let metrics = BenchmarkMetrics::new();

        // Add some latencies
        {
            let mut latencies = metrics.latencies.write();
            for i in 1..=100 {
                latencies.push(i);
            }
        }

        let stats = metrics.calculate_latency_stats();

        assert_eq!(stats.min_us, 1);
        assert_eq!(stats.max_us, 100);
        assert_eq!(stats.p50_us, 50);
        assert!(stats.p99_us >= 99);
    }

    #[test]
    fn test_benchmark_suite() {
        let mut suite = presets::smoke_test();
        let results = suite.run();

        assert!(!results.is_empty());
        for result in results {
            assert!(result.operations > 0);
        }
    }

    #[test]
    fn test_comparison() {
        let baseline = BaselineData {
            name: "v1.0".to_string(),
            timestamp: "2024-01-01".to_string(),
            results: vec![BenchmarkResult {
                workload: "test".to_string(),
                throughput: ThroughputStats {
                    ops_per_sec: 1000.0,
                    ..Default::default()
                },
                latency: LatencyStats {
                    p99_us: 1000,
                    ..Default::default()
                },
                ..Default::default()
            }],
        };

        let mut suite = BenchmarkSuite::new()
            .with_baseline(baseline);

        suite.results = vec![BenchmarkResult {
            workload: "test".to_string(),
            throughput: ThroughputStats {
                ops_per_sec: 1100.0, // 10% improvement
                ..Default::default()
            },
            latency: LatencyStats {
                p99_us: 900, // 10% improvement
                ..Default::default()
            },
            ..Default::default()
        }];

        let report = suite.build_report();
        assert!(report.comparison.is_some());
        assert!(!report.comparison.unwrap().regression_detected);
    }

    #[test]
    fn test_html_report() {
        let suite = BenchmarkSuite::new();
        let report = BenchmarkReport {
            title: "Test Report".to_string(),
            results: vec![],
            comparison: None,
            recommendations: vec!["Test recommendation".to_string()],
        };

        let html = BenchmarkSuite::render_html(&report);
        assert!(html.contains("Test Report"));
        assert!(html.contains("Test recommendation"));
    }
}
