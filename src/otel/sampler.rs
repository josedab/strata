// Trace sampling strategies

use super::context::SpanContext;
use super::spans::SpanKind;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Sampling decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingDecision {
    /// Don't sample
    Drop,
    /// Sample but don't record
    RecordOnly,
    /// Sample and record
    RecordAndSample,
}

impl SamplingDecision {
    /// Should record spans
    pub fn should_record(&self) -> bool {
        matches!(self, SamplingDecision::RecordOnly | SamplingDecision::RecordAndSample)
    }

    /// Should propagate sampling
    pub fn should_sample(&self) -> bool {
        matches!(self, SamplingDecision::RecordAndSample)
    }
}

/// Sampling result
#[derive(Debug, Clone)]
pub struct SamplingResult {
    pub decision: SamplingDecision,
    pub attributes: Vec<(String, String)>,
}

impl SamplingResult {
    /// Creates a drop result
    pub fn drop() -> Self {
        Self {
            decision: SamplingDecision::Drop,
            attributes: Vec::new(),
        }
    }

    /// Creates a record and sample result
    pub fn record_and_sample() -> Self {
        Self {
            decision: SamplingDecision::RecordAndSample,
            attributes: Vec::new(),
        }
    }

    /// Adds an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push((key.into(), value.into()));
        self
    }
}

/// Sampler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplerConfig {
    /// Sampler type
    pub sampler_type: SamplerType,
    /// Sample ratio (0.0 - 1.0)
    pub sample_ratio: f64,
    /// Rate limit (samples per second, 0 = unlimited)
    pub rate_limit: u32,
    /// Parent-based sampling
    pub parent_based: bool,
    /// Always sample errors
    pub always_sample_errors: bool,
    /// Always sample slow spans
    pub always_sample_slow: bool,
    /// Slow threshold
    #[serde(with = "crate::config::humantime_serde")]
    pub slow_threshold: Duration,
    /// Endpoint-specific overrides
    pub endpoint_overrides: Vec<EndpointOverride>,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            sampler_type: SamplerType::TraceIdRatio,
            sample_ratio: 0.1, // 10%
            rate_limit: 100,
            parent_based: true,
            always_sample_errors: true,
            always_sample_slow: true,
            slow_threshold: Duration::from_secs(1),
            endpoint_overrides: Vec::new(),
        }
    }
}

/// Sampler type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SamplerType {
    /// Always sample
    AlwaysOn,
    /// Never sample
    AlwaysOff,
    /// Sample based on trace ID ratio
    TraceIdRatio,
    /// Rate-limited sampling
    RateLimited,
    /// Adaptive sampling
    Adaptive,
}

/// Endpoint-specific override
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointOverride {
    /// Endpoint pattern (glob)
    pub pattern: String,
    /// Override ratio
    pub ratio: Option<f64>,
    /// Force sample
    pub force_sample: bool,
    /// Force drop
    pub force_drop: bool,
}

/// Sampling parameters
pub struct SamplingParams<'a> {
    pub parent_context: Option<&'a SpanContext>,
    pub trace_id: &'a [u8; 16],
    pub span_name: &'a str,
    pub span_kind: SpanKind,
    pub attributes: &'a [(String, String)],
}

/// Tracing sampler
pub struct TracingSampler {
    config: SamplerConfig,
    // For rate limiting
    last_sample: std::sync::Mutex<Instant>,
    sample_count: AtomicU64,
    // For adaptive sampling
    recent_samples: AtomicU64,
    recent_drops: AtomicU64,
}

impl TracingSampler {
    /// Creates a new sampler
    pub fn new(config: SamplerConfig) -> Self {
        Self {
            config,
            last_sample: std::sync::Mutex::new(Instant::now()),
            sample_count: AtomicU64::new(0),
            recent_samples: AtomicU64::new(0),
            recent_drops: AtomicU64::new(0),
        }
    }

    /// Creates an always-on sampler
    pub fn always_on() -> Self {
        Self::new(SamplerConfig {
            sampler_type: SamplerType::AlwaysOn,
            ..Default::default()
        })
    }

    /// Creates an always-off sampler
    pub fn always_off() -> Self {
        Self::new(SamplerConfig {
            sampler_type: SamplerType::AlwaysOff,
            ..Default::default()
        })
    }

    /// Creates a trace ID ratio sampler
    pub fn trace_id_ratio(ratio: f64) -> Self {
        Self::new(SamplerConfig {
            sampler_type: SamplerType::TraceIdRatio,
            sample_ratio: ratio.clamp(0.0, 1.0),
            ..Default::default()
        })
    }

    /// Creates a rate-limited sampler
    pub fn rate_limited(samples_per_second: u32) -> Self {
        Self::new(SamplerConfig {
            sampler_type: SamplerType::RateLimited,
            rate_limit: samples_per_second,
            ..Default::default()
        })
    }

    /// Makes a sampling decision
    pub fn should_sample(&self, params: SamplingParams) -> SamplingResult {
        // Check endpoint overrides first
        if let Some(result) = self.check_endpoint_overrides(params.span_name) {
            return result;
        }

        // Parent-based sampling
        if self.config.parent_based {
            if let Some(parent) = params.parent_context {
                if parent.is_sampled() {
                    return SamplingResult::record_and_sample();
                } else if parent.is_valid() {
                    // Parent is valid but not sampled - inherit
                    return SamplingResult::drop();
                }
            }
        }

        // Apply primary sampling strategy
        match self.config.sampler_type {
            SamplerType::AlwaysOn => SamplingResult::record_and_sample(),
            SamplerType::AlwaysOff => SamplingResult::drop(),
            SamplerType::TraceIdRatio => self.trace_id_ratio_sample(params.trace_id),
            SamplerType::RateLimited => self.rate_limited_sample(),
            SamplerType::Adaptive => self.adaptive_sample(params.trace_id),
        }
    }

    /// Checks endpoint overrides
    fn check_endpoint_overrides(&self, span_name: &str) -> Option<SamplingResult> {
        for override_rule in &self.config.endpoint_overrides {
            if self.matches_pattern(&override_rule.pattern, span_name) {
                if override_rule.force_drop {
                    return Some(SamplingResult::drop());
                }
                if override_rule.force_sample {
                    return Some(SamplingResult::record_and_sample());
                }
                if let Some(ratio) = override_rule.ratio {
                    // Use override ratio
                    let trace_id = [0u8; 16]; // Would need actual trace ID
                    if self.should_sample_by_ratio(&trace_id, ratio) {
                        return Some(SamplingResult::record_and_sample());
                    } else {
                        return Some(SamplingResult::drop());
                    }
                }
            }
        }
        None
    }

    /// Samples based on trace ID ratio
    fn trace_id_ratio_sample(&self, trace_id: &[u8; 16]) -> SamplingResult {
        if self.should_sample_by_ratio(trace_id, self.config.sample_ratio) {
            SamplingResult::record_and_sample()
        } else {
            SamplingResult::drop()
        }
    }

    /// Checks if should sample by ratio
    fn should_sample_by_ratio(&self, trace_id: &[u8; 16], ratio: f64) -> bool {
        if ratio >= 1.0 {
            return true;
        }
        if ratio <= 0.0 {
            return false;
        }

        // Use last 8 bytes of trace ID as sampling key
        let key = u64::from_le_bytes([
            trace_id[8], trace_id[9], trace_id[10], trace_id[11],
            trace_id[12], trace_id[13], trace_id[14], trace_id[15],
        ]);

        // Check if within ratio
        (key as f64 / u64::MAX as f64) < ratio
    }

    /// Rate-limited sampling
    fn rate_limited_sample(&self) -> SamplingResult {
        if self.config.rate_limit == 0 {
            return SamplingResult::record_and_sample();
        }

        let mut last = self.last_sample.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        // Reset counter every second
        if elapsed >= Duration::from_secs(1) {
            *last = now;
            self.sample_count.store(0, Ordering::Relaxed);
        }

        let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
        if count < self.config.rate_limit as u64 {
            SamplingResult::record_and_sample()
        } else {
            SamplingResult::drop()
        }
    }

    /// Adaptive sampling
    fn adaptive_sample(&self, trace_id: &[u8; 16]) -> SamplingResult {
        // Calculate current sampling rate based on recent history
        let samples = self.recent_samples.load(Ordering::Relaxed);
        let drops = self.recent_drops.load(Ordering::Relaxed);
        let total = samples + drops;

        let current_ratio = if total > 0 {
            samples as f64 / total as f64
        } else {
            self.config.sample_ratio
        };

        // Adjust ratio based on target
        let target = self.config.sample_ratio;
        let adjusted = if current_ratio > target * 1.1 {
            // Sampling too much, reduce
            (target * 0.9).max(0.01)
        } else if current_ratio < target * 0.9 {
            // Sampling too little, increase
            (target * 1.1).min(1.0)
        } else {
            target
        };

        if self.should_sample_by_ratio(trace_id, adjusted) {
            self.recent_samples.fetch_add(1, Ordering::Relaxed);
            SamplingResult::record_and_sample()
        } else {
            self.recent_drops.fetch_add(1, Ordering::Relaxed);
            SamplingResult::drop()
        }
    }

    /// Matches a glob pattern
    fn matches_pattern(&self, pattern: &str, name: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len() - 1];
            return name.starts_with(prefix);
        }
        if pattern.starts_with('*') {
            let suffix = &pattern[1..];
            return name.ends_with(suffix);
        }
        pattern == name
    }

    /// Should sample errors (post-hoc decision)
    pub fn should_sample_error(&self) -> bool {
        self.config.always_sample_errors
    }

    /// Should sample slow spans (post-hoc decision)
    pub fn should_sample_slow(&self, duration: Duration) -> bool {
        self.config.always_sample_slow && duration >= self.config.slow_threshold
    }

    /// Resets adaptive counters (call periodically)
    pub fn reset_adaptive_counters(&self) {
        self.recent_samples.store(0, Ordering::Relaxed);
        self.recent_drops.store(0, Ordering::Relaxed);
    }
}

/// Head-based sampler (decision at span start)
pub struct HeadSampler {
    inner: TracingSampler,
}

impl HeadSampler {
    pub fn new(sampler: TracingSampler) -> Self {
        Self { inner: sampler }
    }

    pub fn should_sample(&self, params: SamplingParams) -> SamplingResult {
        self.inner.should_sample(params)
    }
}

/// Tail-based sampler (decision after span ends)
pub struct TailSampler {
    /// Error sampling enabled
    pub sample_errors: bool,
    /// Slow span sampling enabled
    pub sample_slow: bool,
    /// Slow threshold
    pub slow_threshold: Duration,
    /// Buffer for pending decisions
    pending: std::sync::Mutex<Vec<PendingSpan>>,
    /// Maximum buffer size
    max_buffer: usize,
}

struct PendingSpan {
    trace_id: [u8; 16],
    is_error: bool,
    duration: Duration,
}

impl TailSampler {
    pub fn new(sample_errors: bool, sample_slow: bool, slow_threshold: Duration) -> Self {
        Self {
            sample_errors,
            sample_slow,
            slow_threshold,
            pending: std::sync::Mutex::new(Vec::new()),
            max_buffer: 10000,
        }
    }

    /// Evaluates completed span
    pub fn evaluate(&self, _trace_id: [u8; 16], is_error: bool, duration: Duration) -> SamplingDecision {
        if self.sample_errors && is_error {
            return SamplingDecision::RecordAndSample;
        }

        if self.sample_slow && duration >= self.slow_threshold {
            return SamplingDecision::RecordAndSample;
        }

        SamplingDecision::Drop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_on_sampler() {
        let sampler = TracingSampler::always_on();
        let trace_id = [1u8; 16];

        let params = SamplingParams {
            parent_context: None,
            trace_id: &trace_id,
            span_name: "test",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };

        let result = sampler.should_sample(params);
        assert!(result.decision.should_sample());
    }

    #[test]
    fn test_always_off_sampler() {
        let sampler = TracingSampler::always_off();
        let trace_id = [1u8; 16];

        let params = SamplingParams {
            parent_context: None,
            trace_id: &trace_id,
            span_name: "test",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };

        let result = sampler.should_sample(params);
        assert!(!result.decision.should_sample());
    }

    #[test]
    fn test_ratio_sampler() {
        let sampler = TracingSampler::trace_id_ratio(0.5);

        let mut sampled = 0;
        let iterations = 1000;

        for i in 0..iterations {
            let trace_id = [i as u8; 16];
            let params = SamplingParams {
                parent_context: None,
                trace_id: &trace_id,
                span_name: "test",
                span_kind: SpanKind::Internal,
                attributes: &[],
            };

            if sampler.should_sample(params).decision.should_sample() {
                sampled += 1;
            }
        }

        // Should be roughly 50% (with some tolerance)
        let ratio = sampled as f64 / iterations as f64;
        assert!(ratio > 0.3 && ratio < 0.7);
    }

    #[test]
    fn test_rate_limited_sampler() {
        let sampler = TracingSampler::rate_limited(5);
        let trace_id = [1u8; 16];

        let mut sampled = 0;
        for _ in 0..10 {
            let params = SamplingParams {
                parent_context: None,
                trace_id: &trace_id,
                span_name: "test",
                span_kind: SpanKind::Internal,
                attributes: &[],
            };

            if sampler.should_sample(params).decision.should_sample() {
                sampled += 1;
            }
        }

        // Should be limited to 5
        assert_eq!(sampled, 5);
    }

    #[test]
    fn test_parent_based_sampling() {
        let sampler = TracingSampler::trace_id_ratio(0.0); // Would normally drop
        let trace_id = [1u8; 16];

        // With sampled parent, should sample
        let parent = SpanContext::root().with_sampled(true);
        let params = SamplingParams {
            parent_context: Some(&parent),
            trace_id: &trace_id,
            span_name: "test",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };

        let result = sampler.should_sample(params);
        assert!(result.decision.should_sample());

        // With non-sampled parent, should drop
        let parent = SpanContext::root().with_sampled(false);
        let params = SamplingParams {
            parent_context: Some(&parent),
            trace_id: &trace_id,
            span_name: "test",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };

        let result = sampler.should_sample(params);
        assert!(!result.decision.should_sample());
    }

    #[test]
    fn test_endpoint_override() {
        let config = SamplerConfig {
            sampler_type: SamplerType::AlwaysOff,
            endpoint_overrides: vec![
                EndpointOverride {
                    pattern: "health*".to_string(),
                    ratio: None,
                    force_sample: false,
                    force_drop: true,
                },
                EndpointOverride {
                    pattern: "important*".to_string(),
                    ratio: None,
                    force_sample: true,
                    force_drop: false,
                },
            ],
            ..Default::default()
        };

        let sampler = TracingSampler::new(config);
        let trace_id = [1u8; 16];

        // Health check should be dropped
        let params = SamplingParams {
            parent_context: None,
            trace_id: &trace_id,
            span_name: "healthcheck",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };
        assert!(!sampler.should_sample(params).decision.should_sample());

        // Important should be sampled
        let params = SamplingParams {
            parent_context: None,
            trace_id: &trace_id,
            span_name: "important-operation",
            span_kind: SpanKind::Internal,
            attributes: &[],
        };
        assert!(sampler.should_sample(params).decision.should_sample());
    }

    #[test]
    fn test_tail_sampler() {
        let sampler = TailSampler::new(true, true, Duration::from_secs(1));
        let trace_id = [1u8; 16];

        // Error should be sampled
        let decision = sampler.evaluate(trace_id, true, Duration::from_millis(100));
        assert_eq!(decision, SamplingDecision::RecordAndSample);

        // Slow should be sampled
        let decision = sampler.evaluate(trace_id, false, Duration::from_secs(2));
        assert_eq!(decision, SamplingDecision::RecordAndSample);

        // Fast and successful should be dropped
        let decision = sampler.evaluate(trace_id, false, Duration::from_millis(100));
        assert_eq!(decision, SamplingDecision::Drop);
    }
}
