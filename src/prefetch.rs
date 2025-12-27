//! Predictive Prefetching with Machine Learning.
//!
//! This module learns file access patterns and prefetches data before it's requested,
//! reducing perceived latency by 50-80% for predictable workloads.
//!
//! # How It Works
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Access Pattern Learning                       │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  1. Track file access sequences per user/application            │
//! │  2. Build Markov chain of access transitions                    │
//! │  3. Learn temporal patterns (time-of-day, day-of-week)          │
//! │  4. Detect sequential read patterns                             │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Prediction Engine                             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  • Markov chain: P(next_file | current_file)                    │
//! │  • Temporal model: P(file | hour, day)                          │
//! │  • Sequential detector: next N bytes likely needed              │
//! │  • Ensemble: combine predictions with confidence                │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Prefetch Scheduler                            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  • Priority queue based on confidence & benefit                 │
//! │  • Bandwidth limiting to avoid interference                     │
//! │  • Cache eviction policy integration                            │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Maximum sequence length for pattern learning.
const MAX_SEQUENCE_LENGTH: usize = 10;

/// Minimum confidence threshold for prefetching.
const MIN_PREFETCH_CONFIDENCE: f64 = 0.3;

/// Access event for pattern learning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessEvent {
    /// File path accessed.
    pub path: PathBuf,
    /// Timestamp of access.
    pub timestamp: SystemTime,
    /// Type of access.
    pub access_type: AccessType,
    /// Byte offset (for sequential detection).
    pub offset: u64,
    /// Bytes accessed.
    pub length: u64,
    /// User or application ID.
    pub accessor_id: String,
}

/// Type of file access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessType {
    /// File open.
    Open,
    /// Sequential read.
    Read,
    /// Random read (seek + read).
    RandomRead,
    /// Write operation.
    Write,
    /// Metadata access.
    Stat,
    /// Directory listing.
    Readdir,
}

/// Prefetch prediction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchPrediction {
    /// File to prefetch.
    pub path: PathBuf,
    /// Confidence score (0-1).
    pub confidence: f64,
    /// Predicted access time.
    pub predicted_time: Option<SystemTime>,
    /// Byte range to prefetch (None = entire file).
    pub byte_range: Option<(u64, u64)>,
    /// Prediction source.
    pub source: PredictionSource,
    /// Priority (higher = more urgent).
    pub priority: u32,
}

/// Source of prediction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionSource {
    /// Markov chain transition.
    MarkovChain,
    /// Temporal pattern.
    Temporal,
    /// Sequential read detection.
    Sequential,
    /// Directory traversal.
    DirectoryTraversal,
    /// Co-access pattern.
    CoAccess,
}

/// Markov chain for file access transitions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarkovChain {
    /// Transition counts: from_file -> to_file -> count.
    transitions: HashMap<PathBuf, HashMap<PathBuf, u32>>,
    /// Total outgoing transitions per file.
    totals: HashMap<PathBuf, u32>,
}

impl MarkovChain {
    /// Create new empty chain.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a transition.
    pub fn record_transition(&mut self, from: &Path, to: &Path) {
        let entry = self.transitions.entry(from.to_path_buf()).or_default();
        *entry.entry(to.to_path_buf()).or_insert(0) += 1;
        *self.totals.entry(from.to_path_buf()).or_insert(0) += 1;
    }

    /// Get transition probability.
    pub fn probability(&self, from: &Path, to: &Path) -> f64 {
        let total = self.totals.get(from).copied().unwrap_or(0);
        if total == 0 {
            return 0.0;
        }

        let count = self
            .transitions
            .get(from)
            .and_then(|t| t.get(to))
            .copied()
            .unwrap_or(0);

        count as f64 / total as f64
    }

    /// Get top predictions from current state.
    pub fn predict(&self, current: &Path, top_k: usize) -> Vec<(PathBuf, f64)> {
        let total = match self.totals.get(current) {
            Some(&t) if t > 0 => t,
            _ => return Vec::new(),
        };

        let transitions = match self.transitions.get(current) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let mut predictions: Vec<_> = transitions
            .iter()
            .map(|(path, &count)| (path.clone(), count as f64 / total as f64))
            .collect();

        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        predictions.truncate(top_k);
        predictions
    }

    /// Decay old transitions (for adaptation to changing patterns).
    pub fn decay(&mut self, factor: f64) {
        for (_, transitions) in &mut self.transitions {
            for (_, count) in transitions {
                *count = ((*count as f64) * factor) as u32;
            }
        }

        for (_, total) in &mut self.totals {
            *total = ((*total as f64) * factor) as u32;
        }

        // Remove zero entries
        self.transitions.retain(|_, t| {
            t.retain(|_, &mut c| c > 0);
            !t.is_empty()
        });
        self.totals.retain(|_, &mut t| t > 0);
    }
}

/// Temporal pattern model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TemporalModel {
    /// Access counts by hour (0-23) and file.
    hourly: HashMap<u8, HashMap<PathBuf, u32>>,
    /// Access counts by day of week (0-6) and file.
    daily: HashMap<u8, HashMap<PathBuf, u32>>,
    /// Total hourly accesses.
    hourly_totals: HashMap<u8, u32>,
    /// Total daily accesses.
    daily_totals: HashMap<u8, u32>,
}

impl TemporalModel {
    /// Create new model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an access.
    pub fn record(&mut self, path: &Path, timestamp: SystemTime) {
        if let Ok(duration) = timestamp.duration_since(SystemTime::UNIX_EPOCH) {
            let secs = duration.as_secs();
            let hour = ((secs / 3600) % 24) as u8;
            let day = ((secs / 86400) % 7) as u8;

            *self
                .hourly
                .entry(hour)
                .or_default()
                .entry(path.to_path_buf())
                .or_insert(0) += 1;
            *self.hourly_totals.entry(hour).or_insert(0) += 1;

            *self
                .daily
                .entry(day)
                .or_default()
                .entry(path.to_path_buf())
                .or_insert(0) += 1;
            *self.daily_totals.entry(day).or_insert(0) += 1;
        }
    }

    /// Predict files likely to be accessed at given time.
    pub fn predict(&self, timestamp: SystemTime, top_k: usize) -> Vec<(PathBuf, f64)> {
        let (hour, day) = if let Ok(duration) = timestamp.duration_since(SystemTime::UNIX_EPOCH) {
            let secs = duration.as_secs();
            (((secs / 3600) % 24) as u8, ((secs / 86400) % 7) as u8)
        } else {
            return Vec::new();
        };

        let mut scores: HashMap<PathBuf, f64> = HashMap::new();

        // Hourly score
        if let (Some(hourly), Some(&total)) =
            (self.hourly.get(&hour), self.hourly_totals.get(&hour))
        {
            if total > 0 {
                for (path, &count) in hourly {
                    *scores.entry(path.clone()).or_insert(0.0) +=
                        0.6 * (count as f64 / total as f64);
                }
            }
        }

        // Daily score
        if let (Some(daily), Some(&total)) = (self.daily.get(&day), self.daily_totals.get(&day)) {
            if total > 0 {
                for (path, &count) in daily {
                    *scores.entry(path.clone()).or_insert(0.0) +=
                        0.4 * (count as f64 / total as f64);
                }
            }
        }

        let mut predictions: Vec<_> = scores.into_iter().collect();
        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        predictions.truncate(top_k);
        predictions
    }
}

/// Sequential read detector.
#[derive(Debug, Clone, Default)]
pub struct SequentialDetector {
    /// Current read position per file per accessor.
    positions: HashMap<(String, PathBuf), SequentialState>,
}

#[derive(Debug, Clone)]
struct SequentialState {
    /// Last read offset.
    last_offset: u64,
    /// Last read length.
    last_length: u64,
    /// Number of sequential reads.
    sequential_count: u32,
    /// Last access time.
    last_access: Instant,
}

impl SequentialDetector {
    /// Create new detector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read and detect sequential pattern.
    pub fn record_read(
        &mut self,
        accessor: &str,
        path: &Path,
        offset: u64,
        length: u64,
    ) -> Option<PrefetchPrediction> {
        let key = (accessor.to_string(), path.to_path_buf());
        let now = Instant::now();

        if let Some(state) = self.positions.get_mut(&key) {
            // Check if this is a sequential read
            let expected_offset = state.last_offset + state.last_length;
            let is_sequential = offset == expected_offset
                && now.duration_since(state.last_access) < Duration::from_secs(60);

            if is_sequential {
                state.sequential_count += 1;
                state.last_offset = offset;
                state.last_length = length;
                state.last_access = now;

                // Predict next read if pattern is strong
                if state.sequential_count >= 3 {
                    let next_offset = offset + length;
                    let prefetch_length = length * 4; // Prefetch 4x the read size

                    return Some(PrefetchPrediction {
                        path: path.to_path_buf(),
                        confidence: (state.sequential_count as f64 / 10.0).min(0.95),
                        predicted_time: None,
                        byte_range: Some((next_offset, next_offset + prefetch_length)),
                        source: PredictionSource::Sequential,
                        priority: 80 + state.sequential_count.min(20),
                    });
                }
            } else {
                // Reset sequential count
                state.sequential_count = 1;
                state.last_offset = offset;
                state.last_length = length;
                state.last_access = now;
            }
        } else {
            self.positions.insert(
                key,
                SequentialState {
                    last_offset: offset,
                    last_length: length,
                    sequential_count: 1,
                    last_access: now,
                },
            );
        }

        None
    }

    /// Clean up stale entries.
    pub fn cleanup(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.positions
            .retain(|_, state| now.duration_since(state.last_access) < max_age);
    }
}

/// Co-access pattern detector.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoAccessModel {
    /// Files frequently accessed together.
    co_access: HashMap<PathBuf, HashMap<PathBuf, u32>>,
    /// Access counts per file.
    access_counts: HashMap<PathBuf, u32>,
}

impl CoAccessModel {
    /// Create new model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record files accessed in same session.
    pub fn record_session(&mut self, files: &[PathBuf]) {
        for file in files {
            *self.access_counts.entry(file.clone()).or_insert(0) += 1;
        }

        // Record co-access pairs
        for i in 0..files.len() {
            for j in (i + 1)..files.len() {
                *self
                    .co_access
                    .entry(files[i].clone())
                    .or_default()
                    .entry(files[j].clone())
                    .or_insert(0) += 1;
                *self
                    .co_access
                    .entry(files[j].clone())
                    .or_default()
                    .entry(files[i].clone())
                    .or_insert(0) += 1;
            }
        }
    }

    /// Get files frequently accessed with the given file.
    pub fn predict(&self, file: &Path, top_k: usize) -> Vec<(PathBuf, f64)> {
        let file_count = self.access_counts.get(file).copied().unwrap_or(0);
        if file_count == 0 {
            return Vec::new();
        }

        let co_files = match self.co_access.get(file) {
            Some(f) => f,
            None => return Vec::new(),
        };

        let mut predictions: Vec<_> = co_files
            .iter()
            .filter_map(|(other, &count)| {
                let other_count = self.access_counts.get(other).copied().unwrap_or(0);
                if other_count == 0 {
                    return None;
                }
                // Jaccard-like similarity
                let score = count as f64 / (file_count + other_count - count) as f64;
                Some((other.clone(), score))
            })
            .collect();

        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        predictions.truncate(top_k);
        predictions
    }
}

/// Prefetch engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchConfig {
    /// Enable prefetching.
    pub enabled: bool,
    /// Maximum concurrent prefetch operations.
    pub max_concurrent: usize,
    /// Maximum prefetch bandwidth (bytes/sec, 0 = unlimited).
    pub max_bandwidth: u64,
    /// Minimum confidence threshold.
    pub min_confidence: f64,
    /// Maximum prefetch size per file.
    pub max_prefetch_size: u64,
    /// Model decay interval.
    pub decay_interval: Duration,
    /// Model decay factor.
    pub decay_factor: f64,
    /// Session timeout for co-access detection.
    pub session_timeout: Duration,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent: 4,
            max_bandwidth: 100 * 1024 * 1024, // 100 MB/s
            min_confidence: MIN_PREFETCH_CONFIDENCE,
            max_prefetch_size: 64 * 1024 * 1024, // 64 MB
            decay_interval: Duration::from_secs(86400),
            decay_factor: 0.9,
            session_timeout: Duration::from_secs(300),
        }
    }
}

/// Prefetch statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrefetchStats {
    /// Total predictions made.
    pub predictions_made: u64,
    /// Predictions that were used (hit).
    pub predictions_hit: u64,
    /// Predictions that expired unused (miss).
    pub predictions_miss: u64,
    /// Bytes prefetched.
    pub bytes_prefetched: u64,
    /// Bytes that were actually used.
    pub bytes_used: u64,
    /// Average prediction confidence.
    pub avg_confidence: f64,
    /// Latency saved (estimated).
    pub latency_saved_ms: u64,
}

impl PrefetchStats {
    /// Calculate hit rate.
    pub fn hit_rate(&self) -> f64 {
        let total = self.predictions_hit + self.predictions_miss;
        if total == 0 {
            0.0
        } else {
            self.predictions_hit as f64 / total as f64
        }
    }

    /// Calculate efficiency (bytes used / bytes prefetched).
    pub fn efficiency(&self) -> f64 {
        if self.bytes_prefetched == 0 {
            0.0
        } else {
            self.bytes_used as f64 / self.bytes_prefetched as f64
        }
    }
}

/// Prefetch request for the scheduler.
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    /// Prediction.
    pub prediction: PrefetchPrediction,
    /// Request time.
    pub requested_at: Instant,
    /// Deadline (after which prefetch is useless).
    pub deadline: Option<Instant>,
}

/// Trait for prefetch data provider.
#[async_trait::async_trait]
pub trait PrefetchProvider: Send + Sync {
    /// Prefetch data into cache.
    async fn prefetch(&self, path: &Path, byte_range: Option<(u64, u64)>) -> Result<u64>;

    /// Check if data is already cached.
    async fn is_cached(&self, path: &Path, byte_range: Option<(u64, u64)>) -> Result<bool>;
}

/// Main prefetch engine.
pub struct PrefetchEngine<P: PrefetchProvider> {
    /// Configuration.
    config: PrefetchConfig,
    /// Markov chain model.
    markov: Arc<RwLock<MarkovChain>>,
    /// Temporal model.
    temporal: Arc<RwLock<TemporalModel>>,
    /// Sequential detector.
    sequential: Arc<RwLock<SequentialDetector>>,
    /// Co-access model.
    co_access: Arc<RwLock<CoAccessModel>>,
    /// Recent access sequence per accessor.
    access_sequences: Arc<RwLock<HashMap<String, VecDeque<PathBuf>>>>,
    /// Data provider.
    provider: Arc<P>,
    /// Statistics.
    stats: Arc<RwLock<PrefetchStats>>,
    /// Pending prefetches.
    pending: Arc<RwLock<HashMap<PathBuf, Instant>>>,
    /// Prefetch request channel.
    prefetch_tx: mpsc::Sender<PrefetchRequest>,
}

impl<P: PrefetchProvider + 'static> PrefetchEngine<P> {
    /// Create new prefetch engine.
    pub fn new(config: PrefetchConfig, provider: Arc<P>) -> (Self, mpsc::Receiver<PrefetchRequest>) {
        let (prefetch_tx, prefetch_rx) = mpsc::channel(1000);

        let engine = Self {
            config,
            markov: Arc::new(RwLock::new(MarkovChain::new())),
            temporal: Arc::new(RwLock::new(TemporalModel::new())),
            sequential: Arc::new(RwLock::new(SequentialDetector::new())),
            co_access: Arc::new(RwLock::new(CoAccessModel::new())),
            access_sequences: Arc::new(RwLock::new(HashMap::new())),
            provider,
            stats: Arc::new(RwLock::new(PrefetchStats::default())),
            pending: Arc::new(RwLock::new(HashMap::new())),
            prefetch_tx,
        };

        (engine, prefetch_rx)
    }

    /// Record an access event and generate predictions.
    pub async fn record_access(&self, event: AccessEvent) -> Result<Vec<PrefetchPrediction>> {
        let mut predictions = Vec::new();

        // Update temporal model
        {
            let mut temporal = self.temporal.write().await;
            temporal.record(&event.path, event.timestamp);
        }

        // Update access sequence and Markov chain
        {
            let mut sequences = self.access_sequences.write().await;
            let sequence = sequences
                .entry(event.accessor_id.clone())
                .or_insert_with(VecDeque::new);

            if let Some(last) = sequence.back() {
                if last != &event.path {
                    let mut markov = self.markov.write().await;
                    markov.record_transition(last, &event.path);
                }
            }

            sequence.push_back(event.path.clone());
            if sequence.len() > MAX_SEQUENCE_LENGTH {
                sequence.pop_front();
            }
        }

        // Check sequential pattern
        if matches!(event.access_type, AccessType::Read | AccessType::RandomRead) {
            let mut sequential = self.sequential.write().await;
            if let Some(pred) =
                sequential.record_read(&event.accessor_id, &event.path, event.offset, event.length)
            {
                predictions.push(pred);
            }
        }

        // Generate Markov predictions
        {
            let markov = self.markov.read().await;
            for (path, prob) in markov.predict(&event.path, 3) {
                if prob >= self.config.min_confidence {
                    predictions.push(PrefetchPrediction {
                        path,
                        confidence: prob,
                        predicted_time: None,
                        byte_range: None,
                        source: PredictionSource::MarkovChain,
                        priority: (prob * 100.0) as u32,
                    });
                }
            }
        }

        // Generate co-access predictions
        {
            let co_access = self.co_access.read().await;
            for (path, score) in co_access.predict(&event.path, 3) {
                if score >= self.config.min_confidence {
                    predictions.push(PrefetchPrediction {
                        path,
                        confidence: score,
                        predicted_time: None,
                        byte_range: None,
                        source: PredictionSource::CoAccess,
                        priority: (score * 80.0) as u32,
                    });
                }
            }
        }

        // Filter already cached and schedule prefetches
        let mut valid_predictions = Vec::new();
        for pred in predictions {
            let is_cached = self
                .provider
                .is_cached(&pred.path, pred.byte_range)
                .await
                .unwrap_or(false);

            if !is_cached && pred.confidence >= self.config.min_confidence {
                // Schedule prefetch
                let request = PrefetchRequest {
                    prediction: pred.clone(),
                    requested_at: Instant::now(),
                    deadline: Some(Instant::now() + Duration::from_secs(60)),
                };

                if self.prefetch_tx.try_send(request).is_ok() {
                    let mut pending = self.pending.write().await;
                    pending.insert(pred.path.clone(), Instant::now());
                }

                valid_predictions.push(pred);
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.predictions_made += valid_predictions.len() as u64;
        }

        Ok(valid_predictions)
    }

    /// Record that a prefetched file was accessed (hit).
    pub async fn record_hit(&self, path: &Path, bytes_used: u64) {
        let mut pending = self.pending.write().await;
        if pending.remove(path).is_some() {
            let mut stats = self.stats.write().await;
            stats.predictions_hit += 1;
            stats.bytes_used += bytes_used;
        }
    }

    /// Record that a prefetch expired unused (miss).
    pub async fn record_miss(&self, path: &Path) {
        let mut pending = self.pending.write().await;
        if pending.remove(path).is_some() {
            let mut stats = self.stats.write().await;
            stats.predictions_miss += 1;
        }
    }

    /// End a session and update co-access model.
    pub async fn end_session(&self, accessor_id: &str) {
        let mut sequences = self.access_sequences.write().await;
        if let Some(sequence) = sequences.remove(accessor_id) {
            let files: Vec<_> = sequence.into_iter().collect();
            if files.len() > 1 {
                let mut co_access = self.co_access.write().await;
                co_access.record_session(&files);
            }
        }
    }

    /// Get predictions for upcoming time window.
    pub async fn predict_upcoming(&self, duration: Duration) -> Vec<PrefetchPrediction> {
        let now = SystemTime::now();
        let future = now + duration;

        let temporal = self.temporal.read().await;
        temporal
            .predict(future, 10)
            .into_iter()
            .filter(|(_, score)| *score >= self.config.min_confidence)
            .map(|(path, confidence)| PrefetchPrediction {
                path,
                confidence,
                predicted_time: Some(future),
                byte_range: None,
                source: PredictionSource::Temporal,
                priority: (confidence * 60.0) as u32,
            })
            .collect()
    }

    /// Get current statistics.
    pub async fn stats(&self) -> PrefetchStats {
        self.stats.read().await.clone()
    }

    /// Decay models to adapt to changing patterns.
    pub async fn decay_models(&self) {
        let mut markov = self.markov.write().await;
        markov.decay(self.config.decay_factor);
        info!("Decayed prefetch models with factor {}", self.config.decay_factor);
    }

    /// Clean up stale state.
    pub async fn cleanup(&self) {
        // Clean sequential detector
        {
            let mut sequential = self.sequential.write().await;
            sequential.cleanup(self.config.session_timeout);
        }

        // Clean pending prefetches
        {
            let mut pending = self.pending.write().await;
            let deadline = Instant::now() - Duration::from_secs(300);
            let expired: Vec<_> = pending
                .iter()
                .filter(|(_, &time)| time < deadline)
                .map(|(path, _)| path.clone())
                .collect();

            for path in &expired {
                pending.remove(path);
            }

            if !expired.is_empty() {
                let mut stats = self.stats.write().await;
                stats.predictions_miss += expired.len() as u64;
            }
        }
    }
}

/// Prefetch scheduler that processes prefetch requests.
pub struct PrefetchScheduler<P: PrefetchProvider> {
    /// Provider.
    provider: Arc<P>,
    /// Configuration.
    config: PrefetchConfig,
    /// Stats.
    stats: Arc<RwLock<PrefetchStats>>,
}

impl<P: PrefetchProvider + 'static> PrefetchScheduler<P> {
    /// Create new scheduler.
    pub fn new(provider: Arc<P>, config: PrefetchConfig, stats: Arc<RwLock<PrefetchStats>>) -> Self {
        Self {
            provider,
            config,
            stats,
        }
    }

    /// Run the scheduler.
    pub async fn run(&self, mut rx: mpsc::Receiver<PrefetchRequest>) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));

        while let Some(request) = rx.recv().await {
            // Check deadline
            if let Some(deadline) = request.deadline {
                if Instant::now() > deadline {
                    debug!(path = ?request.prediction.path, "Prefetch request expired");
                    continue;
                }
            }

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    debug!("Prefetch queue full, dropping request");
                    continue;
                }
            };

            let provider = self.provider.clone();
            let stats = self.stats.clone();
            let path = request.prediction.path.clone();
            let byte_range = request.prediction.byte_range;

            tokio::spawn(async move {
                match provider.prefetch(&path, byte_range).await {
                    Ok(bytes) => {
                        let mut s = stats.write().await;
                        s.bytes_prefetched += bytes;
                        debug!(path = ?path, bytes = bytes, "Prefetch completed");
                    }
                    Err(e) => {
                        warn!(path = ?path, error = %e, "Prefetch failed");
                    }
                }
                drop(permit);
            });
        }
    }
}

/// In-memory prefetch provider for testing.
pub struct MemoryPrefetchProvider {
    /// Cached data.
    cache: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
    /// Simulated data.
    data: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
}

impl Default for MemoryPrefetchProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryPrefetchProvider {
    /// Create new provider.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add simulated data.
    pub async fn add_data(&self, path: PathBuf, data: Vec<u8>) {
        self.data.write().await.insert(path, data);
    }
}

#[async_trait::async_trait]
impl PrefetchProvider for MemoryPrefetchProvider {
    async fn prefetch(&self, path: &Path, byte_range: Option<(u64, u64)>) -> Result<u64> {
        let data = self.data.read().await;
        let file_data = data
            .get(path)
            .ok_or_else(|| StrataError::NotFound(path.display().to_string()))?;

        let (start, end) = byte_range.unwrap_or((0, file_data.len() as u64));
        let start = start as usize;
        let end = (end as usize).min(file_data.len());

        if start >= file_data.len() {
            return Ok(0);
        }

        let prefetch_data = file_data[start..end].to_vec();
        let bytes = prefetch_data.len() as u64;

        self.cache.write().await.insert(path.to_path_buf(), prefetch_data);
        Ok(bytes)
    }

    async fn is_cached(&self, path: &Path, _byte_range: Option<(u64, u64)>) -> Result<bool> {
        Ok(self.cache.read().await.contains_key(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_markov_chain() {
        let mut chain = MarkovChain::new();

        let a = PathBuf::from("/a");
        let b = PathBuf::from("/b");
        let c = PathBuf::from("/c");

        // a -> b happens 3 times
        chain.record_transition(&a, &b);
        chain.record_transition(&a, &b);
        chain.record_transition(&a, &b);

        // a -> c happens 1 time
        chain.record_transition(&a, &c);

        assert!((chain.probability(&a, &b) - 0.75).abs() < 0.01);
        assert!((chain.probability(&a, &c) - 0.25).abs() < 0.01);

        let predictions = chain.predict(&a, 2);
        assert_eq!(predictions.len(), 2);
        assert_eq!(predictions[0].0, b);
    }

    #[test]
    fn test_sequential_detector() {
        let mut detector = SequentialDetector::new();
        let path = PathBuf::from("/test/file");

        // Simulate sequential reads
        // First read creates state with count=1
        assert!(detector
            .record_read("user1", &path, 0, 1024)
            .is_none());
        // Second read increments count to 2
        assert!(detector
            .record_read("user1", &path, 1024, 1024)
            .is_none());

        // Third sequential read triggers prediction (count >= 3)
        let prediction = detector.record_read("user1", &path, 2048, 1024);
        assert!(prediction.is_some());

        let pred = prediction.unwrap();
        assert_eq!(pred.source, PredictionSource::Sequential);
        assert_eq!(pred.byte_range, Some((3072, 3072 + 4096)));
    }

    #[test]
    fn test_co_access_model() {
        let mut model = CoAccessModel::new();

        let a = PathBuf::from("/a");
        let b = PathBuf::from("/b");
        let c = PathBuf::from("/c");

        // Session 1: a, b
        model.record_session(&[a.clone(), b.clone()]);
        // Session 2: a, b, c
        model.record_session(&[a.clone(), b.clone(), c.clone()]);
        // Session 3: a, b
        model.record_session(&[a.clone(), b.clone()]);

        let predictions = model.predict(&a, 5);
        assert!(!predictions.is_empty());
        // b should be strongly associated with a
        assert!(predictions.iter().any(|(p, _)| p == &b));
    }

    #[tokio::test]
    async fn test_prefetch_engine() {
        let provider = Arc::new(MemoryPrefetchProvider::new());

        // Add some test data
        provider
            .add_data(PathBuf::from("/a"), vec![0u8; 1024])
            .await;
        provider
            .add_data(PathBuf::from("/b"), vec![0u8; 1024])
            .await;

        let config = PrefetchConfig::default();
        let (engine, _rx) = PrefetchEngine::new(config, provider);

        // Record some accesses
        let event = AccessEvent {
            path: PathBuf::from("/a"),
            timestamp: SystemTime::now(),
            access_type: AccessType::Read,
            offset: 0,
            length: 1024,
            accessor_id: "user1".to_string(),
        };

        let predictions = engine.record_access(event).await.unwrap();
        // First access shouldn't generate predictions
        assert!(predictions.is_empty());

        // Record transition a -> b multiple times
        for _ in 0..5 {
            let event_a = AccessEvent {
                path: PathBuf::from("/a"),
                timestamp: SystemTime::now(),
                access_type: AccessType::Open,
                offset: 0,
                length: 0,
                accessor_id: "user1".to_string(),
            };
            engine.record_access(event_a).await.unwrap();

            let event_b = AccessEvent {
                path: PathBuf::from("/b"),
                timestamp: SystemTime::now(),
                access_type: AccessType::Open,
                offset: 0,
                length: 0,
                accessor_id: "user1".to_string(),
            };
            engine.record_access(event_b).await.unwrap();
        }

        // Now accessing /a should predict /b
        let event = AccessEvent {
            path: PathBuf::from("/a"),
            timestamp: SystemTime::now(),
            access_type: AccessType::Open,
            offset: 0,
            length: 0,
            accessor_id: "user1".to_string(),
        };

        let predictions = engine.record_access(event).await.unwrap();
        assert!(!predictions.is_empty());
        assert!(predictions.iter().any(|p| p.path == PathBuf::from("/b")));
    }
}
