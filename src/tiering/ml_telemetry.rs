// ML Telemetry for Smart Tiering
//
// This module provides telemetry collection and feature extraction for
// ML-based tiering decisions. It collects access patterns, temporal features,
// and object metadata to enable intelligent tier predictions.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::policy::TierType;
use super::tracker::{AccessType, ObjectAccessStats};

/// Feature vector for ML model input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringFeatures {
    /// Object identifier/path
    pub object_id: String,
    /// Current storage tier
    pub current_tier: TierType,
    /// Timestamp when features were extracted
    pub timestamp: u64,

    // === Access Pattern Features ===
    /// Total access count (lifetime)
    pub total_accesses: u64,
    /// Read count (lifetime)
    pub read_count: u64,
    /// Write count (lifetime)
    pub write_count: u64,
    /// Read/write ratio (0-1, higher = more reads)
    pub read_write_ratio: f64,

    // === Temporal Features ===
    /// Object age in seconds
    pub age_seconds: u64,
    /// Time since last access in seconds
    pub time_since_last_access: u64,
    /// Accesses in last hour
    pub accesses_last_hour: u64,
    /// Accesses in last day
    pub accesses_last_day: u64,
    /// Accesses in last week
    pub accesses_last_week: u64,
    /// Accesses in last month
    pub accesses_last_month: u64,

    // === Access Velocity Features ===
    /// Average accesses per day
    pub avg_accesses_per_day: f64,
    /// Access trend (positive = increasing, negative = decreasing)
    pub access_trend: f64,
    /// Heat score from tracker
    pub heat_score: f64,
    /// Access burstiness (variance of inter-access times)
    pub burstiness: f64,

    // === Size Features ===
    /// Object size in bytes
    pub size_bytes: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Average read size
    pub avg_read_size: f64,
    /// Average write size
    pub avg_write_size: f64,

    // === Temporal Pattern Features ===
    /// Peak hour of access (0-23)
    pub peak_access_hour: u8,
    /// Is weekend access higher than weekday
    pub weekend_bias: f64,
    /// Hour-of-day distribution entropy
    pub temporal_entropy: f64,

    // === Derived/Categorical Features ===
    /// File extension category (0=unknown, 1=text, 2=binary, 3=media, etc.)
    pub file_category: u8,
    /// Directory depth
    pub path_depth: u8,
    /// Is in a frequently accessed prefix
    pub hot_prefix: bool,
}

impl Default for TieringFeatures {
    fn default() -> Self {
        Self {
            object_id: String::new(),
            current_tier: TierType::Hot,
            timestamp: 0,
            total_accesses: 0,
            read_count: 0,
            write_count: 0,
            read_write_ratio: 0.5,
            age_seconds: 0,
            time_since_last_access: 0,
            accesses_last_hour: 0,
            accesses_last_day: 0,
            accesses_last_week: 0,
            accesses_last_month: 0,
            avg_accesses_per_day: 0.0,
            access_trend: 0.0,
            heat_score: 0.0,
            burstiness: 0.0,
            size_bytes: 0,
            bytes_read: 0,
            bytes_written: 0,
            avg_read_size: 0.0,
            avg_write_size: 0.0,
            peak_access_hour: 12,
            weekend_bias: 0.0,
            temporal_entropy: 0.0,
            file_category: 0,
            path_depth: 0,
            hot_prefix: false,
        }
    }
}

/// ML model prediction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierPrediction {
    /// Recommended tier
    pub recommended_tier: TierType,
    /// Confidence score (0-1)
    pub confidence: f64,
    /// Probability distribution across tiers
    pub tier_probabilities: HashMap<TierType, f64>,
    /// Expected time until next tier change (seconds)
    pub expected_stability_seconds: u64,
    /// Explanation/reasoning
    pub explanation: String,
}

/// Training sample for ML model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingSample {
    /// Features at decision time
    pub features: TieringFeatures,
    /// Actual tier assigned
    pub actual_tier: TierType,
    /// Outcome metrics (collected after some time)
    pub outcome: Option<TrainingOutcome>,
}

/// Outcome metrics for training feedback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingOutcome {
    /// Time since tier decision (seconds)
    pub elapsed_seconds: u64,
    /// Accesses since decision
    pub accesses_since: u64,
    /// Was migration triggered after decision
    pub was_migrated: bool,
    /// Time to first access after decision (if any)
    pub time_to_first_access: Option<u64>,
    /// Cost metric (access latency * count)
    pub access_cost: f64,
}

/// Configuration for ML telemetry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLTelemetryConfig {
    /// Enable feature collection
    pub enabled: bool,
    /// Maximum samples to retain
    pub max_samples: usize,
    /// Sample retention period
    #[serde(with = "crate::config::humantime_serde")]
    pub retention_period: Duration,
    /// Feature extraction interval
    #[serde(with = "crate::config::humantime_serde")]
    pub extraction_interval: Duration,
    /// Hot prefix patterns to track
    pub hot_prefixes: Vec<String>,
    /// Export path for training data
    pub export_path: Option<String>,
    /// Enable real-time feature streaming
    pub enable_streaming: bool,
}

impl Default for MLTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples: 100_000,
            retention_period: Duration::from_secs(30 * 24 * 3600), // 30 days
            extraction_interval: Duration::from_secs(300), // 5 minutes
            hot_prefixes: vec![
                "/hot/".to_string(),
                "/cache/".to_string(),
                "/tmp/".to_string(),
            ],
            export_path: None,
            enable_streaming: false,
        }
    }
}

/// ML Telemetry collector
pub struct MLTelemetry {
    config: MLTelemetryConfig,
    /// Recent feature extractions
    features: Arc<RwLock<VecDeque<TieringFeatures>>>,
    /// Training samples with outcomes
    training_samples: Arc<RwLock<VecDeque<TrainingSample>>>,
    /// Hourly access counts for temporal analysis
    hourly_counts: Arc<RwLock<HashMap<String, [u64; 24]>>>,
    /// Daily access counts (weekday vs weekend)
    daily_counts: Arc<RwLock<HashMap<String, [u64; 7]>>>,
    /// Feature extraction count
    extraction_count: Arc<std::sync::atomic::AtomicU64>,
}

impl MLTelemetry {
    /// Creates a new ML telemetry collector
    pub fn new(config: MLTelemetryConfig) -> Self {
        Self {
            config,
            features: Arc::new(RwLock::new(VecDeque::new())),
            training_samples: Arc::new(RwLock::new(VecDeque::new())),
            hourly_counts: Arc::new(RwLock::new(HashMap::new())),
            daily_counts: Arc::new(RwLock::new(HashMap::new())),
            extraction_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Creates with default configuration
    pub fn default_telemetry() -> Self {
        Self::new(MLTelemetryConfig::default())
    }

    /// Records an access for temporal pattern tracking
    pub async fn record_access(&self, object_id: &str, access_type: AccessType) {
        if !self.config.enabled {
            return;
        }

        let now = SystemTime::now();
        let datetime = now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let total_secs = datetime.as_secs();

        // Calculate hour (0-23) and day of week (0-6, 0=Sunday)
        let hour = ((total_secs / 3600) % 24) as usize;
        let day = ((total_secs / 86400 + 4) % 7) as usize; // Unix epoch was Thursday

        // Update hourly counts
        {
            let mut hourly = self.hourly_counts.write().await;
            let counts = hourly
                .entry(object_id.to_string())
                .or_insert([0u64; 24]);
            counts[hour] = counts[hour].saturating_add(1);
        }

        // Update daily counts
        {
            let mut daily = self.daily_counts.write().await;
            let counts = daily
                .entry(object_id.to_string())
                .or_insert([0u64; 7]);
            counts[day] = counts[day].saturating_add(1);
        }

        debug!(
            object_id = %object_id,
            hour = hour,
            day = day,
            access_type = ?access_type,
            "Recorded access for ML telemetry"
        );
    }

    /// Extracts features for an object
    pub async fn extract_features(
        &self,
        object_id: &str,
        stats: &ObjectAccessStats,
        current_tier: TierType,
        size_bytes: u64,
    ) -> TieringFeatures {
        let now = SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age_seconds = now
            .duration_since(stats.first_access)
            .unwrap_or_default()
            .as_secs();

        let time_since_last_access = now
            .duration_since(stats.last_access)
            .unwrap_or_default()
            .as_secs();

        // Calculate access counts in time windows
        let accesses_last_hour = stats.accesses_in_window(Duration::from_secs(3600));
        let accesses_last_day = stats.accesses_in_window(Duration::from_secs(86400));
        let accesses_last_week = stats.accesses_in_window(Duration::from_secs(7 * 86400));
        let accesses_last_month = stats.accesses_in_window(Duration::from_secs(30 * 86400));

        // Calculate read/write ratio
        let total_rw = stats.read_count + stats.write_count;
        let read_write_ratio = if total_rw > 0 {
            stats.read_count as f64 / total_rw as f64
        } else {
            0.5
        };

        // Calculate average sizes
        let avg_read_size = if stats.read_count > 0 {
            stats.bytes_read as f64 / stats.read_count as f64
        } else {
            0.0
        };

        let avg_write_size = if stats.write_count > 0 {
            stats.bytes_written as f64 / stats.write_count as f64
        } else {
            0.0
        };

        // Calculate access trend (recent vs historical)
        let access_trend = self.calculate_access_trend(stats);

        // Calculate burstiness
        let burstiness = self.calculate_burstiness(stats);

        // Get temporal pattern features
        let (peak_hour, weekend_bias, temporal_entropy) =
            self.get_temporal_features(object_id).await;

        // Determine file category
        let file_category = self.categorize_file(object_id);

        // Calculate path depth
        let path_depth = object_id.matches('/').count() as u8;

        // Check if in hot prefix
        let hot_prefix = self
            .config
            .hot_prefixes
            .iter()
            .any(|p| object_id.starts_with(p));

        TieringFeatures {
            object_id: object_id.to_string(),
            current_tier,
            timestamp,
            total_accesses: stats.total_accesses,
            read_count: stats.read_count,
            write_count: stats.write_count,
            read_write_ratio,
            age_seconds,
            time_since_last_access,
            accesses_last_hour,
            accesses_last_day,
            accesses_last_week,
            accesses_last_month,
            avg_accesses_per_day: stats.access_frequency(),
            access_trend,
            heat_score: stats.heat_score,
            burstiness,
            size_bytes,
            bytes_read: stats.bytes_read,
            bytes_written: stats.bytes_written,
            avg_read_size,
            avg_write_size,
            peak_access_hour: peak_hour,
            weekend_bias,
            temporal_entropy,
            file_category,
            path_depth,
            hot_prefix,
        }
    }

    /// Calculates access trend (positive = increasing activity)
    fn calculate_access_trend(&self, stats: &ObjectAccessStats) -> f64 {
        if stats.recent_accesses.len() < 4 {
            return 0.0;
        }

        let mid = stats.recent_accesses.len() / 2;
        let recent_half: Vec<_> = stats.recent_accesses.iter().skip(mid).collect();
        let older_half: Vec<_> = stats.recent_accesses.iter().take(mid).collect();

        let recent_count = recent_half.len() as f64;
        let older_count = older_half.len() as f64;

        if older_count == 0.0 {
            return 0.0;
        }

        // Simple trend: ratio of recent to older accesses
        (recent_count / older_count) - 1.0
    }

    /// Calculates burstiness (variance in inter-access times)
    fn calculate_burstiness(&self, stats: &ObjectAccessStats) -> f64 {
        if stats.recent_accesses.len() < 3 {
            return 0.0;
        }

        let mut intervals: Vec<f64> = Vec::new();
        let accesses: Vec<_> = stats.recent_accesses.iter().collect();

        for i in 1..accesses.len() {
            if let Ok(duration) = accesses[i].timestamp.duration_since(accesses[i - 1].timestamp) {
                intervals.push(duration.as_secs_f64());
            }
        }

        if intervals.is_empty() {
            return 0.0;
        }

        let mean = intervals.iter().sum::<f64>() / intervals.len() as f64;
        let variance = intervals
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / intervals.len() as f64;

        // Coefficient of variation (normalized burstiness)
        if mean > 0.0 {
            variance.sqrt() / mean
        } else {
            0.0
        }
    }

    /// Gets temporal features from hourly/daily counts
    async fn get_temporal_features(&self, object_id: &str) -> (u8, f64, f64) {
        let hourly = self.hourly_counts.read().await;
        let daily = self.daily_counts.read().await;

        // Peak hour
        let peak_hour = if let Some(counts) = hourly.get(object_id) {
            counts
                .iter()
                .enumerate()
                .max_by_key(|(_, &c)| c)
                .map(|(h, _)| h as u8)
                .unwrap_or(12)
        } else {
            12
        };

        // Weekend bias (ratio of weekend to weekday accesses)
        let weekend_bias = if let Some(counts) = daily.get(object_id) {
            let weekend = counts[0] + counts[6]; // Sunday + Saturday
            let weekday: u64 = counts[1..6].iter().sum();
            if weekday > 0 {
                (weekend as f64 * 5.0) / (weekday as f64 * 2.0) - 1.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Temporal entropy (how spread out are accesses across hours)
        let temporal_entropy = if let Some(counts) = hourly.get(object_id) {
            let total: u64 = counts.iter().sum();
            if total > 0 {
                let mut entropy = 0.0;
                for &count in counts.iter() {
                    if count > 0 {
                        let p = count as f64 / total as f64;
                        entropy -= p * p.ln();
                    }
                }
                // Normalize to 0-1 (max entropy is ln(24))
                entropy / (24.0_f64).ln()
            } else {
                0.0
            }
        } else {
            0.0
        };

        (peak_hour, weekend_bias, temporal_entropy)
    }

    /// Categorizes file by extension
    fn categorize_file(&self, path: &str) -> u8 {
        let ext = path
            .rsplit('.')
            .next()
            .unwrap_or("")
            .to_lowercase();

        match ext.as_str() {
            // Text/code
            "txt" | "md" | "json" | "xml" | "yaml" | "yml" | "toml" | "csv" | "log" | "rs"
            | "py" | "js" | "ts" | "go" | "java" | "c" | "cpp" | "h" | "hpp" => 1,
            // Binary/compiled
            "exe" | "dll" | "so" | "dylib" | "bin" | "o" | "a" | "lib" | "wasm" => 2,
            // Media
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "svg" | "webp" | "mp3" | "mp4" | "wav"
            | "avi" | "mkv" | "mov" | "flac" | "ogg" => 3,
            // Archives
            "zip" | "tar" | "gz" | "bz2" | "xz" | "7z" | "rar" => 4,
            // Documents
            "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx" | "odt" | "ods" => 5,
            // Data/database
            "db" | "sqlite" | "parquet" | "arrow" | "avro" | "orc" => 6,
            // Config
            "conf" | "cfg" | "ini" | "env" => 7,
            // Unknown
            _ => 0,
        }
    }

    /// Stores extracted features
    pub async fn store_features(&self, features: TieringFeatures) {
        let mut f = self.features.write().await;

        // Enforce max capacity
        while f.len() >= self.config.max_samples {
            f.pop_front();
        }

        f.push_back(features);

        self.extraction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Creates a training sample
    pub async fn create_training_sample(
        &self,
        features: TieringFeatures,
        actual_tier: TierType,
    ) -> String {
        let sample_id = format!(
            "{}_{}_{}",
            features.object_id.replace('/', "_"),
            features.timestamp,
            actual_tier as u8
        );

        let sample = TrainingSample {
            features,
            actual_tier,
            outcome: None,
        };

        let mut samples = self.training_samples.write().await;

        // Enforce max capacity
        while samples.len() >= self.config.max_samples {
            samples.pop_front();
        }

        samples.push_back(sample);

        sample_id
    }

    /// Updates training sample with outcome
    pub async fn record_outcome(
        &self,
        object_id: &str,
        timestamp: u64,
        outcome: TrainingOutcome,
    ) {
        let mut samples = self.training_samples.write().await;

        // Find matching sample
        for sample in samples.iter_mut() {
            if sample.features.object_id == object_id
                && sample.features.timestamp == timestamp
                && sample.outcome.is_none()
            {
                sample.outcome = Some(outcome);
                debug!(
                    object_id = %object_id,
                    "Recorded training outcome"
                );
                return;
            }
        }

        warn!(
            object_id = %object_id,
            timestamp = timestamp,
            "No matching sample found for outcome"
        );
    }

    /// Gets recent features for analysis
    pub async fn get_recent_features(&self, limit: usize) -> Vec<TieringFeatures> {
        let features = self.features.read().await;
        features.iter().rev().take(limit).cloned().collect()
    }

    /// Gets training samples with outcomes
    pub async fn get_training_samples(&self, with_outcome_only: bool) -> Vec<TrainingSample> {
        let samples = self.training_samples.read().await;

        if with_outcome_only {
            samples
                .iter()
                .filter(|s| s.outcome.is_some())
                .cloned()
                .collect()
        } else {
            samples.iter().cloned().collect()
        }
    }

    /// Exports training data to JSON
    pub async fn export_training_data(&self) -> Result<String> {
        let samples = self.get_training_samples(false).await;
        serde_json::to_string_pretty(&samples)
            .map_err(|e| crate::error::StrataError::Serialization(e.to_string()))
    }

    /// Exports features to CSV format for ML pipelines
    pub async fn export_features_csv(&self) -> String {
        let features = self.features.read().await;

        let mut csv = String::new();

        // Header
        csv.push_str("object_id,current_tier,timestamp,total_accesses,read_count,write_count,");
        csv.push_str("read_write_ratio,age_seconds,time_since_last_access,");
        csv.push_str("accesses_last_hour,accesses_last_day,accesses_last_week,accesses_last_month,");
        csv.push_str("avg_accesses_per_day,access_trend,heat_score,burstiness,");
        csv.push_str("size_bytes,bytes_read,bytes_written,avg_read_size,avg_write_size,");
        csv.push_str("peak_access_hour,weekend_bias,temporal_entropy,");
        csv.push_str("file_category,path_depth,hot_prefix\n");

        // Data rows
        for f in features.iter() {
            csv.push_str(&format!(
                "{},{:?},{},{},{},{},{:.4},{},{},{},{},{},{},{:.4},{:.4},{:.4},{:.4},{},{},{},{:.2},{:.2},{},{:.4},{:.4},{},{},{}\n",
                f.object_id,
                f.current_tier,
                f.timestamp,
                f.total_accesses,
                f.read_count,
                f.write_count,
                f.read_write_ratio,
                f.age_seconds,
                f.time_since_last_access,
                f.accesses_last_hour,
                f.accesses_last_day,
                f.accesses_last_week,
                f.accesses_last_month,
                f.avg_accesses_per_day,
                f.access_trend,
                f.heat_score,
                f.burstiness,
                f.size_bytes,
                f.bytes_read,
                f.bytes_written,
                f.avg_read_size,
                f.avg_write_size,
                f.peak_access_hour,
                f.weekend_bias,
                f.temporal_entropy,
                f.file_category,
                f.path_depth,
                f.hot_prefix as u8,
            ));
        }

        csv
    }

    /// Gets telemetry statistics
    pub async fn get_stats(&self) -> TelemetryStats {
        let features = self.features.read().await;
        let samples = self.training_samples.read().await;
        let samples_with_outcome = samples.iter().filter(|s| s.outcome.is_some()).count();

        TelemetryStats {
            total_extractions: self
                .extraction_count
                .load(std::sync::atomic::Ordering::Relaxed),
            features_stored: features.len(),
            training_samples: samples.len(),
            samples_with_outcome,
            hourly_buckets: self.hourly_counts.read().await.len(),
        }
    }

    /// Cleans up old data
    pub async fn cleanup(&self) {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            - self.config.retention_period.as_secs();

        // Clean old features
        {
            let mut features = self.features.write().await;
            let before = features.len();
            features.retain(|f| f.timestamp >= cutoff);
            let removed = before - features.len();
            if removed > 0 {
                info!(removed = removed, "Cleaned up old feature extractions");
            }
        }

        // Clean old training samples
        {
            let mut samples = self.training_samples.write().await;
            let before = samples.len();
            samples.retain(|s| s.features.timestamp >= cutoff);
            let removed = before - samples.len();
            if removed > 0 {
                info!(removed = removed, "Cleaned up old training samples");
            }
        }
    }

    /// Starts background tasks
    pub fn start_background_tasks(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let telemetry = self;
        tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                cleanup_interval.tick().await;
                telemetry.cleanup().await;
            }
        })
    }
}

/// Telemetry statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryStats {
    pub total_extractions: u64,
    pub features_stored: usize,
    pub training_samples: usize,
    pub samples_with_outcome: usize,
    pub hourly_buckets: usize,
}

/// Trait for ML model integration
#[async_trait::async_trait]
pub trait TieringModel: Send + Sync {
    /// Predicts optimal tier for given features
    async fn predict(&self, features: &TieringFeatures) -> Result<TierPrediction>;

    /// Batch prediction for multiple objects
    async fn predict_batch(&self, features: &[TieringFeatures]) -> Result<Vec<TierPrediction>>;

    /// Updates model with new training data (online learning)
    async fn update(&self, samples: &[TrainingSample]) -> Result<()>;

    /// Gets model metadata
    fn metadata(&self) -> ModelMetadata;
}

/// Model metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub name: String,
    pub version: String,
    pub trained_samples: u64,
    pub last_updated: Option<SystemTime>,
    pub accuracy: Option<f64>,
}

/// Simple rule-based model (baseline/fallback)
pub struct RuleBasedModel {
    metadata: ModelMetadata,
}

impl RuleBasedModel {
    pub fn new() -> Self {
        Self {
            metadata: ModelMetadata {
                name: "rule_based".to_string(),
                version: "1.0.0".to_string(),
                trained_samples: 0,
                last_updated: None,
                accuracy: None,
            },
        }
    }
}

impl Default for RuleBasedModel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TieringModel for RuleBasedModel {
    async fn predict(&self, features: &TieringFeatures) -> Result<TierPrediction> {
        let mut probabilities = HashMap::new();

        // Simple rule-based scoring
        let hot_score = self.calculate_hot_score(features);
        let warm_score = self.calculate_warm_score(features);
        let cold_score = self.calculate_cold_score(features);
        let archive_score = self.calculate_archive_score(features);

        let total = hot_score + warm_score + cold_score + archive_score;

        probabilities.insert(TierType::Hot, hot_score / total);
        probabilities.insert(TierType::Warm, warm_score / total);
        probabilities.insert(TierType::Cold, cold_score / total);
        probabilities.insert(TierType::Archive, archive_score / total);

        // Find recommended tier
        let (recommended_tier, &confidence) = probabilities
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .unwrap();

        let explanation = self.generate_explanation(features, *recommended_tier);

        Ok(TierPrediction {
            recommended_tier: *recommended_tier,
            confidence,
            tier_probabilities: probabilities,
            expected_stability_seconds: self.estimate_stability(features),
            explanation,
        })
    }

    async fn predict_batch(&self, features: &[TieringFeatures]) -> Result<Vec<TierPrediction>> {
        let mut predictions = Vec::with_capacity(features.len());
        for f in features {
            predictions.push(self.predict(f).await?);
        }
        Ok(predictions)
    }

    async fn update(&self, _samples: &[TrainingSample]) -> Result<()> {
        // Rule-based model doesn't learn
        Ok(())
    }

    fn metadata(&self) -> ModelMetadata {
        self.metadata.clone()
    }
}

impl RuleBasedModel {
    fn calculate_hot_score(&self, f: &TieringFeatures) -> f64 {
        let mut score = 1.0;

        // Recent access boost
        if f.time_since_last_access < 3600 {
            score += 3.0;
        } else if f.time_since_last_access < 86400 {
            score += 1.5;
        }

        // High heat score
        if f.heat_score > 10.0 {
            score += 2.0;
        } else if f.heat_score > 5.0 {
            score += 1.0;
        }

        // Recent access frequency
        if f.accesses_last_hour > 10 {
            score += 2.0;
        } else if f.accesses_last_day > 50 {
            score += 1.0;
        }

        // Small files preferred for hot tier
        if f.size_bytes < 1024 * 1024 {
            score += 0.5;
        }

        score
    }

    fn calculate_warm_score(&self, f: &TieringFeatures) -> f64 {
        let mut score = 1.0;

        // Moderate access
        if f.time_since_last_access >= 3600 && f.time_since_last_access < 7 * 86400 {
            score += 2.0;
        }

        if f.heat_score >= 1.0 && f.heat_score <= 10.0 {
            score += 1.5;
        }

        if f.accesses_last_week > 5 && f.accesses_last_week <= 50 {
            score += 1.0;
        }

        score
    }

    fn calculate_cold_score(&self, f: &TieringFeatures) -> f64 {
        let mut score = 1.0;

        // Infrequent access
        if f.time_since_last_access >= 7 * 86400 && f.time_since_last_access < 30 * 86400 {
            score += 2.0;
        }

        if f.heat_score > 0.1 && f.heat_score <= 1.0 {
            score += 1.5;
        }

        if f.accesses_last_month > 0 && f.accesses_last_month <= 10 {
            score += 1.0;
        }

        // Larger files
        if f.size_bytes > 100 * 1024 * 1024 {
            score += 0.5;
        }

        score
    }

    fn calculate_archive_score(&self, f: &TieringFeatures) -> f64 {
        let mut score = 1.0;

        // Very old/inactive
        if f.time_since_last_access >= 30 * 86400 {
            score += 3.0;
        }

        if f.heat_score <= 0.1 {
            score += 2.0;
        }

        if f.accesses_last_month == 0 {
            score += 2.0;
        }

        // Old object age
        if f.age_seconds > 90 * 86400 {
            score += 1.0;
        }

        score
    }

    fn estimate_stability(&self, f: &TieringFeatures) -> u64 {
        // Estimate how long object will stay in recommended tier
        if f.heat_score > 10.0 {
            // Hot objects are volatile
            3600 // 1 hour
        } else if f.heat_score > 1.0 {
            86400 // 1 day
        } else if f.heat_score > 0.1 {
            7 * 86400 // 1 week
        } else {
            30 * 86400 // 1 month
        }
    }

    fn generate_explanation(&self, f: &TieringFeatures, tier: TierType) -> String {
        match tier {
            TierType::Hot => format!(
                "High activity: {} accesses/day, heat={:.2}, last access {}s ago",
                f.avg_accesses_per_day, f.heat_score, f.time_since_last_access
            ),
            TierType::Warm => format!(
                "Moderate activity: {} accesses/day, heat={:.2}",
                f.avg_accesses_per_day, f.heat_score
            ),
            TierType::Cold => format!(
                "Low activity: {} accesses/day, {} days since last access",
                f.avg_accesses_per_day,
                f.time_since_last_access / 86400
            ),
            TierType::Archive => format!(
                "Inactive: {} days since last access, heat={:.4}",
                f.time_since_last_access / 86400,
                f.heat_score
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_extraction_defaults() {
        let features = TieringFeatures::default();
        assert_eq!(features.total_accesses, 0);
        assert_eq!(features.read_write_ratio, 0.5);
    }

    #[test]
    fn test_file_categorization() {
        let telemetry = MLTelemetry::default_telemetry();

        assert_eq!(telemetry.categorize_file("/path/to/file.rs"), 1); // Text/code
        assert_eq!(telemetry.categorize_file("/path/to/file.exe"), 2); // Binary
        assert_eq!(telemetry.categorize_file("/path/to/file.mp4"), 3); // Media
        assert_eq!(telemetry.categorize_file("/path/to/file.zip"), 4); // Archive
        assert_eq!(telemetry.categorize_file("/path/to/file.pdf"), 5); // Document
        assert_eq!(telemetry.categorize_file("/path/to/file.db"), 6); // Data
        assert_eq!(telemetry.categorize_file("/path/to/file.conf"), 7); // Config
        assert_eq!(telemetry.categorize_file("/path/to/file.unknown"), 0); // Unknown
    }

    #[tokio::test]
    async fn test_rule_based_prediction() {
        let model = RuleBasedModel::new();

        // Hot object features
        let hot_features = TieringFeatures {
            heat_score: 15.0,
            time_since_last_access: 60,
            accesses_last_hour: 20,
            ..Default::default()
        };

        let prediction = model.predict(&hot_features).await.unwrap();
        assert_eq!(prediction.recommended_tier, TierType::Hot);
        assert!(prediction.confidence > 0.3);

        // Cold object features
        let cold_features = TieringFeatures {
            heat_score: 0.05,
            time_since_last_access: 60 * 86400,
            accesses_last_month: 0,
            age_seconds: 180 * 86400,
            ..Default::default()
        };

        let prediction = model.predict(&cold_features).await.unwrap();
        assert_eq!(prediction.recommended_tier, TierType::Archive);
    }

    #[tokio::test]
    async fn test_telemetry_storage() {
        let telemetry = MLTelemetry::default_telemetry();

        let features = TieringFeatures {
            object_id: "/test/file.txt".to_string(),
            timestamp: 12345,
            ..Default::default()
        };

        telemetry.store_features(features.clone()).await;

        let recent = telemetry.get_recent_features(10).await;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].object_id, "/test/file.txt");
    }

    #[tokio::test]
    async fn test_training_sample() {
        let telemetry = MLTelemetry::default_telemetry();

        let features = TieringFeatures {
            object_id: "/test/file.txt".to_string(),
            timestamp: 12345,
            ..Default::default()
        };

        let sample_id = telemetry
            .create_training_sample(features, TierType::Warm)
            .await;

        assert!(!sample_id.is_empty());

        let samples = telemetry.get_training_samples(false).await;
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].actual_tier, TierType::Warm);
    }

    #[tokio::test]
    async fn test_csv_export() {
        let telemetry = MLTelemetry::default_telemetry();

        let features = TieringFeatures {
            object_id: "/test/file.txt".to_string(),
            current_tier: TierType::Hot,
            timestamp: 12345,
            total_accesses: 100,
            ..Default::default()
        };

        telemetry.store_features(features).await;

        let csv = telemetry.export_features_csv().await;
        assert!(csv.contains("object_id"));
        assert!(csv.contains("/test/file.txt"));
    }
}
