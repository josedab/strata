// Data Tiering Engine - Automated lifecycle management across storage tiers
//
// Provides intelligent data placement based on access patterns, age, and policies.
// Supports hot (NVMe/SSD), warm (HDD), cold (object storage), and archive tiers.
//
// The ML telemetry module enables smart tiering through:
// - Feature extraction for ML model training
// - Temporal pattern analysis
// - Training sample collection with outcome tracking
// - Pluggable model interface for custom ML implementations

pub mod engine;
pub mod policy;
pub mod tracker;
pub mod migration;
pub mod ml_telemetry;

pub use engine::TieringEngine;
pub use policy::{TieringPolicy, TieringRule, TierType};
pub use tracker::AccessTracker;
pub use migration::MigrationManager;
pub use ml_telemetry::{
    MLTelemetry, MLTelemetryConfig, TieringFeatures, TierPrediction,
    TieringModel, RuleBasedModel, TrainingSample, TrainingOutcome,
};
