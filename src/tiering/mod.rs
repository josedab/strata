// Data Tiering Engine - Automated lifecycle management across storage tiers
//
// Provides intelligent data placement based on access patterns, age, and policies.
// Supports hot (NVMe/SSD), warm (HDD), cold (object storage), and archive tiers.

pub mod engine;
pub mod policy;
pub mod tracker;
pub mod migration;

pub use engine::TieringEngine;
pub use policy::{TieringPolicy, TieringRule, TierType};
pub use tracker::AccessTracker;
pub use migration::MigrationManager;
