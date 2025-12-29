//! Data Lineage and Catalog for Strata
//!
//! Provides data governance and discovery capabilities:
//! - Data catalog with metadata management
//! - Lineage tracking and visualization
//! - Schema registry and evolution
//! - Asset discovery and classification
//! - Impact analysis

pub mod catalog;
pub mod discovery;
pub mod graph;
pub mod schema;

pub use catalog::{Catalog, CatalogConfig, Asset, AssetType};
pub use discovery::{Discovery, DiscoveryConfig, DiscoveryResult};
pub use graph::{LineageGraph, LineageNode, LineageEdge, Provenance};
pub use schema::{SchemaRegistry, SchemaVersion, SchemaEvolution};
