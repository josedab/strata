//! Error types for the Strata operator

use thiserror::Error;

/// Main error type for the operator
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("CRD not installed. Please install the Strata CRDs first.")]
    CrdNotInstalled,

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Reconciliation failed: {0}")]
    ReconcileFailed(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Timeout waiting for {0}")]
    Timeout(String),

    #[error("Cluster is not ready")]
    ClusterNotReady,

    #[error("Invalid cluster state: {0}")]
    InvalidState(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}
