//! ML Model Serving for Strata
//!
//! Provides model management and inference capabilities:
//! - Model registry and versioning
//! - Model serving and inference
//! - A/B testing and traffic splitting
//! - Auto-scaling and load balancing
//! - Multi-framework support (ONNX, TensorFlow, PyTorch)

pub mod inference;
pub mod model;
pub mod registry;
pub mod serving;

pub use inference::{InferenceEngine, InferenceRequest, InferenceResponse};
pub use model::{Model, ModelConfig, ModelFormat, ModelMetadata};
pub use registry::{ModelRegistry, ModelVersion, RegistryConfig};
pub use serving::{ServingConfig, ServingEndpoint, ModelServer};
