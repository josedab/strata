// ML Model Serving Infrastructure

use super::inference::{InferenceConfig, InferenceEngine, InferenceRequest, TensorData};
use super::registry::{ModelRegistry, ModelStatus, RegistryConfig};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Serving configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServingConfig {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
    /// Number of workers
    pub workers: usize,
    /// Request timeout ms
    pub timeout_ms: u64,
    /// Maximum request body size
    pub max_request_size: usize,
    /// Enable TLS
    pub tls: bool,
    /// TLS certificate path
    pub cert_path: Option<String>,
    /// TLS key path
    pub key_path: Option<String>,
    /// Enable health endpoint
    pub health_endpoint: bool,
    /// Enable metrics endpoint
    pub metrics_endpoint: bool,
    /// Enable CORS
    pub cors: bool,
    /// Allowed origins for CORS
    pub allowed_origins: Vec<String>,
    /// Rate limiting
    pub rate_limit: Option<RateLimitConfig>,
    /// Authentication
    pub auth: Option<AuthConfig>,
}

impl Default for ServingConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            workers: 4,
            timeout_ms: 30000,
            max_request_size: 100 * 1024 * 1024, // 100MB
            tls: false,
            cert_path: None,
            key_path: None,
            health_endpoint: true,
            metrics_endpoint: true,
            cors: true,
            allowed_origins: vec!["*".to_string()],
            rate_limit: None,
            auth: None,
        }
    }
}

/// Rate limit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Requests per second
    pub requests_per_second: u32,
    /// Burst size
    pub burst_size: u32,
    /// Enable per-client limits
    pub per_client: bool,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication
    pub enabled: bool,
    /// Auth type
    pub auth_type: AuthType,
    /// API keys (for ApiKey auth)
    pub api_keys: Vec<String>,
    /// JWT secret (for JWT auth)
    pub jwt_secret: Option<String>,
}

/// Authentication type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    /// No authentication
    None,
    /// API key authentication
    ApiKey,
    /// JWT authentication
    Jwt,
    /// Basic authentication
    Basic,
}

/// Serving endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServingEndpoint {
    /// Endpoint name
    pub name: String,
    /// Model name
    pub model: String,
    /// Model version (None for latest)
    pub version: Option<u32>,
    /// Endpoint path
    pub path: String,
    /// Traffic weight (for A/B testing)
    pub weight: f32,
    /// Is active
    pub active: bool,
    /// Created timestamp
    pub created_at: u64,
    /// Updated timestamp
    pub updated_at: u64,
    /// Configuration overrides
    pub config: EndpointConfig,
}

/// Endpoint configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointConfig {
    /// Override batch size
    pub batch_size: Option<usize>,
    /// Override timeout
    pub timeout_ms: Option<u64>,
    /// Custom preprocessing
    pub preprocessing: HashMap<String, serde_json::Value>,
    /// Custom postprocessing
    pub postprocessing: HashMap<String, serde_json::Value>,
    /// Response format
    pub response_format: ResponseFormat,
}

/// Response format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResponseFormat {
    #[default]
    Json,
    Protobuf,
    Msgpack,
    Raw,
}

/// Model server
pub struct ModelServer {
    /// Serving config
    config: ServingConfig,
    /// Model registry
    registry: Arc<ModelRegistry>,
    /// Inference engine
    engine: Arc<InferenceEngine>,
    /// Endpoints
    endpoints: Arc<RwLock<HashMap<String, ServingEndpoint>>>,
    /// Traffic routing rules
    routing: Arc<RwLock<TrafficRouting>>,
    /// Server statistics
    stats: Arc<ServerStats>,
    /// Running flag
    running: Arc<RwLock<bool>>,
}

/// Traffic routing for A/B testing and canary deployments
#[derive(Debug, Clone, Default)]
pub struct TrafficRouting {
    /// Routes by path
    routes: HashMap<String, Vec<RouteRule>>,
    /// Default model per path
    defaults: HashMap<String, String>,
}

/// Route rule
#[derive(Debug, Clone)]
pub struct RouteRule {
    /// Endpoint name
    pub endpoint: String,
    /// Traffic percentage (0.0 - 1.0)
    pub weight: f32,
    /// Conditions
    pub conditions: Vec<RouteCondition>,
}

/// Route condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouteCondition {
    /// Header match
    Header { name: String, value: String },
    /// Query parameter match
    Query { name: String, value: String },
    /// User ID hash (for consistent routing)
    UserHash { bucket: u32, total_buckets: u32 },
    /// Random percentage
    Random { percentage: f32 },
}

/// Server statistics
pub struct ServerStats {
    /// Total requests
    pub total_requests: AtomicU64,
    /// Successful requests
    pub successful_requests: AtomicU64,
    /// Failed requests
    pub failed_requests: AtomicU64,
    /// Total latency us
    pub total_latency_us: AtomicU64,
    /// Active connections
    pub active_connections: AtomicU64,
    /// Requests per endpoint
    pub endpoint_requests: RwLock<HashMap<String, u64>>,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            endpoint_requests: RwLock::new(HashMap::new()),
        }
    }
}

impl ModelServer {
    /// Creates a new model server
    pub fn new(
        config: ServingConfig,
        registry_config: RegistryConfig,
        inference_config: InferenceConfig,
    ) -> Self {
        Self {
            config,
            registry: Arc::new(ModelRegistry::new(registry_config)),
            engine: Arc::new(InferenceEngine::new(inference_config)),
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            routing: Arc::new(RwLock::new(TrafficRouting::default())),
            stats: Arc::new(ServerStats::default()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Creates with existing registry and engine
    pub fn with_components(
        config: ServingConfig,
        registry: Arc<ModelRegistry>,
        engine: Arc<InferenceEngine>,
    ) -> Self {
        Self {
            config,
            registry,
            engine,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            routing: Arc::new(RwLock::new(TrafficRouting::default())),
            stats: Arc::new(ServerStats::default()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts the server
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }

        // Start inference engine
        self.engine.start().await?;

        // Load deployed models
        self.load_deployed_models().await?;

        *running = true;

        // In a real implementation, this would start an HTTP server
        // using axum, actix-web, or similar framework

        Ok(())
    }

    /// Stops the server
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;

        self.engine.stop().await?;

        Ok(())
    }

    /// Loads all deployed models from registry
    async fn load_deployed_models(&self) -> Result<()> {
        let deployed = self.registry.list_deployed().await;

        for (name, version_info) in deployed {
            // Get full model from registry
            if let Some(model_version) = self.registry.get(&name, Some(version_info.version)).await {
                // Create model from version info
                let model = super::model::Model::new(
                    model_version.config.clone(),
                    model_version.metadata.clone(),
                );

                self.engine.load_model(model, model_version.version).await?;

                // Create default endpoint
                let endpoint = ServingEndpoint {
                    name: format!("{}-v{}", name, model_version.version),
                    model: name.clone(),
                    version: Some(model_version.version),
                    path: format!("/v1/models/{}/predict", name),
                    weight: 1.0,
                    active: true,
                    created_at: model_version.created_at,
                    updated_at: model_version.deployed_at.unwrap_or(model_version.created_at),
                    config: EndpointConfig::default(),
                };

                let mut endpoints = self.endpoints.write().await;
                endpoints.insert(endpoint.name.clone(), endpoint);
            }
        }

        Ok(())
    }

    /// Creates an endpoint
    pub async fn create_endpoint(&self, endpoint: ServingEndpoint) -> Result<()> {
        // Validate model exists
        let model_version = self.registry.get(&endpoint.model, endpoint.version).await
            .ok_or_else(|| StrataError::NotFound(
                format!("Model {} not found", endpoint.model)
            ))?;

        if model_version.status != ModelStatus::Ready && model_version.status != ModelStatus::Deployed {
            return Err(StrataError::InvalidOperation(
                format!("Model {} is not ready for serving", endpoint.model)
            ));
        }

        // Ensure model is loaded
        let model = super::model::Model::new(
            model_version.config.clone(),
            model_version.metadata.clone(),
        );
        self.engine.load_model(model, model_version.version).await?;

        // Add endpoint
        let mut endpoints = self.endpoints.write().await;
        endpoints.insert(endpoint.name.clone(), endpoint);

        Ok(())
    }

    /// Updates an endpoint
    pub async fn update_endpoint(&self, name: &str, updates: EndpointUpdate) -> Result<()> {
        let mut endpoints = self.endpoints.write().await;

        let endpoint = endpoints.get_mut(name)
            .ok_or_else(|| StrataError::NotFound(format!("Endpoint {} not found", name)))?;

        if let Some(weight) = updates.weight {
            endpoint.weight = weight;
        }
        if let Some(active) = updates.active {
            endpoint.active = active;
        }
        if let Some(config) = updates.config {
            endpoint.config = config;
        }

        endpoint.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Ok(())
    }

    /// Deletes an endpoint
    pub async fn delete_endpoint(&self, name: &str) -> Result<bool> {
        let mut endpoints = self.endpoints.write().await;
        Ok(endpoints.remove(name).is_some())
    }

    /// Lists endpoints
    pub async fn list_endpoints(&self) -> Vec<ServingEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints.values().cloned().collect()
    }

    /// Gets endpoint by name
    pub async fn get_endpoint(&self, name: &str) -> Option<ServingEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints.get(name).cloned()
    }

    /// Handles inference request
    pub async fn predict(&self, path: &str, request: PredictRequest) -> Result<PredictResponse> {
        let start = std::time::Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Find endpoint for path
        let endpoint = self.find_endpoint(path, &request.context).await?;

        // Record endpoint request
        {
            let mut counts = self.stats.endpoint_requests.write().await;
            *counts.entry(endpoint.name.clone()).or_insert(0) += 1;
        }

        // Build inference request
        let inference_request = InferenceRequest::new(&endpoint.model)
            .with_version(endpoint.version.unwrap_or(1));

        // Add inputs
        let mut inference_request = inference_request;
        for (name, data) in request.inputs {
            inference_request.inputs.insert(name, data);
        }

        // Run inference
        let response = self.engine.infer(inference_request).await?;

        let latency = start.elapsed();
        self.stats.total_latency_us.fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.stats.successful_requests.fetch_add(1, Ordering::Relaxed);

        Ok(PredictResponse {
            model: endpoint.model,
            version: endpoint.version.unwrap_or(1),
            outputs: response.outputs,
            inference_time_us: response.inference_time_us,
            endpoint: endpoint.name,
        })
    }

    /// Finds endpoint for path with traffic routing
    async fn find_endpoint(&self, path: &str, context: &RequestContext) -> Result<ServingEndpoint> {
        let endpoints = self.endpoints.read().await;
        let routing = self.routing.read().await;

        // Check for explicit routing rules
        if let Some(rules) = routing.routes.get(path) {
            for rule in rules {
                if self.matches_conditions(&rule.conditions, context) {
                    if let Some(endpoint) = endpoints.get(&rule.endpoint) {
                        if endpoint.active {
                            return Ok(endpoint.clone());
                        }
                    }
                }
            }
        }

        // Find endpoints matching path
        let matching: Vec<_> = endpoints.values()
            .filter(|e| e.path == path && e.active)
            .collect();

        if matching.is_empty() {
            return Err(StrataError::NotFound(format!("No endpoint for path: {}", path)));
        }

        // Weight-based selection
        if matching.len() == 1 {
            return Ok(matching[0].clone());
        }

        let total_weight: f32 = matching.iter().map(|e| e.weight).sum();
        let random: f32 = rand::random::<f32>() * total_weight;

        let mut cumulative = 0.0;
        for endpoint in &matching {
            cumulative += endpoint.weight;
            if random <= cumulative {
                return Ok((*endpoint).clone());
            }
        }

        Ok(matching[0].clone())
    }

    /// Checks if route conditions match
    fn matches_conditions(&self, conditions: &[RouteCondition], context: &RequestContext) -> bool {
        for condition in conditions {
            match condition {
                RouteCondition::Header { name, value } => {
                    if context.headers.get(name) != Some(value) {
                        return false;
                    }
                }
                RouteCondition::Query { name, value } => {
                    if context.query_params.get(name) != Some(value) {
                        return false;
                    }
                }
                RouteCondition::UserHash { bucket, total_buckets } => {
                    if let Some(user_id) = &context.user_id {
                        let hash = self.simple_hash(user_id) % (*total_buckets as u64);
                        if hash != *bucket as u64 {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RouteCondition::Random { percentage } => {
                    if rand::random::<f32>() > *percentage {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Simple hash function for user ID routing
    fn simple_hash(&self, s: &str) -> u64 {
        let mut hash: u64 = 5381;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
        hash
    }

    /// Sets up traffic routing
    pub async fn set_routing(&self, path: &str, rules: Vec<RouteRule>) -> Result<()> {
        let mut routing = self.routing.write().await;
        routing.routes.insert(path.to_string(), rules);
        Ok(())
    }

    /// Creates A/B test
    pub async fn create_ab_test(&self, test: ABTest) -> Result<()> {
        // Create endpoints for each variant
        for variant in &test.variants {
            let endpoint = ServingEndpoint {
                name: format!("{}-{}", test.name, variant.name),
                model: variant.model.clone(),
                version: variant.version,
                path: test.path.clone(),
                weight: variant.traffic_percentage / 100.0,
                active: true,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                config: EndpointConfig::default(),
            };

            self.create_endpoint(endpoint).await?;
        }

        Ok(())
    }

    /// Creates canary deployment
    pub async fn create_canary(&self, canary: CanaryDeployment) -> Result<()> {
        // Create canary endpoint with small traffic percentage
        let canary_endpoint = ServingEndpoint {
            name: format!("{}-canary", canary.model),
            model: canary.model.clone(),
            version: Some(canary.new_version),
            path: canary.path.clone(),
            weight: canary.initial_percentage / 100.0,
            active: true,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            config: EndpointConfig::default(),
        };

        self.create_endpoint(canary_endpoint).await?;

        // Update production endpoint weight
        if let Some(production_endpoint) = self.get_endpoint(&format!("{}-production", canary.model)).await {
            self.update_endpoint(
                &production_endpoint.name,
                EndpointUpdate {
                    weight: Some((100.0 - canary.initial_percentage) / 100.0),
                    active: None,
                    config: None,
                },
            ).await?;
        }

        Ok(())
    }

    /// Promotes canary to production
    pub async fn promote_canary(&self, model: &str) -> Result<()> {
        let canary_name = format!("{}-canary", model);
        let production_name = format!("{}-production", model);

        // Get canary endpoint
        let canary = self.get_endpoint(&canary_name).await
            .ok_or_else(|| StrataError::NotFound(format!("Canary endpoint not found for {}", model)))?;

        // Update production to use canary version
        if let Some(_production) = self.get_endpoint(&production_name).await {
            self.update_endpoint(
                &production_name,
                EndpointUpdate {
                    weight: Some(1.0),
                    active: Some(true),
                    config: None,
                },
            ).await?;

            // Update to canary model version
            let mut endpoints = self.endpoints.write().await;
            if let Some(prod) = endpoints.get_mut(&production_name) {
                prod.version = canary.version;
            }
        }

        // Deactivate canary
        self.update_endpoint(
            &canary_name,
            EndpointUpdate {
                weight: Some(0.0),
                active: Some(false),
                config: None,
            },
        ).await?;

        Ok(())
    }

    /// Rollback canary
    pub async fn rollback_canary(&self, model: &str) -> Result<()> {
        let canary_name = format!("{}-canary", model);
        let production_name = format!("{}-production", model);

        // Restore production weight
        self.update_endpoint(
            &production_name,
            EndpointUpdate {
                weight: Some(1.0),
                active: Some(true),
                config: None,
            },
        ).await?;

        // Deactivate canary
        self.update_endpoint(
            &canary_name,
            EndpointUpdate {
                weight: Some(0.0),
                active: Some(false),
                config: None,
            },
        ).await?;

        Ok(())
    }

    /// Gets server statistics
    pub async fn stats(&self) -> ServerStatsSnapshot {
        let total = self.stats.total_requests.load(Ordering::Relaxed);
        let successful = self.stats.successful_requests.load(Ordering::Relaxed);
        let total_latency = self.stats.total_latency_us.load(Ordering::Relaxed);

        let endpoint_stats = self.stats.endpoint_requests.read().await.clone();

        ServerStatsSnapshot {
            total_requests: total,
            successful_requests: successful,
            failed_requests: self.stats.failed_requests.load(Ordering::Relaxed),
            avg_latency_us: if successful > 0 { total_latency / successful } else { 0 },
            active_connections: self.stats.active_connections.load(Ordering::Relaxed),
            requests_per_endpoint: endpoint_stats,
        }
    }

    /// Gets server health
    pub async fn health(&self) -> ServerHealth {
        let running = *self.running.read().await;
        let models = self.engine.list_models().await;
        let endpoints = self.endpoints.read().await;
        let active_endpoints = endpoints.values().filter(|e| e.active).count();

        ServerHealth {
            status: if running { HealthStatus::Healthy } else { HealthStatus::Unhealthy },
            server_running: running,
            models_loaded: models.len(),
            active_endpoints,
            total_endpoints: endpoints.len(),
            uptime_seconds: 0, // Would track actual uptime
        }
    }

    /// Gets registry reference
    pub fn registry(&self) -> Arc<ModelRegistry> {
        self.registry.clone()
    }

    /// Gets engine reference
    pub fn engine(&self) -> Arc<InferenceEngine> {
        self.engine.clone()
    }
}

/// Endpoint update
#[derive(Debug, Clone, Default)]
pub struct EndpointUpdate {
    pub weight: Option<f32>,
    pub active: Option<bool>,
    pub config: Option<EndpointConfig>,
}

/// Predict request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictRequest {
    /// Input tensors
    pub inputs: HashMap<String, TensorData>,
    /// Request context
    #[serde(default)]
    pub context: RequestContext,
}

/// Request context
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestContext {
    /// User ID
    pub user_id: Option<String>,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Query parameters
    pub query_params: HashMap<String, String>,
}

/// Predict response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictResponse {
    /// Model name
    pub model: String,
    /// Model version
    pub version: u32,
    /// Output tensors
    pub outputs: HashMap<String, TensorData>,
    /// Inference time
    pub inference_time_us: u64,
    /// Endpoint used
    pub endpoint: String,
}

/// A/B test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABTest {
    /// Test name
    pub name: String,
    /// Endpoint path
    pub path: String,
    /// Test variants
    pub variants: Vec<ABVariant>,
    /// Start time
    pub start_at: Option<u64>,
    /// End time
    pub end_at: Option<u64>,
}

/// A/B test variant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABVariant {
    /// Variant name
    pub name: String,
    /// Model name
    pub model: String,
    /// Model version
    pub version: Option<u32>,
    /// Traffic percentage (0-100)
    pub traffic_percentage: f32,
}

/// Canary deployment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryDeployment {
    /// Model name
    pub model: String,
    /// Endpoint path
    pub path: String,
    /// New version to deploy
    pub new_version: u32,
    /// Initial traffic percentage
    pub initial_percentage: f32,
    /// Step size for gradual rollout
    pub step_percentage: f32,
    /// Rollout interval seconds
    pub interval_seconds: u64,
    /// Auto-promote if metrics are good
    pub auto_promote: bool,
    /// Metric thresholds for promotion
    pub thresholds: Option<CanaryThresholds>,
}

/// Canary metric thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryThresholds {
    /// Maximum error rate
    pub max_error_rate: f32,
    /// Maximum latency percentile 95
    pub max_latency_p95_ms: u64,
    /// Minimum success rate
    pub min_success_rate: f32,
}

/// Server statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_latency_us: u64,
    pub active_connections: u64,
    pub requests_per_endpoint: HashMap<String, u64>,
}

/// Server health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHealth {
    pub status: HealthStatus,
    pub server_running: bool,
    pub models_loaded: usize,
    pub active_endpoints: usize,
    pub total_endpoints: usize,
    pub uptime_seconds: u64,
}

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_model_server_creation() {
        let server = ModelServer::new(
            ServingConfig::default(),
            RegistryConfig::default(),
            InferenceConfig::default(),
        );

        let endpoints = server.list_endpoints().await;
        assert!(endpoints.is_empty());
    }

    #[tokio::test]
    async fn test_server_health() {
        let server = ModelServer::new(
            ServingConfig::default(),
            RegistryConfig::default(),
            InferenceConfig::default(),
        );

        let health = server.health().await;
        assert_eq!(health.status, HealthStatus::Unhealthy); // Not started
        assert!(!health.server_running);
    }

    #[tokio::test]
    async fn test_endpoint_lifecycle() {
        let server = ModelServer::new(
            ServingConfig::default(),
            RegistryConfig::default(),
            InferenceConfig::default(),
        );
        server.start().await.unwrap();

        // Register a model first
        use super::super::model::{ModelBuilder, ModelFormat, TensorSpec, DataType};

        let model = ModelBuilder::new("test_model")
            .format(ModelFormat::Onnx)
            .version("1.0")
            .input(TensorSpec::new("input", DataType::Float32, vec![-1, 10]))
            .output(TensorSpec::new("output", DataType::Float32, vec![-1, 5]))
            .build();

        server.registry().register(model).await.unwrap();
        server.registry().deploy("test_model", 1).await.unwrap();

        // Create endpoint
        let endpoint = ServingEndpoint {
            name: "test-endpoint".to_string(),
            model: "test_model".to_string(),
            version: Some(1),
            path: "/v1/models/test/predict".to_string(),
            weight: 1.0,
            active: true,
            created_at: 0,
            updated_at: 0,
            config: EndpointConfig::default(),
        };

        server.create_endpoint(endpoint).await.unwrap();

        let endpoints = server.list_endpoints().await;
        assert!(!endpoints.is_empty());

        // Delete endpoint
        server.delete_endpoint("test-endpoint").await.unwrap();

        let endpoints = server.list_endpoints().await;
        let found = endpoints.iter().find(|e| e.name == "test-endpoint");
        assert!(found.is_none());

        server.stop().await.unwrap();
    }

    #[test]
    fn test_serving_config_defaults() {
        let config = ServingConfig::default();
        assert_eq!(config.port, 8080);
        assert!(config.health_endpoint);
        assert!(config.metrics_endpoint);
    }
}
