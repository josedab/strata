//! Health check endpoints and probes for Strata.
//!
//! Provides comprehensive health checking for Kubernetes-style deployments.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// Arc reserved for future shared state
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Overall health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Service is healthy.
    Healthy,
    /// Service is degraded but operational.
    Degraded,
    /// Service is unhealthy.
    Unhealthy,
}

impl HealthStatus {
    /// Convert to HTTP status code.
    pub fn to_status_code(&self) -> u16 {
        match self {
            HealthStatus::Healthy => 200,
            HealthStatus::Degraded => 200, // Still operational
            HealthStatus::Unhealthy => 503,
        }
    }

    /// Combine two statuses (worst wins).
    pub fn combine(&self, other: &HealthStatus) -> HealthStatus {
        match (self, other) {
            (HealthStatus::Unhealthy, _) | (_, HealthStatus::Unhealthy) => HealthStatus::Unhealthy,
            (HealthStatus::Degraded, _) | (_, HealthStatus::Degraded) => HealthStatus::Degraded,
            _ => HealthStatus::Healthy,
        }
    }
}

/// Individual component health check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name.
    pub name: String,
    /// Health status.
    pub status: HealthStatus,
    /// Optional message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Check latency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Additional details.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub details: HashMap<String, String>,
}

impl ComponentHealth {
    /// Create a healthy component.
    pub fn healthy(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Healthy,
            message: None,
            latency_ms: None,
            details: HashMap::new(),
        }
    }

    /// Create a degraded component.
    pub fn degraded(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Degraded,
            message: Some(message.into()),
            latency_ms: None,
            details: HashMap::new(),
        }
    }

    /// Create an unhealthy component.
    pub fn unhealthy(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Unhealthy,
            message: Some(message.into()),
            latency_ms: None,
            details: HashMap::new(),
        }
    }

    /// Add latency.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency_ms = Some(latency.as_millis() as u64);
        self
    }

    /// Add detail.
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }
}

/// Full health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Overall status.
    pub status: HealthStatus,
    /// Service version.
    pub version: String,
    /// Uptime in seconds.
    pub uptime_seconds: u64,
    /// Individual component checks.
    pub components: Vec<ComponentHealth>,
    /// Timestamp.
    pub timestamp: String,
}

impl HealthResponse {
    /// Create a new health response.
    pub fn new(version: impl Into<String>, start_time: Instant) -> Self {
        Self {
            status: HealthStatus::Healthy,
            version: version.into(),
            uptime_seconds: start_time.elapsed().as_secs(),
            components: Vec::new(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Add a component check.
    pub fn add_component(&mut self, component: ComponentHealth) {
        self.status = self.status.combine(&component.status);
        self.components.push(component);
    }

    /// Build response from components.
    pub fn with_components(mut self, components: Vec<ComponentHealth>) -> Self {
        for component in components {
            self.add_component(component);
        }
        self
    }
}

/// Readiness check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessResponse {
    /// Whether the service is ready to accept traffic.
    pub ready: bool,
    /// Reason if not ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Dependencies status.
    pub dependencies: Vec<DependencyStatus>,
}

impl ReadinessResponse {
    /// Create a ready response.
    pub fn ready() -> Self {
        Self {
            ready: true,
            reason: None,
            dependencies: Vec::new(),
        }
    }

    /// Create a not-ready response.
    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            reason: Some(reason.into()),
            dependencies: Vec::new(),
        }
    }

    /// Add dependency status.
    pub fn with_dependency(mut self, dep: DependencyStatus) -> Self {
        if !dep.available {
            self.ready = false;
            if self.reason.is_none() {
                self.reason = Some(format!("Dependency '{}' unavailable", dep.name));
            }
        }
        self.dependencies.push(dep);
        self
    }
}

/// Dependency status for readiness checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyStatus {
    /// Dependency name.
    pub name: String,
    /// Whether it's available.
    pub available: bool,
    /// Optional error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Latency to check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

impl DependencyStatus {
    /// Create an available dependency.
    pub fn available(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: true,
            error: None,
            latency_ms: None,
        }
    }

    /// Create an unavailable dependency.
    pub fn unavailable(name: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: false,
            error: Some(error.into()),
            latency_ms: None,
        }
    }

    /// Add latency.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency_ms = Some(latency.as_millis() as u64);
        self
    }
}

/// Liveness check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessResponse {
    /// Whether the service is alive.
    pub alive: bool,
    /// Reason if not alive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl LivenessResponse {
    /// Create an alive response.
    pub fn alive() -> Self {
        Self {
            alive: true,
            reason: None,
        }
    }

    /// Create a not-alive response.
    pub fn not_alive(reason: impl Into<String>) -> Self {
        Self {
            alive: false,
            reason: Some(reason.into()),
        }
    }
}

/// Health checker for running periodic health checks.
pub struct HealthChecker {
    /// Service name.
    #[allow(dead_code)]
    name: String,
    /// Service version.
    version: String,
    /// When the service started.
    start_time: Instant,
    /// Registered health check functions.
    checks: RwLock<Vec<HealthCheck>>,
    /// Registered readiness checks.
    readiness_checks: RwLock<Vec<ReadinessCheck>>,
    /// Last health status.
    last_status: RwLock<Option<HealthResponse>>,
}

/// A health check function.
pub struct HealthCheck {
    /// Component name.
    #[allow(dead_code)]
    name: String,
    /// Check function.
    check: Box<dyn Fn() -> ComponentHealth + Send + Sync>,
}

/// A readiness check function.
pub struct ReadinessCheck {
    /// Dependency name.
    #[allow(dead_code)]
    name: String,
    /// Check function (returns true if ready).
    check: Box<dyn Fn() -> DependencyStatus + Send + Sync>,
}

impl HealthChecker {
    /// Create a new health checker.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            start_time: Instant::now(),
            checks: RwLock::new(Vec::new()),
            readiness_checks: RwLock::new(Vec::new()),
            last_status: RwLock::new(None),
        }
    }

    /// Register a health check.
    pub async fn register_check<F>(&self, name: impl Into<String>, check: F)
    where
        F: Fn() -> ComponentHealth + Send + Sync + 'static,
    {
        let mut checks = self.checks.write().await;
        checks.push(HealthCheck {
            name: name.into(),
            check: Box::new(check),
        });
    }

    /// Register a readiness check.
    pub async fn register_readiness<F>(&self, name: impl Into<String>, check: F)
    where
        F: Fn() -> DependencyStatus + Send + Sync + 'static,
    {
        let mut checks = self.readiness_checks.write().await;
        checks.push(ReadinessCheck {
            name: name.into(),
            check: Box::new(check),
        });
    }

    /// Run health checks and return response.
    pub async fn check_health(&self) -> HealthResponse {
        let mut response = HealthResponse::new(&self.version, self.start_time);

        let checks = self.checks.read().await;
        for check in checks.iter() {
            let start = Instant::now();
            let mut component = (check.check)();
            component.latency_ms = Some(start.elapsed().as_millis() as u64);
            response.add_component(component);
        }

        // Cache the response
        *self.last_status.write().await = Some(response.clone());

        response
    }

    /// Run readiness checks.
    pub async fn check_readiness(&self) -> ReadinessResponse {
        let mut response = ReadinessResponse::ready();

        let checks = self.readiness_checks.read().await;
        for check in checks.iter() {
            let start = Instant::now();
            let mut status = (check.check)();
            status.latency_ms = Some(start.elapsed().as_millis() as u64);
            response = response.with_dependency(status);
        }

        response
    }

    /// Run liveness check (simple check that the service is responding).
    pub fn check_liveness(&self) -> LivenessResponse {
        // A liveness check should be minimal - just verify the service is running
        LivenessResponse::alive()
    }

    /// Get the last cached health status.
    pub async fn last_status(&self) -> Option<HealthResponse> {
        self.last_status.read().await.clone()
    }

    /// Get uptime.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Pre-built health checks for common components.
pub mod checks {
    use super::*;

    /// Check disk space.
    pub fn disk_space(path: &str, _min_free_bytes: u64) -> ComponentHealth {
        // In a real implementation, check actual disk space
        // For now, return healthy
        ComponentHealth::healthy("disk_space").with_detail("path", path)
    }

    /// Check memory usage.
    pub fn memory_usage(_max_percent: f64) -> ComponentHealth {
        // In a real implementation, check actual memory usage
        ComponentHealth::healthy("memory")
    }

    /// Check if a port is listening.
    pub fn port_listening(port: u16) -> ComponentHealth {
        // In a real implementation, check if port is bound
        ComponentHealth::healthy("port").with_detail("port", port.to_string())
    }

    /// Create a custom check that pings an endpoint.
    pub fn http_endpoint(name: &str, _url: &str, _timeout: Duration) -> DependencyStatus {
        // In a real implementation, make an HTTP request
        // For now, return available
        DependencyStatus::available(name).with_latency(Duration::from_millis(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_combine() {
        assert_eq!(
            HealthStatus::Healthy.combine(&HealthStatus::Healthy),
            HealthStatus::Healthy
        );
        assert_eq!(
            HealthStatus::Healthy.combine(&HealthStatus::Degraded),
            HealthStatus::Degraded
        );
        assert_eq!(
            HealthStatus::Degraded.combine(&HealthStatus::Unhealthy),
            HealthStatus::Unhealthy
        );
    }

    #[test]
    fn test_health_status_to_code() {
        assert_eq!(HealthStatus::Healthy.to_status_code(), 200);
        assert_eq!(HealthStatus::Degraded.to_status_code(), 200);
        assert_eq!(HealthStatus::Unhealthy.to_status_code(), 503);
    }

    #[test]
    fn test_component_health_builders() {
        let healthy = ComponentHealth::healthy("test");
        assert_eq!(healthy.status, HealthStatus::Healthy);

        let degraded = ComponentHealth::degraded("test", "slow");
        assert_eq!(degraded.status, HealthStatus::Degraded);
        assert_eq!(degraded.message, Some("slow".to_string()));

        let unhealthy = ComponentHealth::unhealthy("test", "down");
        assert_eq!(unhealthy.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_component_health_with_details() {
        let health = ComponentHealth::healthy("test")
            .with_latency(Duration::from_millis(50))
            .with_detail("key", "value");

        assert_eq!(health.latency_ms, Some(50));
        assert_eq!(health.details.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_health_response() {
        let start = Instant::now();
        let mut response = HealthResponse::new("1.0.0", start);

        response.add_component(ComponentHealth::healthy("db"));
        response.add_component(ComponentHealth::degraded("cache", "high latency"));

        assert_eq!(response.status, HealthStatus::Degraded);
        assert_eq!(response.components.len(), 2);
    }

    #[test]
    fn test_readiness_response() {
        let response = ReadinessResponse::ready()
            .with_dependency(DependencyStatus::available("db"))
            .with_dependency(DependencyStatus::unavailable("cache", "connection refused"));

        assert!(!response.ready);
        assert!(response.reason.is_some());
        assert_eq!(response.dependencies.len(), 2);
    }

    #[test]
    fn test_liveness_response() {
        let alive = LivenessResponse::alive();
        assert!(alive.alive);

        let not_alive = LivenessResponse::not_alive("deadlock detected");
        assert!(!not_alive.alive);
    }

    #[tokio::test]
    async fn test_health_checker() {
        let checker = HealthChecker::new("test-service", "1.0.0");

        checker
            .register_check("test", || ComponentHealth::healthy("test"))
            .await;

        let health = checker.check_health().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.components.len(), 1);
    }

    #[tokio::test]
    async fn test_health_checker_readiness() {
        let checker = HealthChecker::new("test-service", "1.0.0");

        checker
            .register_readiness("db", || DependencyStatus::available("db"))
            .await;

        let readiness = checker.check_readiness().await;
        assert!(readiness.ready);
    }

    #[test]
    fn test_health_checker_liveness() {
        let checker = HealthChecker::new("test-service", "1.0.0");
        let liveness = checker.check_liveness();
        assert!(liveness.alive);
    }

    #[tokio::test]
    async fn test_health_checker_uptime() {
        let checker = HealthChecker::new("test-service", "1.0.0");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(checker.uptime() >= Duration::from_millis(10));
    }
}
