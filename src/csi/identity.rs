//! CSI Identity Service implementation.
//!
//! The Identity service provides information about the CSI driver itself.

#[allow(unused_imports)]
use super::{CsiConfig, DriverCapabilities, DRIVER_NAME, DRIVER_VERSION};
use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Plugin information response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    /// Driver name.
    pub name: String,
    /// Driver version.
    pub vendor_version: String,
    /// Additional manifest entries.
    pub manifest: std::collections::HashMap<String, String>,
}

/// Plugin capability type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PluginCapabilityType {
    /// Controller service is supported.
    ControllerService,
    /// Volume accessibility constraints supported.
    VolumeAccessibilityConstraints,
    /// Group controller service supported.
    GroupControllerService,
}

/// Probe response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResponse {
    /// Whether the plugin is ready.
    pub ready: bool,
}

/// CSI Identity Service.
pub struct IdentityService {
    config: CsiConfig,
    capabilities: DriverCapabilities,
}

impl IdentityService {
    /// Create a new identity service.
    pub fn new(config: CsiConfig, capabilities: DriverCapabilities) -> Self {
        Self {
            config,
            capabilities,
        }
    }

    /// Get plugin information.
    ///
    /// CSI RPC: GetPluginInfo
    pub fn get_plugin_info(&self) -> PluginInfo {
        let mut manifest = std::collections::HashMap::new();
        manifest.insert("git.commit".to_string(), env!("CARGO_PKG_VERSION").to_string());

        PluginInfo {
            name: self.config.driver_name.clone(),
            vendor_version: DRIVER_VERSION.to_string(),
            manifest,
        }
    }

    /// Get plugin capabilities.
    ///
    /// CSI RPC: GetPluginCapabilities
    pub fn get_plugin_capabilities(&self) -> Vec<PluginCapabilityType> {
        let mut capabilities = vec![PluginCapabilityType::ControllerService];

        if self.config.topology_enabled {
            capabilities.push(PluginCapabilityType::VolumeAccessibilityConstraints);
        }

        capabilities
    }

    /// Probe the plugin readiness.
    ///
    /// CSI RPC: Probe
    pub async fn probe(&self) -> Result<ProbeResponse> {
        // In a real implementation, this would check:
        // - Connection to Strata metadata server
        // - Required permissions
        // - Storage availability

        Ok(ProbeResponse { ready: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_plugin_info() {
        let config = CsiConfig::default();
        let caps = DriverCapabilities::default();
        let service = IdentityService::new(config, caps);

        let info = service.get_plugin_info();
        assert_eq!(info.name, DRIVER_NAME);
        assert!(!info.vendor_version.is_empty());
    }

    #[test]
    fn test_get_plugin_capabilities() {
        let config = CsiConfig::default();
        let caps = DriverCapabilities::default();
        let service = IdentityService::new(config, caps);

        let capabilities = service.get_plugin_capabilities();
        assert!(capabilities.contains(&PluginCapabilityType::ControllerService));
    }

    #[tokio::test]
    async fn test_probe() {
        let config = CsiConfig::default();
        let caps = DriverCapabilities::default();
        let service = IdentityService::new(config, caps);

        let response = service.probe().await.unwrap();
        assert!(response.ready);
    }
}
