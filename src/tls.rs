//! TLS configuration and support for Strata.
//!
//! Provides utilities for configuring TLS/HTTPS for HTTP servers.

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS.
    pub enabled: bool,
    /// Path to certificate file (PEM format).
    pub cert_path: PathBuf,
    /// Path to private key file (PEM format).
    pub key_path: PathBuf,
    /// Path to CA certificate file for client verification (optional).
    pub ca_cert_path: Option<PathBuf>,
    /// Require client certificate verification.
    pub require_client_cert: bool,
    /// Minimum TLS version.
    pub min_version: TlsVersion,
    /// Allowed cipher suites (empty = use defaults).
    pub cipher_suites: Vec<String>,
}

/// Recommended cipher suites for TLS 1.3 (in preference order).
pub const RECOMMENDED_TLS13_CIPHERS: &[&str] = &[
    "TLS_AES_256_GCM_SHA384",
    "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256",
];

/// Recommended cipher suites for TLS 1.2 (in preference order).
/// Only AEAD ciphers with forward secrecy are included.
pub const RECOMMENDED_TLS12_CIPHERS: &[&str] = &[
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
];

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: PathBuf::from("certs/server.crt"),
            key_path: PathBuf::from("certs/server.key"),
            ca_cert_path: None,
            require_client_cert: false,
            min_version: TlsVersion::Tls13,
            cipher_suites: Vec::new(), // Empty means use library defaults
        }
    }
}

impl TlsConfig {
    /// Get recommended cipher suites based on the minimum TLS version.
    pub fn recommended_ciphers(&self) -> Vec<String> {
        match self.min_version {
            TlsVersion::Tls13 => RECOMMENDED_TLS13_CIPHERS.iter().map(|s| s.to_string()).collect(),
            TlsVersion::Tls12 => {
                let mut ciphers: Vec<String> = RECOMMENDED_TLS13_CIPHERS.iter().map(|s| s.to_string()).collect();
                ciphers.extend(RECOMMENDED_TLS12_CIPHERS.iter().map(|s| s.to_string()));
                ciphers
            }
        }
    }

    /// Apply recommended security settings.
    /// Sets TLS 1.3 minimum and recommended cipher suites.
    pub fn with_recommended_security(mut self) -> Self {
        self.min_version = TlsVersion::Tls13;
        self.cipher_suites = self.recommended_ciphers();
        self
    }
}

impl TlsConfig {
    /// Create a new TLS config with the given certificate and key paths.
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            ..Default::default()
        }
    }

    /// Enable client certificate verification.
    pub fn with_client_cert(mut self, ca_cert_path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(ca_cert_path.into());
        self.require_client_cert = true;
        self
    }

    /// Set minimum TLS version.
    pub fn with_min_version(mut self, version: TlsVersion) -> Self {
        self.min_version = version;
        self
    }

    /// Validate that certificate and key files exist.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if !self.cert_path.exists() {
            return Err(StrataError::Config(format!(
                "TLS certificate not found: {}",
                self.cert_path.display()
            )));
        }

        if !self.key_path.exists() {
            return Err(StrataError::Config(format!(
                "TLS key not found: {}",
                self.key_path.display()
            )));
        }

        if let Some(ca_path) = &self.ca_cert_path {
            if !ca_path.exists() {
                return Err(StrataError::Config(format!(
                    "CA certificate not found: {}",
                    ca_path.display()
                )));
            }
        }

        Ok(())
    }
}

/// Minimum TLS version.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TlsVersion {
    /// TLS 1.2.
    Tls12,
    /// TLS 1.3 - secure default for production use.
    #[default]
    Tls13,
}

/// Certificate data loaded from files.
#[derive(Clone)]
pub struct CertificateData {
    /// Server certificate chain (PEM).
    pub cert_chain: Vec<u8>,
    /// Private key (PEM).
    pub private_key: Vec<u8>,
    /// CA certificates for client verification.
    pub ca_certs: Option<Vec<u8>>,
}

impl CertificateData {
    /// Load certificates from files.
    pub fn load(config: &TlsConfig) -> Result<Self> {
        config.validate()?;

        let cert_chain = fs::read(&config.cert_path).map_err(|e| {
            StrataError::Config(format!(
                "Failed to read certificate {}: {}",
                config.cert_path.display(),
                e
            ))
        })?;

        let private_key = fs::read(&config.key_path).map_err(|e| {
            StrataError::Config(format!(
                "Failed to read key {}: {}",
                config.key_path.display(),
                e
            ))
        })?;

        let ca_certs = if let Some(ca_path) = &config.ca_cert_path {
            Some(fs::read(ca_path).map_err(|e| {
                StrataError::Config(format!(
                    "Failed to read CA certificate {}: {}",
                    ca_path.display(),
                    e
                ))
            })?)
        } else {
            None
        };

        Ok(Self {
            cert_chain,
            private_key,
            ca_certs,
        })
    }

    /// Get certificate chain as string.
    pub fn cert_chain_pem(&self) -> Result<String> {
        String::from_utf8(self.cert_chain.clone())
            .map_err(|e| StrataError::Config(format!("Invalid certificate encoding: {}", e)))
    }

    /// Get private key as string.
    pub fn private_key_pem(&self) -> Result<String> {
        String::from_utf8(self.private_key.clone())
            .map_err(|e| StrataError::Config(format!("Invalid key encoding: {}", e)))
    }
}

/// TLS acceptor builder for configuring server TLS.
pub struct TlsAcceptorBuilder {
    config: TlsConfig,
    cert_data: Option<CertificateData>,
}

impl TlsAcceptorBuilder {
    /// Create a new TLS acceptor builder.
    pub fn new(config: TlsConfig) -> Self {
        Self {
            config,
            cert_data: None,
        }
    }

    /// Load certificates from files.
    pub fn load_certificates(mut self) -> Result<Self> {
        if self.config.enabled {
            self.cert_data = Some(CertificateData::load(&self.config)?);
        }
        Ok(self)
    }

    /// Check if TLS is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the loaded certificate data.
    pub fn cert_data(&self) -> Option<&CertificateData> {
        self.cert_data.as_ref()
    }

    /// Get the TLS configuration.
    pub fn config(&self) -> &TlsConfig {
        &self.config
    }
}

/// TLS client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsClientConfig {
    /// Enable TLS.
    pub enabled: bool,
    /// Path to client certificate (optional, for mTLS).
    pub cert_path: Option<PathBuf>,
    /// Path to client key (optional, for mTLS).
    pub key_path: Option<PathBuf>,
    /// Path to CA certificate for server verification.
    pub ca_cert_path: Option<PathBuf>,
    /// Skip server certificate verification (INSECURE - for testing only).
    pub danger_skip_verification: bool,
}

impl Default for TlsClientConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cert_path: None,
            key_path: None,
            ca_cert_path: None,
            danger_skip_verification: false,
        }
    }
}

impl TlsClientConfig {
    /// Create a secure client config.
    pub fn secure() -> Self {
        Self::default()
    }

    /// Create an insecure client config (for testing ONLY).
    ///
    /// # Warning
    /// This method is only available in test builds. Using TLS without
    /// certificate verification in production is a critical security vulnerability.
    #[cfg(test)]
    pub fn insecure() -> Self {
        Self {
            enabled: true,
            danger_skip_verification: true,
            ..Default::default()
        }
    }

    /// Create an insecure client config with an explicit acknowledgement.
    ///
    /// # Security Warning
    /// This disables server certificate verification, making the connection
    /// vulnerable to man-in-the-middle attacks. Only use this in development
    /// or testing environments where you understand the risks.
    ///
    /// The `_i_understand_this_is_insecure` parameter serves as acknowledgement
    /// that you understand the security implications.
    pub fn insecure_for_development(_i_understand_this_is_insecure: bool) -> Self {
        tracing::warn!(
            "Creating TLS client with certificate verification DISABLED. \
             This is a security risk and should only be used in development."
        );
        Self {
            enabled: true,
            danger_skip_verification: true,
            ..Default::default()
        }
    }

    /// Create client config with custom CA.
    pub fn with_ca(ca_cert_path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            ca_cert_path: Some(ca_cert_path.into()),
            ..Default::default()
        }
    }

    /// Add client certificate for mTLS.
    pub fn with_client_cert(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.cert_path = Some(cert_path.into());
        self.key_path = Some(key_path.into());
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(ca_path) = &self.ca_cert_path {
            if !ca_path.exists() {
                return Err(StrataError::Config(format!(
                    "CA certificate not found: {}",
                    ca_path.display()
                )));
            }
        }

        // If client cert is specified, both cert and key must exist
        match (&self.cert_path, &self.key_path) {
            (Some(cert), Some(key)) => {
                if !cert.exists() {
                    return Err(StrataError::Config(format!(
                        "Client certificate not found: {}",
                        cert.display()
                    )));
                }
                if !key.exists() {
                    return Err(StrataError::Config(format!(
                        "Client key not found: {}",
                        key.display()
                    )));
                }
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(StrataError::Config(
                    "Both client certificate and key must be specified".to_string(),
                ));
            }
            (None, None) => {}
        }

        Ok(())
    }
}

/// Generate placeholder certificate files for testing.
///
/// # Warning
/// This function creates PLACEHOLDER files, not actual certificates.
/// These files CANNOT be used for real TLS connections.
///
/// For development/testing, use one of:
/// - `openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes -subj '/CN=localhost'`
/// - A tool like `mkcert` for local development certificates
/// - The `rcgen` crate for programmatic certificate generation
///
/// For production, always use certificates from a trusted Certificate Authority.
#[cfg(test)]
pub fn generate_test_cert_placeholders(
    common_name: &str,
    output_dir: impl AsRef<std::path::Path>,
) -> Result<(PathBuf, PathBuf)> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;

    let cert_path = output_dir.join("server.crt");
    let key_path = output_dir.join("server.key");

    // Write placeholder content that clearly indicates this is not a real certificate
    let placeholder = format!(
        "# PLACEHOLDER - NOT A REAL CERTIFICATE\n\
         # Common Name: {}\n\
         #\n\
         # Generate real certificates with:\n\
         # openssl req -x509 -newkey rsa:4096 -keyout {} -out {} -days 365 -nodes \\\n\
         #   -subj '/CN={}'\n",
        common_name,
        key_path.display(),
        cert_path.display(),
        common_name
    );

    fs::write(&cert_path, &placeholder)?;
    fs::write(&key_path, &placeholder)?;

    tracing::warn!(
        "Generated PLACEHOLDER certificate files at {:?}. These are NOT real certificates.",
        output_dir
    );

    Ok((cert_path, key_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_version, TlsVersion::Tls13);
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_new() {
        let config = TlsConfig::new("cert.pem", "key.pem");
        assert!(config.enabled);
        assert_eq!(config.cert_path, PathBuf::from("cert.pem"));
        assert_eq!(config.key_path, PathBuf::from("key.pem"));
    }

    #[test]
    fn test_tls_config_with_client_cert() {
        let config = TlsConfig::new("cert.pem", "key.pem").with_client_cert("ca.pem");
        assert!(config.require_client_cert);
        assert_eq!(config.ca_cert_path, Some(PathBuf::from("ca.pem")));
    }

    #[test]
    fn test_tls_config_validate_disabled() {
        let config = TlsConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validate_missing_cert() {
        let config = TlsConfig::new("nonexistent.crt", "nonexistent.key");
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_config_validate_with_files() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("server.crt");
        let key_path = temp_dir.path().join("server.key");

        fs::write(&cert_path, "cert data").unwrap();
        fs::write(&key_path, "key data").unwrap();

        let config = TlsConfig::new(&cert_path, &key_path);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_certificate_data_load() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("server.crt");
        let key_path = temp_dir.path().join("server.key");

        fs::write(&cert_path, "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n")
            .unwrap();
        fs::write(&key_path, "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n")
            .unwrap();

        let config = TlsConfig::new(&cert_path, &key_path);
        let data = CertificateData::load(&config).unwrap();

        assert!(!data.cert_chain.is_empty());
        assert!(!data.private_key.is_empty());
        assert!(data.ca_certs.is_none());
    }

    #[test]
    fn test_tls_acceptor_builder() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("server.crt");
        let key_path = temp_dir.path().join("server.key");

        fs::write(&cert_path, "cert data").unwrap();
        fs::write(&key_path, "key data").unwrap();

        let config = TlsConfig::new(&cert_path, &key_path);
        let builder = TlsAcceptorBuilder::new(config).load_certificates().unwrap();

        assert!(builder.is_enabled());
        assert!(builder.cert_data().is_some());
    }

    #[test]
    fn test_tls_client_config_default() {
        let config = TlsClientConfig::default();
        assert!(config.enabled);
        assert!(!config.danger_skip_verification);
    }

    #[test]
    fn test_tls_client_config_insecure() {
        let config = TlsClientConfig::insecure();
        assert!(config.danger_skip_verification);
    }

    #[test]
    fn test_tls_client_config_with_ca() {
        let config = TlsClientConfig::with_ca("ca.pem");
        assert_eq!(config.ca_cert_path, Some(PathBuf::from("ca.pem")));
    }

    #[test]
    fn test_tls_client_config_validate_ok() {
        let config = TlsClientConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_client_config_validate_missing_key() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("client.crt");
        fs::write(&cert_path, "cert").unwrap();

        let config = TlsClientConfig::default()
            .with_client_cert(&cert_path, "nonexistent.key");
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_generate_test_cert_placeholders() {
        let temp_dir = TempDir::new().unwrap();
        let (cert_path, key_path) =
            generate_test_cert_placeholders("test.local", temp_dir.path()).unwrap();

        assert!(cert_path.exists());
        assert!(key_path.exists());
    }

    #[test]
    fn test_tls_version_default() {
        assert_eq!(TlsVersion::default(), TlsVersion::Tls13);
    }

    #[test]
    fn test_recommended_ciphers_tls13() {
        let config = TlsConfig::default();
        let ciphers = config.recommended_ciphers();
        assert!(!ciphers.is_empty());
        assert!(ciphers.contains(&"TLS_AES_256_GCM_SHA384".to_string()));
        assert!(ciphers.contains(&"TLS_AES_128_GCM_SHA256".to_string()));
    }

    #[test]
    fn test_recommended_ciphers_tls12() {
        let mut config = TlsConfig::default();
        config.min_version = TlsVersion::Tls12;
        let ciphers = config.recommended_ciphers();
        // TLS 1.2 config should include both TLS 1.3 and TLS 1.2 ciphers
        assert!(ciphers.contains(&"TLS_AES_256_GCM_SHA384".to_string()));
        assert!(ciphers.contains(&"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".to_string()));
    }

    #[test]
    fn test_with_recommended_security() {
        let config = TlsConfig::new("cert.pem", "key.pem").with_recommended_security();
        assert_eq!(config.min_version, TlsVersion::Tls13);
        assert!(!config.cipher_suites.is_empty());
    }

    #[test]
    fn test_insecure_for_development() {
        let config = TlsClientConfig::insecure_for_development(true);
        assert!(config.danger_skip_verification);
        assert!(config.enabled);
    }
}
