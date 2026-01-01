//! HTTP client for Strata API

use anyhow::{Context, Result};
use reqwest::{Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;

use crate::config::Config;

/// Strata API client
pub struct StrataClient {
    client: Client,
    endpoint: String,
    token: Option<String>,
}

impl StrataClient {
    /// Create a new client
    pub fn new(endpoint: &str, config: &Config) -> Result<Self> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .user_agent(format!("strata-cli/{}", env!("CARGO_PKG_VERSION")));

        if config.tls.skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: config.token.clone(),
        })
    }

    /// Build URL for an API path
    fn url(&self, path: &str) -> String {
        format!("{}/api/v1{}", self.endpoint, path)
    }

    /// Add authentication headers
    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    /// GET request
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let req = self.client.get(self.url(path));
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_response(resp).await
    }

    /// GET request with query parameters
    pub async fn get_with_query<T: DeserializeOwned, Q: Serialize>(
        &self,
        path: &str,
        query: &Q,
    ) -> Result<T> {
        let req = self.client.get(self.url(path)).query(query);
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_response(resp).await
    }

    /// POST request
    pub async fn post<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let req = self.client.post(self.url(path)).json(body);
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_response(resp).await
    }

    /// POST request without response body
    pub async fn post_empty<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
        let req = self.client.post(self.url(path)).json(body);
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_empty_response(resp).await
    }

    /// PUT request
    pub async fn put<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let req = self.client.put(self.url(path)).json(body);
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_response(resp).await
    }

    /// PUT request with raw bytes
    pub async fn put_bytes(&self, path: &str, data: Vec<u8>, content_type: &str) -> Result<UploadResponse> {
        let req = self.client
            .put(self.url(path))
            .header("Content-Type", content_type)
            .body(data);
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_response(resp).await
    }

    /// DELETE request
    pub async fn delete(&self, path: &str) -> Result<()> {
        let req = self.client.delete(self.url(path));
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;
        self.handle_empty_response(resp).await
    }

    /// Download file
    pub async fn download(&self, path: &str) -> Result<bytes::Bytes> {
        let req = self.client.get(self.url(path));
        let req = self.add_auth(req);

        let resp = req.send().await.context("Request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Request failed with status {}: {}", status, body);
        }

        Ok(resp.bytes().await?)
    }

    /// Handle JSON response
    async fn handle_response<T: DeserializeOwned>(&self, resp: Response) -> Result<T> {
        let status = resp.status();

        if status.is_success() {
            resp.json().await.context("Failed to parse response")
        } else {
            let body = resp.text().await.unwrap_or_default();
            match status {
                StatusCode::NOT_FOUND => anyhow::bail!("Resource not found"),
                StatusCode::UNAUTHORIZED => anyhow::bail!("Unauthorized - check your credentials"),
                StatusCode::FORBIDDEN => anyhow::bail!("Access denied"),
                StatusCode::CONFLICT => anyhow::bail!("Resource already exists"),
                _ => anyhow::bail!("Request failed with status {}: {}", status, body),
            }
        }
    }

    /// Handle empty response
    async fn handle_empty_response(&self, resp: Response) -> Result<()> {
        let status = resp.status();

        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            match status {
                StatusCode::NOT_FOUND => anyhow::bail!("Resource not found"),
                StatusCode::UNAUTHORIZED => anyhow::bail!("Unauthorized - check your credentials"),
                StatusCode::FORBIDDEN => anyhow::bail!("Access denied"),
                _ => anyhow::bail!("Request failed with status {}: {}", status, body),
            }
        }
    }
}

// API Response types
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct UploadResponse {
    pub etag: String,
    pub version_id: Option<String>,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub region: Option<String>,
    pub versioning: bool,
    pub encryption: bool,
    pub object_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub content_type: String,
    pub etag: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub storage_class: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterHealth {
    pub status: String,
    pub nodes: u32,
    pub healthy_nodes: u32,
    pub uptime: u64,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct ClusterStats {
    pub total_buckets: u64,
    pub total_objects: u64,
    pub total_size_bytes: u64,
    pub used_capacity_bytes: u64,
    pub total_capacity_bytes: u64,
    pub requests_per_second: f64,
}

#[derive(Debug, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub name: String,
    pub address: String,
    pub role: String,
    pub status: String,
    pub uptime: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub version: String,
}
