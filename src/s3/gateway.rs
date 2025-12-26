//! S3 gateway server implementation.

use super::handlers::*;
use crate::client::MetadataClient;
use crate::config::S3Config;
use crate::error::{Result, StrataError};
use axum::{
    routing::{delete, get, head, put},
    Router,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

/// Shared state for S3 gateway handlers.
#[derive(Clone)]
pub struct S3State {
    pub metadata: MetadataClient,
    pub region: String,
}

/// Run the S3-compatible gateway server.
pub async fn run_s3_gateway(config: S3Config, metadata_addr: SocketAddr) -> Result<()> {
    info!("Starting S3 gateway");

    let metadata = MetadataClient::new(metadata_addr);

    let state = S3State {
        metadata,
        region: config.region.clone(),
    };

    // Build router with S3 API routes
    let app = Router::new()
        // Service level operations
        .route("/", get(list_buckets))
        // Bucket level operations
        .route("/:bucket", get(list_objects))
        .route("/:bucket", put(create_bucket))
        .route("/:bucket", delete(delete_bucket))
        .route("/:bucket", head(head_bucket))
        // Object level operations
        .route("/:bucket/*key", get(get_object))
        .route("/:bucket/*key", put(put_object))
        .route("/:bucket/*key", delete(delete_object))
        .route("/:bucket/*key", head(head_object))
        .with_state(state);

    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "S3 gateway listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| StrataError::Network(e.to_string()))?;

    Ok(())
}
