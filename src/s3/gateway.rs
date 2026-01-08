//! S3 gateway server implementation.

use super::cors::{s3_cors_middleware, CorsState};
use super::credentials::{S3AccessKey, S3CredentialStore};
use super::encryption::EncryptionState;
use super::handlers::*;
use super::lifecycle::LifecycleState;
use super::middleware::{s3_auth_middleware, S3AuthState};
use super::object_lock::ObjectLockState;
use super::replication::ReplicationState;
use super::storage_class::StorageClassState;
use super::versioning::VersioningState;
use crate::client::{DataClient, MetadataClient};
use crate::config::S3Config;
use crate::error::{Result, StrataError};
use crate::ratelimit::{ClientRateLimiter, RateLimitConfig, RateLimitResult};
use crate::types::DEFAULT_CHUNK_SIZE;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Shared state for S3 gateway handlers.
#[derive(Clone)]
pub struct S3State {
    /// Client for metadata operations.
    pub metadata: MetadataClient,
    /// Client for data operations.
    pub data: DataClient,
    /// S3 region name.
    pub region: String,
    /// Chunk size for splitting large objects.
    pub chunk_size: usize,
}

/// State for rate limiting middleware.
#[derive(Clone)]
pub struct RateLimitState {
    limiter: Arc<ClientRateLimiter>,
}

impl RateLimitState {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiter: Arc::new(ClientRateLimiter::new(config)),
        }
    }
}

/// Rate limiting middleware for S3 gateway.
pub async fn s3_rate_limit_middleware(
    State(state): State<RateLimitState>,
    req: Request,
    next: Next,
) -> Response {
    // Extract client ID from X-Forwarded-For header or use remote address
    let client_id = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "anonymous".to_string());

    match state.limiter.check(&client_id).await {
        RateLimitResult::Allowed => next.run(req).await,
        RateLimitResult::GlobalLimitExceeded { retry_after } => {
            warn!("Global rate limit exceeded");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(
                    "Retry-After",
                    retry_after.as_secs().to_string(),
                )],
                "SlowDown: Rate limit exceeded. Please reduce your request rate.",
            )
                .into_response()
        }
        RateLimitResult::ClientLimitExceeded { client_id, retry_after } => {
            warn!(client_id = %client_id, "Client rate limit exceeded");
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    "Retry-After",
                    retry_after.as_secs().to_string(),
                )],
                "SlowDown: Rate limit exceeded. Please reduce your request rate.",
            )
                .into_response()
        }
    }
}

/// Run the S3-compatible gateway server.
pub async fn run_s3_gateway(
    config: S3Config,
    metadata_addr: SocketAddr,
    data_addr: SocketAddr,
) -> Result<()> {
    info!("Starting S3 gateway");

    let metadata = MetadataClient::new(metadata_addr);
    let data = DataClient::new(data_addr);

    let state = S3State {
        metadata,
        data,
        region: config.region.clone(),
        chunk_size: DEFAULT_CHUNK_SIZE,
    };

    // Build credential store from config
    let credential_store = build_credential_store(&config);
    let auth_state = S3AuthState::new(
        credential_store,
        config.region.clone(),
        config.auth.allow_anonymous,
    );

    info!(
        allow_anonymous = config.auth.allow_anonymous,
        credentials_count = config.auth.credentials.len(),
        "S3 authentication configured"
    );

    // Build rate limiter with default config
    let rate_limit_state = RateLimitState::new(RateLimitConfig::default());
    info!("S3 rate limiting enabled");

    // Build CORS state
    let cors_state = CorsState::new();
    info!("S3 CORS support enabled");

    // Build versioning state
    let versioning_state = VersioningState::new();
    info!("S3 versioning support enabled");

    // Build lifecycle state
    let lifecycle_state = LifecycleState::new();
    info!("S3 lifecycle support enabled");

    // Build encryption state
    let encryption_state = EncryptionState::new();
    info!("S3 server-side encryption support enabled");

    // Build object lock state
    let object_lock_state = ObjectLockState::new();
    info!("S3 object lock (WORM) support enabled");

    // Build replication state
    let replication_state = ReplicationState::new();
    info!("S3 bucket replication support enabled");

    // Build storage class state
    let storage_class_state = StorageClassState::new();
    info!("S3 storage class transitions support enabled");

    // Combined state for routes that need S3, CORS, Versioning, Lifecycle, Encryption, Object Lock, Replication, and Storage Class state
    let combined_state: CombinedS3State = (state.clone(), cors_state.clone(), versioning_state, lifecycle_state, encryption_state, object_lock_state, replication_state, storage_class_state);

    // Build router with S3 API routes
    // Middleware order (outermost to innermost): CORS -> Rate Limit -> Auth
    let app = Router::new()
        // Service level operations
        .route("/", get(list_buckets).with_state(state.clone()))
        // Bucket level operations (with multipart upload listing, CORS, and versioning)
        .route("/:bucket", get(handle_get_bucket_full).with_state(combined_state.clone()))
        .route("/:bucket", put(handle_put_bucket_full).with_state(combined_state.clone()))
        .route("/:bucket", delete(handle_delete_bucket_full).with_state(combined_state.clone()))
        .route("/:bucket", head(head_bucket).with_state(state.clone()))
        // Object level operations (with multipart support, S3 Select, and restore)
        .route("/:bucket/*key", get(handle_get_object).with_state(state.clone()))
        .route("/:bucket/*key", put(handle_put_object).with_state(state.clone()))
        .route("/:bucket/*key", post(handle_post_object_full).with_state(combined_state))
        .route("/:bucket/*key", delete(handle_delete_object).with_state(state.clone()))
        .route("/:bucket/*key", head(head_object).with_state(state))
        .layer(middleware::from_fn_with_state(auth_state, s3_auth_middleware))
        .layer(middleware::from_fn_with_state(rate_limit_state, s3_rate_limit_middleware))
        .layer(middleware::from_fn(move |req, next| {
            let cors = cors_state.clone();
            s3_cors_middleware(cors, req, next)
        }));

    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "S3 gateway listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| StrataError::Network(e.to_string()))?;

    Ok(())
}

/// Build the S3 credential store from configuration.
fn build_credential_store(config: &S3Config) -> S3CredentialStore {
    let keys: Vec<S3AccessKey> = config
        .auth
        .credentials
        .iter()
        .map(|c| {
            let mut key = S3AccessKey::new(&c.access_key_id, &c.secret_key, &c.user_id);
            if let Some(ref name) = c.display_name {
                key = key.with_display_name(name);
            }
            key
        })
        .collect();

    S3CredentialStore::from_keys(keys)
}
