//! S3 API request handlers.

use super::cors::{cors_to_xml, parse_cors_xml, CorsState};
use super::encryption::{encryption_to_xml, parse_encryption_xml, EncryptionState};
use super::object_lock::{
    legal_hold_to_xml, object_lock_to_xml, parse_legal_hold_xml, parse_object_lock_xml,
    parse_retention_xml, retention_to_xml, ObjectKey, ObjectLegalHold, ObjectLockState,
};
use super::replication::{parse_replication_xml, replication_to_xml, ReplicationState};
use super::storage_class::{
    parse_restore_xml, ObjectStorageInfo, RestoreStatus, StorageClassState,
};
use super::select::{execute_csv_select, parse_select_xml, select_stats_to_xml};
use super::gateway::S3State;
use super::lifecycle::{lifecycle_to_xml, parse_lifecycle_xml, LifecycleState};
use super::versioning::{parse_versioning_xml, versioning_to_xml, VersioningState};
use super::xml;
use crate::client::CompletedPartRequest;
use crate::types::{ChunkId, FileType, InodeId};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Root inode ID (buckets are directories at root level).
const ROOT_INODE: InodeId = 1;

// XML response helpers

fn xml_response(body: String) -> Response {
    (
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response()
}

fn xml_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
</Error>"#,
        code, message
    );

    (status, [("Content-Type", "application/xml")], body).into_response()
}

// Input validation helpers

/// Maximum allowed key length (S3 allows up to 1024 bytes).
const MAX_KEY_LENGTH: usize = 1024;

/// Maximum allowed bucket name length (S3 allows 3-63 chars).
const MAX_BUCKET_LENGTH: usize = 63;

/// Validate an S3 object key for security issues.
fn validate_s3_key(key: &str) -> Result<(), Response> {
    // Check length
    if key.len() > MAX_KEY_LENGTH {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "KeyTooLong",
            "Object key exceeds maximum length",
        ));
    }

    // Prevent path traversal attacks
    if key.contains("..") {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidKey",
            "Object key contains invalid path components",
        ));
    }

    // Prevent null byte injection
    if key.contains('\0') {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidKey",
            "Object key contains invalid characters",
        ));
    }

    Ok(())
}

/// Validate an S3 bucket name.
fn validate_bucket_name(bucket: &str) -> Result<(), Response> {
    // Check length
    if bucket.is_empty() || bucket.len() > MAX_BUCKET_LENGTH {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidBucketName",
            "Bucket name must be between 1 and 63 characters",
        ));
    }

    // Prevent path traversal
    if bucket.contains("..") || bucket.contains('/') {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidBucketName",
            "Bucket name contains invalid characters",
        ));
    }

    // Prevent null byte injection
    if bucket.contains('\0') {
        return Err(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidBucketName",
            "Bucket name contains invalid characters",
        ));
    }

    Ok(())
}

// Service-level operations

pub async fn list_buckets(State(state): State<S3State>) -> Response {
    debug!("ListBuckets");

    // List directories at root level (these are the buckets)
    match state.metadata.readdir(ROOT_INODE).await {
        Ok(response) => {
            if !response.success {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Unknown error"),
                );
            }

            let buckets: Vec<String> = response
                .entries
                .iter()
                .filter(|e| matches!(e.file_type, FileType::Directory))
                .map(|e| {
                    format!(
                        r#"        <Bucket>
            <Name>{}</Name>
            <CreationDate>2024-01-01T00:00:00.000Z</CreationDate>
        </Bucket>"#,
                        e.name
                    )
                })
                .collect();

            let body = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>strata</ID>
        <DisplayName>strata</DisplayName>
    </Owner>
    <Buckets>
{}
    </Buckets>
</ListAllMyBucketsResult>"#,
                buckets.join("\n")
            );

            xml_response(body)
        }
        Err(e) => {
            error!(?e, "ListBuckets failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

// Bucket-level operations

pub async fn create_bucket(State(state): State<S3State>, Path(bucket): Path<String>) -> Response {
    debug!(bucket = %bucket, "CreateBucket");

    // Check if bucket already exists
    match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(_)) => {
            return xml_error(
                StatusCode::CONFLICT,
                "BucketAlreadyExists",
                "The requested bucket name is not available.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "CreateBucket lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
        Ok(None) => {}
    }

    // Create bucket directory
    match state
        .metadata
        .create_directory(ROOT_INODE, &bucket, 0o755, 0, 0)
        .await
    {
        Ok(response) => {
            if response.success {
                (StatusCode::OK, "").into_response()
            } else {
                xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Unknown error"),
                )
            }
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "CreateBucket failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

pub async fn delete_bucket(State(state): State<S3State>, Path(bucket): Path<String>) -> Response {
    debug!(bucket = %bucket, "DeleteBucket");

    // Delete bucket directory
    match state.metadata.delete(ROOT_INODE, &bucket).await {
        Ok(response) => {
            if response.success {
                (StatusCode::NO_CONTENT, "").into_response()
            } else {
                let error_msg = response.error.as_deref().unwrap_or("Unknown error");
                if error_msg.contains("not empty") {
                    xml_error(
                        StatusCode::CONFLICT,
                        "BucketNotEmpty",
                        "The bucket you tried to delete is not empty.",
                    )
                } else {
                    xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", error_msg)
                }
            }
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "DeleteBucket failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

pub async fn head_bucket(State(state): State<S3State>, Path(bucket): Path<String>) -> Response {
    debug!(bucket = %bucket, "HeadBucket");

    match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(_)) => (StatusCode::OK, "").into_response(),
        Ok(None) => xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist.",
        ),
        Err(e) => {
            error!(?e, bucket = %bucket, "HeadBucket failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

#[derive(Deserialize)]
pub struct ListObjectsQuery {
    #[serde(rename = "list-type")]
    list_type: Option<i32>,
    prefix: Option<String>,
    #[serde(rename = "max-keys")]
    max_keys: Option<i32>,
    #[allow(dead_code)]
    delimiter: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
}

pub async fn list_objects(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> Response {
    debug!(
        bucket = %bucket,
        prefix = ?params.prefix,
        "ListObjects"
    );

    let prefix = params.prefix.unwrap_or_default();
    let max_keys = params.max_keys.unwrap_or(1000);

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "ListObjects lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // List objects in the bucket
    match state.metadata.readdir(bucket_inode).await {
        Ok(response) => {
            if !response.success {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Unknown error"),
                );
            }

            let objects: Vec<String> = response
                .entries
                .iter()
                .filter(|e| {
                    matches!(e.file_type, FileType::RegularFile) && e.name.starts_with(&prefix)
                })
                .take(max_keys as usize)
                .map(|e| {
                    format!(
                        r#"    <Contents>
        <Key>{}</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>""</ETag>
        <Size>0</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>"#,
                        e.name
                    )
                })
                .collect();

            let key_count = objects.len();

            let body = if params.list_type == Some(2) {
                // ListObjectsV2
                format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{}</Name>
    <Prefix>{}</Prefix>
    <MaxKeys>{}</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <KeyCount>{}</KeyCount>
{}
</ListBucketResult>"#,
                    bucket,
                    prefix,
                    max_keys,
                    key_count,
                    objects.join("\n")
                )
            } else {
                // ListObjects (v1)
                format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{}</Name>
    <Prefix>{}</Prefix>
    <MaxKeys>{}</MaxKeys>
    <IsTruncated>false</IsTruncated>
{}
</ListBucketResult>"#,
                    bucket,
                    prefix,
                    max_keys,
                    objects.join("\n")
                )
            };

            xml_response(body)
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "ListObjects failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

// Object-level operations

pub async fn get_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "GetObject");

    // Validate inputs
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "GetObject bucket lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Look up the object
    match state.metadata.lookup(bucket_inode, &key).await {
        Ok(Some(inode)) => {
            let size = inode.size as usize;

            // Fetch data from data servers for each chunk
            let mut data = Vec::with_capacity(size);
            for chunk_id in &inode.chunks {
                match state.data.read_chunk(*chunk_id, state.chunk_size).await {
                    Ok(chunk_data) => {
                        data.extend(chunk_data);
                    }
                    Err(e) => {
                        error!(?e, chunk_id = %chunk_id, "GetObject chunk read failed");
                        return xml_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Failed to read data",
                        );
                    }
                }
            }

            // Truncate to actual file size (last chunk may be partial)
            data.truncate(size);

            // Calculate ETag from actual data
            let etag = format!("\"{:x}\"", md5::compute(&data));

            (
                StatusCode::OK,
                [
                    ("Content-Length", size.to_string()),
                    ("ETag", etag),
                    ("Content-Type", "application/octet-stream".to_string()),
                ],
                Bytes::from(data),
            )
                .into_response()
        }
        Ok(None) => xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        ),
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "GetObject lookup failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

pub async fn put_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    debug!(
        bucket = %bucket,
        key = %key,
        size = body.len(),
        "PutObject"
    );

    // Validate inputs
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "PutObject bucket lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Calculate ETag (MD5 hash) before storing
    let etag = format!("\"{:x}\"", md5::compute(&body));
    let body_len = body.len() as u64;

    // Check if file already exists and delete it first
    if let Ok(Some(_)) = state.metadata.lookup(bucket_inode, &key).await {
        if let Err(e) = state.metadata.delete(bucket_inode, &key).await {
            error!(?e, bucket = %bucket, key = %key, "PutObject delete existing failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    }

    // Create file in metadata
    let file_inode = match state
        .metadata
        .create_file(bucket_inode, &key, 0o644, 0, 0)
        .await
    {
        Ok(response) => {
            if response.success {
                match response.inode {
                    Some(inode_id) => inode_id,
                    None => {
                        error!(bucket = %bucket, key = %key, "PutObject no inode returned");
                        return xml_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Request failed",
                        );
                    }
                }
            } else {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Unknown error"),
                );
            }
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "PutObject create file failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Split body into chunks and store each one
    let chunk_size = state.chunk_size;
    for chunk_data in body.chunks(chunk_size) {
        let chunk_id = ChunkId::new();

        // Write chunk to data server
        match state.data.write_chunk(chunk_id, chunk_data).await {
            Ok(response) => {
                if !response.success {
                    return xml_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        response.error.as_deref().unwrap_or("Failed to write chunk"),
                    );
                }
            }
            Err(e) => {
                error!(?e, bucket = %bucket, key = %key, "PutObject write chunk failed");
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Failed to write data",
                );
            }
        }

        // Associate chunk with file
        if let Err(e) = state.metadata.add_chunk(file_inode, chunk_id).await {
            error!(?e, bucket = %bucket, key = %key, "PutObject add chunk failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    }

    // Update file size
    if let Err(e) = state.metadata.set_size(file_inode, body_len).await {
        error!(?e, bucket = %bucket, key = %key, "PutObject set size failed");
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "Request failed",
        );
    }

    (StatusCode::OK, [("ETag", etag.as_str())], "").into_response()
}

pub async fn delete_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "DeleteObject");

    // Validate inputs
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "DeleteObject lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Delete object
    match state.metadata.delete(bucket_inode, &key).await {
        Ok(response) => {
            if response.success {
                (StatusCode::NO_CONTENT, "").into_response()
            } else {
                // S3 returns success even if object doesn't exist
                (StatusCode::NO_CONTENT, "").into_response()
            }
        }
        Err(_) => {
            // S3 returns success even if object doesn't exist
            (StatusCode::NO_CONTENT, "").into_response()
        }
    }
}

pub async fn head_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "HeadObject");

    // Validate inputs
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "HeadObject bucket lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Look up the object
    match state.metadata.lookup(bucket_inode, &key).await {
        Ok(Some(inode)) => {
            let size = inode.size;
            // Compute ETag from chunk data if available, otherwise use stored ETag or compute from metadata
            let etag = if !inode.chunks.is_empty() {
                // For HEAD, we compute ETag based on number of chunks and size (avoiding data fetch)
                // Real implementation would store ETag in metadata
                format!("\"{:x}-{}\"", size, inode.chunks.len())
            } else {
                format!("\"{:x}\"", md5::compute(&[]))
            };

            (
                StatusCode::OK,
                [
                    ("Content-Length", size.to_string()),
                    ("ETag", etag),
                    ("Content-Type", "application/octet-stream".to_string()),
                ],
                "",
            )
                .into_response()
        }
        Ok(None) => xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        ),
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "HeadObject lookup failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

// Multipart upload operations

/// Query parameters for multipart upload operations.
#[derive(Debug, Deserialize)]
pub struct MultipartQuery {
    /// Initiates a multipart upload when present.
    pub uploads: Option<String>,
    /// Upload ID for multipart operations.
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    /// Part number for UploadPart.
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
    /// S3 Select query when present.
    pub select: Option<String>,
    /// S3 Select type (should be "2").
    #[serde(rename = "select-type")]
    pub select_type: Option<String>,
    /// Restore archived object when present.
    pub restore: Option<String>,
}

/// Create (initiate) a multipart upload.
/// POST /{bucket}/{key}?uploads
pub async fn create_multipart_upload(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "CreateMultipartUpload");

    // First, find the bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "CreateMultipartUpload lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Generate upload ID
    let upload_id = Uuid::new_v4().to_string();

    // Initiate multipart upload in metadata
    match state
        .metadata
        .initiate_multipart_upload(bucket_inode, &key, &upload_id)
        .await
    {
        Ok(()) => {
            let body = xml::initiate_multipart_upload_result(&bucket, &key, &upload_id);
            xml_response(body)
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "CreateMultipartUpload failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

/// Upload a part of a multipart upload.
/// PUT /{bucket}/{key}?partNumber=N&uploadId=X
pub async fn upload_part(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    body: Bytes,
) -> Response {
    let upload_id = match query.upload_id {
        Some(id) => id,
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing uploadId parameter",
            );
        }
    };

    let part_number = match query.part_number {
        Some(n) if n >= 1 && n <= 10000 => n,
        Some(n) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidPartNumber",
                &format!("Part number must be between 1 and 10000, got {}", n),
            );
        }
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing partNumber parameter",
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        upload_id = %upload_id,
        part_number,
        size = body.len(),
        "UploadPart"
    );

    // Write chunk to data server
    let chunk_id = ChunkId::new();
    let chunk_size = body.len() as u64;

    match state.data.write_chunk(chunk_id, &body).await {
        Ok(response) => {
            if !response.success {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Failed to write chunk"),
                );
            }
        }
        Err(e) => {
            error!(?e, upload_id = %upload_id, part_number, "UploadPart write chunk failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Failed to write data",
            );
        }
    }

    // Calculate ETag (MD5 hash)
    let etag = format!("\"{:x}\"", md5::compute(&body));

    // Record part in metadata
    match state
        .metadata
        .upload_part(&upload_id, part_number, chunk_id, chunk_size, &etag)
        .await
    {
        Ok(()) => (StatusCode::OK, [("ETag", etag.as_str())], "").into_response(),
        Err(e) => {
            warn!(upload_id = %upload_id, part_number, error = %e, "Failed to record uploaded part");
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchUpload",
                "The specified upload does not exist.",
            )
        }
    }
}

/// Complete a multipart upload.
/// POST /{bucket}/{key}?uploadId=X
pub async fn complete_multipart_upload(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    body: Bytes,
) -> Response {
    let upload_id = match query.upload_id {
        Some(id) => id,
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing uploadId parameter",
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        upload_id = %upload_id,
        "CompleteMultipartUpload"
    );

    // Parse the CompleteMultipartUpload XML request body
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Request body is not valid UTF-8",
            );
        }
    };

    let completed_parts = match xml::parse_complete_multipart_upload(body_str) {
        Ok(parts) => parts,
        Err(e) => {
            return xml_error(StatusCode::BAD_REQUEST, "MalformedXML", &e.to_string());
        }
    };

    // Convert to client request format
    let parts: Vec<CompletedPartRequest> = completed_parts
        .into_iter()
        .map(|p| CompletedPartRequest {
            part_number: p.part_number,
            etag: p.etag,
        })
        .collect();

    // Complete the multipart upload
    match state
        .metadata
        .complete_multipart_upload(&upload_id, parts)
        .await
    {
        Ok(response) => {
            if response.success {
                // Calculate combined ETag (this is simplified - real S3 uses a specific algorithm)
                let combined_etag = format!("\"combined-{}-{}\"", upload_id, response.inode.unwrap_or(0));
                let location = format!("http://localhost/{}/{}", bucket, key);
                let body = xml::complete_multipart_upload_result(&bucket, &key, &location, &combined_etag);
                xml_response(body)
            } else {
                xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Failed to complete upload"),
                )
            }
        }
        Err(e) => {
            warn!(upload_id = %upload_id, error = %e, "Failed to complete multipart upload");
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchUpload",
                "The specified upload does not exist.",
            )
        }
    }
}

/// Abort a multipart upload.
/// DELETE /{bucket}/{key}?uploadId=X
pub async fn abort_multipart_upload(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
) -> Response {
    let upload_id = match query.upload_id {
        Some(id) => id,
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing uploadId parameter",
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        upload_id = %upload_id,
        "AbortMultipartUpload"
    );

    match state.metadata.abort_multipart_upload(&upload_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => {
            warn!(upload_id = %upload_id, error = %e, "Failed to abort multipart upload");
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchUpload",
                "The specified upload does not exist.",
            )
        }
    }
}

/// List parts for a multipart upload.
/// GET /{bucket}/{key}?uploadId=X
pub async fn list_parts(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
) -> Response {
    // Validate inputs
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    let upload_id = match query.upload_id {
        Some(id) => id,
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing uploadId parameter",
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        upload_id = %upload_id,
        "ListParts"
    );

    match state.metadata.list_parts(&upload_id).await {
        Ok(response) => {
            let parts: Vec<xml::PartInfo> = response
                .parts
                .iter()
                .map(|p| xml::PartInfo {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                    size: p.size,
                })
                .collect();

            let body = xml::list_parts_result(&bucket, &key, &upload_id, &parts);
            xml_response(body)
        }
        Err(e) => {
            warn!(upload_id = %upload_id, error = %e, "Failed to list parts");
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchUpload",
                "The specified upload does not exist.",
            )
        }
    }
}

/// List multipart uploads for a bucket.
/// GET /{bucket}?uploads
pub async fn list_multipart_uploads(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
) -> Response {
    // Validate bucket name
    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    debug!(bucket = %bucket, "ListMultipartUploads");

    // Get bucket inode
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode_id)) => inode_id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist",
            );
        }
        Err(e) => {
            error!(?e, "ListMultipartUploads lookup failed");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    match state.metadata.list_multipart_uploads(bucket_inode.id).await {
        Ok(response) => {
            let uploads: Vec<xml::UploadInfo> = response
                .uploads
                .iter()
                .map(|u| xml::UploadInfo {
                    key: u.key.clone(),
                    upload_id: u.upload_id.clone(),
                    initiated: u.initiated.clone(),
                })
                .collect();

            let body = xml::list_multipart_uploads_result(&bucket, &uploads);
            xml_response(body)
        }
        Err(e) => {
            error!(?e, "ListMultipartUploads failed");
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            )
        }
    }
}

/// Handle POST requests to object paths - routes to multipart operations and S3 Select.
/// POST /{bucket}/{key} with ?uploads -> CreateMultipartUpload
/// POST /{bucket}/{key} with ?uploadId=X -> CompleteMultipartUpload
/// POST /{bucket}/{key} with ?select&select-type=2 -> SelectObjectContent
/// POST /{bucket}/{key} with ?restore -> RestoreObject (requires StorageClassState)
pub async fn handle_post_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    body: Bytes,
) -> Response {
    // Check for ?uploads query parameter
    if query.uploads.is_some() {
        return create_multipart_upload(State(state), Path((bucket, key))).await;
    }

    // Check for ?uploadId=X query parameter (CompleteMultipartUpload)
    if query.upload_id.is_some() {
        return complete_multipart_upload(State(state), Path((bucket, key)), Query(query), body).await;
    }

    // Check for ?select&select-type=2 query parameters (S3 Select)
    if query.select.is_some() {
        // Validate select-type is "2"
        if query.select_type.as_deref() != Some("2") {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "S3 Select requires select-type=2",
            );
        }
        return select_object_content(State(state), Path((bucket, key)), body).await;
    }

    // Note: ?restore requires StorageClassState which isn't available here
    // It's routed via the combined state handler instead
    if query.restore.is_some() {
        return xml_error(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "RestoreObject requires using the combined state route",
        );
    }

    // No recognized query parameters
    xml_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "POST requests require ?uploads, ?uploadId, or ?select parameter",
    )
}

/// Handle POST requests to object paths with full combined state.
/// This handler supports all POST operations including restore.
/// POST /{bucket}/{key} with ?uploads -> CreateMultipartUpload
/// POST /{bucket}/{key} with ?uploadId=X -> CompleteMultipartUpload
/// POST /{bucket}/{key} with ?select&select-type=2 -> SelectObjectContent
/// POST /{bucket}/{key} with ?restore -> RestoreObject
pub async fn handle_post_object_full(
    State((state, _cors, _versioning, _lifecycle, _encryption, _object_lock, _replication, storage_class_state)): State<CombinedS3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    body: Bytes,
) -> Response {
    // Check for ?uploads query parameter
    if query.uploads.is_some() {
        return create_multipart_upload(State(state), Path((bucket, key))).await;
    }

    // Check for ?uploadId=X query parameter (CompleteMultipartUpload)
    if query.upload_id.is_some() {
        return complete_multipart_upload(State(state), Path((bucket, key)), Query(query), body).await;
    }

    // Check for ?select&select-type=2 query parameters (S3 Select)
    if query.select.is_some() {
        // Validate select-type is "2"
        if query.select_type.as_deref() != Some("2") {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "S3 Select requires select-type=2",
            );
        }
        return select_object_content(State(state), Path((bucket, key)), body).await;
    }

    // Check for ?restore query parameter
    if query.restore.is_some() {
        return restore_object(State(storage_class_state), Path((bucket, key)), body).await;
    }

    // No recognized query parameters
    xml_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "POST requests require ?uploads, ?uploadId, ?select, or ?restore parameter",
    )
}

/// Handle DELETE requests to object paths - routes based on query params.
/// DELETE /{bucket}/{key} with ?uploadId=X -> AbortMultipartUpload
/// DELETE /{bucket}/{key} without query -> DeleteObject
pub async fn handle_delete_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
) -> Response {
    // Check for ?uploadId=X query parameter (AbortMultipartUpload)
    if query.upload_id.is_some() {
        return abort_multipart_upload(State(state), Path((bucket, key)), Query(query)).await;
    }

    // Regular DeleteObject
    delete_object(State(state), Path((bucket, key))).await
}

/// Handle PUT requests to object paths - routes based on query params.
/// PUT /{bucket}/{key} with ?partNumber=N&uploadId=X -> UploadPart
/// PUT /{bucket}/{key} without query -> PutObject
pub async fn handle_put_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for multipart upload part
    if query.upload_id.is_some() && query.part_number.is_some() {
        return upload_part(State(state), Path((bucket, key)), Query(query), body).await;
    }

    // Regular PutObject
    put_object(State(state), Path((bucket, key)), headers, body).await
}

/// Handle GET requests to bucket paths - routes based on query params.
/// GET /{bucket}?uploads -> ListMultipartUploads
/// GET /{bucket} -> ListObjects
pub async fn handle_get_bucket(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    Query(query): Query<MultipartQuery>,
    Query(list_query): Query<ListObjectsQuery>,
) -> Response {
    // Check for ?uploads query parameter
    if query.uploads.is_some() {
        return list_multipart_uploads(State(state), Path(bucket)).await;
    }

    // Regular ListObjects
    list_objects(State(state), Path(bucket), Query(list_query)).await
}

/// Handle GET requests to object paths - routes based on query params.
/// GET /{bucket}/{key}?uploadId=X -> ListParts
/// GET /{bucket}/{key} -> GetObject
pub async fn handle_get_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
) -> Response {
    // Check for ?uploadId=X query parameter (ListParts)
    if query.upload_id.is_some() {
        return list_parts(State(state), Path((bucket, key)), Query(query)).await;
    }

    // Regular GetObject
    get_object(State(state), Path((bucket, key))).await
}

// CORS operations

/// Query parameters for bucket configuration operations.
#[derive(Debug, Deserialize, Default)]
pub struct BucketConfigQuery {
    /// CORS configuration request when present.
    pub cors: Option<String>,
    /// Versioning configuration request when present.
    pub versioning: Option<String>,
    /// Lifecycle configuration request when present.
    pub lifecycle: Option<String>,
    /// Encryption configuration request when present.
    pub encryption: Option<String>,
    /// Object lock configuration request when present.
    #[serde(rename = "object-lock")]
    pub object_lock: Option<String>,
    /// Replication configuration request when present.
    pub replication: Option<String>,
}

/// Query parameters for object-level configuration operations.
#[derive(Debug, Deserialize, Default)]
pub struct ObjectConfigQuery {
    /// Retention configuration request when present.
    pub retention: Option<String>,
    /// Legal hold configuration request when present.
    #[serde(rename = "legal-hold")]
    pub legal_hold: Option<String>,
    /// Restore request when present (for archived objects).
    pub restore: Option<String>,
    /// Select request when present (for SQL queries).
    pub select: Option<String>,
    /// Select type indicator.
    #[serde(rename = "select-type")]
    pub select_type: Option<String>,
}

/// Get bucket CORS configuration.
/// GET /{bucket}?cors
pub async fn get_bucket_cors(
    State(cors_state): State<CorsState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketCors");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    match cors_state.store.get(&bucket) {
        Some(config) => {
            let body = cors_to_xml(&config);
            xml_response(body)
        }
        None => {
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
            )
        }
    }
}

/// Set bucket CORS configuration.
/// PUT /{bucket}?cors
pub async fn put_bucket_cors(
    State(cors_state): State<CorsState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketCors");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_cors_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse CORS XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    match cors_state.store.set(&bucket, config) {
        Ok(()) => {
            (StatusCode::OK, "").into_response()
        }
        Err(e) => {
            error!(bucket = %bucket, error = %e, "Failed to set CORS config");
            xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &e.to_string(),
            )
        }
    }
}

/// Delete bucket CORS configuration.
/// DELETE /{bucket}?cors
pub async fn delete_bucket_cors(
    State(cors_state): State<CorsState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "DeleteBucketCors");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    cors_state.store.remove(&bucket);
    (StatusCode::NO_CONTENT, "").into_response()
}

/// Handle GET requests to bucket paths - routes based on query params.
/// GET /{bucket}?cors -> GetBucketCors
/// GET /{bucket}?uploads -> ListMultipartUploads
/// GET /{bucket} -> ListObjects
pub async fn handle_get_bucket_with_cors(
    State((s3_state, cors_state)): State<(S3State, CorsState)>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
    Query(query): Query<MultipartQuery>,
    Query(list_query): Query<ListObjectsQuery>,
) -> Response {
    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return get_bucket_cors(State(cors_state), Path(bucket)).await;
    }

    // Check for ?uploads query parameter
    if query.uploads.is_some() {
        return list_multipart_uploads(State(s3_state), Path(bucket)).await;
    }

    // Regular ListObjects
    list_objects(State(s3_state), Path(bucket), Query(list_query)).await
}

/// Handle PUT requests to bucket paths - routes based on query params.
/// PUT /{bucket}?cors -> PutBucketCors
/// PUT /{bucket} -> CreateBucket
pub async fn handle_put_bucket_with_cors(
    State((s3_state, cors_state)): State<(S3State, CorsState)>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
    body: Bytes,
) -> Response {
    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return put_bucket_cors(State(cors_state), Path(bucket), body).await;
    }

    // Regular CreateBucket
    create_bucket(State(s3_state), Path(bucket)).await
}

/// Handle DELETE requests to bucket paths - routes based on query params.
/// DELETE /{bucket}?cors -> DeleteBucketCors
/// DELETE /{bucket} -> DeleteBucket
pub async fn handle_delete_bucket_with_cors(
    State((s3_state, cors_state)): State<(S3State, CorsState)>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
) -> Response {
    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return delete_bucket_cors(State(cors_state), Path(bucket)).await;
    }

    // Regular DeleteBucket
    delete_bucket(State(s3_state), Path(bucket)).await
}

// Versioning operations

/// Get bucket versioning configuration.
/// GET /{bucket}?versioning
pub async fn get_bucket_versioning(
    State(versioning_state): State<VersioningState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketVersioning");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let config = versioning_state.store.get(&bucket);
    let body = versioning_to_xml(&config);
    xml_response(body)
}

/// Set bucket versioning configuration.
/// PUT /{bucket}?versioning
pub async fn put_bucket_versioning(
    State(versioning_state): State<VersioningState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketVersioning");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_versioning_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse versioning XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    versioning_state.store.set(&bucket, config);
    (StatusCode::OK, "").into_response()
}

// Lifecycle operations

/// Get bucket lifecycle configuration.
/// GET /{bucket}?lifecycle
pub async fn get_bucket_lifecycle(
    State(lifecycle_state): State<LifecycleState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketLifecycle");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    match lifecycle_state.store.get(&bucket) {
        Some(config) => {
            let body = lifecycle_to_xml(&config);
            xml_response(body)
        }
        None => {
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
            )
        }
    }
}

/// Set bucket lifecycle configuration.
/// PUT /{bucket}?lifecycle
pub async fn put_bucket_lifecycle(
    State(lifecycle_state): State<LifecycleState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketLifecycle");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_lifecycle_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse lifecycle XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    match lifecycle_state.store.set(&bucket, config) {
        Ok(()) => (StatusCode::OK, "").into_response(),
        Err(e) => {
            error!(bucket = %bucket, error = %e, "Failed to set lifecycle config");
            xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &e.to_string(),
            )
        }
    }
}

/// Delete bucket lifecycle configuration.
/// DELETE /{bucket}?lifecycle
pub async fn delete_bucket_lifecycle(
    State(lifecycle_state): State<LifecycleState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "DeleteBucketLifecycle");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    lifecycle_state.store.remove(&bucket);
    (StatusCode::NO_CONTENT, "").into_response()
}

/// Combined state for handlers that need S3, CORS, Versioning, Lifecycle, and other feature states.
pub type CombinedS3State = (S3State, CorsState, VersioningState, LifecycleState, EncryptionState, ObjectLockState, ReplicationState, StorageClassState);

/// Handle GET requests to bucket paths - routes based on query params.
/// GET /{bucket}?lifecycle -> GetBucketLifecycle
/// GET /{bucket}?versioning -> GetBucketVersioning
/// GET /{bucket}?cors -> GetBucketCors
/// GET /{bucket}?uploads -> ListMultipartUploads
/// GET /{bucket} -> ListObjects
pub async fn handle_get_bucket_full(
    State((s3_state, cors_state, versioning_state, lifecycle_state, encryption_state, object_lock_state, replication_state, _storage_class_state)): State<CombinedS3State>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
    Query(query): Query<MultipartQuery>,
    Query(list_query): Query<ListObjectsQuery>,
) -> Response {
    // Check for ?replication query parameter
    if config_query.replication.is_some() {
        return get_bucket_replication(State(replication_state), Path(bucket)).await;
    }

    // Check for ?object-lock query parameter
    if config_query.object_lock.is_some() {
        return get_bucket_object_lock(State(object_lock_state), Path(bucket)).await;
    }

    // Check for ?encryption query parameter
    if config_query.encryption.is_some() {
        return get_bucket_encryption(State(encryption_state), Path(bucket)).await;
    }

    // Check for ?lifecycle query parameter
    if config_query.lifecycle.is_some() {
        return get_bucket_lifecycle(State(lifecycle_state), Path(bucket)).await;
    }

    // Check for ?versioning query parameter
    if config_query.versioning.is_some() {
        return get_bucket_versioning(State(versioning_state), Path(bucket)).await;
    }

    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return get_bucket_cors(State(cors_state), Path(bucket)).await;
    }

    // Check for ?uploads query parameter
    if query.uploads.is_some() {
        return list_multipart_uploads(State(s3_state), Path(bucket)).await;
    }

    // Regular ListObjects
    list_objects(State(s3_state), Path(bucket), Query(list_query)).await
}

/// Handle PUT requests to bucket paths - routes based on query params.
/// PUT /{bucket}?lifecycle -> PutBucketLifecycle
/// PUT /{bucket}?versioning -> PutBucketVersioning
/// PUT /{bucket}?cors -> PutBucketCors
/// PUT /{bucket} -> CreateBucket
pub async fn handle_put_bucket_full(
    State((s3_state, cors_state, versioning_state, lifecycle_state, encryption_state, object_lock_state, replication_state, _storage_class_state)): State<CombinedS3State>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
    body: Bytes,
) -> Response {
    // Check for ?replication query parameter
    if config_query.replication.is_some() {
        return put_bucket_replication(State(replication_state), Path(bucket), body).await;
    }

    // Check for ?object-lock query parameter
    if config_query.object_lock.is_some() {
        return put_bucket_object_lock(State(object_lock_state), Path(bucket), body).await;
    }

    // Check for ?encryption query parameter
    if config_query.encryption.is_some() {
        return put_bucket_encryption(State(encryption_state), Path(bucket), body).await;
    }

    // Check for ?lifecycle query parameter
    if config_query.lifecycle.is_some() {
        return put_bucket_lifecycle(State(lifecycle_state), Path(bucket), body).await;
    }

    // Check for ?versioning query parameter
    if config_query.versioning.is_some() {
        return put_bucket_versioning(State(versioning_state), Path(bucket), body).await;
    }

    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return put_bucket_cors(State(cors_state), Path(bucket), body).await;
    }

    // Regular CreateBucket
    create_bucket(State(s3_state), Path(bucket)).await
}

/// Handle DELETE requests to bucket paths - routes based on query params.
/// DELETE /{bucket}?lifecycle -> DeleteBucketLifecycle
/// DELETE /{bucket}?cors -> DeleteBucketCors
/// DELETE /{bucket} -> DeleteBucket
pub async fn handle_delete_bucket_full(
    State((s3_state, cors_state, _versioning_state, lifecycle_state, encryption_state, _object_lock_state, replication_state, _storage_class_state)): State<CombinedS3State>,
    Path(bucket): Path<String>,
    Query(config_query): Query<BucketConfigQuery>,
) -> Response {
    // Note: Object Lock configuration cannot be deleted once enabled (per S3 spec)

    // Check for ?replication query parameter
    if config_query.replication.is_some() {
        return delete_bucket_replication(State(replication_state), Path(bucket)).await;
    }

    // Check for ?encryption query parameter
    if config_query.encryption.is_some() {
        return delete_bucket_encryption(State(encryption_state), Path(bucket)).await;
    }

    // Check for ?lifecycle query parameter
    if config_query.lifecycle.is_some() {
        return delete_bucket_lifecycle(State(lifecycle_state), Path(bucket)).await;
    }

    // Check for ?cors query parameter
    if config_query.cors.is_some() {
        return delete_bucket_cors(State(cors_state), Path(bucket)).await;
    }

    // Regular DeleteBucket
    delete_bucket(State(s3_state), Path(bucket)).await
}

// Encryption operations

/// Get bucket encryption configuration.
/// GET /{bucket}?encryption
pub async fn get_bucket_encryption(
    State(encryption_state): State<EncryptionState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketEncryption");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    match encryption_state.store.get(&bucket) {
        Some(config) => {
            let body = encryption_to_xml(&config);
            xml_response(body)
        }
        None => {
            xml_error(
                StatusCode::NOT_FOUND,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
            )
        }
    }
}

/// Set bucket encryption configuration.
/// PUT /{bucket}?encryption
pub async fn put_bucket_encryption(
    State(encryption_state): State<EncryptionState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketEncryption");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_encryption_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse encryption XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    encryption_state.store.set(&bucket, config);
    (StatusCode::OK, "").into_response()
}

/// Delete bucket encryption configuration.
/// DELETE /{bucket}?encryption
pub async fn delete_bucket_encryption(
    State(encryption_state): State<EncryptionState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "DeleteBucketEncryption");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    encryption_state.store.remove(&bucket);
    (StatusCode::NO_CONTENT, "").into_response()
}

// Object Lock operations

/// Get bucket object lock configuration.
/// GET /{bucket}?object-lock
pub async fn get_bucket_object_lock(
    State(object_lock_state): State<ObjectLockState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketObjectLockConfiguration");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let store = object_lock_state.read();
    match store.get_bucket_config(&bucket) {
        Some(config) => xml_response(object_lock_to_xml(config)),
        None => xml_error(
            StatusCode::NOT_FOUND,
            "ObjectLockConfigurationNotFoundError",
            "Object Lock configuration does not exist for this bucket",
        ),
    }
}

/// Set bucket object lock configuration.
/// PUT /{bucket}?object-lock
pub async fn put_bucket_object_lock(
    State(object_lock_state): State<ObjectLockState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketObjectLockConfiguration");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_object_lock_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse object lock XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    let mut store = object_lock_state.write();
    match store.set_bucket_config(bucket, config) {
        Ok(_) => (StatusCode::OK, "").into_response(),
        Err(e) => xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            &e.to_string(),
        ),
    }
}

/// Get object retention.
/// GET /{bucket}/{key}?retention
pub async fn get_object_retention(
    State(object_lock_state): State<ObjectLockState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "GetObjectRetention");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    let object_key = ObjectKey {
        bucket,
        key,
        version_id: None,
    };

    let store = object_lock_state.read();
    match store.get_retention(&object_key) {
        Some(retention) => xml_response(retention_to_xml(retention)),
        None => xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchObjectLockConfiguration",
            "The specified object does not have a retention configuration",
        ),
    }
}

/// Set object retention.
/// PUT /{bucket}/{key}?retention
pub async fn put_object_retention(
    State(object_lock_state): State<ObjectLockState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, key = %key, "PutObjectRetention");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // Check if object lock is enabled for the bucket
    {
        let store = object_lock_state.read();
        if !store.is_enabled(&bucket) {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Object Lock is not enabled for this bucket",
            );
        }
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let retention = match parse_retention_xml(xml) {
        Ok(r) => r,
        Err(e) => {
            warn!(bucket = %bucket, key = %key, error = %e, "Failed to parse retention XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    let object_key = ObjectKey {
        bucket,
        key,
        version_id: None,
    };

    let mut store = object_lock_state.write();
    match store.set_retention(object_key, retention) {
        Ok(_) => (StatusCode::OK, "").into_response(),
        Err(e) => xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            &e.to_string(),
        ),
    }
}

/// Get object legal hold.
/// GET /{bucket}/{key}?legal-hold
pub async fn get_object_legal_hold(
    State(object_lock_state): State<ObjectLockState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "GetObjectLegalHold");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    let object_key = ObjectKey {
        bucket,
        key,
        version_id: None,
    };

    let store = object_lock_state.read();
    match store.get_legal_hold(&object_key) {
        Some(hold) => xml_response(legal_hold_to_xml(hold)),
        None => {
            // Return default OFF status if not set
            xml_response(legal_hold_to_xml(&ObjectLegalHold {
                status: super::object_lock::LegalHoldStatus::Off,
            }))
        }
    }
}

/// Set object legal hold.
/// PUT /{bucket}/{key}?legal-hold
pub async fn put_object_legal_hold(
    State(object_lock_state): State<ObjectLockState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, key = %key, "PutObjectLegalHold");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // Check if object lock is enabled for the bucket
    {
        let store = object_lock_state.read();
        if !store.is_enabled(&bucket) {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Object Lock is not enabled for this bucket",
            );
        }
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let hold = match parse_legal_hold_xml(xml) {
        Ok(h) => h,
        Err(e) => {
            warn!(bucket = %bucket, key = %key, error = %e, "Failed to parse legal hold XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    let object_key = ObjectKey {
        bucket,
        key,
        version_id: None,
    };

    let mut store = object_lock_state.write();
    store.set_legal_hold(object_key, hold);
    (StatusCode::OK, "").into_response()
}

// Replication operations

/// Get bucket replication configuration.
/// GET /{bucket}?replication
pub async fn get_bucket_replication(
    State(replication_state): State<ReplicationState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "GetBucketReplication");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let store = replication_state.read();
    match store.get(&bucket) {
        Some(config) => xml_response(replication_to_xml(config)),
        None => xml_error(
            StatusCode::NOT_FOUND,
            "ReplicationConfigurationNotFoundError",
            "The replication configuration was not found",
        ),
    }
}

/// Set bucket replication configuration.
/// PUT /{bucket}?replication
pub async fn put_bucket_replication(
    State(replication_state): State<ReplicationState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, "PutBucketReplication");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let config = match parse_replication_xml(xml) {
        Ok(c) => c,
        Err(e) => {
            warn!(bucket = %bucket, error = %e, "Failed to parse replication XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    let mut store = replication_state.write();
    match store.set(bucket, config) {
        Ok(_) => (StatusCode::OK, "").into_response(),
        Err(e) => xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            &e.to_string(),
        ),
    }
}

/// Delete bucket replication configuration.
/// DELETE /{bucket}?replication
pub async fn delete_bucket_replication(
    State(replication_state): State<ReplicationState>,
    Path(bucket): Path<String>,
) -> Response {
    debug!(bucket = %bucket, "DeleteBucketReplication");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }

    let mut store = replication_state.write();
    store.remove(&bucket);
    (StatusCode::NO_CONTENT, "").into_response()
}

// Storage Class operations

/// Restore an archived object.
/// POST /{bucket}/{key}?restore
pub async fn restore_object(
    State(storage_class_state): State<StorageClassState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, key = %key, "RestoreObject");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // Get storage info for the object
    let storage_info = match storage_class_state.store.get(&bucket, &key) {
        Some(info) => info,
        None => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
            );
        }
    };

    // Check if object is in an archived storage class
    use super::lifecycle::StorageClass;
    let is_archived = matches!(
        storage_info.storage_class,
        StorageClass::Glacier | StorageClass::GlacierIr | StorageClass::DeepArchive
    );

    if !is_archived {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidObjectState",
            "The operation is not valid for the object's storage class.",
        );
    }

    // Parse restore request XML
    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let restore_request = match parse_restore_xml(xml) {
        Ok(r) => r,
        Err(e) => {
            warn!(bucket = %bucket, key = %key, error = %e, "Failed to parse restore XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        days = restore_request.days,
        tier = %restore_request.tier.as_str(),
        "Initiating object restore"
    );

    // In a real implementation, this would trigger an async restore job
    // For now, we'll return 202 Accepted to indicate the restore is in progress
    (
        StatusCode::ACCEPTED,
        [("x-amz-restore", "ongoing-request=\"true\"")],
        "",
    )
        .into_response()
}

/// Get object storage class information.
/// HEAD /{bucket}/{key} with x-amz-storage-class header
pub async fn get_object_storage_class(
    State(storage_class_state): State<StorageClassState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "GetObjectStorageClass");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    match storage_class_state.store.get(&bucket, &key) {
        Some(info) => {
            use axum::http::header::HeaderName;

            let storage_class = info.storage_class.as_str();

            // Check for ongoing restore
            if matches!(
                info.storage_class,
                super::lifecycle::StorageClass::Glacier
                    | super::lifecycle::StorageClass::GlacierIr
                    | super::lifecycle::StorageClass::DeepArchive
            ) {
                // For archived objects, include restore status
                let restore_status = RestoreStatus {
                    in_progress: false,
                    expiry_date: None,
                };
                let restore_header = super::storage_class::restore_status_header(&restore_status);
                (
                    StatusCode::OK,
                    [
                        (HeaderName::from_static("x-amz-storage-class"), storage_class.to_string()),
                        (HeaderName::from_static("x-amz-restore"), restore_header),
                    ],
                    "",
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    [(HeaderName::from_static("x-amz-storage-class"), storage_class.to_string())],
                    "",
                )
                    .into_response()
            }
        }
        None => {
            // Object not tracked in storage class store - assume STANDARD
            (
                StatusCode::OK,
                [("x-amz-storage-class", "STANDARD")],
                "",
            )
                .into_response()
        }
    }
}

/// Set object storage class (via copy or lifecycle).
/// This is typically done via x-amz-storage-class header on PUT
pub async fn set_object_storage_class(
    State(storage_class_state): State<StorageClassState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    debug!(bucket = %bucket, key = %key, "SetObjectStorageClass");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // Get storage class from header
    let storage_class_str = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("STANDARD");

    use super::lifecycle::StorageClass;
    let storage_class = StorageClass::from_str(storage_class_str).unwrap_or(StorageClass::Standard);

    // Create or update storage info
    let info = ObjectStorageInfo::new(&key, 0).with_storage_class(storage_class);
    storage_class_state.store.set(&bucket, &key, info);

    (StatusCode::OK, "").into_response()
}

// S3 Select operations

/// Execute S3 Select query on an object.
/// POST /{bucket}/{key}?select&select-type=2
pub async fn select_object_content(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    debug!(bucket = %bucket, key = %key, "SelectObjectContent");

    if let Err(e) = validate_bucket_name(&bucket) {
        return e;
    }
    if let Err(e) = validate_s3_key(&key) {
        return e;
    }

    // Parse request XML
    let xml = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "Invalid UTF-8 in request body",
            );
        }
    };

    let select_request = match parse_select_xml(xml) {
        Ok(r) => r,
        Err(e) => {
            warn!(bucket = %bucket, key = %key, error = %e, "Failed to parse select XML");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                &e.to_string(),
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        expression = %select_request.expression,
        "Executing S3 Select query"
    );

    // Look up the bucket
    let bucket_inode = match state.metadata.lookup(ROOT_INODE, &bucket).await {
        Ok(Some(inode)) => inode.id,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, "Failed to lookup bucket");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Look up the object
    let object_inode = match state.metadata.lookup(bucket_inode, &key).await {
        Ok(Some(inode)) => inode,
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
            );
        }
        Err(e) => {
            error!(?e, bucket = %bucket, key = %key, "Failed to lookup object");
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Request failed",
            );
        }
    };

    // Fetch object data from chunks
    let mut object_data = Vec::with_capacity(object_inode.size as usize);
    for chunk_id in &object_inode.chunks {
        match state.data.read_chunk(*chunk_id, state.chunk_size).await {
            Ok(data) => {
                object_data.extend_from_slice(&data);
            }
            Err(e) => {
                error!(
                    ?e,
                    bucket = %bucket,
                    key = %key,
                    chunk_id = %chunk_id,
                    "Failed to fetch chunk"
                );
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Object data unavailable",
                );
            }
        }
    }

    // Execute the select query
    let result = match execute_csv_select(&object_data, &select_request) {
        Ok(r) => r,
        Err(e) => {
            warn!(bucket = %bucket, key = %key, error = %e, "S3 Select query failed");
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &e.to_string(),
            );
        }
    };

    debug!(
        bucket = %bucket,
        key = %key,
        records_count = result.records.len(),
        bytes_scanned = result.stats.bytes_scanned,
        "S3 Select query completed"
    );

    // Format output based on output serialization
    let output = if select_request.output_serialization.json.is_some() {
        // JSON output format
        let json_records: Vec<String> = result
            .records
            .iter()
            .map(|r| format!("{{\"_1\":\"{}\"}}", r.replace('"', "\\\"")))
            .collect();
        json_records.join("\n")
    } else {
        // Default CSV output
        result.records.join("\n")
    };

    // Include stats in response headers
    let stats_xml = select_stats_to_xml(&result.stats);

    (
        StatusCode::OK,
        [
            ("Content-Type", "application/octet-stream"),
            ("x-amz-request-charged", "requester"),
        ],
        format!("{}\n<!-- Stats: {} -->", output, stats_xml),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xml_error() {
        let _response = xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "Key not found");
        // Just verify it compiles and returns
    }
}
