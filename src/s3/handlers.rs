//! S3 API request handlers.

use super::gateway::S3State;
use crate::types::{FileType, InodeId};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::debug;

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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
    }
}

// Object-level operations

pub async fn get_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "GetObject");

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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
            );
        }
    };

    // Look up the object
    match state.metadata.lookup(bucket_inode, &key).await {
        Ok(Some(inode)) => {
            // Object exists, but we need data from data servers
            // For now, return empty content with metadata
            let size = inode.size;
            let etag = format!("\"{:x}\"", md5::compute(&[]));

            (
                StatusCode::OK,
                [
                    ("Content-Length", size.to_string()),
                    ("ETag", etag),
                    ("Content-Type", "application/octet-stream".to_string()),
                ],
                Bytes::new(), // TODO: Fetch actual data from data servers
            )
                .into_response()
        }
        Ok(None) => xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        ),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
            );
        }
    };

    // Calculate ETag (MD5 hash)
    let etag = format!("\"{:x}\"", md5::compute(&body));

    // Create file in metadata
    // TODO: Also store data on data servers
    match state
        .metadata
        .create_file(bucket_inode, &key, 0o644, 0, 0)
        .await
    {
        Ok(response) => {
            if response.success {
                (StatusCode::OK, [("ETag", etag.as_str())], "").into_response()
            } else {
                xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    response.error.as_deref().unwrap_or("Unknown error"),
                )
            }
        }
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
    }
}

pub async fn delete_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!(bucket = %bucket, key = %key, "DeleteObject");

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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
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
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &e.to_string(),
            );
        }
    };

    // Look up the object
    match state.metadata.lookup(bucket_inode, &key).await {
        Ok(Some(inode)) => {
            let size = inode.size;
            let etag = format!("\"{:x}\"", md5::compute(&[]));

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
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &e.to_string(),
        ),
    }
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
