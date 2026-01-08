//! S3-compatible gateway for Strata.
//!
//! This module provides an S3-compatible API for accessing Strata storage,
//! allowing cloud-native applications to use Strata as an S3 backend.

mod cors;
mod credentials;
mod encryption;
mod gateway;
mod handlers;
mod lifecycle;
mod middleware;
mod object_lock;
mod presigned;
mod replication;
mod select;
mod signature;
mod storage_class;
mod versioning;
pub mod xml;

pub use cors::{
    cors_to_xml, parse_cors_xml, s3_cors_middleware, CorsConfiguration, CorsError, CorsRule,
    CorsState, CorsStore,
};
pub use encryption::{
    encryption_to_xml, extract_sse_headers, parse_encryption_xml, EncryptionConfiguration,
    EncryptionError, EncryptionRule, EncryptionState, EncryptionStore, ObjectEncryption,
    SseAlgorithm,
};
pub use lifecycle::{
    lifecycle_to_xml, parse_lifecycle_xml, LifecycleConfiguration, LifecycleError,
    LifecycleRule, LifecycleState, LifecycleStore, RuleStatus, StorageClass,
};
pub use versioning::{
    parse_versioning_xml, versioning_to_xml, VersionIdGenerator, VersioningConfiguration,
    VersioningError, VersioningState, VersioningStatus, VersioningStore,
};
pub use object_lock::{
    legal_hold_to_xml, object_lock_to_xml, parse_legal_hold_xml, parse_object_lock_xml,
    parse_retention_xml, retention_to_xml, DefaultRetention, LegalHoldStatus, ObjectKey,
    ObjectLegalHold, ObjectLockConfiguration, ObjectLockError, ObjectLockState, ObjectLockStore,
    ObjectRetention, RetentionMode,
};
pub use replication::{
    parse_replication_xml, replication_to_xml, ReplicationConfiguration, ReplicationDestination,
    ReplicationError, ReplicationFilter, ReplicationRule, ReplicationState, ReplicationStatus,
    ReplicationStore,
};
pub use storage_class::{
    is_valid_transition, parse_restore_xml, restore_status_header, ObjectStorageInfo,
    RestoreRequest, RestoreStatus, RestoreTier, StorageClassError, StorageClassState,
    StorageClassStore, TransitionExecutor, TransitionResult,
};
pub use select::{
    execute_csv_select, parse_select_xml, select_stats_to_xml, AggregateFunction,
    CompressionType, CsvInput, CsvOutput, FileHeaderInfo, InputSerialization, JsonInput,
    JsonOutput, JsonType, OutputSerialization, ParsedQuery, QuoteFields, SelectColumn,
    SelectError, SelectRequest, SelectResult, SelectStats, SqlParser,
};
pub use credentials::{CredentialError, S3AccessKey, S3AuthConfig, S3CredentialStore};
pub use gateway::run_s3_gateway;
pub use middleware::{s3_auth_middleware, S3AuthInfo, S3AuthState};
pub use presigned::{PresignedError, PresignedParams, PresignedUrlGenerator, PresignedValidator};
pub use signature::{AuthorizationHeader, SignatureError, SignatureValidator};
pub use xml::{CompletedPart, XmlParseError};
