/**
 * Type definitions for the Strata SDK
 */

export interface BucketOptions {
  region?: string;
  versioning?: boolean;
  encryption?: boolean;
}

export interface PutObjectOptions {
  contentType?: string;
  metadata?: Record<string, string>;
  cacheControl?: string;
  contentDisposition?: string;
  contentEncoding?: string;
  contentLanguage?: string;
  expires?: Date;
  storageClass?: StorageClass;
  acl?: CannedACL;
  tags?: Record<string, string>;
}

export interface GetObjectOptions {
  range?: { start: number; end: number };
  versionId?: string;
  ifMatch?: string;
  ifNoneMatch?: string;
  ifModifiedSince?: Date;
  ifUnmodifiedSince?: Date;
}

export interface ListObjectsOptions {
  prefix?: string;
  delimiter?: string;
  maxKeys?: number;
  continuationToken?: string;
  startAfter?: string;
  fetchOwner?: boolean;
}

export interface CopyObjectOptions {
  metadata?: Record<string, string>;
  metadataDirective?: 'COPY' | 'REPLACE';
  taggingDirective?: 'COPY' | 'REPLACE';
  storageClass?: StorageClass;
  acl?: CannedACL;
}

export interface DeleteObjectOptions {
  versionId?: string;
  bypassGovernanceRetention?: boolean;
}

export interface DeleteObjectsOptions {
  quiet?: boolean;
}

export interface DeleteObjectsResult {
  deleted: Array<{ key: string; versionId?: string }>;
  errors: Array<{ key: string; versionId?: string; code: string; message: string }>;
}

export type StorageClass =
  | 'STANDARD'
  | 'REDUCED_REDUNDANCY'
  | 'STANDARD_IA'
  | 'ONEZONE_IA'
  | 'INTELLIGENT_TIERING'
  | 'GLACIER'
  | 'DEEP_ARCHIVE'
  | 'GLACIER_IR';

export type CannedACL =
  | 'private'
  | 'public-read'
  | 'public-read-write'
  | 'authenticated-read'
  | 'aws-exec-read'
  | 'bucket-owner-read'
  | 'bucket-owner-full-control';

export interface BucketVersioning {
  enabled: boolean;
  mfaDelete?: boolean;
}

export interface BucketEncryption {
  enabled: boolean;
  algorithm: 'AES256' | 'aws:kms';
  kmsKeyId?: string;
}

export interface LifecycleRule {
  id: string;
  prefix?: string;
  enabled: boolean;
  expirationDays?: number;
  transitionDays?: number;
  transitionStorageClass?: StorageClass;
  noncurrentVersionExpirationDays?: number;
  abortIncompleteMultipartUploadDays?: number;
}

export interface CORSRule {
  allowedOrigins: string[];
  allowedMethods: string[];
  allowedHeaders?: string[];
  exposeHeaders?: string[];
  maxAgeSeconds?: number;
}

export interface PolicyStatement {
  sid?: string;
  effect: 'Allow' | 'Deny';
  principal: string | string[] | { [key: string]: string | string[] };
  action: string | string[];
  resource: string | string[];
  condition?: Record<string, Record<string, string | string[]>>;
}

export interface BucketPolicy {
  version: string;
  statements: PolicyStatement[];
}

export interface MultipartUploadPart {
  partNumber: number;
  etag: string;
  size: number;
}

export interface PresignedUrlOptions {
  expiresIn?: number; // seconds
  method?: 'GET' | 'PUT';
  contentType?: string;
}

export interface BucketMetrics {
  objectCount: number;
  totalSizeBytes: number;
  requestsLastHour: number;
  bandwidthLastHourBytes: number;
}

export interface StreamOptions {
  highWaterMark?: number;
}
