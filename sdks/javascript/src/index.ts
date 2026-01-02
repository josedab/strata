/**
 * Strata JavaScript/TypeScript SDK
 *
 * A client library for the Strata distributed filesystem.
 */

export { StrataClient } from './client';
export { Bucket } from './bucket';
export { StrataObject, ObjectMetadata, ListObjectsResult } from './object';
export { ClientConfig, TLSConfig, RetryConfig } from './config';
export {
  StrataError,
  NotFoundError,
  PermissionDeniedError,
  AlreadyExistsError,
  TimeoutError,
  ConnectionError,
  ValidationError,
  QuotaExceededError,
  RateLimitError,
} from './errors';
export type {
  BucketOptions,
  PutObjectOptions,
  GetObjectOptions,
  ListObjectsOptions,
  CopyObjectOptions,
} from './types';
