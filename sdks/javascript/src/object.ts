/**
 * Object types and classes for the Strata SDK
 */

import type { StorageClass, CannedACL } from './types';

/**
 * Metadata for a stored object
 */
export interface ObjectMetadata {
  key: string;
  bucket: string;
  size: number;
  etag: string;
  versionId?: string;
  contentType: string;
  lastModified: Date;
  metadata: Record<string, string>;
  storageClass?: StorageClass;
  owner?: {
    id: string;
    displayName?: string;
  };
  checksumCRC32?: string;
  checksumCRC32C?: string;
  checksumSHA1?: string;
  checksumSHA256?: string;
}

/**
 * Result of listing objects in a bucket
 */
export interface ListObjectsResult {
  objects: ObjectMetadata[];
  commonPrefixes: string[];
  isTruncated: boolean;
  nextContinuationToken?: string;
  keyCount: number;
}

/**
 * Represents a Strata object with its data and metadata
 */
export class StrataObject {
  readonly metadata: ObjectMetadata;
  private data: Buffer;

  constructor(metadata: ObjectMetadata, data: Buffer) {
    this.metadata = metadata;
    this.data = data;
  }

  /**
   * Get the object key
   */
  get key(): string {
    return this.metadata.key;
  }

  /**
   * Get the bucket name
   */
  get bucket(): string {
    return this.metadata.bucket;
  }

  /**
   * Get the object size in bytes
   */
  get size(): number {
    return this.metadata.size;
  }

  /**
   * Get the content type
   */
  get contentType(): string {
    return this.metadata.contentType;
  }

  /**
   * Get the ETag
   */
  get etag(): string {
    return this.metadata.etag;
  }

  /**
   * Get the version ID (if versioning is enabled)
   */
  get versionId(): string | undefined {
    return this.metadata.versionId;
  }

  /**
   * Get the last modified date
   */
  get lastModified(): Date {
    return this.metadata.lastModified;
  }

  /**
   * Get custom metadata
   */
  get customMetadata(): Record<string, string> {
    return this.metadata.metadata;
  }

  /**
   * Get the object data as a Buffer
   */
  toBuffer(): Buffer {
    return this.data;
  }

  /**
   * Get the object data as a Uint8Array
   */
  toUint8Array(): Uint8Array {
    return new Uint8Array(this.data);
  }

  /**
   * Get the object data as a string
   */
  toString(encoding: BufferEncoding = 'utf-8'): string {
    return this.data.toString(encoding);
  }

  /**
   * Parse the object data as JSON
   */
  toJSON<T = unknown>(): T {
    return JSON.parse(this.toString()) as T;
  }

  /**
   * Get the object data as an ArrayBuffer
   */
  toArrayBuffer(): ArrayBuffer {
    return this.data.buffer.slice(
      this.data.byteOffset,
      this.data.byteOffset + this.data.byteLength
    );
  }

  /**
   * Create a Blob from the object data (browser environment)
   */
  toBlob(): Blob {
    return new Blob([this.data], { type: this.contentType });
  }

  /**
   * Get a readable stream of the object data
   */
  toStream(): ReadableStream<Uint8Array> {
    const data = this.data;
    return new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(data));
        controller.close();
      },
    });
  }

  /**
   * Check if the object data matches an expected checksum
   */
  async verifyChecksum(
    algorithm: 'SHA-256' | 'SHA-1',
    expectedHash: string
  ): Promise<boolean> {
    const hashBuffer = await crypto.subtle.digest(algorithm, this.data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
    return hashHex === expectedHash.toLowerCase();
  }
}

/**
 * Builder for creating object upload options
 */
export class ObjectUploadBuilder {
  private options: {
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
  } = {};

  /**
   * Set the content type
   */
  contentType(type: string): this {
    this.options.contentType = type;
    return this;
  }

  /**
   * Add custom metadata
   */
  metadata(key: string, value: string): this {
    this.options.metadata = this.options.metadata ?? {};
    this.options.metadata[key] = value;
    return this;
  }

  /**
   * Set all custom metadata at once
   */
  allMetadata(metadata: Record<string, string>): this {
    this.options.metadata = { ...this.options.metadata, ...metadata };
    return this;
  }

  /**
   * Set cache control header
   */
  cacheControl(value: string): this {
    this.options.cacheControl = value;
    return this;
  }

  /**
   * Set content disposition header
   */
  contentDisposition(value: string): this {
    this.options.contentDisposition = value;
    return this;
  }

  /**
   * Set content encoding
   */
  contentEncoding(value: string): this {
    this.options.contentEncoding = value;
    return this;
  }

  /**
   * Set content language
   */
  contentLanguage(value: string): this {
    this.options.contentLanguage = value;
    return this;
  }

  /**
   * Set expiration date
   */
  expires(date: Date): this {
    this.options.expires = date;
    return this;
  }

  /**
   * Set storage class
   */
  storageClass(storageClass: StorageClass): this {
    this.options.storageClass = storageClass;
    return this;
  }

  /**
   * Set ACL
   */
  acl(acl: CannedACL): this {
    this.options.acl = acl;
    return this;
  }

  /**
   * Add a tag
   */
  tag(key: string, value: string): this {
    this.options.tags = this.options.tags ?? {};
    this.options.tags[key] = value;
    return this;
  }

  /**
   * Set all tags at once
   */
  allTags(tags: Record<string, string>): this {
    this.options.tags = { ...this.options.tags, ...tags };
    return this;
  }

  /**
   * Build the options object
   */
  build(): typeof this.options {
    return { ...this.options };
  }
}

/**
 * Multipart upload session
 */
export class MultipartUpload {
  readonly bucket: string;
  readonly key: string;
  readonly uploadId: string;
  private parts: Array<{ partNumber: number; etag: string; size: number }> = [];
  private uploadedBytes = 0;

  constructor(bucket: string, key: string, uploadId: string) {
    this.bucket = bucket;
    this.key = key;
    this.uploadId = uploadId;
  }

  /**
   * Add a completed part
   */
  addPart(partNumber: number, etag: string, size: number): void {
    this.parts.push({ partNumber, etag, size });
    this.uploadedBytes += size;
  }

  /**
   * Get all completed parts
   */
  getParts(): Array<{ partNumber: number; etag: string }> {
    return this.parts
      .sort((a, b) => a.partNumber - b.partNumber)
      .map(({ partNumber, etag }) => ({ partNumber, etag }));
  }

  /**
   * Get the number of completed parts
   */
  get partCount(): number {
    return this.parts.length;
  }

  /**
   * Get total uploaded bytes
   */
  get totalUploadedBytes(): number {
    return this.uploadedBytes;
  }

  /**
   * Check if the upload is ready to complete
   */
  isReady(): boolean {
    return this.parts.length > 0;
  }
}

/**
 * Object version information
 */
export interface ObjectVersion {
  key: string;
  versionId: string;
  isLatest: boolean;
  lastModified: Date;
  etag: string;
  size: number;
  storageClass?: StorageClass;
  owner?: {
    id: string;
    displayName?: string;
  };
}

/**
 * Delete marker for versioned objects
 */
export interface DeleteMarker {
  key: string;
  versionId: string;
  isLatest: boolean;
  lastModified: Date;
  owner?: {
    id: string;
    displayName?: string;
  };
}

/**
 * Result of listing object versions
 */
export interface ListVersionsResult {
  versions: ObjectVersion[];
  deleteMarkers: DeleteMarker[];
  commonPrefixes: string[];
  isTruncated: boolean;
  nextKeyMarker?: string;
  nextVersionIdMarker?: string;
}

/**
 * Object tagging
 */
export interface ObjectTagging {
  tagSet: Array<{ key: string; value: string }>;
}

/**
 * Object ACL grant
 */
export interface ObjectACLGrant {
  grantee: {
    type: 'CanonicalUser' | 'Group' | 'Email';
    id?: string;
    uri?: string;
    email?: string;
    displayName?: string;
  };
  permission: 'FULL_CONTROL' | 'READ' | 'READ_ACP' | 'WRITE_ACP';
}

/**
 * Object ACL
 */
export interface ObjectACL {
  owner: {
    id: string;
    displayName?: string;
  };
  grants: ObjectACLGrant[];
}

/**
 * Object legal hold status
 */
export interface ObjectLegalHold {
  status: 'ON' | 'OFF';
}

/**
 * Object retention configuration
 */
export interface ObjectRetention {
  mode: 'GOVERNANCE' | 'COMPLIANCE';
  retainUntilDate: Date;
}
