/**
 * Bucket operations for the Strata SDK
 */

import type { StrataClient } from './client';
import { StrataObject, ObjectMetadata, ListObjectsResult } from './object';
import { NotFoundError, ValidationError } from './errors';
import type {
  BucketOptions,
  PutObjectOptions,
  GetObjectOptions,
  ListObjectsOptions,
  CopyObjectOptions,
  DeleteObjectOptions,
  DeleteObjectsOptions,
  DeleteObjectsResult,
  BucketVersioning,
  BucketEncryption,
  LifecycleRule,
  CORSRule,
  BucketPolicy,
  PresignedUrlOptions,
  BucketMetrics,
  StreamOptions,
} from './types';

export class Bucket {
  private client: StrataClient;
  readonly name: string;
  readonly options: BucketOptions;

  constructor(client: StrataClient, name: string, options: BucketOptions = {}) {
    this.client = client;
    this.name = name;
    this.options = options;
  }

  /**
   * Put an object into the bucket
   */
  async putObject(
    key: string,
    data: Buffer | Uint8Array | string | ReadableStream,
    options: PutObjectOptions = {}
  ): Promise<ObjectMetadata> {
    this.validateKey(key);

    const body = this.normalizeData(data);
    const contentType = options.contentType ?? this.detectContentType(key);

    const response = await this.client.executeRequest<{
      etag: string;
      versionId?: string;
      size: number;
    }>('putObject', {
      bucket: this.name,
      key,
      body,
      contentType,
      ...options,
    });

    return {
      key,
      bucket: this.name,
      size: response.size,
      etag: response.etag,
      versionId: response.versionId,
      contentType,
      lastModified: new Date(),
      metadata: options.metadata ?? {},
    };
  }

  /**
   * Get an object from the bucket
   */
  async getObject(key: string, options: GetObjectOptions = {}): Promise<StrataObject> {
    this.validateKey(key);

    const response = await this.client.executeRequest<{
      data: Buffer;
      metadata: ObjectMetadata;
    }>('getObject', {
      bucket: this.name,
      key,
      ...options,
    });

    return new StrataObject(response.metadata, response.data);
  }

  /**
   * Get object as a readable stream
   */
  async getObjectStream(
    key: string,
    options: GetObjectOptions & StreamOptions = {}
  ): Promise<{ stream: ReadableStream<Uint8Array>; metadata: ObjectMetadata }> {
    this.validateKey(key);

    const response = await this.client.executeRequest<{
      metadata: ObjectMetadata;
    }>('getObjectStream', {
      bucket: this.name,
      key,
      ...options,
    });

    // Create a readable stream from the data
    // In real implementation, this would be a streaming response
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(0));
        controller.close();
      },
    });

    return { stream, metadata: response.metadata };
  }

  /**
   * Delete an object from the bucket
   */
  async deleteObject(key: string, options: DeleteObjectOptions = {}): Promise<void> {
    this.validateKey(key);

    await this.client.executeRequest('deleteObject', {
      bucket: this.name,
      key,
      ...options,
    });
  }

  /**
   * Delete multiple objects from the bucket
   */
  async deleteObjects(
    keys: Array<{ key: string; versionId?: string }>,
    options: DeleteObjectsOptions = {}
  ): Promise<DeleteObjectsResult> {
    if (keys.length === 0) {
      return { deleted: [], errors: [] };
    }

    if (keys.length > 1000) {
      throw new ValidationError('keys', 'Cannot delete more than 1000 objects at once');
    }

    return this.client.executeRequest<DeleteObjectsResult>('deleteObjects', {
      bucket: this.name,
      objects: keys,
      ...options,
    });
  }

  /**
   * Check if an object exists
   */
  async objectExists(key: string, versionId?: string): Promise<boolean> {
    this.validateKey(key);

    try {
      const response = await this.client.executeRequest<{ exists: boolean }>('headObject', {
        bucket: this.name,
        key,
        versionId,
      });
      return response.exists;
    } catch (error) {
      if (error instanceof NotFoundError) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Get object metadata without downloading the content
   */
  async headObject(key: string, versionId?: string): Promise<ObjectMetadata> {
    this.validateKey(key);

    return this.client.executeRequest<ObjectMetadata>('headObject', {
      bucket: this.name,
      key,
      versionId,
    });
  }

  /**
   * Copy an object within or between buckets
   */
  async copyObject(
    sourceKey: string,
    destKey: string,
    options: CopyObjectOptions & { destBucket?: string } = {}
  ): Promise<ObjectMetadata> {
    this.validateKey(sourceKey);
    this.validateKey(destKey);

    const destBucket = options.destBucket ?? this.name;

    return this.client.executeRequest<ObjectMetadata>('copyObject', {
      sourceBucket: this.name,
      sourceKey,
      destBucket,
      destKey,
      ...options,
    });
  }

  /**
   * List objects in the bucket
   */
  async listObjects(options: ListObjectsOptions = {}): Promise<ListObjectsResult> {
    return this.client.executeRequest<ListObjectsResult>('listObjects', {
      bucket: this.name,
      ...options,
    });
  }

  /**
   * Iterate over all objects in the bucket
   */
  async *listObjectsIterator(
    options: Omit<ListObjectsOptions, 'continuationToken'> = {}
  ): AsyncGenerator<ObjectMetadata, void, undefined> {
    let continuationToken: string | undefined;

    do {
      const result = await this.listObjects({
        ...options,
        continuationToken,
      });

      for (const object of result.objects) {
        yield object;
      }

      continuationToken = result.nextContinuationToken;
    } while (continuationToken);
  }

  /**
   * Generate a presigned URL for an object
   */
  async getPresignedUrl(key: string, options: PresignedUrlOptions = {}): Promise<string> {
    this.validateKey(key);

    const expiresIn = options.expiresIn ?? 3600; // Default 1 hour

    const response = await this.client.executeRequest<{ url: string }>('getPresignedUrl', {
      bucket: this.name,
      key,
      expiresIn,
      method: options.method ?? 'GET',
      contentType: options.contentType,
    });

    return response.url;
  }

  // Bucket configuration methods

  /**
   * Get bucket versioning configuration
   */
  async getVersioning(): Promise<BucketVersioning> {
    return this.client.executeRequest<BucketVersioning>('getBucketVersioning', {
      bucket: this.name,
    });
  }

  /**
   * Set bucket versioning configuration
   */
  async setVersioning(config: BucketVersioning): Promise<void> {
    await this.client.executeRequest('setBucketVersioning', {
      bucket: this.name,
      ...config,
    });
  }

  /**
   * Get bucket encryption configuration
   */
  async getEncryption(): Promise<BucketEncryption | null> {
    try {
      return await this.client.executeRequest<BucketEncryption>('getBucketEncryption', {
        bucket: this.name,
      });
    } catch (error) {
      if (error instanceof NotFoundError) {
        return null;
      }
      throw error;
    }
  }

  /**
   * Set bucket encryption configuration
   */
  async setEncryption(config: BucketEncryption): Promise<void> {
    await this.client.executeRequest('setBucketEncryption', {
      bucket: this.name,
      ...config,
    });
  }

  /**
   * Delete bucket encryption configuration
   */
  async deleteEncryption(): Promise<void> {
    await this.client.executeRequest('deleteBucketEncryption', {
      bucket: this.name,
    });
  }

  /**
   * Get bucket lifecycle rules
   */
  async getLifecycleRules(): Promise<LifecycleRule[]> {
    const response = await this.client.executeRequest<{ rules: LifecycleRule[] }>(
      'getBucketLifecycle',
      { bucket: this.name }
    );
    return response.rules;
  }

  /**
   * Set bucket lifecycle rules
   */
  async setLifecycleRules(rules: LifecycleRule[]): Promise<void> {
    await this.client.executeRequest('setBucketLifecycle', {
      bucket: this.name,
      rules,
    });
  }

  /**
   * Delete bucket lifecycle configuration
   */
  async deleteLifecycleRules(): Promise<void> {
    await this.client.executeRequest('deleteBucketLifecycle', {
      bucket: this.name,
    });
  }

  /**
   * Get bucket CORS configuration
   */
  async getCORS(): Promise<CORSRule[]> {
    const response = await this.client.executeRequest<{ rules: CORSRule[] }>('getBucketCORS', {
      bucket: this.name,
    });
    return response.rules;
  }

  /**
   * Set bucket CORS configuration
   */
  async setCORS(rules: CORSRule[]): Promise<void> {
    await this.client.executeRequest('setBucketCORS', {
      bucket: this.name,
      rules,
    });
  }

  /**
   * Delete bucket CORS configuration
   */
  async deleteCORS(): Promise<void> {
    await this.client.executeRequest('deleteBucketCORS', {
      bucket: this.name,
    });
  }

  /**
   * Get bucket policy
   */
  async getPolicy(): Promise<BucketPolicy | null> {
    try {
      return await this.client.executeRequest<BucketPolicy>('getBucketPolicy', {
        bucket: this.name,
      });
    } catch (error) {
      if (error instanceof NotFoundError) {
        return null;
      }
      throw error;
    }
  }

  /**
   * Set bucket policy
   */
  async setPolicy(policy: BucketPolicy): Promise<void> {
    await this.client.executeRequest('setBucketPolicy', {
      bucket: this.name,
      policy,
    });
  }

  /**
   * Delete bucket policy
   */
  async deletePolicy(): Promise<void> {
    await this.client.executeRequest('deleteBucketPolicy', {
      bucket: this.name,
    });
  }

  /**
   * Get bucket metrics
   */
  async getMetrics(): Promise<BucketMetrics> {
    return this.client.executeRequest<BucketMetrics>('getBucketMetrics', {
      bucket: this.name,
    });
  }

  // Multipart upload methods

  /**
   * Initiate a multipart upload
   */
  async createMultipartUpload(
    key: string,
    options: PutObjectOptions = {}
  ): Promise<{ uploadId: string; key: string }> {
    this.validateKey(key);

    return this.client.executeRequest('createMultipartUpload', {
      bucket: this.name,
      key,
      ...options,
    });
  }

  /**
   * Upload a part in a multipart upload
   */
  async uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    data: Buffer | Uint8Array
  ): Promise<{ etag: string; partNumber: number }> {
    this.validateKey(key);

    if (partNumber < 1 || partNumber > 10000) {
      throw new ValidationError('partNumber', 'Part number must be between 1 and 10000');
    }

    return this.client.executeRequest('uploadPart', {
      bucket: this.name,
      key,
      uploadId,
      partNumber,
      body: data,
    });
  }

  /**
   * Complete a multipart upload
   */
  async completeMultipartUpload(
    key: string,
    uploadId: string,
    parts: Array<{ partNumber: number; etag: string }>
  ): Promise<ObjectMetadata> {
    this.validateKey(key);

    return this.client.executeRequest<ObjectMetadata>('completeMultipartUpload', {
      bucket: this.name,
      key,
      uploadId,
      parts,
    });
  }

  /**
   * Abort a multipart upload
   */
  async abortMultipartUpload(key: string, uploadId: string): Promise<void> {
    this.validateKey(key);

    await this.client.executeRequest('abortMultipartUpload', {
      bucket: this.name,
      key,
      uploadId,
    });
  }

  /**
   * List ongoing multipart uploads
   */
  async listMultipartUploads(options: { prefix?: string; maxUploads?: number } = {}): Promise<{
    uploads: Array<{
      key: string;
      uploadId: string;
      initiated: Date;
    }>;
  }> {
    return this.client.executeRequest('listMultipartUploads', {
      bucket: this.name,
      ...options,
    });
  }

  // Helper methods

  private validateKey(key: string): void {
    if (!key || key.length === 0) {
      throw new ValidationError('key', 'Object key cannot be empty');
    }

    if (key.length > 1024) {
      throw new ValidationError('key', 'Object key cannot exceed 1024 characters');
    }
  }

  private normalizeData(data: Buffer | Uint8Array | string | ReadableStream): Buffer {
    if (typeof data === 'string') {
      return Buffer.from(data, 'utf-8');
    }
    if (data instanceof Uint8Array) {
      return Buffer.from(data);
    }
    if (Buffer.isBuffer(data)) {
      return data;
    }
    // For ReadableStream, in real implementation we would handle streaming
    throw new ValidationError('data', 'ReadableStream not supported in this context');
  }

  private detectContentType(key: string): string {
    const ext = key.split('.').pop()?.toLowerCase();

    const contentTypes: Record<string, string> = {
      txt: 'text/plain',
      html: 'text/html',
      htm: 'text/html',
      css: 'text/css',
      js: 'application/javascript',
      json: 'application/json',
      xml: 'application/xml',
      pdf: 'application/pdf',
      zip: 'application/zip',
      tar: 'application/x-tar',
      gz: 'application/gzip',
      png: 'image/png',
      jpg: 'image/jpeg',
      jpeg: 'image/jpeg',
      gif: 'image/gif',
      svg: 'image/svg+xml',
      webp: 'image/webp',
      mp3: 'audio/mpeg',
      mp4: 'video/mp4',
      webm: 'video/webm',
    };

    return contentTypes[ext ?? ''] ?? 'application/octet-stream';
  }
}
