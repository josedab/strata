/**
 * Strata Client - Main entry point for the SDK
 */

import { Bucket } from './bucket';
import { ClientConfig, defaultConfig, mergeConfig, configFromEnv } from './config';
import {
  StrataError,
  NotFoundError,
  ConnectionError,
  PermissionDeniedError,
  RateLimitError,
} from './errors';
import type { BucketOptions, BucketMetrics } from './types';

export interface StrataClientOptions {
  config?: ClientConfig;
  useEnvConfig?: boolean;
}

interface GrpcChannel {
  endpoint: string;
  connected: boolean;
  lastActivity: number;
}

interface RequestOptions {
  timeout?: number;
  signal?: AbortSignal;
}

export class StrataClient {
  private config: Required<ClientConfig>;
  private channel: GrpcChannel | null = null;
  private bucketCache: Map<string, Bucket> = new Map();
  private connectionPromise: Promise<void> | null = null;

  constructor(options: StrataClientOptions = {}) {
    const baseConfig = options.useEnvConfig ? configFromEnv() : {};
    const providedConfig = options.config ?? {};
    this.config = mergeConfig({ ...baseConfig, ...providedConfig });
  }

  /**
   * Create a client from environment variables
   */
  static fromEnv(): StrataClient {
    return new StrataClient({ useEnvConfig: true });
  }

  /**
   * Create a client with explicit configuration
   */
  static withConfig(config: ClientConfig): StrataClient {
    return new StrataClient({ config });
  }

  /**
   * Connect to the Strata server
   */
  async connect(): Promise<void> {
    if (this.channel?.connected) {
      return;
    }

    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = this.establishConnection();
    try {
      await this.connectionPromise;
    } finally {
      this.connectionPromise = null;
    }
  }

  private async establishConnection(): Promise<void> {
    const endpoint = this.config.endpoint;

    try {
      // In a real implementation, this would establish a gRPC connection
      // For now, we simulate the connection
      await this.simulateConnection(endpoint);

      this.channel = {
        endpoint,
        connected: true,
        lastActivity: Date.now(),
      };

      if (this.config.debug) {
        console.log(`Connected to Strata server at ${endpoint}`);
      }
    } catch (error) {
      throw new ConnectionError(
        endpoint,
        error instanceof Error ? error.message : 'Unknown error'
      );
    }
  }

  private async simulateConnection(endpoint: string): Promise<void> {
    // Simulate network latency
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Validate endpoint format
    if (!endpoint || endpoint.length === 0) {
      throw new Error('Invalid endpoint');
    }
  }

  /**
   * Disconnect from the server
   */
  async disconnect(): Promise<void> {
    if (this.channel) {
      this.channel.connected = false;
      this.channel = null;
      this.bucketCache.clear();

      if (this.config.debug) {
        console.log('Disconnected from Strata server');
      }
    }
  }

  /**
   * Check if connected to the server
   */
  isConnected(): boolean {
    return this.channel?.connected ?? false;
  }

  /**
   * Ensure connection is established
   */
  private async ensureConnected(): Promise<void> {
    if (!this.isConnected()) {
      await this.connect();
    }
  }

  /**
   * Create a new bucket
   */
  async createBucket(name: string, options: BucketOptions = {}): Promise<Bucket> {
    await this.ensureConnected();

    this.validateBucketName(name);

    // Simulate bucket creation
    await this.executeRequest('createBucket', { name, options });

    const bucket = new Bucket(this, name, options);
    this.bucketCache.set(name, bucket);

    return bucket;
  }

  /**
   * Get an existing bucket
   */
  async getBucket(name: string): Promise<Bucket> {
    await this.ensureConnected();

    // Check cache first
    const cached = this.bucketCache.get(name);
    if (cached) {
      return cached;
    }

    // Fetch from server
    const bucketInfo = await this.executeRequest<{ exists: boolean; options: BucketOptions }>(
      'getBucket',
      { name }
    );

    if (!bucketInfo.exists) {
      throw new NotFoundError('Bucket', name);
    }

    const bucket = new Bucket(this, name, bucketInfo.options);
    this.bucketCache.set(name, bucket);

    return bucket;
  }

  /**
   * List all buckets
   */
  async listBuckets(): Promise<Bucket[]> {
    await this.ensureConnected();

    const response = await this.executeRequest<{ buckets: Array<{ name: string; options: BucketOptions }> }>(
      'listBuckets',
      {}
    );

    return response.buckets.map((info) => {
      const cached = this.bucketCache.get(info.name);
      if (cached) {
        return cached;
      }
      const bucket = new Bucket(this, info.name, info.options);
      this.bucketCache.set(info.name, bucket);
      return bucket;
    });
  }

  /**
   * Delete a bucket
   */
  async deleteBucket(name: string, force = false): Promise<void> {
    await this.ensureConnected();

    await this.executeRequest('deleteBucket', { name, force });
    this.bucketCache.delete(name);
  }

  /**
   * Check if a bucket exists
   */
  async bucketExists(name: string): Promise<boolean> {
    await this.ensureConnected();

    try {
      const response = await this.executeRequest<{ exists: boolean }>('bucketExists', { name });
      return response.exists;
    } catch (error) {
      if (error instanceof NotFoundError) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Get bucket metrics
   */
  async getBucketMetrics(name: string): Promise<BucketMetrics> {
    await this.ensureConnected();

    return this.executeRequest<BucketMetrics>('getBucketMetrics', { name });
  }

  /**
   * Get cluster health status
   */
  async getHealth(): Promise<{
    status: 'healthy' | 'degraded' | 'unhealthy';
    nodes: number;
    uptime: number;
  }> {
    await this.ensureConnected();

    return this.executeRequest('getHealth', {});
  }

  /**
   * Get cluster statistics
   */
  async getStats(): Promise<{
    totalBuckets: number;
    totalObjects: number;
    totalSizeBytes: number;
    requestsPerSecond: number;
  }> {
    await this.ensureConnected();

    return this.executeRequest('getStats', {});
  }

  /**
   * Execute a request with retry logic
   */
  async executeRequest<T = unknown>(
    method: string,
    params: Record<string, unknown>,
    options: RequestOptions = {}
  ): Promise<T> {
    const maxAttempts = this.config.retry.maxAttempts;
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await this.doRequest<T>(method, params, options);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        // Don't retry on certain errors
        if (
          error instanceof NotFoundError ||
          error instanceof PermissionDeniedError
        ) {
          throw error;
        }

        // Check if we should retry
        if (attempt < maxAttempts) {
          const backoff = this.calculateBackoff(attempt);
          if (this.config.debug) {
            console.log(
              `Request failed (attempt ${attempt}/${maxAttempts}), retrying in ${backoff}ms...`
            );
          }
          await this.sleep(backoff);
        }
      }
    }

    throw lastError ?? new StrataError('Request failed after all retries');
  }

  private async doRequest<T>(
    method: string,
    params: Record<string, unknown>,
    options: RequestOptions
  ): Promise<T> {
    // In a real implementation, this would make a gRPC call
    // For now, we simulate responses

    if (this.channel) {
      this.channel.lastActivity = Date.now();
    }

    // Simulate network latency
    await this.sleep(5);

    // Simulate responses based on method
    return this.simulateResponse<T>(method, params);
  }

  private simulateResponse<T>(method: string, params: Record<string, unknown>): T {
    // This is a placeholder - real implementation would use gRPC
    switch (method) {
      case 'createBucket':
        return { success: true } as T;
      case 'getBucket':
        return { exists: true, options: {} } as T;
      case 'listBuckets':
        return { buckets: [] } as T;
      case 'deleteBucket':
        return { success: true } as T;
      case 'bucketExists':
        return { exists: true } as T;
      case 'getBucketMetrics':
        return {
          objectCount: 0,
          totalSizeBytes: 0,
          requestsLastHour: 0,
          bandwidthLastHourBytes: 0,
        } as T;
      case 'getHealth':
        return { status: 'healthy', nodes: 3, uptime: 86400 } as T;
      case 'getStats':
        return {
          totalBuckets: 0,
          totalObjects: 0,
          totalSizeBytes: 0,
          requestsPerSecond: 0,
        } as T;
      default:
        return {} as T;
    }
  }

  private calculateBackoff(attempt: number): number {
    const { initialBackoffMs, maxBackoffMs, backoffMultiplier } = this.config.retry;
    const backoff = initialBackoffMs * Math.pow(backoffMultiplier, attempt - 1);
    // Add jitter
    const jitter = Math.random() * 0.3 * backoff;
    return Math.min(backoff + jitter, maxBackoffMs);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private validateBucketName(name: string): void {
    if (!name || name.length < 3 || name.length > 63) {
      throw new StrataError(
        'Bucket name must be between 3 and 63 characters',
        'INVALID_BUCKET_NAME'
      );
    }

    if (!/^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(name)) {
      throw new StrataError(
        'Bucket name must start and end with a letter or number',
        'INVALID_BUCKET_NAME'
      );
    }

    if (/\.\./.test(name)) {
      throw new StrataError(
        'Bucket name cannot contain consecutive periods',
        'INVALID_BUCKET_NAME'
      );
    }
  }

  /**
   * Get the current configuration
   */
  getConfig(): Readonly<Required<ClientConfig>> {
    return this.config;
  }
}
