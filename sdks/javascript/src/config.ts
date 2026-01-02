/**
 * Client configuration
 */

export interface TLSConfig {
  enabled: boolean;
  caCertPath?: string;
  clientCertPath?: string;
  clientKeyPath?: string;
  skipVerify?: boolean;
}

export interface RetryConfig {
  maxAttempts: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
  backoffMultiplier: number;
}

export interface ClientConfig {
  // Connection settings
  endpoint: string;
  s3Endpoint?: string;

  // Authentication
  accessKey?: string;
  secretKey?: string;
  token?: string;

  // Timeouts (in milliseconds)
  connectTimeout?: number;
  requestTimeout?: number;
  readTimeout?: number;
  writeTimeout?: number;

  // Connection pool
  maxConnections?: number;
  maxIdleConnections?: number;
  idleTimeout?: number;

  // TLS
  tls?: TLSConfig;

  // Retry
  retry?: RetryConfig;

  // Performance
  chunkSize?: number;
  multipartThreshold?: number;
  maxConcurrentUploads?: number;
  maxConcurrentDownloads?: number;

  // Caching
  enableCache?: boolean;
  cacheSizeMb?: number;
  cacheTtlSeconds?: number;

  // Compression
  enableCompression?: boolean;
  compressionLevel?: number;

  // Debug
  debug?: boolean;
}

export const defaultConfig: Required<ClientConfig> = {
  endpoint: 'localhost:9000',
  s3Endpoint: '',
  accessKey: '',
  secretKey: '',
  token: '',
  connectTimeout: 10000,
  requestTimeout: 60000,
  readTimeout: 300000,
  writeTimeout: 300000,
  maxConnections: 100,
  maxIdleConnections: 10,
  idleTimeout: 300000,
  tls: {
    enabled: false,
  },
  retry: {
    maxAttempts: 3,
    initialBackoffMs: 100,
    maxBackoffMs: 10000,
    backoffMultiplier: 2.0,
  },
  chunkSize: 8 * 1024 * 1024,
  multipartThreshold: 64 * 1024 * 1024,
  maxConcurrentUploads: 4,
  maxConcurrentDownloads: 4,
  enableCache: true,
  cacheSizeMb: 256,
  cacheTtlSeconds: 300,
  enableCompression: true,
  compressionLevel: 6,
  debug: false,
};

export function configFromEnv(): ClientConfig {
  const config: ClientConfig = {
    endpoint: process.env['STRATA_ENDPOINT'] ?? defaultConfig.endpoint,
  };

  if (process.env['STRATA_S3_ENDPOINT']) {
    config.s3Endpoint = process.env['STRATA_S3_ENDPOINT'];
  }
  if (process.env['STRATA_ACCESS_KEY']) {
    config.accessKey = process.env['STRATA_ACCESS_KEY'];
  }
  if (process.env['STRATA_SECRET_KEY']) {
    config.secretKey = process.env['STRATA_SECRET_KEY'];
  }
  if (process.env['STRATA_TOKEN']) {
    config.token = process.env['STRATA_TOKEN'];
  }
  if (process.env['STRATA_TLS_ENABLED'] === 'true') {
    config.tls = { enabled: true };
  }
  if (process.env['STRATA_DEBUG'] === 'true') {
    config.debug = true;
  }

  return config;
}

export function mergeConfig(config: ClientConfig): Required<ClientConfig> {
  return {
    ...defaultConfig,
    ...config,
    tls: { ...defaultConfig.tls, ...config.tls },
    retry: { ...defaultConfig.retry, ...config.retry },
  };
}
