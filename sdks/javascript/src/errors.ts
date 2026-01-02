/**
 * Error types for the Strata SDK
 */

export class StrataError extends Error {
  code?: string;
  details?: Record<string, unknown>;

  constructor(message: string, code?: string, details?: Record<string, unknown>) {
    super(message);
    this.name = 'StrataError';
    this.code = code;
    this.details = details;
  }
}

export class NotFoundError extends StrataError {
  resourceType: string;
  resourceId: string;

  constructor(resourceType: string, resourceId: string) {
    super(`${resourceType} not found: ${resourceId}`, 'NOT_FOUND', {
      resourceType,
      resourceId,
    });
    this.name = 'NotFoundError';
    this.resourceType = resourceType;
    this.resourceId = resourceId;
  }
}

export class PermissionDeniedError extends StrataError {
  resource: string;
  operation: string;

  constructor(resource: string, operation: string) {
    super(`Permission denied: ${operation} on ${resource}`, 'PERMISSION_DENIED', {
      resource,
      operation,
    });
    this.name = 'PermissionDeniedError';
    this.resource = resource;
    this.operation = operation;
  }
}

export class AlreadyExistsError extends StrataError {
  resourceType: string;
  resourceId: string;

  constructor(resourceType: string, resourceId: string) {
    super(`${resourceType} already exists: ${resourceId}`, 'ALREADY_EXISTS', {
      resourceType,
      resourceId,
    });
    this.name = 'AlreadyExistsError';
    this.resourceType = resourceType;
    this.resourceId = resourceId;
  }
}

export class TimeoutError extends StrataError {
  operation: string;
  timeoutSeconds: number;

  constructor(operation: string, timeoutSeconds: number) {
    super(`Operation timed out: ${operation} after ${timeoutSeconds}s`, 'TIMEOUT', {
      operation,
      timeoutSeconds,
    });
    this.name = 'TimeoutError';
    this.operation = operation;
    this.timeoutSeconds = timeoutSeconds;
  }
}

export class ConnectionError extends StrataError {
  endpoint: string;
  reason: string;

  constructor(endpoint: string, reason: string) {
    super(`Connection failed to ${endpoint}: ${reason}`, 'CONNECTION_ERROR', {
      endpoint,
      reason,
    });
    this.name = 'ConnectionError';
    this.endpoint = endpoint;
    this.reason = reason;
  }
}

export class ValidationError extends StrataError {
  field: string;
  reason: string;

  constructor(field: string, reason: string) {
    super(`Validation error for ${field}: ${reason}`, 'VALIDATION_ERROR', {
      field,
      reason,
    });
    this.name = 'ValidationError';
    this.field = field;
    this.reason = reason;
  }
}

export class QuotaExceededError extends StrataError {
  quotaType: string;
  limit: number;
  current: number;

  constructor(quotaType: string, limit: number, current: number) {
    super(`Quota exceeded for ${quotaType}: ${current}/${limit}`, 'QUOTA_EXCEEDED', {
      quotaType,
      limit,
      current,
    });
    this.name = 'QuotaExceededError';
    this.quotaType = quotaType;
    this.limit = limit;
    this.current = current;
  }
}

export class RateLimitError extends StrataError {
  retryAfterSeconds?: number;

  constructor(retryAfterSeconds?: number) {
    const message = retryAfterSeconds
      ? `Rate limit exceeded, retry after ${retryAfterSeconds}s`
      : 'Rate limit exceeded';
    super(message, 'RATE_LIMITED', { retryAfterSeconds });
    this.name = 'RateLimitError';
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

// Type guards
export function isNotFound(error: unknown): error is NotFoundError {
  return error instanceof NotFoundError;
}

export function isPermissionDenied(error: unknown): error is PermissionDeniedError {
  return error instanceof PermissionDeniedError;
}

export function isAlreadyExists(error: unknown): error is AlreadyExistsError {
  return error instanceof AlreadyExistsError;
}

export function isTimeout(error: unknown): error is TimeoutError {
  return error instanceof TimeoutError;
}

export function isRateLimited(error: unknown): error is RateLimitError {
  return error instanceof RateLimitError;
}
