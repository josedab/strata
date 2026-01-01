"""Exceptions for the Strata SDK."""

from typing import Optional, Dict, Any


class StrataError(Exception):
    """Base exception for Strata SDK."""

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class NotFoundError(StrataError):
    """Resource not found error."""

    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(
            f"{resource_type} not found: {resource_id}",
            code="NOT_FOUND",
            details={"resource_type": resource_type, "resource_id": resource_id},
        )


class PermissionDeniedError(StrataError):
    """Permission denied error."""

    def __init__(self, resource: str, operation: str):
        super().__init__(
            f"Permission denied: {operation} on {resource}",
            code="PERMISSION_DENIED",
            details={"resource": resource, "operation": operation},
        )


class AlreadyExistsError(StrataError):
    """Resource already exists error."""

    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(
            f"{resource_type} already exists: {resource_id}",
            code="ALREADY_EXISTS",
            details={"resource_type": resource_type, "resource_id": resource_id},
        )


class TimeoutError(StrataError):
    """Operation timeout error."""

    def __init__(self, operation: str, timeout_seconds: float):
        super().__init__(
            f"Operation timed out: {operation} after {timeout_seconds}s",
            code="TIMEOUT",
            details={"operation": operation, "timeout_seconds": timeout_seconds},
        )


class ConnectionError(StrataError):
    """Connection error."""

    def __init__(self, endpoint: str, reason: str):
        super().__init__(
            f"Connection failed to {endpoint}: {reason}",
            code="CONNECTION_ERROR",
            details={"endpoint": endpoint, "reason": reason},
        )


class ValidationError(StrataError):
    """Validation error."""

    def __init__(self, field: str, reason: str):
        super().__init__(
            f"Validation error for {field}: {reason}",
            code="VALIDATION_ERROR",
            details={"field": field, "reason": reason},
        )


class QuotaExceededError(StrataError):
    """Quota exceeded error."""

    def __init__(self, quota_type: str, limit: int, current: int):
        super().__init__(
            f"Quota exceeded for {quota_type}: {current}/{limit}",
            code="QUOTA_EXCEEDED",
            details={"quota_type": quota_type, "limit": limit, "current": current},
        )


class InternalError(StrataError):
    """Internal server error."""

    def __init__(self, message: str = "Internal server error"):
        super().__init__(message, code="INTERNAL_ERROR")


class RateLimitError(StrataError):
    """Rate limit exceeded error."""

    def __init__(self, retry_after_seconds: Optional[float] = None):
        message = "Rate limit exceeded"
        if retry_after_seconds:
            message += f", retry after {retry_after_seconds}s"
        super().__init__(
            message,
            code="RATE_LIMITED",
            details={"retry_after_seconds": retry_after_seconds},
        )
        self.retry_after_seconds = retry_after_seconds
