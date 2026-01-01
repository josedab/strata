package strata

import "fmt"

// StrataError is the base error type for the SDK.
type StrataError struct {
	Code    string
	Message string
	Details map[string]interface{}
}

func (e *StrataError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("[%s] %s", e.Code, e.Message)
	}
	return e.Message
}

// NotFoundError indicates a resource was not found.
type NotFoundError struct {
	ResourceType string
	ResourceID   string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s not found: %s", e.ResourceType, e.ResourceID)
}

// PermissionDeniedError indicates permission was denied.
type PermissionDeniedError struct {
	Resource  string
	Operation string
}

func (e *PermissionDeniedError) Error() string {
	return fmt.Sprintf("permission denied: %s on %s", e.Operation, e.Resource)
}

// AlreadyExistsError indicates a resource already exists.
type AlreadyExistsError struct {
	ResourceType string
	ResourceID   string
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("%s already exists: %s", e.ResourceType, e.ResourceID)
}

// TimeoutError indicates an operation timed out.
type TimeoutError struct {
	Operation      string
	TimeoutSeconds float64
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("operation timed out: %s after %.1fs", e.Operation, e.TimeoutSeconds)
}

// ConnectionError indicates a connection failure.
type ConnectionError struct {
	Endpoint string
	Message  string
}

func (e *ConnectionError) Error() string {
	if e.Endpoint != "" {
		return fmt.Sprintf("connection failed to %s: %s", e.Endpoint, e.Message)
	}
	return fmt.Sprintf("connection error: %s", e.Message)
}

// ValidationError indicates a validation failure.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for %s: %s", e.Field, e.Reason)
}

// QuotaExceededError indicates quota was exceeded.
type QuotaExceededError struct {
	QuotaType string
	Limit     int64
	Current   int64
}

func (e *QuotaExceededError) Error() string {
	return fmt.Sprintf("quota exceeded for %s: %d/%d", e.QuotaType, e.Current, e.Limit)
}

// InternalError indicates an internal server error.
type InternalError struct {
	Message string
}

func (e *InternalError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("internal error: %s", e.Message)
	}
	return "internal server error"
}

// RateLimitError indicates rate limiting.
type RateLimitError struct {
	RetryAfterSeconds float64
}

func (e *RateLimitError) Error() string {
	if e.RetryAfterSeconds > 0 {
		return fmt.Sprintf("rate limit exceeded, retry after %.1fs", e.RetryAfterSeconds)
	}
	return "rate limit exceeded"
}

// IsNotFound returns true if the error is a NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}

// IsPermissionDenied returns true if the error is a PermissionDeniedError.
func IsPermissionDenied(err error) bool {
	_, ok := err.(*PermissionDeniedError)
	return ok
}

// IsAlreadyExists returns true if the error is an AlreadyExistsError.
func IsAlreadyExists(err error) bool {
	_, ok := err.(*AlreadyExistsError)
	return ok
}

// IsTimeout returns true if the error is a TimeoutError.
func IsTimeout(err error) bool {
	_, ok := err.(*TimeoutError)
	return ok
}

// IsRateLimited returns true if the error is a RateLimitError.
func IsRateLimited(err error) bool {
	_, ok := err.(*RateLimitError)
	return ok
}
