package strata

import "time"

// Bucket represents a storage bucket.
type Bucket struct {
	client            *Client
	Name              string
	Region            string
	VersioningEnabled bool
	EncryptionEnabled bool
	CreatedAt         time.Time
}

// Object represents a storage object.
type Object struct {
	client      *Client
	Bucket      string
	Key         string
	Size        int64
	ETag        string
	ContentType string
	Metadata    ObjectMetadata
}

// ObjectMetadata holds object metadata.
type ObjectMetadata struct {
	Custom             map[string]string
	LastModified       time.Time
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	Expires            time.Time
	StorageClass       string
	VersionID          string
	DeleteMarker       bool
}

// ListObjectsResult holds the result of listing objects.
type ListObjectsResult struct {
	Objects               []*Object
	Prefixes              []string
	IsTruncated           bool
	NextContinuationToken string
}

// MultipartUpload represents an in-progress multipart upload.
type MultipartUpload struct {
	client   *Client
	Bucket   string
	Key      string
	UploadID string
	Parts    []PartInfo
}

// PartInfo holds information about an uploaded part.
type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
}

// BucketPolicy represents a bucket access policy.
type BucketPolicy struct {
	Version    string
	Statements []PolicyStatement
}

// PolicyStatement represents a policy statement.
type PolicyStatement struct {
	Sid       string
	Effect    string
	Principal interface{}
	Action    []string
	Resource  []string
	Condition map[string]interface{}
}

// LifecycleRule represents a lifecycle rule.
type LifecycleRule struct {
	ID                            string
	Prefix                        string
	Enabled                       bool
	ExpirationDays                int
	TransitionDays                int
	TransitionStorageClass        string
	NoncurrentVersionExpirationDays int
}

// CORSRule represents a CORS rule.
type CORSRule struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	ExposeHeaders  []string
	MaxAgeSeconds  int
}

// =============================================================================
// Options
// =============================================================================

type bucketOptions struct {
	region     string
	versioning bool
	encryption bool
}

// BucketOption is a function that configures bucket creation.
type BucketOption func(*bucketOptions)

// WithRegion sets the bucket region.
func WithRegion(region string) BucketOption {
	return func(o *bucketOptions) {
		o.region = region
	}
}

// WithVersioning enables versioning.
func WithVersioning(enabled bool) BucketOption {
	return func(o *bucketOptions) {
		o.versioning = enabled
	}
}

// WithEncryption enables encryption.
func WithEncryption(enabled bool) BucketOption {
	return func(o *bucketOptions) {
		o.encryption = enabled
	}
}

type deleteBucketOptions struct {
	force bool
}

// DeleteBucketOption is a function that configures bucket deletion.
type DeleteBucketOption func(*deleteBucketOptions)

// ForceDelete forces deletion even if bucket is not empty.
func ForceDelete(force bool) DeleteBucketOption {
	return func(o *deleteBucketOptions) {
		o.force = force
	}
}

type putObjectOptions struct {
	contentType string
	metadata    map[string]string
}

// PutObjectOption is a function that configures object upload.
type PutObjectOption func(*putObjectOptions)

// WithContentType sets the content type.
func WithContentType(contentType string) PutObjectOption {
	return func(o *putObjectOptions) {
		o.contentType = contentType
	}
}

// WithMetadata sets custom metadata.
func WithMetadata(metadata map[string]string) PutObjectOption {
	return func(o *putObjectOptions) {
		o.metadata = metadata
	}
}

type listObjectsOptions struct {
	prefix            string
	delimiter         string
	maxKeys           int
	continuationToken string
}

// ListObjectsOption is a function that configures object listing.
type ListObjectsOption func(*listObjectsOptions)

// WithPrefix filters by prefix.
func WithPrefix(prefix string) ListObjectsOption {
	return func(o *listObjectsOptions) {
		o.prefix = prefix
	}
}

// WithDelimiter groups by delimiter.
func WithDelimiter(delimiter string) ListObjectsOption {
	return func(o *listObjectsOptions) {
		o.delimiter = delimiter
	}
}

// WithMaxKeys limits the number of keys returned.
func WithMaxKeys(maxKeys int) ListObjectsOption {
	return func(o *listObjectsOptions) {
		o.maxKeys = maxKeys
	}
}

// WithContinuationToken sets the pagination token.
func WithContinuationToken(token string) ListObjectsOption {
	return func(o *listObjectsOptions) {
		o.continuationToken = token
	}
}
