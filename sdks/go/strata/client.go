// Package strata provides a Go client for the Strata distributed filesystem.
package strata

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Client is the main client for interacting with Strata.
type Client struct {
	config    *Config
	conn      interface{} // placeholder for gRPC connection
	mu        sync.RWMutex
	connected bool
}

// NewClient creates a new Strata client with the given configuration.
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Client{
		config:    config,
		connected: false,
	}, nil
}

// NewClientFromEnv creates a new client using environment variables.
func NewClientFromEnv() (*Client, error) {
	config := ConfigFromEnv()
	return NewClient(config)
}

// Connect establishes a connection to the Strata cluster.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	// Placeholder for actual gRPC connection
	c.connected = true
	return nil
}

// Close closes the connection to Strata.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}

	c.connected = false
	c.conn = nil
	return nil
}

// IsConnected returns whether the client is connected.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// =============================================================================
// Bucket Operations
// =============================================================================

// CreateBucket creates a new bucket.
func (c *Client) CreateBucket(ctx context.Context, name string, opts ...BucketOption) (*Bucket, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	options := &bucketOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return &Bucket{
		client:            c,
		Name:              name,
		Region:            options.region,
		VersioningEnabled: options.versioning,
		EncryptionEnabled: options.encryption,
		CreatedAt:         time.Now(),
	}, nil
}

// GetBucket retrieves a bucket by name.
func (c *Client) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return &Bucket{
		client:            c,
		Name:              name,
		EncryptionEnabled: true,
		CreatedAt:         time.Now(),
	}, nil
}

// ListBuckets lists all buckets.
func (c *Client) ListBuckets(ctx context.Context) ([]*Bucket, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return []*Bucket{}, nil
}

// DeleteBucket deletes a bucket.
func (c *Client) DeleteBucket(ctx context.Context, name string, opts ...DeleteBucketOption) error {
	if err := c.ensureConnected(); err != nil {
		return err
	}

	return nil
}

// =============================================================================
// Object Operations
// =============================================================================

// PutObject uploads an object to a bucket.
func (c *Client) PutObject(ctx context.Context, bucket, key string, reader io.Reader, opts ...PutObjectOption) (*Object, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	options := &putObjectOptions{
		contentType: "application/octet-stream",
	}
	for _, opt := range opts {
		opt(options)
	}

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	// Calculate MD5
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	return &Object{
		client:      c,
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(data)),
		ETag:        etag,
		ContentType: options.contentType,
		Metadata: ObjectMetadata{
			Custom:       options.metadata,
			LastModified: time.Now(),
		},
	}, nil
}

// PutObjectFromFile uploads a file as an object.
func (c *Client) PutObjectFromFile(ctx context.Context, bucket, key, filepath string, opts ...PutObjectOption) (*Object, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return c.PutObject(ctx, bucket, key, file, opts...)
}

// GetObject downloads an object.
func (c *Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return nil, &NotFoundError{ResourceType: "Object", ResourceID: fmt.Sprintf("%s/%s", bucket, key)}
}

// GetObjectReader returns a reader for streaming object data.
func (c *Client) GetObjectReader(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return nil, &NotFoundError{ResourceType: "Object", ResourceID: fmt.Sprintf("%s/%s", bucket, key)}
}

// HeadObject gets object metadata without downloading the object.
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (*ObjectMetadata, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return nil, &NotFoundError{ResourceType: "Object", ResourceID: fmt.Sprintf("%s/%s", bucket, key)}
}

// DeleteObject deletes an object.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := c.ensureConnected(); err != nil {
		return err
	}

	return nil
}

// ListObjects lists objects in a bucket.
func (c *Client) ListObjects(ctx context.Context, bucket string, opts ...ListObjectsOption) (*ListObjectsResult, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return &ListObjectsResult{
		Objects:     []*Object{},
		Prefixes:    []string{},
		IsTruncated: false,
	}, nil
}

// CopyObject copies an object to a new location.
func (c *Client) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*Object, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return &Object{
		client:      c,
		Bucket:      dstBucket,
		Key:         dstKey,
		Size:        0,
		ETag:        "",
		ContentType: "application/octet-stream",
		Metadata:    ObjectMetadata{LastModified: time.Now()},
	}, nil
}

// =============================================================================
// Multipart Upload
// =============================================================================

// CreateMultipartUpload initiates a multipart upload.
func (c *Client) CreateMultipartUpload(ctx context.Context, bucket, key string, opts ...PutObjectOption) (*MultipartUpload, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	return &MultipartUpload{
		client:   c,
		Bucket:   bucket,
		Key:      key,
		UploadID: fmt.Sprintf("upload-%d", time.Now().UnixNano()),
		Parts:    make([]PartInfo, 0),
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

func (c *Client) ensureConnected() error {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()

	if !connected {
		return &ConnectionError{Message: "client is not connected"}
	}
	return nil
}
