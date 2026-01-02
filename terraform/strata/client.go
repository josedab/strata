// HTTP client for Strata API

package strata

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config holds the provider configuration
type Config struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Token         string
	Region        string
	TLSEnabled    bool
	TLSSkipVerify bool
}

// Client is the Strata API client
type Client struct {
	config     *Config
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new Strata API client
func NewClient(config *Config) (*Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.TLSSkipVerify,
		},
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	return &Client{
		config:     config,
		httpClient: httpClient,
		baseURL:    config.Endpoint + "/api/v1",
	}, nil
}

// Request makes an HTTP request to the API
func (c *Client) Request(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "terraform-provider-strata/0.1.0")

	if c.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.Token)
	} else if c.config.AccessKey != "" && c.config.SecretKey != "" {
		// Add AWS-style signature or basic auth
		req.SetBasicAuth(c.config.AccessKey, c.config.SecretKey)
	}

	return c.httpClient.Do(req)
}

// Get performs a GET request
func (c *Client) Get(ctx context.Context, path string, result interface{}) error {
	resp, err := c.Request(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return c.handleError(resp)
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

// Post performs a POST request
func (c *Client) Post(ctx context.Context, path string, body, result interface{}) error {
	resp, err := c.Request(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return c.handleError(resp)
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

// Put performs a PUT request
func (c *Client) Put(ctx context.Context, path string, body, result interface{}) error {
	resp, err := c.Request(ctx, http.MethodPut, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return c.handleError(resp)
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

// Delete performs a DELETE request
func (c *Client) Delete(ctx context.Context, path string) error {
	resp, err := c.Request(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return c.handleError(resp)
	}

	return nil
}

func (c *Client) handleError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)

	var apiErr struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(body, &apiErr); err == nil && apiErr.Message != "" {
		return fmt.Errorf("API error (%s): %s", apiErr.Code, apiErr.Message)
	}

	return fmt.Errorf("API error: %s (status %d)", string(body), resp.StatusCode)
}

// Bucket represents a Strata bucket
type Bucket struct {
	Name        string            `json:"name"`
	Region      string            `json:"region,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	Versioning  bool              `json:"versioning"`
	Encryption  bool              `json:"encryption"`
	ObjectCount int64             `json:"object_count"`
	TotalSize   int64             `json:"total_size"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Object represents a Strata object
type Object struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ContentType  string            `json:"content_type"`
	ETag         string            `json:"etag"`
	LastModified time.Time         `json:"last_modified"`
	StorageClass string            `json:"storage_class"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// User represents a Strata user
type User struct {
	Username  string    `json:"username"`
	Email     string    `json:"email,omitempty"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

// AccessKey represents an access key
type AccessKey struct {
	AccessKeyID string    `json:"access_key_id"`
	SecretKey   string    `json:"secret_key,omitempty"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsed    time.Time `json:"last_used,omitempty"`
}

// ClusterInfo represents cluster information
type ClusterInfo struct {
	Status        string `json:"status"`
	Nodes         int    `json:"nodes"`
	HealthyNodes  int    `json:"healthy_nodes"`
	Version       string `json:"version"`
	TotalCapacity int64  `json:"total_capacity_bytes"`
	UsedCapacity  int64  `json:"used_capacity_bytes"`
}
