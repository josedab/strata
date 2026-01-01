package strata

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the client configuration.
type Config struct {
	// Connection settings
	Endpoint   string
	S3Endpoint string

	// Authentication
	AccessKey string
	SecretKey string
	Token     string

	// Timeouts
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	// Connection pool
	MaxConnections     int
	MaxIdleConnections int
	IdleTimeout        time.Duration

	// TLS
	TLS TLSConfig

	// Retry
	Retry RetryConfig

	// Performance
	ChunkSize             int
	MultipartThreshold    int
	MaxConcurrentUploads  int
	MaxConcurrentDownloads int

	// Caching
	EnableCache      bool
	CacheSizeMB      int
	CacheTTLSeconds  int

	// Compression
	EnableCompression bool
	CompressionLevel  int

	// Debug
	Debug bool
}

// TLSConfig holds TLS configuration.
type TLSConfig struct {
	Enabled        bool
	CACertPath     string
	ClientCertPath string
	ClientKeyPath  string
	SkipVerify     bool
}

// RetryConfig holds retry configuration.
type RetryConfig struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Endpoint:       "localhost:9000",
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 60 * time.Second,
		ReadTimeout:    300 * time.Second,
		WriteTimeout:   300 * time.Second,

		MaxConnections:     100,
		MaxIdleConnections: 10,
		IdleTimeout:        300 * time.Second,

		TLS: TLSConfig{
			Enabled: false,
		},

		Retry: RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
		},

		ChunkSize:             8 * 1024 * 1024,  // 8MB
		MultipartThreshold:    64 * 1024 * 1024, // 64MB
		MaxConcurrentUploads:  4,
		MaxConcurrentDownloads: 4,

		EnableCache:     true,
		CacheSizeMB:     256,
		CacheTTLSeconds: 300,

		EnableCompression: true,
		CompressionLevel:  6,

		Debug: false,
	}
}

// ConfigFromEnv creates a configuration from environment variables.
func ConfigFromEnv() *Config {
	config := DefaultConfig()

	if v := os.Getenv("STRATA_ENDPOINT"); v != "" {
		config.Endpoint = v
	}
	if v := os.Getenv("STRATA_S3_ENDPOINT"); v != "" {
		config.S3Endpoint = v
	}
	if v := os.Getenv("STRATA_ACCESS_KEY"); v != "" {
		config.AccessKey = v
	}
	if v := os.Getenv("STRATA_SECRET_KEY"); v != "" {
		config.SecretKey = v
	}
	if v := os.Getenv("STRATA_TOKEN"); v != "" {
		config.Token = v
	}
	if v := os.Getenv("STRATA_TLS_ENABLED"); v == "true" {
		config.TLS.Enabled = true
	}
	if v := os.Getenv("STRATA_CA_CERT"); v != "" {
		config.TLS.CACertPath = v
	}
	if v := os.Getenv("STRATA_DEBUG"); v == "true" {
		config.Debug = true
	}
	if v := os.Getenv("STRATA_CONNECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			config.ConnectTimeout = d
		}
	}
	if v := os.Getenv("STRATA_REQUEST_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			config.RequestTimeout = d
		}
	}
	if v := os.Getenv("STRATA_MAX_CONNECTIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			config.MaxConnections = n
		}
	}

	return config
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if c.ConnectTimeout <= 0 {
		return fmt.Errorf("connect timeout must be positive")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if c.Retry.MaxAttempts < 0 {
		return fmt.Errorf("max retry attempts must be non-negative")
	}
	return nil
}

// WithEndpoint sets the endpoint.
func (c *Config) WithEndpoint(endpoint string) *Config {
	c.Endpoint = endpoint
	return c
}

// WithCredentials sets the access credentials.
func (c *Config) WithCredentials(accessKey, secretKey string) *Config {
	c.AccessKey = accessKey
	c.SecretKey = secretKey
	return c
}

// WithTLS enables TLS.
func (c *Config) WithTLS(caCertPath string) *Config {
	c.TLS.Enabled = true
	c.TLS.CACertPath = caCertPath
	return c
}

// WithDebug enables debug mode.
func (c *Config) WithDebug(debug bool) *Config {
	c.Debug = debug
	return c
}
