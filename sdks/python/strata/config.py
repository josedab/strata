"""Client configuration for Strata SDK."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path


@dataclass
class TLSConfig:
    """TLS configuration."""

    enabled: bool = False
    ca_cert_path: Optional[Path] = None
    client_cert_path: Optional[Path] = None
    client_key_path: Optional[Path] = None
    skip_verify: bool = False


@dataclass
class RetryConfig:
    """Retry configuration."""

    max_attempts: int = 3
    initial_backoff_ms: int = 100
    max_backoff_ms: int = 10000
    backoff_multiplier: float = 2.0


@dataclass
class ClientConfig:
    """Configuration for the Strata client."""

    # Connection settings
    endpoint: str = "localhost:9000"
    s3_endpoint: Optional[str] = None

    # Authentication
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    token: Optional[str] = None

    # Timeouts (in seconds)
    connect_timeout: float = 10.0
    request_timeout: float = 60.0
    read_timeout: float = 300.0
    write_timeout: float = 300.0

    # Connection pool
    max_connections: int = 100
    max_idle_connections: int = 10
    idle_timeout: float = 300.0

    # TLS
    tls: TLSConfig = field(default_factory=TLSConfig)

    # Retry
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Performance
    chunk_size: int = 8 * 1024 * 1024  # 8MB
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    max_concurrent_uploads: int = 4
    max_concurrent_downloads: int = 4

    # Caching
    enable_cache: bool = True
    cache_size_mb: int = 256
    cache_ttl_seconds: int = 300

    # Compression
    enable_compression: bool = True
    compression_level: int = 6

    # Debug
    debug: bool = False

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Create configuration from environment variables."""
        import os

        config = cls()

        if endpoint := os.getenv("STRATA_ENDPOINT"):
            config.endpoint = endpoint
        if s3_endpoint := os.getenv("STRATA_S3_ENDPOINT"):
            config.s3_endpoint = s3_endpoint
        if access_key := os.getenv("STRATA_ACCESS_KEY"):
            config.access_key = access_key
        if secret_key := os.getenv("STRATA_SECRET_KEY"):
            config.secret_key = secret_key
        if token := os.getenv("STRATA_TOKEN"):
            config.token = token
        if os.getenv("STRATA_TLS_ENABLED", "").lower() == "true":
            config.tls.enabled = True
        if ca_cert := os.getenv("STRATA_CA_CERT"):
            config.tls.ca_cert_path = Path(ca_cert)
        if os.getenv("STRATA_DEBUG", "").lower() == "true":
            config.debug = True

        return config

    @classmethod
    def from_file(cls, path: Path) -> "ClientConfig":
        """Load configuration from a TOML or JSON file."""
        import json

        config = cls()

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        content = path.read_text()

        if path.suffix == ".json":
            data = json.loads(content)
        elif path.suffix == ".toml":
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib
            data = tomllib.loads(content)
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")

        # Apply configuration
        for key, value in data.items():
            if hasattr(config, key):
                setattr(config, key, value)

        return config

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "endpoint": self.endpoint,
            "s3_endpoint": self.s3_endpoint,
            "access_key": self.access_key,
            "connect_timeout": self.connect_timeout,
            "request_timeout": self.request_timeout,
            "chunk_size": self.chunk_size,
            "enable_compression": self.enable_compression,
            "debug": self.debug,
        }
