"""Main client for Strata SDK."""

import asyncio
from typing import Optional, List, BinaryIO, Iterator, AsyncIterator, Union
from pathlib import Path
import hashlib
import time

from strata.config import ClientConfig
from strata.bucket import Bucket
from strata.object import Object, ObjectMetadata, ListObjectsResult
from strata.exceptions import (
    StrataError,
    NotFoundError,
    ConnectionError,
    TimeoutError,
)


class StrataClient:
    """Synchronous client for Strata distributed filesystem."""

    def __init__(self, config: Optional[ClientConfig] = None):
        """
        Initialize the Strata client.

        Args:
            config: Client configuration. If not provided, uses environment variables.
        """
        self.config = config or ClientConfig.from_env()
        self._connected = False
        self._connection = None

    def connect(self) -> "StrataClient":
        """
        Connect to the Strata cluster.

        Returns:
            Self for method chaining.

        Raises:
            ConnectionError: If connection fails.
        """
        try:
            # Initialize connection (placeholder for actual gRPC connection)
            self._connected = True
            return self
        except Exception as e:
            raise ConnectionError(self.config.endpoint, str(e))

    def close(self) -> None:
        """Close the connection."""
        self._connected = False
        if self._connection:
            self._connection = None

    def __enter__(self) -> "StrataClient":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # =========================================================================
    # Bucket operations
    # =========================================================================

    def create_bucket(
        self,
        name: str,
        *,
        region: Optional[str] = None,
        versioning: bool = False,
        encryption: bool = True,
    ) -> Bucket:
        """
        Create a new bucket.

        Args:
            name: Bucket name.
            region: Optional region for the bucket.
            versioning: Enable versioning.
            encryption: Enable server-side encryption.

        Returns:
            The created bucket.

        Raises:
            AlreadyExistsError: If bucket already exists.
            ValidationError: If bucket name is invalid.
        """
        self._ensure_connected()

        # Placeholder implementation
        return Bucket(
            client=self,
            name=name,
            region=region,
            versioning_enabled=versioning,
            encryption_enabled=encryption,
            created_at=time.time(),
        )

    def get_bucket(self, name: str) -> Bucket:
        """
        Get a bucket by name.

        Args:
            name: Bucket name.

        Returns:
            The bucket.

        Raises:
            NotFoundError: If bucket doesn't exist.
        """
        self._ensure_connected()

        # Placeholder implementation
        return Bucket(
            client=self,
            name=name,
            region=None,
            versioning_enabled=False,
            encryption_enabled=True,
            created_at=time.time(),
        )

    def list_buckets(self) -> List[Bucket]:
        """
        List all buckets.

        Returns:
            List of buckets.
        """
        self._ensure_connected()
        return []

    def delete_bucket(self, name: str, *, force: bool = False) -> None:
        """
        Delete a bucket.

        Args:
            name: Bucket name.
            force: Force delete even if bucket is not empty.

        Raises:
            NotFoundError: If bucket doesn't exist.
            ValidationError: If bucket is not empty and force is False.
        """
        self._ensure_connected()

    # =========================================================================
    # Object operations
    # =========================================================================

    def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, BinaryIO, Path],
        *,
        content_type: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Object:
        """
        Upload an object.

        Args:
            bucket: Bucket name.
            key: Object key.
            data: Object data (bytes, file-like object, or path).
            content_type: Content type.
            metadata: Custom metadata.

        Returns:
            The uploaded object.
        """
        self._ensure_connected()

        # Handle different data types
        if isinstance(data, Path):
            with open(data, "rb") as f:
                content = f.read()
        elif isinstance(data, bytes):
            content = data
        else:
            content = data.read()

        size = len(content)
        etag = hashlib.md5(content).hexdigest()

        return Object(
            client=self,
            bucket=bucket,
            key=key,
            size=size,
            etag=etag,
            content_type=content_type or "application/octet-stream",
            metadata=ObjectMetadata(
                custom=metadata or {},
                last_modified=time.time(),
            ),
        )

    def get_object(self, bucket: str, key: str) -> bytes:
        """
        Download an object.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Object data as bytes.

        Raises:
            NotFoundError: If object doesn't exist.
        """
        self._ensure_connected()
        raise NotFoundError("Object", f"{bucket}/{key}")

    def get_object_stream(
        self, bucket: str, key: str, *, chunk_size: Optional[int] = None
    ) -> Iterator[bytes]:
        """
        Stream an object.

        Args:
            bucket: Bucket name.
            key: Object key.
            chunk_size: Chunk size for streaming.

        Yields:
            Chunks of object data.

        Raises:
            NotFoundError: If object doesn't exist.
        """
        self._ensure_connected()
        return iter([])

    def head_object(self, bucket: str, key: str) -> ObjectMetadata:
        """
        Get object metadata without downloading.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Object metadata.

        Raises:
            NotFoundError: If object doesn't exist.
        """
        self._ensure_connected()
        raise NotFoundError("Object", f"{bucket}/{key}")

    def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete an object.

        Args:
            bucket: Bucket name.
            key: Object key.

        Raises:
            NotFoundError: If object doesn't exist.
        """
        self._ensure_connected()

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> ListObjectsResult:
        """
        List objects in a bucket.

        Args:
            bucket: Bucket name.
            prefix: Filter by prefix.
            delimiter: Group by delimiter.
            max_keys: Maximum number of keys to return.
            continuation_token: Token for pagination.

        Returns:
            List of objects and pagination info.
        """
        self._ensure_connected()
        return ListObjectsResult(
            objects=[],
            prefixes=[],
            is_truncated=False,
            next_continuation_token=None,
        )

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> Object:
        """
        Copy an object.

        Args:
            source_bucket: Source bucket name.
            source_key: Source object key.
            dest_bucket: Destination bucket name.
            dest_key: Destination object key.

        Returns:
            The copied object.
        """
        self._ensure_connected()
        return Object(
            client=self,
            bucket=dest_bucket,
            key=dest_key,
            size=0,
            etag="",
            content_type="application/octet-stream",
            metadata=ObjectMetadata(custom={}, last_modified=time.time()),
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if not self._connected:
            raise StrataError("Client is not connected. Call connect() first.")


class AsyncStrataClient:
    """Asynchronous client for Strata distributed filesystem."""

    def __init__(self, config: Optional[ClientConfig] = None):
        """
        Initialize the async Strata client.

        Args:
            config: Client configuration. If not provided, uses environment variables.
        """
        self.config = config or ClientConfig.from_env()
        self._connected = False
        self._connection = None

    async def connect(self) -> "AsyncStrataClient":
        """
        Connect to the Strata cluster.

        Returns:
            Self for method chaining.

        Raises:
            ConnectionError: If connection fails.
        """
        try:
            self._connected = True
            return self
        except Exception as e:
            raise ConnectionError(self.config.endpoint, str(e))

    async def close(self) -> None:
        """Close the connection."""
        self._connected = False
        if self._connection:
            self._connection = None

    async def __aenter__(self) -> "AsyncStrataClient":
        return await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # =========================================================================
    # Bucket operations
    # =========================================================================

    async def create_bucket(
        self,
        name: str,
        *,
        region: Optional[str] = None,
        versioning: bool = False,
        encryption: bool = True,
    ) -> Bucket:
        """Create a new bucket."""
        self._ensure_connected()
        return Bucket(
            client=self,
            name=name,
            region=region,
            versioning_enabled=versioning,
            encryption_enabled=encryption,
            created_at=time.time(),
        )

    async def get_bucket(self, name: str) -> Bucket:
        """Get a bucket by name."""
        self._ensure_connected()
        return Bucket(
            client=self,
            name=name,
            region=None,
            versioning_enabled=False,
            encryption_enabled=True,
            created_at=time.time(),
        )

    async def list_buckets(self) -> List[Bucket]:
        """List all buckets."""
        self._ensure_connected()
        return []

    async def delete_bucket(self, name: str, *, force: bool = False) -> None:
        """Delete a bucket."""
        self._ensure_connected()

    # =========================================================================
    # Object operations
    # =========================================================================

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, BinaryIO, Path],
        *,
        content_type: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Object:
        """Upload an object."""
        self._ensure_connected()

        if isinstance(data, Path):
            content = await asyncio.to_thread(data.read_bytes)
        elif isinstance(data, bytes):
            content = data
        else:
            content = await asyncio.to_thread(data.read)

        size = len(content)
        etag = hashlib.md5(content).hexdigest()

        return Object(
            client=self,
            bucket=bucket,
            key=key,
            size=size,
            etag=etag,
            content_type=content_type or "application/octet-stream",
            metadata=ObjectMetadata(
                custom=metadata or {},
                last_modified=time.time(),
            ),
        )

    async def get_object(self, bucket: str, key: str) -> bytes:
        """Download an object."""
        self._ensure_connected()
        raise NotFoundError("Object", f"{bucket}/{key}")

    async def get_object_stream(
        self, bucket: str, key: str, *, chunk_size: Optional[int] = None
    ) -> AsyncIterator[bytes]:
        """Stream an object."""
        self._ensure_connected()
        return
        yield  # type: ignore

    async def head_object(self, bucket: str, key: str) -> ObjectMetadata:
        """Get object metadata without downloading."""
        self._ensure_connected()
        raise NotFoundError("Object", f"{bucket}/{key}")

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object."""
        self._ensure_connected()

    async def list_objects(
        self,
        bucket: str,
        *,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> ListObjectsResult:
        """List objects in a bucket."""
        self._ensure_connected()
        return ListObjectsResult(
            objects=[],
            prefixes=[],
            is_truncated=False,
            next_continuation_token=None,
        )

    async def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> Object:
        """Copy an object."""
        self._ensure_connected()
        return Object(
            client=self,
            bucket=dest_bucket,
            key=dest_key,
            size=0,
            etag="",
            content_type="application/octet-stream",
            metadata=ObjectMetadata(custom={}, last_modified=time.time()),
        )

    def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if not self._connected:
            raise StrataError("Client is not connected. Call connect() first.")
