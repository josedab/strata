"""Object representation for Strata SDK."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from strata.client import StrataClient, AsyncStrataClient


@dataclass
class ObjectMetadata:
    """Metadata for a storage object."""

    custom: Dict[str, str] = field(default_factory=dict)
    last_modified: Optional[float] = None
    cache_control: Optional[str] = None
    content_disposition: Optional[str] = None
    content_encoding: Optional[str] = None
    content_language: Optional[str] = None
    expires: Optional[float] = None
    storage_class: str = "STANDARD"
    version_id: Optional[str] = None
    delete_marker: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "custom": self.custom,
            "storage_class": self.storage_class,
        }
        if self.last_modified:
            result["last_modified"] = self.last_modified
        if self.cache_control:
            result["cache_control"] = self.cache_control
        if self.content_disposition:
            result["content_disposition"] = self.content_disposition
        if self.content_encoding:
            result["content_encoding"] = self.content_encoding
        if self.content_language:
            result["content_language"] = self.content_language
        if self.expires:
            result["expires"] = self.expires
        if self.version_id:
            result["version_id"] = self.version_id
        return result


@dataclass
class Object:
    """Represents a storage object."""

    client: "StrataClient | AsyncStrataClient"
    bucket: str
    key: str
    size: int
    etag: str
    content_type: str
    metadata: ObjectMetadata

    def __repr__(self) -> str:
        return f"Object(bucket='{self.bucket}', key='{self.key}', size={self.size})"

    @property
    def last_modified(self) -> Optional[float]:
        """Get last modified timestamp."""
        return self.metadata.last_modified

    @property
    def version_id(self) -> Optional[str]:
        """Get version ID."""
        return self.metadata.version_id

    @property
    def storage_class(self) -> str:
        """Get storage class."""
        return self.metadata.storage_class

    # =========================================================================
    # Operations
    # =========================================================================

    def download(self) -> bytes:
        """
        Download the object data.

        Returns:
            Object data as bytes.
        """
        return self.client.get_object(self.bucket, self.key)

    def download_to_file(self, path: str) -> None:
        """
        Download the object to a file.

        Args:
            path: Path to save the file.
        """
        data = self.download()
        with open(path, "wb") as f:
            f.write(data)

    def delete(self) -> None:
        """Delete this object."""
        self.client.delete_object(self.bucket, self.key)

    def copy_to(self, dest_bucket: str, dest_key: str) -> "Object":
        """
        Copy this object to a new location.

        Args:
            dest_bucket: Destination bucket.
            dest_key: Destination key.

        Returns:
            The copied object.
        """
        return self.client.copy_object(
            self.bucket, self.key, dest_bucket, dest_key
        )

    def rename(self, new_key: str) -> "Object":
        """
        Rename this object (copy and delete).

        Args:
            new_key: New object key.

        Returns:
            The renamed object.
        """
        new_obj = self.copy_to(self.bucket, new_key)
        self.delete()
        return new_obj

    # =========================================================================
    # Metadata operations
    # =========================================================================

    def get_metadata(self) -> ObjectMetadata:
        """Get object metadata."""
        return self.metadata

    def set_metadata(self, metadata: Dict[str, str]) -> None:
        """
        Set custom metadata.

        Args:
            metadata: Custom metadata key-value pairs.
        """
        self.metadata.custom = metadata

    def add_metadata(self, key: str, value: str) -> None:
        """
        Add a metadata key-value pair.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        self.metadata.custom[key] = value

    def get_tags(self) -> Dict[str, str]:
        """Get object tags."""
        return {}

    def set_tags(self, tags: Dict[str, str]) -> None:
        """
        Set object tags.

        Args:
            tags: Dictionary of tags.
        """
        pass

    # =========================================================================
    # Access control
    # =========================================================================

    def get_acl(self) -> Dict[str, Any]:
        """Get object ACL."""
        return {"owner": None, "grants": []}

    def set_acl(self, acl: str) -> None:
        """
        Set object ACL.

        Args:
            acl: Canned ACL (private, public-read, etc.)
        """
        pass

    # =========================================================================
    # Presigned URLs
    # =========================================================================

    def generate_presigned_url(
        self,
        expires_in: int = 3600,
        *,
        method: str = "GET",
    ) -> str:
        """
        Generate a presigned URL for this object.

        Args:
            expires_in: Expiration time in seconds.
            method: HTTP method (GET, PUT).

        Returns:
            Presigned URL.
        """
        # Placeholder
        return f"https://strata.io/{self.bucket}/{self.key}?expires={expires_in}"

    # =========================================================================
    # Versioning
    # =========================================================================

    def list_versions(self) -> List["Object"]:
        """
        List all versions of this object.

        Returns:
            List of object versions.
        """
        return [self]

    def restore_version(self, version_id: str) -> "Object":
        """
        Restore a specific version as the current version.

        Args:
            version_id: Version ID to restore.

        Returns:
            The restored object.
        """
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bucket": self.bucket,
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "content_type": self.content_type,
            "metadata": self.metadata.to_dict(),
        }


@dataclass
class ListObjectsResult:
    """Result of listing objects."""

    objects: List[Object]
    prefixes: List[str]
    is_truncated: bool
    next_continuation_token: Optional[str]

    def __iter__(self):
        return iter(self.objects)

    def __len__(self) -> int:
        return len(self.objects)


@dataclass
class MultipartUpload:
    """Represents an in-progress multipart upload."""

    client: "StrataClient | AsyncStrataClient"
    bucket: str
    key: str
    upload_id: str
    parts: List[Dict[str, Any]] = field(default_factory=list)

    def upload_part(self, part_number: int, data: bytes) -> Dict[str, Any]:
        """
        Upload a part.

        Args:
            part_number: Part number (1-10000).
            data: Part data.

        Returns:
            Part info with ETag.
        """
        import hashlib

        etag = hashlib.md5(data).hexdigest()
        part = {"part_number": part_number, "etag": etag, "size": len(data)}
        self.parts.append(part)
        return part

    def complete(self) -> Object:
        """
        Complete the multipart upload.

        Returns:
            The completed object.
        """
        total_size = sum(p["size"] for p in self.parts)
        return Object(
            client=self.client,
            bucket=self.bucket,
            key=self.key,
            size=total_size,
            etag=f'"{self.upload_id}"',
            content_type="application/octet-stream",
            metadata=ObjectMetadata(),
        )

    def abort(self) -> None:
        """Abort the multipart upload."""
        self.parts = []
