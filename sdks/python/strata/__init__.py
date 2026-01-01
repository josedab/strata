"""
Strata Python SDK

A Python client library for the Strata distributed filesystem.
"""

from strata.client import StrataClient, AsyncStrataClient
from strata.bucket import Bucket
from strata.object import Object, ObjectMetadata
from strata.exceptions import (
    StrataError,
    NotFoundError,
    PermissionDeniedError,
    AlreadyExistsError,
    TimeoutError,
    ConnectionError,
)
from strata.config import ClientConfig

__version__ = "0.1.0"
__all__ = [
    "StrataClient",
    "AsyncStrataClient",
    "Bucket",
    "Object",
    "ObjectMetadata",
    "ClientConfig",
    "StrataError",
    "NotFoundError",
    "PermissionDeniedError",
    "AlreadyExistsError",
    "TimeoutError",
    "ConnectionError",
]
