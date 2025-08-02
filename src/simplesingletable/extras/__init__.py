"""Extra utilities and patterns for simplesingletable."""

from .repository import ResourceRepository
from .versioned_repository import VersionedResourceRepository, VersionInfo

__all__ = ["ResourceRepository", "VersionedResourceRepository", "VersionInfo"]
