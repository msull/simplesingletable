import gzip
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from .exceptions import BlobNotFoundError, BlobPreconditionFailedError, BlobTooLargeError
from .models import BlobFieldConfig, BlobPlaceholder

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def normalize_etag(etag: Optional[str]) -> Optional[str]:
    """Put an entity tag in its canonical quoted form.

    HTTP entity tags are quoted strings (RFC 9110) and S3 returns them with the quotes
    attached, but boto does not re-add quotes that a caller has stripped -- so an ETag
    that has been round-tripped through a store that trims them (a database column, a
    JSON payload someone tidied) may arrive here bare. Normalizing on the way in means
    both forms compare equal and behave identically, rather than one of them silently
    matching nothing.
    """
    if etag is None:
        return None
    return '"' + etag.strip('"') + '"'


def _is_precondition_failed(error: ClientError) -> bool:
    """True if a ClientError represents an S3 412 Precondition Failed."""
    return (
        error.response.get("Error", {}).get("Code") == "PreconditionFailed"
        or error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412
    )


def _is_not_found(error: ClientError) -> bool:
    """True if a ClientError represents a missing key.

    ``get_object`` reports ``NoSuchKey``; ``head_object`` has no body to carry an error
    code and surfaces a bare ``404``.
    """
    return error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey")


@dataclass
class CacheEntry:
    """Represents a cached blob entry."""

    data: Any
    size_bytes: int
    timestamp: float
    access_count: int = 0


@dataclass
class CacheStats:
    """Statistics for blob cache performance."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    current_size_bytes: int = 0
    current_items: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0


class S3BlobStorage:
    """Handles blob storage operations in S3 with optional caching."""

    def __init__(
        self,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        s3_client: Optional["S3Client"] = None,
        connection_params: Optional[dict] = None,
        endpoint_url: Optional[str] = None,
        cache_enabled: bool = True,
        cache_max_size_bytes: int = 100 * 1024 * 1024,  # 100MB default
        cache_max_items: int = 1000,
        cache_ttl_seconds: Optional[float] = 900,  # 15 minutes default
        cache_max_item_size_bytes: int = 1024 * 1024,  # 1MB default
    ):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix or ""
        self._s3_client = s3_client
        self.connection_params = connection_params or {}
        self.endpoint_url = endpoint_url

        # Cache configuration
        self.cache_enabled = cache_enabled
        self.cache_max_size_bytes = cache_max_size_bytes
        self.cache_max_items = cache_max_items
        self.cache_ttl_seconds = cache_ttl_seconds
        self.cache_max_item_size_bytes = cache_max_item_size_bytes

        # Cache storage and statistics
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_stats = CacheStats()

    @property
    def s3_client(self) -> "S3Client":
        if not self._s3_client:
            self._s3_client = boto3.client("s3", endpoint_url=self.endpoint_url, **self.connection_params)
        return self._s3_client

    def _build_s3_key(
        self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None
    ) -> str:
        """Build S3 key for a blob field."""
        parts = []
        if self.key_prefix:
            parts.append(self.key_prefix.rstrip("/"))
        parts.append(resource_type)
        parts.append(resource_id)
        if version is not None:
            parts.append(f"v{version}")
        parts.append(field_name)
        return "/".join(parts)

    def put_blob(
        self,
        resource_type: str,
        resource_id: str,
        field_name: str,
        value: Any,
        config: BlobFieldConfig,
        version: Optional[int] = None,
        field_annotation: Optional[type] = None,
    ) -> BlobPlaceholder:
        """Store a blob field in S3.

        Args:
            resource_type: Type name of the resource
            resource_id: Unique ID of the resource
            field_name: Name of the blob field
            value: Value to store
            config: Blob field configuration
            version: Optional version number for versioned resources
            field_annotation: Optional type annotation for proper serialization

        Returns:
            BlobPlaceholder with metadata about the stored blob
        """
        from pydantic import BaseModel, TypeAdapter

        # Serialize the value
        if isinstance(value, bytes):
            data = value
        elif field_annotation is not None:
            # Use TypeAdapter with known type annotation (preferred)
            # This handles ANY complex type: list[Model], dict[str, Model], nested structures, etc.
            adapter = TypeAdapter(field_annotation)
            data = adapter.dump_json(value)
        elif isinstance(value, BaseModel):
            # Auto-detect: single Pydantic model
            data = value.model_dump_json(mode="json").encode("utf-8")
        elif isinstance(value, list) and value and isinstance(value[0], BaseModel):
            # Auto-detect: list of Pydantic models
            item_type = type(value[0])
            adapter = TypeAdapter(list[item_type])
            data = adapter.dump_json(value)
        else:
            # Fallback for plain data (dicts, lists, primitives)
            # Note: This won't properly handle sets in nested structures
            data = json.dumps(value).encode("utf-8")

        # Apply compression if configured
        compressed = config.get("compress", False)
        if compressed:
            data = gzip.compress(data)

        # Check size limit if configured
        size_bytes = len(data)
        max_size = config.get("max_size_bytes")
        if max_size and size_bytes > max_size:
            raise ValueError(f"Blob field {field_name} exceeds maximum size " f"({size_bytes} > {max_size} bytes)")

        # Build S3 key
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        # Prepare S3 put parameters
        put_params = {
            "Bucket": self.bucket_name,
            "Key": s3_key,
            "Body": data,
        }

        # Add content type if specified
        content_type = config.get("content_type")
        if content_type:
            put_params["ContentType"] = content_type

        # Add metadata
        put_params["Metadata"] = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "field_name": field_name,
            "compressed": str(compressed),
        }
        if version is not None:
            put_params["Metadata"]["version"] = str(version)

        # Upload to S3
        put_response = self.s3_client.put_object(**put_params)
        etag = normalize_etag(put_response.get("ETag"))

        # Update cache with the original (uncompressed) value
        cache_key = self._cache_key(resource_type, resource_id, field_name, version)
        self._cache_put(cache_key, value, size_bytes=len(data) if compressed else size_bytes)

        # Return placeholder metadata
        return BlobPlaceholder(
            field_name=field_name,
            s3_key=s3_key,
            size_bytes=size_bytes,
            content_type=content_type,
            compressed=compressed,
            etag=etag,
        )

    def _cache_key(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> str:
        """Generate a cache key for a blob."""
        version_str = f"v{version}" if version is not None else "latest"
        return f"{resource_type}#{resource_id}#{field_name}#{version_str}"

    def _cache_get(
        self, cache_key: str, if_match: Optional[str] = None, max_bytes: Optional[int] = None
    ) -> Optional[Any]:
        """Get an item from cache if available and valid.

        A conditional read is never served from cache. A cached ETag is only evidence of
        what the object was when it was cached, so matching against it would answer the
        wrong question -- the caller is asking about the object *now*, which only S3 can
        say. ``if_match`` therefore forces a round trip and lets S3 adjudicate.

        Likewise an entry that looks larger than ``max_bytes`` is not served, so that the
        limit is decided by S3's authoritative ``ContentLength`` rather than by the
        cache's approximate size accounting.
        """
        if not self.cache_enabled or if_match is not None:
            return None

        with self._cache_lock:
            if cache_key not in self._cache:
                return None

            entry = self._cache[cache_key]

            # Check TTL if configured
            if self.cache_ttl_seconds is not None:
                age = time.time() - entry.timestamp
                if age > self.cache_ttl_seconds:
                    # Expired entry, remove it
                    self._cache_stats.current_size_bytes -= entry.size_bytes
                    self._cache_stats.current_items -= 1
                    del self._cache[cache_key]
                    return None

            # Let S3 adjudicate a size limit rather than the cache's approximate size
            if max_bytes is not None and entry.size_bytes > max_bytes:
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(cache_key)
            entry.access_count += 1
            self._cache_stats.hits += 1
            return entry.data

    def _get_size(self, data: Any) -> int:
        """Calculate the approximate size of data in bytes."""
        if isinstance(data, bytes):
            return len(data)
        elif isinstance(data, str):
            return len(data.encode("utf-8"))
        else:
            # For other types, use JSON representation as approximation
            return len(json.dumps(data, default=str).encode("utf-8"))

    def _cache_put(self, cache_key: str, data: Any, size_bytes: Optional[int] = None) -> None:
        """Put an item into the cache with LRU eviction."""
        if not self.cache_enabled:
            return

        # Calculate size if not provided
        if size_bytes is None:
            size_bytes = self._get_size(data)

        # Don't cache if item is too large
        if size_bytes > self.cache_max_item_size_bytes:
            return

        with self._cache_lock:
            # If key already exists, remove old entry size
            if cache_key in self._cache:
                old_entry = self._cache[cache_key]
                self._cache_stats.current_size_bytes -= old_entry.size_bytes
                self._cache_stats.current_items -= 1

            # Evict items if necessary
            self._evict_if_needed(size_bytes)

            # Add new entry
            entry = CacheEntry(
                data=data,
                size_bytes=size_bytes,
                timestamp=time.time(),
                access_count=0,
            )
            self._cache[cache_key] = entry
            self._cache_stats.current_size_bytes += size_bytes
            self._cache_stats.current_items += 1

    def _evict_if_needed(self, new_item_size: int) -> None:
        """Evict items from cache if size or item limits would be exceeded."""
        # Must be called with lock held

        # Evict by item count
        while self._cache and self._cache_stats.current_items >= self.cache_max_items:
            self._evict_oldest()

        # Evict by size
        while self._cache and (self._cache_stats.current_size_bytes + new_item_size) > self.cache_max_size_bytes:
            self._evict_oldest()

    def _evict_oldest(self) -> None:
        """Evict the oldest (least recently used) item from cache."""
        # Must be called with lock held
        if not self._cache:
            return

        # Pop the first item (oldest due to OrderedDict)
        cache_key, entry = self._cache.popitem(last=False)
        self._cache_stats.current_size_bytes -= entry.size_bytes
        self._cache_stats.current_items -= 1
        self._cache_stats.evictions += 1

    def get_blob(
        self,
        resource_type: str,
        resource_id: str,
        field_name: str,
        version: Optional[int] = None,
        *,
        if_match: Optional[str] = None,
        max_bytes: Optional[int] = None,
    ) -> Any:
        """Retrieve a blob field from S3 with caching.

        Args:
            resource_type: Type name of the resource
            resource_id: Unique ID of the resource
            field_name: Name of the blob field
            version: Optional version number for versioned resources
            if_match: ETag the stored object must still have, as returned by ``head_blob``
                or ``put_blob``. Quoted and unquoted forms are both accepted -- the value
                is normalized to the canonical quoted form before use. A conditional read
                always goes to S3: the cache cannot vouch for what the object is now, only
                for what it was.
            max_bytes: Refuse to download an object whose stored (post-compression) size
                exceeds this. The size is checked before the body is read, so an
                oversized payload is never allocated.

        Returns:
            The deserialized blob data

        Raises:
            BlobNotFoundError: No object exists at the blob's key.
            BlobPreconditionFailedError: ``if_match`` was supplied and the stored object
                has been replaced since that ETag was captured.
            BlobTooLargeError: The object is larger than ``max_bytes``.
        """
        if_match = normalize_etag(if_match)

        # Check cache first
        cache_key = self._cache_key(resource_type, resource_id, field_name, version)
        cached_data = self._cache_get(cache_key, if_match=if_match, max_bytes=max_bytes)
        if cached_data is not None:
            return cached_data

        # Cache miss - record stat
        if self.cache_enabled:
            with self._cache_lock:
                self._cache_stats.misses += 1

        # Build S3 key
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        get_params: dict[str, Any] = {"Bucket": self.bucket_name, "Key": s3_key}
        if if_match is not None:
            get_params["IfMatch"] = if_match

        try:
            # Get object from S3
            response = self.s3_client.get_object(**get_params)
        except ClientError as e:
            if _is_precondition_failed(e):
                raise BlobPreconditionFailedError(
                    f"Blob changed since it was last observed: {s3_key}",
                    s3_key=s3_key,
                    bucket=self.bucket_name,
                    expected_etag=if_match,
                ) from e
            if _is_not_found(e):
                raise BlobNotFoundError(f"Blob not found: {s3_key}", s3_key=s3_key, bucket=self.bucket_name) from e
            raise

        # ContentLength is available before the body is touched, so an oversized object
        # can be refused without ever allocating its payload.
        if max_bytes is not None and response["ContentLength"] > max_bytes:
            response["Body"].close()
            raise BlobTooLargeError(
                f"Blob {s3_key} is {response['ContentLength']} bytes, exceeding max_bytes={max_bytes}",
                s3_key=s3_key,
                size_bytes=response["ContentLength"],
                max_bytes=max_bytes,
            )

        # Read data
        data = response["Body"].read()

        # Check if compressed (from metadata)
        metadata = response.get("Metadata", {})
        compressed = metadata.get("compressed", "False").lower() == "true"

        # Decompress if needed
        if compressed:
            data = gzip.decompress(data)

        # Try to deserialize as JSON
        try:
            result = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Return as bytes if not JSON
            result = data

        # Cache the result
        self._cache_put(cache_key, result, size_bytes=len(data))

        return result

    def _cache_invalidate(self, cache_key: str) -> None:
        """Remove a single cache entry by key."""
        with self._cache_lock:
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                self._cache_stats.current_size_bytes -= entry.size_bytes
                self._cache_stats.current_items -= 1
                del self._cache[cache_key]

    def head_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> dict:
        """Get metadata about a blob without downloading it.

        Returns:
            Dict with keys: size_bytes, compressed, content_type, metadata, s3_key, etag.

            ``etag`` is preserved verbatim, quotes included -- entity tags are quoted
            strings (RFC 9110) and must be handed back to ``get_blob(if_match=)`` or
            ``copy_blob(source_etag=)`` unmodified. It is an opaque identity token, not
            a checksum: multipart uploads produce ETags that are not MD5s.

        Raises:
            BlobNotFoundError: No object exists at the blob's key.
        """
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            if _is_not_found(e):
                raise BlobNotFoundError(f"Blob not found: {s3_key}", s3_key=s3_key, bucket=self.bucket_name) from e
            raise

        metadata = response.get("Metadata", {})
        return {
            "size_bytes": response["ContentLength"],
            "compressed": metadata.get("compressed", "False").lower() == "true",
            "content_type": response.get("ContentType"),
            "metadata": metadata,
            "s3_key": s3_key,
            "etag": normalize_etag(response.get("ETag")),
        }

    def copy_blob_object(
        self,
        source_s3_key: str,
        target_resource_type: str,
        target_resource_id: str,
        target_field_name: str,
        target_version: Optional[int] = None,
        compressed: bool = False,
        content_type: Optional[str] = None,
        source_bucket: Optional[str] = None,
        source_etag: Optional[str] = None,
    ) -> BlobPlaceholder:
        """Server-side copy of an S3 object to a managed blob key.

        Args:
            source_s3_key: Full S3 key of the source object
            target_resource_type: Type name for the target blob
            target_resource_id: Resource ID for the target blob
            target_field_name: Field name for the target blob
            target_version: Optional version number for versioned resources
            compressed: Whether the source data is gzip-compressed
            content_type: Content type for the target object
            source_bucket: Source bucket (defaults to managed bucket)
            source_etag: ETag the source object must still have, verbatim with quotes.
                Copies the object only if it has not been replaced since that ETag was
                captured.

        Returns:
            BlobPlaceholder with metadata about the copied blob

        Raises:
            BlobPreconditionFailedError: ``source_etag`` was supplied and the source has
                been replaced since.
            BlobNotFoundError: The source object does not exist.
        """
        target_s3_key = self._build_s3_key(target_resource_type, target_resource_id, target_field_name, target_version)
        source_bucket = source_bucket or self.bucket_name

        # Build copy source
        copy_source = {"Bucket": source_bucket, "Key": source_s3_key}

        # Build metadata for the target object
        target_metadata = {
            "resource_type": target_resource_type,
            "resource_id": target_resource_id,
            "field_name": target_field_name,
            "compressed": str(compressed),
        }
        if target_version is not None:
            target_metadata["version"] = str(target_version)

        # Build copy parameters
        copy_params = {
            "Bucket": self.bucket_name,
            "Key": target_s3_key,
            "CopySource": copy_source,
            "Metadata": target_metadata,
            "MetadataDirective": "REPLACE",
        }
        if content_type:
            copy_params["ContentType"] = content_type
        source_etag = normalize_etag(source_etag)
        if source_etag is not None:
            copy_params["CopySourceIfMatch"] = source_etag

        # Perform server-side copy
        try:
            self.s3_client.copy_object(**copy_params)
        except ClientError as e:
            if _is_precondition_failed(e):
                raise BlobPreconditionFailedError(
                    f"Source blob changed since it was last observed: {source_s3_key}",
                    s3_key=source_s3_key,
                    bucket=source_bucket,
                    expected_etag=source_etag,
                ) from e
            if _is_not_found(e):
                raise BlobNotFoundError(
                    f"Source blob not found: {source_s3_key}", s3_key=source_s3_key, bucket=source_bucket
                ) from e
            raise

        # Get size of copied object
        head_response = self.s3_client.head_object(Bucket=self.bucket_name, Key=target_s3_key)
        size_bytes = head_response["ContentLength"]

        # Invalidate any cached entry for the target key
        cache_key = self._cache_key(target_resource_type, target_resource_id, target_field_name, target_version)
        self._cache_invalidate(cache_key)

        return BlobPlaceholder(
            field_name=target_field_name,
            s3_key=target_s3_key,
            size_bytes=size_bytes,
            content_type=content_type,
            compressed=compressed,
            etag=normalize_etag(head_response.get("ETag")),
        )

    def delete_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> None:
        """Delete a blob field from S3 and remove from cache."""
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        # Remove from cache
        cache_key = self._cache_key(resource_type, resource_id, field_name, version)
        self._cache_invalidate(cache_key)

        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            # Ignore if key doesn't exist
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

    def delete_all_blobs(self, resource_type: str, resource_id: str) -> int:
        """Delete all blobs for a resource and clear from cache.

        Returns:
            Number of blobs deleted
        """
        # Clear matching items from cache
        with self._cache_lock:
            cache_prefix = f"{resource_type}#{resource_id}#"
            keys_to_remove = [k for k in self._cache.keys() if k.startswith(cache_prefix)]
            for cache_key in keys_to_remove:
                entry = self._cache[cache_key]
                self._cache_stats.current_size_bytes -= entry.size_bytes
                self._cache_stats.current_items -= 1
                del self._cache[cache_key]

        # Build prefix for all blobs of this resource
        prefix = self._build_s3_key(resource_type, resource_id, "", None).rstrip("/")

        # List all objects with this prefix
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        # Collect all keys to delete
        keys_to_delete = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys_to_delete.append({"Key": obj["Key"]})

        if not keys_to_delete:
            return 0

        # Delete in batches (S3 allows max 1000 per request)
        deleted_count = 0
        batch_size = 1000
        for i in range(0, len(keys_to_delete), batch_size):
            batch = keys_to_delete[i : i + batch_size]
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": batch})
            deleted_count += len(batch)

        return deleted_count

    def list_blob_versions(self, resource_type: str, resource_id: str, field_name: str) -> list[int]:
        """List all versions of a blob field.

        Returns:
            List of version numbers
        """
        # Build prefix for this field's blobs
        prefix_parts = []
        if self.key_prefix:
            prefix_parts.append(self.key_prefix.rstrip("/"))
        prefix_parts.extend([resource_type, resource_id])
        prefix = "/".join(prefix_parts) + "/"

        # List objects and extract versions
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        versions = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    # Parse version from key if it matches pattern
                    parts = key.split("/")
                    if len(parts) >= 2 and parts[-1] == field_name:
                        # Check if second-to-last part is version
                        version_part = parts[-2]
                        if version_part.startswith("v"):
                            try:
                                version = int(version_part[1:])
                                versions.append(version)
                            except ValueError:
                                pass

        return sorted(versions)

    def clear_cache(self) -> None:
        """Clear all items from the cache."""
        with self._cache_lock:
            self._cache.clear()
            self._cache_stats.current_size_bytes = 0
            self._cache_stats.current_items = 0

    def get_cache_stats(self) -> CacheStats:
        """Get current cache statistics."""
        with self._cache_lock:
            return CacheStats(
                hits=self._cache_stats.hits,
                misses=self._cache_stats.misses,
                evictions=self._cache_stats.evictions,
                current_size_bytes=self._cache_stats.current_size_bytes,
                current_items=self._cache_stats.current_items,
            )

    def warm_cache(
        self,
        items: list[Tuple[str, str, str, Optional[int]]],
        batch_size: int = 10,
    ) -> int:
        """Pre-load frequently used blobs into cache.

        Args:
            items: List of (resource_type, resource_id, field_name, version) tuples
            batch_size: Number of items to fetch in parallel (not implemented as parallel yet)

        Returns:
            Number of items successfully loaded into cache
        """
        loaded = 0
        for resource_type, resource_id, field_name, version in items:
            try:
                # This will load the item into cache as a side effect
                self.get_blob(resource_type, resource_id, field_name, version)
                loaded += 1
            except Exception:
                # Skip items that fail to load
                continue
        return loaded

    def get_cache_info(self) -> dict:
        """Get detailed cache information for debugging."""
        with self._cache_lock:
            # Create stats directly without calling get_cache_stats to avoid deadlock
            stats = CacheStats(
                hits=self._cache_stats.hits,
                misses=self._cache_stats.misses,
                evictions=self._cache_stats.evictions,
                current_size_bytes=self._cache_stats.current_size_bytes,
                current_items=self._cache_stats.current_items,
            )

            # Get top accessed items
            top_items = []
            for cache_key, entry in self._cache.items():
                top_items.append(
                    {
                        "key": cache_key,
                        "size_bytes": entry.size_bytes,
                        "access_count": entry.access_count,
                        "age_seconds": time.time() - entry.timestamp,
                    }
                )
            # Sort by access count
            top_items.sort(key=lambda x: x["access_count"], reverse=True)

            return {
                "enabled": self.cache_enabled,
                "max_size_bytes": self.cache_max_size_bytes,
                "max_items": self.cache_max_items,
                "max_item_size_bytes": self.cache_max_item_size_bytes,
                "ttl_seconds": self.cache_ttl_seconds,
                "stats": {
                    "hits": stats.hits,
                    "misses": stats.misses,
                    "hit_rate": stats.hit_rate,
                    "evictions": stats.evictions,
                    "current_size_bytes": stats.current_size_bytes,
                    "current_items": stats.current_items,
                },
                "top_accessed_items": top_items[:10],  # Top 10 most accessed
            }
