"""Local file-based blob storage implementation for offline/demo usage."""

import gzip
import json
import shutil
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, TypeAdapter

from .models import BlobFieldConfig, BlobPlaceholder


class LocalBlobStorage:
    """Handles blob storage operations using local filesystem.

    Provides the same interface as S3BlobStorage but stores blobs as files
    in a local directory structure. Intended for offline demos and local testing.
    """

    def __init__(
        self,
        storage_dir: str,
        key_prefix: Optional[str] = None,
    ):
        """Initialize local blob storage.

        Args:
            storage_dir: Base directory for blob storage
            key_prefix: Optional prefix for all blob paths
        """
        self.storage_dir = Path(storage_dir)
        self.key_prefix = key_prefix or ""

        # Create blobs directory
        self.blobs_dir = self.storage_dir / "blobs"
        self.blobs_dir.mkdir(parents=True, exist_ok=True)

    def _build_s3_key(
        self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None
    ) -> str:
        """Build storage key (path) for a blob field.

        Mirrors S3BlobStorage._build_s3_key for compatibility.
        """
        parts = []
        if self.key_prefix:
            parts.append(self.key_prefix.rstrip("/"))
        parts.append(resource_type)
        parts.append(resource_id)
        if version is not None:
            parts.append(f"v{version}")
        parts.append(field_name)
        return "/".join(parts)

    def _key_to_path(self, s3_key: str) -> Path:
        """Convert an S3-style key to a local file path."""
        return self.blobs_dir / s3_key

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
        """Store a blob field in local filesystem.

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
        # Serialize the value (same logic as S3BlobStorage)
        if isinstance(value, bytes):
            data = value
        elif field_annotation is not None:
            # Use TypeAdapter with known type annotation (preferred)
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

        # Build storage key and path
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)
        file_path = self._key_to_path(s3_key)

        # Create parent directories
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write blob data to file
        file_path.write_bytes(data)

        # Write metadata to companion file
        metadata = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "field_name": field_name,
            "compressed": str(compressed),
            "content_type": config.get("content_type"),
        }
        if version is not None:
            metadata["version"] = str(version)

        metadata_path = file_path.with_suffix(file_path.suffix + ".meta")
        metadata_path.write_text(json.dumps(metadata))

        # Return placeholder metadata
        content_type = config.get("content_type")
        return BlobPlaceholder(
            field_name=field_name,
            s3_key=s3_key,
            size_bytes=size_bytes,
            content_type=content_type,
            compressed=compressed,
        )

    def get_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> Any:
        """Retrieve a blob field from local filesystem.

        Returns:
            The deserialized blob data
        """
        # Build storage key and path
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)
        file_path = self._key_to_path(s3_key)

        if not file_path.exists():
            raise ValueError(f"Blob not found: {s3_key}")

        # Read data
        data = file_path.read_bytes()

        # Check if compressed (from metadata)
        metadata_path = file_path.with_suffix(file_path.suffix + ".meta")
        compressed = False
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
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

        return result

    def head_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> dict:
        """Get metadata about a blob without reading its contents.

        Returns:
            Dict with keys: size_bytes, compressed, content_type, metadata, s3_key
        """
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)
        file_path = self._key_to_path(s3_key)

        if not file_path.exists():
            raise ValueError(f"Blob not found: {s3_key}")

        size_bytes = file_path.stat().st_size

        # Read metadata from companion file
        metadata = {}
        compressed = False
        content_type = None
        metadata_path = file_path.with_suffix(file_path.suffix + ".meta")
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
            compressed = metadata.get("compressed", "False").lower() == "true"
            content_type = metadata.get("content_type")

        return {
            "size_bytes": size_bytes,
            "compressed": compressed,
            "content_type": content_type,
            "metadata": metadata,
            "s3_key": s3_key,
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
    ) -> "BlobPlaceholder":
        """Copy a blob file to a new managed blob location.

        Args:
            source_s3_key: Storage key of the source blob
            target_resource_type: Type name for the target blob
            target_resource_id: Resource ID for the target blob
            target_field_name: Field name for the target blob
            target_version: Optional version number for versioned resources
            compressed: Whether the source data is gzip-compressed
            content_type: Content type for the target object
            source_bucket: Ignored for local storage (kept for API parity)

        Returns:
            BlobPlaceholder with metadata about the copied blob
        """
        target_s3_key = self._build_s3_key(target_resource_type, target_resource_id, target_field_name, target_version)

        source_path = self._key_to_path(source_s3_key)
        target_path = self._key_to_path(target_s3_key)

        if not source_path.exists():
            raise ValueError(f"Source blob not found: {source_s3_key}")

        # Create parent directories
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy blob file
        shutil.copy2(str(source_path), str(target_path))

        # Write new metadata companion file
        target_metadata = {
            "resource_type": target_resource_type,
            "resource_id": target_resource_id,
            "field_name": target_field_name,
            "compressed": str(compressed),
            "content_type": content_type,
        }
        if target_version is not None:
            target_metadata["version"] = str(target_version)

        metadata_path = target_path.with_suffix(target_path.suffix + ".meta")
        metadata_path.write_text(json.dumps(target_metadata))

        size_bytes = target_path.stat().st_size

        return BlobPlaceholder(
            field_name=target_field_name,
            s3_key=target_s3_key,
            size_bytes=size_bytes,
            content_type=content_type,
            compressed=compressed,
        )

    def delete_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> None:
        """Delete a blob field from local filesystem."""
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)
        file_path = self._key_to_path(s3_key)

        # Delete blob file if it exists
        if file_path.exists():
            file_path.unlink()

        # Delete metadata file if it exists
        metadata_path = file_path.with_suffix(file_path.suffix + ".meta")
        if metadata_path.exists():
            metadata_path.unlink()

        # Clean up empty parent directories
        try:
            parent = file_path.parent
            while parent != self.blobs_dir and parent.exists():
                if not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
                else:
                    break
        except OSError:
            # Ignore errors during cleanup
            pass

    def delete_all_blobs(self, resource_type: str, resource_id: str) -> int:
        """Delete all blobs for a resource.

        Returns:
            Number of blobs deleted
        """
        # Build prefix for all blobs of this resource
        prefix = self._build_s3_key(resource_type, resource_id, "", None).rstrip("/")
        prefix_path = self._key_to_path(prefix)

        if not prefix_path.exists():
            return 0

        # Count and delete all blob files (excluding .meta files)
        deleted_count = 0
        for file_path in prefix_path.rglob("*"):
            if file_path.is_file() and not file_path.name.endswith(".meta"):
                # Delete the blob file
                file_path.unlink()
                deleted_count += 1

                # Delete companion metadata file
                metadata_path = file_path.with_suffix(file_path.suffix + ".meta")
                if metadata_path.exists():
                    metadata_path.unlink()

        # Remove the directory tree for this resource
        if prefix_path.exists():
            shutil.rmtree(prefix_path)

        return deleted_count

    def list_blob_versions(self, resource_type: str, resource_id: str, field_name: str) -> list[int]:
        """List all versions of a blob field.

        Returns:
            List of version numbers
        """
        # Build base path for this resource
        prefix_parts = []
        if self.key_prefix:
            prefix_parts.append(self.key_prefix.rstrip("/"))
        prefix_parts.extend([resource_type, resource_id])
        prefix_path = self.blobs_dir / "/".join(prefix_parts)

        if not prefix_path.exists():
            return []

        versions = []
        # Look for version directories
        for item in prefix_path.iterdir():
            if item.is_dir() and item.name.startswith("v"):
                # Check if field file exists in this version
                field_file = item / field_name
                if field_file.exists():
                    try:
                        version = int(item.name[1:])
                        versions.append(version)
                    except ValueError:
                        pass

        return sorted(versions)
