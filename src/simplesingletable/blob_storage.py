import gzip
import json
from typing import TYPE_CHECKING, Any, Optional

import boto3
from botocore.exceptions import ClientError

from .models import BlobFieldConfig, BlobPlaceholder

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


class S3BlobStorage:
    """Handles blob storage operations in S3."""

    def __init__(
        self,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        s3_client: Optional["S3Client"] = None,
        connection_params: Optional[dict] = None,
        endpoint_url: Optional[str] = None,
    ):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix or ""
        self._s3_client = s3_client
        self.connection_params = connection_params or {}
        self.endpoint_url = endpoint_url

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
    ) -> BlobPlaceholder:
        """Store a blob field in S3.

        Returns:
            BlobPlaceholder with metadata about the stored blob
        """
        # Serialize the value
        if isinstance(value, bytes):
            data = value
        else:
            # Convert to JSON for non-bytes data
            data = json.dumps(value, default=str).encode("utf-8")

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
        self.s3_client.put_object(**put_params)

        # Return placeholder metadata
        return BlobPlaceholder(
            field_name=field_name,
            s3_key=s3_key,
            size_bytes=size_bytes,
            content_type=content_type,
            compressed=compressed,
        )

    def get_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> Any:
        """Retrieve a blob field from S3.

        Returns:
            The deserialized blob data
        """
        # Build S3 key
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        try:
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)

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
                return json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Return as bytes if not JSON
                return data

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise ValueError(f"Blob not found: {s3_key}") from e
            raise

    def delete_blob(self, resource_type: str, resource_id: str, field_name: str, version: Optional[int] = None) -> None:
        """Delete a blob field from S3."""
        s3_key = self._build_s3_key(resource_type, resource_id, field_name, version)

        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as e:
            # Ignore if key doesn't exist
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

    def delete_all_blobs(self, resource_type: str, resource_id: str) -> int:
        """Delete all blobs for a resource.

        Returns:
            Number of blobs deleted
        """
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
