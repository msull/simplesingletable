"""Read-only versioned repository with version management capabilities.

This module provides a read-only variant of the VersionedResourceRepository that
allows safe read access to versioned resources without modification capabilities.

The read-only versioned repository:
- Provides version listing and retrieval operations
- Prevents any modifications including version restoration
- Maintains full type safety and compatibility

Example:
    class Document(DynamoDbVersionedResource):
        title: str
        content: str

    # Initialize read-only versioned repository
    doc_reader = ReadOnlyVersionedResourceRepository(
        ddb=memory,
        model_class=Document
    )

    # Safe read and version operations
    doc = doc_reader.get(doc_id)
    versions = doc_reader.list_versions(doc_id)
    v1_doc = doc_reader.get_version(doc_id, 1)

    # Mutation operations are not available
    # doc_reader.create(...) # This method doesn't exist
    # doc_reader.restore_version(...) # This method doesn't exist
"""

import logging
from datetime import datetime
from typing import List, Optional, Type, TypeVar

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource

from .readonly_repository import ReadOnlyResourceRepository

VersionedResource = DynamoDbVersionedResource

T = TypeVar("T", bound=VersionedResource)


class VersionInfo(BaseModel):
    """Metadata about a specific version of a resource."""

    version_id: str
    version_number: int
    created_at: datetime
    updated_at: datetime
    is_latest: bool = False


class ReadOnlyVersionedResourceRepository(ReadOnlyResourceRepository):
    """Read-only repository for versioned resources with version querying capabilities."""

    def __init__(
        self,
        ddb: DynamoDbMemory,
        model_class: Type[T],
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize a read-only versioned repository.

        Args:
            ddb: DynamoDbMemory instance for database access
            model_class: The versioned resource model class to work with
            logger: Optional logger instance

        Raises:
            ValueError: If model_class is not a DynamoDbVersionedResource
        """
        if not issubclass(model_class, DynamoDbVersionedResource):
            raise ValueError(
                "ReadOnlyVersionedResourceRepository can only be used with DynamoDbVersionedResource models"
            )

        super().__init__(
            ddb=ddb,
            model_class=model_class,
            logger=logger,
        )

    def list_versions(self, item_id: str) -> List[VersionInfo]:
        """
        List all versions of a specific item.

        Args:
            item_id: The resource ID to list versions for

        Returns:
            List of VersionInfo objects containing metadata for each version
        """
        self.logger.debug(f"Listing versions for {self.model_class.__name__} with id: {item_id}")

        # Query all versions for this resource
        response = self.ddb.dynamodb_table.query(
            KeyConditionExpression=Key("pk").eq(f"{self.model_class.get_unique_key_prefix()}#{item_id}")
            & Key("sk").begins_with("v"),
            ProjectionExpression="sk, version, created_at, updated_at",
        )

        versions = []
        latest_version = 0

        # Process each version
        for item in response.get("Items", []):
            sk = item["sk"]
            if sk == "v0":
                # v0 is the latest version marker
                continue

            version_number = int(sk[1:])  # Extract number from "v1", "v2", etc.
            latest_version = max(latest_version, version_number)

            versions.append(
                VersionInfo(
                    version_id=sk,
                    version_number=version_number,
                    created_at=item.get("created_at", datetime.now()),
                    updated_at=item.get("updated_at", datetime.now()),
                    is_latest=False,  # Will be updated after we know the latest
                )
            )

        # Mark the latest version
        for version in versions:
            if version.version_number == latest_version:
                version.is_latest = True
                break

        # Sort by version number descending (newest first)
        versions.sort(key=lambda v: v.version_number, reverse=True)

        return versions

    def get_version(self, item_id: str, version: int) -> Optional[T]:
        """
        Retrieve a specific version of an item.

        Args:
            item_id: The resource ID
            version: The version number (e.g., 1, 2, 3)

        Returns:
            The specific version of the item, or None if not found

        Raises:
            ValueError: If version is not a positive integer
        """
        self.logger.debug(f"Getting version {version} of {self.model_class.__name__} with id: {item_id}")

        if version <= 0:
            raise ValueError(f"Version must be a positive integer, got: {version}")

        # Use the existing get_existing method with version parameter
        return self.ddb.get_existing(item_id, self.model_class, version=version)
