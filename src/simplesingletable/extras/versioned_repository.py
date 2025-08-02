"""Versioned repository implementation with version management capabilities.

This module extends the ResourceRepository to provide version-specific operations
for DynamoDbVersionedResource models, including listing versions, retrieving
specific versions, and restoring previous versions.

Example:
    class Document(DynamoDbVersionedResource):
        title: str
        content: str

    class CreateDocumentSchema(BaseModel):
        title: str
        content: str

    class UpdateDocumentSchema(BaseModel):
        title: Optional[str] = None
        content: Optional[str] = None

    # Initialize versioned repository
    doc_repo = VersionedResourceRepository(
        ddb=memory,
        model_class=Document,
        create_schema_class=CreateDocumentSchema,
        update_schema_class=UpdateDocumentSchema
    )

    # Use versioning features
    doc = doc_repo.create({"title": "My Doc", "content": "Version 1"})
    doc = doc_repo.update(doc.resource_id, {"content": "Version 2"})
    
    # List all versions
    versions = doc_repo.list_versions(doc.resource_id)
    
    # Get specific version
    v1_doc = doc_repo.get_version(doc.resource_id, 1)
    
    # Restore previous version
    restored_doc = doc_repo.restore_version(doc.resource_id, 1)
"""

import logging
from datetime import datetime
from typing import List, Optional, Type, TypeVar

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel

from .. import DynamoDbMemory, DynamoDbVersionedResource
from .repository import ResourceRepository

CreateSchema = BaseModel
UpdateSchema = BaseModel
VersionedResource = DynamoDbVersionedResource

T = TypeVar("T", bound=VersionedResource)
CreateSchemaType = TypeVar("CreateSchemaType", bound=CreateSchema)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=UpdateSchema)


class VersionInfo(BaseModel):
    """Metadata about a specific version of a resource."""

    version_id: str
    version_number: int
    created_at: datetime
    updated_at: datetime
    is_latest: bool = False


class VersionedResourceRepository(ResourceRepository):
    """Repository for versioned resources with version management capabilities."""

    def __init__(
        self,
        ddb: DynamoDbMemory,
        model_class: Type[T],
        create_schema_class: Type[CreateSchemaType],
        update_schema_class: Type[UpdateSchemaType],
        logger: Optional[logging.Logger] = None,
        default_create_obj_fn=None,
        override_id_fn=None,
    ):
        if not issubclass(model_class, DynamoDbVersionedResource):
            raise ValueError("VersionedResourceRepository can only be used with DynamoDbVersionedResource models")

        super().__init__(
            ddb=ddb,
            model_class=model_class,
            create_schema_class=create_schema_class,
            update_schema_class=update_schema_class,
            logger=logger,
            default_create_obj_fn=default_create_obj_fn,
            override_id_fn=override_id_fn,
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
        """
        self.logger.debug(f"Getting version {version} of {self.model_class.__name__} with id: {item_id}")

        if version <= 0:
            raise ValueError(f"Version must be a positive integer, got: {version}")

        # Use the existing get_existing method with version parameter
        return self.ddb.get_existing(item_id, self.model_class, version=version)

    def restore_version(self, item_id: str, version: int) -> T:
        """
        Restore a previous version by creating a new version with the same content.

        This doesn't actually rollback; it creates a new version that is identical
        to the specified older version.

        Args:
            item_id: The resource ID
            version: The version number to restore (e.g., 1, 2, 3)

        Returns:
            The newly created item that matches the restored version
        """
        self.logger.debug(f"Restoring version {version} of {self.model_class.__name__} with id: {item_id}")

        # Get the version to restore
        version_to_restore = self.get_version(item_id, version)
        if not version_to_restore:
            raise ValueError(f"Version {version} not found for item {item_id}")

        # Get the current latest version to ensure we're creating a new one
        current = self.get(item_id)
        if not current:
            raise ValueError(f"Item {item_id} not found")

        # Create update data from the old version, excluding system fields
        update_data = version_to_restore.model_dump(exclude={"resource_id", "version", "created_at", "updated_at"})

        # Update the current item with the old version's data
        # This will create a new version automatically
        restored_item = self.update(current, update_data)

        self.logger.info(
            f"Restored {self.model_class.__name__} {item_id} from version {version} "
            f"as new version {restored_item.version}"
        )

        return restored_item
