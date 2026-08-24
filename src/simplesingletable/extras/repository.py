"""A simplified repository interface for DynamoDB operations.

This module provides a generic repository pattern implementation that wraps
the DynamoDbMemory class to offer a simplified CRUD interface. It supports
both versioned and non-versioned resources with customizable create/update schemas.

The ResourceRepository class provides:
- Type-safe CRUD operations with Pydantic schema validation
- Support for both versioned and non-versioned DynamoDB resources
- Flexible ID generation with optional override functions
- Default object creation with customizable factory functions
- Comprehensive logging for debugging and monitoring

Example:
    class User(DynamoDbResource):
        name: str
        email: str

    class CreateUserSchema(BaseModel):
        name: str
        email: str

    class UpdateUserSchema(BaseModel):
        name: Optional[str] = None
        email: Optional[str] = None

    # Initialize repository
    user_repo = ResourceRepository(
        ddb=memory,
        model_class=User,
        create_schema_class=CreateUserSchema,
        update_schema_class=UpdateUserSchema
    )

    # Use the repository
    user = user_repo.create({"name": "John", "email": "john@example.com"})
    updated_user = user_repo.update(user.resource_id, {"name": "Jane"})
    found_user = user_repo.get(user.resource_id)
    users = user_repo.list(limit=10)
"""

import builtins
import logging
from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.exceptions import VersionConflictError

from .cache import TTLCache

# helper names
CreateSchema = BaseModel
UpdateSchema = BaseModel
Resource = DynamoDbResource
VersionedResource = DynamoDbVersionedResource

T = TypeVar("T", Resource, VersionedResource)  # The model type (e.g., User)
CreateSchemaType = TypeVar("CreateSchemaType", bound=CreateSchema)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=UpdateSchema)


class ResourceRepository:
    def __init__(
        self,
        ddb: DynamoDbMemory,
        model_class: type[T],
        create_schema_class: type[CreateSchemaType],
        update_schema_class: type[UpdateSchemaType],
        logger: logging.Logger | None = None,
        default_create_obj_fn: Callable[[str], CreateSchemaType] | None = None,
        override_id_fn: Callable[[CreateSchemaType], str] | None = None,
        cache_ttl_seconds: int | None = None,
    ):
        self.ddb = ddb
        self.model_class = model_class
        self.create_schema_class = create_schema_class
        self.update_schema_class = update_schema_class
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.default_create_object_fn = default_create_obj_fn
        self.override_id_fn = override_id_fn
        self._cache: TTLCache | None = (
            TTLCache(cache_ttl_seconds, copy_fn=lambda v: v.model_copy(deep=True))
            if cache_ttl_seconds and cache_ttl_seconds > 0
            else None
        )

    def create(
        self,
        obj_in: CreateSchemaType | dict,
        override_id: str | None = None,
        changed_by: str | None = None,
        audit_metadata: dict | None = None,
    ) -> T:
        """
        Create a new record using the create schema and return the model instance.

        Args:
            obj_in: The create schema or dict to create the record from
            override_id: Optional ID to use instead of auto-generated
            changed_by: Optional identifier of user/service making the change (for audit logging)
            audit_metadata: Optional additional metadata to include in audit log
        """
        self.logger.debug(f"Creating {self.model_class.__name__}")
        if isinstance(obj_in, dict):
            self.logger.debug("Converting dict into to schema model")
            obj_in = self.create_schema_class.model_validate(obj_in)
        return self._create(obj_in, override_id, changed_by=changed_by, audit_metadata=audit_metadata)

    def get_or_create(self, id: Any) -> T:
        """
        Retrieve a record by its identifier, or creates a new record
        """
        if existing := self.get(id):
            return existing
        self.logger.debug(f"No record found for {self.model_class.__name__} with id: {id}; creating new record")
        if self.default_create_object_fn is not None:
            self.logger.debug("Using default create object function to create a new record")
            obj_in = self.default_create_object_fn(id)
            return self.create(obj_in, override_id=id)
        else:
            self.logger.debug("Creating a new record using the default schema")
            obj_in = self.create_schema_class()
            return self.create(obj_in, override_id=id)

    def get(self, id: Any) -> T | None:
        """
        Retrieve a record by its identifier. Returns None if not found.
        """
        self.logger.debug(f"Fetching {self.model_class.__name__} with id: {id}")
        return self._get(id)

    def read(self, id: Any) -> T:
        """
        Retrieve a record by its identifier or raise an error if not found.
        """
        self.logger.debug(f"Reading {self.model_class.__name__} with id: {id}")
        obj = self.get(id)
        if obj is None:
            self.logger.error(f"{self.model_class.__name__} not found for id: {id}")
            raise ValueError(f"{self.model_class.__name__} with id {id} not found")
        return obj

    def update(
        self,
        id_or_obj: Any,
        obj_in: UpdateSchemaType | dict,
        clear_fields: set[str] | None = None,
        changed_by: str | None = None,
        audit_metadata: dict | None = None,
    ) -> T:
        """
        Update an existing record by its identifier with the update schema.

        Args:
            id_or_obj: Either the ID of the record to update or the record object itself
            obj_in: Update data (None values normally excluded)
            clear_fields: Set of field names to explicitly clear to None,
                         even if they are None in obj_in
            changed_by: Optional identifier of user/service making the change (for audit logging)
            audit_metadata: Optional additional metadata to include in audit log
        """
        if isinstance(id_or_obj, self.model_class):
            id_val = id_or_obj.resource_id
        else:
            id_val = id_or_obj
        self.logger.debug(f"Updating {self.model_class.__name__} id={id_val}")
        if clear_fields:
            self.logger.debug(f"Clear fields: {clear_fields}")
        if isinstance(obj_in, dict):
            self.logger.debug("Converting dict into to schema model")
            obj_in = self.update_schema_class.model_validate(obj_in)
        if isinstance(id_or_obj, self.model_class):
            existing = id_or_obj
        else:
            existing = self.read(id_or_obj)
        return self._update(
            existing, obj_in, clear_fields=clear_fields, changed_by=changed_by, audit_metadata=audit_metadata
        )

    def save(
        self,
        id: Any,
        obj_in: CreateSchemaType | UpdateSchemaType | dict,
        expected_version: int,
        clear_fields: set[str] | None = None,
        changed_by: str | None = None,
        audit_metadata: dict | None = None,
    ) -> T:
        """Write a record, failing if it is not at exactly ``expected_version``.

        This is the "create or update, but only from the state I read" primitive.
        ``expected_version=0`` means *must not exist yet* and validates ``obj_in``
        against the create schema; any higher value means *must currently be at that
        version* and validates against the update schema. Either way a lost race
        raises a :class:`ConflictError` subclass, so the caller writes one
        ``except`` instead of branching on create-vs-update and matching message text::

            try:
                doc = repo.save(doc_id, changes, expected_version=known_version)
            except ConflictError:
                ...  # re-read and retry, or return HTTP 409

        The read this performs is only for the pre-image and to fail early: the write
        itself is conditioned on the stored version, so a writer that slips in between
        the read and the write is still caught. That is what makes this safe where a
        hand-rolled ``get`` then ``update`` is not.

        Args:
            id: Resource id to write. Required even on create -- ``save`` is for
                callers who already know the identity of the thing they are writing.
            obj_in: Create schema (``expected_version=0``) or update schema.
            expected_version: ``0`` to create; otherwise the version the caller read.
            clear_fields: Fields to explicitly clear to None; update only.
            changed_by: Optional identifier of user/service making the change.
            audit_metadata: Optional additional metadata for the audit log.

        Raises:
            TypeError: If this repository's model is not a versioned resource.
            ValueError: If ``expected_version`` is negative.
            ResourceExistsError: ``expected_version=0`` but the resource exists.
            VersionConflictError: The resource is absent, or at a different version.
        """
        if not issubclass(self.model_class, DynamoDbVersionedResource):
            raise TypeError(
                f"save() requires a versioned resource; {self.model_class.__name__} is not a "
                "DynamoDbVersionedResource and has no version to condition on. Use create()/update()."
            )
        if expected_version < 0:
            raise ValueError(f"expected_version must be 0 (create) or a positive version, got {expected_version}")

        if expected_version == 0:
            self.logger.debug(f"Saving new {self.model_class.__name__} id={id}")
            if isinstance(obj_in, dict):
                obj_in = self.create_schema_class.model_validate(obj_in)
            return self._create(obj_in, override_id=id, changed_by=changed_by, audit_metadata=audit_metadata)

        self.logger.debug(f"Saving {self.model_class.__name__} id={id} from version {expected_version}")
        if isinstance(obj_in, dict):
            obj_in = self.update_schema_class.model_validate(obj_in)

        # Deliberately bypasses the repository cache: a cached pre-image could be
        # stale, which would turn the guard into a rubber stamp.
        existing = self.ddb.get_existing(id, self.model_class)
        if existing is None or existing.version != expected_version:
            if self._cache:
                self._cache.invalidate(str(id))
            raise VersionConflictError(
                f"{self.model_class.__name__} {id} is not at version {expected_version}"
                + ("; it does not exist" if existing is None else f"; it is at version {existing.version}"),
                resource_type=self.model_class.__name__,
                resource_id=str(id),
                expected_version=expected_version,
                actual_version=None if existing is None else existing.version,
            )
        try:
            return self._update(
                existing, obj_in, clear_fields=clear_fields, changed_by=changed_by, audit_metadata=audit_metadata
            )
        except VersionConflictError:
            if self._cache:
                self._cache.invalidate(str(id))
            raise

    def delete(self, id: Any, changed_by: str | None = None, audit_metadata: dict | None = None) -> None:
        """
        Delete a record by its identifier.

        Args:
            id: The ID of the record to delete
            changed_by: Optional identifier of user/service making the change (for audit logging)
            audit_metadata: Optional additional metadata to include in audit log
        """
        self.logger.debug(f"Deleting {self.model_class.__name__} with id: {id}")
        obj = self.read(id)
        return self._delete(obj, changed_by=changed_by, audit_metadata=audit_metadata)

    def batch_get(self, ids: list[str]) -> dict[str, T]:
        """
        Retrieve multiple records by their identifiers. Returns only found items.

        Uses cache for hits when caching is enabled, and only fetches missing
        IDs from the database.

        Args:
            ids: List of resource IDs to fetch

        Returns:
            Dict mapping resource_id -> resource for found items only.
        """
        self.logger.debug(f"Batch getting {self.model_class.__name__} with {len(ids)} ids")
        if not ids:
            return {}

        results: dict[str, T] = {}
        ids_to_fetch: list[str] = []

        if self._cache:
            cached = self._cache.get_many(ids)
            results.update(cached)
            ids_to_fetch = [rid for rid in ids if rid not in cached]
        else:
            ids_to_fetch = list(ids)

        if ids_to_fetch:
            fetched = self.ddb.batch_get_existing(ids_to_fetch, self.model_class)
            results.update(fetched)
            if self._cache and fetched:
                self._cache.put_many(fetched)

        return results

    def clear_cache(self) -> None:
        """Clear the repository cache."""
        if self._cache:
            self._cache.clear()

    def list(self, limit: int | None = None) -> list[T]:
        """
        List all records of this type, with optional limit.
        """
        self.logger.debug(f"Listing {self.model_class.__name__} with limit={limit}")
        return self._list(limit)

    def _create(
        self,
        obj_in: CreateSchemaType,
        override_id: str | None = None,
        changed_by: str | None = None,
        audit_metadata: dict | None = None,
    ) -> T:
        if override_id:
            final_override_id = override_id
        elif self.override_id_fn:
            final_override_id = self.override_id_fn(obj_in)
        else:
            final_override_id = None
        result = self.ddb.create_new(
            self.model_class,
            obj_in,
            override_id=final_override_id,
            changed_by=changed_by,
            audit_metadata=audit_metadata,
        )
        if self._cache:
            self._cache.put(str(result.resource_id), result)
        return result

    def _get(self, id: Any) -> T | None:
        if self._cache:
            cached = self._cache.get(str(id))
            if cached is not None:
                return cached
        result = self.ddb.get_existing(id, self.model_class)
        if result is not None and self._cache:
            self._cache.put(str(id), result)
        return result

    def _update(
        self,
        existing_obj: T,
        obj_in: UpdateSchemaType,
        clear_fields: set[str] | None = None,
        changed_by: str | None = None,
        audit_metadata: dict | None = None,
    ) -> T:
        result = self.ddb.update_existing(
            existing_obj,
            obj_in,
            clear_fields=clear_fields,
            changed_by=changed_by,
            audit_metadata=audit_metadata,
        )
        if self._cache:
            self._cache.put(str(result.resource_id), result)
        return result

    def _delete(self, obj: T, changed_by: str | None = None, audit_metadata: dict | None = None) -> None:
        self.ddb.delete_existing(obj, changed_by=changed_by, audit_metadata=audit_metadata)
        if self._cache:
            self._cache.invalidate(str(obj.resource_id))

    def _list(self, limit: int | None) -> builtins.list[T]:
        result = self.ddb.list_type_by_updated_at(self.model_class, results_limit=limit)
        return result.as_list()
