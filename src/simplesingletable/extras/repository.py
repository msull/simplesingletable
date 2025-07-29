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

import logging
from typing import Any, Callable, List, Optional, Set, Type, TypeVar

from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource

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
        model_class: Type[T],
        create_schema_class: Type[CreateSchemaType],
        update_schema_class: Type[UpdateSchemaType],
        logger: Optional[logging.Logger] = None,
        default_create_obj_fn: Optional[Callable[[str], CreateSchemaType]] = None,
        override_id_fn: Optional[Callable[[CreateSchemaType], str]] = None,
    ):
        self.ddb = ddb
        self.model_class = model_class
        self.create_schema_class = create_schema_class
        self.update_schema_class = update_schema_class
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.default_create_object_fn = default_create_obj_fn
        self.override_id_fn = override_id_fn

    def create(self, obj_in: CreateSchemaType | dict, override_id: Optional[str] = None) -> T:
        """
        Create a new record using the create schema and return the model instance.
        """
        self.logger.debug(f"Creating {self.model_class.__name__} with: {obj_in}")
        if isinstance(obj_in, dict):
            self.logger.debug("Converting dict into to schema model")
            obj_in = self.create_schema_class.model_validate(obj_in)
        return self._create(obj_in, override_id)

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

    def get(self, id: Any) -> Optional[T]:
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

    def update(self, id_or_obj: Any, obj_in: UpdateSchemaType | dict, clear_fields: Optional[Set[str]] = None) -> T:
        """
        Update an existing record by its identifier with the update schema.

        Args:
            id_or_obj: Either the ID of the record to update or the record object itself
            obj_in: Update data (None values normally excluded)
            clear_fields: Set of field names to explicitly clear to None,
                         even if they are None in obj_in
        """
        if isinstance(id_or_obj, self.model_class):
            id_val = id_or_obj.resource_id
        else:
            id_val = id_or_obj
        self.logger.debug(f"Updating {self.model_class.__name__} id={id_val} with: {obj_in}")
        if clear_fields:
            self.logger.debug(f"Clear fields: {clear_fields}")
        if isinstance(obj_in, dict):
            self.logger.debug("Converting dict into to schema model")
            obj_in = self.update_schema_class.model_validate(obj_in)
        if isinstance(id_or_obj, self.model_class):
            existing = id_or_obj
        else:
            existing = self.read(id_or_obj)
        return self._update(existing, obj_in, clear_fields=clear_fields)

    def delete(self, id: Any) -> None:
        """
        Delete a record by its identifier.
        """
        self.logger.debug(f"Deleting {self.model_class.__name__} with id: {id}")
        obj = self.read(id)
        return self._delete(obj)

    def list(self, limit: Optional[int] = None) -> List[T]:
        """
        List all records of this type, with optional limit.
        """
        self.logger.debug(f"Listing {self.model_class.__name__} with limit={limit}")
        return self._list(limit)

    def _create(self, obj_in: CreateSchemaType, override_id: Optional[str] = None) -> T:
        if override_id:
            final_override_id = override_id
        elif self.override_id_fn:
            final_override_id = self.override_id_fn(obj_in)
        else:
            final_override_id = None
        return self.ddb.create_new(self.model_class, obj_in, override_id=final_override_id)

    def _get(self, id: Any) -> Optional[T]:
        return self.ddb.get_existing(id, self.model_class)

    def _update(self, existing_obj: T, obj_in: UpdateSchemaType, clear_fields: Optional[Set[str]] = None) -> T:
        return self.ddb.update_existing(existing_obj, obj_in, clear_fields=clear_fields)

    def _delete(self, obj: T) -> None:
        self.ddb.delete_existing(obj)

    def _list(self, limit: Optional[int]) -> List[T]:
        result = self.ddb.list_type_by_updated_at(self.model_class, results_limit=limit)
        return result.as_list()
