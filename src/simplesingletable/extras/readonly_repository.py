"""Read-only repository interface for safe data access.

This module provides read-only variants of the repository pattern that prevent
any modifications to the underlying resources. Useful for services and components
that should only have read access to data.

The read-only repositories:
- Expose only safe read operations (get, read, list)
- Hide all mutation methods (create, update, delete)
- Raise NotImplementedError if mutation methods are accessed
- Maintain full type safety and schema validation

Example:
    class User(DynamoDbResource):
        name: str
        email: str

    # Initialize read-only repository
    user_reader = ReadOnlyResourceRepository(
        ddb=memory,
        model_class=User
    )

    # Safe read operations
    user = user_reader.get(user_id)
    users = user_reader.list(limit=10)

    # Mutation operations are not available
    # user_reader.create(...) # This method doesn't exist
"""

import logging
from typing import Any, List, Optional, Type, TypeVar

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource

Resource = DynamoDbResource
VersionedResource = DynamoDbVersionedResource

T = TypeVar("T", Resource, VersionedResource)


class ReadOnlyResourceRepository:
    """Read-only repository providing safe read access to resources."""

    def __init__(
        self,
        ddb: DynamoDbMemory,
        model_class: Type[T],
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize a read-only repository.

        Args:
            ddb: DynamoDbMemory instance for database access
            model_class: The resource model class to work with
            logger: Optional logger instance
        """
        self.ddb = ddb
        self.model_class = model_class
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def get(self, id: Any) -> Optional[T]:
        """
        Retrieve a record by its identifier. Returns None if not found.

        Args:
            id: The resource identifier

        Returns:
            The resource instance or None
        """
        self.logger.debug(f"Fetching {self.model_class.__name__} with id: {id}")
        return self._get(id)

    def read(self, id: Any) -> T:
        """
        Retrieve a record by its identifier or raise an error if not found.

        Args:
            id: The resource identifier

        Returns:
            The resource instance

        Raises:
            ValueError: If the resource is not found
        """
        self.logger.debug(f"Reading {self.model_class.__name__} with id: {id}")
        obj = self.get(id)
        if obj is None:
            self.logger.error(f"{self.model_class.__name__} not found for id: {id}")
            raise ValueError(f"{self.model_class.__name__} with id {id} not found")
        return obj

    def list(self, limit: Optional[int] = None) -> List[T]:
        """
        List all records of this type, with optional limit.

        Args:
            limit: Optional maximum number of results

        Returns:
            List of resource instances
        """
        self.logger.debug(f"Listing {self.model_class.__name__} with limit={limit}")
        return self._list(limit)

    def _get(self, id: Any) -> Optional[T]:
        """Internal method to retrieve a resource."""
        return self.ddb.get_existing(id, self.model_class)

    def _list(self, limit: Optional[int]) -> List[T]:
        """Internal method to list resources."""
        result = self.ddb.list_type_by_updated_at(self.model_class, results_limit=limit)
        return result.as_list()
