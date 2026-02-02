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
from typing import Any, Dict, List, Optional, Type, TypeVar

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource

from .cache import TTLCache

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
        cache_ttl_seconds: Optional[int] = None,
    ):
        """Initialize a read-only repository.

        Args:
            ddb: DynamoDbMemory instance for database access
            model_class: The resource model class to work with
            logger: Optional logger instance
            cache_ttl_seconds: Optional TTL for cache entries. When set and > 0, enables caching.
        """
        self.ddb = ddb
        self.model_class = model_class
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._cache: Optional[TTLCache] = (
            TTLCache(cache_ttl_seconds, copy_fn=lambda v: v.model_copy(deep=True))
            if cache_ttl_seconds and cache_ttl_seconds > 0
            else None
        )

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

    def batch_get(self, ids: list[str]) -> Dict[str, T]:
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

        results: Dict[str, T] = {}
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
        if self._cache:
            cached = self._cache.get(str(id))
            if cached is not None:
                return cached
        result = self.ddb.get_existing(id, self.model_class)
        if result is not None and self._cache:
            self._cache.put(str(id), result)
        return result

    def _list(self, limit: Optional[int]) -> List[T]:
        """Internal method to list resources."""
        result = self.ddb.list_type_by_updated_at(self.model_class, results_limit=limit)
        return result.as_list()
