"""Simple TTL cache for repository-level caching.

Provides an opt-in, stdlib-only TTL cache that uses time.monotonic()
for expiration (immune to clock changes). Lazy eviction on access,
no background threads. Not thread-safe (consistent with rest of library).
"""

import copy
import time
from typing import Any, Callable, Optional


class TTLCache:
    """A simple TTL-based cache with lazy eviction.

    Values are defensively copied on both put and get to prevent callers
    from mutating cached state. By default uses ``copy.copy``; pass a
    custom ``copy_fn`` for richer objects (e.g. Pydantic ``model_copy``).

    Args:
        ttl_seconds: Time-to-live for cache entries in seconds.
        copy_fn: Optional function used to copy values on put/get.
                 Defaults to ``copy.copy``.

    Example:
        cache = TTLCache(ttl_seconds=60)
        cache.put("key1", some_resource)
        result = cache.get("key1")  # Returns a *copy*, or None if expired
    """

    def __init__(self, ttl_seconds: int, copy_fn: Optional[Callable[[Any], Any]] = None):
        self._ttl_seconds = ttl_seconds
        self._copy_fn: Callable[[Any], Any] = copy_fn or copy.copy
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get a copy of the value by key. Returns None if missing or expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            del self._store[key]
            return None
        return self._copy_fn(value)

    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """Get copies of multiple values by key. Only returns found, non-expired entries."""
        result = {}
        now = time.monotonic()
        for key in keys:
            entry = self._store.get(key)
            if entry is None:
                continue
            expires_at, value = entry
            if now >= expires_at:
                del self._store[key]
            else:
                result[key] = self._copy_fn(value)
        return result

    def put(self, key: str, value: Any) -> None:
        """Store a defensive copy of the value with TTL."""
        self._store[key] = (time.monotonic() + self._ttl_seconds, self._copy_fn(value))

    def put_many(self, items: dict[str, Any]) -> None:
        """Store defensive copies of multiple values with TTL."""
        expires_at = time.monotonic() + self._ttl_seconds
        for key, value in items.items():
            self._store[key] = (expires_at, self._copy_fn(value))

    def invalidate(self, key: str) -> None:
        """Remove a specific key from the cache."""
        self._store.pop(key, None)

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._store.clear()
