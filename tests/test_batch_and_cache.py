"""Tests for batch_get_existing and repository-level caching."""

import time
from typing import ClassVar, Optional
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.cache import TTLCache
from simplesingletable.extras.readonly_repository import ReadOnlyResourceRepository
from simplesingletable.extras.readonly_versioned_repository import ReadOnlyVersionedResourceRepository
from simplesingletable.extras.repository import ResourceRepository
from simplesingletable.extras.versioned_repository import VersionedResourceRepository


# --- Test resource classes ---


class Widget(DynamoDbResource):
    name: str
    color: Optional[str] = None


class CreateWidgetSchema(BaseModel):
    name: str
    color: Optional[str] = None


class UpdateWidgetSchema(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None


class VersionedNote(DynamoDbVersionedResource):
    title: str
    body: str


class CreateNoteSchema(BaseModel):
    title: str
    body: str


class UpdateNoteSchema(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None


# --- Fixtures ---


@pytest.fixture
def widget_repo(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=Widget,
        create_schema_class=CreateWidgetSchema,
        update_schema_class=UpdateWidgetSchema,
    )


@pytest.fixture
def cached_widget_repo(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=Widget,
        create_schema_class=CreateWidgetSchema,
        update_schema_class=UpdateWidgetSchema,
        cache_ttl_seconds=300,
    )


@pytest.fixture
def note_repo(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=VersionedNote,
        create_schema_class=CreateNoteSchema,
        update_schema_class=UpdateNoteSchema,
    )


@pytest.fixture
def cached_note_repo(dynamodb_memory: DynamoDbMemory):
    return VersionedResourceRepository(
        ddb=dynamodb_memory,
        model_class=VersionedNote,
        create_schema_class=CreateNoteSchema,
        update_schema_class=UpdateNoteSchema,
        cache_ttl_seconds=300,
    )


@pytest.fixture
def readonly_widget_repo(dynamodb_memory: DynamoDbMemory):
    return ReadOnlyResourceRepository(
        ddb=dynamodb_memory,
        model_class=Widget,
        cache_ttl_seconds=300,
    )


@pytest.fixture
def readonly_note_repo(dynamodb_memory: DynamoDbMemory):
    return ReadOnlyVersionedResourceRepository(
        ddb=dynamodb_memory,
        model_class=VersionedNote,
        cache_ttl_seconds=300,
    )


# ===========================================================================
# TTLCache unit tests (no DynamoDB needed)
# ===========================================================================


class TestTTLCache:
    def test_put_and_get(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put("k1", "v1")
        assert cache.get("k1") == "v1"

    def test_get_missing_key(self):
        cache = TTLCache(ttl_seconds=60)
        assert cache.get("missing") is None

    def test_expiration(self):
        cache = TTLCache(ttl_seconds=1)
        cache.put("k1", "v1")
        assert cache.get("k1") == "v1"
        time.sleep(1.1)
        assert cache.get("k1") is None

    def test_get_many(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        result = cache.get_many(["a", "b", "missing"])
        assert result == {"a": 1, "b": 2}

    def test_get_many_with_expired(self):
        cache = TTLCache(ttl_seconds=1)
        cache.put("a", 1)
        time.sleep(1.1)
        cache.put("b", 2)
        result = cache.get_many(["a", "b"])
        assert result == {"b": 2}

    def test_put_many(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put_many({"x": 10, "y": 20})
        assert cache.get("x") == 10
        assert cache.get("y") == 20

    def test_invalidate(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put("k1", "v1")
        cache.invalidate("k1")
        assert cache.get("k1") is None

    def test_invalidate_missing_key_no_error(self):
        cache = TTLCache(ttl_seconds=60)
        cache.invalidate("nope")  # should not raise

    def test_clear(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.clear()
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_overwrite(self):
        cache = TTLCache(ttl_seconds=60)
        cache.put("k", "old")
        cache.put("k", "new")
        assert cache.get("k") == "new"


# ===========================================================================
# batch_get_existing on DynamoDbMemory
# ===========================================================================


class TestBatchGetExistingNonVersioned:
    def test_batch_get_nonversioned(self, dynamodb_memory: DynamoDbMemory):
        w1 = dynamodb_memory.create_new(Widget, {"name": "w1", "color": "red"})
        w2 = dynamodb_memory.create_new(Widget, {"name": "w2", "color": "blue"})

        result = dynamodb_memory.batch_get_existing(
            [w1.resource_id, w2.resource_id], Widget
        )
        assert len(result) == 2
        assert result[w1.resource_id].name == "w1"
        assert result[w2.resource_id].name == "w2"

    def test_missing_ids_absent(self, dynamodb_memory: DynamoDbMemory):
        w1 = dynamodb_memory.create_new(Widget, {"name": "exists"})
        result = dynamodb_memory.batch_get_existing(
            [w1.resource_id, "does-not-exist"], Widget
        )
        assert len(result) == 1
        assert w1.resource_id in result
        assert "does-not-exist" not in result

    def test_empty_list_returns_empty_dict(self, dynamodb_memory: DynamoDbMemory):
        result = dynamodb_memory.batch_get_existing([], Widget)
        assert result == {}

    def test_deduplication(self, dynamodb_memory: DynamoDbMemory):
        w1 = dynamodb_memory.create_new(Widget, {"name": "dup"})
        result = dynamodb_memory.batch_get_existing(
            [w1.resource_id, w1.resource_id, w1.resource_id], Widget
        )
        assert len(result) == 1
        assert result[w1.resource_id].name == "dup"


class TestBatchGetExistingVersioned:
    def test_batch_get_versioned(self, dynamodb_memory: DynamoDbMemory):
        n1 = dynamodb_memory.create_new(VersionedNote, {"title": "n1", "body": "b1"})
        n2 = dynamodb_memory.create_new(VersionedNote, {"title": "n2", "body": "b2"})

        result = dynamodb_memory.batch_get_existing(
            [n1.resource_id, n2.resource_id], VersionedNote
        )
        assert len(result) == 2
        assert result[n1.resource_id].title == "n1"
        assert result[n2.resource_id].title == "n2"

    def test_batch_get_versioned_returns_current_version(self, dynamodb_memory: DynamoDbMemory):
        n = dynamodb_memory.create_new(VersionedNote, {"title": "t", "body": "v1"})
        n = dynamodb_memory.update_existing(n, {"body": "v2"})

        result = dynamodb_memory.batch_get_existing([n.resource_id], VersionedNote)
        assert result[n.resource_id].body == "v2"
        assert result[n.resource_id].version == 2


class TestBatchGetAutoChunking:
    def test_auto_chunking_over_100(self, dynamodb_memory: DynamoDbMemory):
        """Create >100 items and batch-get them all in one call."""
        ids = []
        for i in range(105):
            w = dynamodb_memory.create_new(Widget, {"name": f"w{i}"})
            ids.append(w.resource_id)

        result = dynamodb_memory.batch_get_existing(ids, Widget)
        assert len(result) == 105


# ===========================================================================
# Repository batch_get
# ===========================================================================


class TestRepositoryBatchGet:
    def test_batch_get_no_cache(self, widget_repo):
        w1 = widget_repo.create({"name": "a"})
        w2 = widget_repo.create({"name": "b"})
        result = widget_repo.batch_get([w1.resource_id, w2.resource_id])
        assert len(result) == 2

    def test_batch_get_empty(self, widget_repo):
        assert widget_repo.batch_get([]) == {}


class TestReadOnlyRepositoryBatchGet:
    def test_batch_get(self, dynamodb_memory, readonly_widget_repo):
        w1 = dynamodb_memory.create_new(Widget, {"name": "ro1"})
        w2 = dynamodb_memory.create_new(Widget, {"name": "ro2"})

        result = readonly_widget_repo.batch_get([w1.resource_id, w2.resource_id])
        assert len(result) == 2
        assert result[w1.resource_id].name == "ro1"

    def test_batch_get_versioned(self, dynamodb_memory, readonly_note_repo):
        n1 = dynamodb_memory.create_new(VersionedNote, {"title": "t1", "body": "b1"})
        result = readonly_note_repo.batch_get([n1.resource_id])
        assert len(result) == 1
        assert result[n1.resource_id].title == "t1"


# ===========================================================================
# Repository-level caching
# ===========================================================================


class TestCacheIntegration:
    def test_cache_disabled_by_default(self, widget_repo):
        """Default repo has no cache overhead."""
        assert widget_repo._cache is None

    def test_cache_enabled(self, cached_widget_repo):
        assert cached_widget_repo._cache is not None

    def test_get_populates_cache(self, cached_widget_repo):
        w = cached_widget_repo.create({"name": "cached"})
        # Clear cache to simulate cold start
        cached_widget_repo.clear_cache()

        # First get -> cache miss, fetches from DDB
        result = cached_widget_repo.get(w.resource_id)
        assert result is not None
        assert result.name == "cached"

        # Second get -> cache hit (verify by checking cache directly)
        cached = cached_widget_repo._cache.get(w.resource_id)
        assert cached is not None
        assert cached.name == "cached"

    def test_create_populates_cache(self, cached_widget_repo):
        w = cached_widget_repo.create({"name": "new"})
        cached = cached_widget_repo._cache.get(w.resource_id)
        assert cached is not None
        assert cached.name == "new"

    def test_update_updates_cache(self, cached_widget_repo):
        w = cached_widget_repo.create({"name": "old"})
        cached_widget_repo.update(w.resource_id, {"name": "new"})
        cached = cached_widget_repo._cache.get(w.resource_id)
        assert cached is not None
        assert cached.name == "new"

    def test_delete_invalidates_cache(self, cached_widget_repo):
        w = cached_widget_repo.create({"name": "to_delete"})
        assert cached_widget_repo._cache.get(w.resource_id) is not None

        cached_widget_repo.delete(w.resource_id)
        assert cached_widget_repo._cache.get(w.resource_id) is None

    def test_clear_cache(self, cached_widget_repo):
        cached_widget_repo.create({"name": "a"})
        cached_widget_repo.create({"name": "b"})
        cached_widget_repo.clear_cache()
        # After clear, cache should be empty (we can't easily check size,
        # but a get on a known key should return None)
        assert cached_widget_repo._cache.get("any") is None

    def test_batch_get_partial_cache_hits(self, cached_widget_repo, dynamodb_memory):
        """batch_get should use cache for hits and only fetch missing from DDB."""
        w1 = cached_widget_repo.create({"name": "w1"})
        w2 = cached_widget_repo.create({"name": "w2"})
        w3 = cached_widget_repo.create({"name": "w3"})

        # Warm only w1 in cache (w2, w3 evicted by clear + re-get)
        cached_widget_repo.clear_cache()
        cached_widget_repo.get(w1.resource_id)  # populates w1 in cache

        # batch_get all three — w1 from cache, w2+w3 from DDB
        result = cached_widget_repo.batch_get(
            [w1.resource_id, w2.resource_id, w3.resource_id]
        )
        assert len(result) == 3
        assert result[w1.resource_id].name == "w1"
        assert result[w2.resource_id].name == "w2"
        assert result[w3.resource_id].name == "w3"

        # Now w2 and w3 should also be in cache
        assert cached_widget_repo._cache.get(w2.resource_id) is not None
        assert cached_widget_repo._cache.get(w3.resource_id) is not None


class TestCacheWithVersionedRepo:
    def test_versioned_cache(self, cached_note_repo):
        n = cached_note_repo.create({"title": "t", "body": "v1"})
        assert cached_note_repo._cache.get(n.resource_id) is not None

        updated = cached_note_repo.update(n.resource_id, {"body": "v2"})
        cached = cached_note_repo._cache.get(updated.resource_id)
        assert cached is not None
        assert cached.body == "v2"
        assert cached.version == 2

    def test_versioned_batch_get(self, cached_note_repo):
        n1 = cached_note_repo.create({"title": "a", "body": "b1"})
        n2 = cached_note_repo.create({"title": "b", "body": "b2"})
        cached_note_repo.clear_cache()

        result = cached_note_repo.batch_get([n1.resource_id, n2.resource_id])
        assert len(result) == 2


class TestReadOnlyCacheIntegration:
    def test_readonly_cache_on_get(self, dynamodb_memory, readonly_widget_repo):
        w = dynamodb_memory.create_new(Widget, {"name": "ro_cached"})

        result = readonly_widget_repo.get(w.resource_id)
        assert result.name == "ro_cached"

        # Should be cached
        cached = readonly_widget_repo._cache.get(w.resource_id)
        assert cached is not None

    def test_readonly_clear_cache(self, dynamodb_memory, readonly_widget_repo):
        w = dynamodb_memory.create_new(Widget, {"name": "to_clear"})
        readonly_widget_repo.get(w.resource_id)
        readonly_widget_repo.clear_cache()
        assert readonly_widget_repo._cache.get(w.resource_id) is None

    def test_readonly_batch_get_uses_cache(self, dynamodb_memory, readonly_widget_repo):
        w1 = dynamodb_memory.create_new(Widget, {"name": "rw1"})
        w2 = dynamodb_memory.create_new(Widget, {"name": "rw2"})

        # Warm w1
        readonly_widget_repo.get(w1.resource_id)

        result = readonly_widget_repo.batch_get([w1.resource_id, w2.resource_id])
        assert len(result) == 2

    def test_clear_cache_no_cache_configured(self, widget_repo):
        """clear_cache should not raise when cache is not configured."""
        widget_repo.clear_cache()  # should be a no-op


class TestCacheMutationIsolation:
    def test_mutating_get_result_does_not_corrupt_cache(self, cached_widget_repo):
        """Modifying a resource returned by get() must not affect the cached copy."""
        w = cached_widget_repo.create({"name": "immutable"})

        first = cached_widget_repo.get(w.resource_id)
        first.name = "MUTATED"

        second = cached_widget_repo.get(w.resource_id)
        assert second.name == "immutable"

    def test_mutating_batch_get_result_does_not_corrupt_cache(self, cached_widget_repo):
        """Modifying a resource from batch_get() must not affect the cached copy."""
        w = cached_widget_repo.create({"name": "safe"})
        cached_widget_repo.clear_cache()

        result = cached_widget_repo.batch_get([w.resource_id])
        result[w.resource_id].name = "MUTATED"

        second = cached_widget_repo.get(w.resource_id)
        assert second.name == "safe"

    def test_mutating_created_resource_does_not_corrupt_cache(self, cached_widget_repo):
        """Modifying the object returned by create() must not affect the cached copy."""
        w = cached_widget_repo.create({"name": "fresh"})
        w.name = "MUTATED"

        from_cache = cached_widget_repo.get(w.resource_id)
        assert from_cache.name == "fresh"

    def test_ttl_cache_unit_mutation_isolation(self):
        """Pure unit test: TTLCache copies prevent caller mutation of stored values."""
        from simplesingletable.extras.cache import TTLCache

        class Obj:
            def __init__(self, val):
                self.val = val

        cache = TTLCache(ttl_seconds=60)
        cache.put("k", Obj(1))

        retrieved = cache.get("k")
        retrieved.val = 999

        assert cache.get("k").val == 1

    def test_ttl_cache_put_copies_input(self):
        """Mutating the original object after put() must not affect the cache."""
        from simplesingletable.extras.cache import TTLCache

        class Obj:
            def __init__(self, val):
                self.val = val

        cache = TTLCache(ttl_seconds=60)
        obj = Obj(1)
        cache.put("k", obj)

        obj.val = 999
        assert cache.get("k").val == 1


class TestCacheTTLExpiration:
    def test_cache_expires(self, dynamodb_memory):
        repo = ResourceRepository(
            ddb=dynamodb_memory,
            model_class=Widget,
            create_schema_class=CreateWidgetSchema,
            update_schema_class=UpdateWidgetSchema,
            cache_ttl_seconds=1,
        )
        w = repo.create({"name": "short_lived"})
        assert repo._cache.get(w.resource_id) is not None

        time.sleep(1.1)
        assert repo._cache.get(w.resource_id) is None

        # get() should still work (re-fetches from DDB)
        result = repo.get(w.resource_id)
        assert result is not None
        assert result.name == "short_lived"
