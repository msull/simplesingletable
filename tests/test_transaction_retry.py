"""Tests for issue #8 P2: transaction retry behavior.

P2a — implicit-condition retries must invalidate stale cached pre-images
(``op.current`` / snapshot ``read_cache``) before rebuilding, otherwise every
retry re-derives the same stale version number and is guaranteed to fail.

P2b — transient failures (TransactionConflict, throttling,
TransactionInProgressException) are retried with backoff instead of raising
immediately.
"""

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource


class RetryDoc(DynamoDbVersionedResource):
    title: str
    body: str = ""


def test_retry_with_stale_current_re_reads_fresh_state(dynamodb_memory: DynamoDbMemory):
    """A caller-supplied ``current=`` that has gone stale must not poison every retry."""
    doc = dynamodb_memory.create_new(RetryDoc, {"title": "v1"})
    stale = doc.model_copy(deep=True)

    # Out-of-band write bumps the stored version to 2; ``stale`` still says 1.
    dynamodb_memory.update_existing(doc, {"title": "v2"})

    with dynamodb_memory.transaction() as txn:
        txn.update(RetryDoc, resource_id=doc.resource_id, updates={"body": "patched"}, current=stale)

    reloaded = dynamodb_memory.read_existing(doc.resource_id, RetryDoc)
    assert reloaded.version == 3
    assert reloaded.body == "patched"
    # The retry rebuilt from the fresh v2 state, not the stale pre-image.
    assert reloaded.title == "v2"


def test_retry_with_stale_snapshot_cache_re_reads_fresh_state(dynamodb_memory: DynamoDbMemory):
    """Snapshot-isolation read cache entries are dropped for failed ops on retry."""
    doc = dynamodb_memory.create_new(RetryDoc, {"title": "v1"})

    with dynamodb_memory.transaction(isolation_level="snapshot") as txn:
        cached = txn.read(RetryDoc, doc.resource_id)
        assert cached.version == 1

        # Another writer sneaks in after our snapshot read.
        dynamodb_memory.update_existing(doc, {"title": "v2"})

        txn.update(RetryDoc, resource_id=doc.resource_id, updates={"body": "patched"})

    reloaded = dynamodb_memory.read_existing(doc.resource_id, RetryDoc)
    assert reloaded.version == 3
    assert reloaded.body == "patched"
    assert reloaded.title == "v2"
