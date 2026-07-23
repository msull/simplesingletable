"""Tests for issue #8 P2: transaction retry behavior.

P2a — implicit-condition retries must invalidate stale cached pre-images
(``op.current`` / snapshot ``read_cache``) before rebuilding, otherwise every
retry re-derives the same stale version number and is guaranteed to fail.

P2b — transient failures (TransactionConflict, throttling,
TransactionInProgressException) are retried with backoff instead of raising
immediately.
"""

import uuid
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.transactions import TransactionContext, TransactionError


class RetryDoc(DynamoDbVersionedResource):
    title: str
    body: str = ""


class RetryItem(DynamoDbResource):
    name: str


def _new_item(name: str) -> RetryItem:
    now = datetime.now(timezone.utc)
    return RetryItem(name=name, resource_id=uuid.uuid4().hex, created_at=now, updated_at=now)


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


class FlakyClient:
    """Wraps the real DynamoDB client, raising queued errors before delegating."""

    def __init__(self, real_client, errors):
        self._real = real_client
        self._errors = list(errors)
        self.calls = 0

    def transact_write_items(self, **kwargs):
        self.calls += 1
        if self._errors:
            raise self._errors.pop(0)
        return self._real.transact_write_items(**kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _client_error(code: str, reasons=None) -> ClientError:
    response = {"Error": {"Code": code, "Message": "simulated"}}
    if reasons is not None:
        response["CancellationReasons"] = reasons
    return ClientError(response, "TransactWriteItems")


def _conflict_cancellation() -> ClientError:
    return _client_error(
        "TransactionCanceledException", reasons=[{"Code": "TransactionConflict", "Message": "simulated"}]
    )


@pytest.fixture
def no_backoff_sleep(monkeypatch):
    monkeypatch.setattr(TransactionContext, "_sleep_backoff", staticmethod(lambda attempt: None))


def test_transaction_conflict_is_retried(dynamodb_memory: DynamoDbMemory, no_backoff_sleep):
    flaky = FlakyClient(dynamodb_memory.dynamodb_client, [_conflict_cancellation(), _conflict_cancellation()])
    dynamodb_memory._dynamodb_client = flaky
    try:
        with dynamodb_memory.transaction() as txn:
            item = txn.create(_new_item("survived"))
    finally:
        dynamodb_memory._dynamodb_client = flaky._real

    assert flaky.calls == 3
    assert dynamodb_memory.get_existing(item.resource_id, RetryItem).name == "survived"


def test_transaction_in_progress_is_retried(dynamodb_memory: DynamoDbMemory, no_backoff_sleep):
    flaky = FlakyClient(dynamodb_memory.dynamodb_client, [_client_error("TransactionInProgressException")])
    dynamodb_memory._dynamodb_client = flaky
    try:
        with dynamodb_memory.transaction() as txn:
            item = txn.create(_new_item("in-progress"))
    finally:
        dynamodb_memory._dynamodb_client = flaky._real

    assert flaky.calls == 2
    assert dynamodb_memory.get_existing(item.resource_id, RetryItem) is not None


def test_transient_retries_exhausted_raises_transaction_error(dynamodb_memory: DynamoDbMemory, no_backoff_sleep):
    errors = [_conflict_cancellation()] * 3
    flaky = FlakyClient(dynamodb_memory.dynamodb_client, errors)
    dynamodb_memory._dynamodb_client = flaky
    try:
        with pytest.raises(TransactionError, match="TransactionConflict"):
            with dynamodb_memory.transaction(max_retries=2) as txn:
                txn.create(_new_item("doomed"))
    finally:
        dynamodb_memory._dynamodb_client = flaky._real

    assert flaky.calls == 3  # initial attempt + 2 retries


def test_transient_not_retried_when_auto_retry_disabled(dynamodb_memory: DynamoDbMemory, no_backoff_sleep):
    flaky = FlakyClient(dynamodb_memory.dynamodb_client, [_conflict_cancellation()])
    dynamodb_memory._dynamodb_client = flaky
    try:
        with pytest.raises(TransactionError):
            with dynamodb_memory.transaction(auto_retry=False) as txn:
                txn.create(_new_item("no-retry"))
    finally:
        dynamodb_memory._dynamodb_client = flaky._real

    assert flaky.calls == 1
