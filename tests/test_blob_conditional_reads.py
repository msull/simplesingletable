"""Tests for conditional blob reads, typed blob errors, and read-side size enforcement.

The S3 tests run against MinIO, not a filesystem fake, which is what gives them teeth:
entity tags are quoted strings (RFC 9110) and a local implementation would naturally
produce a bare md5, hiding the quoting requirement entirely.
"""

from typing import Optional

import pytest
from logzero import logger

from simplesingletable import (
    BlobError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
    BlobTooLargeError,
    DynamoDbResource,
    DynamoDbVersionedResource,
    LocalStorageMemory,
)
from simplesingletable.models import BlobFieldConfig, ResourceConfig


class UploadedDoc(DynamoDbResource):
    """Non-versioned resource with a blob whose S3 key is stable and overwritable."""

    name: str
    payload: Optional[str] = None

    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={"payload": BlobFieldConfig(compress=False, content_type="text/plain")},
    )


class BoundedDoc(DynamoDbResource):
    """Non-versioned resource with a configured max blob size."""

    name: str
    payload: Optional[str] = None

    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={"payload": BlobFieldConfig(compress=False, max_size_bytes=10_000)},
    )


class VersionedDoc(DynamoDbVersionedResource):
    title: str
    body: Optional[str] = None

    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={"body": BlobFieldConfig(compress=True, content_type="text/plain")},
    )


def _overwrite_out_of_band(memory, s3_key: str, body: bytes) -> None:
    """Replace a blob object behind the library's back, as a presigned re-PUT would."""
    memory.s3_blob_storage.s3_client.put_object(
        Bucket=memory.s3_blob_storage.bucket_name,
        Key=s3_key,
        Body=body,
    )


class TestEtagContract:
    def test_head_blob_returns_quoted_etag(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": '"hello"'})

        head = memory.head_blob(doc, "payload")

        assert head["etag"]
        assert head["etag"].startswith('"') and head["etag"].endswith('"')

    def test_put_placeholder_carries_same_etag_as_head(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        placeholder = memory.s3_blob_storage.put_blob(
            resource_type="UploadedDoc",
            resource_id="abc",
            field_name="payload",
            value="content",
            config=BlobFieldConfig(compress=False),
        )
        head = memory.s3_blob_storage.head_blob("UploadedDoc", "abc", "payload")

        assert placeholder["etag"] == head["etag"]

    def test_unquoted_etag_is_accepted(self, dynamodb_memory_with_s3):
        """ETags that lost their quotes in transit must behave identically, not silently
        stop matching."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        etag = memory.head_blob(doc, "payload")["etag"]

        assert memory.read_blob(doc, "payload", if_match=etag.strip('"')) == "content"

    def test_unquoted_stale_etag_still_fails(self, dynamodb_memory_with_s3):
        """Normalizing quotes must not soften the guard itself."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "validated"})
        head = memory.head_blob(doc, "payload")
        _overwrite_out_of_band(memory, head["s3_key"], b'"swapped"')

        with pytest.raises(BlobPreconditionFailedError):
            memory.read_blob(doc, "payload", if_match=head["etag"].strip('"'))


class TestConditionalRead:
    def test_matching_etag_reads_successfully(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "original"})
        etag = memory.head_blob(doc, "payload")["etag"]

        assert memory.read_blob(doc, "payload", if_match=etag) == "original"

    def test_replaced_object_fails_the_precondition(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "validated"})
        head = memory.head_blob(doc, "payload")

        _overwrite_out_of_band(memory, head["s3_key"], b'"swapped"')

        with pytest.raises(BlobPreconditionFailedError) as exc_info:
            memory.read_blob(doc, "payload", if_match=head["etag"])

        assert exc_info.value.expected_etag == head["etag"]
        assert exc_info.value.s3_key == head["s3_key"]

    def test_conditional_read_is_not_served_stale_from_cache(self, dynamodb_memory_with_s3):
        """The cache is keyed on identity that a non-versioned overwrite does not change."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "validated"})
        head = memory.head_blob(doc, "payload")

        # Warm the cache, then replace the object behind the library's back.
        assert memory.read_blob(doc, "payload") == "validated"
        _overwrite_out_of_band(memory, head["s3_key"], b'"swapped"')

        # An unconditional read may still serve the cached bytes...
        assert memory.read_blob(doc, "payload") == "validated"

        # ...but a conditional read must not: it re-fetches and S3 rejects it.
        with pytest.raises(BlobPreconditionFailedError):
            memory.read_blob(doc, "payload", if_match=head["etag"])

    def test_conditional_read_after_replacement_succeeds_with_new_etag(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "original"})
        head = memory.head_blob(doc, "payload")
        _overwrite_out_of_band(memory, head["s3_key"], b'"replacement"')

        new_etag = memory.head_blob(doc, "payload")["etag"]

        assert new_etag != head["etag"]
        assert memory.read_blob(doc, "payload", if_match=new_etag) == "replacement"

    def test_conditional_read_on_versioned_resource(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(VersionedDoc, {"title": "t", "body": "v1 body"})
        head = memory.head_blob(doc, "body")

        assert memory.read_blob(doc, "body", if_match=head["etag"]) == "v1 body"


class TestTypedErrors:
    def test_missing_blob_raises_blob_not_found(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        with pytest.raises(BlobNotFoundError):
            memory.s3_blob_storage.head_blob("UploadedDoc", "does-not-exist", "payload")
        with pytest.raises(BlobNotFoundError):
            memory.s3_blob_storage.get_blob("UploadedDoc", "does-not-exist", "payload")

    def test_blob_not_found_is_both_value_error_and_file_not_found(self, dynamodb_memory_with_s3):
        """Backwards compatibility: the API used to raise a bare ValueError."""
        memory = dynamodb_memory_with_s3
        with pytest.raises(ValueError):
            memory.s3_blob_storage.head_blob("UploadedDoc", "nope", "payload")
        with pytest.raises(FileNotFoundError):
            memory.s3_blob_storage.head_blob("UploadedDoc", "nope", "payload")

    def test_all_blob_errors_are_value_errors(self):
        for exc_type in (BlobNotFoundError, BlobPreconditionFailedError, BlobTooLargeError):
            assert issubclass(exc_type, BlobError)
            assert issubclass(exc_type, ValueError)

    def test_precondition_and_not_found_are_distinguishable(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        head = memory.head_blob(doc, "payload")
        _overwrite_out_of_band(memory, head["s3_key"], b'"other"')

        with pytest.raises(BlobPreconditionFailedError):
            memory.read_blob(doc, "payload", if_match=head["etag"])

        # ...and the missing case does not masquerade as a changed one
        missing = memory.create_new(UploadedDoc, {"name": "no-blob"})
        with pytest.raises(BlobNotFoundError):
            memory.head_blob(missing, "payload")


class TestSizeEnforcement:
    def test_max_bytes_refuses_oversized_blob(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "x" * 5000})
        size = memory.head_blob(doc, "payload")["size_bytes"]

        with pytest.raises(BlobTooLargeError) as exc_info:
            memory.read_blob(doc, "payload", max_bytes=100)

        assert exc_info.value.size_bytes == size
        assert exc_info.value.max_bytes == 100

    def test_max_bytes_allows_blob_within_limit(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "small"})

        assert memory.read_blob(doc, "payload", max_bytes=1_000_000) == "small"

    def test_oversized_cached_blob_is_still_refused(self, dynamodb_memory_with_s3):
        """A warm cache must not smuggle an oversized object past the limit."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc", "payload": "x" * 5000})

        assert memory.read_blob(doc, "payload")  # warm the cache

        with pytest.raises(BlobTooLargeError):
            memory.read_blob(doc, "payload", max_bytes=100)

    def test_read_blob_defaults_to_configured_max_size_bytes(self, dynamodb_memory_with_s3):
        """max_size_bytes was previously enforced only on write."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(BoundedDoc, {"name": "doc", "payload": "small"})
        head = memory.head_blob(doc, "payload")

        # Grow the stored object past the configured limit, behind the library's back
        _overwrite_out_of_band(memory, head["s3_key"], b'"' + b"x" * 20_000 + b'"')
        memory.s3_blob_storage.clear_cache()

        with pytest.raises(BlobTooLargeError) as exc_info:
            memory.read_blob(doc, "payload")

        assert exc_info.value.max_bytes == 10_000

    def test_explicit_max_bytes_overrides_configured_limit(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(BoundedDoc, {"name": "doc", "payload": "small"})
        head = memory.head_blob(doc, "payload")
        _overwrite_out_of_band(memory, head["s3_key"], b'"' + b"x" * 20_000 + b'"')
        memory.s3_blob_storage.clear_cache()

        assert len(memory.read_blob(doc, "payload", max_bytes=50_000)) == 20_000


class TestCopyGuards:
    def test_register_external_blob_honors_source_etag(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        storage = memory.s3_blob_storage
        storage.s3_client.put_object(Bucket=storage.bucket_name, Key="incoming/upload.txt", Body=b'"validated"')
        source_etag = storage.s3_client.head_object(Bucket=storage.bucket_name, Key="incoming/upload.txt")["ETag"]

        doc = memory.create_new(UploadedDoc, {"name": "doc"})
        placeholder = memory.register_external_blob(
            doc, "payload", source_s3_key="incoming/upload.txt", source_etag=source_etag
        )

        assert placeholder["etag"]
        assert memory.read_blob(doc, "payload") == "validated"

    def test_register_external_blob_rejects_replaced_source(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        storage = memory.s3_blob_storage
        storage.s3_client.put_object(Bucket=storage.bucket_name, Key="incoming/upload.txt", Body=b'"validated"')
        source_etag = storage.s3_client.head_object(Bucket=storage.bucket_name, Key="incoming/upload.txt")["ETag"]

        # The operator re-PUTs to the presigned URL after validation
        storage.s3_client.put_object(Bucket=storage.bucket_name, Key="incoming/upload.txt", Body=b'"swapped"')

        doc = memory.create_new(UploadedDoc, {"name": "doc"})
        with pytest.raises(BlobPreconditionFailedError):
            memory.register_external_blob(
                doc, "payload", source_s3_key="incoming/upload.txt", source_etag=source_etag
            )

    def test_register_external_blob_missing_source_raises_not_found(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(UploadedDoc, {"name": "doc"})

        with pytest.raises(BlobNotFoundError):
            memory.register_external_blob(doc, "payload", source_s3_key="incoming/absent.txt")

    def test_copy_blob_rejects_stale_source_etag(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        source = memory.create_new(UploadedDoc, {"name": "src", "payload": "original"})
        target = memory.create_new(VersionedDoc, {"title": "dst"})
        stale_etag = memory.head_blob(source, "payload")["etag"]

        _overwrite_out_of_band(memory, memory.head_blob(source, "payload")["s3_key"], b'"replaced"')

        with pytest.raises(BlobPreconditionFailedError):
            memory.copy_blob(source, "payload", target, "body", source_etag=stale_etag)

    def test_copy_blob_still_works_without_explicit_etag(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        source = memory.create_new(UploadedDoc, {"name": "src", "payload": "original"})
        target = memory.create_new(VersionedDoc, {"title": "dst"})

        memory.copy_blob(source, "payload", target, "body")

        assert memory.read_blob(target, "body") == "original"


class TestLocalStorageParity:
    """The local backend must present the same contract, quoted ETags included."""

    @pytest.fixture()
    def local_memory(self, tmp_path):
        return LocalStorageMemory(logger=logger, storage_dir=str(tmp_path / "store"), use_blob_storage=True)

    def test_head_blob_returns_quoted_etag(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        head = local_memory.head_blob(doc, "payload")

        assert head["etag"].startswith('"') and head["etag"].endswith('"')

    def test_matching_etag_reads_successfully(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        etag = local_memory.head_blob(doc, "payload")["etag"]

        assert local_memory.read_blob(doc, "payload", if_match=etag) == "content"

    def test_replaced_blob_fails_the_precondition(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        head = local_memory.head_blob(doc, "payload")

        local_memory.s3_blob_storage._key_to_path(head["s3_key"]).write_bytes(b'"swapped"')

        with pytest.raises(BlobPreconditionFailedError):
            local_memory.read_blob(doc, "payload", if_match=head["etag"])

    def test_unquoted_etag_is_accepted(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc", "payload": "content"})
        etag = local_memory.head_blob(doc, "payload")["etag"]

        assert local_memory.read_blob(doc, "payload", if_match=etag.strip('"')) == "content"

    def test_max_bytes_refuses_oversized_blob(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc", "payload": "x" * 5000})

        with pytest.raises(BlobTooLargeError):
            local_memory.read_blob(doc, "payload", max_bytes=100)

    def test_missing_blob_raises_blob_not_found(self, local_memory):
        doc = local_memory.create_new(UploadedDoc, {"name": "doc"})

        with pytest.raises(BlobNotFoundError):
            local_memory.head_blob(doc, "payload")
