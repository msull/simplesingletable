"""Tests for presigned blob downloads.

The S3 tests run against MinIO and fetch the minted URL with a real HTTP GET, so the
signature, expiry and response overrides are checked by an S3 implementation rather
than by inspecting the URL alone.
"""

import gzip
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import url2pathname

import boto3
import pytest
import requests
from botocore.config import Config
from botocore.credentials import Credentials
from logzero import logger

from simplesingletable import (
    BlobCompressedError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
    DynamoDbResource,
    DynamoDbVersionedResource,
    LocalStorageMemory,
    PresignedBlobUrl,
)
from simplesingletable.blob_storage import S3BlobStorage, build_content_disposition
from simplesingletable.models import BlobFieldConfig, ResourceConfig


class FileDoc(DynamoDbResource):
    name: str
    data: Optional[bytes] = None

    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={"data": BlobFieldConfig(compress=False, content_type="application/pdf")},
    )


class VersionedFile(DynamoDbVersionedResource):
    name: str
    data: Optional[bytes] = None

    resource_config = ResourceConfig(
        compress_data=False,
        max_versions=5,
        blob_fields={"data": BlobFieldConfig(compress=False, content_type="application/octet-stream")},
    )


class CompressedDoc(DynamoDbResource):
    name: str
    data: Optional[bytes] = None

    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={"data": BlobFieldConfig(compress=True)},
    )


PDF_BYTES = b"%PDF-1.4 ..."


def _overwrite_out_of_band(memory, s3_key: str, body: bytes) -> None:
    """Replace a blob object behind the library's back, as a presigned re-PUT would."""
    memory.s3_blob_storage.s3_client.put_object(
        Bucket=memory.s3_blob_storage.bucket_name,
        Key=s3_key,
        Body=body,
    )


def _stage(memory, key: str, body: bytes) -> None:
    """Write a raw object to a staging key in the managed bucket."""
    memory.s3_blob_storage.s3_client.put_object(Bucket=memory.s3_blob_storage.bucket_name, Key=key, Body=body)


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query)


def _assert_sigv4(url: str) -> None:
    q = _query(url)
    assert q["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert "X-Amz-Signature" in q
    assert "Signature" not in q
    assert "AWSAccessKeyId" not in q


def _minio_client(endpoint_url: str, **kwargs):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        use_ssl=False,
        **kwargs,
    )


class TestSigV4:
    def test_injected_client_presigns_sigv4(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data")

        _assert_sigv4(p.url)
        assert _query(p.url)["X-Amz-SignedHeaders"] == ["host"]
        response = requests.get(p.url)
        assert response.status_code == 200
        assert response.content == PDF_BYTES

    def test_library_built_client_presigns_sigv4(self, dynamodb_memory_with_library_built_s3):
        memory = dynamodb_memory_with_library_built_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data")

        _assert_sigv4(p.url)
        assert _query(p.url)["X-Amz-SignedHeaders"] == ["host"]
        response = requests.get(p.url)
        assert response.status_code == 200
        assert response.content == PDF_BYTES

    def test_explicit_presign_client_is_used(self, dynamodb_memory_with_s3, minio_via_docker, minio_s3_bucket):
        memory = dynamodb_memory_with_s3
        presign_client = _minio_client(minio_via_docker, config=Config(signature_version="s3v4"))
        memory._s3_blob_storage = S3BlobStorage(
            bucket_name=minio_s3_bucket,
            key_prefix="test-blobs",
            s3_client=_minio_client(minio_via_docker),
            presign_s3_client=presign_client,
        )
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data")

        assert memory.s3_blob_storage.presign_client is presign_client
        _assert_sigv4(p.url)
        assert requests.get(p.url).status_code == 200

    def test_operations_client_is_untouched(self, dynamodb_memory_with_s3, dynamodb_memory_with_library_built_s3):
        # Injected operations client: same object, no handler registered, still SigV2 by default.
        memory = dynamodb_memory_with_s3
        injected = memory.s3_blob_storage.s3_client
        config_before = injected.meta.config
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        memory.presign_blob_download(doc, "data")

        assert memory.s3_blob_storage.s3_client is injected
        assert injected.meta.config is config_before
        raw = injected.generate_presigned_url(
            "get_object", Params={"Bucket": memory.s3_blob_storage.bucket_name, "Key": "any"}
        )
        assert "Signature" in _query(raw)

        # Library-built operations client: separate from the presign client, config unchanged.
        built = dynamodb_memory_with_library_built_s3
        storage = built.s3_blob_storage
        ops_config_before = storage.s3_client.meta.config
        built_doc = built.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        built.presign_blob_download(built_doc, "data")

        assert storage.presign_client is not storage.s3_client
        assert storage.s3_client.meta.config is ops_config_before
        assert built.read_blob(built_doc, "data") == PDF_BYTES

    def test_derived_client_rebuilds_when_credentials_rotate(self, dynamodb_memory_with_s3, monkeypatch):
        """Pins the botocore-private ``_get_credentials`` dependency."""
        memory = dynamodb_memory_with_s3
        storage = memory.s3_blob_storage
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        memory.presign_blob_download(doc, "data")
        first = storage.presign_client
        assert storage.presign_client is first  # cached while credentials are unchanged

        monkeypatch.setattr(
            storage.s3_client, "_get_credentials", lambda: Credentials("minioadmin", "minioadmin", "tok")
        )
        p = memory.presign_blob_download(doc, "data")

        assert storage.presign_client is not first
        # MinIO would reject the fake token, so only the query params are checked.
        assert _query(p.url)["X-Amz-Security-Token"] == ["tok"]

    def test_injected_client_without_credentials_raises(self, dynamodb_memory_with_s3, monkeypatch):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        monkeypatch.setattr(memory.s3_blob_storage.s3_client, "_get_credentials", lambda: None)

        with pytest.raises(ValueError, match="presign_s3_client"):
            memory.presign_blob_download(doc, "data")


class TestPresignedDownload:
    def test_url_downloads_stored_bytes(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data")

        assert isinstance(p, PresignedBlobUrl)
        response = requests.get(p.url)
        assert response.status_code == 200
        assert response.content == PDF_BYTES
        assert response.headers["Content-Type"] == "application/pdf"

    def test_returns_key_etag_and_expiry(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        head = memory.head_blob(doc, "data")

        p = memory.presign_blob_download(doc, "data")

        assert p.s3_key == head["s3_key"]
        assert p.etag == head["etag"]
        expected = datetime.now(timezone.utc) + timedelta(seconds=900)
        assert abs((p.expires_at - expected).total_seconds()) < 5
        assert _query(p.url)["X-Amz-Expires"] == ["900"]

    def test_url_expires(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data", expires_in=1)
        time.sleep(2)

        assert requests.get(p.url).status_code == 403

    def test_content_disposition_attachment_with_unicode_filename(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        filename = 'résumé "final".pdf'

        p = memory.presign_blob_download(doc, "data", filename=filename)
        response = requests.get(p.url)

        assert response.status_code == 200
        disposition = response.headers["Content-Disposition"]
        assert disposition == build_content_disposition(filename)
        assert 'filename="r_sum_ _final_.pdf"' in disposition
        assert "filename*=UTF-8''r%C3%A9sum%C3%A9%20%22final%22.pdf" in disposition

    def test_inline_without_filename(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data", inline=True)

        assert requests.get(p.url).headers["Content-Disposition"] == "inline"

    def test_content_type_override(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data", content_type="text/plain")

        assert requests.get(p.url).headers["Content-Type"] == "text/plain"

    def test_overrides_are_signed_query_params(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = memory.presign_blob_download(doc, "data", filename="a.pdf", content_type="text/plain")

        q = _query(p.url)
        assert "response-content-disposition" in q
        assert "response-content-type" in q
        assert q["X-Amz-SignedHeaders"] == ["host"]

    def test_versioned_resource_presigns_current_version_key(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(VersionedFile, {"name": "doc", "data": b"v1-bytes"})
        memory.update_existing(doc, {"data": b"v2-bytes"})
        current = memory.get_existing(doc.resource_id, VersionedFile)

        p = memory.presign_blob_download(current, "data")

        assert p.s3_key.endswith("/v2/data")
        assert requests.get(p.url).content == b"v2-bytes"

        v1 = memory.get_existing(doc.resource_id, VersionedFile, version=1)
        assert requests.get(memory.presign_blob_download(v1, "data").url).content == b"v1-bytes"

    def test_copied_blob_presigns_resolved_version(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        src = memory.create_new(FileDoc, {"name": "src", "data": PDF_BYTES})
        tgt = memory.create_new(VersionedFile, {"name": "tgt"})

        memory.copy_blob(source_resource=src, source_field="data", target_resource=tgt, target_field="data")
        fresh = memory.get_existing(tgt.resource_id, VersionedFile)

        p = memory.presign_blob_download(fresh, "data")
        assert requests.get(p.url).content == PDF_BYTES

    def test_does_not_touch_cache(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        storage = memory.s3_blob_storage

        before = storage.get_cache_stats()
        memory.presign_blob_download(doc, "data")
        after = storage.get_cache_stats()

        assert (before.hits, before.misses, before.current_items) == (after.hits, after.misses, after.current_items)


class TestPresignIfMatch:
    def test_matching_etag_mints_url(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        head = memory.head_blob(doc, "data")

        p = memory.presign_blob_download(doc, "data", if_match=head["etag"])

        assert p.etag == head["etag"]
        assert requests.get(p.url).status_code == 200

    def test_unquoted_etag_is_accepted(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        etag = memory.head_blob(doc, "data")["etag"]

        p = memory.presign_blob_download(doc, "data", if_match=etag.strip('"'))

        assert p.etag == etag
        assert p.etag.startswith('"') and p.etag.endswith('"')

    def test_stale_etag_raises_precondition_failed(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        head = memory.head_blob(doc, "data")
        _overwrite_out_of_band(memory, head["s3_key"], b"swapped")

        with pytest.raises(BlobPreconditionFailedError) as exc_info:
            memory.presign_blob_download(doc, "data", if_match=head["etag"])

        assert exc_info.value.expected_etag == head["etag"]
        assert exc_info.value.bucket == memory.s3_blob_storage.bucket_name

    @pytest.mark.parametrize("if_match", [None, '"abc"'])
    def test_missing_blob_raises_not_found(self, dynamodb_memory_with_s3, if_match):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "no-blob"})

        with pytest.raises(BlobNotFoundError):
            memory.presign_blob_download(doc, "data", if_match=if_match)

    def test_url_does_not_require_if_match_header(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        etag = memory.head_blob(doc, "data")["etag"]

        p = memory.presign_blob_download(doc, "data", if_match=etag)

        assert _query(p.url)["X-Amz-SignedHeaders"] == ["host"]
        assert requests.get(p.url).status_code == 200


class TestMintTimeWindow:
    """Checks apply at mint time only; the URL serves whatever is at its key while valid.

    This is the accepted, documented behaviour. Pinning the URL to an S3 ``VersionId``
    (on versioned buckets) is the follow-up that would close the window.
    """

    def test_non_versioned_key_replaced_after_mint_serves_new_bytes(self, dynamodb_memory_with_s3):
        """Accepted behaviour; ``VersionId`` pinning is the follow-up."""
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        etag = memory.head_blob(doc, "data")["etag"]

        p = memory.presign_blob_download(doc, "data", if_match=etag)
        _overwrite_out_of_band(memory, p.s3_key, b"replacement")

        assert requests.get(p.url).content == b"replacement"

    def test_versioned_current_key_replaced_by_register_external_blob_serves_new_bytes(self, dynamodb_memory_with_s3):
        """Accepted behaviour; ``VersionId`` pinning is the follow-up."""
        memory = dynamodb_memory_with_s3
        created = memory.create_new(VersionedFile, {"name": "doc", "data": b"v1-original"})
        doc = memory.get_existing(created.resource_id, VersionedFile)
        p = memory.presign_blob_download(doc, "data")

        _stage(memory, "staging/replacement", b"replacement")
        placeholder = memory.register_external_blob(doc, "data", source_s3_key="staging/replacement")

        assert placeholder["s3_key"] == p.s3_key
        assert memory.get_existing(doc.resource_id, VersionedFile).version == 1
        assert requests.get(p.url).content == b"replacement"
        with pytest.raises(BlobPreconditionFailedError):
            memory.presign_blob_download(doc, "data", if_match=p.etag)

    def test_versioned_current_key_replaced_by_copy_blob_serves_new_bytes(self, dynamodb_memory_with_s3):
        """Accepted behaviour; ``VersionId`` pinning is the follow-up."""
        memory = dynamodb_memory_with_s3
        created = memory.create_new(VersionedFile, {"name": "doc", "data": b"v1-original"})
        doc = memory.get_existing(created.resource_id, VersionedFile)
        p = memory.presign_blob_download(doc, "data")

        src = memory.create_new(FileDoc, {"name": "src", "data": b"replacement"})
        placeholder = memory.copy_blob(source_resource=src, source_field="data", target_resource=doc, target_field="data")

        assert placeholder["s3_key"] == p.s3_key
        assert memory.get_existing(doc.resource_id, VersionedFile).version == 1
        assert requests.get(p.url).content == b"replacement"
        with pytest.raises(BlobPreconditionFailedError):
            memory.presign_blob_download(doc, "data", if_match=p.etag)


class TestPresignCompression:
    def test_compress_configured_field_refused_without_network(self, dynamodb_memory_with_s3, monkeypatch):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(CompressedDoc, {"name": "doc", "data": b"payload"})

        def _no_network(*args, **kwargs):
            raise AssertionError("head_object must not be called")

        monkeypatch.setattr(memory.s3_blob_storage.s3_client, "head_object", _no_network)

        with pytest.raises(BlobCompressedError) as exc_info:
            memory.presign_blob_download(doc, "data")

        assert exc_info.value.field_name == "data"
        assert isinstance(exc_info.value, ValueError)

    def _register_compressed(self, memory, resource_class):
        _stage(memory, "staging/gz", gzip.compress(b"payload"))
        doc = memory.create_new(resource_class, {"name": "doc"})
        memory.register_external_blob(doc, "data", source_s3_key="staging/gz", compressed=True)
        return memory.get_existing(doc.resource_id, resource_class)

    def test_registered_compressed_object_refused_after_reload_without_if_match(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        fresh = self._register_compressed(memory, FileDoc)

        with pytest.raises(BlobCompressedError):
            memory.presign_blob_download(fresh, "data")

    def test_registered_compressed_object_refused_after_load_blob_fields(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        fresh = self._register_compressed(memory, FileDoc)
        fresh.load_blob_fields(memory)

        with pytest.raises(BlobCompressedError):
            memory.presign_blob_download(fresh, "data")

    def test_registered_compressed_object_on_versioned_resource_refused(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        fresh = self._register_compressed(memory, VersionedFile)

        with pytest.raises(BlobCompressedError):
            memory.presign_blob_download(fresh, "data")

    def test_copied_compressed_source_into_uncompressed_field_refused_after_reload(
        self, dynamodb_memory_with_s3, caplog
    ):
        """``copy_blob`` warns on a compression mismatch and copies as-is; that is unchanged."""
        memory = dynamodb_memory_with_s3
        src = memory.create_new(CompressedDoc, {"name": "src", "data": b"payload"})
        tgt = memory.create_new(FileDoc, {"name": "tgt"})

        with caplog.at_level(logging.WARNING):
            memory.copy_blob(source_resource=src, source_field="data", target_resource=tgt, target_field="data")

        assert "Compression mismatch" in caplog.text
        assert memory.head_blob(tgt, "data")["compressed"] is True

        fresh = memory.get_existing(tgt.resource_id, FileDoc)
        with pytest.raises(BlobCompressedError):
            memory.presign_blob_download(fresh, "data")

    def test_copied_uncompressed_source_presigns_after_reload(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        src = memory.create_new(FileDoc, {"name": "src", "data": PDF_BYTES})
        tgt = memory.create_new(FileDoc, {"name": "tgt"})

        memory.copy_blob(source_resource=src, source_field="data", target_resource=tgt, target_field="data")
        fresh = memory.get_existing(tgt.resource_id, FileDoc)

        p = memory.presign_blob_download(fresh, "data")
        assert requests.get(p.url).content == PDF_BYTES

    def test_direct_storage_call_refuses_compressed_object(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        fresh = self._register_compressed(memory, FileDoc)

        with pytest.raises(BlobCompressedError) as exc_info:
            memory.s3_blob_storage.generate_presigned_get("FileDoc", fresh.resource_id, "data")

        assert exc_info.value.bucket == memory.s3_blob_storage.bucket_name

    def test_direct_storage_call_refuses_library_compressed_object(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(CompressedDoc, {"name": "doc", "data": b"payload"})

        with pytest.raises(BlobCompressedError):
            memory.s3_blob_storage.generate_presigned_get("CompressedDoc", doc.resource_id, "data")


class TestPresignValidation:
    def test_non_blob_field_raises_value_error(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        with pytest.raises(ValueError, match="not configured as a blob field"):
            memory.presign_blob_download(doc, "name")

    def test_s3_not_configured_raises_value_error(self, dynamodb_memory):
        doc = dynamodb_memory.create_new(FileDoc, {"name": "doc"})

        with pytest.raises(ValueError, match="S3 blob storage not configured"):
            dynamodb_memory.presign_blob_download(doc, "data")

    def test_expires_in_out_of_range(self, dynamodb_memory_with_s3):
        memory = dynamodb_memory_with_s3
        doc = memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        for bad in (0, -1, 604_801):
            with pytest.raises(ValueError, match="expires_in"):
                memory.presign_blob_download(doc, "data", expires_in=bad)

        assert memory.presign_blob_download(doc, "data", expires_in=604_800).url


class TestLocalPresignParity:
    @pytest.fixture()
    def local_memory(self, tmp_path):
        return LocalStorageMemory(logger=logger, storage_dir=str(tmp_path / "store"), use_blob_storage=True)

    @staticmethod
    def _read_uri(url: str) -> bytes:
        with open(url2pathname(urlparse(url).path), "rb") as f:
            return f.read()

    def _register_compressed(self, local_memory):
        staged = local_memory.s3_blob_storage._key_to_path("staging/gz")
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(gzip.compress(b"payload"))
        doc = local_memory.create_new(FileDoc, {"name": "doc"})
        local_memory.register_external_blob(doc, "data", source_s3_key="staging/gz", compressed=True)
        return local_memory.get_existing(doc.resource_id, FileDoc)

    def test_returns_file_uri_to_blob(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        head = local_memory.head_blob(doc, "data")

        p = local_memory.presign_blob_download(doc, "data")

        assert p.url.startswith("file://")
        assert self._read_uri(p.url) == PDF_BYTES
        assert p.expires_at is None
        assert p.s3_key == head["s3_key"]
        assert p.etag == head["etag"]

    def test_relative_storage_dir_yields_absolute_uri(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        local_memory = LocalStorageMemory(logger=logger, storage_dir="store", use_blob_storage=True)
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        p = local_memory.presign_blob_download(doc, "data")

        assert self._read_uri(p.url) == PDF_BYTES

    def test_matching_etag(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        etag = local_memory.head_blob(doc, "data")["etag"]

        assert local_memory.presign_blob_download(doc, "data", if_match=etag).etag == etag

    def test_unquoted_etag_is_accepted(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        etag = local_memory.head_blob(doc, "data")["etag"]

        assert local_memory.presign_blob_download(doc, "data", if_match=etag.strip('"')).etag == etag

    def test_stale_etag_raises_precondition_failed(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})
        head = local_memory.head_blob(doc, "data")
        local_memory.s3_blob_storage._key_to_path(head["s3_key"]).write_bytes(b"swapped")

        with pytest.raises(BlobPreconditionFailedError):
            local_memory.presign_blob_download(doc, "data", if_match=head["etag"])

    @pytest.mark.parametrize("if_match", [None, '"abc"'])
    def test_missing_blob_raises_not_found(self, local_memory, if_match):
        doc = local_memory.create_new(FileDoc, {"name": "no-blob"})

        with pytest.raises(BlobNotFoundError):
            local_memory.presign_blob_download(doc, "data", if_match=if_match)

    def test_non_blob_field_raises_value_error(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        with pytest.raises(ValueError, match="not configured as a blob field"):
            local_memory.presign_blob_download(doc, "name")

    def test_expires_in_out_of_range(self, local_memory):
        doc = local_memory.create_new(FileDoc, {"name": "doc", "data": PDF_BYTES})

        for bad in (0, -1, 604_801):
            with pytest.raises(ValueError, match="expires_in"):
                local_memory.presign_blob_download(doc, "data", expires_in=bad)

        assert local_memory.presign_blob_download(doc, "data", expires_in=604_800).url

    def test_compress_configured_field_refused(self, local_memory):
        doc = local_memory.create_new(CompressedDoc, {"name": "doc", "data": b"payload"})

        with pytest.raises(BlobCompressedError):
            local_memory.presign_blob_download(doc, "data")

    def test_registered_compressed_object_refused_after_reload(self, local_memory):
        fresh = self._register_compressed(local_memory)

        with pytest.raises(BlobCompressedError):
            local_memory.presign_blob_download(fresh, "data")

    def test_direct_storage_call_refuses_compressed_object(self, local_memory):
        fresh = self._register_compressed(local_memory)

        with pytest.raises(BlobCompressedError):
            local_memory.s3_blob_storage.generate_presigned_get("FileDoc", fresh.resource_id, "data")

    def test_copied_compressed_source_into_uncompressed_field_refused_after_reload(self, local_memory, caplog):
        src = local_memory.create_new(CompressedDoc, {"name": "src", "data": b"payload"})
        tgt = local_memory.create_new(FileDoc, {"name": "tgt"})

        with caplog.at_level(logging.WARNING):
            local_memory.copy_blob(source_resource=src, source_field="data", target_resource=tgt, target_field="data")

        assert "Compression mismatch" in caplog.text
        fresh = local_memory.get_existing(tgt.resource_id, FileDoc)
        with pytest.raises(BlobCompressedError):
            local_memory.presign_blob_download(fresh, "data")

    def test_versioned_current_key_replaced_by_register_external_blob(self, local_memory):
        """Accepted behaviour; ``VersionId`` pinning is the follow-up (S3 only)."""
        created = local_memory.create_new(VersionedFile, {"name": "doc", "data": b"v1-original"})
        doc = local_memory.get_existing(created.resource_id, VersionedFile)
        p = local_memory.presign_blob_download(doc, "data")

        staged = local_memory.s3_blob_storage._key_to_path("staging/replacement")
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(b"replacement")
        local_memory.register_external_blob(doc, "data", source_s3_key="staging/replacement")

        assert local_memory.presign_blob_download(doc, "data").url == p.url
        assert self._read_uri(p.url) == b"replacement"
        with pytest.raises(BlobPreconditionFailedError):
            local_memory.presign_blob_download(doc, "data", if_match=p.etag)

    def test_blob_storage_disabled_raises_value_error(self, tmp_path):
        local_memory = LocalStorageMemory(logger=logger, storage_dir=str(tmp_path / "store"), use_blob_storage=False)
        doc = local_memory.create_new(FileDoc, {"name": "doc"})

        with pytest.raises(ValueError, match="Blob storage not configured"):
            local_memory.presign_blob_download(doc, "data")


class TestConstructorCompatibility:
    def test_positional_cache_configuration_is_preserved(self):
        client = boto3.client("s3", region_name="us-east-1")
        storage = S3BlobStorage("bucket", "prefix", client, {"region_name": "us-east-1"}, "http://x", False, 123, 45, 67, 89)

        assert storage.bucket_name == "bucket"
        assert storage.key_prefix == "prefix"
        assert storage.cache_enabled is False
        assert storage.cache_max_size_bytes == 123
        assert storage.cache_max_items == 45
        assert storage.cache_ttl_seconds == 67
        assert storage.cache_max_item_size_bytes == 89
        assert storage._presign_s3_client is None

    def test_presign_client_is_keyword_only(self):
        client = boto3.client("s3", region_name="us-east-1")
        with pytest.raises(TypeError):
            S3BlobStorage("bucket", "prefix", client, None, None, False, 123, 45, 67, 89, client)  # type: ignore[misc]

        presign = object()
        storage = S3BlobStorage("bucket", s3_client=client, presign_s3_client=presign)  # type: ignore[arg-type]
        assert storage._presign_s3_client is presign
        assert storage.presign_client is presign


class TestContentDisposition:
    def test_no_filename_attachment_is_none(self):
        assert build_content_disposition(None) is None

    def test_no_filename_inline(self):
        assert build_content_disposition(None, inline=True) == "inline"

    def test_ascii_name(self):
        assert build_content_disposition("report.pdf") == "attachment; filename=\"report.pdf\"; filename*=UTF-8''report.pdf"

    def test_quote_and_backslash(self):
        assert build_content_disposition('a"b\\c.pdf', inline=True) == (
            "inline; filename=\"a_b_c.pdf\"; filename*=UTF-8''a%22b%5Cc.pdf"
        )

    def test_non_ascii_name(self):
        assert build_content_disposition("日本.txt") == (
            "attachment; filename=\"__.txt\"; filename*=UTF-8''%E6%97%A5%E6%9C%AC.txt"
        )
