"""Deleting one resource's S3 blobs must leave the blobs of a resource whose id starts with its id."""

import pytest

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.blob_storage import S3BlobStorage
from simplesingletable.models import BlobFieldConfig, ResourceConfig


class Note(DynamoDbResource):
    title: str
    body: dict | None = None

    resource_config = ResourceConfig(blob_fields={"body": BlobFieldConfig(compress=False)})


class VersionedNote(DynamoDbVersionedResource):
    title: str
    body: dict | None = None

    resource_config = ResourceConfig(compress_data=False, blob_fields={"body": BlobFieldConfig(compress=False)})


def _stored_keys(storage: S3BlobStorage, resource_type: str, resource_id: str) -> list[str]:
    prefix = f"{storage.key_prefix}/{resource_type}/{resource_id}/"
    response = storage.s3_client.list_objects_v2(Bucket=storage.bucket_name, Prefix=prefix)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def _all_keys(storage: S3BlobStorage) -> list[str]:
    response = storage.s3_client.list_objects_v2(Bucket=storage.bucket_name)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


@pytest.mark.parametrize(
    "key_prefix, expected_key",
    [
        (None, "Note/alexander/body"),
        ("", "Note/alexander/body"),
        ("blobs", "blobs/Note/alexander/body"),
        ("blobs/", "blobs/Note/alexander/body"),
    ],
)
def test_delete_all_blobs_leaves_an_id_it_prefixes(minio_s3_client, minio_s3_bucket, key_prefix, expected_key):
    storage = S3BlobStorage(bucket_name=minio_s3_bucket, key_prefix=key_prefix, s3_client=minio_s3_client)
    config = BlobFieldConfig(compress=False)
    storage.put_blob("Note", "alex", "body", {"who": "alex"}, config)
    storage.put_blob("Note", "alexander", "body", {"who": "alexander"}, config)

    deleted = storage.delete_all_blobs("Note", "alex")

    assert deleted == 1
    assert _all_keys(storage) == [expected_key]


def test_deleting_a_resource_keeps_the_blobs_of_an_id_it_prefixes(dynamodb_memory_with_s3):
    memory = dynamodb_memory_with_s3
    storage = memory.s3_blob_storage
    memory.create_new(Note, {"title": "a", "body": {"who": "alex"}}, override_id="alex")
    memory.create_new(Note, {"title": "b", "body": {"who": "alexander"}}, override_id="alexander")

    memory.delete_existing(memory.get_existing("alex", Note))

    assert _stored_keys(storage, "Note", "alex") == []
    assert _stored_keys(storage, "Note", "alexander") == ["test-blobs/Note/alexander/body"]
    storage.clear_cache()
    assert memory.get_existing("alexander", Note, load_blobs=True).body == {"who": "alexander"}


def test_deleting_all_versions_keeps_the_blobs_of_an_id_it_prefixes(dynamodb_memory_with_s3):
    memory = dynamodb_memory_with_s3
    storage = memory.s3_blob_storage
    alex = memory.create_new(VersionedNote, {"title": "a", "body": {"who": "alex"}}, override_id="alex")
    memory.update_existing(alex, {"body": {"who": "alex, again"}})
    memory.create_new(VersionedNote, {"title": "b", "body": {"who": "alexander"}}, override_id="alexander")
    assert _stored_keys(storage, "VersionedNote", "alex") == [
        "test-blobs/VersionedNote/alex/v1/body",
        "test-blobs/VersionedNote/alex/v2/body",
    ]

    memory.delete_all_versions("alex", VersionedNote)

    assert _stored_keys(storage, "VersionedNote", "alex") == []
    assert _stored_keys(storage, "VersionedNote", "alexander") == ["test-blobs/VersionedNote/alexander/v1/body"]
    storage.clear_cache()
    assert memory.get_existing("alexander", VersionedNote, load_blobs=True).body == {"who": "alexander"}
