"""``resource_config`` inherits from the nearest ancestor, and reads detect the stored format.

Covers issue #15. ``DynamoDbResource`` and ``DynamoDbVersionedResource`` used to build every
subclass's ``resource_config`` by merging its own keys over the *library root* default, never
over the parent's. A class two or more levels below a root therefore lost what its
intermediate (domain) base had set, in two ways:

1. **Restating some keys** -- ``Child(Base)`` with ``ResourceConfig(max_versions=None)`` lost
   ``Base``'s ``compress_data=False`` and reverted to the root's ``True``.
2. **Restating none** -- ``Silent(Base)`` with no ``resource_config`` at all reset entirely
   to the root, losing every key ``Base`` set.

The fix merges each class's own config over its nearest ancestor's already-resolved config.
Because that can flip an existing class's effective ``compress_data``, reads now decide the
storage format from the item itself (is ``data`` a gzip envelope of *this* record?) instead
of from config, so items written under the old, wrong config keep reading.

The integration tests exercise every ``resource_config`` key through real writes and reads,
each through two grandchildren of one domain base: ``...Silent`` (declares nothing) and
``...Override`` (restates one unrelated key). Both must behave like the base.
"""

import gzip
import json
import logging
import re
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import Binary
from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource, LocalStorageMemory
from simplesingletable.dynamodb_memory import exhaust_pagination
from simplesingletable.exceptions import ConflictError
from simplesingletable.extras.audit import AuditLogQuerier
from simplesingletable.models import AuditConfig, BlobFieldConfig, ResourceConfig
from simplesingletable.transactions import TransactionError

# A gzip-looking value that fails to decode with ``gzip.BadGzipFile`` (bad compression method).
CORRUPT_GZIP = b"\x1f\x8b" + b"junk" * 4


# ---------------------------------------------------------------------------
# Raw item helpers -- every integration test checks the key layout with these
# ---------------------------------------------------------------------------


def _raw_nonversioned(memory: DynamoDbMemory, cls: type[DynamoDbResource], rid: str) -> dict[str, Any]:
    pk = f"{cls.get_unique_key_prefix()}#{rid}"
    item = memory.dynamodb_table.get_item(Key={"pk": pk, "sk": pk})["Item"]
    assert item["pk"] == item["sk"] == pk  # non-versioned: sk == pk == "{prefix}#{id}"
    return item


def _raw_versioned(
    memory: DynamoDbMemory, cls: type[DynamoDbVersionedResource], rid: str
) -> dict[str, dict[str, Any]]:
    pk = f"{cls.get_unique_key_prefix()}#{rid}"
    items = memory.dynamodb_table.query(KeyConditionExpression=Key("pk").eq(pk))["Items"]
    by_sk = {i["sk"]: i for i in items}
    assert "v0" in by_sk
    assert all(re.fullmatch(r"v\d+", sk) for sk in by_sk)  # only v0 / vN sort keys
    assert by_sk["v0"]["version"] == max(int(i["version"]) for sk, i in by_sk.items() if sk != "v0")
    for sk, i in by_sk.items():  # indexes confined to v0
        if sk != "v0":
            assert "gsitype" not in i and "gsitypesk" not in i
            assert not any(k.startswith("gsi") for k in i)
    return by_sk


def _is_gzip_binary(value: Any) -> bool:
    return isinstance(value, Binary) and bytes(value)[:2] == b"\x1f\x8b"


# ---------------------------------------------------------------------------
# Class hierarchies
# ---------------------------------------------------------------------------


class VBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=False, max_versions=5)


class VChildOverride(VBase):
    resource_config = ResourceConfig(max_versions=None)


class VChildSilent(VBase):
    pass


class VMid(VBase):
    resource_config = ResourceConfig(omit_none_attributes=True)


class VLeaf(VMid):
    resource_config = ResourceConfig(max_versions=2)


class DirectVersioned(DynamoDbVersionedResource):
    resource_config = ResourceConfig(max_versions=3)


class DirectPlain(DynamoDbResource):
    pass


class VPlainDictChild(VBase):
    resource_config = {"max_versions": 1}


class ShallowBase(DynamoDbVersionedResource):
    a: str | None = None
    b: str | None = None
    resource_config = ResourceConfig(
        blob_fields={"a": BlobFieldConfig(compress=True), "b": BlobFieldConfig(compress=False)}
    )


class ShallowChild(ShallowBase):
    resource_config = ResourceConfig(blob_fields={"a": BlobFieldConfig(compress=False)})


class Mixin(BaseModel):
    extra_field: str = "x"


class Mixed(Mixin, VBase):
    pass


# Non-versioned domain base: omit_none + TTL
class NBase(DynamoDbResource):
    resource_config = ResourceConfig(omit_none_attributes=True, ttl_field="expires_at", ttl_attribute_name="ttl")
    name: str
    assigned_user_id: str | None = None
    expires_at: datetime | None = None


class NSilent(NBase):
    pass


class NOverrideTtlOnly(NBase):
    resource_config = ResourceConfig(ttl_attribute_name="expires_ttl")


class NOmitOff(NBase):
    resource_config = ResourceConfig(omit_none_attributes=False)


# Storage format
class VFmtSilent(VBase):
    name: str


class VFmtOverride(VBase):
    resource_config = ResourceConfig(max_versions=None)
    name: str


# Version pruning
class PruneBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=False, max_versions=2)
    n: int


class PruneSilent(PruneBase):
    pass


class PruneOverrideOther(PruneBase):
    resource_config = ResourceConfig(omit_none_attributes=True)


class PruneDisabled(PruneBase):
    resource_config = ResourceConfig(max_versions=None)


# Audit
_AUDIT_ON = AuditConfig(enabled=True, track_field_changes=True, include_snapshot=True, changed_by_field=None)


class AuditedBase(DynamoDbResource):
    resource_config = ResourceConfig(omit_none_attributes=True, audit_config=_AUDIT_ON)
    name: str


class AuditedVBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=False, audit_config=_AUDIT_ON)
    name: str


class AuditedSilent(AuditedBase):
    pass


class AuditedOverride(AuditedBase):
    resource_config = ResourceConfig(omit_none_attributes=False)


class AuditedVSilent(AuditedVBase):
    pass


class AuditedVOverride(AuditedVBase):
    resource_config = ResourceConfig(max_versions=3)


class AuditOffChild(AuditedBase):
    resource_config = ResourceConfig(audit_config=AuditConfig(enabled=False))


# Blobs
class BlobVBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(
        compress_data=False,
        max_versions=3,
        blob_fields={"body": BlobFieldConfig(compress=True, content_type="text/plain")},
    )
    title: str
    body: str | None = None


class BlobNBase(DynamoDbResource):
    resource_config = ResourceConfig(
        blob_fields={"payload": BlobFieldConfig(compress=False, content_type="application/json")}
    )
    name: str
    payload: dict | None = None


class BlobVSilent(BlobVBase):
    pass


class BlobVOverride(BlobVBase):
    resource_config = ResourceConfig(max_versions=None)


class BlobNSilent(BlobNBase):
    pass


class BlobNOverride(BlobNBase):
    resource_config = ResourceConfig(omit_none_attributes=True)


# GSIs
def _category_gsi_config() -> dict[str, dict]:
    return {"gsi1": {"gsi1pk": lambda self: f"cat#{self.category}"}}


class GsiVBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=False, max_versions=2)
    category: str

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return _category_gsi_config()


class GsiNBase(DynamoDbResource):
    resource_config = ResourceConfig(omit_none_attributes=True)
    category: str
    note: str | None = None

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return _category_gsi_config()


class GsiVSilent(GsiVBase):
    pass


class GsiVOverride(GsiVBase):
    resource_config = ResourceConfig(max_versions=None)


class GsiNSilent(GsiNBase):
    pass


class GsiNOverride(GsiNBase):
    resource_config = ResourceConfig(compress_data=True)


# GSI / TTL attribute names that overlap model fields (must keep working as on main)
def _overlap_gsi_config() -> dict[str, dict]:
    return {"custom": {("resource_id", "created_at"): lambda self: (self.resource_id, self.created_at.isoformat())}}


class ShadowBoth(DynamoDbVersionedResource):
    name: str

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return _overlap_gsi_config()


class ShadowTtl(DynamoDbVersionedResource):
    resource_config = ResourceConfig(ttl_field="expires_at", ttl_attribute_name="created_at")
    name: str
    expires_at: datetime

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return {"custom": {"resource_id": lambda self: self.resource_id}}


class ShadowData(ShadowBoth):
    data: str


class ShadowPlain(DynamoDbResource):
    resource_config = ResourceConfig(compress_data=True)
    name: str

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return _overlap_gsi_config()


class ShadowBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(ttl_field="expires_at", ttl_attribute_name="created_at")
    name: str
    expires_at: datetime

    @classmethod
    def get_gsi_config(cls) -> dict[str, dict]:
        return {"custom": {"resource_id": lambda self: self.resource_id}}


class ShadowChildSilent(ShadowBase):
    pass


class ShadowChildOverride(ShadowBase):
    resource_config = ResourceConfig(max_versions=2)


# Domain bases for the format-flip readers
class NGzBase(DynamoDbResource):
    resource_config = ResourceConfig(compress_data=True)


class VGzBase(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=True, max_versions=5)


class NPlainBase(DynamoDbResource):
    resource_config = ResourceConfig(omit_none_attributes=True)


# Probe-level classes: an uncompressed writer and a compressed reader sharing one prefix
class ProbeBytesPlain(DynamoDbResource):
    name: str
    data: bytes


class ProbeBytesGz(DynamoDbResource):
    resource_config = ResourceConfig(compress_data=True)
    name: str
    data: bytes

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "ProbeBytesPlain"


class HashPrefixed(DynamoDbVersionedResource):
    name: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "tenant#HashPrefixed"


# Legacy writers: each writes in the format a buggy grandchild used, under the reader's keys.
class FlippedBytesCompressed(NGzBase):
    name: str
    data: bytes


class LegacyBytesWriter(DynamoDbResource):
    name: str
    data: bytes

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "FlippedBytesCompressed"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "FlippedBytesCompressed"


class FlippedBytesPlain(NPlainBase):
    name: str
    data: bytes


class LegacyBytesWriterPlain(DynamoDbResource):
    name: str
    data: bytes

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "FlippedBytesPlain"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "FlippedBytesPlain"


# v-gz-to-plain (user `data` field)
class VPlainReaderD(VBase):
    name: str
    data: str


class VGzWriterD(DynamoDbVersionedResource):
    name: str
    data: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "VPlainReaderD"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "VPlainReaderD"


# n-plain-to-gz (user `data` field)
class NGzReaderD(NGzBase):
    name: str
    data: str


class NPlainWriterD(DynamoDbResource):
    name: str
    data: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "NGzReaderD"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "NGzReaderD"


# v-plain-to-gz (user `data` field) -- not producible by #15; the discriminator is symmetric
class VGzReaderD(VGzBase):
    name: str
    data: str


class VPlainWriterD(DynamoDbVersionedResource):
    resource_config = ResourceConfig(compress_data=False)
    name: str
    data: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "VGzReaderD"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "VGzReaderD"


# n-gz-to-plain (user `data` field) -- not producible by #15
class NPlainReaderD(NPlainBase):
    name: str
    data: str


class NGzWriterD(DynamoDbResource):
    resource_config = ResourceConfig(compress_data=True)
    name: str
    data: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "NPlainReaderD"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "NPlainReaderD"


# v-gz-to-plain / n-plain-to-gz without a `data` field
class VPlainReader(VBase):
    name: str


class VGzWriter(DynamoDbVersionedResource):
    name: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "VPlainReader"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "VPlainReader"


class NGzReader(NGzBase):
    name: str


class NPlainWriter(DynamoDbResource):
    name: str

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "NGzReader"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return "NGzReader"


class PlainWithData(DynamoDbResource):
    name: str
    data: str


# Blob migration pair. S3 blob keys use the class ``__name__``, so the writer is given the
# reader's name (it is bound to a different module-level variable).
class BlobMigReader(BlobVBase):
    pass


def _make_blob_mig_writer() -> type[DynamoDbVersionedResource]:
    class BlobMigReader(DynamoDbVersionedResource):  # noqa: F811 - deliberately shares the reader's name
        resource_config = ResourceConfig(
            blob_fields={"body": BlobFieldConfig(compress=True, content_type="text/plain")}
        )
        title: str
        body: str | None = None

    return BlobMigReader


BlobMigWriter = _make_blob_mig_writer()


# (id, writer, reader, has user data field, versioned)
FLIP_PAIRS = [
    pytest.param(VGzWriterD, VPlainReaderD, True, id="v-gz-to-plain"),
    pytest.param(NPlainWriterD, NGzReaderD, True, id="n-plain-to-gz"),
    pytest.param(VPlainWriterD, VGzReaderD, True, id="v-plain-to-gz"),
    pytest.param(NGzWriterD, NPlainReaderD, True, id="n-gz-to-plain"),
    pytest.param(VGzWriter, VPlainReader, False, id="v-gz-to-plain-no-data"),
    pytest.param(NPlainWriter, NGzReader, False, id="n-plain-to-gz-no-data"),
]


@pytest.fixture
def local_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalStorageMemory(logger=logging.getLogger(__name__), storage_dir=tmpdir)


@pytest.fixture(params=["dynamodb", "local"])
def any_memory(request: pytest.FixtureRequest) -> DynamoDbMemory | LocalStorageMemory:
    if request.param == "dynamodb":
        return request.getfixturevalue("dynamodb_memory")
    return request.getfixturevalue("local_storage")


# ---------------------------------------------------------------------------
# 1-10: class-definition tests (no database)
# ---------------------------------------------------------------------------


def test_versioned_grandchild_overriding_one_key_keeps_base_keys() -> None:
    """The exact repro from the issue: restating max_versions kept Base's compress_data."""
    assert VChildOverride.resource_config == {"compress_data": False, "max_versions": None}


def test_versioned_grandchild_without_config_inherits_base() -> None:
    assert VChildSilent.resource_config == {"compress_data": False, "max_versions": 5}


def test_nonversioned_grandchild_without_config_inherits_base() -> None:
    assert NSilent.resource_config == NBase.resource_config
    assert NSilent.resource_config["ttl_field"] == "expires_at"
    assert NSilent.resource_config["compress_data"] is False


def test_nonversioned_grandchild_overriding_one_key_keeps_base_keys() -> None:
    assert NOmitOff.resource_config["ttl_field"] == "expires_at"
    assert NOmitOff.resource_config["ttl_attribute_name"] == "ttl"
    assert NOmitOff.resource_config["omit_none_attributes"] is False


def test_three_level_chain_accumulates() -> None:
    assert VLeaf.resource_config == {"compress_data": False, "max_versions": 2, "omit_none_attributes": True}


def test_direct_subclass_unchanged() -> None:
    assert DirectVersioned.resource_config == {"compress_data": True, "max_versions": 3}
    assert DirectPlain.resource_config == {"compress_data": False}


def test_plain_dict_config_still_merges() -> None:
    assert VPlainDictChild.resource_config == {"compress_data": False, "max_versions": 1}


def test_merge_is_shallow_for_nested_dicts() -> None:
    assert ShallowChild.resource_config["blob_fields"] == {"a": {"compress": False}}


def test_child_config_is_not_parent_object() -> None:
    assert VChildSilent.resource_config is not VBase.resource_config
    try:
        VChildSilent.resource_config["max_versions"] = 99
        assert VBase.resource_config["max_versions"] == 5
    finally:
        VChildSilent.resource_config["max_versions"] = 5


def test_mixin_without_config_is_ignored() -> None:
    assert Mixed.resource_config == VBase.resource_config


# ---------------------------------------------------------------------------
# 11: the probe and the read decision, without a database
# ---------------------------------------------------------------------------


def _probe_obj(data: bytes = b"plain", resource_id: str = "01TESTID") -> ProbeBytesPlain:
    now = datetime.now(timezone.utc)
    return ProbeBytesPlain(resource_id=resource_id, created_at=now, updated_at=now, name="n", data=data)


_OTHER_RECORD = gzip.compress(_probe_obj(resource_id="other-record").model_dump_json().encode())

# (value stored in a user `data: bytes` field, expected probe decode error type or None)
_USER_BYTES_VALUES = [
    pytest.param(b"\x1f\x8bnot-a-gzip-stream", gzip.BadGzipFile, id="gzip-magic-junk"),
    pytest.param(gzip.compress(b"not json"), json.JSONDecodeError, id="gzip-non-json"),
    pytest.param(gzip.compress(b"[1, 2]"), None, id="gzip-list"),
    pytest.param(gzip.compress(b'{"x": 1}'), None, id="gzip-object-no-id"),
    pytest.param(_OTHER_RECORD, None, id="gzip-other-record"),
]


@pytest.mark.parametrize("wrap", [bytes, Binary], ids=["bytes", "Binary"])
@pytest.mark.parametrize("value, decode_error", _USER_BYTES_VALUES)
def test_probe_and_read_decision_table_user_bytes(value: bytes, decode_error: type | None, wrap: type) -> None:
    """Valid uncompressed user data reads through both readers, whatever the config says."""
    item = _probe_obj(value).to_dynamodb_item()
    item["data"] = wrap(value)

    for reader in (ProbeBytesPlain, ProbeBytesGz):
        probe = reader._probe_compressed_envelope(item)
        assert probe.payload is None
        if decode_error is None:
            assert probe.decode_error is None
        else:
            assert isinstance(probe.decode_error, decode_error)

        obj = reader.from_dynamodb_item(item)
        assert obj.data == value
        assert type(obj.data) is bytes
        assert obj.name == "n"


def test_probe_genuine_envelope_and_edge_cases() -> None:
    obj = _probe_obj(b"plain")
    envelope = Binary(gzip.compress(obj.model_dump_json().encode()))
    pk = f"ProbeBytesPlain#{obj.resource_id}"

    # genuine envelope
    probe = ProbeBytesGz._probe_compressed_envelope({"pk": pk, "sk": pk, "data": envelope})
    assert probe.payload is not None and probe.payload["resource_id"] == obj.resource_id
    assert probe.decode_error is None
    # same answer from the uncompressed-configured class: config is not consulted
    assert ProbeBytesPlain._probe_compressed_envelope({"pk": pk, "data": envelope}).payload is not None

    # mismatched resource_id vs pk
    mismatched = ProbeBytesGz._probe_compressed_envelope({"pk": "ProbeBytesPlain#someone-else", "data": envelope})
    assert mismatched == (None, None)

    # missing pk: conditions 1-3 decide
    assert ProbeBytesGz._probe_compressed_envelope({"data": envelope}).payload is not None

    # a prefix that itself contains '#'
    now = datetime.now(timezone.utc)
    hashed = HashPrefixed(resource_id="rid1", version=1, created_at=now, updated_at=now, name="h")
    item = hashed.to_dynamodb_item(v0_object=True)
    assert item["pk"] == "tenant#HashPrefixed#rid1"
    assert HashPrefixed._probe_compressed_envelope(item).payload is not None
    assert HashPrefixed.from_dynamodb_item(item).name == "h"

    # no data / str data / non-gzip bytes
    assert ProbeBytesGz._probe_compressed_envelope({"pk": pk}) == (None, None)
    assert ProbeBytesGz._probe_compressed_envelope({"pk": pk, "data": "text"}) == (None, None)
    assert ProbeBytesGz._probe_compressed_envelope({"pk": pk, "data": b"plain"}) == (None, None)


def test_probe_and_read_corrupt_envelope() -> None:
    """A gzip-looking `data` on an item with no model fields keeps today's errors."""
    pk = "ProbeBytesPlain#01CORRUPT"
    corrupt = {"pk": pk, "sk": pk, "data": Binary(CORRUPT_GZIP)}

    probe = ProbeBytesGz._probe_compressed_envelope(corrupt)
    assert probe.payload is None and isinstance(probe.decode_error, gzip.BadGzipFile)

    with pytest.raises(gzip.BadGzipFile) as exc_info:
        ProbeBytesGz.from_dynamodb_item(corrupt)
    assert isinstance(exc_info.value.__cause__, ValidationError)

    with pytest.raises(ValidationError):
        ProbeBytesPlain.from_dynamodb_item(corrupt)

    non_json = {"pk": pk, "sk": pk, "data": Binary(gzip.compress(b"not json"))}
    with pytest.raises(json.JSONDecodeError):
        ProbeBytesGz.from_dynamodb_item(non_json)


def test_malformed_item_on_compressed_class_raises_validation_error() -> None:
    """Neither an envelope nor model fields: ValidationError (was KeyError: 'data')."""
    with pytest.raises(ValidationError):
        ProbeBytesGz.from_dynamodb_item({"pk": "ProbeBytesPlain#x", "sk": "ProbeBytesPlain#x"})


# ---------------------------------------------------------------------------
# 12: GSI / TTL attribute names overlapping model fields keep round-tripping
# ---------------------------------------------------------------------------


def _check_versioned_roundtrip(
    memory: DynamoDbMemory | LocalStorageMemory,
    cls: type[DynamoDbVersionedResource],
    extra: dict[str, Any],
    oldest_kept: int = 1,
) -> str:
    """create + two updates; every read path returns correct objects. Returns the resource id."""
    created = memory.create_new(cls, {"name": "one", **extra})
    rid = created.resource_id
    v2 = memory.update_existing(created, {"name": "two"})
    memory.update_existing(v2, {"name": "three"})

    current = memory.read_existing(rid, cls)
    assert (current.name, current.version, current.created_at) == ("three", 3, created.created_at)
    oldest = memory.get_existing(rid, cls, version=oldest_kept)
    expected_name = ["one", "two", "three"][oldest_kept - 1]
    assert (oldest.name, oldest.version, oldest.created_at) == (expected_name, oldest_kept, created.created_at)
    all_versions = memory.get_all_versions(rid, cls)
    assert {v.version for v in all_versions} == set(range(oldest_kept, 4))
    assert all(v.created_at == created.created_at for v in all_versions)
    listed = memory.list_type_by_updated_at(cls)
    assert [o.resource_id for o in listed] == [rid]
    assert listed[0].name == "three"
    for key, value in extra.items():
        assert getattr(current, key) == value
    return rid


@pytest.mark.parametrize("cls, extra", [(ShadowBoth, {}), (ShadowData, {"data": "user"})], ids=["12a", "12c"])
def test_compressed_gsi_attrs_named_resource_id_and_created_at(
    any_memory: DynamoDbMemory | LocalStorageMemory, cls: type[DynamoDbVersionedResource], extra: dict[str, Any]
) -> None:
    rid = _check_versioned_roundtrip(any_memory, cls, extra)
    if isinstance(any_memory, DynamoDbMemory):
        v0 = _raw_versioned(any_memory, cls, rid)["v0"]
        assert v0["resource_id"] == rid
        assert isinstance(v0["created_at"], str)
        assert _is_gzip_binary(v0["data"])


def test_compressed_gsi_resource_id_with_ttl_attribute_created_at(
    any_memory: DynamoDbMemory | LocalStorageMemory,
) -> None:
    expires = datetime.now(timezone.utc) + timedelta(days=1)
    rid = _check_versioned_roundtrip(any_memory, ShadowTtl, {"expires_at": expires})
    assert isinstance(any_memory.read_existing(rid, ShadowTtl).created_at, datetime)
    if isinstance(any_memory, DynamoDbMemory):
        v0 = _raw_versioned(any_memory, ShadowTtl, rid)["v0"]
        assert int(v0["created_at"]) == int(expires.timestamp())  # the TTL epoch, not the model field
        assert v0["resource_id"] == rid
        assert _is_gzip_binary(v0["data"])


def test_nonversioned_compressed_overlap(dynamodb_memory: DynamoDbMemory) -> None:
    created = dynamodb_memory.create_new(ShadowPlain, {"name": "one"})
    rid = created.resource_id
    dynamodb_memory.update_existing(created, {"name": "two"})
    current = dynamodb_memory.read_existing(rid, ShadowPlain)
    assert (current.name, current.created_at) == ("two", created.created_at)
    assert [o.name for o in dynamodb_memory.list_type_by_updated_at(ShadowPlain)] == ["two"]

    raw = _raw_nonversioned(dynamodb_memory, ShadowPlain, rid)
    assert raw["resource_id"] == rid and _is_gzip_binary(raw["data"])

    with dynamodb_memory.transaction() as txn:
        txn.put(current.update_existing({"name": "txn"}), optimistic=False)
    assert dynamodb_memory.read_existing(rid, ShadowPlain).name == "txn"
    _raw_nonversioned(dynamodb_memory, ShadowPlain, rid)


@pytest.mark.parametrize("cls", [ShadowChildSilent, ShadowChildOverride])
def test_overlap_through_inheritance(dynamodb_memory: DynamoDbMemory, cls: type[ShadowBase]) -> None:
    expires = datetime.now(timezone.utc) + timedelta(days=1)
    oldest_kept = 2 if cls is ShadowChildOverride else 1
    rid = _check_versioned_roundtrip(dynamodb_memory, cls, {"expires_at": expires}, oldest_kept)
    items = _raw_versioned(dynamodb_memory, cls, rid)
    assert int(items["v0"]["created_at"]) == int(expires.timestamp())
    assert _is_gzip_binary(items["v0"]["data"])
    if cls is ShadowChildOverride:
        assert set(items) == {"v0", "v2", "v3"}
    else:
        assert set(items) == {"v0", "v1", "v2", "v3"}


_FLIP_BYTES_VALUES = [
    b"\x1f\x8bnot-a-gzip-stream",
    gzip.compress(b"not json"),
    gzip.compress(b"[1, 2]"),
    gzip.compress(b'{"x": 1}'),
    b"plain bytes",
]


@pytest.mark.parametrize("value", _FLIP_BYTES_VALUES, ids=["magic-junk", "non-json", "list", "object", "plain"])
def test_flip_to_compressed_reads_legacy_binary_user_data(dynamodb_memory: DynamoDbMemory, value: bytes) -> None:
    """Non-versioned uncompressed -> compressed, the flip #15 produces, with a `data: bytes` field."""
    legacy = dynamodb_memory.create_new(LegacyBytesWriter, {"name": "before", "data": value})
    rid = legacy.resource_id

    raw = _raw_nonversioned(dynamodb_memory, FlippedBytesCompressed, rid)
    assert isinstance(raw["data"], Binary) and bytes(raw["data"]) == value
    assert raw["resource_id"] == rid

    obj = dynamodb_memory.read_existing(rid, FlippedBytesCompressed)
    assert obj.data == value and type(obj.data) is bytes
    assert [o.resource_id for o in dynamodb_memory.list_type_by_updated_at(FlippedBytesCompressed)] == [rid]

    if value == b"plain bytes":
        dynamodb_memory.update_existing(obj, {"name": "after"})
        raw = _raw_nonversioned(dynamodb_memory, FlippedBytesCompressed, rid)
        assert _is_gzip_binary(raw["data"])
        assert json.loads(gzip.decompress(bytes(raw["data"])))["resource_id"] == rid
        assert "name" not in raw
        again = dynamodb_memory.read_existing(rid, FlippedBytesCompressed)
        assert (again.name, again.data) == ("after", value)
    else:
        # Pre-existing limit: pydantic cannot JSON-encode non-UTF-8 bytes, so no compressed
        # class can write this value. The legacy item is left untouched and stays readable.
        with pytest.raises(PydanticSerializationError):
            dynamodb_memory.update_existing(obj, {"name": "after"})
        assert _raw_nonversioned(dynamodb_memory, FlippedBytesCompressed, rid) == raw
        assert dynamodb_memory.read_existing(rid, FlippedBytesCompressed).data == value

    # control: the same legacy data through an uncompressed grandchild
    control = dynamodb_memory.create_new(LegacyBytesWriterPlain, {"name": "ctl", "data": value})
    _raw_nonversioned(dynamodb_memory, FlippedBytesPlain, control.resource_id)
    assert dynamodb_memory.read_existing(control.resource_id, FlippedBytesPlain).data == value


def test_corrupt_envelope_still_raises_on_compressed_class(dynamodb_memory: DynamoDbMemory) -> None:
    for rid, data, error in [
        ("01CORRUPTGZIP", CORRUPT_GZIP, gzip.BadGzipFile),
        ("01CORRUPTJSON", gzip.compress(b"not json"), json.JSONDecodeError),
    ]:
        pk = f"ShadowPlain#{rid}"
        dynamodb_memory.dynamodb_table.put_item(
            Item={"pk": pk, "sk": pk, "gsitype": "ShadowPlain", "gsitypesk": "x", "data": Binary(data)}
        )
        _raw_nonversioned(dynamodb_memory, ShadowPlain, rid)
        with pytest.raises(error):
            dynamodb_memory.get_existing(rid, ShadowPlain)


# ---------------------------------------------------------------------------
# 13-24: every inherited key, through real writes and reads
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [VFmtSilent, VFmtOverride])
def test_grandchild_of_uncompressed_base_writes_uncompressed(
    dynamodb_memory: DynamoDbMemory, cls: type[VBase]
) -> None:
    created = dynamodb_memory.create_new(cls, {"name": "a"})
    dynamodb_memory.update_existing(created, {"name": "b"})
    for item in _raw_versioned(dynamodb_memory, cls, created.resource_id).values():
        assert "data" not in item
        assert "name" in item
    assert dynamodb_memory.read_existing(created.resource_id, cls).name == "b"


def _five_versions(memory: DynamoDbMemory, cls: type[PruneBase]) -> str:
    obj = memory.create_new(cls, {"n": 1})
    for n in range(2, 6):
        obj = memory.update_existing(obj, {"n": n})
    return obj.resource_id


@pytest.mark.parametrize("cls", [PruneSilent, PruneOverrideOther])
def test_inherited_max_versions_prunes(dynamodb_memory: DynamoDbMemory, cls: type[PruneBase]) -> None:
    dynamodb_memory.logger = MagicMock(wraps=dynamodb_memory.logger)
    rid = _five_versions(dynamodb_memory, cls)

    items = _raw_versioned(dynamodb_memory, cls, rid)
    assert set(items) == {"v0", "v4", "v5"}
    assert {r.version for r in dynamodb_memory.get_all_versions(rid, cls)} == {4, 5}
    assert dynamodb_memory.get_existing(rid, cls, version=3) is None
    assert any("Deleted 1 old versions" in str(c) for c in dynamodb_memory.logger.info.call_args_list)
    assert all("data" not in item for item in items.values())


def test_max_versions_none_on_child_disables_inherited_pruning(dynamodb_memory: DynamoDbMemory) -> None:
    rid = _five_versions(dynamodb_memory, PruneDisabled)
    items = _raw_versioned(dynamodb_memory, PruneDisabled, rid)
    assert set(items) == {"v0", "v1", "v2", "v3", "v4", "v5"}
    assert {r.version for r in dynamodb_memory.get_all_versions(rid, PruneDisabled)} == {1, 2, 3, 4, 5}
    assert all("data" not in item for item in items.values())


def _claim(memory: DynamoDbMemory, cls: type[NBase], rid: str) -> None:
    with memory.transaction(auto_retry=False) as txn:
        txn.update(
            cls,
            resource_id=rid,
            updates={"assigned_user_id": "alice"},
            condition="attribute_not_exists(assigned_user_id)",
        )


@pytest.mark.parametrize("cls", [NSilent, NOverrideTtlOnly])
def test_inherited_omit_none_drops_null_attributes_and_allows_claim(
    dynamodb_memory: DynamoDbMemory, cls: type[NBase]
) -> None:
    rid = dynamodb_memory.create_new(cls, {"name": "x"}).resource_id
    assert "assigned_user_id" not in _raw_nonversioned(dynamodb_memory, cls, rid)

    _claim(dynamodb_memory, cls, rid)
    assert dynamodb_memory.get_existing(rid, cls).assigned_user_id == "alice"
    with pytest.raises(TransactionError):
        _claim(dynamodb_memory, cls, rid)


def test_omit_none_override_on_child_restores_null_attributes(dynamodb_memory: DynamoDbMemory) -> None:
    rid = dynamodb_memory.create_new(NOmitOff, {"name": "x"}).resource_id
    raw = _raw_nonversioned(dynamodb_memory, NOmitOff, rid)
    assert "assigned_user_id" in raw and raw["assigned_user_id"] is None
    with pytest.raises(TransactionError):
        _claim(dynamodb_memory, NOmitOff, rid)


@pytest.mark.parametrize("cls, ttl_attr", [(NSilent, "ttl"), (NOverrideTtlOnly, "expires_ttl")])
def test_inherited_ttl_is_written(dynamodb_memory: DynamoDbMemory, cls: type[NBase], ttl_attr: str) -> None:
    expires = datetime.now(timezone.utc) + timedelta(days=1)
    rid = dynamodb_memory.create_new(cls, {"name": "x", "expires_at": expires}).resource_id
    raw = _raw_nonversioned(dynamodb_memory, cls, rid)
    assert abs(int(raw[ttl_attr]) - expires.timestamp()) <= 60
    assert "assigned_user_id" not in raw


def _audited_lifecycle(memory: DynamoDbMemory, cls: type) -> str:
    obj = memory.create_new(cls, {"name": "a"}, changed_by="admin@example.com")
    rid = obj.resource_id
    obj = memory.update_existing(obj, {"name": "b"}, changed_by="admin@example.com")
    if issubclass(cls, DynamoDbVersionedResource):
        _raw_versioned(memory, cls, rid)
    else:
        _raw_nonversioned(memory, cls, rid)
    memory.delete_existing(obj, changed_by="admin@example.com")
    return rid


@pytest.mark.parametrize("cls", [AuditedSilent, AuditedOverride, AuditedVSilent, AuditedVOverride])
def test_inherited_audit_config_emits_logs(dynamodb_memory: DynamoDbMemory, cls: type) -> None:
    rid = _audited_lifecycle(dynamodb_memory, cls)
    logs = AuditLogQuerier(dynamodb_memory).get_logs_for_resource(cls.__name__, rid)
    by_op = {log.operation: log for log in logs}
    assert set(by_op) == {"CREATE", "UPDATE", "DELETE"}
    assert "name" in by_op["UPDATE"].changed_fields
    assert by_op["CREATE"].resource_snapshot


def test_child_can_disable_inherited_audit(dynamodb_memory: DynamoDbMemory) -> None:
    rid = _audited_lifecycle(dynamodb_memory, AuditOffChild)
    assert len(AuditLogQuerier(dynamodb_memory).get_logs_for_resource("AuditOffChild", rid)) == 0


@pytest.mark.parametrize("cls", [BlobVSilent, BlobVOverride])
def test_inherited_blob_fields_offload_versioned(dynamodb_memory_with_s3: DynamoDbMemory, cls: type[BlobVBase]) -> None:
    memory = dynamodb_memory_with_s3
    body = "x" * 5000
    obj = memory.create_new(cls, {"title": "t1", "body": body})
    rid = obj.resource_id
    obj = memory.update_existing(obj, {"title": "t2"})
    obj = memory.update_existing(obj, {"title": "t3"})

    v0 = _raw_versioned(memory, cls, rid)["v0"]
    assert "body" not in v0 and "data" not in v0
    assert v0["_blob_fields"] == ["body"]
    assert memory.get_existing(rid, cls, load_blobs=True).body == body
    assert memory.get_existing(rid, cls, version=1, load_blobs=True).body == body

    memory.update_existing(obj, {"title": "t4"})
    keys = set(_raw_versioned(memory, cls, rid))
    if cls is BlobVSilent:
        assert keys == {"v0", "v2", "v3", "v4"}
    else:
        assert keys == {"v0", "v1", "v2", "v3", "v4"}


@pytest.mark.parametrize("cls", [BlobNSilent, BlobNOverride])
def test_inherited_blob_fields_offload_nonversioned(
    dynamodb_memory_with_s3: DynamoDbMemory, cls: type[BlobNBase]
) -> None:
    memory = dynamodb_memory_with_s3
    payload = {"k": "v" * 2000}
    rid = memory.create_new(cls, {"name": "n", "payload": payload}).resource_id
    raw = _raw_nonversioned(memory, cls, rid)
    assert "payload" not in raw
    assert raw["_blob_fields"] == ["payload"]
    assert memory.get_existing(rid, cls, load_blobs=True).payload == payload


@pytest.mark.parametrize("cls", [GsiVSilent, GsiVOverride])
def test_versioned_grandchild_gsi_query_hits_only_v0(dynamodb_memory: DynamoDbMemory, cls: type[GsiVBase]) -> None:
    obj = dynamodb_memory.create_new(cls, {"category": cls.__name__})
    obj = dynamodb_memory.update_existing(obj, {"category": cls.__name__})
    dynamodb_memory.update_existing(obj, {"category": cls.__name__})

    key = Key("gsi1pk").eq(f"cat#{cls.__name__}")
    results = dynamodb_memory.paginated_dynamodb_query(key_condition=key, index_name="gsi1", resource_class=cls)
    assert [r.version for r in results] == [3]
    assert len(dynamodb_memory.list_type_by_updated_at(cls)) == 1

    items = _raw_versioned(dynamodb_memory, cls, obj.resource_id)
    assert {"gsi1pk", "gsitype", "gsitypesk"} <= set(items["v0"])

    filtered = dynamodb_memory.paginated_dynamodb_query(
        key_condition=key,
        index_name="gsi1",
        resource_class=cls,
        filter_expression=Attr("category").eq(cls.__name__),
    )
    assert len(filtered) == 1  # only possible because the item is uncompressed


@pytest.mark.parametrize("cls", [GsiNSilent, GsiNOverride])
def test_nonversioned_grandchild_gsi_query(dynamodb_memory: DynamoDbMemory, cls: type[GsiNBase]) -> None:
    rid = dynamodb_memory.create_new(cls, {"category": cls.__name__}).resource_id
    key = Key("gsi1pk").eq(f"cat#{cls.__name__}")
    assert len(dynamodb_memory.paginated_dynamodb_query(key_condition=key, index_name="gsi1", resource_class=cls)) == 1

    raw = _raw_nonversioned(dynamodb_memory, cls, rid)
    assert "gsi1pk" in raw
    if cls is GsiNSilent:
        assert "note" not in raw
        filtered = dynamodb_memory.paginated_dynamodb_query(
            key_condition=key,
            index_name="gsi1",
            resource_class=cls,
            filter_expression=Attr("category").eq(cls.__name__),
        )
        assert len(filtered) == 1
    else:
        assert _is_gzip_binary(raw["data"])
        assert "category" not in raw
        assert dynamodb_memory.get_existing(rid, cls).category == cls.__name__


# ---------------------------------------------------------------------------
# 25-27: reading items written under the old (wrong) config
# ---------------------------------------------------------------------------


def _assert_current_format(raw: dict[str, Any], reader: type, has_data: bool) -> None:
    if reader.resource_config["compress_data"]:
        assert "resource_id" not in raw
        assert _is_gzip_binary(raw["data"])
    else:
        assert "resource_id" in raw
        if has_data:
            assert isinstance(raw["data"], str)
        else:
            assert "data" not in raw


@pytest.mark.parametrize("writer, reader, has_data", FLIP_PAIRS)
def test_reads_legacy_format_after_flip(dynamodb_memory: DynamoDbMemory, writer: type, reader: type, has_data: bool) -> None:
    versioned = issubclass(reader, DynamoDbVersionedResource)
    extra = {"data": "user-data"} if has_data else {}
    legacy = dynamodb_memory.create_new(writer, {"name": "one", **extra})
    rid = legacy.resource_id
    legacy = dynamodb_memory.update_existing(legacy, {"name": "two"})
    if versioned:
        legacy = dynamodb_memory.update_existing(legacy, {"name": "three"})

    obj = dynamodb_memory.read_existing(rid, reader)
    assert type(obj) is reader
    assert (obj.name, obj.resource_id, obj.created_at, obj.updated_at) == (
        legacy.name,
        rid,
        legacy.created_at,
        legacy.updated_at,
    )
    if has_data:
        assert obj.data == "user-data"
    if versioned:
        assert {v.version for v in dynamodb_memory.get_all_versions(rid, reader)} == {1, 2, 3}
        assert dynamodb_memory.get_existing(rid, reader, version=1).name == "one"
    else:
        assert [o.resource_id for o in dynamodb_memory.list_type_by_updated_at(reader)] == [rid]

    dynamodb_memory.update_existing(obj, {"name": "after"})
    assert dynamodb_memory.read_existing(rid, reader).name == "after"

    if versioned:
        items = _raw_versioned(dynamodb_memory, reader, rid)
        _assert_current_format(items["v0"], reader, has_data)
        _assert_current_format(items["v4"], reader, has_data)
        # history stays in the legacy (writer's) format and still reads
        _assert_current_format(items["v1"], writer, has_data)
        assert dynamodb_memory.get_existing(rid, reader, version=2).name == "two"
    else:
        _assert_current_format(_raw_nonversioned(dynamodb_memory, reader, rid), reader, has_data)


@pytest.mark.parametrize("writer, reader", [(VGzWriterD, VPlainReaderD), (NPlainWriterD, NGzReaderD)])
def test_reads_legacy_format_via_local_storage(local_storage: LocalStorageMemory, writer: type, reader: type) -> None:
    legacy = local_storage.create_new(writer, {"name": "one", "data": "user-data"})
    obj = local_storage.read_existing(legacy.resource_id, reader)
    assert (obj.name, obj.data, obj.created_at) == ("one", "user-data", legacy.created_at)
    local_storage.update_existing(obj, {"name": "after"})
    again = local_storage.read_existing(legacy.resource_id, reader)
    assert (again.name, again.data) == ("after", "user-data")


def test_uncompressed_data_field_unchanged(dynamodb_memory: DynamoDbMemory) -> None:
    created = dynamodb_memory.create_new(PlainWithData, {"name": "n", "data": "user"})
    raw = _raw_nonversioned(dynamodb_memory, PlainWithData, created.resource_id)
    assert raw["data"] == "user"
    obj = dynamodb_memory.read_existing(created.resource_id, PlainWithData)
    assert (obj.name, obj.data) == ("n", "user")


# ---------------------------------------------------------------------------
# 28: the documented migration recipe (copied verbatim from AGENT_KNOWLEDGE.md)
# ---------------------------------------------------------------------------


def _collect_ids(
    memory: DynamoDbMemory,
    cls: type[DynamoDbResource] | type[DynamoDbVersionedResource],
    page_size: int = 250,
) -> list[str]:
    """Phase 1: snapshot every current resource id of ``cls``, following every page.

    Read-only. Never mutate while paginating: rewriting bumps updated_at, the gsitype
    index's sort key, and so reorders the very index being paged.
    """
    return [
        obj.resource_id
        for page in exhaust_pagination(
            lambda key: memory.list_type_by_updated_at(cls, results_limit=page_size, pagination_key=key)
        )
        for obj in page
    ]


def _rewrite_ids(
    memory: DynamoDbMemory,
    cls: type[DynamoDbResource] | type[DynamoDbVersionedResource],
    resource_ids: list[str],
) -> int:
    """Phase 2: recheck each id by primary key and rewrite only what still needs it."""
    want_compressed = bool(cls.resource_config.get("compress_data"))
    versioned = issubclass(cls, DynamoDbVersionedResource)
    rewritten = 0
    for resource_id in resource_ids:
        pk = f"{cls.get_unique_key_prefix()}#{resource_id}"
        sk = "v0" if versioned else pk
        raw = memory.dynamodb_table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=True).get("Item")
        if raw is None:
            continue  # deleted since phase 1
        if (cls._probe_compressed_envelope(raw).payload is not None) == want_compressed:
            continue  # already in the configured format (possibly rewritten by a concurrent write)
        current = memory.read_existing(resource_id, cls, consistent_read=True)
        try:
            memory.update_existing(current, {})  # full-item write in the corrected format
        except ConflictError:
            continue  # versioned: a concurrent update won, and it wrote the corrected format
        except PydanticSerializationError:
            # e.g. a non-UTF-8 `bytes` field cannot be JSON-compressed (pre-existing limit);
            # the item stays readable in its legacy uncompressed format.
            memory.logger.warning(f"Cannot rewrite {cls.__name__} {resource_id} compressed; left as-is")
            continue
        rewritten += 1
    return rewritten


def migrate_resource_format(
    memory: DynamoDbMemory,
    cls: type[DynamoDbResource] | type[DynamoDbVersionedResource],
    page_size: int = 250,
) -> int:
    """Rewrite every current item of ``cls`` not stored in ``cls``'s configured format.

    Returns the number of items rewritten. Safe to rerun.
    """
    return _rewrite_ids(memory, cls, _collect_ids(memory, cls, page_size))


@pytest.mark.parametrize(
    "writer, reader, values",
    [
        pytest.param(VGzWriterD, VPlainReaderD, [f"user-{i}" for i in range(7)], id="v-gz-to-plain"),
        pytest.param(LegacyBytesWriter, FlippedBytesCompressed, [f"user-{i}".encode() for i in range(7)], id="n-plain-to-gz"),
    ],
)
def test_migration_recipe_across_pages(
    dynamodb_memory: DynamoDbMemory, writer: type, reader: type, values: list[Any]
) -> None:
    versioned = issubclass(reader, DynamoDbVersionedResource)
    # Interleave legacy (even) and already-converted (odd) items in updated_at order.
    ids = [
        dynamodb_memory.create_new(writer if i % 2 == 0 else reader, {"name": "mig", "data": value}).resource_id
        for i, value in enumerate(values)
    ]
    unserializable_id = None
    if not versioned:
        unserializable_id = dynamodb_memory.create_new(writer, {"name": "mig", "data": b"\x1f\x8b\xff"}).resource_id

    if versioned:
        before = dynamodb_memory.list_type_by_updated_at(reader, filter_expression=Attr("name").eq("mig"))
        assert sorted(o.resource_id for o in before) == sorted(ids[1::2])  # legacy gzipped items are invisible

    assert migrate_resource_format(dynamodb_memory, reader, page_size=3) == 4

    for rid, value in zip(ids, values):
        if versioned:
            raw = _raw_versioned(dynamodb_memory, reader, rid)["v0"]
            assert raw["name"] == "mig" and raw["data"] == value
        else:
            raw = _raw_nonversioned(dynamodb_memory, reader, rid)
            assert reader._probe_compressed_envelope(raw).payload is not None
        obj = dynamodb_memory.read_existing(rid, reader)
        assert (obj.name, obj.data) == ("mig", value)

    if versioned:
        after = dynamodb_memory.list_type_by_updated_at(reader, filter_expression=Attr("name").eq("mig"))
        assert len(after) == 7
        for i, rid in enumerate(ids):
            expected = {1, 2} if i % 2 == 0 else {1}
            assert {v.version for v in dynamodb_memory.get_all_versions(rid, reader)} == expected
            assert dynamodb_memory.get_existing(rid, reader, version=1).data == values[i]
    else:
        # Skipped (not counted), left unchanged, still readable.
        raw = _raw_nonversioned(dynamodb_memory, reader, unserializable_id)
        assert bytes(raw["data"]) == b"\x1f\x8b\xff" and raw["name"] == "mig"
        assert dynamodb_memory.read_existing(unserializable_id, reader).data == b"\x1f\x8b\xff"

    assert migrate_resource_format(dynamodb_memory, reader, page_size=3) == 0


def test_migration_recipe_preserves_blobs(dynamodb_memory_with_s3: DynamoDbMemory) -> None:
    memory = dynamodb_memory_with_s3
    body = "b" * 5000
    ids = [memory.create_new(BlobMigWriter, {"title": f"t{i}", "body": body}).resource_id for i in range(2)]
    assert _is_gzip_binary(_raw_versioned(memory, BlobMigReader, ids[0])["v0"]["data"])

    assert migrate_resource_format(memory, BlobMigReader, page_size=1) == 2

    for rid in ids:
        v0 = _raw_versioned(memory, BlobMigReader, rid)["v0"]
        assert "data" not in v0 and "title" in v0
        assert memory.get_existing(rid, BlobMigReader, load_blobs=True).body == body
    assert migrate_resource_format(memory, BlobMigReader) == 0


def test_migration_recipe_skips_concurrently_updated_versioned_item(dynamodb_memory: DynamoDbMemory) -> None:
    legacy_ids = [dynamodb_memory.create_new(VGzWriterD, {"name": "n", "data": f"d{i}"}).resource_id for i in range(3)]
    collected = _collect_ids(dynamodb_memory, VPlainReaderD)
    assert sorted(collected) == sorted(legacy_ids)

    concurrent = dynamodb_memory.read_existing(legacy_ids[0], VPlainReaderD)
    dynamodb_memory.update_existing(concurrent, {"name": "concurrent"})

    assert _rewrite_ids(dynamodb_memory, VPlainReaderD, collected) == len(legacy_ids) - 1
