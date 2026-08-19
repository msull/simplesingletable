"""Tests for GSI index names that use a separator (``gsi-1``) rather than matching the
attribute prefix (``gsi1pk`` / ``gsi1sk``).

Both spellings name the same index. Writes and first-page queries never noticed the
difference -- gsi_config dict keys are only iterated as ``.values()`` on the write path,
and index_name is passed verbatim to ``dynamodb.query(IndexName=...)`` -- so the mismatch
only surfaced in ``build_lek_data``, on the secondary path that synthesizes a
LastEvaluatedKey when client-side filtering truncates a page.
"""

from uuid import uuid4

import boto3
import pytest
from logzero import logger
from boto3.dynamodb.conditions import Attr, Key

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.dynamodb_memory import build_lek_data
from simplesingletable.utils import normalize_index_name


class DescriptiveLabelResource(DynamoDbResource):
    """gsi_config keyed by a descriptive label, not by the index name."""

    owner_id: str
    status: str

    @classmethod
    def get_gsi_config(cls):
        return {
            "by-owner": {
                "gsi1pk": lambda self: f"things#{self.owner_id}",
                "gsi1sk": lambda self: self.resource_id,
            },
        }


class IndexNameLabelResource(DynamoDbResource):
    """gsi_config keyed by the dashed index name itself."""

    owner_id: str
    status: str

    @classmethod
    def get_gsi_config(cls):
        return {
            "gsi-1": {
                "gsi1pk": lambda self: f"things#{self.owner_id}",
                "gsi1sk": lambda self: self.resource_id,
            },
        }


class TupleKeyResource(DynamoDbResource):
    """gsi_config declaring its pk/sk pair as a tuple."""

    owner_id: str

    @classmethod
    def get_gsi_config(cls):
        return {
            "by-priority": {
                ("gsi3pk", "gsi3sk"): lambda self: (f"owner#{self.owner_id}", self.resource_id),
            },
        }


DB_ITEM = {
    "pk": "DescriptiveLabelResource#abc",
    "sk": "v0",
    "gsi1pk": "things#owner-1",
    "gsi1sk": "abc",
}


class TestNormalizeIndexName:
    @pytest.mark.parametrize(
        "index_name,expected",
        [("gsi-1", "gsi1"), ("gsi1", "gsi1"), ("GSI-1", "gsi1"), ("gsi_1", "gsi1"), ("gsitype", "gsitype")],
    )
    def test_spellings_reduce_to_the_attribute_prefix(self, index_name, expected):
        assert normalize_index_name(index_name) == expected


class TestBuildLekData:
    def test_dashed_index_with_descriptive_labels(self):
        """The reported failure: raised RuntimeError('Unsupported index gsi-1')."""
        lek = build_lek_data(DB_ITEM, "gsi-1", DescriptiveLabelResource)

        assert lek == DB_ITEM

    def test_dashed_index_with_matching_dashed_label(self):
        """The reported near-miss: derived 'gsi-1pk', found nothing, returned a key that
        silently restarted pagination from the beginning."""
        lek = build_lek_data(DB_ITEM, "gsi-1", IndexNameLabelResource)

        assert lek["gsi1pk"] == "things#owner-1"
        assert lek["gsi1sk"] == "abc"

    def test_undashed_index_with_descriptive_labels(self):
        lek = build_lek_data(DB_ITEM, "gsi1", DescriptiveLabelResource)

        assert lek == DB_ITEM

    def test_tuple_declared_keys(self):
        db_item = {"pk": "TupleKeyResource#abc", "sk": "v0", "gsi3pk": "owner#1", "gsi3sk": "abc"}

        assert build_lek_data(db_item, "gsi-3", TupleKeyResource) == db_item

    def test_no_index_returns_table_key_only(self):
        assert build_lek_data(DB_ITEM, None, DescriptiveLabelResource) == {
            "pk": DB_ITEM["pk"],
            "sk": DB_ITEM["sk"],
        }

    def test_gsitype_index(self):
        db_item = {"pk": "p", "sk": "v0", "gsitype": "DescriptiveLabelResource", "gsitypesk": "2026-01-01"}

        assert build_lek_data(db_item, "gsitype", DescriptiveLabelResource) == db_item

    def test_gsi1_sort_key_is_the_table_pk(self):
        """gsi1/gsi2 range on the table's own pk, so no gsi1sk attribute exists."""
        db_item = {"pk": "p", "sk": "v0", "gsi1pk": "task|COMPLETE"}

        assert build_lek_data(db_item, "gsi-1", DescriptiveLabelResource) == db_item

    def test_lek_never_carries_more_than_one_pk_and_sk(self):
        """A LEK with attributes outside the index's key schema is rejected by DynamoDB."""
        db_item = dict(DB_ITEM, gsi2pk="other", gsi3pk="other", gsi3sk="other")

        lek = build_lek_data(db_item, "gsi-1", DescriptiveLabelResource)

        assert set(lek) == {"pk", "sk", "gsi1pk", "gsi1sk"}

    def test_genuinely_unknown_index_still_raises(self):
        with pytest.raises(RuntimeError, match="Unsupported index"):
            build_lek_data(DB_ITEM, "not-an-index", DescriptiveLabelResource)


@pytest.fixture()
def memory_with_dashed_index(dynamodb_via_docker) -> DynamoDbMemory:
    """A table whose GSI is genuinely named ``gsi-1``, indexing ``gsi1pk``/``gsi1sk``.

    The shared test table names its indexes without a separator, so it cannot exercise the
    mismatch this module is about.
    """
    connection_params = {
        "aws_access_key_id": "unused",
        "aws_secret_access_key": "unused",
        "region_name": "us-west-2",
    }
    table_name = f"dashed-gsi-test-table-{uuid4().hex}"
    client = boto3.client("dynamodb", endpoint_url=dynamodb_via_docker, **connection_params)
    resource = boto3.resource("dynamodb", endpoint_url=dynamodb_via_docker, **connection_params)

    resource.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi1sk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-1",
                "KeySchema": [
                    {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resource.Table(table_name).wait_until_exists()

    try:
        yield DynamoDbMemory(
            logger=logger,
            table_name=table_name,
            endpoint_url=dynamodb_via_docker,
            connection_params=connection_params,
        )
    finally:
        client.delete_table(TableName=table_name)


class TestPaginationAgainstDynamoDb:
    def test_filtered_query_on_dashed_index_paginates(self, memory_with_dashed_index: DynamoDbMemory):
        """End to end: a filter that rejects enough rows forces the library to synthesize
        the LEK itself, which is the only path that reads the index name."""
        for idx in range(10):
            memory_with_dashed_index.create_new(
                DescriptiveLabelResource,
                {"owner_id": "owner-1", "status": "active" if idx % 2 == 0 else "archived"},
            )

        seen = []
        pagination_key = None
        for _ in range(5):
            page = memory_with_dashed_index.paginated_dynamodb_query(
                key_condition=Key("gsi1pk").eq("things#owner-1"),
                index_name="gsi-1",
                resource_class=DescriptiveLabelResource,
                filter_expression=Attr("status").eq("active"),
                results_limit=2,
                pagination_key=pagination_key,
            )
            seen.extend(resource.resource_id for resource in page)
            pagination_key = page.next_pagination_key
            if not pagination_key:
                break

        assert len(seen) == 5, "every active row should be seen exactly once"
        assert len(set(seen)) == 5, "pagination restarted and re-served rows"


def _pre_fix_build_lek_data(db_item, index_name, resource_class):
    """``build_lek_data`` exactly as it stood before dashed index names were handled.

    Kept verbatim as the reference point for the compatibility invariant below.
    """
    lek_data = {"pk": db_item["pk"], "sk": db_item["sk"]}
    if not index_name:
        return lek_data
    if index_name == "gsitype":
        if "gsitype" in db_item:
            lek_data["gsitype"] = db_item["gsitype"]
        if "gsitypesk" in db_item:
            lek_data["gsitypesk"] = db_item["gsitypesk"]
        return lek_data
    gsi_config = resource_class.get_gsi_config()
    if index_name in gsi_config:
        pk_field = f"{index_name}pk"
        if pk_field in db_item:
            lek_data[pk_field] = db_item[pk_field]
        sk_field = f"{index_name}sk"
        if sk_field in db_item:
            lek_data[sk_field] = db_item[sk_field]
        return lek_data
    if index_name in ["gsi1", "gsi2", "gsi3"]:
        pk_field = f"{index_name}pk"
        if pk_field in db_item:
            lek_data[pk_field] = db_item[pk_field]
        if index_name == "gsi3":
            sk_field = f"{index_name}sk"
            if sk_field in db_item:
                lek_data[sk_field] = db_item[sk_field]
        return lek_data
    raise RuntimeError(f"Unsupported index {index_name=}")


def _resource_with(gsi_config):
    return type("MatrixResource", (), {"get_gsi_config": classmethod(lambda cls: gsi_config)})


MATRIX_CONFIGS = {
    "no config (legacy db_get_gsiNpk resources)": {},
    "keyed by legacy index name": {
        "gsi1": {"gsi1pk": lambda s: None},
        "gsi3": {"gsi3pk": lambda s: None, "gsi3sk": lambda s: None},
    },
    "keyed by legacy name, tuple pair": {"gsi3": {("gsi3pk", "gsi3sk"): lambda s: None}},
    "descriptive labels": {"by-owner": {"gsi1pk": lambda s: None, "gsi1sk": lambda s: None}},
    "dashed label": {"gsi-1": {"gsi1pk": lambda s: None, "gsi1sk": lambda s: None}},
    "custom attributes, dashed label": {"gsi-1": {"ownerpk": lambda s: None, "ownersk": lambda s: None}},
    "custom attributes, descriptive label": {"by-owner": {"ownerpk": lambda s: None, "ownersk": lambda s: None}},
}

MATRIX_ITEMS = {
    # gsi1/gsi2 range on the table pk, so a standard item carries no gsi1sk/gsi2sk
    "standard table item": {
        "pk": "R#1", "sk": "v0", "gsitype": "R", "gsitypesk": "t",
        "gsi1pk": "a", "gsi2pk": "b", "gsi3pk": "c", "gsi3sk": "d",
    },
    "custom attributes only": {"pk": "R#1", "sk": "v0", "ownerpk": "o", "ownersk": "s"},
    "custom attributes plus gsi1pk": {"pk": "R#1", "sk": "v0", "ownerpk": "o", "ownersk": "s", "gsi1pk": "a"},
}

MATRIX_INDEXES = [None, "gsitype", "gsi1", "gsi2", "gsi3", "gsi-1", "gsi-3", "by-owner", "unknown-index"]


class TestBackwardsCompatibility:
    """Whenever the pre-fix code produced a usable key, the current code must produce the
    identical key. It may only differ where the old behavior was a RuntimeError or a
    key so truncated it would have restarted pagination."""

    @pytest.mark.parametrize("config_name", list(MATRIX_CONFIGS))
    @pytest.mark.parametrize("item_name", list(MATRIX_ITEMS))
    @pytest.mark.parametrize("index_name", MATRIX_INDEXES)
    def test_usable_keys_are_unchanged(self, config_name, item_name, index_name):
        resource_class = _resource_with(MATRIX_CONFIGS[config_name])
        db_item = MATRIX_ITEMS[item_name]

        try:
            previous = _pre_fix_build_lek_data(db_item, index_name, resource_class)
        except RuntimeError:
            return  # old code refused outright; anything the new code does is an improvement

        # A key of pk/sk alone carries no index attribute: DynamoDB would have restarted
        # the query from the beginning, so it was never a usable key to preserve.
        if len(previous) <= 2 and index_name is not None:
            return

        assert build_lek_data(db_item, index_name, resource_class) == previous

    def test_legacy_index_names_are_never_resolved_by_normalization(self):
        """`gsi1` and `gsi-1` are distinct, legal index names; a table may have both, so a
        config entry labeled for one must not capture queries against the other."""
        resource_class = _resource_with({"gsi-1": {"gsi2pk": lambda s: None}})
        db_item = {"pk": "R#1", "sk": "v0", "gsi1pk": "a", "gsi2pk": "b"}

        assert build_lek_data(db_item, "gsi1", resource_class) == {"pk": "R#1", "sk": "v0", "gsi1pk": "a"}

    def test_label_naming_one_index_does_not_speak_for_another(self):
        """Labels are arbitrary documentation, so a label may name one index while declaring
        another's attributes. The index name supplied at query time decides."""
        resource_class = _resource_with({"gsi1": {"gsi2pk": lambda s: None}})
        db_item = {"pk": "R#1", "sk": "v0", "gsi1pk": "a", "gsi2pk": "b"}

        assert build_lek_data(db_item, "gsi1", resource_class) == {"pk": "R#1", "sk": "v0", "gsi1pk": "a"}
        assert build_lek_data(db_item, "gsi2", resource_class) == {"pk": "R#1", "sk": "v0", "gsi2pk": "b"}

    def test_declared_attributes_resolve_an_index_the_convention_cannot(self):
        """The one case the index name alone cannot answer: attributes that do not follow
        the naming convention. Then the config entry's own keys are the only evidence."""
        resource_class = _resource_with({"gsi-1": {"ownerpk": lambda s: None, "ownersk": lambda s: None}})
        db_item = {"pk": "R#1", "sk": "v0", "ownerpk": "o", "ownersk": "s"}

        assert build_lek_data(db_item, "gsi-1", resource_class) == db_item

    def test_partition_and_sort_attributes_come_from_one_source(self):
        """Mixing a conventional pk with a declared sk would synthesize a key that never
        existed; the first group the item carries supplies both."""
        resource_class = _resource_with({"gsi-1": {"ownerpk": lambda s: None, "ownersk": lambda s: None}})
        db_item = {"pk": "R#1", "sk": "v0", "gsi1pk": "a", "ownerpk": "o", "ownersk": "s"}

        lek = build_lek_data(db_item, "gsi-1", resource_class)

        assert lek == {"pk": "R#1", "sk": "v0", "gsi1pk": "a"}, "conventional attributes win when present"

    def test_legacy_name_honors_a_declared_sort_attribute(self):
        """A config declaring gsi1sk means the resource writes one, so gsi1 ranges on it."""
        resource_class = _resource_with({"by-owner": {"gsi1pk": lambda s: None, "gsi1sk": lambda s: None}})
        db_item = {"pk": "R#1", "sk": "v0", "gsi1pk": "a", "gsi1sk": "b"}

        assert build_lek_data(db_item, "gsi1", resource_class) == db_item
