"""GSI configuration shapes observed across the library's downstream consumers.

These come from an audit of the largest codebases built on this library, recorded here as
shapes rather than as named systems. They are pinned as tests so that changes to
index-name resolution are checked against how the library is actually used, not only
against cases invented while editing it.

Two infrastructure conventions are in play, and both are load-bearing:

* **undashed** index names (``gsi1``/``gsi2``/``gsi3``), where gsi1 and gsi2 range on the
  table's own ``pk`` so no ``gsi1sk``/``gsi2sk`` attribute exists, and only gsi3 and beyond
  carry their own sort attribute; and
* **dashed** index names (``gsi-1``/``gsi-2``/``gsi-3``), which keep the index name distinct
  from the attributes it indexes, and where every index has its own sort attribute.

The outer key of a ``gsi_config`` entry is a label, not an identifier: consumers variously
use the deployed index name, a descriptive access-pattern name, or a label that names one
index while declaring another's attributes. The index name supplied at query time is what
identifies the index.
"""

import pytest

from simplesingletable.dynamodb_memory import build_lek_data


def _resource_with(gsi_config):
    return type("ObservedResource", (), {"get_gsi_config": classmethod(lambda cls: gsi_config)})


def _value(*_args):
    """Stand-in for the callables real configs use; only the keys matter here."""
    return None


TYPE_INDEX_ATTRS = {"gsitype": "ObservedResource", "gsitypesk": "2026-01-01T00:00:00"}

UNDASHED_ITEM = {
    "pk": "ObservedResource#01ABC", "sk": "v0", **TYPE_INDEX_ATTRS,
    "gsi1pk": "<gsi1pk>", "gsi2pk": "<gsi2pk>",
    "gsi3pk": "<gsi3pk>", "gsi3sk": "<gsi3sk>",
    "gsi4pk": "<gsi4pk>", "gsi4sk": "<gsi4sk>",
    "gsi5pk": "<gsi5pk>", "gsi5sk": "<gsi5sk>",
}

DASHED_ITEM = {
    "pk": "ObservedResource#01ABC", "sk": "v0", **TYPE_INDEX_ATTRS,
    "gsi1pk": "<gsi1pk>", "gsi1sk": "<gsi1sk>",
    "gsi2pk": "<gsi2pk>", "gsi2sk": "<gsi2sk>",
    "gsi3pk": "<gsi3pk>", "gsi3sk": "<gsi3sk>",
}


UNDASHED_PATTERNS = [
    # (shape, gsi_config, index queried, attributes expected in the synthesized key)
    (
        "two pk-only indexes, one of them sparse",
        {"gsi1": {"gsi1pk": _value}, "gsi2": {"gsi2pk": _value}},
        "gsi2", ["gsi2pk"],
    ),
    (
        "pk-only index alongside one with its own sort attribute",
        {"gsi1": {"gsi1pk": _value}, "gsi3": {"gsi3pk": _value, "gsi3sk": _value}},
        "gsi3", ["gsi3pk", "gsi3sk"],
    ),
    (
        "tuple-declared pk/sk pair",
        {"gsi2": {"gsi2pk": _value}, "gsi3": {("gsi3pk", "gsi3sk"): _value}},
        "gsi3", ["gsi3pk", "gsi3sk"],
    ),
    (
        "three indexes, the last tuple-declared",
        {"gsi1": {"gsi1pk": _value}, "gsi2": {"gsi2pk": _value}, "gsi3": {("gsi3pk", "gsi3sk"): _value}},
        "gsi1", ["gsi1pk"],
    ),
    (
        "a fourth index, beyond the three the library originally assumed",
        {"gsi3": {"gsi3pk": _value, "gsi3sk": _value}, "gsi4": {"gsi4pk": _value, "gsi4sk": _value}},
        "gsi4", ["gsi4pk", "gsi4sk"],
    ),
    (
        "five indexes, two of them sparse and pk-only",
        {"gsi3": {"gsi3pk": _value, "gsi3sk": _value},
         "gsi4": {"gsi4pk": _value, "gsi4sk": _value},
         "gsi1": {"gsi1pk": _value},
         "gsi2": {"gsi2pk": _value},
         "gsi5": {"gsi5pk": _value, "gsi5sk": _value}},
        "gsi5", ["gsi5pk", "gsi5sk"],
    ),
    (
        "two tuple-declared pairs",
        {"gsi3": {("gsi3pk", "gsi3sk"): _value}, "gsi4": {("gsi4pk", "gsi4sk"): _value}},
        "gsi4", ["gsi4pk", "gsi4sk"],
    ),
    (
        "no gsi_config at all -- legacy db_get_gsiNpk overrides",
        {}, "gsi1", ["gsi1pk"],
    ),
    (
        "no gsi_config at all -- legacy db_get_gsi3pk_and_sk override",
        {}, "gsi3", ["gsi3pk", "gsi3sk"],
    ),
]

DASHED_PATTERNS = [
    (
        "label is the dashed index name, tuple-declared pair",
        {"gsi-1": {("gsi1pk", "gsi1sk"): _value}},
        "gsi-1", ["gsi1pk", "gsi1sk"],
    ),
    (
        "label is the dashed index name, separately declared pk and sk",
        {"gsi-3": {"gsi3pk": _value, "gsi3sk": _value}},
        "gsi-3", ["gsi3pk", "gsi3sk"],
    ),
    (
        "three dashed indexes on one resource, all tuple-declared",
        {"gsi-1": {("gsi1pk", "gsi1sk"): _value},
         "gsi-2": {("gsi2pk", "gsi2sk"): _value},
         "gsi-3": {("gsi3pk", "gsi3sk"): _value}},
        "gsi-2", ["gsi2pk", "gsi2sk"],
    ),
    (
        "single sparse dashed index",
        {"gsi-3": {("gsi3pk", "gsi3sk"): _value}},
        "gsi-3", ["gsi3pk", "gsi3sk"],
    ),
    (
        "undashed label while the deployed index name is dashed",
        {"gsi3": {"gsi3pk": _value, "gsi3sk": _value}},
        "gsi-3", ["gsi3pk", "gsi3sk"],
    ),
    (
        "dashed index names with no gsi_config at all",
        {}, "gsi-1", ["gsi1pk", "gsi1sk"],
    ),
    (
        "descriptive label naming no index at all",
        {"by-owner": {"gsi1pk": _value, "gsi1sk": _value}},
        "gsi-1", ["gsi1pk", "gsi1sk"],
    ),
]


@pytest.mark.parametrize("shape,gsi_config,index_name,expected", UNDASHED_PATTERNS)
def test_undashed_index_names(shape, gsi_config, index_name, expected):
    lek = build_lek_data(UNDASHED_ITEM, index_name, _resource_with(gsi_config))

    assert lek == {"pk": UNDASHED_ITEM["pk"], "sk": "v0", **{a: UNDASHED_ITEM[a] for a in expected}}, shape


@pytest.mark.parametrize("shape,gsi_config,index_name,expected", DASHED_PATTERNS)
def test_dashed_index_names(shape, gsi_config, index_name, expected):
    lek = build_lek_data(DASHED_ITEM, index_name, _resource_with(gsi_config))

    assert lek == {"pk": DASHED_ITEM["pk"], "sk": "v0", **{a: DASHED_ITEM[a] for a in expected}}, shape


@pytest.mark.parametrize("item", [UNDASHED_ITEM, DASHED_ITEM])
def test_built_in_type_index_is_unaffected(item):
    """Every consumer queries gsitype, usually for batch sweeps."""
    lek = build_lek_data(item, "gsitype", _resource_with({"gsi1": {"gsi1pk": _value}}))

    assert lek == {"pk": item["pk"], "sk": "v0", **TYPE_INDEX_ATTRS}
