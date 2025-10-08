"""Tests for float type support in DynamoDB resources.

DynamoDB doesn't natively support float types - they must be converted to Decimal.
This test suite verifies that our automatic float-to-Decimal conversion works correctly.
"""

from decimal import Decimal
from typing import Optional, List
from simplesingletable.models import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.dynamodb_memory import DynamoDbMemory


class SimpleFloatResource(DynamoDbResource):
    """Basic non-versioned resource with float fields."""

    name: str
    price: float
    quantity: int
    discount: Optional[float] = None


class ComplexFloatResource(DynamoDbVersionedResource):
    """Versioned resource with various float configurations."""

    name: str
    price: float
    rating: float
    measurements: dict  # Can contain nested structures with floats
    scores: List[float]
    optional_weight: Optional[float] = None

    # Enable compression to test float handling with compressed data
    resource_config = {"compress_data": True}


class MixedTypeResource(DynamoDbResource):
    """Resource with mixed numeric types to ensure proper handling."""

    int_field: int
    float_field: float
    complex_data: dict
    mixed_list: list


class UncompressedFloatResource(DynamoDbVersionedResource):
    """Versioned resource without compression to show different float behavior."""

    name: str
    price: float
    data_dict: dict
    float_list: List[float]

    # Explicitly disable compression
    resource_config = {"compress_data": False}


def test_basic_float_support(dynamodb_memory: DynamoDbMemory):
    """Test basic float field creation, retrieval, and updates."""

    # Create resource with float fields
    resource = dynamodb_memory.create_new(
        SimpleFloatResource, {"name": "Product A", "price": 19.99, "quantity": 100, "discount": 0.15}
    )

    # Verify float values are preserved
    assert resource.price == 19.99
    assert resource.discount == 0.15
    assert isinstance(resource.price, float)
    assert isinstance(resource.discount, float)

    # Read back from database
    retrieved = dynamodb_memory.read_existing(resource.resource_id, SimpleFloatResource)

    assert retrieved.price == 19.99
    assert retrieved.discount == 0.15

    # Update float fields
    updated = dynamodb_memory.update_existing(resource, {"price": 24.99, "discount": 0.20})

    assert updated.price == 24.99
    assert updated.discount == 0.20


def test_float_precision_edge_cases(dynamodb_memory: DynamoDbMemory):
    """Test various float values including edge cases for precision."""

    test_cases = [
        ("zero", 0.0),
        ("negative", -99.99),
        ("small_positive", 0.0001),
        ("small_negative", -0.0001),
        ("large", 1234567.89),
        ("scientific_small", 1e-10),
        ("scientific_large", 1e10),
        ("repeating_decimal", 1 / 3),  # 0.3333...
        ("pi_approximation", 3.14159265359),
        ("max_precision", 0.123456789012345),
    ]

    for name, value in test_cases:
        resource = dynamodb_memory.create_new(
            SimpleFloatResource, {"name": f"Test {name}", "price": value, "quantity": 1}
        )

        retrieved = dynamodb_memory.read_existing(resource.resource_id, SimpleFloatResource)

        # For repeating decimals, we need to be more lenient with comparison
        if name == "repeating_decimal":
            assert abs(retrieved.price - value) < 1e-15
        else:
            assert retrieved.price == value, f"Failed for {name}: expected {value}, got {retrieved.price}"


def test_nested_float_structures(dynamodb_memory: DynamoDbMemory):
    """Test floats in nested dictionaries and lists.

    Note: For compressed resources, floats are preserved through JSON serialization.
    For non-compressed resources, floats in dicts return as Decimal.
    List[float] fields are always properly converted back to float.
    """

    resource = dynamodb_memory.create_new(
        ComplexFloatResource,
        {
            "name": "Complex Product",
            "price": 99.99,
            "rating": 4.5,
            "measurements": {
                "weight": 2.5,
                "height": 10.25,
                "depth": 5.0,
                "nested": {"inner_value": 3.14159, "another_float": 2.71828},
            },
            "scores": [1.1, 2.2, 3.3, 4.4, 5.5],
            "optional_weight": 1.25,
        },
    )

    # Verify all nested floats are preserved
    assert resource.price == 99.99
    assert resource.measurements["weight"] == 2.5
    assert resource.measurements["nested"]["inner_value"] == 3.14159
    assert resource.scores == [1.1, 2.2, 3.3, 4.4, 5.5]
    assert resource.optional_weight == 1.25

    # Read back and verify
    retrieved = dynamodb_memory.read_existing(resource.resource_id, ComplexFloatResource)

    assert retrieved.price == 99.99
    # For compressed resources (like ComplexFloatResource), floats are preserved
    # through JSON serialization/deserialization
    assert retrieved.measurements["weight"] == 2.5
    assert retrieved.measurements["nested"]["inner_value"] == 3.14159
    # List[float] fields are properly converted back to float
    assert retrieved.scores[2] == 3.3
    assert isinstance(retrieved.scores[2], float)


def test_versioned_resource_float_updates(dynamodb_memory: DynamoDbMemory):
    """Test float handling across multiple versions of a versioned resource."""

    # Create initial version
    resource = dynamodb_memory.create_new(
        ComplexFloatResource,
        {
            "name": "Versioned Product",
            "price": 50.00,
            "rating": 3.5,
            "measurements": {"weight": 1.0},
            "scores": [1.0, 2.0, 3.0],
        },
    )

    assert resource.version == 1
    assert resource.price == 50.00

    # Update to create version 2
    v2 = dynamodb_memory.update_existing(resource, {"price": 45.99, "rating": 4.0, "scores": [1.5, 2.5, 3.5, 4.5]})

    assert v2.version == 2
    assert v2.price == 45.99
    assert v2.rating == 4.0
    assert len(v2.scores) == 4

    # Create version 3
    v3 = dynamodb_memory.update_existing(v2, {"price": 39.99, "rating": 4.5, "optional_weight": 0.75})

    assert v3.version == 3
    assert v3.price == 39.99
    assert v3.optional_weight == 0.75

    # Verify all versions maintain their float values
    v1_read = dynamodb_memory.read_existing(resource.resource_id, ComplexFloatResource, version=1)
    v2_read = dynamodb_memory.read_existing(resource.resource_id, ComplexFloatResource, version=2)
    v3_read = dynamodb_memory.read_existing(resource.resource_id, ComplexFloatResource, version=3)

    assert v1_read.price == 50.00
    assert v1_read.rating == 3.5
    # For compressed resources, floats in dicts are preserved
    assert v1_read.measurements["weight"] == 1.0
    assert v1_read.scores == [1.0, 2.0, 3.0]  # List[float] properly converted
    assert all(isinstance(s, float) for s in v1_read.scores)

    assert v2_read.price == 45.99
    assert v2_read.rating == 4.0
    assert v2_read.scores == [1.5, 2.5, 3.5, 4.5]

    assert v3_read.price == 39.99
    assert v3_read.rating == 4.5
    assert v3_read.optional_weight == 0.75


def test_mixed_numeric_types(dynamodb_memory: DynamoDbMemory):
    """Test that int and float types are handled correctly when mixed."""

    resource = dynamodb_memory.create_new(
        MixedTypeResource,
        {
            "int_field": 42,
            "float_field": 3.14,
            "complex_data": {"an_int": 100, "a_float": 99.99, "nested": {"int_val": 5, "float_val": 5.5}},
            "mixed_list": [1, 1.1, 2, 2.2, "string", True, 3.3],
        },
    )

    # Verify types are preserved correctly
    assert resource.int_field == 42
    assert isinstance(resource.int_field, int)

    assert resource.float_field == 3.14
    assert isinstance(resource.float_field, float)

    assert resource.complex_data["an_int"] == 100
    assert resource.complex_data["a_float"] == 99.99

    # Check mixed list maintains types
    assert resource.mixed_list[0] == 1  # int
    assert resource.mixed_list[1] == 1.1  # float
    assert resource.mixed_list[4] == "string"  # string
    assert resource.mixed_list[5] is True  # bool

    # Read back and verify
    retrieved = dynamodb_memory.read_existing(resource.resource_id, MixedTypeResource)

    assert retrieved.int_field == 42
    assert retrieved.float_field == 3.14

    # In mixed lists (not typed as List[float]), floats come back as Decimal from DynamoDB
    # This is expected behavior since the list type is generic
    assert retrieved.mixed_list[1] == Decimal("1.1")
    assert retrieved.mixed_list[6] == Decimal("3.3")
    assert isinstance(retrieved.mixed_list[1], Decimal)

    # Similarly for nested dicts
    assert retrieved.complex_data["a_float"] == Decimal("99.99")
    assert retrieved.complex_data["nested"]["float_val"] == Decimal("5.5")

    # Other types should be preserved
    assert retrieved.mixed_list[0] == 1  # int
    assert retrieved.mixed_list[4] == "string"  # string
    assert retrieved.mixed_list[5] is True  # bool


def test_null_and_optional_floats(dynamodb_memory: DynamoDbMemory):
    """Test handling of null/None values for optional float fields."""

    # Create without optional field
    resource = dynamodb_memory.create_new(SimpleFloatResource, {"name": "No Discount", "price": 29.99, "quantity": 50})

    assert resource.discount is None

    # Read back
    retrieved = dynamodb_memory.read_existing(resource.resource_id, SimpleFloatResource)

    assert retrieved.discount is None

    # Update to add discount
    with_discount = dynamodb_memory.update_existing(resource, {"discount": 0.10})

    assert with_discount.discount == 0.10

    # Update to remove discount
    no_discount = dynamodb_memory.update_existing(with_discount, {"discount": None})

    assert no_discount.discount is None


def test_float_clear_fields(dynamodb_memory: DynamoDbMemory):
    """Test clearing float fields using the clear_fields parameter."""

    # Create with all fields
    resource = dynamodb_memory.create_new(
        SimpleFloatResource, {"name": "Clear Test", "price": 19.99, "quantity": 10, "discount": 0.25}
    )

    assert resource.discount == 0.25

    # Clear the discount field
    cleared = dynamodb_memory.update_existing(resource, {"price": 21.99}, clear_fields={"discount"})

    assert cleared.price == 21.99
    assert cleared.discount is None

    # Verify in database
    retrieved = dynamodb_memory.read_existing(resource.resource_id, SimpleFloatResource)

    assert retrieved.discount is None
    assert retrieved.price == 21.99


def test_compressed_resource_with_floats(dynamodb_memory: DynamoDbMemory):
    """Test that float conversion works correctly with compressed resources."""

    # ComplexFloatResource has compression enabled
    resource = dynamodb_memory.create_new(
        ComplexFloatResource,
        {
            "name": "Compressed with Floats",
            "price": 199.99,
            "rating": 4.8,
            "measurements": {"weight": 5.5, "volume": 10.25},
            "scores": [9.1, 9.2, 9.3, 9.4, 9.5],
        },
    )

    # Verify floats work with compression
    assert resource.price == 199.99
    assert resource.rating == 4.8
    assert resource.measurements["weight"] == 5.5

    # Read back (will decompress)
    retrieved = dynamodb_memory.read_existing(resource.resource_id, ComplexFloatResource)

    assert retrieved.price == 199.99
    assert retrieved.measurements["volume"] == 10.25
    assert retrieved.scores[-1] == 9.5

    # Update compressed resource
    updated = dynamodb_memory.update_existing(resource, {"price": 189.99, "rating": 4.9})

    assert updated.price == 189.99
    assert updated.rating == 4.9


def test_float_in_gsi_operations(dynamodb_memory: DynamoDbMemory):
    """Test that resources with floats work correctly with GSI queries."""

    # Create multiple resources
    resources = []
    for i in range(3):
        resources.append(
            dynamodb_memory.create_new(
                ComplexFloatResource,
                {
                    "name": f"Product {i}",
                    "price": 10.0 + (i * 5.5),  # 10.0, 15.5, 21.0
                    "rating": 3.0 + (i * 0.5),  # 3.0, 3.5, 4.0
                    "measurements": {"weight": float(i + 1)},
                    "scores": [float(j) for j in range(i + 1)],
                },
            )
        )

    # Query all by type (uses gsitype index)
    all_products = dynamodb_memory.list_type_by_updated_at(ComplexFloatResource, ascending=True)

    assert len(all_products) == 3

    # Verify float values are preserved in query results
    for i, product in enumerate(all_products):
        expected_price = 10.0 + (i * 5.5)
        assert product.price == expected_price
        assert product.rating == 3.0 + (i * 0.5)


def test_uncompressed_vs_compressed_float_behavior(dynamodb_memory: DynamoDbMemory):
    """Test the difference in float handling between compressed and uncompressed resources."""

    # Test uncompressed resource
    uncompressed = dynamodb_memory.create_new(
        UncompressedFloatResource,
        {
            "name": "Uncompressed",
            "price": 19.99,
            "data_dict": {"weight": 2.5, "nested": {"value": 3.14}},
            "float_list": [1.1, 2.2, 3.3],
        },
    )

    # Read back uncompressed
    retrieved_uncompressed = dynamodb_memory.read_existing(uncompressed.resource_id, UncompressedFloatResource)

    # For uncompressed resources:
    # - Top-level float fields work (Pydantic handles conversion)
    assert retrieved_uncompressed.price == 19.99
    # - Dict values come back as Decimal
    assert retrieved_uncompressed.data_dict["weight"] == Decimal("2.5")
    assert retrieved_uncompressed.data_dict["nested"]["value"] == Decimal("3.14")
    # - List[float] fields are converted back to float
    assert retrieved_uncompressed.float_list == [1.1, 2.2, 3.3]
    assert all(isinstance(x, float) for x in retrieved_uncompressed.float_list)

    # Test compressed resource (ComplexFloatResource)
    compressed = dynamodb_memory.create_new(
        ComplexFloatResource,
        {
            "name": "Compressed",
            "price": 19.99,
            "rating": 4.5,
            "measurements": {"weight": 2.5, "nested": {"value": 3.14}},
            "scores": [1.1, 2.2, 3.3],
        },
    )

    # Read back compressed
    retrieved_compressed = dynamodb_memory.read_existing(compressed.resource_id, ComplexFloatResource)

    # For compressed resources:
    # - All float values are preserved through JSON serialization
    assert retrieved_compressed.price == 19.99
    assert retrieved_compressed.measurements["weight"] == 2.5
    assert retrieved_compressed.measurements["nested"]["value"] == 3.14
    assert retrieved_compressed.scores == [1.1, 2.2, 3.3]


def test_float_serialization_internals(dynamodb_memory: DynamoDbMemory):
    """Test the internal serialization to ensure floats are converted to Decimal."""

    resource = SimpleFloatResource.create_new(
        {"name": "Internal Test", "price": 99.99, "quantity": 1, "discount": 0.05}
    )

    # Get the DynamoDB item representation
    db_item = resource.to_dynamodb_item()

    # Check that floats were converted to Decimal in the serialized form
    assert isinstance(db_item["price"], Decimal)
    assert isinstance(db_item["discount"], Decimal)

    # Verify the Decimal values match the original floats
    assert float(db_item["price"]) == 99.99
    assert float(db_item["discount"]) == 0.05

    # Ensure integers remain as integers
    assert isinstance(db_item["quantity"], int)
    assert db_item["quantity"] == 1
