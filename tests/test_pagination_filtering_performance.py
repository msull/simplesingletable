from boto3.dynamodb.conditions import Attr
from simplesingletable import DynamoDbMemory, DynamoDbResource


class FilterTestResource(DynamoDbResource):
    """Resource for testing filter performance."""

    category: str
    value: int


#
# def test_heavy_filtering_api_calls(dynamodb_memory: DynamoDbMemory):
#     """
#     Tests learned multiplier optimization with heavy filtering.
#
#     With 20% filter match rate and requesting 100 items:
#     - Without optimization: Would be 7-8 API calls
#     - With learned multiplier: Only 2-3 API calls
#     """
#     # Create 1000 items where only 20% match our filter
#     for i in range(50):
#         dynamodb_memory.create_new(
#             FilterTestResource,
#             {
#                 "category": "match" if i % 5 == 0 else "no_match",  # 20% match rate
#                 "value": i,
#             },
#         )
#
#     # Request 25 matching items with heavy filtering
#     result = dynamodb_memory.list_type_by_updated_at(
#         FilterTestResource,
#         filter_expression=Attr("category").eq("match"),
#         results_limit=1000,
#     )
#
#     # Verify we got the right data
#     assert len(result) == 10
#     assert all(r.category == "match" for r in result)
#
#     print(f"\nAPI calls made: {result.api_calls_made}")
#     print(f"Items returned: {len(result)}")
#     print(f"RCUs consumed: {result.rcus_consumed_by_query}")
#     print(f"Total scanned: {result.total_items_scanned}")
#     print(f"Filter efficiency: {result.filter_efficiency:.2%}")
#
#     # Verify learned multiplier optimization is working
#     assert 0.19 < result.filter_efficiency < 0.21, "Should be around 20%"
#     assert result.total_items_scanned > len(result), "Should scan more items than returned"
#     assert result.api_calls_made == 2, "Should only need 2 API calls with learned multiplier"
#


def test_extreme_filtering_performance(dynamodb_memory: DynamoDbMemory):
    """Test with very selective filter (5% match rate)."""
    for i in range(100):
        dynamodb_memory.create_new(
            FilterTestResource,
            {
                "category": "match" if i % 20 == 0 else "no_match",  # 5% match rate
                "value": i,
            },
        )

    result = dynamodb_memory.list_type_by_updated_at(
        FilterTestResource,
        filter_expression=Attr("category").eq("match"),
        results_limit=5,
        max_api_calls=20,
    )
    print(f"\nWith 5% filter rate - API calls: {result.api_calls_made}")
    assert len(result) == 5
    assert result.api_calls_made == 2
    assert all(r.category == "match" for r in result)
    if hasattr(result, "filter_efficiency"):
        assert 0.04 < result.filter_efficiency < 0.06, "Should be around 5%"
    # Before optimization, this would be 10+ calls


def test_no_filtering_unchanged_behavior(dynamodb_memory: DynamoDbMemory):
    """Ensure optimization doesn't affect queries without filters."""
    # Create 300 items
    for i in range(20):
        dynamodb_memory.create_new(
            FilterTestResource,
            {
                "category": "all",
                "value": i,
            },
        )

    # Query without any filter
    result = dynamodb_memory.list_type_by_updated_at(
        FilterTestResource,
        results_limit=20,
    )

    assert len(result) == 20
    if hasattr(result, "total_items_scanned"):
        assert result.total_items_scanned == 20
    if hasattr(result, "filter_efficiency"):
        assert result.filter_efficiency is None
    assert result.api_calls_made == 1
    print(f"\nNo filtering - API calls: {result.api_calls_made}")
