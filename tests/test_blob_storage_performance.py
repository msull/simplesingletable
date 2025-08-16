"""
Performance comparison tests for blob storage vs regular storage.

This test demonstrates the RCU and query time benefits of using blob storage
for large fields by comparing queries on resources with and without blob storage.

NOTE: These tests are marked as 'benchmark' and are EXCLUDED by default when running pytest.

Usage:
    # Run regular tests (default - excludes benchmarks)
    pytest

    # Run ONLY benchmark/performance tests
    pytest -m benchmark
    pytest tests/test_blob_storage_performance.py -m benchmark

    # Run ALL tests including benchmarks (override default exclusion)
    pytest -m ""
"""

import time
from typing import Optional
import pytest
from simplesingletable import DynamoDbResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig


class LargeResourceWithoutBlob(DynamoDbResource):
    """Resource storing large data directly in DynamoDB."""

    name: str
    large_content: str  # Large field stored directly in DynamoDB

    resource_config = ResourceConfig(
        compress_data=False,  # No compression for fair comparison
        blob_fields={},  # No blob storage
    )


class LargeResourceWithBlob(DynamoDbResource):
    """Resource storing large data in S3 via blob storage."""

    name: str
    large_content: Optional[str] = None  # Large field stored in S3

    resource_config = ResourceConfig(
        compress_data=False,  # No compression for fair comparison
        blob_fields={
            "large_content": BlobFieldConfig(
                compress=False,  # No compression for fair comparison
                content_type="text/plain",
            )
        },
    )


@pytest.mark.benchmark
class TestBlobStoragePerformance:
    """Test performance benefits of blob storage."""

    @pytest.mark.slow
    def test_rcu_and_query_time_comparison(self, dynamodb_memory_with_s3):
        """Compare RCU consumption and query time between blob and non-blob storage."""
        memory = dynamodb_memory_with_s3
        num_items = 5000

        # Generate large content (approximately 10KB per item)
        # DynamoDB items have a 400KB limit, but large items consume more RCUs
        large_content = "X" * 10000  # 10KB of data

        print("\n\n=== Blob Storage Performance Comparison ===\n")

        # Create items WITHOUT blob storage (storing large data in DynamoDB)
        print(f"Creating {num_items} items WITHOUT blob storage...")
        start_time = time.time()
        without_blob_ids = []
        for i in range(num_items):
            resource = memory.create_new(
                LargeResourceWithoutBlob, {"name": f"without_blob_{i}", "large_content": large_content}
            )
            without_blob_ids.append(resource.resource_id)

        create_time_without_blob = (time.time() - start_time) * 1000
        print(f"  - Time to create {num_items} WITHOUT blob storage: {create_time_without_blob:.2f} ms")
        print(f"  - Average per item: {create_time_without_blob/num_items:.2f} ms")

        # Create items WITH blob storage (storing large data in S3)
        print(f"Creating {num_items} items WITH blob storage...")
        start_time = time.time()
        with_blob_ids = []
        for i in range(num_items):
            resource = memory.create_new(
                LargeResourceWithBlob, {"name": f"with_blob_{i}", "large_content": large_content}
            )
            with_blob_ids.append(resource.resource_id)
        create_time_with_blob = (time.time() - start_time) * 1000
        print(f"  - Time to create {num_items} WITH blob storage: {create_time_with_blob:.2f} ms")
        print(f"  - Average per item: {create_time_with_blob/num_items:.2f} ms")

        # Query all items WITHOUT blob storage
        print("\nQuerying items WITHOUT blob storage...")
        start_time = time.time()
        from boto3.dynamodb.conditions import Key

        result_without_blob = memory.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(LargeResourceWithoutBlob.__name__),
            index_name="gsitype",
            resource_class=LargeResourceWithoutBlob,
            results_limit=10000,  # Get all items in one query
            max_api_calls=1000,
        )
        query_time_without_blob = (time.time() - start_time) * 1000  # Convert to ms

        # Query all items WITH blob storage (without loading blobs)
        print("Querying items WITH blob storage (blobs not loaded)...")
        start_time = time.time()
        result_with_blob = memory.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(LargeResourceWithBlob.__name__),
            index_name="gsitype",
            resource_class=LargeResourceWithBlob,
            results_limit=10000,  # Get all items in one query
            max_api_calls=1000,
        )
        query_time_with_blob = (time.time() - start_time) * 1000  # Convert to ms

        # Display results
        print("\n=== RESULTS ===\n")
        print("Items WITHOUT blob storage:")
        print(f"  - Number of items retrieved: {len(result_without_blob)}")
        print(f"  - API Calls required: {result_without_blob.api_calls_made}")
        print(f"  - RCUs consumed: {result_without_blob.rcus_consumed_by_query}")
        print(f"  - Query time (DynamoDB): {result_without_blob.query_time_ms:.2f} ms")
        print(f"  - Total query time: {query_time_without_blob:.2f} ms")
        print("  - Average item size: ~10KB")

        print("\nItems WITH blob storage:")
        print(f"  - Number of items retrieved: {len(result_with_blob)}")
        print(f"  - API Calls required: {result_with_blob.api_calls_made}")
        print(f"  - RCUs consumed: {result_with_blob.rcus_consumed_by_query}")
        print(f"  - Query time (DynamoDB): {result_with_blob.query_time_ms:.2f} ms")
        print(f"  - Total query time: {query_time_with_blob:.2f} ms")
        print("  - Average item size: ~1KB (metadata only, blob in S3)")

        # Calculate savings
        rcu_savings = result_without_blob.rcus_consumed_by_query - result_with_blob.rcus_consumed_by_query
        rcu_savings_percent = (rcu_savings / result_without_blob.rcus_consumed_by_query) * 100

        time_savings = result_without_blob.query_time_ms - result_with_blob.query_time_ms
        time_savings_percent = (time_savings / result_without_blob.query_time_ms) * 100

        print("\n=== PERFORMANCE COMPARISON SUMMARY ===")
        print("\nCreation Performance:")
        print(f"  - WITHOUT blob storage: {create_time_without_blob:.2f} ms for {num_items} items")
        print(f"  - WITH blob storage: {create_time_with_blob:.2f} ms for {num_items} items")
        if create_time_with_blob > create_time_without_blob:
            overhead = create_time_with_blob - create_time_without_blob
            overhead_percent = (overhead / create_time_without_blob) * 100
            print(f"  - Blob storage creation overhead: {overhead:.2f} ms ({overhead_percent:.1f}% slower)")
            print(f"  - Note: This overhead is due to S3 uploads but provides significant query benefits")
        else:
            savings = create_time_without_blob - create_time_with_blob
            savings_percent = (savings / create_time_without_blob) * 100
            print(f"  - Blob storage creation savings: {savings:.2f} ms ({savings_percent:.1f}% faster)")
        
        print("\nQuery Performance Savings:")
        print(f"  - RCU savings: {rcu_savings} RCUs ({rcu_savings_percent:.1f}% reduction)")
        print(f"  - Query time savings: {time_savings:.2f} ms ({time_savings_percent:.1f}% faster)")
        print(f"  - API calls: {result_without_blob.api_calls_made} (without blobs) vs {result_with_blob.api_calls_made} (with blobs)")

        # Assertions to verify blob storage benefits
        assert len(result_without_blob) == num_items
        assert len(result_with_blob) == num_items

        # Blob storage should consume significantly fewer RCUs
        # Each 4KB of data consumes 1 RCU for eventually consistent reads
        # With 10KB items, we expect ~3 RCUs per item without blobs
        # With blob storage, items are ~1KB, so ~1 RCU per 4 items
        assert result_with_blob.rcus_consumed_by_query < result_without_blob.rcus_consumed_by_query

        # Query time should also be faster with blob storage due to smaller items
        assert result_with_blob.query_time_ms < result_without_blob.query_time_ms

        print("\n✅ Test passed: Blob storage significantly reduces RCU consumption and query time!")

        # Optional: Test loading blobs for a subset
        print("\n=== BONUS: Loading blobs for 10 items ===")
        subset_with_blobs = result_with_blob[:10]

        start_time = time.time()
        for item in subset_with_blobs:
            item.load_blob_fields(memory)
        load_time = (time.time() - start_time) * 1000

        print(f"  - Time to load 10 blobs from S3: {load_time:.2f} ms")
        print(f"  - Average per blob: {load_time/10:.2f} ms")

        # Verify blobs were loaded correctly
        for item in subset_with_blobs:
            assert item.large_content == large_content

        print("\n✅ Blob loading verification passed!")
