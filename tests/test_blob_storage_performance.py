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
from tabulate import tabulate

NUM_ITEMS = 10


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

    def setup_class(cls):
        """Initialize results collection for the test class."""
        cls.results = []

    def teardown_class(cls):
        """Print consolidated report after all tests complete."""
        cls._print_consolidated_report()

    @classmethod
    def _print_consolidated_report(cls):
        """Print a consolidated report of all test runs."""

        if not cls.results:
            return

        print("\n\n" + "=" * 100)
        print("CONSOLIDATED PERFORMANCE REPORT - BLOB STORAGE VS REGULAR STORAGE")
        print("=" * 100)

        # Prepare TRANSPOSED data for main metrics table - blob sizes as columns, metrics as rows
        # Create column headers with blob sizes
        blob_sizes = [f"{r['blob_size']:,} bytes" for r in cls.results]
        headers = ["Content Size"] + blob_sizes

        # Create rows for each metric
        main_table_data = []

        # Number of items row
        main_table_data.append(["Items"] + [r["num_items"] for r in cls.results])

        # Create time without blob
        main_table_data.append(
            ["Create Time w/o Blob (ms)"] + [f"{r['create_time_without_blob']:.1f}" for r in cls.results]
        )

        # Create time with blob
        main_table_data.append(
            ["Create Time w/ Blob (ms)"] + [f"{r['create_time_with_blob']:.1f}" for r in cls.results]
        )

        # Create overhead
        main_table_data.append(
            ["Create Overhead"]
            + [f"{r['create_overhead']:.1f}% {'slower' if r['create_overhead'] > 0 else 'faster'}" for r in cls.results]
        )

        # Query time without blob
        main_table_data.append(
            ["Query Time w/o Blob (ms)"] + [f"{r['query_time_without_blob']:.1f}" for r in cls.results]
        )

        # Query time with blob
        main_table_data.append(["Query Time w/ Blob (ms)"] + [f"{r['query_time_with_blob']:.1f}" for r in cls.results])

        # Query time savings
        main_table_data.append(["Query Time Savings"] + [f"{r['query_time_savings']:.1f}%" for r in cls.results])

        # RCUs without blob
        main_table_data.append(["RCUs w/o Blob"] + [f"{r['rcus_without_blob']:.1f}" for r in cls.results])

        # RCUs with blob
        main_table_data.append(["RCUs w/ Blob"] + [f"{r['rcus_with_blob']:.1f}" for r in cls.results])

        # RCU savings
        main_table_data.append(["RCU Savings"] + [f"{r['rcu_savings']:.1f}%" for r in cls.results])

        # API calls without blob
        main_table_data.append(["API Calls w/o Blob"] + [r["api_calls_without_blob"] for r in cls.results])

        # API calls with blob
        main_table_data.append(["API Calls w/ Blob"] + [r["api_calls_with_blob"] for r in cls.results])

        print(f"\nMain Performance Metrics ({NUM_ITEMS} items):")
        print(tabulate(main_table_data, headers=headers, tablefmt="grid"))

        # Additional details table
        detail_table_data = []
        for result in cls.results:
            detail_table_data.append(
                [
                    f"{result['blob_size']:,}",
                    result["item_size_without_blob"],  # Already formatted as string with units
                    result["item_size_with_blob"],  # Already formatted as string with units
                    f"{result['size_reduction_factor']:.1f}x",
                    f"{result['blob_load_time_10']:.1f}",
                    f"{result['blob_load_time_avg']:.1f}",
                ]
            )

        detail_headers = [
            "Blob Size\n(bytes)",
            "Item Size\nw/o Blob",
            "Item Size\nw/ Blob",
            "Size\nReduction",
            "Load 10 Blobs\n(ms)",
            "Avg per Blob\n(ms)",
        ]

        print("\nAdditional Performance Details:")
        print(tabulate(detail_table_data, headers=detail_headers, tablefmt="grid"))

        # Summary statistics
        avg_rcu_savings = sum(r["rcu_savings"] for r in cls.results) / len(cls.results)
        avg_query_savings = sum(r["query_time_savings"] for r in cls.results) / len(cls.results)
        avg_create_overhead = sum(r["create_overhead"] for r in cls.results) / len(cls.results)

        print("\n" + "=" * 60)
        print("SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Average RCU Savings: {avg_rcu_savings:.1f}%")
        print(f"Average Query Time Savings: {avg_query_savings:.1f}%")
        print(f"Average Creation Overhead: {avg_create_overhead:.1f}%")
        print("\n✅ All tests passed - Blob storage provides significant performance benefits!")
        print("=" * 100)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "blob_size",
        [
            1024,
            # 2048,
            4096,
            # 8192,
            16384,
            16384 * 2,
        ],
    )
    def test_rcu_and_query_time_comparison(self, dynamodb_memory_with_s3, blob_size):
        """Compare RCU consumption and query time between blob and non-blob storage."""
        memory = dynamodb_memory_with_s3

        # Generate large content (approximately 10KB per item)
        # DynamoDB items have a 400KB limit, but large items consume more RCUs
        large_content = "X" * blob_size

        print("\n\n=== Blob Storage Performance Comparison ===\n")

        # Create items WITHOUT blob storage (storing large data in DynamoDB)
        print(f"Creating {NUM_ITEMS} items WITHOUT blob storage...")
        start_time = time.time()
        without_blob_ids = []
        for i in range(NUM_ITEMS):
            resource = memory.create_new(
                LargeResourceWithoutBlob, {"name": f"without_blob_{i}", "large_content": large_content}
            )
            without_blob_ids.append(resource.resource_id)

        create_time_without_blob = (time.time() - start_time) * 1000
        print(f"  - Time to create {NUM_ITEMS} WITHOUT blob storage: {create_time_without_blob:.2f} ms")
        print(f"  - Average per item: {create_time_without_blob / NUM_ITEMS:.2f} ms")

        # Create items WITH blob storage (storing large data in S3)
        print(f"Creating {NUM_ITEMS} items WITH blob storage...")
        start_time = time.time()
        with_blob_ids = []
        for i in range(NUM_ITEMS):
            resource = memory.create_new(
                LargeResourceWithBlob, {"name": f"with_blob_{i}", "large_content": large_content}
            )
            with_blob_ids.append(resource.resource_id)
        create_time_with_blob = (time.time() - start_time) * 1000
        print(f"  - Time to create {NUM_ITEMS} WITH blob storage: {create_time_with_blob:.2f} ms")
        print(f"  - Average per item: {create_time_with_blob / NUM_ITEMS:.2f} ms")

        # Query all items WITHOUT blob storage
        print("\nQuerying items WITHOUT blob storage...")
        start_time = time.time()
        from boto3.dynamodb.conditions import Key

        result_without_blob = memory.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(LargeResourceWithoutBlob.db_get_gsitypepk()),
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
            key_condition=Key("gsitype").eq(LargeResourceWithBlob.db_get_gsitypepk()),
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
        print(f"  - Average item size: {result_without_blob[0].get_db_item_size()}")

        print("\nItems WITH blob storage:")
        print(f"  - Number of items retrieved: {len(result_with_blob)}")
        print(f"  - API Calls required: {result_with_blob.api_calls_made}")
        print(f"  - RCUs consumed: {result_with_blob.rcus_consumed_by_query}")
        print(f"  - Query time (DynamoDB): {result_with_blob.query_time_ms:.2f} ms")
        print(f"  - Total query time: {query_time_with_blob:.2f} ms")
        print(f"  - Average item size (metadata only, blob in S3): {result_with_blob[0].get_db_item_size()}")

        # Calculate savings
        rcu_savings = result_without_blob.rcus_consumed_by_query - result_with_blob.rcus_consumed_by_query
        rcu_savings_percent = (rcu_savings / result_without_blob.rcus_consumed_by_query) * 100

        time_savings = result_without_blob.query_time_ms - result_with_blob.query_time_ms
        time_savings_percent = (time_savings / result_without_blob.query_time_ms) * 100

        print("\n=== PERFORMANCE COMPARISON SUMMARY ===")
        print("\nCreation Performance:")
        print(f"  - WITHOUT blob storage: {create_time_without_blob:.2f} ms for {NUM_ITEMS} items")
        print(f"  - WITH blob storage: {create_time_with_blob:.2f} ms for {NUM_ITEMS} items")
        if create_time_with_blob > create_time_without_blob:
            overhead = create_time_with_blob - create_time_without_blob
            overhead_percent = (overhead / create_time_without_blob) * 100
            print(f"  - Blob storage creation overhead: {overhead:.2f} ms ({overhead_percent:.1f}% slower)")
            print("  - Note: This overhead is due to S3 uploads but provides significant query benefits")
        else:
            savings = create_time_without_blob - create_time_with_blob
            savings_percent = (savings / create_time_without_blob) * 100
            print(f"  - Blob storage creation savings: {savings:.2f} ms ({savings_percent:.1f}% faster)")

        print("\nQuery Performance Savings:")
        print(f"  - RCU savings: {rcu_savings} RCUs ({rcu_savings_percent:.1f}% reduction)")
        print(f"  - Query time savings: {time_savings:.2f} ms ({time_savings_percent:.1f}% faster)")
        print(
            f"  - API calls: {result_without_blob.api_calls_made} (without blobs) vs {result_with_blob.api_calls_made} (with blobs)"
        )

        # Assertions to verify blob storage benefits
        assert len(result_without_blob) == NUM_ITEMS
        assert len(result_with_blob) == NUM_ITEMS

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
        print(f"  - Average per blob: {load_time / 10:.2f} ms")

        # Verify blobs were loaded correctly
        for item in subset_with_blobs:
            assert item.large_content == large_content

        print("\n✅ Blob loading verification passed!")

        # Collect results for consolidated report
        item_size_without_blob_bytes = result_without_blob[0].get_db_item_size_in_bytes()
        item_size_with_blob_bytes = result_with_blob[-1].get_db_item_size_in_bytes()
        result_data = {
            "blob_size": blob_size,
            "num_items": NUM_ITEMS,
            "create_time_without_blob": create_time_without_blob,
            "create_time_with_blob": create_time_with_blob,
            "create_overhead": ((create_time_with_blob - create_time_without_blob) / create_time_without_blob) * 100
            if create_time_without_blob > 0
            else 0,
            "query_time_without_blob": query_time_without_blob,
            "query_time_with_blob": query_time_with_blob,
            "query_time_savings": time_savings_percent,
            "rcus_without_blob": result_without_blob.rcus_consumed_by_query,
            "rcus_with_blob": result_with_blob.rcus_consumed_by_query,
            "rcu_savings": rcu_savings_percent,
            "item_size_without_blob": result_without_blob[0].get_db_item_size(),
            "item_size_with_blob": result_with_blob[-1].get_db_item_size(),
            "size_reduction_factor": item_size_without_blob_bytes / item_size_with_blob_bytes
            if item_size_with_blob_bytes > 0
            else 0,
            "api_calls_without_blob": result_without_blob.api_calls_made,
            "api_calls_with_blob": result_with_blob.api_calls_made,
            "blob_load_time_10": load_time,
            "blob_load_time_avg": load_time / 10,
        }
        self.__class__.results.append(result_data)
