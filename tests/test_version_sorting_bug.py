"""Test to demonstrate the version sorting bug when versions exceed 9."""

from boto3.dynamodb.conditions import Key

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource


class ExampleVersionedResource(DynamoDbVersionedResource):
    """Test resource with max_versions to demonstrate the bug."""

    content: str

    # Set max_versions to 5 to demonstrate the issue
    resource_config = {"max_versions": 5}


def test_version_sorting_bug_with_double_digit_versions(dynamodb_memory: DynamoDbMemory):
    """Test that demonstrates the lexicographical sorting issue with version numbers > 9."""

    # Create a versioned resource
    resource = dynamodb_memory.create_new(ExampleVersionedResource, {"content": "Initial version"})

    # Store the resource_id for later queries
    resource_id = resource.resource_id

    # Create 12 versions to go past single digits
    for i in range(12):
        try:
            resource = dynamodb_memory.update_existing(resource, {"content": f"Version {i+2}"})
            print(f"Created version {resource.version}")
        except ValueError as e:
            print(f"Failed to create version {i+2}: {e}")
            break

    # The last successful version
    print(f"Last successful version: {resource.version}")

    # Query all versions to see what's actually stored
    all_versions = dynamodb_memory.dynamodb_table.query(
        KeyConditionExpression=Key("pk").eq(f"ExampleVersionedResource#{resource_id}") & Key("sk").begins_with("v"),
        ScanIndexForward=True,  # Ascending order
    )["Items"]

    # Print out the versions for debugging
    print("\nAll versions found (sorted by SK):")
    for item in all_versions:
        if item["sk"] != "v0":
            print(f"  SK: {item['sk']}, Version: {item['version']}")

    # Extract version numbers (excluding v0)
    version_items = [(item["sk"], item["version"]) for item in all_versions if item["sk"] != "v0"]
    version_items.sort(key=lambda x: x[0])  # Sort by SK (lexicographically)

    print("\nLexicographical order of SKs:")
    for sk, version in version_items:
        print(f"  {sk} -> version {version}")

    # This will show the bug: v10, v11, v12, v13 come BEFORE v2, v3, etc. in lexicographical order
    # So when we keep only the last 5 versions, we're actually keeping the wrong ones!

    # Check which versions are actually kept (should be 9, 10, 11, 12, 13 but won't be due to bug)
    kept_versions = [item["version"] for item in all_versions if item["sk"] != "v0"]
    kept_versions.sort()

    print("\nVersions that should be kept (last 5): [9, 10, 11, 12, 13]")
    print(f"Versions actually kept: {kept_versions}")

    # With the fix, we should be keeping the correct last 5 versions
    # Converting Decimals to ints for comparison
    kept_versions_int = [int(v) for v in kept_versions]
    expected_versions = [9, 10, 11, 12, 13]  # The actual last 5 versions
    assert kept_versions_int == expected_versions, f"Expected {expected_versions}, got {kept_versions_int}"


def test_demonstrate_lexicographical_sorting():
    """Simple test to show how version strings sort lexicographically."""

    version_strings = [f"v{i}" for i in range(1, 15)]

    print("\nNumeric order:")
    print(version_strings)

    sorted_strings = sorted(version_strings)
    print("\nLexicographical order:")
    print(sorted_strings)

    # This shows: ['v1', 'v10', 'v11', 'v12', 'v13', 'v14', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    assert sorted_strings == ["v1", "v10", "v11", "v12", "v13", "v14", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9"]


def test_version_limit_fix_with_double_digits(dynamodb_memory: DynamoDbMemory):
    """Test that the fix properly handles version numbers > 9."""

    # Create a resource with max_versions=3 for quicker testing
    class FixedVersionedResource(DynamoDbVersionedResource):
        content: str
        resource_config = {"compress_data": False, "max_versions": 3}

    # Create initial resource
    resource = dynamodb_memory.create_new(FixedVersionedResource, {"content": "Initial version"})

    # Create 12 versions to go well beyond single digits
    for i in range(12):
        resource = dynamodb_memory.update_existing(resource, {"content": f"Version {i+2}"})

    # Should now be at version 13
    assert resource.version == 13

    # Query all versions
    all_versions = dynamodb_memory.dynamodb_table.query(
        KeyConditionExpression=Key("pk").eq(f"FixedVersionedResource#{resource.resource_id}")
        & Key("sk").begins_with("v"),
        ScanIndexForward=True,
    )["Items"]

    # Get non-v0 versions
    version_items = [v for v in all_versions if v["sk"] != "v0"]
    kept_versions = sorted([int(v["version"]) for v in version_items])

    # Should keep the last 3 versions: 11, 12, 13
    assert kept_versions == [11, 12, 13], f"Expected [11, 12, 13], got {kept_versions}"

    print(f"✅ Fix confirmed: Kept versions {kept_versions} (last 3 versions as expected)")
