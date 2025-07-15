from boto3.dynamodb.conditions import Attr

from simplesingletable import DynamoDbMemory, DynamoDbResource


class ResourceWithFilters(DynamoDbResource):
    name: str
    status: str
    priority: int
    tags: list[str]


def test_filter_expression_with_attr_eq(dynamodb_memory: DynamoDbMemory):
    # Create test resources with different statuses
    resource1 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 1", "status": "active", "priority": 1, "tags": ["important"]},
    )
    resource2 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 2", "status": "inactive", "priority": 2, "tags": ["low"]},
    )
    resource3 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 3", "status": "active", "priority": 3, "tags": ["urgent"]},
    )

    # Test filter by status
    active_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("status").eq("active"),
    )

    assert len(active_resources) == 2
    assert all(r.status == "active" for r in active_resources)
    assert resource1 in active_resources
    assert resource3 in active_resources
    assert resource2 not in active_resources


def test_filter_expression_with_attr_gt(dynamodb_memory: DynamoDbMemory):
    # Create test resources with different priorities
    resource1 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Low Priority", "status": "active", "priority": 1, "tags": []},
    )
    resource2 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Medium Priority", "status": "active", "priority": 5, "tags": []},
    )
    resource3 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "High Priority", "status": "active", "priority": 10, "tags": []},
    )

    # Test filter by priority > 3
    high_priority_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("priority").gt(3),
    )

    assert len(high_priority_resources) == 2
    assert resource2 in high_priority_resources
    assert resource3 in high_priority_resources
    assert resource1 not in high_priority_resources


def test_filter_expression_with_compound_conditions(dynamodb_memory: DynamoDbMemory):
    # Create test resources
    resource1 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 1", "status": "active", "priority": 5, "tags": ["work"]},
    )
    resource2 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 2", "status": "active", "priority": 2, "tags": ["personal"]},
    )
    resource3 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 3", "status": "inactive", "priority": 8, "tags": ["work"]},
    )
    resource4 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Task 4", "status": "active", "priority": 7, "tags": ["urgent"]},
    )

    # Test compound filter: status='active' AND priority > 3
    filtered_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("status").eq("active") & Attr("priority").gt(3),
    )

    assert len(filtered_resources) == 2
    assert resource1 in filtered_resources
    assert resource4 in filtered_resources
    assert resource2 not in filtered_resources  # priority too low
    assert resource3 not in filtered_resources  # inactive status


def test_filter_expression_with_contains(dynamodb_memory: DynamoDbMemory):
    # Create test resources
    resource1 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Important Task", "status": "active", "priority": 5, "tags": ["urgent", "work"]},
    )
    resource2 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Regular Task", "status": "active", "priority": 3, "tags": ["personal"]},
    )
    resource3 = dynamodb_memory.create_new(
        ResourceWithFilters,
        {"name": "Another Task", "status": "active", "priority": 4, "tags": ["work", "review"]},
    )

    # Test filter by tags containing "work"
    work_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("tags").contains("work"),
    )

    assert len(work_resources) == 2
    assert resource1 in work_resources
    assert resource3 in work_resources
    assert resource2 not in work_resources


def test_filter_expression_with_between(dynamodb_memory: DynamoDbMemory):
    # Create test resources with various priorities
    resources = []
    for i in range(1, 11):
        resource = dynamodb_memory.create_new(
            ResourceWithFilters,
            {"name": f"Task {i}", "status": "active", "priority": i, "tags": []},
        )
        resources.append(resource)

    # Test filter by priority between 3 and 7 (inclusive)
    mid_priority_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("priority").between(3, 7),
    )

    assert len(mid_priority_resources) == 5
    expected_priorities = {3, 4, 5, 6, 7}
    actual_priorities = {r.priority for r in mid_priority_resources}
    assert actual_priorities == expected_priorities


def test_filter_expression_pagination(dynamodb_memory: DynamoDbMemory):
    # Create many resources to test pagination with filters
    for i in range(20):
        dynamodb_memory.create_new(
            ResourceWithFilters,
            {
                "name": f"Task {i}",
                "status": "active" if i % 2 == 0 else "inactive",
                "priority": i,
                "tags": [],
            },
        )

    # Test pagination with filter - get only active resources
    page1 = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("status").eq("active"),
        results_limit=5,
    )

    assert len(page1) == 5
    assert all(r.status == "active" for r in page1)
    assert page1.next_pagination_key is not None

    # Get next page
    page2 = dynamodb_memory.list_type_by_updated_at(
        ResourceWithFilters,
        filter_expression=Attr("status").eq("active"),
        results_limit=5,
        pagination_key=page1.next_pagination_key,
    )

    assert len(page2) == 5
    assert all(r.status == "active" for r in page2)
    # Ensure no overlap between pages
    page1_ids = {r.resource_id for r in page1}
    page2_ids = {r.resource_id for r in page2}
    assert page1_ids.isdisjoint(page2_ids)


def test_filter_expression_with_reserved_words(dynamodb_memory: DynamoDbMemory):
    # Test that we can filter on attributes with names that are DynamoDB reserved words
    # This demonstrates the benefit of using ConditionBase/Attr which handles this automatically

    class ResourceWithReservedWords(DynamoDbResource):
        name: str
        size: int  # 'size' is a reserved word in DynamoDB
        data: dict  # 'data' is a reserved word in DynamoDB

    # Create test resources
    resource1 = dynamodb_memory.create_new(
        ResourceWithReservedWords,
        {"name": "Small", "size": 10, "data": {"type": "A"}},
    )
    resource2 = dynamodb_memory.create_new(
        ResourceWithReservedWords,
        {"name": "Medium", "size": 50, "data": {"type": "B"}},
    )
    resource3 = dynamodb_memory.create_new(
        ResourceWithReservedWords,
        {"name": "Large", "size": 100, "data": {"type": "C"}},
    )

    # Filter by size > 30 (size is a reserved word, but Attr handles this)
    large_resources = dynamodb_memory.list_type_by_updated_at(
        ResourceWithReservedWords,
        filter_expression=Attr("size").gt(30),
    )

    assert len(large_resources) == 2
    assert resource2 in large_resources
    assert resource3 in large_resources
    assert resource1 not in large_resources
