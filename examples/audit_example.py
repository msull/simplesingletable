"""Example demonstrating audit logging capabilities in simplesingletable.

This example shows how to:
1. Enable audit logging for resources
2. Track field-level changes
3. Query audit logs by various criteria
4. Work with blob fields in audit logs
5. View change history for specific fields

Requirements:
- AWS credentials configured
- DynamoDB table created
- S3 bucket configured (for blob fields)
"""

from datetime import datetime, timedelta
from typing import ClassVar, Optional

from pydantic import BaseModel

from simplesingletable import (
    AuditConfig,
    AuditLogQuerier,
    DynamoDbMemory,
    DynamoDbResource,
    DynamoDbVersionedResource,
)
from simplesingletable.models import BlobFieldConfig, ResourceConfig


# ==============================================================================
# Define Resources with Audit Logging
# ==============================================================================


class UserProfile(DynamoDbVersionedResource):
    """User profile with comprehensive audit tracking."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        max_versions=10,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,  # Track field-level changes
            include_snapshot=True,  # Include full resource snapshot
            exclude_fields={"password_hash"},  # Don't audit sensitive fields
        ),
    )

    username: str
    email: str
    full_name: str
    role: str
    password_hash: str  # Excluded from audit logs
    is_active: bool = True


class Address(BaseModel):
    """Nested Pydantic model for address."""

    street: str
    city: str
    state: str
    zip_code: str
    country: str = "USA"


class Order(DynamoDbResource):
    """Order resource with nested models and selective auditing."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )

    customer_email: str
    total_amount: float
    status: str
    shipping_address: Address
    notes: Optional[str] = None


class Document(DynamoDbResource):
    """Document with blob fields (S3 storage) and audit logging."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"content"},  # Optional: exclude blob from audit
        ),
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "attachment": BlobFieldConfig(compress=False, content_type="application/pdf"),
        },
    )

    title: str
    author: str
    content: Optional[str] = None  # Stored in S3
    attachment: Optional[bytes] = None  # Stored in S3
    status: str = "draft"


# ==============================================================================
# Initialize DynamoDbMemory
# ==============================================================================


def get_memory() -> DynamoDbMemory:
    """Initialize DynamoDbMemory with S3 bucket for blob storage."""
    return DynamoDbMemory(
        table_name="your-dynamodb-table",
        s3_bucket_name="your-s3-bucket",  # Required for blob fields
    )


# ==============================================================================
# Example 1: Basic Audit Logging
# ==============================================================================


def example_basic_audit():
    """Demonstrate basic audit logging with CREATE, UPDATE, DELETE."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # CREATE: Creates audit log entry
    user = memory.create_new(
        UserProfile,
        {
            "username": "jdoe",
            "email": "john@example.com",
            "full_name": "John Doe",
            "role": "user",
            "password_hash": "hashed_password",
        },
        changed_by="admin@example.com",
    )
    print(f"✓ Created user: {user.username}")

    # UPDATE: Creates audit log with field-level changes
    user = memory.update_existing(
        user,
        {"role": "admin", "email": "john.doe@example.com"},
        changed_by="superadmin@example.com",
    )
    print(f"✓ Updated user role to: {user.role}")

    # DELETE: Creates final audit log entry
    memory.delete_existing(user, changed_by="superadmin@example.com")
    print(f"✓ Deleted user: {user.username}")

    # Query all audit logs for this resource
    logs = querier.get_logs_for_resource("UserProfile", user.resource_id)
    print(f"\n📋 Total audit logs: {len(logs)}")

    for log in logs:
        print(f"  - {log.operation} at {log.created_at} by {log.changed_by}")
        if log.changed_fields:
            print(f"    Changed fields: {list(log.changed_fields.keys())}")


# ==============================================================================
# Example 2: Field-Level Change Tracking
# ==============================================================================


def example_field_tracking():
    """Track changes to specific fields over time."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # Create order
    order = memory.create_new(
        Order,
        {
            "customer_email": "customer@example.com",
            "total_amount": 99.99,
            "status": "pending",
            "shipping_address": Address(
                street="123 Main St",
                city="Springfield",
                state="IL",
                zip_code="62701",
            ),
        },
        changed_by="system",
    )

    # Update status multiple times
    for new_status in ["processing", "shipped", "delivered"]:
        order = memory.update_existing(
            order,
            {"status": new_status},
            changed_by="fulfillment@example.com",
        )
        print(f"✓ Order status: {new_status}")

    # Get complete history for 'status' field
    status_history = querier.get_field_history("Order", order.resource_id, "status")

    print(f"\n📊 Status change history:")
    for change in status_history:
        print(f"  {change['timestamp']}: {change['old_value']} → {change['new_value']}")
        print(f"    Changed by: {change['changed_by']}")


# ==============================================================================
# Example 3: Querying Audit Logs
# ==============================================================================


def example_querying():
    """Demonstrate various audit log query patterns."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # Create some test data
    for i in range(5):
        memory.create_new(
            UserProfile,
            {
                "username": f"user{i}",
                "email": f"user{i}@example.com",
                "full_name": f"User {i}",
                "role": "user",
                "password_hash": "hash",
            },
            changed_by=f"admin{i % 2}@example.com",
        )

    # Query 1: Get all logs for a resource type
    print("\n🔍 Query: All UserProfile logs")
    all_user_logs = querier.get_logs_for_resource_type("UserProfile", limit=10)
    print(f"  Found {len(all_user_logs)} audit logs")

    # Query 2: Filter by operation type
    print("\n🔍 Query: All CREATE operations")
    create_logs = querier.get_logs_by_operation("UserProfile", "CREATE", limit=10)
    print(f"  Found {len(create_logs)} CREATE operations")

    # Query 3: Filter by who made changes
    print("\n🔍 Query: Changes by admin0@example.com")
    admin0_changes = querier.get_logs_by_changer("admin0@example.com")
    print(f"  Found {len(admin0_changes)} changes by admin0@example.com")

    # Query 4: Recent changes across all resources
    print("\n🔍 Query: 10 most recent changes")
    recent = querier.get_recent_changes(limit=10)
    for log in recent[:5]:
        print(f"  - {log.resource_type} {log.operation} by {log.changed_by}")

    # Query 5: Date range filtering (ULID-optimized)
    print("\n🔍 Query: Changes in last 24 hours")
    yesterday = datetime.utcnow() - timedelta(days=1)
    recent_changes = querier.get_logs_for_resource_type(
        "UserProfile",
        start_date=yesterday,
        limit=20,
    )
    print(f"  Found {len(recent_changes)} changes in last 24 hours")


# ==============================================================================
# Example 4: Blob Fields in Audit Logs
# ==============================================================================


def example_blob_audit():
    """Demonstrate audit logging with blob fields (S3-stored data)."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # Create document with blob content
    large_content = "This is a large document..." * 1000
    doc = memory.create_new(
        Document,
        {
            "title": "Annual Report 2024",
            "author": "finance@example.com",
            "content": large_content,  # Stored in S3
            "status": "draft",
        },
        changed_by="author@example.com",
    )
    print(f"✓ Created document: {doc.title}")

    # Update the blob field
    updated_content = "Updated document content..." * 1200
    doc = memory.update_existing(
        doc,
        {"content": updated_content, "status": "published"},
        changed_by="editor@example.com",
    )
    print(f"✓ Updated document status: {doc.status}")

    # Retrieve audit logs
    logs = querier.get_logs_for_resource("Document", doc.resource_id)

    print(f"\n📋 Audit logs for document:")
    for log in logs:
        print(f"\n  {log.operation} at {log.created_at}")

        # Blob fields appear as metadata (not full content)
        if log.resource_snapshot and "content" in log.resource_snapshot:
            blob_meta = log.resource_snapshot["content"]
            if isinstance(blob_meta, dict) and blob_meta.get("__blob_ref__"):
                print(f"    Content (blob): {blob_meta['size_bytes']} bytes")
                print(f"    Compressed: {blob_meta.get('compressed', False)}")

        # Changed fields show blob metadata changes
        if log.changed_fields and "content" in log.changed_fields:
            change = log.changed_fields["content"]
            old_size = change["old"].get("size_bytes") if change["old"] else 0
            new_size = change["new"].get("size_bytes") if change["new"] else 0
            print(f"    Content changed: {old_size} → {new_size} bytes")


# ==============================================================================
# Example 5: Nested Pydantic Models
# ==============================================================================


def example_nested_models():
    """Track changes to nested Pydantic models."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # Create order with address
    order = memory.create_new(
        Order,
        {
            "customer_email": "customer@example.com",
            "total_amount": 149.99,
            "status": "pending",
            "shipping_address": Address(
                street="123 Main St",
                city="Springfield",
                state="IL",
                zip_code="62701",
            ),
        },
        changed_by="customer@example.com",
    )

    # Update nested address
    order = memory.update_existing(
        order,
        {
            "shipping_address": Address(
                street="456 Oak Ave",  # Changed
                city="Springfield",
                state="IL",
                zip_code="62702",  # Changed
            ),
        },
        changed_by="customer@example.com",
    )

    # View audit log
    logs = querier.get_logs_for_resource("Order", order.resource_id)
    update_log = logs[0]

    print("\n📦 Nested model change tracking:")
    if update_log.changed_fields and "shipping_address" in update_log.changed_fields:
        change = update_log.changed_fields["shipping_address"]
        print(f"  Old address: {change['old']}")
        print(f"  New address: {change['new']}")


# ==============================================================================
# Example 6: Pagination
# ==============================================================================


def example_pagination():
    """Handle large result sets with pagination."""
    memory = get_memory()
    querier = AuditLogQuerier(memory)

    # Get first page of results
    first_page = querier.get_logs_for_resource_type("UserProfile", limit=10)

    print(f"\n📄 First page: {len(first_page)} results")
    print(f"   Has more: {first_page.has_next_page()}")

    # Get next page if available
    if first_page.has_next_page():
        next_page = querier.get_logs_for_resource_type(
            "UserProfile",
            limit=10,
            pagination_key=first_page.next_page_key,
        )
        print(f"   Next page: {len(next_page)} results")


# ==============================================================================
# Main
# ==============================================================================


if __name__ == "__main__":
    print("=" * 80)
    print("Audit Logging Examples - simplesingletable")
    print("=" * 80)

    # Uncomment the examples you want to run:

    # example_basic_audit()
    # example_field_tracking()
    # example_querying()
    # example_blob_audit()
    # example_nested_models()
    # example_pagination()

    print("\n✅ Examples completed!")
    print("\nNote: Update get_memory() with your actual DynamoDB table and S3 bucket.")
