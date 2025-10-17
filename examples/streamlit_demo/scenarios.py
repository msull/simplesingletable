"""Scenario definitions for the simplesingletable Streamlit demo.

Each scenario demonstrates a specific feature of the library with example resources
and interactive CRUD operations.
"""

from typing import ClassVar, Optional
import streamlit as st
from simplesingletable import (
    DynamoDbMemory,
    DynamoDbResource,
    DynamoDbVersionedResource,
    AuditConfig,
    AuditLogQuerier,
)
from simplesingletable.models import ResourceConfig, BlobFieldConfig


# ==============================================================================
# Scenario 1: Basic Resource CRUD
# ==============================================================================


class User(DynamoDbResource):
    """Simple user resource for demonstrating basic CRUD operations."""

    name: str
    email: str
    tags: set[str] = set()


def scenario_basic_crud(memory: DynamoDbMemory):
    """Demonstrate basic CRUD operations with a simple resource."""
    st.subheader("Basic Resource CRUD")

    st.markdown("""
    This scenario demonstrates basic Create, Read, Update, Delete operations
    with a simple `DynamoDbResource`.`.
    """)

    # Show the resource definition
    st.code(
        '''class User(DynamoDbResource):
    """Simple user resource for basic CRUD."""

    name: str
    email: str
    tags: set[str] = set()
''',
        language="python",
    )

    st.divider()

    # List existing users
    users = memory.list_type_by_updated_at(User, results_limit=50)
    st.write(f"**Existing Users:** {len(users)}")

    if users:
        for user in users.as_list():
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                st.text(f"{user.name} ({user.email})")
            with col2:
                st.text(f"Tags: {', '.join(user.tags) if user.tags else 'none'}")
            with col3:
                if st.button("Delete", key=f"delete_user_{user.resource_id}"):
                    memory.delete_existing(user)
                    st.rerun()

    st.divider()

    # Create new user
    with st.form("create_user"):
        st.write("**Create New User**")
        name = st.text_input("Name", placeholder="John Doe")
        email = st.text_input("Email", placeholder="john@example.com")
        tags_input = st.text_input("Tags (comma-separated)", placeholder="admin,developer")

        if st.form_submit_button("Create User"):
            if name and email:
                tags = set(tag.strip() for tag in tags_input.split(",") if tag.strip())
                user = memory.create_new(
                    User,
                    {"name": name, "email": email, "tags": tags},
                )
                st.success(f"Created user: {user.name} (ID: {user.resource_id})")
                st.rerun()
            else:
                st.error("Name and email are required")

    # Update existing user
    if users:
        st.divider()
        st.write("**Update User**")
        selected_user_id = st.selectbox(
            "Select user to update",
            options=[u.resource_id for u in users.as_list()],
            format_func=lambda uid: next(u.name for u in users.as_list() if u.resource_id == uid),
        )

        if selected_user_id:
            user = memory.read_existing(selected_user_id, User)
            with st.form("update_user"):
                new_name = st.text_input("Name", value=user.name)
                new_email = st.text_input("Email", value=user.email)
                new_tags = st.text_input("Tags (comma-separated)", value=", ".join(user.tags) if user.tags else "")

                if st.form_submit_button("Update"):
                    tags = set(tag.strip() for tag in new_tags.split(",") if tag.strip())
                    memory.update_existing(user, {"name": new_name, "email": new_email, "tags": tags})
                    st.success(f"Updated user: {new_name}")
                    st.rerun()


# ==============================================================================
# Scenario 2: Versioned Resource
# ==============================================================================


class Document(DynamoDbVersionedResource):
    """Versioned document resource that maintains complete history."""

    title: str
    content: str
    status: str = "draft"


def scenario_versioned_resource(memory: DynamoDbMemory):
    """Demonstrate versioned resources with full history tracking."""
    st.subheader("Versioned Resource")

    st.markdown("""
    This scenario shows `DynamoDbVersionedResource` which maintains complete
    version history. Notice in the table view how each update creates a new item
    with incrementing `sk` values (0=current, 1=v1, 2=v2, etc.).
    """)

    st.code(
        '''class Document(DynamoDbVersionedResource):
    """Versioned document with full history."""

    title: str
    content: str
    status: str = "draft"
''',
        language="python",
    )

    st.divider()

    # List existing documents
    docs = memory.list_type_by_updated_at(Document, results_limit=50)
    st.write(f"**Existing Documents:** {len(docs)}")

    if docs:
        for doc in docs.as_list():
            with st.expander(f"{doc.title} (v{doc.version}) - {doc.status}"):
                st.text(f"ID: {doc.resource_id}")
                st.text(f"Version: {doc.version}")
                st.text(f"Status: {doc.status}")
                st.text(f"Content: {doc.content[:100]}..." if len(doc.content) > 100 else doc.content)

                # Show version history
                all_versions = memory.get_all_versions(doc.resource_id, Document)
                st.write(f"**Version History** ({len(all_versions)} versions):")
                for v in all_versions:
                    st.text(f"  v{v.version}: {v.updated_at} - {v.status}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Delete All Versions", key=f"delete_doc_{doc.resource_id}"):
                        memory.delete_all_versions(doc.resource_id, Document)
                        st.rerun()
                with col2:
                    if doc.version > 1:
                        restore_version = st.number_input(
                            "Restore version",
                            min_value=1,
                            max_value=doc.version - 1,
                            key=f"restore_version_{doc.resource_id}",
                        )
                        if st.button("Restore", key=f"restore_{doc.resource_id}"):
                            restored = memory.restore_version(doc.resource_id, Document, restore_version)
                            st.success(f"Restored to version {restore_version}")
                            st.rerun()

    st.divider()

    # Create new document
    with st.form("create_document"):
        st.write("**Create New Document**")
        title = st.text_input("Title", placeholder="My Document")
        content = st.text_area("Content", placeholder="Document content here...")
        status = st.selectbox("Status", ["draft", "published", "archived"])

        if st.form_submit_button("Create Document"):
            if title and content:
                doc = memory.create_new(
                    Document,
                    {"title": title, "content": content, "status": status},
                )
                st.success(f"Created document: {doc.title} (v{doc.version})")
                st.rerun()
            else:
                st.error("Title and content are required")

    # Update existing document
    if docs:
        st.divider()
        st.write("**Update Document (creates new version)**")
        selected_doc_id = st.selectbox(
            "Select document to update",
            options=[d.resource_id for d in docs.as_list()],
            format_func=lambda did: next(d.title for d in docs.as_list() if d.resource_id == did),
        )

        if selected_doc_id:
            doc = memory.read_existing(selected_doc_id, Document)
            with st.form("update_document"):
                new_title = st.text_input("Title", value=doc.title)
                new_content = st.text_area("Content", value=doc.content)
                new_status = st.selectbox(
                    "Status",
                    ["draft", "published", "archived"],
                    index=["draft", "published", "archived"].index(doc.status),
                )

                if st.form_submit_button("Update (Create New Version)"):
                    updated = memory.update_existing(
                        doc, {"title": new_title, "content": new_content, "status": new_status}
                    )
                    st.success(f"Created version {updated.version}")
                    st.rerun()


# ==============================================================================
# Scenario 3: Resource with Auditing
# ==============================================================================


class Order(DynamoDbResource):
    """Order resource with comprehensive audit logging."""

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
    items: list[str]


def scenario_audit_logging(memory: DynamoDbMemory):
    """Demonstrate audit logging with field-level change tracking."""
    st.subheader("Resource with Audit Logging")

    st.markdown("""
    This scenario demonstrates audit logging which tracks all changes to resources.
    Notice in the table view the `AuditLog` entries that appear alongside Order items.
    Each change is tracked with field-level details and attribution.
    """)

    st.code(
        '''class Order(DynamoDbResource):
    """Order with comprehensive audit logging."""

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
    items: list[str]
''',
        language="python",
    )

    st.divider()

    # List existing orders
    orders = memory.list_type_by_updated_at(Order, results_limit=50)
    st.write(f"**Existing Orders:** {len(orders)}")

    if orders:
        querier = AuditLogQuerier(memory)

        for order in orders.as_list():
            with st.expander(f"Order for {order.customer_email} - {order.status} (${order.total_amount})"):
                st.text(f"ID: {order.resource_id}")
                st.text(f"Status: {order.status}")
                st.text(f"Total: ${order.total_amount:.2f}")
                st.text(f"Items: {', '.join(order.items)}")

                # Show audit logs
                logs = querier.get_logs_for_resource("Order", order.resource_id)
                st.write(f"**Audit Trail** ({len(logs)} entries):")
                for log in logs.as_list():
                    st.text(f"  {log.operation} at {log.created_at}")
                    st.text(f"  Changed by: {log.changed_by or 'system'}")
                    if log.changed_fields:
                        st.text(f"  Fields changed: {', '.join(log.changed_fields.keys())}")
                        for field, change in log.changed_fields.items():
                            st.text(f"    {field}: {change.get('old')} → {change.get('new')}")

                if st.button("Delete Order", key=f"delete_order_{order.resource_id}"):
                    memory.delete_existing(order, changed_by="demo-user")
                    st.rerun()

    st.divider()

    # Create new order
    with st.form("create_order"):
        st.write("**Create New Order**")
        customer_email = st.text_input("Customer Email", placeholder="customer@example.com")
        total_amount = st.number_input("Total Amount", min_value=0.0, value=99.99, step=0.01)
        status = st.selectbox("Status", ["pending", "processing", "shipped", "delivered", "cancelled"])
        items_input = st.text_input("Items (comma-separated)", placeholder="item1,item2,item3")
        changed_by = st.text_input("Changed By", placeholder="your-email@example.com", value="demo-user")

        if st.form_submit_button("Create Order"):
            if customer_email and items_input:
                items = [item.strip() for item in items_input.split(",") if item.strip()]
                order = memory.create_new(
                    Order,
                    {
                        "customer_email": customer_email,
                        "total_amount": total_amount,
                        "status": status,
                        "items": items,
                    },
                    changed_by=changed_by,
                )
                st.success(f"Created order: {order.resource_id}")
                st.rerun()
            else:
                st.error("Customer email and items are required")

    # Update existing order
    if orders:
        st.divider()
        st.write("**Update Order (tracked in audit log)**")
        selected_order_id = st.selectbox(
            "Select order to update",
            options=[o.resource_id for o in orders.as_list()],
            format_func=lambda oid: next(
                f"{o.customer_email} (${o.total_amount})" for o in orders.as_list() if o.resource_id == oid
            ),
        )

        if selected_order_id:
            order = memory.read_existing(selected_order_id, Order)
            with st.form("update_order"):
                new_status = st.selectbox(
                    "Status",
                    ["pending", "processing", "shipped", "delivered", "cancelled"],
                    index=["pending", "processing", "shipped", "delivered", "cancelled"].index(order.status),
                )
                new_total = st.number_input("Total Amount", value=order.total_amount, step=0.01)
                changed_by = st.text_input("Changed By", value="demo-user")

                if st.form_submit_button("Update Order"):
                    memory.update_existing(
                        order,
                        {"status": new_status, "total_amount": new_total},
                        changed_by=changed_by,
                    )
                    st.success(f"Updated order status to: {new_status}")
                    st.rerun()


# ==============================================================================
# Scenario 4: Resource with Blob Storage
# ==============================================================================


class Report(DynamoDbResource):
    """Report resource with large content stored in S3/MinIO."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "data": BlobFieldConfig(compress=True, content_type="application/json"),
        },
    )

    title: str
    author: str
    summary: str
    # Large fields stored in S3/MinIO
    content: Optional[str] = None
    data: Optional[dict] = None


def scenario_blob_storage(memory: DynamoDbMemory):
    """Demonstrate blob storage for large fields in S3/MinIO."""
    st.subheader("Resource with Blob Storage")

    st.markdown("""
    This scenario demonstrates blob storage where large fields are stored in S3/MinIO
    instead of DynamoDB. Notice in the table view how the resource contains blob
    references rather than the actual data. Blobs are loaded on-demand for performance.
    """)

    st.code(
        '''class Report(DynamoDbResource):
    """Report with blob storage for large content."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "data": BlobFieldConfig(compress=True, content_type="application/json"),
        },
    )

    title: str
    author: str
    summary: str
    content: Optional[str] = None  # Stored in S3
    data: Optional[dict] = None     # Stored in S3
''',
        language="python",
    )

    st.divider()

    # List existing reports
    reports = memory.list_type_by_updated_at(Report, results_limit=50)
    st.write(f"**Existing Reports:** {len(reports)}")

    if reports:
        for report in reports.as_list():
            with st.expander(f"{report.title} by {report.author}"):
                st.text(f"ID: {report.resource_id}")
                st.text(f"Summary: {report.summary}")
                st.text(f"Has unloaded blobs: {report.has_unloaded_blobs()}")

                if report.has_unloaded_blobs():
                    if st.button("Load Blobs", key=f"load_blobs_{report.resource_id}"):
                        report.load_blob_fields(memory)
                        st.success("Blobs loaded!")
                        st.rerun()
                else:
                    st.text(f"Content length: {len(report.content) if report.content else 0} chars")
                    if report.content:
                        st.text_area("Content (first 500 chars)", report.content[:500], disabled=True, height=150)
                    if report.data:
                        st.json(report.data)

                if st.button("Delete Report", key=f"delete_report_{report.resource_id}"):
                    memory.delete_existing(report)
                    st.rerun()

    st.divider()

    # Create new report
    with st.form("create_report"):
        st.write("**Create New Report**")
        title = st.text_input("Title", placeholder="Q4 2024 Report")
        author = st.text_input("Author", placeholder="analytics@example.com")
        summary = st.text_input("Summary", placeholder="Brief summary of the report")
        content = st.text_area(
            "Content (will be stored in S3/MinIO)",
            placeholder="Large report content...",
            height=150,
        )
        data_input = st.text_area(
            "Data JSON (will be stored in S3/MinIO)",
            placeholder='{"metric1": 100, "metric2": 200}',
            height=100,
        )

        if st.form_submit_button("Create Report"):
            if title and author and summary:
                import json

                data_dict = None
                if data_input.strip():
                    try:
                        data_dict = json.loads(data_input)
                    except json.JSONDecodeError:
                        st.error("Invalid JSON in data field")
                        st.stop()

                report = memory.create_new(
                    Report,
                    {
                        "title": title,
                        "author": author,
                        "summary": summary,
                        "content": content if content else None,
                        "data": data_dict,
                    },
                )
                st.success(f"Created report: {report.title}")
                st.rerun()
            else:
                st.error("Title, author, and summary are required")

    # Update existing report
    if reports:
        st.divider()
        st.write("**Update Report**")
        selected_report_id = st.selectbox(
            "Select report to update",
            options=[r.resource_id for r in reports.as_list()],
            format_func=lambda rid: next(r.title for r in reports.as_list() if r.resource_id == rid),
        )

        if selected_report_id:
            report = memory.read_existing(selected_report_id, Report, load_blobs=True)
            with st.form("update_report"):
                new_summary = st.text_input("Summary", value=report.summary)
                new_content = st.text_area("Content", value=report.content or "", height=150)

                if st.form_submit_button("Update Report"):
                    memory.update_existing(
                        report, {"summary": new_summary, "content": new_content if new_content else None}
                    )
                    st.success(f"Updated report: {report.title}")
                    st.rerun()


# ==============================================================================
# Scenario Registry
# ==============================================================================

SCENARIOS = {
    "Basic Resource CRUD": {
        "description": "Simple CRUD operations with DynamoDbResource",
        "function": scenario_basic_crud,
        "resource_classes": [User],
    },
    "Versioned Resource": {
        "description": "Version history tracking with DynamoDbVersionedResource",
        "function": scenario_versioned_resource,
        "resource_classes": [Document],
    },
    "Resource with Auditing": {
        "description": "Audit logging with field-level change tracking",
        "function": scenario_audit_logging,
        "resource_classes": [Order],
    },
    "Resource with Blob Storage": {
        "description": "Large fields stored in S3/MinIO",
        "function": scenario_blob_storage,
        "resource_classes": [Report],
    },
}
