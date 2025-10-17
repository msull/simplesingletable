"""
Streamlit Demo App for simplesingletable

This interactive demo showcases the core features of simplesingletable including:
- Basic CRUD operations
- Versioned resources with history
- Audit logging with field-level tracking
- Blob storage for large fields

Run with: streamlit run app.py
"""

from typing import Any
import boto3
import requests
import streamlit as st
from logzero import logger
from botocore.exceptions import ClientError

from simplesingletable import DynamoDbMemory
from simplesingletable.utils import create_standard_dynamodb_table, truncate_dynamo_table
from simplesingletable.blob_storage import S3BlobStorage

from scenarios import SCENARIOS


# ==============================================================================
# Configuration
# ==============================================================================

DYNAMODB_ENDPOINT = "http://localhost:8000"
MINIO_ENDPOINT = "http://localhost:9000"
TABLE_NAME = "simplesingletable-demo"
BUCKET_NAME = "demo-blobs"

AWS_CONFIG = {
    "aws_access_key_id": "unused",
    "aws_secret_access_key": "unused",
    "region_name": "us-west-2",
}

MINIO_CONFIG = {
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1",
}


# ==============================================================================
# Helper Functions
# ==============================================================================


def check_dynamodb_connection() -> bool:
    """Check if DynamoDB Local is responsive."""
    try:
        response = requests.get(DYNAMODB_ENDPOINT, timeout=2)
        return response.status_code == 400  # DynamoDB returns 400 for root endpoint
    except requests.RequestException:
        return False


def check_minio_connection() -> bool:
    """Check if MinIO is responsive."""
    try:
        response = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=2)
        return response.status_code == 200
    except requests.RequestException:
        return False


def initialize_dynamodb() -> Any:
    """Initialize DynamoDB table if it doesn't exist."""
    try:
        resource = boto3.resource(
            "dynamodb",
            endpoint_url=DYNAMODB_ENDPOINT,
            **AWS_CONFIG,
        )

        # Check if table exists
        try:
            table = resource.Table(TABLE_NAME)
            table.load()
            logger.info(f"Table {TABLE_NAME} already exists")
            return table
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                # Create table
                logger.info(f"Creating table {TABLE_NAME}")
                table = create_standard_dynamodb_table(table_name=TABLE_NAME, dynamodb_resource=resource)
                return table
            else:
                raise

    except Exception as e:
        logger.error(f"Failed to initialize DynamoDB: {e}")
        raise


def initialize_minio() -> None:
    """Initialize MinIO bucket if it doesn't exist."""
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            **MINIO_CONFIG,
            use_ssl=False,
        )

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            logger.info(f"Bucket {BUCKET_NAME} already exists")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # Create bucket
                logger.info(f"Creating bucket {BUCKET_NAME}")
                s3_client.create_bucket(Bucket=BUCKET_NAME)
            else:
                raise

    except Exception as e:
        logger.error(f"Failed to initialize MinIO: {e}")
        raise


def get_memory() -> DynamoDbMemory:
    """Get or create DynamoDbMemory instance."""
    if "memory" not in st.session_state:
        # Initialize services
        initialize_dynamodb()
        initialize_minio()

        # Create S3 client for MinIO
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            **MINIO_CONFIG,
            use_ssl=False,
        )

        # Create S3BlobStorage instance
        s3_blob_storage = S3BlobStorage(
            bucket_name=BUCKET_NAME,
            key_prefix="demo-blobs",
            s3_client=s3_client,
        )

        # Create memory instance
        memory = DynamoDbMemory(
            logger=logger,
            table_name=TABLE_NAME,
            endpoint_url=DYNAMODB_ENDPOINT,
            s3_bucket=BUCKET_NAME,
            s3_key_prefix="demo-blobs",
            connection_params=AWS_CONFIG,
        )

        # Override with MinIO-configured S3 storage
        memory._s3_blob_storage = s3_blob_storage

        st.session_state.memory = memory

    return st.session_state.memory


def reset_environment(memory: DynamoDbMemory) -> None:
    """Reset the demo environment by clearing all data."""
    try:
        # Truncate DynamoDB table
        truncate_dynamo_table(memory.dynamodb_table)
        logger.info("DynamoDB table truncated")

        # Clear MinIO bucket
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            **MINIO_CONFIG,
            use_ssl=False,
        )

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": objects})

        logger.info("MinIO bucket cleared")
        st.success("Environment reset successfully!")

    except Exception as e:
        logger.error(f"Failed to reset environment: {e}")
        st.error(f"Failed to reset environment: {e}")


def scan_dynamodb_table(memory: DynamoDbMemory) -> list[dict]:
    """Scan DynamoDB table and return all items."""
    try:
        response = memory.dynamodb_table.scan()
        items = response.get("Items", [])

        # Handle pagination
        while "LastEvaluatedKey" in response:
            response = memory.dynamodb_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))

        return items
    except Exception as e:
        logger.error(f"Failed to scan table: {e}")
        return []


def format_table_items_as_dataframe(items: list[dict]) -> Any:
    """Convert DynamoDB items to a formatted dataframe."""
    import pandas as pd

    if not items:
        return pd.DataFrame()

    # Extract key fields for display
    formatted_data = []
    for item in items:
        row = {
            "pk": item.get("pk", ""),
            "sk": item.get("sk", ""),
            "gsitype": item.get("gsitype", ""),
            "gsitypesk": item.get("gsitypesk", ""),
            "gsi1pk": item.get("gsi1pk", ""),
            "gsi2pk": item.get("gsi2pk", ""),
            "gsi3pk": item.get("gsi3pk", ""),
            "gsi3sk": item.get("gsi3sk", ""),
            "gsi4pk": item.get("gsi4pk", ""),
            "gsi4sk": item.get("gsi4sk", ""),
            "resource_id": item.get("resource_id", ""),
            "created_at": item.get("created_at", ""),
            "updated_at": item.get("updated_at", ""),
        }

        # Add resource-specific fields
        if item.get("type") == "User":
            row.update({"name": item.get("name", ""), "email": item.get("email", "")})
        elif item.get("type") == "Document":
            row.update({"title": item.get("title", ""), "version": item.get("version", "")})
        elif item.get("type") == "Order":
            row.update(
                {
                    "customer_email": item.get("customer_email", ""),
                    "status": item.get("status", ""),
                    "total_amount": item.get("total_amount", ""),
                }
            )
        elif item.get("type") == "Report":
            row.update({"title": item.get("title", ""), "author": item.get("author", "")})
        elif item.get("type") == "AuditLog":
            row.update(
                {
                    "operation": item.get("operation", ""),
                    "resource_type": item.get("resource_type", ""),
                    "changed_by": item.get("changed_by", ""),
                }
            )

        formatted_data.append(row)

    return pd.DataFrame(formatted_data)


# ==============================================================================
# Main App
# ==============================================================================


def main():
    st.set_page_config(
        page_title="simplesingletable Demo",
        page_icon="🗄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🗄️ simplesingletable Interactive Demo")
    st.markdown(
        """
        This demo showcases the core features of **simplesingletable** - a Pydantic-based
        abstraction layer for AWS DynamoDB implementing single-table design patterns.
        """
    )

    # Check service connections
    st.sidebar.header("Service Status")
    dynamodb_status = check_dynamodb_connection()
    minio_status = check_minio_connection()

    st.sidebar.write(f"**DynamoDB:** {'✅ Connected' if dynamodb_status else '❌ Disconnected'}")
    st.sidebar.write(f"**MinIO (S3):** {'✅ Connected' if minio_status else '❌ Disconnected'}")

    if not dynamodb_status or not minio_status:
        st.error(
            """
            **Services not available!**

            Please start the required services using:
            ```bash
            cd examples/streamlit_demo
            docker-compose up -d
            ```
            """
        )
        st.stop()

    # Initialize memory
    try:
        memory = get_memory()
    except Exception as e:
        st.error(f"Failed to initialize: {e}")
        st.stop()

    # Sidebar controls
    st.sidebar.divider()
    st.sidebar.header("Controls")

    if st.sidebar.button("🔄 Reset Environment", type="primary", use_container_width=True):
        reset_environment(memory)
        st.rerun()

    # Scenario selector
    st.sidebar.divider()
    st.sidebar.header("Select Scenario")
    selected_scenario = st.sidebar.selectbox(
        "Choose a feature to explore:",
        options=list(SCENARIOS.keys()),
        format_func=lambda x: f"{x}",
    )

    scenario_info = SCENARIOS[selected_scenario]
    st.sidebar.info(scenario_info["description"])

    # Main content area - two columns
    left_col, right_col = st.columns([3, 2])

    # Left column - interactive scenario
    with left_col:
        st.header("Interactive Demo")
        scenario_info["function"](memory)

    # Right column - table viewer
    with right_col:
        st.header("DynamoDB Table View")

        # View toggle
        view_mode = st.radio(
            "View Mode", ["Formatted Table", "Raw JSON"], horizontal=True, label_visibility="collapsed"
        )

        # Refresh button
        if st.button("🔄 Refresh Table View"):
            st.rerun()

        # Scan table
        items = scan_dynamodb_table(memory)
        st.caption(f"Total items in table: {len(items)}")

        if not items:
            st.info("No items in table. Create some resources to see them here!")
        else:
            if view_mode == "Formatted Table":
                df = format_table_items_as_dataframe(items)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Show expandable details
                with st.expander("View Full Item Details"):
                    selected_pk = st.selectbox(
                        "Select item by PK",
                        options=[item["pk"] for item in items],
                    )
                    if selected_pk:
                        selected_item = next(item for item in items if item["pk"] == selected_pk)
                        st.json(selected_item, expanded=True)
            else:
                # Raw JSON view
                st.json(items, expanded=False)

    # Footer
    st.divider()
    st.markdown(
        """
        **Learn More:**
        - [GitHub Repository](https://github.com/msull/simplesingletable)
        - [Documentation](https://github.com/msull/simplesingletable#readme)

        Built with ❤️ using [Streamlit](https://streamlit.io)
        """
    )


if __name__ == "__main__":
    main()
