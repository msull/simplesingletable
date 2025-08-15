#!/usr/bin/env python3
"""
Debug script to check if _blob_versions is being properly stored and retrieved.
"""

import json
from typing import Optional
from datetime import datetime
import boto3

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig


# Define a test versioned resource with blob fields
class DocumentWithBlobs(DynamoDbVersionedResource):
    """A document with both regular and blob fields."""
    
    title: str
    author: str
    tags: list[str]
    # Blob fields - should be preserved across updates
    content: Optional[str] = None  # Large document content
    attachments: Optional[dict] = None  # Attachment metadata
    
    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={
            "content": BlobFieldConfig(
                compress=True,
                content_type="text/plain"
            ),
            "attachments": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            )
        }
    )


def inspect_dynamodb_item(memory: DynamoDbMemory, resource_id: str, version: int = 0):
    """Directly inspect a DynamoDB item to see its raw structure."""
    key = {
        'pk': f'DocumentWithBlobs#{resource_id}',
        'sk': f'v{version}'
    }
    response = memory.dynamodb_table.get_item(Key=key)
    item = response.get('Item', {})
    
    print(f"\nDynamoDB item for version {version}:")
    print(f"  Keys present: {list(item.keys())}")
    if '_blob_fields' in item:
        print(f"  _blob_fields: {item['_blob_fields']}")
    if '_blob_versions' in item:
        print(f"  _blob_versions: {item['_blob_versions']}")
    
    return item


class SimpleLogger:
    """Simple logger that forwards to print."""
    def debug(self, msg): pass  # Suppress debug messages
    def info(self, msg): print(f"[INFO] {msg}")
    def warning(self, msg): print(f"[WARN] {msg}")
    def error(self, msg): print(f"[ERROR] {msg}")


def main():
    """Debug blob version references."""
    
    print("\n=== Blob Version Reference Debug ===\n")
    
    # Initialize DynamoDbMemory with real AWS resources
    memory = DynamoDbMemory(
        logger=SimpleLogger(),
        table_name="awsapi-table",
        s3_bucket="awsapi-bucket-947991878136-us-west-2",
        s3_key_prefix="blob-debug",
    )
    
    # Step 1: Create a document with blob fields
    print("Step 1: Creating document with blob fields...")
    large_content = "This is a large document content. " * 100
    attachments_data = {
        "files": ["report.pdf", "data.xlsx"],
        "metadata": {"size": 1024000, "type": "archive"}
    }
    
    doc = memory.create_new(
        DocumentWithBlobs,
        {
            "title": "Test Document",
            "author": "Test Author",
            "tags": ["test"],
            "content": large_content,
            "attachments": attachments_data
        }
    )
    
    print(f"Created document ID: {doc.resource_id}, Version: {doc.version}")
    print(f"Document _blob_versions: {doc._blob_versions}")
    
    # Inspect v1 in DynamoDB
    item_v1 = inspect_dynamodb_item(memory, doc.resource_id, version=1)
    
    # Step 2: Load without blobs
    print("\nStep 2: Loading document without blobs...")
    loaded = memory.get_existing(
        doc.resource_id,
        DocumentWithBlobs,
        load_blobs=False
    )
    
    print(f"Loaded version: {loaded.version}")
    print(f"Loaded _blob_versions: {loaded._blob_versions}")
    print(f"Has unloaded blobs: {loaded.has_unloaded_blobs()}")
    
    # Step 3: Update without changing blob fields
    print("\nStep 3: Updating document (not touching blob fields)...")
    updated = memory.update_existing(
        loaded,
        {
            "title": "Test Document - UPDATED",
            "tags": ["test", "updated"]
        }
    )
    
    print(f"Updated version: {updated.version}")
    print(f"Updated _blob_versions: {updated._blob_versions}")
    
    # Inspect v2 in DynamoDB
    item_v2 = inspect_dynamodb_item(memory, doc.resource_id, version=2)
    
    # Step 4: Try to load blob fields
    print("\nStep 4: Loading blob fields from updated version...")
    if updated.has_unloaded_blobs():
        try:
            updated.load_blob_fields(memory)
            print("✓ Successfully loaded blob fields!")
            print(f"  Content loaded: {len(updated.content) if updated.content else 0} chars")
            print(f"  Attachments loaded: {updated.attachments is not None}")
        except Exception as e:
            print(f"✗ Failed to load blob fields: {e}")
    
    # Clean up
    print("\nCleaning up...")
    memory.delete_all_versions(doc.resource_id, DocumentWithBlobs)
    print("✓ Cleaned up test resources")


if __name__ == "__main__":
    main()