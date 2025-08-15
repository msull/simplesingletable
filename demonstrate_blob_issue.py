#!/usr/bin/env python3
"""
Demonstration script showing the blob field preservation issue in simplesingletable.

This script connects to real AWS infrastructure to demonstrate that when updating
a versioned resource without modifying its blob fields, the blob field metadata
is lost, causing blob fields to become regular None values.
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


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def inspect_dynamodb_item(memory: DynamoDbMemory, resource_id: str, version: int = 0):
    """Directly inspect a DynamoDB item to see its raw structure."""
    key = {
        'pk': f'DocumentWithBlobs#{resource_id}',
        'sk': f'v{version}'
    }
    response = memory.dynamodb_table.get_item(Key=key)
    item = response.get('Item', {})
    
    # Check for _blob_fields marker
    has_blob_fields = '_blob_fields' in item
    blob_fields = item.get('_blob_fields', [])
    
    print(f"\nDynamoDB item inspection for version {version}:")
    print(f"  Has _blob_fields marker: {has_blob_fields}")
    if has_blob_fields:
        print(f"  Blob fields listed: {blob_fields}")
    print(f"  Item keys: {list(item.keys())}")
    
    return has_blob_fields, blob_fields


def check_s3_blobs(memory: DynamoDbMemory, resource_id: str):
    """Check what blobs exist in S3 for this resource."""
    if not memory.s3_blob_storage:
        print("No S3 storage configured")
        return []
    
    s3_client = memory.s3_blob_storage.s3_client
    prefix = f"{memory.s3_key_prefix}/DocumentWithBlobs/{resource_id}/"
    
    print(f"\nS3 blobs with prefix '{prefix}':")
    try:
        response = s3_client.list_objects_v2(
            Bucket=memory.s3_bucket,
            Prefix=prefix
        )
        
        objects = response.get('Contents', [])
        for obj in objects:
            key = obj['Key']
            size = obj['Size']
            print(f"  {key} ({size} bytes)")
        
        return [obj['Key'] for obj in objects]
    except Exception as e:
        print(f"  Error listing objects: {e}")
        return []


class SimpleLogger:
    """Simple logger that forwards to print."""
    def debug(self, msg): pass  # Suppress debug messages
    def info(self, msg): print(f"[INFO] {msg}")
    def warning(self, msg): print(f"[WARN] {msg}")
    def error(self, msg): print(f"[ERROR] {msg}")


def main():
    """Demonstrate the blob field preservation issue."""
    
    print_section("Blob Field Preservation Issue Demonstration")
    
    # Initialize DynamoDbMemory with real AWS resources
    print("\nInitializing connection to AWS...")
    memory = DynamoDbMemory(
        logger=SimpleLogger(),
        table_name="awsapi-table",
        s3_bucket="awsapi-bucket-947991878136-us-west-2",
        s3_key_prefix="blob-demo",
        # Using default AWS credentials from environment/profile
    )
    print("✓ Connected to DynamoDB and S3")
    
    print_section("Step 1: Create Document with Blob Fields")
    
    # Create a document with blob fields
    large_content = "This is a large document content. " * 100  # Simulate large content
    attachments_data = {
        "files": ["report.pdf", "data.xlsx"],
        "metadata": {"size": 1024000, "type": "archive"}
    }
    
    doc = memory.create_new(
        DocumentWithBlobs,
        {
            "title": "Q4 2024 Report",
            "author": "John Doe",
            "tags": ["finance", "quarterly"],
            "content": large_content,
            "attachments": attachments_data
        }
    )
    
    print(f"Created document:")
    print(f"  ID: {doc.resource_id}")
    print(f"  Version: {doc.version}")
    print(f"  Title: {doc.title}")
    print(f"  Has content: {doc.content is not None}")
    print(f"  Has attachments: {doc.attachments is not None}")
    
    # Inspect DynamoDB item
    has_blob_v1, blob_fields_v1 = inspect_dynamodb_item(memory, doc.resource_id, version=1)
    
    # Check S3 blobs
    s3_blobs_v1 = check_s3_blobs(memory, doc.resource_id)
    
    print_section("Step 2: Load Document Without Blobs")
    
    # Load the document without blob fields
    loaded = memory.get_existing(
        doc.resource_id,
        DocumentWithBlobs,
        load_blobs=False
    )
    
    print(f"Loaded document (without blobs):")
    print(f"  Version: {loaded.version}")
    print(f"  Title: {loaded.title}")
    print(f"  Content is None: {loaded.content is None}")
    print(f"  Attachments is None: {loaded.attachments is None}")
    print(f"  Has unloaded blobs: {loaded.has_unloaded_blobs()}")
    
    if loaded.has_unloaded_blobs():
        print(f"  Unloaded blob fields: {loaded.get_unloaded_blob_fields()}")
    
    print_section("Step 3: Update Document (NOT modifying blob fields)")
    
    # Update only the title and tags, NOT touching blob fields
    print("\nUpdating only title and tags (not blob fields)...")
    updated = memory.update_existing(
        loaded,
        {
            "title": "Q4 2024 Report - REVISED",
            "tags": ["finance", "quarterly", "revised"]
        }
    )
    
    print(f"Updated document:")
    print(f"  Version: {updated.version}")
    print(f"  Title: {updated.title}")
    print(f"  Tags: {updated.tags}")
    print(f"  Content is None: {updated.content is None}")
    print(f"  Attachments is None: {updated.attachments is None}")
    print(f"  Has unloaded blobs: {updated.has_unloaded_blobs()}")
    
    if updated.has_unloaded_blobs():
        print(f"  Unloaded blob fields: {updated.get_unloaded_blob_fields()}")
    
    # Inspect DynamoDB item for v2
    has_blob_v2, blob_fields_v2 = inspect_dynamodb_item(memory, doc.resource_id, version=2)
    
    # Check S3 blobs after update
    s3_blobs_v2 = check_s3_blobs(memory, doc.resource_id)
    
    print_section("Step 4: Try to Load Blobs from Updated Version")
    
    # Try to load the blob fields from the updated version
    print("\nAttempting to load blob fields from v2...")
    if updated.has_unloaded_blobs():
        updated.load_blob_fields(memory)
        print(f"  Content loaded: {updated.content[:50]}..." if updated.content else "  Content: None")
        print(f"  Attachments loaded: {updated.attachments}")
    else:
        print("  ❌ No blob placeholders - cannot load blob fields!")
        print("  This is the issue: blob field metadata was lost during update")
    
    print_section("Step 5: Load Version 1 to Verify Blobs Still Exist")
    
    # Load version 1 to show the blobs are still there
    v1 = memory.get_existing(
        doc.resource_id,
        DocumentWithBlobs,
        version=1,
        load_blobs=True
    )
    
    print(f"Version 1 (with blobs loaded):")
    print(f"  Version: {v1.version}")
    print(f"  Title: {v1.title}")
    print(f"  Content loaded: {v1.content[:50]}..." if v1.content else "  Content: None")
    print(f"  Attachments loaded: {v1.attachments}")
    
    print_section("Issue Summary")
    
    print("\n🔴 THE PROBLEM:")
    print("1. Version 1 has _blob_fields marker in DynamoDB: ", has_blob_v1)
    print("2. Version 2 has _blob_fields marker in DynamoDB: ", has_blob_v2)
    print("3. S3 blobs for v1 exist: ", len([b for b in s3_blobs_v1 if 'v1' in b]) > 0)
    print("4. S3 blobs for v2 exist: ", len([b for b in s3_blobs_v2 if 'v2' in b]) > 0)
    print("\nWhen updating without modifying blob fields:")
    print("- Version 2 loses the _blob_fields metadata")
    print("- The blob data from v1 is not copied/referenced in v2")
    print("- Result: blob fields become permanently None in v2")
    
    print("\n🎯 EXPECTED BEHAVIOR:")
    print("- Version 2 should preserve _blob_fields metadata")
    print("- Version 2 should reference the same S3 blobs as v1")
    print("- Blob fields should work transparently across versions")
    
    print_section("Cleanup")
    
    # Clean up the test resources
    print("\nCleaning up test resources...")
    memory.delete_all_versions(doc.resource_id, DocumentWithBlobs)
    print("✓ Deleted all versions and blobs")


if __name__ == "__main__":
    main()