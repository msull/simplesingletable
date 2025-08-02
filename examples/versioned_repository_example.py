#!/usr/bin/env python3
"""Example demonstrating the VersionedResourceRepository with versioning API.

This example shows how to use the versioning features:
- list_versions: Get metadata about all versions
- get_version: Retrieve a specific version
- restore_version: Restore an older version by creating a new version
"""

from typing import Optional
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource
from simplesingletable.extras.versioned_repository import VersionedResourceRepository


class Document(DynamoDbVersionedResource):
    """A versioned document model."""
    title: str
    content: str
    author: Optional[str] = None
    tags: Optional[list[str]] = None


class CreateDocumentSchema(BaseModel):
    """Schema for creating documents."""
    title: str
    content: str
    author: Optional[str] = None
    tags: Optional[list[str]] = None


class UpdateDocumentSchema(BaseModel):
    """Schema for updating documents."""
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    tags: Optional[list[str]] = None


def main():
    """Demonstrate versioning API usage."""
    # Initialize the repository
    memory = DynamoDbMemory(table_name="your-table-name")
    doc_repo = VersionedResourceRepository(
        ddb=memory,
        model_class=Document,
        create_schema_class=CreateDocumentSchema,
        update_schema_class=UpdateDocumentSchema,
    )
    
    print("=== Versioned Repository Example ===\n")
    
    # 1. Create initial document (version 1)
    print("1. Creating initial document...")
    doc = doc_repo.create({
        "title": "My Important Document",
        "content": "This is the initial version of my document.",
        "author": "John Doe",
        "tags": ["draft", "important"]
    })
    print(f"   Created: {doc.title} (v{doc.version})")
    doc_id = doc.resource_id
    
    # 2. Update document (version 2)
    print("\n2. Updating document content...")
    doc = doc_repo.update(doc_id, {
        "content": "This is the revised version with better content.",
        "tags": ["revised", "important"]
    })
    print(f"   Updated: {doc.title} (v{doc.version})")
    
    # 3. Another update (version 3)
    print("\n3. Another update...")
    doc = doc_repo.update(doc_id, {
        "title": "My Very Important Document",
        "content": "This is the final version with the best content ever!",
        "author": "Jane Smith",
        "tags": ["final", "published"]
    })
    print(f"   Updated: {doc.title} (v{doc.version})")
    
    # 4. List all versions
    print("\n4. Listing all versions...")
    versions = doc_repo.list_versions(doc_id)
    for version in versions:
        latest_mark = " (LATEST)" if version.is_latest else ""
        print(f"   - {version.version_id}: "
              f"v{version.version_number} - "
              f"{version.updated_at.strftime('%Y-%m-%d %H:%M:%S')}{latest_mark}")
    
    # 5. Get specific versions
    print("\n5. Retrieving specific versions...")
    
    # Get version 1
    v1_doc = doc_repo.get_version(doc_id, 1)
    if v1_doc:
        print(f"   v1: '{v1_doc.title}' by {v1_doc.author}")
        print(f"       Content: {v1_doc.content}")
        print(f"       Tags: {v1_doc.tags}")
    
    # Get version 2
    v2_doc = doc_repo.get_version(doc_id, 2)
    if v2_doc:
        print(f"   v2: '{v2_doc.title}' by {v2_doc.author}")
        print(f"       Content: {v2_doc.content}")
        print(f"       Tags: {v2_doc.tags}")
    
    # Get latest version
    latest = doc_repo.get(doc_id)
    if latest:
        print(f"   Latest: '{latest.title}' by {latest.author} (v{latest.version})")
        print(f"          Content: {latest.content}")
        print(f"          Tags: {latest.tags}")
    
    # 6. Restore previous version
    print("\n6. Restoring version 1...")
    restored = doc_repo.restore_version(doc_id, 1)
    print(f"   Restored v1 as new v{restored.version}")
    print(f"   Title: {restored.title}")
    print(f"   Author: {restored.author}")
    print(f"   Content: {restored.content}")
    print(f"   Tags: {restored.tags}")
    
    # 7. Final version list
    print("\n7. Final version list after restoration...")
    final_versions = doc_repo.list_versions(doc_id)
    for version in final_versions:
        latest_mark = " (LATEST)" if version.is_latest else ""
        print(f"   - {version.version_id}: "
              f"v{version.version_number} - "
              f"{version.updated_at.strftime('%Y-%m-%d %H:%M:%S')}{latest_mark}")
    
    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()