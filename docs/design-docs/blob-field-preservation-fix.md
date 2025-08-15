# Blob Field Preservation Fix Design

## Problem Statement

When updating a versioned resource without modifying its blob fields, the blob field metadata (`_blob_fields`) is not preserved in the new version. This causes blob fields to become permanently `None` in the new version, even though the blob data still exists in S3.

## Root Cause

The `to_dynamodb_item()` method only adds `_blob_fields` marker when there's actual blob data to store. When updating without changing blob fields, these fields are `None`, so no marker is added.

## Proposed Solution: Blob Version References

### Key Changes

1. **Always preserve `_blob_fields` list**: Include it in DynamoDB items whenever the resource has blob field configuration, regardless of whether there's data.

2. **Add `_blob_versions` mapping**: Track which S3 version each blob field references:
   ```python
   {
     "content": 1,      # References v1/content in S3
     "attachments": 1   # References v1/attachments in S3
   }
   ```

3. **Smart blob handling in updates**:
   - For unchanged blob fields: preserve the version reference
   - For updated blob fields: store new blob and update reference
   - For cleared blob fields: remove the version reference

### Implementation Details

#### 1. Modified DynamoDB Item Structure

```python
# Version 1 (with blobs)
{
  "pk": "Document#123",
  "sk": "v1",
  "version": 1,
  "data": "...",
  "_blob_fields": ["content", "attachments"],
  "_blob_versions": {"content": 1, "attachments": 1}
}

# Version 2 (after update without changing blobs)
{
  "pk": "Document#123", 
  "sk": "v2",
  "version": 2,
  "data": "...",
  "_blob_fields": ["content", "attachments"],
  "_blob_versions": {"content": 1, "attachments": 1}  # Still references v1 blobs
}

# Version 3 (after updating only content)
{
  "pk": "Document#123",
  "sk": "v3", 
  "version": 3,
  "data": "...",
  "_blob_fields": ["content", "attachments"],
  "_blob_versions": {"content": 3, "attachments": 1}  # Mixed versions
}
```

#### 2. Update Flow

```python
def update_existing(resource, update_data):
    # 1. Get current blob state
    current_blob_versions = resource._blob_versions or {}
    
    # 2. Determine which blob fields are being updated
    blob_config = resource.resource_config.get("blob_fields", {})
    
    # 3. Build new blob versions map
    new_blob_versions = {}
    for field_name in blob_config:
        if field_name in update_data:
            # Field is being updated - will get new version
            new_blob_versions[field_name] = resource.version + 1
        elif field_name in current_blob_versions:
            # Field not updated - preserve existing reference
            new_blob_versions[field_name] = current_blob_versions[field_name]
    
    # 4. Store with preserved metadata
    # ...
```

#### 3. Loading Blobs

```python
def load_blob_fields(resource, memory):
    blob_versions = resource._blob_versions or {}
    
    for field_name in resource._blob_placeholders:
        # Use the version reference to load the correct S3 object
        blob_version = blob_versions.get(field_name, resource.version)
        
        blob_data = memory.s3_blob_storage.get_blob(
            resource_type=resource.__class__.__name__,
            resource_id=resource.resource_id,
            field_name=field_name,
            version=blob_version  # Use referenced version, not current
        )
        
        setattr(resource, field_name, blob_data)
```

## Benefits

1. **Preserves blob accessibility**: Blob fields remain loadable across all versions
2. **Storage efficient**: No duplication of unchanged blobs
3. **Version integrity**: Each version correctly references its blob data
4. **Backward compatible**: Existing code continues to work

## Migration

For existing resources without `_blob_versions`:
- Assume blob version equals resource version (current behavior)
- Gradually migrate as resources are updated

## Testing

1. Create versioned resource with blobs
2. Update without changing blobs - verify they're preserved
3. Update with partial blob changes - verify mixed versions work
4. Clear blob fields - verify they're properly removed
5. Load blobs from various versions - verify correct data is loaded