# Local Storage Streamlit Demo

This directory contains **two versions** of the simplesingletable Streamlit demo:

## 🐳 Docker Version (`app.py`)

Uses **DynamoDB Local** and **MinIO** (local S3) running in Docker containers.

**Requirements:**
- Docker and Docker Compose
- DynamoDB Local container
- MinIO S3 container

**Setup:**
```bash
# Start services
docker-compose up -d

# Run demo
streamlit run app.py
```

**Best for:**
- Testing with real DynamoDB/S3 behavior
- Development that will deploy to AWS
- Testing multi-container setups

---

## 📁 Local File Storage Version (`app_local.py`)

Uses **LocalStorageMemory** with files stored directly on your filesystem. **No Docker needed!**

**Requirements:**
- Just Python and the simplesingletable package

**Setup:**
```bash
# Run demo (that's it!)
streamlit run app_local.py
```

**Best for:**
- Quick demos and exploration
- Offline development
- Learning simplesingletable features
- Situations where Docker isn't available

---

## Key Differences

| Feature | Docker Version (`app.py`) | Local Version (`app_local.py`) |
|---------|---------------------------|--------------------------------|
| **Setup** | Requires Docker containers | No setup needed |
| **Storage** | DynamoDB Local + MinIO | JSON files + local blobs |
| **Speed** | Network calls to containers | Direct file I/O |
| **Inspection** | DynamoDB/S3 tools | Text editors, file browser |
| **Portability** | Needs Docker everywhere | Works anywhere Python runs |
| **AWS Similarity** | Very similar to real AWS | Simplified implementation |

## Storage Locations

### Docker Version
- **DynamoDB Data**: Stored in Docker volume
- **S3/MinIO Data**: Stored in Docker volume
- **Access**: Via AWS CLI or web interfaces

### Local Version
- **All Data**: `~/.simplesingletable_demo/`
  - `resources/*.json` - Resource data
  - `blobs/` - Blob field storage
- **Access**: Browse with any file manager or text editor

## Features Comparison

Both versions support the same demo scenarios:

✅ **Basic Resource CRUD** - Create, read, update, delete operations
✅ **Versioned Resources** - Full version history tracking
✅ **Audit Logging** - Field-level change tracking
✅ **Blob Storage** - Large fields stored separately

The local version uses `LocalStorageMemory` which provides the **same API** as `DynamoDbMemory`, making it a true drop-in replacement for demos and testing.

## Example: Switching Between Versions

Both use the same scenarios from `scenarios.py`:

```python
# app.py (Docker version)
memory = DynamoDbMemory(
    logger=logger,
    table_name=TABLE_NAME,
    endpoint_url=DYNAMODB_ENDPOINT,
    s3_bucket=BUCKET_NAME,
    connection_params=AWS_CONFIG,
)

# app_local.py (Local version)
memory = LocalStorageMemory(
    logger=logger,
    storage_dir=str(storage_path),
    track_stats=True,
    use_blob_storage=True,
)

# Same API after this point!
users = memory.list_type_by_updated_at(User, results_limit=50)
```

## Tips for Local Version

1. **Persistent Storage**: By default, data is stored in `~/.simplesingletable_demo/`. Change `STORAGE_DIR` in `app_local.py` to use a different location.

2. **Temporary Storage**: Uncomment the `tempfile` lines to use truly temporary storage that gets cleaned up.

3. **Inspect Data**: Click "Open Storage Folder" in the sidebar to browse the JSON files directly.

4. **Reset Data**: Use the "Reset Environment" button to delete all data and start fresh.

5. **File Format**: All resource data is stored as human-readable JSON with base64-encoded binary data.

## Running the Demo

### Quick Start (Local Version)
```bash
cd examples/streamlit_demo
streamlit run app_local.py
```

Visit http://localhost:8501 and start exploring!

### With Docker (Full Version)
```bash
cd examples/streamlit_demo
docker-compose up -d
streamlit run app.py
```

## Troubleshooting

### Local Version
- **Permission errors**: Check write permissions for `~/.simplesingletable_demo/`
- **Storage not clearing**: Manually delete the storage directory
- **Slow performance**: Large datasets work better with the Docker version

### Docker Version
- **Services not starting**: Run `docker-compose up -d` first
- **Port conflicts**: Check ports 8000 (DynamoDB) and 9000 (MinIO) are free
- **Connection refused**: Wait for containers to fully start (10-15 seconds)

## Learn More

- **LocalStorageMemory**: See `examples/local_storage_example.py`
- **DynamoDbMemory**: See main project documentation
- **Scenarios**: See `scenarios.py` for resource definitions
