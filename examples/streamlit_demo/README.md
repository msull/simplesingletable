# simplesingletable Interactive Demo

This is an interactive Streamlit application that demonstrates the core features of the **simplesingletable** library.

## Features Demonstrated

1. **Basic Resource CRUD** - Simple create, read, update, delete operations with `DynamoDbResource`
2. **Versioned Resource** - Full version history tracking with `DynamoDbVersionedResource`
3. **Resource with Auditing** - Comprehensive audit logging with field-level change tracking
4. **Resource with Blob Storage** - Large fields stored in S3/MinIO with lazy loading

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- simplesingletable installed with dev dependencies

## Quick Start

### 1. Install Dependencies

From the repository root:

```bash
# Install the package with dev dependencies
pip install -e ".[dev]"
```

### 2. Start Docker Services

Navigate to the demo directory and start DynamoDB Local and MinIO:

```bash
cd examples/streamlit_demo
docker-compose up -d
```

This will start:
- **DynamoDB Local** on port 8000
- **MinIO** (S3-compatible storage) on ports 9000 (API) and 9001 (Console)

You can access the MinIO console at http://localhost:9001 with credentials:
- Username: `minioadmin`
- Password: `minioadmin`

### 3. Run the Demo App

From the `examples/streamlit_demo` directory:

```bash
streamlit run app.py
```

The app should automatically open in your browser at http://localhost:8501

## Using the Demo

### Layout

The demo has a **two-column layout**:

**Left Column (Interactive Demo):**
- Select different scenarios from the sidebar
- View code examples showing resource definitions
- Use interactive forms to create, read, update, and delete resources
- See results and status messages

**Right Column (DynamoDB Table View):**
- Toggle between "Formatted Table" and "Raw JSON" views
- See all items in the DynamoDB table in real-time
- Observe how single-table design works with different resource types
- Expand individual items to see full details

### Scenarios

#### 1. Basic Resource CRUD
- Create simple User resources with name, email, and tags
- Update and delete users
- Notice how resources use a single DynamoDB item (sk="0")

#### 2. Versioned Resource
- Create Document resources that maintain version history
- Update documents to create new versions
- Restore previous versions
- See how each version creates a new item in DynamoDB (sk="0", "1", "2", ...)

#### 3. Resource with Auditing
- Create Order resources with audit logging enabled
- Track who made changes with the `changed_by` parameter
- View complete audit trail with field-level changes
- See AuditLog entries appear alongside resource items in the table

#### 4. Resource with Blob Storage
- Create Report resources with large content fields
- Observe how large fields are stored in MinIO (S3) instead of DynamoDB
- See blob references in the DynamoDB table
- Load blobs on-demand for performance

### Controls

- **Reset Environment** - Clears all data from DynamoDB and MinIO (useful for starting fresh)
- **Refresh Table View** - Updates the right column with latest table data
- **Service Status** - Shows connection status for DynamoDB and MinIO

## Understanding the Code

The demo consists of three main files:

1. **`app.py`** - Main Streamlit application with:
   - Service initialization (DynamoDB + MinIO)
   - Two-column layout
   - Table viewer with formatted and raw views
   - Reset and refresh controls

2. **`scenarios.py`** - Scenario definitions including:
   - Resource class definitions (User, Document, Order, Report)
   - Interactive forms for each scenario
   - Example CRUD operations
   - Audit log querying

3. **`docker-compose.yml`** - Docker services:
   - DynamoDB Local
   - MinIO (S3-compatible object storage)

## Exploring the DynamoDB Table

The right column shows how simplesingletable implements **single-table design**:

- All resource types share the same table
- Primary key (`pk`) and sort key (`sk`) create unique items
- Current version: `sk="0"`, historical versions: `sk="1"`, `sk="2"`, etc.
- GSI (Global Secondary Index) fields support secondary access patterns
- Blob fields show as references rather than actual data

## Troubleshooting

### Services not available

If you see "Services not available" error:

```bash
# Check if containers are running
docker-compose ps

# View container logs
docker-compose logs

# Restart services
docker-compose restart
```

### Port conflicts

If ports 8000, 9000, or 9001 are already in use:

1. Stop the conflicting service
2. Or modify `docker-compose.yml` to use different ports
3. Update the endpoints in `app.py` to match

### Clear all data

Use the "Reset Environment" button in the app, or manually:

```bash
# Stop and remove containers (this clears all data)
docker-compose down -v

# Start fresh
docker-compose up -d
```

## Stopping the Demo

When you're done:

```bash
# Stop the Streamlit app (Ctrl+C in the terminal)

# Stop Docker services
docker-compose down

# Or keep services running for next time
docker-compose stop
```

## Next Steps

After exploring the demo, check out:

- [Main README](../../README.md) - Library overview and features
- [Examples directory](../) - More code examples
- [Source code](../../src/simplesingletable/) - Implementation details
- [Tests](../../tests/) - Comprehensive test suite

## Learn More

- **Repository:** https://github.com/msull/simplesingletable
- **PyPI:** https://pypi.org/project/simplesingletable/
- **DynamoDB Single-Table Design:** https://aws.amazon.com/blogs/compute/creating-a-single-table-design-with-amazon-dynamodb/
