"""
Streamlit Demo App for simplesingletable - LOCAL FILE STORAGE VERSION

This interactive demo showcases the core features of simplesingletable using
LOCAL FILE STORAGE instead of DynamoDB and S3. No Docker containers needed!

Features:
- Basic CRUD operations
- Versioned resources with history
- Audit logging with field-level tracking
- Blob storage for large fields

Run with: streamlit run app_local.py
"""

import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from logzero import logger

from simplesingletable import LocalStorageMemory
from scenarios import SCENARIOS

# ==============================================================================
# Configuration
# ==============================================================================

# Use a persistent local directory (or temp directory for demo)
# Change this to a permanent path if you want data to persist between runs
STORAGE_DIR = Path.home() / ".simplesingletable_demo"
# For truly temporary storage, use:
# import tempfile
# STORAGE_DIR = Path(tempfile.gettempdir()) / "simplesingletable_demo"


# ==============================================================================
# Helper Functions
# ==============================================================================


def initialize_storage() -> Path:
    """Initialize local storage directory."""
    storage_path = Path(STORAGE_DIR)
    storage_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using storage directory: {storage_path}")
    return storage_path


def get_memory() -> LocalStorageMemory:
    """Get or create LocalStorageMemory instance."""
    if "memory" not in st.session_state:
        # Initialize storage directory
        storage_path = initialize_storage()

        # Create memory instance
        memory = LocalStorageMemory(
            logger=logger,
            storage_dir=str(storage_path),
            track_stats=True,
            use_blob_storage=True,
        )

        st.session_state.memory = memory
        st.session_state.storage_path = storage_path

    return st.session_state.memory


def reset_environment() -> None:
    """Reset the demo environment by clearing all data."""
    try:
        storage_path = st.session_state.get("storage_path", STORAGE_DIR)

        # Remove all data
        if Path(storage_path).exists():
            shutil.rmtree(storage_path)
            logger.info(f"Removed storage directory: {storage_path}")

        # Reinitialize
        if "memory" in st.session_state:
            del st.session_state.memory
        if "storage_path" in st.session_state:
            del st.session_state.storage_path

        # Clear blob loading session state
        if "loaded_blob_reports" in st.session_state:
            del st.session_state.loaded_blob_reports

        st.success("Environment reset successfully!")

    except Exception as e:
        logger.error(f"Failed to reset environment: {e}")
        st.error(f"Failed to reset environment: {e}")


def scan_local_storage(memory: LocalStorageMemory) -> list[dict]:
    """Scan local storage files and return all items."""
    try:
        all_items = []
        resources_dir = Path(memory.storage_dir) / "resources"

        if not resources_dir.exists():
            return []

        # Read all JSON files
        for json_file in resources_dir.glob("*.json"):
            try:
                with open(json_file, "r") as f:
                    data = json.load(f)
                    # Each file contains multiple items keyed by storage_key
                    for storage_key, item in data.items():
                        # Decode base64-encoded binary data if present
                        from simplesingletable.local_storage_memory import _decode_binary_data

                        decoded_item = _decode_binary_data(item)
                        all_items.append(decoded_item)
            except Exception as e:
                logger.error(f"Error reading {json_file}: {e}")

        return all_items

    except Exception as e:
        logger.error(f"Failed to scan storage: {e}")
        return []


def format_table_items_as_dataframe(items: list[dict]) -> Any:
    """Convert storage items to a formatted dataframe."""
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

        # Add resource-specific fields based on what's in the data
        # Handle compressed data
        if "data" in item and isinstance(item.get("data"), dict):
            # Compressed data case - extract from nested data dict
            data_content = item["data"]
            if "type" in data_content:
                resource_type = data_content["type"]
            else:
                resource_type = None
        else:
            # Non-compressed case
            resource_type = item.get("type")

        # Try to determine type from pk/sk if not found
        if not resource_type:
            pk = item.get("pk", "")
            if "User#" in pk:
                resource_type = "User"
            elif "Document#" in pk:
                resource_type = "Document"
            elif "Order#" in pk:
                resource_type = "Order"
            elif "Report#" in pk:
                resource_type = "Report"
            elif "AuditLog#" in pk:
                resource_type = "AuditLog"

        row["type"] = resource_type

        # Extract type-specific fields
        if resource_type == "User":
            row.update({"name": item.get("name", ""), "email": item.get("email", "")})
        elif resource_type == "Document":
            row.update({"title": item.get("title", ""), "version": item.get("version", "")})
        elif resource_type == "Order":
            row.update(
                {
                    "customer_email": item.get("customer_email", ""),
                    "status": item.get("status", ""),
                    "total_amount": item.get("total_amount", ""),
                }
            )
        elif resource_type == "Report":
            row.update({"title": item.get("title", ""), "author": item.get("author", "")})
        elif resource_type == "AuditLog":
            row.update(
                {
                    "operation": item.get("operation", ""),
                    "audited_resource_type": item.get("audited_resource_type", ""),
                    "changed_by": item.get("changed_by", ""),
                }
            )

        formatted_data.append(row)

    return pd.DataFrame(formatted_data)


def get_storage_stats(memory: LocalStorageMemory) -> dict:
    """Get storage statistics."""
    stats = {}

    storage_path = Path(memory.storage_dir)

    # Count files
    resources_dir = storage_path / "resources"
    if resources_dir.exists():
        json_files = list(resources_dir.glob("*.json"))
        stats["resource_files"] = len(json_files)

        # Calculate total size
        total_size = sum(f.stat().st_size for f in json_files)
        stats["resource_size_bytes"] = total_size
        stats["resource_size_mb"] = round(total_size / (1024 * 1024), 2)

    # Count blob files
    blobs_dir = storage_path / "blobs"
    if blobs_dir.exists():
        blob_files = list(blobs_dir.rglob("*"))
        blob_files = [f for f in blob_files if f.is_file() and not f.name.endswith(".meta")]
        stats["blob_files"] = len(blob_files)

        # Calculate blob size
        if blob_files:
            total_blob_size = sum(f.stat().st_size for f in blob_files)
            stats["blob_size_bytes"] = total_blob_size
            stats["blob_size_mb"] = round(total_blob_size / (1024 * 1024), 2)
        else:
            stats["blob_size_bytes"] = 0
            stats["blob_size_mb"] = 0.0

    return stats


# ==============================================================================
# Main App
# ==============================================================================


def main():
    st.set_page_config(
        page_title="simplesingletable Demo (Local Storage)",
        page_icon="📁",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title("📁 simplesingletable Interactive Demo")
    st.caption("🚀 Running with **LOCAL FILE STORAGE** - No Docker needed!")

    st.markdown(
        """
        This demo showcases the core features of **simplesingletable** using local file storage
        instead of DynamoDB and S3. All data is stored in JSON and blob files on your local filesystem.
        """
    )

    # Initialize memory
    try:
        memory = get_memory()
        storage_path = st.session_state.storage_path
    except Exception as e:
        st.error(f"Failed to initialize storage: {e}")
        st.stop()

    # Sidebar - Storage Info
    st.sidebar.header("💾 Storage Info")
    st.sidebar.write(f"**Location:** `{storage_path}`")

    # Get storage stats
    stats = get_storage_stats(memory)
    if stats:
        st.sidebar.write(f"**Resource Files:** {stats.get('resource_files', 0)}")
        st.sidebar.write(f"**Resource Size:** {stats.get('resource_size_mb', 0)} MB")
        st.sidebar.write(f"**Blob Files:** {stats.get('blob_files', 0)}")
        st.sidebar.write(f"**Blob Size:** {stats.get('blob_size_mb', 0)} MB")

    # Sidebar controls
    st.sidebar.divider()
    st.sidebar.header("🎮 Controls")

    if st.sidebar.button("🔄 Reset Environment", type="primary", help="Delete all data and start fresh"):
        reset_environment()
        st.rerun()

    if st.sidebar.button("📂 Open Storage Folder", help="Open the storage folder in Finder/Explorer"):
        import platform
        import subprocess

        system = platform.system()
        try:
            if system == "Darwin":  # macOS
                subprocess.run(["open", str(storage_path)])
            elif system == "Windows":
                subprocess.run(["explorer", str(storage_path)])
            elif system == "Linux":
                subprocess.run(["xdg-open", str(storage_path)])
            st.sidebar.success("Opened storage folder!")
        except Exception as e:
            st.sidebar.error(f"Could not open folder: {e}")

    # Scenario selector
    st.sidebar.divider()
    st.sidebar.header("🎯 Select Scenario")
    selected_scenario = st.selectbox(
        "Choose a feature to explore:",
        options=list(SCENARIOS.keys()),
        format_func=lambda x: f"{x}",
        label_visibility="collapsed",
    )

    scenario_info = SCENARIOS[selected_scenario]
    st.sidebar.info(scenario_info["description"])

    # Main content area - two columns
    left_col, right_col = st.columns([3, 2])

    # Left column - interactive scenario
    with left_col:
        st.header("🎮 Interactive Demo")
        scenario_info["function"](memory)

    # Right column - storage viewer
    with right_col:
        st.header("📊 Storage View")

        # View toggle
        view_mode = st.radio("View Mode", ["Formatted Table", "Raw JSON"], horizontal=True, label_visibility="collapsed")

        # Refresh button
        if st.button("🔄 Refresh View"):
            st.rerun()

        # Scan storage
        items = scan_local_storage(memory)
        st.caption(f"Total items in storage: {len(items)}")

        if not items:
            st.info("No items in storage. Create some resources to see them here!")
        else:
            if view_mode == "Formatted Table":
                df = format_table_items_as_dataframe(items)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Show expandable details
                with st.expander("🔍 View Full Item Details"):
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

    # Show file structure
    with st.expander("📁 View File Structure"):
        st.markdown("**Storage Directory Structure:**")

        resources_dir = Path(storage_path) / "resources"
        blobs_dir = Path(storage_path) / "blobs"

        structure = f"""
```
{storage_path.name}/
├── resources/
"""
        if resources_dir.exists():
            for json_file in sorted(resources_dir.glob("*.json")):
                size_kb = json_file.stat().st_size / 1024
                structure += f"│   ├── {json_file.name} ({size_kb:.1f} KB)\n"

        structure += "└── blobs/\n"
        if blobs_dir.exists():
            for resource_type_dir in sorted(blobs_dir.iterdir()):
                if resource_type_dir.is_dir():
                    structure += f"    └── {resource_type_dir.name}/\n"
                    for resource_id_dir in sorted(resource_type_dir.iterdir())[:3]:  # Show first 3
                        if resource_id_dir.is_dir():
                            structure += f"        └── {resource_id_dir.name}/\n"

        structure += "```"
        st.markdown(structure)

    st.markdown(
        """
        ---
        **Key Differences from DynamoDB Version:**
        - ✅ No Docker containers needed
        - ✅ All data stored in local files
        - ✅ Easy to inspect with text editors
        - ✅ Perfect for demos and offline development
        - ✅ Same API as DynamoDbMemory

        **Learn More:**
        - [GitHub Repository](https://github.com/msull/simplesingletable)
        - [Documentation](https://github.com/msull/simplesingletable#readme)
        """
    )


if __name__ == "__main__":
    main()
