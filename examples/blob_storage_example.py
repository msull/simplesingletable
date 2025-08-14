"""
Example demonstrating S3 blob storage for large fields in simplesingletable.

This example shows how to configure certain fields to be stored in S3 instead
of DynamoDB, enabling efficient storage of large data while maintaining fast
query performance.
"""

from typing import Optional

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource, DynamoDbResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig



# Example 1: Non-versioned resource with blob fields
class DocumentResource(DynamoDbResource):
    """A document with large content stored in S3."""
    
    title: str
    author: str
    summary: str
    # Large fields marked as Optional and configured as blobs
    content: Optional[str] = None
    attachments: Optional[dict] = None
    
    resource_config = ResourceConfig(
        compress_data=False,  # Main record compression
        blob_fields={
            "content": BlobFieldConfig(
                compress=True,  # Compress before storing in S3
                content_type="text/plain"
            ),
            "attachments": BlobFieldConfig(
                compress=True,
                content_type="application/json",
                max_size_bytes=10 * 1024 * 1024  # 10MB limit
            )
        }
    )


# Example 2: Versioned resource with blob fields
class AnalysisReport(DynamoDbVersionedResource):
    """Versioned analysis reports with large result data."""
    
    report_name: str
    status: str
    created_by: str
    # Large analysis results stored in S3
    raw_data: Optional[dict] = None
    processed_results: Optional[dict] = None
    
    resource_config = ResourceConfig(
        compress_data=True,  # Compress main record in DynamoDB
        max_versions=10,  # Keep last 10 versions
        blob_fields={
            "raw_data": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            ),
            "processed_results": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            )
        }
    )
    
    def db_get_gsi1pk(self) -> str | None:
        """Enable querying by status."""
        return f"status#{self.status}"


def main():
    # Initialize DynamoDbMemory with S3 configuration
    memory = DynamoDbMemory(
        logger=print,  # Use print for demo
        table_name="my-application-table",
        s3_bucket="my-application-blobs",  # S3 bucket for blob storage
        s3_key_prefix="prod/blobs",  # Optional prefix for S3 keys
        # For local testing with LocalStack or MinIO:
        # endpoint_url="http://localhost:4566",
        # connection_params={
        #     "aws_access_key_id": "test",
        #     "aws_secret_access_key": "test",
        #     "region_name": "us-east-1"
        # }
    )
    
    # Example 1: Create a document with large content
    print("\n=== Creating Document with Blob Fields ===")
    
    # Large content that would be inefficient in DynamoDB
    large_content = "This is a very large document content..." * 1000
    
    document = memory.create_new(
        DocumentResource,
        {
            "title": "Q4 2024 Financial Report",
            "author": "Finance Team",
            "summary": "Quarterly financial results and analysis",
            "content": large_content,  # Automatically stored in S3
            "attachments": {
                "charts": ["revenue.png", "expenses.png"],
                "data": {"revenue": 1000000, "expenses": 800000}
            }
        }
    )
    
    print(f"Created document: {document.resource_id}")
    print(f"Title: {document.title}")
    print(f"Content stored in S3: {len(large_content)} characters")
    
    # Example 2: Reading documents - fast without loading blobs
    print("\n=== Reading Document Metadata (without blobs) ===")
    
    # Fast read - doesn't fetch from S3
    doc = memory.get_existing(
        document.resource_id,
        DocumentResource,
        load_blobs=False  # Don't load blob fields
    )
    
    print(f"Title: {doc.title}")
    print(f"Author: {doc.author}")
    print(f"Summary: {doc.summary}")
    print(f"Content loaded: {doc.content is not None}")
    print(f"Has unloaded blobs: {doc.has_unloaded_blobs()}")
    print(f"Unloaded fields: {doc.get_unloaded_blob_fields()}")
    
    # Example 3: Load blob fields when needed
    print("\n=== Loading Blob Fields On Demand ===")
    
    # Load all blob fields
    doc.load_blob_fields(memory)
    print(f"Content loaded: {doc.content is not None}")
    print(f"Content length: {len(doc.content)} characters")
    print(f"Attachments: {doc.attachments}")
    
    # Or load specific fields only
    doc2 = memory.get_existing(document.resource_id, DocumentResource)
    doc2.load_blob_fields(memory, fields=["content"])  # Load only content
    print(f"Content loaded: {doc2.content is not None}")
    print(f"Attachments loaded: {doc2.attachments is not None}")
    
    # Example 4: Read with blobs in one operation
    print("\n=== Reading Document with Blobs ===")
    
    doc_with_blobs = memory.get_existing(
        document.resource_id,
        DocumentResource,
        load_blobs=True  # Load blob fields immediately
    )
    
    print(f"All fields loaded: content={doc_with_blobs.content is not None}, "
          f"attachments={doc_with_blobs.attachments is not None}")
    
    # Example 5: Versioned resources with blobs
    print("\n=== Versioned Resource with Blobs ===")
    
    # Create analysis report
    report = memory.create_new(
        AnalysisReport,
        {
            "report_name": "Customer Segmentation Analysis",
            "status": "completed",
            "created_by": "analytics-team",
            "raw_data": {"customers": list(range(10000))},  # Large dataset
            "processed_results": {
                "segments": ["premium", "standard", "basic"],
                "distributions": [0.2, 0.5, 0.3]
            }
        }
    )
    
    print(f"Created report v{report.version}: {report.report_name}")
    
    # Update report - creates new version with new blobs
    updated_report = memory.update_existing(
        report,
        {
            "status": "revised",
            "processed_results": {
                "segments": ["premium", "standard", "basic", "inactive"],
                "distributions": [0.15, 0.45, 0.25, 0.15]
            }
        }
    )
    
    print(f"Updated to v{updated_report.version} with status: {updated_report.status}")
    
    # Load different versions
    v1 = memory.get_existing(report.resource_id, AnalysisReport, version=1, load_blobs=True)
    v2 = memory.get_existing(report.resource_id, AnalysisReport, version=2, load_blobs=True)
    
    print(f"Version 1 segments: {v1.processed_results['segments']}")
    print(f"Version 2 segments: {v2.processed_results['segments']}")
    
    # Example 6: Query without loading blobs
    print("\n=== Efficient Queries (without loading blobs) ===")
    
    # Create multiple documents
    for i in range(3):
        memory.create_new(
            DocumentResource,
            {
                "title": f"Document {i}",
                "author": "Test Author",
                "summary": f"Summary {i}",
                "content": f"Large content {i}" * 1000,
                "attachments": {"index": i}
            }
        )
    
    # Query documents - blobs not loaded by default
    documents = memory.list_type_by_updated_at(
        DocumentResource,
        results_limit=10
    )
    
    print(f"Found {len(documents)} documents")
    for doc in documents[:3]:
        print(f"  - {doc.title}: blobs loaded={not doc.has_unloaded_blobs()}")
    
    # Load blobs for specific documents if needed
    if documents and documents[0].has_unloaded_blobs():
        documents[0].load_blob_fields(memory)
        print(f"Loaded blobs for: {documents[0].title}")
    
    # Example 7: Cleanup
    print("\n=== Cleanup ===")
    
    # Deleting a resource also deletes its blobs from S3
    memory.delete_existing(document)
    print("Deleted document and its S3 blobs")
    
    # For versioned resources, delete all versions and their blobs
    memory.delete_all_versions(report.resource_id, AnalysisReport)
    print("Deleted all versions of report and their S3 blobs")


if __name__ == "__main__":
    # Note: This example requires:
    # 1. A DynamoDB table (local or AWS)
    # 2. An S3 bucket (local with LocalStack/MinIO or AWS)
    # 3. Proper AWS credentials or local endpoint configuration
    
    # For local testing with docker-compose:
    # docker run -p 8000:8000 amazon/dynamodb-local
    # docker run -p 9000:9000 minio/minio server /data
    
    main()