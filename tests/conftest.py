import os
from typing import TYPE_CHECKING
from uuid import uuid4

import requests
from logzero import logger
import boto3
import pytest
from simplesingletable.utils import truncate_dynamo_table, create_standard_dynamodb_table
from simplesingletable import DynamoDbMemory
from simplesingletable.blob_storage import S3BlobStorage

from botocore.exceptions import ClientError


if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "tests", "docker-compose.yml")


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 400:
            return True
    except requests.ConnectionError:
        return False


@pytest.fixture(scope="session")
def dynamodb_via_docker(docker_ip, docker_services):
    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("dynamodb", 8000)
    url = "http://{}:{}".format(docker_ip, port)
    docker_services.wait_until_responsive(timeout=30.0, pause=0.1, check=lambda: is_responsive(url))
    return url


@pytest.fixture(scope="session")
def local_dynamodb_test_table(dynamodb_via_docker) -> "Table":
    table_created = False
    table_name = f"delta-dynamodb-test-table-{uuid4().hex}"

    aws_access_key_id = "unused"
    aws_secret_access_key = "unused"
    region_name = "us-west-2"

    client = boto3.client(
        "dynamodb",
        endpoint_url=dynamodb_via_docker,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=dynamodb_via_docker,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    try:
        table = create_standard_dynamodb_table(table_name=table_name, dynamodb_resource=resource)
        dynamodb_table_arn = table.table_arn
        logger.info(f"Dynamo Table Created {table_name=} {dynamodb_table_arn=}")
        table_created = True
        yield resource.Table(table_name)
    finally:
        if table_created:
            logger.info(f"Deleting generated dynamo table {table_name=}")
            client.delete_table(TableName=table_name)


def reset_local_dynamodb_test_table(table: "Table"):
    logger.debug(f"Resetting table {table.table_name}")
    truncate_dynamo_table(table)


@pytest.fixture()
def dynamodb_memory(local_dynamodb_test_table, dynamodb_via_docker) -> DynamoDbMemory:
    reset_local_dynamodb_test_table(local_dynamodb_test_table)
    yield DynamoDbMemory(
        logger=logger,
        table_name=local_dynamodb_test_table.table_name,
        endpoint_url=dynamodb_via_docker,
        connection_params={
            "aws_access_key_id": "unused",
            "aws_secret_access_key": "unused",
            "region_name": "us-west-2",
        },
    )


def is_minio_responsive(url):
    """Check if MinIO is responsive."""
    try:
        response = requests.get(f"{url}/minio/health/live")
        return response.status_code == 200
    except requests.ConnectionError:
        return False


@pytest.fixture(scope="session")
def minio_via_docker(docker_ip, docker_services):
    """Get MinIO service URL from docker-compose."""
    port = docker_services.port_for("minio", 9000)
    url = f"http://{docker_ip}:{port}"
    docker_services.wait_until_responsive(timeout=30.0, pause=0.5, check=lambda: is_minio_responsive(url))
    return url


@pytest.fixture(scope="session")
def minio_s3_client(minio_via_docker) -> "S3Client":
    """Create S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=minio_via_docker,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        use_ssl=False,
    )


@pytest.fixture(scope="function")
def minio_s3_bucket(minio_s3_client):
    """Create a test bucket in MinIO for each test."""
    bucket_name = f"test-bucket-{uuid4().hex[:8]}"

    # Create bucket
    try:
        minio_s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Created MinIO bucket: {bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise

    yield bucket_name

    # Cleanup: Delete all objects and the bucket
    try:
        # List and delete all objects
        paginator = minio_s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    minio_s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

        # Delete the bucket
        minio_s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"Deleted MinIO bucket: {bucket_name}")
    except ClientError as e:
        logger.warning(f"Error cleaning up bucket {bucket_name}: {e}")


@pytest.fixture()
def dynamodb_memory_with_s3(local_dynamodb_test_table, dynamodb_via_docker, minio_via_docker, minio_s3_bucket):
    """Create DynamoDbMemory instance with both DynamoDB and S3 (MinIO) configured."""
    from conftest import reset_local_dynamodb_test_table

    reset_local_dynamodb_test_table(local_dynamodb_test_table)

    # Create S3 client for MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_via_docker,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        use_ssl=False,
    )

    # Create S3BlobStorage instance with MinIO client
    s3_blob_storage = S3BlobStorage(
        bucket_name=minio_s3_bucket,
        key_prefix="test-blobs",
        s3_client=s3_client,
        # cache_enabled=False
    )

    # Create DynamoDbMemory with proper DynamoDB credentials and pass the S3 blob storage
    memory = DynamoDbMemory(
        logger=logger,
        table_name=local_dynamodb_test_table.table_name,
        endpoint_url=dynamodb_via_docker,
        s3_bucket=minio_s3_bucket,
        s3_key_prefix="test-blobs",
        connection_params={
            "aws_access_key_id": "unused",
            "aws_secret_access_key": "unused",
            "region_name": "us-west-2",
        },
    )

    # Override the S3 blob storage with our MinIO-configured one
    memory._s3_blob_storage = s3_blob_storage

    yield memory


@pytest.fixture()
def dynamodb_memory_with_library_built_s3(
    local_dynamodb_test_table, dynamodb_via_docker, minio_via_docker, minio_s3_bucket
):
    """Like dynamodb_memory_with_s3, but S3BlobStorage builds its own clients from endpoint + connection params."""
    from conftest import reset_local_dynamodb_test_table

    reset_local_dynamodb_test_table(local_dynamodb_test_table)

    memory = DynamoDbMemory(
        logger=logger,
        table_name=local_dynamodb_test_table.table_name,
        endpoint_url=dynamodb_via_docker,
        s3_bucket=minio_s3_bucket,
        s3_key_prefix="test-blobs",
        connection_params={
            "aws_access_key_id": "unused",
            "aws_secret_access_key": "unused",
            "region_name": "us-west-2",
        },
    )

    # No s3_client and no config: S3BlobStorage builds both its operations and presign clients.
    memory._s3_blob_storage = S3BlobStorage(
        bucket_name=minio_s3_bucket,
        key_prefix="test-blobs",
        endpoint_url=minio_via_docker,
        connection_params={
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "region_name": "us-east-1",
        },
    )

    yield memory


_LOCAL_CONNECTION_PARAMS = {
    "aws_access_key_id": "unused",
    "aws_secret_access_key": "unused",
    "region_name": "us-west-2",
}


def _memory_with_single_index(dynamodb_via_docker, name_prefix, index_name, key_schema, attribute_types):
    """Create a table with one GSI, yield a DynamoDbMemory on it, then drop the table.

    ``key_schema`` is ``[(attribute, "HASH"|"RANGE"), ...]``; ``attribute_types`` maps each index
    attribute to its DynamoDB scalar type.
    """
    table_name = f"{name_prefix}-{uuid4().hex}"
    client = boto3.client("dynamodb", endpoint_url=dynamodb_via_docker, **_LOCAL_CONNECTION_PARAMS)
    resource = boto3.resource("dynamodb", endpoint_url=dynamodb_via_docker, **_LOCAL_CONNECTION_PARAMS)

    resource.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            *({"AttributeName": a, "AttributeType": t} for a, t in attribute_types.items()),
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": index_name,
                "KeySchema": [{"AttributeName": a, "KeyType": k} for a, k in key_schema],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resource.Table(table_name).wait_until_exists()

    try:
        yield DynamoDbMemory(
            logger=logger,
            table_name=table_name,
            endpoint_url=dynamodb_via_docker,
            connection_params=_LOCAL_CONNECTION_PARAMS,
        )
    finally:
        client.delete_table(TableName=table_name)


@pytest.fixture()
def memory_with_custom_index(dynamodb_via_docker) -> DynamoDbMemory:
    """A table whose only GSI is ``owner-index``, keyed on custom attributes ``ownerKey`` / ``createdKey``.

    Neither the index name nor the attribute names follow the gsiN convention, so nothing can be
    inferred: only an explicit ``GsiIndex`` declaration can bind them.
    """
    yield from _memory_with_single_index(
        dynamodb_via_docker,
        "custom-gsi-test-table",
        "owner-index",
        [("ownerKey", "HASH"), ("createdKey", "RANGE")],
        {"ownerKey": "S", "createdKey": "S"},
    )


@pytest.fixture()
def memory_with_numeric_index(dynamodb_via_docker) -> DynamoDbMemory:
    """A table whose only GSI is ``score-index``, hash key ``scoreKey`` typed ``N``, no range key."""
    yield from _memory_with_single_index(
        dynamodb_via_docker,
        "numeric-gsi-test-table",
        "score-index",
        [("scoreKey", "HASH")],
        {"scoreKey": "N"},
    )
