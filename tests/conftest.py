import os
from typing import TYPE_CHECKING
from uuid import uuid4

import requests
from logzero import logger
import boto3
import pytest
from simplesingletable.utils import truncate_dynamo_table, create_standard_dynamodb_table
from simplesingletable import DynamoDbMemory

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


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
