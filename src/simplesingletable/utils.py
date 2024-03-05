import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import ulid
from boto3.dynamodb.types import TypeSerializer

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table


def _now(tz: Any = False):
    # this function exists only to make it easy to mock the utcnow call in date_id when creating resources in the tests

    # explicitly check for False, so that `None` is a valid option to provide for the tz
    if tz is False:
        tz = timezone.utc
    return datetime.now(tz=tz)


def generate_date_sortable_id(now=None) -> str:
    """Generates a ULID based on the provided timestamp, or the current time if not provided."""
    now = now or _now()
    return ulid.from_timestamp(now).str


def marshall(python_obj: dict) -> dict:
    """Convert a standard dict into a DynamoDB ."""
    serializer = TypeSerializer()
    return {k: serializer.serialize(v) for k, v in python_obj.items()}


def encode_pagination_key(last_evaluated_key: dict) -> str:
    """Turn the dynamodb LEK data into a pagination key we can send to clients."""
    return urlsafe_b64encode(json.dumps(last_evaluated_key).encode()).decode()


def decode_pagination_key(pagination_key: str) -> dict:
    """Turn the pagination key back into the dynamodb LEK dict."""
    return json.loads(urlsafe_b64decode(pagination_key).decode())


def create_standard_dynamodb_table(table_name: str, dynamodb_resource: "DynamoDBServiceResource") -> "Table":
    # Create the DynamoDB table
    table = dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "sk", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsitype", "AttributeType": "S"},
            {"AttributeName": "gsitypesk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi2pk", "AttributeType": "S"},
            {"AttributeName": "gsi3pk", "AttributeType": "S"},
            {"AttributeName": "gsi3sk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsitype",
                "KeySchema": [
                    {"AttributeName": "gsitype", "KeyType": "HASH"},
                    {"AttributeName": "gsitypesk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "gsi2",
                "KeySchema": [
                    {"AttributeName": "gsi2pk", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "gsi3",
                "KeySchema": [
                    {"AttributeName": "gsi3pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi3sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Wait for the table to be created
    table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
    return table


def truncate_dynamo_table(dynamo_table: "Table"):
    """Delete all items from a dynamo table.

    This is not a true SQL style truncation, as it must do a complete scan and
    delete each item. For tables with lots of items, it may be better to recreate the table.

    Adapted from https://stackoverflow.com/a/61641725

    """
    # get the table keys

    """
    NOTE: there are reserved attributes for key names, please see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
    if a hash or range key is in the reserved word list, you will need to use the ExpressionAttributeNames parameter
    described at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan
    """

    table_key_names = [key["AttributeName"] for key in dynamo_table.key_schema]

    # Only retrieve the keys for each item in the table (minimize data transfer)
    ProjectionExpression = ", ".join(table_key_names)

    response = dynamo_table.scan(ProjectionExpression=ProjectionExpression)
    data = response["Items"]

    while "LastEvaluatedKey" in response:
        response = dynamo_table.scan(
            ProjectionExpression=ProjectionExpression,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        data.extend(response["Items"])  # type: ignore

    with dynamo_table.batch_writer() as batch:
        for each in data:
            batch.delete_item(Key={key: each[key] for key in table_key_names})
