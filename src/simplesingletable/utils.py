from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


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
