from .dynamodb_memory import (
    DynamoDbMemory,
    DynamoDbResource,
    DynamoDbVersionedResource,
    PaginatedList,
    exhaust_pagination,
)

package_version = "3.4.0"

_ = DynamoDbMemory
_ = DynamoDbResource
_ = DynamoDbVersionedResource
_ = PaginatedList
_ = exhaust_pagination
