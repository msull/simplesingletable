import decimal
import time
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Optional, Type, TypeVar, Union

import boto3
from boto3.dynamodb.conditions import ConditionBase, Key
from pydantic import BaseModel

from .models import DynamodbResource, DynamodbVersionedResource, PaginatedList
from .utils import decode_pagination_key, encode_pagination_key, marshall

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table


class Constants:
    SYSTEM_DEFAULT_LIMIT = 250
    QUERY_DEFAULT_MAX_API_CALLS = 10


package_version = "1.4.0"

AnyDbResource = TypeVar("AnyDbResource", bound=Union[DynamodbVersionedResource, DynamodbResource])
VersionedDbResourceOnly = TypeVar("VersionedDbResourceOnly", bound=DynamodbVersionedResource)
NonversionedDbResourceOnly = TypeVar("NonversionedDbResourceOnly", bound=DynamodbResource)

_PlainBaseModel = TypeVar("_PlainBaseModel", bound=BaseModel)


def exhaust_pagination(query: Callable[[Optional[str]], PaginatedList]):
    result = query(None)
    while result.next_pagination_key:
        yield result
        result = query(result.next_pagination_key)
    yield result


@dataclass
class DynamoDBMemory:
    logger: Any
    table_name: str
    endpoint_url: Optional[str] = None
    connection_params: Optional[dict] = None
    _dynamodb_client: Optional["DynamoDBClient"] = field(default=None, init=False)
    _dynamodb_table: Optional["Table"] = field(default=None, init=False)

    def get_existing(
        self,
        existing_id: str,
        data_class: Type[AnyDbResource],
        version: int = 0,
        consistent_read=False,
    ) -> Optional[AnyDbResource]:
        """Get object of the specified type with the provided key.

        The `version` parameter is ignored on non-versioned resources.
        """
        if issubclass(data_class, DynamodbResource):
            if version:
                self.logger.warning(
                    f"Version parameter ignored when fetching non-versioned resource; provided {version=}"
                )
            key = data_class.dynamodb_lookup_keys_from_id(existing_id)
        elif issubclass(data_class, DynamodbVersionedResource):
            key = data_class.dynamodb_lookup_keys_from_id(existing_id, version=version)
        else:
            raise ValueError("Invalid data_class provided")
        response = self.dynamodb_table.get_item(Key=key, ConsistentRead=consistent_read)
        item = response.get("Item")
        if item:
            return data_class.from_dynamodb_item(item)

    def read_existing(
        self,
        existing_id: str,
        data_class: Type[AnyDbResource],
        version: int = 0,
        consistent_read=False,
    ) -> AnyDbResource:
        """Return object of the specified type with the provided key.

        The `version` parameter is ignored on non-versioned resources.

        Raises a ValueError if no object with the provided id was found.
        """
        if not (item := self.get_existing(existing_id, data_class, version, consistent_read=consistent_read)):
            raise ValueError("No item found with the provided key.")
        return item

    def update_existing(self, existing_resource: AnyDbResource, update_obj: _PlainBaseModel | dict) -> AnyDbResource:
        data_class = existing_resource.__class__
        updated_resource = existing_resource.update_existing(update_obj)

        if issubclass(data_class, DynamodbResource):
            return self._put_nonversioned_resource(updated_resource)
        elif issubclass(data_class, DynamodbVersionedResource):
            latest_resource = self.read_existing(
                existing_id=existing_resource.resource_id,
                data_class=data_class,
            )
            if existing_resource != latest_resource:
                raise ValueError("Cannot update from non-latest version")

            self._update_existing_versioned(updated_resource, previous_version=latest_resource.version)
            return self.read_existing(
                existing_id=updated_resource.resource_id,
                data_class=data_class,
                version=updated_resource.version,
                consistent_read=True,
            )
        else:
            raise ValueError("Invalid data_class provided")

    @property
    def dynamodb_client(self) -> "DynamoDBClient":
        if not self._dynamodb_client:
            kwargs = self.connection_params or {}
            self._dynamodb_client = boto3.client("dynamodb", endpoint_url=self.endpoint_url, **kwargs)
        return self._dynamodb_client

    @property
    def dynamodb_table(self) -> "Table":
        if not self._dynamodb_table:
            kwargs = self.connection_params or {}
            dynamodb = boto3.resource("dynamodb", endpoint_url=self.endpoint_url, **kwargs)
            self._dynamodb_table = dynamodb.Table(self.table_name)
        return self._dynamodb_table

    def create_new(
        self,
        data_class: Type[AnyDbResource],
        data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> AnyDbResource:
        new_resource = data_class.create_new(data, override_id=override_id)
        if issubclass(data_class, DynamodbResource):
            return self._put_nonversioned_resource(new_resource)
        elif issubclass(data_class, DynamodbVersionedResource):
            return self._create_new_versioned(new_resource)
        else:
            raise ValueError("Invalid data_class provided")

    def _put_nonversioned_resource(self, resource: NonversionedDbResourceOnly) -> NonversionedDbResourceOnly:
        item = resource.to_dynamodb_item()
        self.dynamodb_table.put_item(Item=item)
        return resource

    def _create_new_versioned(self, resource: VersionedDbResourceOnly) -> VersionedDbResourceOnly:
        main_item = resource.to_dynamodb_item()
        v0_item = resource.to_dynamodb_item(v0_object=True)
        self.logger.debug("transact_write_items begin")
        self.dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(main_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(v0_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
            ]
        )
        self.logger.debug("transact_write_items complete")

        return self.read_existing(
            existing_id=resource.resource_id,
            data_class=resource.__class__,
            version=resource.version,
            consistent_read=True,
        )

    def _update_existing_versioned(self, resource: VersionedDbResourceOnly, previous_version: int):
        main_item = resource.to_dynamodb_item()
        v0_item = resource.to_dynamodb_item(v0_object=True)

        self.dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(main_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(v0_item),
                        "ConditionExpression": "attribute_exists(pk) and attribute_exists(sk) and #version = :version",
                        "ExpressionAttributeNames": {"#version": "version"},
                        "ExpressionAttributeValues": marshall({":version": previous_version}),
                    }
                },
            ]
        )

    def list_type_by_updated_at(
        self,
        data_class: Type[AnyDbResource],
        *,
        filter_expression=None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
    ) -> PaginatedList[AnyDbResource]:
        return self.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(data_class.__name__),
            index_name="gsitype",
            resource_class=data_class,
            filter_expression=filter_expression,
            filter_fn=filter_fn,
            results_limit=results_limit,
            max_api_calls=max_api_calls,
            pagination_key=pagination_key,
            ascending=ascending,
            filter_limit_multiplier=filter_limit_multiplier,
        )

    def increment_counter(
        self, existing_resource: NonversionedDbResourceOnly, field_name: str, incr_by: int = 1
    ) -> int:
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        field = existing_resource.model_fields.get(field_name)
        if not field:
            raise ValueError(f"Unknown field {field_name=}")
        if not field.annotation == int:
            raise TypeError(f"Field {field_name=} must be an int")
        response = self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="ADD #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": decimal.Decimal(incr_by)},
            ReturnValues="UPDATED_NEW",
        )
        return int(response["Attributes"][field_name])

    def add_to_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        field = existing_resource.model_fields.get(field_name)
        if not field:
            raise ValueError(f"Unknown field {field_name=}")
        if not (field.annotation == set[str] or field.annotation == Optional[set[str]]):
            raise TypeError(f"Field {field_name=} must be set[str]")
        self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="ADD #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": {val}},
            ReturnValues="NONE",
        )

    def remove_from_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        field = existing_resource.model_fields.get(field_name)
        if not field:
            raise ValueError(f"Unknown field {field_name=}")
        if not (field.annotation == set[str] or field.annotation == Optional[set[str]]):
            raise TypeError(f"Field {field_name=} must be set[str]")
        self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="DELETE #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": {val}},
            ReturnValues="NONE",
        )

    def paginated_dynamodb_query(
        self,
        *,
        key_condition: ConditionBase,
        resource_class: Type[AnyDbResource] = None,
        resource_class_fn: Callable[[dict], Type[AnyDbResource]] = None,
        index_name: Optional[str] = None,
        filter_expression=None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
        _current_api_calls_on_stack: int = 0,
    ) -> PaginatedList[AnyDbResource]:
        """
        Execute a paginated query against a DynamoDB table, supporting filters and optional post-retrieval filtering.

        Parameters:
            key_condition (ConditionBase): The condition used for querying the DynamoDB table.
            resource_class (Type[AnyDbResource], optional): The class type used to deserialize the DynamoDB items.
            resource_class_fn (Callable[[dict], Type[AnyDbResource]], optional): A function to determine
                the resource class type dynamically based on the DynamoDB item data.
            index_name (str, optional): The name of the secondary index to query. If not provided,
                the main table is queried.
            filter_expression (optional): DynamoDB filter expression to limit results returned.
            filter_fn (Callable[[AnyDbResource], bool], optional): A post-retrieval filter function to apply to results.
            results_limit (int, optional): The maximum number of results to return. Defaults to system default limit.
            max_api_calls (int): The maximum number of API calls to make. Defaults to QUERY_DEFAULT_MAX_API_CALLS.
            pagination_key (str, optional): Key to start pagination from, if continuing from a previous query.
            ascending (bool): If True, return results in ascending order. Default is False (descending).
            filter_limit_multiplier (int): Multiplier for results limit when using a filter. Default is 3.
            _current_api_calls_on_stack (int, internal): Tracks the number of API calls made
                during recursive operations.

        Returns:
            PaginatedList[AnyDbResource]: A paginated list of deserialized DynamoDB items.

        Raises:
            ValueError: If neither `resource_class` nor `resource_class_fn` is provided.
            RuntimeError: If an unsupported index name is encountered.

        Notes:
            - The method supports both filtering at the DynamoDB level (using `filter_expression`) and post-retrieval
              filtering using the provided `filter_fn` function. The provided filter_fn will receive a single loaded
              database item and should return True if the item should be included in the results, False if not.
            - If the initial results don't meet the `results_limit` and there are more items in DynamoDB to query,
              this method will recursively query until it either meets the desired results count, exhausts the items
              in DynamoDB, or reaches the `max_api_calls` limit.
        """
        if not (resource_class or resource_class_fn):
            raise ValueError("Must supply either resource_class or resource_class_fn")
        self.logger.info("Beginning paginated dynamodb query")
        started_at = time.time()

        if results_limit is None or results_limit < 1:
            results_limit = Constants.SYSTEM_DEFAULT_LIMIT

        query_limit = results_limit
        # if we are doing any filtering increase the number of objects evaluated, to try and limit the number of
        # api calls we need to make to hit the requested limit; sometimes this means we will pull too much data
        if filter_expression or filter_fn:
            filter_limit_multiplier = int(filter_limit_multiplier)
            if filter_limit_multiplier < 1:
                filter_limit_multiplier = 1
                self.logger.warning("filter_limit_multiplier below 1 not supported; used 1 instead")
            query_limit = min(results_limit * filter_limit_multiplier, 1000)
            self.logger.debug(f"{query_limit=}")

        # boto api requires some fields to not be present on the call at all if no values are supplied;
        # build up the call via partials

        # start with basic query function, and ensure we are getting the RCUs utilized
        query_fn = partial(self.dynamodb_table.query, ReturnConsumedCapacity="TOTAL")

        # then build up with index, pagination key, and filter expression
        if index_name:
            query_fn = partial(query_fn, IndexName=index_name)

        exclusive_start_key = None
        if pagination_key:
            try:
                exclusive_start_key = decode_pagination_key(pagination_key)
            except:  # noqa: E722
                pagination_key = None
        if exclusive_start_key:
            query_fn = partial(query_fn, ExclusiveStartKey=exclusive_start_key)

        if filter_expression:
            query_fn = partial(query_fn, FilterExpression=filter_expression)

        # execute the query and load the data
        query_result = query_fn(
            KeyConditionExpression=key_condition,
            Limit=query_limit,
            ScanIndexForward=ascending,
        )

        if resource_class_fn:
            loaded_data = (resource_class_fn(x).from_dynamodb_item(x) for x in query_result["Items"])
        else:
            loaded_data = (resource_class.from_dynamodb_item(x) for x in query_result["Items"])

        # apply any post-retrieval filtration from the supplied function
        if filter_fn:
            response_data = [x for x in loaded_data if filter_fn(x)]
        else:
            response_data = list(loaded_data)

        # figure out the pagination stuff -- do we have enough results, do we have more data to check on the server,
        #   have we hit the limit on our API calls, etc.
        lek_data = query_result.get("LastEvaluatedKey")
        current_count = len(response_data)

        _current_api_calls_on_stack += 1
        this_call_count = _current_api_calls_on_stack
        rcus_consumed_by_query = query_result["ConsumedCapacity"]["CapacityUnits"]

        if _current_api_calls_on_stack >= max_api_calls:
            self.logger.debug(
                "Reached max API calls before finding requested number of "
                "results or exhausting search; stopping early"
            )
        elif current_count < results_limit:
            # don't have enough results yet -- can we get more?

            if lek_data:
                self.logger.debug(f"Getting more data! Want {results_limit - current_count} more result(s)")
                # recursively call self with the updated limit
                extra_data = self.paginated_dynamodb_query(
                    key_condition=key_condition,
                    resource_class=resource_class,
                    index_name=index_name,
                    filter_expression=filter_expression,
                    filter_fn=filter_fn,
                    results_limit=results_limit - current_count,  # only fetch the amount we need
                    pagination_key=encode_pagination_key(lek_data),  # start from where we just left off
                    ascending=ascending,
                    max_api_calls=max_api_calls,
                    filter_limit_multiplier=filter_limit_multiplier,
                    _current_api_calls_on_stack=_current_api_calls_on_stack,
                )
                response_data += extra_data
                # replace our lek_data with the extra_data's pagination key info
                if extra_data.next_pagination_key:
                    lek_data = decode_pagination_key(extra_data.next_pagination_key)
                else:
                    lek_data = None
                _current_api_calls_on_stack = extra_data.api_calls_made
                rcus_consumed_by_query += extra_data.rcus_consumed_by_query
            else:
                self.logger.debug(f"Want {results_limit - current_count} more results, but no data available")
        elif current_count > results_limit:
            # trim and update lek_data based on the last item in our trimmed result
            response_data = response_data[:results_limit]
            if lek_data:
                self.logger.debug("Have too many results, replacing existing pagination key with new computed one")
            else:
                self.logger.debug("Have too many results, adding a pagination key where one did not exist")
            db_item = response_data[-1].to_dynamodb_item(v0_object=True)
            # hardcoded key information based on index; should figure out how to compute this
            if not index_name:
                lek_data = {"pk": db_item["pk"], "sk": db_item["sk"]}
            elif index_name == "gsitype":
                lek_data = {
                    "pk": db_item["pk"],
                    "sk": db_item["sk"],
                    "gsitype": db_item["gsitype"],
                    "gsitypesk": db_item["gsitypesk"],
                }
            elif index_name == "gsi1":
                lek_data = {
                    "pk": db_item["pk"],
                    "sk": db_item["sk"],
                    "gsi1pk": db_item["gsi1pk"],
                }
            elif index_name == "gsi2":
                lek_data = {
                    "pk": db_item["pk"],
                    "sk": db_item["sk"],
                    "gsi2pk": db_item["gsi2pk"],
                }
            elif index_name == "gsi3":
                lek_data = {
                    "pk": db_item["pk"],
                    "sk": db_item["sk"],
                    "gsi3pk": db_item["gsi3pk"],
                    "gsi3sk": db_item["gsi3sk"],
                }
            else:
                raise RuntimeError(f"Unsupported index {index_name=}")

        if lek_data:
            next_pagination_key = encode_pagination_key(lek_data)
        else:
            next_pagination_key = None

        response_data = PaginatedList(response_data)
        response_data.limit = results_limit
        response_data.current_pagination_key = pagination_key
        response_data.next_pagination_key = next_pagination_key
        response_data.api_calls_made = _current_api_calls_on_stack
        response_data.rcus_consumed_by_query = rcus_consumed_by_query
        response_data.query_time_ms = round((time.time() - started_at) * 1000, 3)

        query_took_ms = response_data.query_time_ms

        items_returned = len(response_data)

        if this_call_count > 1:
            self.logger.debug(
                f"Completed dynamodb recursive sub-query; {query_took_ms=} {this_call_count=} {items_returned=}"
            )
        else:
            api_calls_required = _current_api_calls_on_stack
            self.logger.info(
                f"Completed dynamodb query; {query_took_ms=} {items_returned=} {api_calls_required=} "
                f"{rcus_consumed_by_query=}"
            )

        return response_data
