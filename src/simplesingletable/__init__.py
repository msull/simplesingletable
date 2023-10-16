import gzip
import json
import time
from abc import ABC
from base64 import urlsafe_b64decode, urlsafe_b64encode
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Optional, Type, TypedDict, TypeVar

import boto3
import ulid
from boto3.dynamodb.conditions import ConditionBase, Key
from boto3.dynamodb.types import Binary, TypeSerializer
from humanize import precisedelta
from pydantic import BaseModel, Extra

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table


class Constants:
    SYSTEM_DEFAULT_LIMIT = 250
    QUERY_DEFAULT_MAX_API_CALLS = 10


version = "1.0.0"


_T = TypeVar("_T")


class PaginatedList(list[_T]):
    limit: int
    current_pagination_key: Optional[str] = None
    next_pagination_key: Optional[str] = None
    api_calls_made: int = 0
    rcus_consumed_by_query: int = 0
    query_time_ms: Optional[float] = None

    def as_list(self) -> list[_T]:
        return self


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


_PlainBaseModel = TypeVar("_PlainBaseModel", bound=BaseModel)


class DynamoDbVersionedItemKeys(TypedDict):
    """The specific attributes on the dynamodb items we store"""

    pk: str
    sk: str
    version: int
    data: dict

    # keys for the gsitype index that is automatically applied sparsely on v0 objects
    # the sk value is the "updated_at" datetime value on the object, meaning the gsitype index
    # sorts by modified time of the objects for any particular type
    gsitype: Optional[str]
    gsitypesk: Optional[str]

    # user-defineable attributes, used sparsely on the v0 object to enable secondary lookups / access patterns
    # gsi1 and gsi2 use the pk as the range key; using the default ID generation system, this means it automatically
    # sorts by the creation time of the resources
    # use this for access patterns like "all resources associated with a parent object"
    # and set the pk value to "parent_id#<actual id value>"
    # or use it for categories, or tracking COMPLETE/INCOMPLETE by setting static string values
    # e.g. if you were managing a "Task" resource, you might want to easily be able to find all complete/incomplete
    # tasks and set `gsi1pk` to "t|COMPLETE" or "t|INCOMPLETE" based on the "completed" attribute of the Task

    # gsi3 has a separate sortkey the user defines, to enable lookups that sort by something other than created_at
    gsi1pk: Optional[str]
    gsi2pk: Optional[str]
    gsi3pk: Optional[str]
    gsi3sk: Optional[str]
    metadata: Optional[dict]  # user supplied metadata for anything that needs to be accessible to dynamodb filter expr


class DynamodbVersionedResource(BaseModel, ABC):
    resource_id: str
    version: int
    created_at: datetime
    updated_at: datetime

    class Config:
        extra = Extra.forbid

    # override these in resource classes to enable secondary lookups on the latest version of the resource
    def db_get_gsi1pk(self) -> str | None:
        pass

    def db_get_gsi2pk(self) -> str | None:
        pass

    def db_get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        pass

    def db_get_filter_metadata(self) -> tuple[str, str] | None:
        pass

    def resource_id_as_ulid(self) -> ulid.ULID:
        return ulid.parse(self.resource_id)

    def created_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.created_at), minimum_unit="minutes", format="") + " ago"

    def updated_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.updated_at), minimum_unit="minutes", format="") + " ago"

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        # use the capital letters of the class name to build the prefix by default; override this method
        # to specify something different for a particular resource
        caps = [letter for letter in cls.__name__ if letter.isupper()]
        if not caps:
            raise RuntimeError(f"No capital letters detected in class name {cls.__name__}!")
        return "".join(caps)

    def to_dynamodb_item(self, v0_object: bool = False) -> dict:
        prefix = self.get_unique_key_prefix()
        dynamodb_data = {
            "pk": f"{prefix}#{self.resource_id}",
            "version": self.version,
            "data": self.compress_model_content(),
        }
        if v0_object:
            sk = "v0"
        else:
            sk = f"v{self.version}"
        dynamodb_data["sk"] = sk

        if v0_object:
            # all v0 objects get gsitype applied to enable "get all <type> sorted by last updated"
            dynamodb_data["gsitype"] = self.__class__.__name__
            dynamodb_data["gsitypesk"] = self.updated_at.isoformat()

            # check for the user-defineable key / filter fields
            if gsi1pk := self.db_get_gsi1pk():
                dynamodb_data["gsi1pk"] = gsi1pk
            if gsi2pk := self.db_get_gsi2pk():
                dynamodb_data["gsi2pk"] = gsi2pk
            if filter_metadata := self.db_get_filter_metadata():
                dynamodb_data["metadata"] = filter_metadata
            if data := self.db_get_gsi3pk_and_sk():
                gsi3pk, gsi3sk = data
                dynamodb_data["gsi3pk"] = gsi3pk
                dynamodb_data["gsi3sk"] = gsi3sk

        return dynamodb_data

    @classmethod
    def from_dynamodb_item(
        cls: Type["DynamodbVersionedResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
    ) -> "DynamodbVersionedResource":
        compressed_data = dynamodb_data["data"]
        data = cls.decompress_model_content(compressed_data)  # noqa
        return cls.parse_obj(data)

    @classmethod
    def dynamodb_lookup_keys_from_id(cls, existing_id: str, version: int = 0) -> dict:
        return {
            "pk": f"{cls.get_unique_key_prefix()}#{existing_id}",
            "sk": f"v{version}",
        }

    def compress_model_content(self) -> bytes:
        """Helper that can be used in to_dynamodb_item."""
        return gzip.compress(self.json().encode())

    @staticmethod
    def decompress_model_content(content: bytes | Binary) -> dict:
        if isinstance(content, Binary):
            content = bytes(content)  # noqa
        entry_data: str = gzip.decompress(content).decode()
        return json.loads(entry_data)

    @classmethod
    def create_new(
        cls: Type["DynamodbVersionedResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamodbVersionedResource":
        if isinstance(create_data, BaseModel):
            kwargs = create_data.dict()
        else:
            kwargs = {**create_data}
        now = _now()
        kwargs.update(
            {
                "version": 1,
                "resource_id": override_id or generate_date_sortable_id(now),
                "created_at": now,
                "updated_at": now,
            }
        )
        return cls.parse_obj(kwargs)

    def update_existing(
        self: "DynamodbVersionedResource", update_data: _PlainBaseModel | dict
    ) -> "DynamodbVersionedResource":
        now = _now()
        if isinstance(update_data, BaseModel):
            update_kwargs = update_data.dict(exclude_none=True)
        else:
            update_kwargs = {**update_data}
        kwargs = self.dict()
        kwargs.update(update_kwargs)
        kwargs.update(
            {
                "version": self.version + 1,
                "resource_id": self.resource_id,
                "created_at": self.created_at,
                "updated_at": now,
            }
        )
        return self.__class__.parse_obj(kwargs)


DbResource = TypeVar("DbResource", bound=DynamodbVersionedResource)


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

    def list_resources_of_type(
        self,
        data_class: Type[DbResource],
        num: int = 10,
        ascending: bool = False,
        filter_expression=None,
        filter_fn: Optional[Callable[[DbResource], bool]] = None,
        pagination_key: Optional[str] = None,
    ):
        return self.paginated_dynamodb_query(
            resource_class=data_class,
            index_name="gsitype",
            key_condition=Key("gsitype").eq(data_class.__name__),
            ascending=ascending,
            results_limit=num,
            filter_expression=filter_expression,
            filter_fn=filter_fn,
            pagination_key=pagination_key,
        )

    def read_existing(
        self,
        existing_id: str,
        data_class: Type[DbResource],
        version: int = 0,
        consistent_read=False,
    ) -> DbResource:
        if not (item := self.get_existing(existing_id, data_class, version, consistent_read=consistent_read)):
            raise ValueError("No item found with the provided key.")
        return item

    def update_existing(self, existing_resource: DbResource, update_obj: _PlainBaseModel | dict) -> DbResource:
        latest_resource = self.read_existing(
            existing_id=existing_resource.resource_id,
            data_class=existing_resource.__class__,
        )
        if existing_resource != latest_resource:
            raise ValueError("Cannot update from non-latest version")

        updated_resource = existing_resource.update_existing(update_obj)
        self._update_existing_versioned(updated_resource, previous_version=latest_resource.version)
        return self.read_existing(
            existing_id=updated_resource.resource_id,
            data_class=updated_resource.__class__,
            version=updated_resource.version,
        )

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
        data_class: Type[DbResource],
        data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> DbResource:
        new_resource = data_class.create_new(data, override_id=override_id)
        main_item = new_resource.to_dynamodb_item()
        v0_item = new_resource.to_dynamodb_item(v0_object=True)
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
            existing_id=new_resource.resource_id,
            data_class=new_resource.__class__,
            version=new_resource.version,
            consistent_read=True,
        )

    def _update_existing_versioned(self, resource: DbResource, previous_version: int):
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

    def get_existing(
        self,
        existing_id: str,
        data_class: Type[DbResource],
        version: int = 0,
        consistent_read=False,
    ) -> Optional[DbResource]:
        key = data_class.dynamodb_lookup_keys_from_id(existing_id, version)
        response = self.dynamodb_table.get_item(Key=key, ConsistentRead=consistent_read)
        item = response.get("Item")
        if item:
            return data_class.from_dynamodb_item(item)

    def list_type_by_updated_at(
        self,
        data_class: Type[DbResource],
        *,
        filter_expression=None,
        filter_fn: Optional[Callable[[DbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
    ) -> PaginatedList[DbResource]:
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

    def paginated_dynamodb_query(
        self,
        *,
        key_condition: ConditionBase,
        resource_class: Type[DbResource] = None,
        resource_class_fn: Callable[[dict], Type[DbResource]] = None,
        index_name: Optional[str] = None,
        filter_expression=None,
        filter_fn: Optional[Callable[[DbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
        _current_api_calls_on_stack: int = 0,
    ) -> PaginatedList[DbResource]:
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
            if lek_data:
                self.logger.debug(
                    "Reached max API calls before finding requested number of "
                    "results or exhausting search; stopping early"
                )
                lek_data = None

        if current_count < results_limit:
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
            # todo: track this v0_object -- do I want this to be true? Always??? How can I better handle this
            db_item = response_data[-1].to_dynamodb_item(v0_object=True)
            # hardcoded key information based on index; should figure out how to compute this
            # note: gsirev not currently deployed
            if not index_name or index_name == "gsirev":
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

        self.logger.info(
            f"Completed dynamodb query; {query_took_ms=} {items_returned=} {this_call_count=} {rcus_consumed_by_query=}"
        )

        return response_data
