import gzip
import json
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, ClassVar, Optional, Type, TypedDict, TypeVar

import ulid
from boto3.dynamodb.types import Binary
from humanize import naturalsize, precisedelta
from pydantic import BaseModel, ConfigDict

from .utils import generate_date_sortable_id

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


class ResourceConfig(TypedDict, total=False):
    """A TypedDict for configuring Resource behaviour."""

    compress_data: bool | None
    """Should the resource content be compress (gzip)."""


class BaseDynamoDbResource(BaseModel, ABC):
    """Exists only to provide a common parent for the resource classes."""

    @abstractmethod
    def get_db_resource_base_keys(self) -> set[str]:
        """Returns a set of the string values corresponding to all of attributes on the Base resource object.

        For example, this will return something like {"resource_id", "created_at", "updated_at"} along with others,
        depending on which resource class is being used (for example a versioned resource will have a "version"
        attribute included.

        This can be useful for filtering out all the base attributes, e.g. when calling pydantic's model_dump.
        """

    # override these in resource classes to enable secondary lookups on the latest version of the resource
    def db_get_gsi1pk(self) -> str | None:
        pass

    def db_get_gsi2pk(self) -> str | None:
        pass

    def db_get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        pass

    def db_get_filter_metadata(self) -> tuple[str, str] | None:
        pass

    def db_get_gsitypesk(self) -> str:
        return self.updated_at.isoformat()

    def resource_id_as_ulid(self) -> ulid.ULID:
        return ulid.parse(self.resource_id)

    def created_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.created_at), minimum_unit="minutes") + " ago"

    def updated_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.updated_at), minimum_unit="minutes") + " ago"

    def get_db_item_size_in_bytes(self) -> int:
        """Return the size of the database item, in bytes."""
        return sys.getsizeof(json.dumps(self.to_dynamodb_item(), default=str))

    def get_db_item_size(self) -> str:
        return naturalsize(self.get_db_item_size_in_bytes())

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return cls.__name__

    def compress_model_content(self) -> bytes:
        """Helper that can be used in to_dynamodb_item."""
        return gzip.compress(self.model_dump_json().encode())

    @staticmethod
    def decompress_model_content(content: bytes | Binary) -> dict:
        if isinstance(content, Binary):
            content = bytes(content)  # noqa
        entry_data: str = gzip.decompress(content).decode()
        return json.loads(entry_data)


class DynamoDbResource(BaseDynamoDbResource, ABC):
    resource_id: str
    created_at: datetime
    updated_at: datetime

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    def get_db_resource_base_keys(self) -> set[str]:
        return {"resource_id", "created_at", "updated_at"}

    def to_dynamodb_item(self) -> dict:
        prefix = self.get_unique_key_prefix()
        key = f"{prefix}#{self.resource_id}"

        if self.resource_config["compress_data"]:
            dynamodb_data = {"data": self.compress_model_content()}
        else:
            dynamodb_data = clean_data(self.model_dump(exclude_none=True))

        dynamodb_data.update(
            {
                "pk": key,
                "sk": key,
                "gsitype": self.__class__.__name__,
                "gsitypesk": self.db_get_gsitypesk(),
            }
        )

        # check for the user-defineable key / filter fields
        if gsi1pk := self.db_get_gsi1pk():
            dynamodb_data["gsi1pk"] = gsi1pk
        if gsi2pk := self.db_get_gsi2pk():
            dynamodb_data["gsi2pk"] = gsi2pk
        if data := self.db_get_gsi3pk_and_sk():
            gsi3pk, gsi3sk = data
            dynamodb_data["gsi3pk"] = gsi3pk
            dynamodb_data["gsi3sk"] = gsi3sk

        return dynamodb_data

    @classmethod
    def from_dynamodb_item(
        cls: Type["DynamoDbResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
    ) -> "DynamoDbResource":
        if cls.resource_config["compress_data"]:
            compressed_data = dynamodb_data["data"]
            data = cls.decompress_model_content(compressed_data)  # noqa
        else:
            data = {
                k: v
                for k, v in dynamodb_data.items()
                if k not in {"pk", "sk", "gsitypesk", "gsitype", "gsi1pk", "gsi2pk", "gsi3pk", "gsi3sk"}
            }
        return cls.parse_obj(data)

    @classmethod
    def dynamodb_lookup_keys_from_id(cls, existing_id: str) -> dict:
        key = f"{cls.get_unique_key_prefix()}#{existing_id}"
        return {"pk": key, "sk": key}

    @classmethod
    def create_new(
        cls: Type["DynamoDbResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamoDbResource":
        if isinstance(create_data, BaseModel):
            kwargs = create_data.dict()
        else:
            kwargs = {**create_data}
        now = _now()
        kwargs.update(
            {"resource_id": override_id or generate_date_sortable_id(now), "created_at": now, "updated_at": now}
        )
        return cls.parse_obj(kwargs)

    def update_existing(self: "DynamoDbResource", update_data: _PlainBaseModel | dict) -> "DynamoDbResource":
        now = _now()
        if isinstance(update_data, BaseModel):
            update_kwargs = update_data.dict(exclude_none=True)
        else:
            update_kwargs = {**update_data}
        kwargs = self.dict()
        kwargs.update(update_kwargs)
        kwargs.update({"resource_id": self.resource_id, "created_at": self.created_at, "updated_at": now})
        return self.__class__.parse_obj(kwargs)


# for backwards compatibility
DynamodbResource = DynamoDbResource


class DynamoDbVersionedResource(BaseDynamoDbResource, ABC):
    resource_id: str
    version: int
    created_at: datetime
    updated_at: datetime

    def get_db_resource_base_keys(self) -> set[str]:
        return {"resource_id", "version", "created_at", "updated_at"}

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

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
            dynamodb_data["gsitypesk"] = self.db_get_gsitypesk()

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
        cls: Type["DynamoDbVersionedResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
    ) -> "DynamoDbVersionedResource":
        compressed_data = dynamodb_data["data"]
        data = cls.decompress_model_content(compressed_data)  # noqa
        return cls.parse_obj(data)

    @classmethod
    def dynamodb_lookup_keys_from_id(cls, existing_id: str, version: int = 0) -> dict:
        return {
            "pk": f"{cls.get_unique_key_prefix()}#{existing_id}",
            "sk": f"v{version}",
        }

    @classmethod
    def create_new(
        cls: Type["DynamoDbVersionedResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamoDbVersionedResource":
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
        self: "DynamoDbVersionedResource", update_data: _PlainBaseModel | dict
    ) -> "DynamoDbVersionedResource":
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


DynamodbVersionedResource = DynamoDbVersionedResource


def _now(tz: Any = False):
    # this function exists only to make it easy to mock the utcnow call in date_id when creating resources in the tests

    # explicitly check for False, so that `None` is a valid option to provide for the tz
    if tz is False:
        tz = timezone.utc
    return datetime.now(tz=tz)


def clean_data(data: dict):
    data = {**data}
    del_keys = set()
    for key, value in data.items():
        if isinstance(value, datetime):
            # convert datetimes to isoformat -- dynamodb has no native datetime
            data[key] = value.isoformat()
        elif isinstance(value, set) and not value:
            # clear out empty sets entirely from the data
            del_keys.add(key)
        elif isinstance(value, dict):
            # run recursively on dicts
            data[key] = clean_data(value)

    for key in del_keys:
        data.pop(key)

    return data
