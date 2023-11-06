import gzip
import json
from abc import ABC
from datetime import datetime, timezone
from typing import Any, Optional, Type, TypedDict, TypeVar

import ulid
from boto3.dynamodb.types import Binary
from humanize import precisedelta
from pydantic import BaseModel, Extra

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


class DynamodbResource(BaseModel, ABC):
    resource_id: str
    created_at: datetime
    updated_at: datetime

    class Config:
        extra = Extra.forbid
        compress_data = False

    # override these in resource classes to enable secondary lookups on the latest version of the resource
    def db_get_gsi1pk(self) -> str | None:
        pass

    def db_get_gsi2pk(self) -> str | None:
        pass

    def db_get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        pass

    def resource_id_as_ulid(self) -> ulid.ULID:
        return ulid.parse(self.resource_id)

    def created_ago(self, minimum_unit="minutes", now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.created_at), minimum_unit=minimum_unit) + " ago"

    def updated_ago(self, minimum_unit="minutes", now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.updated_at), minimum_unit=minimum_unit) + " ago"

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        # use the capital letters of the class name to build the prefix by default; override this method
        # to specify something different for a particular resource
        caps = [letter for letter in cls.__name__ if letter.isupper()]
        if not caps:
            raise RuntimeError(f"No capital letters detected in class name {cls.__name__}!")
        return "".join(caps)

    def to_dynamodb_item(self) -> dict:
        prefix = self.get_unique_key_prefix()
        key = f"{prefix}#{self.resource_id}"

        if self.Config.compress_data:
            dynamodb_data = {"data": self.compress_model_content()}
        else:
            dynamodb_data = clean_data(self.model_dump(exclude_none=True))

        dynamodb_data.update(
            {
                "pk": key,
                "sk": key,
                "gsitype": self.__class__.__name__,
                "gsitypesk": self.updated_at.isoformat(),
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
        cls: Type["DynamodbResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
    ) -> "DynamodbResource":
        if cls.Config.compress_data:
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
        cls: Type["DynamodbResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamodbResource":
        if isinstance(create_data, BaseModel):
            kwargs = create_data.dict()
        else:
            kwargs = {**create_data}
        now = _now()
        kwargs.update(
            {"resource_id": override_id or generate_date_sortable_id(now), "created_at": now, "updated_at": now}
        )
        return cls.parse_obj(kwargs)

    def update_existing(self: "DynamodbResource", update_data: _PlainBaseModel | dict) -> "DynamodbResource":
        now = _now()
        if isinstance(update_data, BaseModel):
            update_kwargs = update_data.dict(exclude_none=True)
        else:
            update_kwargs = {**update_data}
        kwargs = self.dict()
        kwargs.update(update_kwargs)
        kwargs.update({"resource_id": self.resource_id, "created_at": self.created_at, "updated_at": now})
        return self.__class__.parse_obj(kwargs)


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
        return precisedelta((now - self.created_at), minimum_unit="minutes") + " ago"

    def updated_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.updated_at), minimum_unit="minutes") + " ago"

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
        return gzip.compress(self.model_dump_json().encode())

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
