"""Basic implementation of resources and a manager class for defining and collecting various types data for Forms.

Form data is similar to a spreadsheet in that it allows for tracking a grid of data (columns / rows).
However, each entry into the form has its own metadata and can be of a complex type.

"""

from dataclasses import dataclass
from logging import Logger
from typing import Callable, Literal, Optional

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel, Field

from simplesingletable import AnyDbResource, DynamoDbMemory, DynamoDbVersionedResource, PaginatedList
from simplesingletable.extras.singleton import SingletonResource


class FormConfig(SingletonResource):
    categories: set[str] = Field(default_factory=set)


class FormDataEntryField(BaseModel):
    name: str
    field_type: Literal["int", "str", "float", "bool"]
    allowed_values: Optional[list]


class FormDataType(DynamoDbVersionedResource):
    """The type of data stored on a particular form."""

    name: str
    entry_schema: list[FormDataEntryField]  # Defines the schema for spreadsheet entries.


class BaseFormData(BaseModel):
    name: str
    category: str
    form_data_type_id: str
    form_data_type_version: int
    form_data_type_schema: list[FormDataEntryField]
    columns: list[str] = Field(min_items=1)
    groups: list[str] = Field(min_items=1)

    # row_identifier_key: str
    # group_identifier_key: str


class NewFormRequest(BaseFormData):
    pass


class UpdateFormRequest(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    # row_identifier_key: Optional[str] = None
    # group_identifier_key: Optional[str] = None


class Form(BaseFormData, DynamoDbVersionedResource):
    column_display_order: Optional[list[str]] = None

    def get_ordered_columns(self) -> list[str]:
        """returns columns in the correct order for display purposes."""
        return self.column_display_order or self.columns

    def db_get_gsi1pk(self) -> str | None:
        """Utilize gsi1 to track forms by category;

        This GSI automatically sorts by the pk / resource_id attribute, which sorts lexicographically by created_at.
        """
        return self.get_unique_key_prefix() + f"#{self.category}"

    @classmethod
    def list_by_category(
        cls,
        memory: DynamoDbMemory,
        category: str,
        *,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        pagination_key: Optional[str] = None,
        ascending=False,
    ) -> PaginatedList["Form"]:
        key = cls.get_unique_key_prefix() + f"#{category}"
        return memory.paginated_dynamodb_query(
            key_condition=Key("gsi1pk").eq(key),
            index_name="gsi1",
            resource_class=cls,
            filter_fn=filter_fn,
            results_limit=results_limit,
            pagination_key=pagination_key,
            ascending=ascending,
        )


class StoredFormData(BaseModel):
    col_idx: int
    row_identifier: str
    group_identifier: str
    data: dict


class FormEntry(StoredFormData, DynamoDbVersionedResource):
    form_id: str

    @staticmethod
    def generate_pk_from_form_data(existing_form: Form, data: StoredFormData) -> str:
        ef = existing_form
        d = data
        return f"{ef.resource_id}#{d.group_identifier}#{d.col_idx}#{d.row_identifier}"

    def db_get_gsi1pk(self) -> str | None:
        """Utilize gsi1 to track all Form entries for a particular form;

        This GSI automatically sorts by the pk / resource_id attribute, allowing retrieval based on the
        separations included in the pk, e.g. by a group, or a specific column of a group.
        """
        return self.get_unique_key_prefix() + f"#{self.form_id}"

    @classmethod
    def retrieve_all_form_entries_for_form(
        cls,
        memory: DynamoDbMemory,
        existing_form: Form,
        *,
        group: Optional[str] = None,
        column: Optional[str] = None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = 1000,
        max_api_calls: int = 10,
        pagination_key: Optional[str] = None,
        ascending=False,
    ) -> PaginatedList["FormEntry"]:
        if column and not group:
            raise ValueError("Cannot specify column without also specifying group")
        key = cls.get_unique_key_prefix() + f"#{existing_form.resource_id}"

        condition = Key("gsi1pk").eq(key)

        if group:
            pk_value = cls.get_unique_key_prefix() + f"#{existing_form.resource_id}#{group}"
            if column:
                pk_value += f"#{column}"
            print("search pk ", pk_value)
            condition &= Key("pk").begins_with(pk_value)
        return memory.paginated_dynamodb_query(
            key_condition=condition,
            index_name="gsi1",
            resource_class=cls,
            filter_fn=filter_fn,
            results_limit=results_limit,
            max_api_calls=max_api_calls,
            pagination_key=pagination_key,
            ascending=ascending,
        )


@dataclass
class FormDataManager:
    memory: DynamoDbMemory

    @property
    def logger(self) -> Logger:
        return self.memory.logger

    def list_form_categories(self) -> set[str]:
        config = FormConfig.ensure_exists(self.memory)
        return config.categories

    def add_form_category(self, category: str):
        config = FormConfig.ensure_exists(self.memory)
        self.memory.add_to_set(existing_resource=config, field_name="categories", val=category)

    def remove_form_category(self, category: str):
        config = FormConfig.ensure_exists(self.memory)
        if self.list_forms(category=category):
            raise ValueError("Cannot remove category that has forms assigned!")
        self.memory.remove_from_set(existing_resource=config, field_name="categories", val=category)

    def list_available_types(
        self, pagination_key: Optional[str] = None, ascending=False
    ) -> PaginatedList[FormDataType]:
        return self.memory.list_type_by_updated_at(
            FormDataType, results_limit=500, pagination_key=pagination_key, ascending=ascending
        )

    def add_new_type(self, name: str, schema: list[FormDataEntryField]) -> FormDataType:
        # use the name as the unique ID; this field is not update-able
        return self.memory.create_new(FormDataType, {"name": name, "entry_schema": schema}, override_id=name)

    def get_type(self, name: str, version: int = 0) -> FormDataType:
        return self.memory.read_existing(name, FormDataType, version=version)

    def update_schema(self, existing_type: FormDataType, new_entry_schema: list[FormDataEntryField]) -> FormDataType:
        self.logger.info("Updating schema for existing form data type")
        if existing_type.entry_schema == new_entry_schema:
            # todo: fix this check, this is not sufficient to detect unchanged values
            self.logger.info("No schema change detected, skipping update")
            return existing_type

        # update_existing ensures we are updating from the latest version of the object
        return self.memory.update_existing(existing_type, {"entry_schema": new_entry_schema})

    def list_forms(
        self,
        *,
        category: Optional[str] = None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        pagination_key: Optional[str] = None,
        ascending=False,
    ) -> PaginatedList[Form]:
        if category:
            return Form.list_by_category(
                self.memory,
                category,
                filter_fn=filter_fn,
                results_limit=results_limit,
                pagination_key=pagination_key,
                ascending=ascending,
            )
        else:
            return self.memory.list_type_by_updated_at(
                Form,
                filter_fn=filter_fn,
                results_limit=results_limit,
                pagination_key=pagination_key,
                ascending=ascending,
            )

    def get_form(self, resource_id: str, version: int = 0) -> Form:
        return self.memory.read_existing(resource_id, Form, version=version)

    def create_form(self, new_form: NewFormRequest) -> Form:
        return self.memory.create_new(Form, new_form)

    def update_form(self, existing_form: Form, update: UpdateFormRequest) -> Form:
        return self.memory.update_existing(existing_form, update)

    def store_form_data(
        self,
        existing_form: Form,
        data: (
            StoredFormData
            | tuple[FormEntry | None, StoredFormData]
            | list[StoredFormData | tuple[FormEntry | None, StoredFormData]]
        ),
    ):
        return_list = True
        if not isinstance(data, list):
            return_list = False
            data = [data]
        for idx, item in enumerate(data):
            if isinstance(item, StoredFormData):
                data[idx] = (None, item)

        new_entries = [form_data for entry, form_data in data if not entry]
        existing_entries = [(entry, form_data) for entry, form_data in data if entry]
        if new_entries:
            output = self._store_new_form_data(existing_form, new_entries)
            if not isinstance(output, list):
                output = [output]
        else:
            output = []
        if existing_entries:
            output.extend(self._update_existing_form_data(existing_form, existing_entries))
        if return_list:
            return output
        else:
            return output[0]

    def _store_new_form_data(
        self, existing_form: Form, data: StoredFormData | list[StoredFormData]
    ) -> FormEntry | list[FormEntry]:
        return_list = True
        if not isinstance(data, list):
            return_list = False
            data = [data]
        new_entries = []

        for entry in data:
            create_data = entry.model_dump()
            create_data["form_id"] = existing_form.resource_id
            new_entries.append(
                self.memory.create_new(
                    FormEntry, create_data, override_id=FormEntry.generate_pk_from_form_data(existing_form, entry)
                )
            )

        if return_list:
            return new_entries
        else:
            return new_entries[0]

    def _update_existing_form_data(
        self, existing_form: Form, updates: list[tuple[FormEntry, StoredFormData]]
    ) -> list[FormEntry]:
        return []
