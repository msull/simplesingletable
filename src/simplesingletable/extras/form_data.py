"""Basic implementation of resources and a manager class for defining and collecting various types data for Forms.

Form data is similar to a spreadsheet in that it allows for tracking a grid of data (columns / rows).
However, each entry into the form has its own metadata, is independently versioned, and can be of a complex type.

Each form supports multiple "groups" (similar to a spreadsheet "tab") -- all "groups" have the same columns.

"""

from dataclasses import dataclass
from logging import Logger
from typing import Callable, Literal, Mapping, Optional

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel, Field

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource, PaginatedList
from simplesingletable.dynamodb_memory import AnyDbResource
from simplesingletable.extras.singleton import SingletonResource


class FormConfig(SingletonResource):
    """Global form configration singleton."""

    categories: set[str] = Field(default_factory=set)


class FormDataEntryField(BaseModel):
    """Definition of a field within a form schema."""

    name: str
    field_type: Literal["int", "str", "float", "bool"]
    allowed_values: Optional[list]


class FormDataType(DynamoDbVersionedResource):
    """The type of data stored on a particular form."""

    name: str
    entry_schema: list[FormDataEntryField]  # Defines the schema for spreadsheet entries.


class BaseFormData(BaseModel):
    """Data being stored about a Form"""

    name: str
    category: str
    form_data_type_id: str
    form_data_type_version: int
    form_data_type_schema: list[FormDataEntryField]
    columns: list[str] = Field(min_items=1)
    groups: list[str] = Field(min_items=1)
    hide_columns_by_group: dict[str, list[int]] = Field(
        default_factory=dict,
        description=(
            "Keys are group identifiers; values are a list of Column indexes for "
            "columns that should be hidden from display for that particular group."
        ),
    )
    user_metadata: dict = Field(default_factory=dict)
    deleted_columns: list[int] = Field(default_factory=list, description="List of column indexes that are deleted.")


class NewFormRequest(BaseFormData):
    pass


class UpdateFormRequest(BaseModel):
    """Things that can be updated on a form directly."""

    name: Optional[str] = None
    category: Optional[str] = None
    user_metadata: Optional[dict] = None


class Form(BaseFormData, DynamoDbVersionedResource):
    """DB Representation of a Form.

    Includes additional handling to allow for columns to be displayed in a different order than they are created;
    however because of the way the form data is stored in DynamoDB we need to leave the columns in their original
    order in the main `columns` attribute.
    """

    column_display_order: Optional[list[str]] = None

    def get_ordered_columns(self, group: Optional[str] = None) -> list[str]:
        """Returns columns in the correct order for display purposes, taking into account deleted columns and
        (optionally) columns hidden for a group."""
        if self.column_display_order:
            return_columns = self.column_display_order
        else:
            return_columns = [column for idx, column in enumerate(self.columns) if idx not in self.deleted_columns]

        if group:
            if group not in self.groups:
                raise ValueError("Bad group")
            col_indexes_to_hide: list[int] = self.hide_columns_by_group.get(group) or []

            # build the filtered listing based on indexes from the original columns
            filtered_col_list = [column for idx, column in enumerate(self.columns) if idx not in col_indexes_to_hide]

            # now filter the final display listing
            return_columns = [x for x in return_columns if x in filtered_col_list]

        return return_columns

    def delete_column(self, column_name: str):
        """Mark a column as deleted by its name."""
        if column_name not in self.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")
        idx = self.columns.index(column_name)
        if idx in self.deleted_columns:
            # Already deleted
            return
        self.deleted_columns.append(idx)

        # If it's in the display order, remove it from there as well
        if self.column_display_order and column_name in self.column_display_order:
            self.column_display_order.remove(column_name)

    def restore_column(self, column_name: str):
        """Restore a previously deleted column."""
        if column_name not in self.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")
        idx = self.columns.index(column_name)
        if idx not in self.deleted_columns:
            # Not deleted, nothing to restore
            return
        self.deleted_columns.remove(idx)

        # Optionally, restore it to the column_display_order
        # In this case, we append it to the end of the display order if the order is set.
        if self.column_display_order is not None and column_name not in self.column_display_order:
            self.column_display_order.append(column_name)

    def get_deleted_columns(self) -> list[str]:
        """Return a list of deleted column names."""
        return [self.columns[i] for i in self.deleted_columns]

    def db_get_gsi1pk(self) -> str | None:
        """Utilize gsi1 to track forms by category;

        This GSI automatically sorts by the pk / resource_id attribute, which sorts lexicographically by created_at.
        """
        return self.get_unique_key_prefix() + f"#{self.category}"

    @property
    def summary_column(self) -> str:
        """Returns the name of the first field in the schema, which is the summary field."""
        return self.form_data_type_schema[0].name

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
    """Data that is being stored for each "cell" of the form."""

    col_idx: int  # the numerical index of the column in the original column order of the Form
    row_identifier: str
    group_identifier: str
    data: dict  # this data is in the schema of the Form `form_data_type_schema`


class FormEntry(StoredFormData, DynamoDbVersionedResource):
    """The DB representation of the StoredFormData."""

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

    def db_get_gsi2pk(self) -> str | None:
        """Utilize gsi2 to track all Form entries for a particular group / row identifier, allowing efficient retrieval
        of a specific row's worth of data."""
        # row then group, to allow looking up across the resource_id across all groups as well as looking up by row_id
        # without knowing the group
        return f"{self.get_unique_key_prefix()}#{self.form_id}#{self.row_identifier}#{self.group_identifier}"

    @classmethod
    def retrieve_all_entries_for_row(
        cls,
        memory: DynamoDbMemory,
        existing_form: Form,
        *,
        row_identifier: str,
        group_identifier: Optional[str] = None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = 1000,
        max_api_calls: int = 10,
        pagination_key: Optional[str] = None,
        ascending=False,
    ) -> PaginatedList["FormEntry"]:
        if group_identifier:
            key = f"{cls.get_unique_key_prefix()}#{existing_form.resource_id}#{row_identifier}#{group_identifier}"
            condition = Key("gsi2pk").eq(key)
        else:
            key = f"{cls.get_unique_key_prefix()}#{existing_form.resource_id}#{row_identifier}#"
            condition = Key("gsi2pk").begins_with(key)

        return memory.paginated_dynamodb_query(
            key_condition=condition,
            index_name="gsi2",
            resource_class=cls,
            filter_fn=filter_fn,
            results_limit=results_limit,
            max_api_calls=max_api_calls,
            pagination_key=pagination_key,
            ascending=ascending,
        )


class FormDataRow(Mapping):
    def __init__(
        self,
        form: Form,
        form_manager: "FormDataManager",
        group: str,
        row_identifier: str,
        column_data: Mapping[int, FormEntry] | None,
    ):
        self.form_manager = form_manager
        self.form = form
        self.group = group
        self.row_identifier = row_identifier
        self.column_data = column_data or {}

    def get_item_by_key(self, key: str | int, *, ignore_hidden_columns=False) -> FormEntry | None:
        """Key can be provided as either the name of a column or as the numerical
        0-based index of the column from the current display order."""

        ordered_columns = (
            self.form.get_ordered_columns() if ignore_hidden_columns else self.form.get_ordered_columns(self.group)
        )
        match key:
            case str():
                if key not in ordered_columns:
                    raise KeyError(key)
                data_index = self.form.columns.index(key)
            case int():
                if key < 0 or key > (len(ordered_columns) - 1):
                    raise KeyError(key)
                # convert from index into ordered columns to index on data columns
                data_index = self.form.columns.index(ordered_columns[key])
            case _:
                raise ValueError()
        return self.column_data.get(data_index)

    def __getitem__(self, key: str | int) -> FormEntry | None:
        return self.get_item_by_key(key)

    def __iter__(self):
        return iter(self.form.get_ordered_columns(self.group))

    def __len__(self):
        return len(self.form.get_ordered_columns(self.group))

    def __repr__(self):
        form = self.form.name
        group = self.group
        row_id = self.row_identifier
        return f'{self.__class__.__name__}({form=}, {group=}, {row_id=})")'


class FormDataMapping(Mapping):
    """Access the data in a Form using a Mapping interface.

    Works with a single Form group at a time.
    """

    def __init__(
        self,
        form: Form,
        form_manager: "FormDataManager",
        active_group: str = "",
        max_results=10000,
        preload=False,
        logger=None,
    ):
        self.form_manager = form_manager
        self.form = form
        self.max_results = max_results
        self._data = None

        # use the specified group, or the first one
        self.active_group = ""
        self.switch_active_group(active_group or self.form.groups[0])

        if logger is None:
            try:
                from logzero import logger
            except ImportError:
                import logging

                logging.basicConfig(level=logging.DEBUG)
                logger = logging.getLogger(__file__)

        self.logger = logger

        if preload:
            self.load_data()

    def load_data(self, reload=False):
        """Load or reload the data for the active_group."""
        if self._data is not None and not reload:
            return
        self.logger.debug(f"Loading data for {self.active_group}")
        form_entries = FormEntry.retrieve_all_form_entries_for_form(
            memory=self.form_manager.memory,
            existing_form=self.form_manager.get_form(self.form.resource_id),
            group=self.active_group,
            results_limit=self.max_results,
        )
        self.logger.debug("Raw Entries received from DB, converting")
        values_by_row_id = {}
        for entry in form_entries:
            rid = entry.row_identifier
            if rid not in values_by_row_id:
                values_by_row_id[rid] = {}
            values_by_row_id[rid][entry.col_idx] = entry

        # values_by_row_id = {
        #     x.row_identifier: {y.col_idx: y for y in form_entries if y.row_identifier == x.row_identifier}
        #     for x in form_entries
        # }
        self.logger.debug("Entries converted to nested dicts")

        self._data = values_by_row_id

    def switch_active_group(self, group: str):
        assert group in self.form.groups
        self.active_group = group
        self._data = None

    def to_list(
        self,
        summary_data=True,
        extra_data_by_rowid: Optional[dict[str, dict | None] | Callable[[str], dict | None]] = None,
        row_identifier_label="row_identifier",
        group_identifier_label="group_identifier",
    ) -> list[dict]:
        # default to an empty dict if we have nothing
        if row_identifier_label in self.form.columns:
            raise ValueError("Cannot use a row_identifier_label that matches a column!")
        if group_identifier_label in self.form.columns:
            raise ValueError("Cannot use a group_identifier_label that matches a column!")
        extra_data_by_rowid = extra_data_by_rowid or {}

        # extra_data can be a dict or a callable -- both should take the row id, and return a dict or None
        # if we have a dictionary, then use dict.get; otherwise we have a Callable, so use it directly

        _get_extra = extra_data_by_rowid.get if isinstance(extra_data_by_rowid, dict) else extra_data_by_rowid

        summary_column = self.form.summary_column

        flat_data = []
        for row_id in sorted(self):
            row_data = {row_identifier_label: row_id, group_identifier_label: self.active_group}

            # include additional columns the user provided, if any
            # this is loaded first, so if there are any duplications
            # with column names, the extra data will be overwritten
            if extra_data := _get_extra(row_id):
                if row_identifier_label in extra_data:
                    raise ValueError("Cannot include key matching the `row_identifier_label` in extra data")
                if group_identifier_label in extra_data:
                    raise ValueError("Cannot include key matching the `group_identifier_label` in extra data")
                row_data.update(extra_data)

            for column in self[row_id]:
                if col_data := self[row_id][column]:
                    if summary_data:
                        row_data[column] = col_data.data.get(summary_column)
                    else:
                        row_data[column] = col_data.data
                else:
                    row_data[column] = None

            flat_data.append(row_data)
        return flat_data

    def __getitem__(self, key) -> FormDataRow:
        # Retrieve an row given its identifier
        self.load_data()
        item_data: Mapping[int, FormEntry] = self._data.get(key)
        return FormDataRow(
            form=self.form,
            form_manager=self.form_manager,
            group=self.active_group,
            row_identifier=key,
            column_data=item_data,
        )

    def __iter__(self):
        # Return an iterator over the keys of the mapping
        self.load_data()
        return iter(self._data)

    def __len__(self):
        # Return the number of items in the mapping
        self.load_data()
        return len(self._data)

    # Optional: Implement this method to provide a meaningful representation
    def __repr__(self):
        form = self.form.name
        group = self.active_group
        return f'{self.__class__.__name__}({form=}, {group=})")'


@dataclass
class FormDataManager:
    """This class handles all the database interactions.

    This is the class one generally instantiates and works with to access form data."""

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

    def update_form_column_display_order(self, existing_form: Form, new_display_order: list[str]) -> Form:
        assert set(existing_form.get_ordered_columns()) == set(new_display_order)
        return self.memory.update_existing(existing_form, {"column_display_order": new_display_order})

    def delete_column_from_form(self, existing_form: Form, column_name: str) -> Form:
        """Delete a column from a form and persist the change."""
        updated = existing_form.model_copy(deep=True)
        updated.delete_column(column_name)
        return self.memory.update_existing(
            existing_form,
            {
                "deleted_columns": updated.deleted_columns,
                "column_display_order": updated.column_display_order,
            },
        )

    def restore_column_to_form(self, existing_form: Form, column_name: str) -> Form:
        """Restore a previously deleted column to a form and persist the change."""
        updated = existing_form.model_copy(deep=True)
        updated.restore_column(column_name)
        return self.memory.update_existing(
            existing_form,
            {
                "deleted_columns": updated.deleted_columns,
                "column_display_order": updated.column_display_order,
            },
        )

    def get_mapping(self, existing_form: Form):
        return FormDataMapping(form=existing_form, form_manager=self, preload=False, logger=self.logger)

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

        num = len(data)
        s = "" if num == 1 else "s"
        self.logger.info(f"Storing {num} cell{s} on FORM:{existing_form.resource_id}")

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

        num = len(data)
        s = "" if num == 1 else "s"
        self.logger.debug(f"Creating {num} new cell{s} on FORM:{existing_form.resource_id}")

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
        num = len(updates)
        s = "" if num == 1 else "s"
        self.logger.debug(f"Updating {num} existing cell{s} on FORM:{existing_form.resource_id}")
        updated = []
        for existing_entry, update in updates:
            update_data = update.model_dump()
            updated.append(self.memory.update_existing(existing_entry, update_data))
        return updated
