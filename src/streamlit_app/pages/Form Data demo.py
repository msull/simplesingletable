import random
from typing import Any, Callable, Optional, TypeVar
from uuid import uuid4

import pandas as pd
import streamlit as st
from logzero import logger
from pydantic import BaseModel, TypeAdapter

from simplesingletable import DynamoDbMemory
from simplesingletable.extras.form_data import (
    Form,
    FormDataEntryField,
    FormDataManager,
    FormDataType,
    FormEntry,
    NewFormRequest,
    StoredFormData,
)


def main():
    memory = DynamoDbMemory(
        logger=logger,
        table_name="standardexample",
        endpoint_url="http://localhost:8000",
        connection_params={
            "aws_access_key_id": "unused",
            "aws_secret_access_key": "unused",
            "region_name": "us-east-1",
        },
    )
    fdm = FormDataManager(memory=memory)

    if lf := st.query_params.get("lf"):
        render_single_form(fdm, lf)
    else:
        render_form_management(fdm)


def render_single_form(fdm: FormDataManager, form_id):
    def _close():
        st.query_params.pop("lf")

    dbform = fdm.get_form(form_id)

    st.button("Close Form", use_container_width=True, type="primary", on_click=_close)
    if st.toggle("Add test data"):

        def _add_test_data(number_to_add: int):
            for x in range(number_to_add):
                group = random.choice(dbform.groups)
                row_id = uuid4().hex
                for col_idx in range(len(dbform.columns)):
                    field_data = {"completed": bool(random.randint(0, 1)), "note": f"NOTE {uuid4()}"}
                    fdm.store_form_data(
                        dbform,
                        StoredFormData(col_idx=col_idx, row_identifier=row_id, group_identifier=group, data=field_data),
                    )

        num_test = st.number_input("Num Test to add", value=100)
        st.button("Add test data", use_container_width=True, type="primary", on_click=_add_test_data, args=(num_test,))

    if st.toggle("Full object"):
        st.code(dbform.model_dump_json(indent=2))
    filter_group = st.radio("Group", dbform.groups + ["all"], horizontal=True)
    if filter_group == "all":
        filter_group = None

    # todo: extract this data manipulation into FDM class
    all_entries = FormEntry.retrieve_all_form_entries_for_form(
        fdm.memory, dbform, group=filter_group, results_limit=50000, max_api_calls=20
    )
    # build the final dict, which is a dict of dicts :
    # {group_identifier: {row_identifier: {col1: val1, col2:val2}}}
    base_cols_dict = {x: None for x in dbform.get_ordered_columns()}
    if filter_group:
        all_form_data = {filter_group: {}}
    else:
        all_form_data = {x: {} for x in dbform.groups}
    for form_data_entry in all_entries:
        group = form_data_entry.group_identifier
        row = form_data_entry.row_identifier
        column = dbform.columns[form_data_entry.col_idx]
        value = form_data_entry.data
        if row not in all_form_data[group]:
            all_form_data[group][row] = base_cols_dict.copy()
        all_form_data[group][row][column] = value

    # df = pd.DataFrame(
    #     [x.model_dump(mode="json", include={"col_idx", "group_identifier", "data"}) for x in all_entries],
    #     index=[x.row_identifier for x in all_entries],
    # )

    schema_field = dbform.form_data_type_schema[0]

    for group, group_data in all_form_data.items():
        c1, c2 = st.columns((2, 1))
        with c1:
            st.header("Group " + group)
        with c2:
            st.metric("Num entries", len(group_data))
        index = []
        col_data = {x: [] for x in dbform.get_ordered_columns()}
        for date, entry in sorted(group_data.items()):
            index.append(date)
            entry: FormEntry
            for col_name in dbform.get_ordered_columns():
                if col_value := entry.get(col_name):
                    col_data[col_name].append(col_value.get(schema_field.name))
                else:
                    col_data[col_name].append(None)

        df = pd.DataFrame(col_data, index=index)
        # df = pd.DataFrame(group_data).transpose()

        st.dataframe(df)
    # st.write(df.loc[df.index == 'test1234'])
    # st.table([x.model_dump(mode="json") for x in all_entries])

    with st.form("Add New Data"):
        group = st.selectbox("Group", dbform.groups)
        row_id = st.text_input("Row ID")
        column = st.selectbox("Column", dbform.get_ordered_columns())
        col_idx = dbform.columns.index(column)
        field_data = {}
        for field in dbform.form_data_type_schema:
            match field.field_type:
                case "bool":
                    field_val = st.checkbox(field.name)
                case "int":
                    field_val = int(st.number_input(field.name, step=1))
                case "str":
                    field_val = st.text_input(field.name)
                case "float":
                    field_val = float(st.number_input(field.name, step=1.0))
                case _:
                    raise ValueError("Bad type")
            field_data[field.name] = field_val
        if st.form_submit_button("Add"):
            new_entry = fdm.store_form_data(
                dbform, StoredFormData(col_idx=col_idx, row_identifier=row_id, group_identifier=group, data=field_data)
            )
            st.code(new_entry.model_dump_json(indent=2))


def render_form_management(fdm: FormDataManager):
    main, sidebar = st.columns((2, 1))

    with sidebar:
        manage_categories = st.toggle("Manage Categories")
        manage_form_data_types = st.toggle("Manage Data Types")

    with main:
        if manage_categories:
            with st.container(border=True):
                st.subheader("Categories")

                def _del(name):
                    fdm.remove_form_category(name)

                for category in fdm.list_form_categories():
                    c1, c2 = st.columns(2)
                    with c1:
                        st.write(category)
                    with c2:
                        st.button("Delete", key=f"delete-{category}", on_click=_del, args=(category,))
                with st.form("Add category", clear_on_submit=True, border=False):
                    new_cat = st.text_input("New category")
                    if st.form_submit_button("Add") and new_cat:
                        fdm.add_form_category(new_cat)
                        st.rerun()

        if manage_form_data_types:
            ta = TypeAdapter(list[FormDataEntryField])
            with st.container(border=True):
                st.header("Data Types")
                for idx, data_type in enumerate(fdm.list_available_types()):
                    if idx:
                        st.divider()
                    st.write(f"**Data Type: {data_type.name}**", len(data_type.entry_schema))
                    with st.expander("Fields"):
                        st.code(ta.dump_json(data_type.entry_schema, indent=2).decode())
                if st.toggle("Add new data type"):
                    num_form_fields = st.number_input("Number of Fields", min_value=1, max_value=10)
                    form = st.form("Add Data Type", clear_on_submit=True, border=False)
                    with form:
                        new_data_type_name = st.text_input("New Data Type Name")
                        fields = []
                        for idx in range(num_form_fields):
                            if idx:
                                st.divider()
                            c1, c2 = st.columns(2)

                            def _fix_allowed(allowed_vals: Optional[str] = None) -> list[str]:
                                if not allowed_vals:
                                    return []
                                return [x.strip() for x in allowed_vals.split("\n")]

                            fields.append(
                                {
                                    "name": c1.text_input("Field Name", key=f"field-{idx}-name"),
                                    "field_type": c2.selectbox(
                                        "Field Type", ("str", "int", "float", "bool"), key=f"field-{idx}-type"
                                    ),
                                    "allowed_values": _fix_allowed(
                                        st.text_area(
                                            "Optional Allowed Values (one per line)", key=f"field-{idx}-allowed"
                                        )
                                    ),
                                }
                            )
                        if st.form_submit_button("Add") and new_data_type_name:
                            valid_fields = ta.validate_python(fields)
                            fdm.add_new_type(new_data_type_name, valid_fields)
                            st.rerun()

        st.header("Forms")
        categories = fdm.list_form_categories()
        filter_category = st.selectbox("Category", categories, None)
        forms = fdm.list_forms(category=filter_category)
        if not forms:
            st.write("_No Matches_")
        else:

            def _set_query(form_obj: Form):
                st.query_params.lf = form_obj.resource_id

            def _display(form_obj: Form):
                st.subheader(f"**Form:** `{form_obj.name}`")
                st.metric("Columns", len(form_obj.get_ordered_columns()))
                st.metric("Groups", len(form_obj.groups))

            display_pydantic_models(
                forms,
                btn_callbacks={"Load": _set_query},
                # select_action_callbacks={"Edit": _set_query},
                code_view=False,
                display_func=_display,
                include_divider=False,
            )

            # for idx, dbform in enumerate(forms):

            # if idx:
            #     st.divider()
            #
            # st.button("Load", on_click=_set_query, args=(dbform.resource_id,), key="loadform-" + dbform.resource_id)
            # if st.toggle("View Full Object", key="toggle-full-form-" + dbform.resource_id):
            #     st.code(dbform.model_dump_json(indent=2))
        st.divider()
        if st.toggle("New Form"):
            render_new_form(fdm, categories)


_T = TypeVar("_T", bound=BaseModel)


def display_pydantic_models(
    data: list[_T],
    code_view=True,
    *,
    display_func: Optional[Callable[[_T], Any]] = None,
    btn_callbacks: Optional[dict[str, Callable[[_T], Any]]] = None,
    select_action_callbacks: Optional[dict[str, Callable[[_T], Any]]] = None,
    include_divider: bool = True,
):
    for idx, row in enumerate(data):
        if include_divider and idx:
            st.divider()
        c1, c2 = st.columns((2, 1))
        with c1:
            if display_func:
                display_func(row)
            elif code_view:
                st.code(row.model_dump_json(indent=2))
            else:
                if hasattr(row, "name"):
                    st.write(f"**{row.name}**")
                else:
                    st.write(str(row))
        with c2:
            if select_action_callbacks:

                def _handle_select_action(act: str, dbojb):
                    if not act:
                        return
                    select_action_callbacks[act](dbojb)

                selected_action = st.selectbox(
                    "Action", select_action_callbacks.keys(), index=None, key=f"{row}-select"
                )
                st.button("Execute", on_click=_handle_select_action, args=(selected_action, row), key=f"{row}-execute")
            if btn_callbacks:

                def _handle_btn_action(act: str, dbojb):
                    btn_callbacks[act](dbojb)

                for label, callback in btn_callbacks.items():
                    st.button(label, on_click=_handle_btn_action, args=(label, row), key=f"{row}-btn-{label}")


def render_new_form(fdm: FormDataManager, categories):
    if not categories:
        st.warning("Add at least one category first!")
        return
    with st.form("New Form"):
        name = st.text_input("Name")
        category = st.selectbox("Category", fdm.list_form_categories())
        form_data_type: FormDataType = st.selectbox(
            "Data Type", fdm.list_available_types(), format_func=lambda x: x.name
        )
        columns = st.text_area("Column Names (one per line) - min 1")
        columns = [x.strip() for x in columns.split("\n")]
        groups = st.text_area("Group Names (one per line) - min 1")
        groups = [x.strip() for x in groups.split("\n")]
        if st.form_submit_button("Create"):
            request = NewFormRequest(
                name=name,
                category=category,
                form_data_type_id=form_data_type.resource_id,
                form_data_type_version=form_data_type.version,
                form_data_type_schema=form_data_type.entry_schema,
                columns=columns,
                groups=groups,
            )
            st.code(request.model_dump_json(indent=2))
            fdm.create_form(request)


if __name__ == "__main__":
    main()
