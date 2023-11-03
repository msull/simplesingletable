from pathlib import Path
from typing import Optional

import streamlit as st
from streamlit_extras.echo_expander import echo_expander
from logzero import logger

TABLE_DEFINITION = Path(__file__).parent / "example_tables" / "standard.yaml"

st.header("Simple Single Table usage example")

basic_tab, advanced_tab = st.tabs(("Basic usage", "Advanced Usage"))

with basic_tab:
    with st.expander('Start with imports and setting up the basic "memory" client'):
        with echo_expander(expander=False, label="Basic Setup"):
            from simplesingletable import (
                DynamoDBMemory,
                DynamodbResource,
                DynamodbVersionedResource,
            )

            memory = DynamoDBMemory(
                logger=logger,
                table_name="standardexample",
                endpoint_url="http://localhost:8000",
            )
        st.write(
            """
        In this example, I'm connecting to a local DynamoDB service running via docker, and have created 
        a table named `standardexample` with the following definition:
        """
        )
        st.code(TABLE_DEFINITION.read_text())

    with st.expander("Model definition and basic create/update"):
        st.write(
            """
        Now define some resource classes for things you want to store in the database.
        Resource classes are Pydantic models that inherit from either `DynamodbResource` or
        `DynamodbVersionedResource`.
        The difference between these, as the name implies, is that one is versioned. 
        If the resource is "versioned" every time it is updated the full object is
        stored in the database again, allowing the full history of the object to be reviewed.
        
        Here are a few simple models:
        """
        )

        with st.echo():

            class User(DynamodbResource):
                name: str
                tags: list[str]
                num_followers: int = 0

            class JournalEntry(DynamodbResource):
                content: str
                user_id: str

            class SimpleTask(DynamodbVersionedResource):
                task: str
                descr: Optional[str]
                completed: bool
                user_id: str
                tags: list[str]

        st.write("Now we can begin storing and retrieving objects from the database")

        RESOURCE_ID = st.text_input(
            "RESOURCE_ID", st.session_state.get("resource_id"), help="Clear this to generate a new resource"
        )
        if RESOURCE_ID:
            st.session_state["resource_id"] = RESOURCE_ID

        if RESOURCE_ID:
            with st.echo():
                created_user = memory.read_existing(RESOURCE_ID, User)
                st.json(created_user.model_dump_json())

        else:
            with st.echo():
                created_user = memory.create_new(
                    User,
                    {"name": "New User", "tags": []},
                )
                st.json(created_user.model_dump_json())
            st.session_state["resource_id"] = created_user.resource_id
        st.write(
            """
        The resource automatically includes a `resource_id`, as well as `created_at` and `updated_at` attributes.
        The default `resource_id` is a ULID, which means that it sorts as a string in order of creation time.
        
        Having this ID sort by creation time is useful in our advanced access patterns that utilize secondary
        indices.
        
        The ID can be converted back to the ULID type with a helper function:
        """
        )
        with st.echo():
            ulid = created_user.resource_id_as_ulid()
            st.write(ulid.timestamp().datetime)

        st.write(
            """
        There are also helpers for viewing how recently resources were created or updated:
        """
        )

        with st.form("Update user object"):
            new_name = st.text_input("name", created_user.name)

            if st.form_submit_button("Update") and new_name:
                with st.echo():
                    created_user = memory.update_existing(created_user, {"name": new_name})
                    st.json(created_user.model_dump_json())
                st.info("Changed user name")

        with st.echo():
            st.write(f"Object was created {created_user.created_ago()}.")
            st.write(f"Object was updated {created_user.updated_ago()}.")
            st.write(f"Object was updated {created_user.updated_ago('seconds')}.")

        st.write("Non-versioned resources support atomic counters with an easy increment method:")
        increase_by = st.slider('Change Followers By', min_value=-10, max_value=10, value=1)
        num_followers = created_user.num_followers
        st.metric("Followers before", num_followers)

        if st.button('Update Followers'):
            with st.echo():
                num_followers = memory.increment_counter(created_user, 'num_followers', incr_by=increase_by)
            st.session_state["resource_id"] = created_user.resource_id
            st.metric("Followers after", num_followers)

