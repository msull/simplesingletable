from datetime import datetime
from typing import Optional

import streamlit as st
from logzero import logger
from pydantic import Field
from streamlit_calendar import calendar as st_calendar

from simplesingletable import DynamoDbMemory
from simplesingletable.extras.habit_tracker import MonthlyHabitTracker


class PersonalHabitsTracker(MonthlyHabitTracker):
    m: Optional[set[str]] = Field(default=None)
    s: Optional[set[str]] = Field(default=None)


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
    month_viewer_input = st.date_input("View for month", format="YYYY/MM/DD")
    habits = PersonalHabitsTracker.get_for_month(memory, for_date=month_viewer_input)
    with st.popover("full object"):
        st.write(habits)

    view = st.selectbox("view", ("dayGridMonth", "listMonth", "listYear"))

    calendar_options = {
        "initialView": view,
        "editable": False,
        # "selectable": False,
        "showNonCurrentEvents": True,
        "initialDate": month_viewer_input.isoformat(),
        "headerToolbar": {
            "left": "",
            "center": "title",
            "right": "",
        },
    }

    def _fmt_title(event):
        if event["note"]:
            return event["tracker"] + ": " + event["note"]
        return event["tracker"]

    events = [{"date": x["when"], "title": _fmt_title(x)} for x in habits.list_all()]
    calendar = st_calendar(events=events, options=calendar_options)
    st.write(calendar)
    with st.popover("track habit"):
        include_date = st.toggle("include date (today otherwise)")

        with st.form("Track", border=False):
            track_this = st.text_input("habit", "m")
            if include_date:
                track_date = st.date_input("habit_date")
                track_time = st.time_input("habit_time")
            else:
                track_date = None
                track_time = None

            track_note = st.text_input("note", max_chars=50)

            if st.form_submit_button("Track") and track_this:
                track_date = (
                    datetime.combine(track_date, track_time).replace(microsecond=0).astimezone()
                    if include_date
                    else None
                )
                habits.track_item_for_date(memory, track_this, dt=track_date, note=track_note)
                st.rerun()

    st.help(st_calendar)


if __name__ == "__main__":
    main()
