from collections import defaultdict
from datetime import date, datetime
from typing import Optional

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource


class HabitTracker(BaseModel):
    """
    A base class for tracking sets of stuff.
    Subclass this and add set[str] fields for each item you want to track.
    Example:
        yoga: set[str] = Field(default_factory=set)
        reading: set[str] = Field(default_factory=set)
    Each entry in the set is something like '2025-01-22T09:15:00#morning session'.
    """

    def count_occurrences(self, habit_name: str) -> int:
        """
        Returns the number of occurrences stored for a given habit.
        """
        habit_set = getattr(self, habit_name) or set()
        return len(habit_set)

    def summarize(self) -> dict[str, int]:
        """
        Returns a dict with habit_name -> count_of_occurrences
        for all fields whose type is set[str].
        """
        summary = {}
        for field_name, field_info in self.model_fields.items():
            annot_str = str(field_info.annotation)
            if annot_str in ["set[str]", "typing.Set[str]", "Optional[set[str]]", "typing.Optional[set[str]]"]:
                summary[field_name] = len(getattr(self, field_name) or set())
        return summary

    def list_all(self) -> list[dict]:
        return_list = []

        # For each habit field that is a set[str], parse its entries
        for field_name, field_info in self.model_fields.items():
            # Check if this field is a set[str] or optional set[str]
            annot_str = str(field_info.annotation)
            if annot_str not in ["set[str]", "typing.Set[str]", "Optional[set[str]]", "typing.Optional[set[str]]"]:
                continue

            # For each timestamp#note in this habit's set
            habit_set = getattr(self, field_name) or set()
            for entry in habit_set:
                if "#" in entry:
                    dt_str, note = entry.split("#", 1)
                else:
                    dt_str = entry
                    note = ""

                return_list.append({"tracker": field_name, "when": dt_str, "note": note})
        return return_list

    def summarize_by_date(self) -> dict[str, dict[str, int]]:
        """
        Returns a nested dict of the form:
            {
                'YYYY-MM-DD': {
                    'habit_name_1': count,
                    'habit_name_2': count,
                    ...
                },
                ...
            }
        For each habit's set of timestamps, we parse out the date (YYYY-MM-DD)
        and accumulate a count of how many times that habit occurs on that date.
        """
        date_summary = defaultdict(lambda: defaultdict(int))

        # For each habit field that is a set[str], parse its entries
        for field_name, field_info in self.model_fields.items():
            # Check if this field is a set[str] or optional set[str]
            annot_str = str(field_info.annotation)
            if annot_str not in ["set[str]", "typing.Set[str]", "Optional[set[str]]", "typing.Optional[set[str]]"]:
                continue

            # For each timestamp#note in this habit's set
            habit_set = getattr(self, field_name) or set()
            for entry in habit_set:
                if "#" in entry:
                    dt_str, _ = entry.split("#", 1)
                else:
                    dt_str = entry

                # We assume ISO8601 string includes 'YYYY-MM-DDT...'
                date_part = dt_str.split("T", 1)[0]  # e.g. "2025-01-22"
                date_summary[date_part][field_name] += 1

        # Convert defaultdict structure to a plain dict
        # date_summary[date_str] -> { habit_name: count }
        final_summary = {date_str: dict(habit_counts) for date_str, habit_counts in date_summary.items()}
        return final_summary

    def parse_habit_details(self, habit_name: str) -> list[tuple[str, str]]:
        """
        Splits each entry into (timestamp, note), if stored as 'YYYY-MM-DDTHH:MM:SS#optional note'.
        """
        habit_set = getattr(self, habit_name) or set()
        details = []
        for entry in habit_set:
            if "#" in entry:
                dt_str, note = entry.split("#", 1)
            else:
                dt_str, note = entry, ""
            details.append((dt_str, note))
        # Sort ascending by datetime string (ISO8601 lexicographically sorts by time)
        details.sort(key=lambda x: x[0])
        return details


class MonthlyHabitTracker(DynamoDbResource, HabitTracker):
    """
    Stores monthly tracking info in DynamoDB.
    The 'month' field is used as the resource_id (e.g. "202501").
    """

    month: str  # e.g. '202501'

    @classmethod
    def get_for_month(
        cls,
        memory: "DynamoDbMemory",
        for_date: Optional[date] = None,
        consistent_read: bool = True,
    ) -> "MonthlyHabitTracker":
        """
        Retrieve or create the tracker for the given date’s month.
        Default is today's date if none provided.
        """
        if for_date is None:
            for_date = date.today()

        month_key = for_date.strftime("%Y%m")  # e.g. "202501"

        existing = memory.get_existing(month_key, data_class=cls, consistent_read=consistent_read)
        if existing:
            return existing

        # If not found, create a new record
        new_tracker_data = {"month": month_key}
        return memory.create_new(cls, new_tracker_data, override_id=month_key)

    def track_item(
        self,
        memory: "DynamoDbMemory",
        habit_name: str,
        dt: Optional[datetime] = None,
        note: str = "",
    ):
        """
        Records a habit occurrence by adding to the appropriate set.
        Ensures the provided datetime is within self.month,
        and that habit_name is declared as set[str].
        """
        if dt is None:
            dt = datetime.now()

        # 1. Validate that dt is within this tracker's month
        year_str, month_str = self.month[:4], self.month[4:]  # e.g. '2025', '01'
        year_int, month_int = int(year_str), int(month_str)
        if dt.year != year_int or dt.month != month_int:
            raise ValueError(f"Provided datetime {dt} is not in the correct month {self.month}.")

        # 2. Ensure habit_name is a declared set[str] field
        field_info = self.model_fields.get(habit_name)
        if not field_info:
            raise ValueError(f"Habit field '{habit_name}' is not declared on this model.")

        annot_str = str(field_info.annotation)
        valid_annotations = {
            "set[str]",
            "typing.Set[str]",
            "Optional[set[str]]",
            "typing.Optional[set[str]]",
        }
        if annot_str not in valid_annotations:
            raise ValueError(f"Habit field '{habit_name}' must be declared as set[str], not {annot_str}.")

        # 3. Prepare the value to store
        dt_str = dt.replace(microsecond=0).astimezone().isoformat()
        value_to_store = dt_str if not note else f"{dt_str}#{note}"

        # 4. Update DynamoDB (atomic add to set)
        memory.add_to_set(
            existing_resource=self,
            field_name=habit_name,
            val=value_to_store,
        )

    @classmethod
    def track_item_for_date(
        cls,
        memory: "DynamoDbMemory",
        habit_name: str,
        dt: Optional[datetime] = None,
        note: str = "",
        consistent_read: bool = True,
    ) -> "MonthlyHabitTracker":
        """
        Convenience classmethod that:
         - Determines the correct month from dt (or uses now if dt=None)
         - Retrieves or creates the MonthlyHabitTracker for that month
         - Calls track_item on that tracker
         - Returns the updated tracker
        """
        if dt is None:
            dt = datetime.now()

        # 1. Get the correct monthly tracker based on dt
        tracker_date = dt.date()
        tracker = cls.get_for_month(
            memory=memory,
            for_date=tracker_date,
            consistent_read=consistent_read,
        )

        # 2. Track the item on that monthly tracker
        tracker.track_item(memory, habit_name=habit_name, dt=dt, note=note)

        return tracker

    @classmethod
    def get_by_month_range(
        cls,
        memory: "DynamoDbMemory",
        start_date: date,
        end_date: date,
    ) -> list["MonthlyHabitTracker"]:
        """
        Retrieves all MonthlyHabitTracker records spanning from start_date's month
        through end_date's month, inclusive.
        Uses a GSI approach with (gsitype, gsitypesk).
        """
        start_month_str = start_date.strftime("%Y%m")
        end_month_str = end_date.strftime("%Y%m")

        return memory.paginated_dynamodb_query(
            key_condition=(Key("gsitype").eq(cls.__name__) & Key("gsitypesk").between(start_month_str, end_month_str)),
            index_name="gsitype",
            resource_class=cls,
        )

    def db_get_gsitypesk(self) -> str:
        """
        Override the base method for GSI sort key usage.
        We use 'month' for quick range queries (e.g. 202501 -> between(202501, 202512)).
        """
        return self.month
