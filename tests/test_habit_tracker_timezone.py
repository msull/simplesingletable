"""The habit tracker's clock is injectable, and defaults to local time.

Habit tracking is a local-time question -- "did I do this today?" is asked and
answered wherever the person is. Defaulting to UTC would file an evening habit
under tomorrow's date for anyone west of it, so local is the deliberate default
and `tz` is how a caller pins a specific zone instead.
"""

from datetime import date, datetime, timedelta, timezone

from simplesingletable.extras.habit_tracker import (
    MonthlyHabitTracker,
    MonthlyHabitTrackerV2,
    habit_now,
    habit_today,
)


def test_default_is_local_and_timezone_aware():
    now = habit_now()

    assert now.tzinfo is not None, "must be aware so DTZ-style ambiguity cannot creep back in"
    assert now.utcoffset() == datetime.now().astimezone().utcoffset()


def test_explicit_timezone_is_honored():
    assert habit_now(timezone.utc).utcoffset() == timedelta(0)

    plus_13 = timezone(timedelta(hours=13))
    assert habit_now(plus_13).utcoffset() == timedelta(hours=13)


def test_today_follows_the_supplied_timezone():
    """The whole point: the date can differ by zone at the same instant."""
    minus_11 = timezone(timedelta(hours=-11))
    plus_13 = timezone(timedelta(hours=13))

    assert habit_today(minus_11) == habit_now(minus_11).date()
    assert habit_today(plus_13) == habit_now(plus_13).date()

    # 24 hours apart, so they are never more than one day apart and often differ.
    assert abs((habit_today(plus_13) - habit_today(minus_11)).days) <= 1


def test_local_default_matches_system_date():
    assert habit_today() == datetime.now().astimezone().date()
    assert isinstance(habit_today(), date)


def test_serialized_form_is_unchanged_by_the_aware_default():
    """Entries were already serialized through .astimezone(), so switching the
    default from naive-local to aware-local must not alter stored strings."""
    naive_local = datetime.now()
    aware_local = habit_now()

    old_style = naive_local.replace(microsecond=0).astimezone().isoformat()
    new_style = aware_local.replace(microsecond=0).isoformat()

    # Same wall clock, same offset, same text (modulo the second they were taken).
    assert old_style[:16] == new_style[:16]
    assert old_style[-6:] == new_style[-6:]


def test_tz_parameter_is_accepted_by_both_tracker_versions():
    import inspect

    for cls in (MonthlyHabitTracker, MonthlyHabitTrackerV2):
        for method_name in ("get_for_month", "track_item", "track_item_for_date"):
            params = inspect.signature(getattr(cls, method_name)).parameters
            assert "tz" in params, f"{cls.__name__}.{method_name} should accept tz"
            assert params["tz"].default is None, f"{cls.__name__}.{method_name} tz must default to None"
