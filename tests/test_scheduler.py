"""Tests for pipewarden.scheduler and pipewarden.schedule_reporter."""

from datetime import datetime, timedelta

import pytest

from pipewarden.scheduler import Scheduler, ScheduleEntry, parse_interval
from pipewarden.schedule_reporter import (
    format_entry,
    format_schedule,
    schedule_summary,
)

# ---------------------------------------------------------------------------
# parse_interval
# ---------------------------------------------------------------------------

def test_parse_hourly():
    assert parse_interval("@hourly") == timedelta(hours=1)


def test_parse_daily():
    assert parse_interval("@daily") == timedelta(days=1)


def test_parse_every_n_minutes():
    assert parse_interval("every 30m") == timedelta(minutes=30)


def test_parse_invalid_raises():
    with pytest.raises(ValueError, match="Unsupported interval"):
        parse_interval("every day")


# ---------------------------------------------------------------------------
# ScheduleEntry
# ---------------------------------------------------------------------------

def test_entry_is_due_when_never_run():
    entry = ScheduleEntry(name="pipe", interval=timedelta(hours=1))
    assert entry.is_due() is True


def test_entry_not_due_immediately_after_run():
    now = datetime(2024, 1, 1, 12, 0, 0)
    entry = ScheduleEntry(name="pipe", interval=timedelta(hours=1), last_run=now)
    assert entry.is_due(now) is False


def test_entry_due_after_interval_elapsed():
    past = datetime(2024, 1, 1, 11, 0, 0)
    now = datetime(2024, 1, 1, 12, 0, 1)
    entry = ScheduleEntry(name="pipe", interval=timedelta(hours=1), last_run=past)
    assert entry.is_due(now) is True


def test_mark_run_sets_last_run():
    now = datetime(2024, 6, 1, 8, 0, 0)
    entry = ScheduleEntry(name="pipe", interval=timedelta(minutes=15))
    entry.mark_run(now)
    assert entry.last_run == now


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def test_register_adds_entry():
    s = Scheduler()
    s.register("etl_orders", "@daily")
    assert "etl_orders" in s.pipeline_names()


def test_get_returns_entry():
    s = Scheduler()
    s.register("etl_users", "every 30m")
    entry = s.get("etl_users")
    assert entry is not None
    assert entry.name == "etl_users"


def test_get_returns_none_for_unknown():
    s = Scheduler()
    assert s.get("missing") is None


def test_due_returns_only_due_entries():
    now = datetime(2024, 1, 1, 12, 0, 0)
    s = Scheduler()
    s.register("a", "every 30m")
    s.register("b", "every 30m")
    s.get("b").mark_run(now)  # mark b as just run
    due = s.due(now)
    assert len(due) == 1
    assert due[0].name == "a"


# ---------------------------------------------------------------------------
# schedule_reporter
# ---------------------------------------------------------------------------

def test_format_entry_shows_due_marker():
    entry = ScheduleEntry(name="pipe", interval=timedelta(minutes=30))
    result = format_entry(entry)
    assert "[DUE]" in result
    assert "pipe" in result


def test_format_entry_no_due_marker_when_just_run():
    now = datetime(2024, 1, 1, 12, 0, 0)
    entry = ScheduleEntry(name="pipe", interval=timedelta(hours=1), last_run=now)
    result = format_entry(entry, now)
    assert "[DUE]" not in result


def test_format_schedule_empty():
    s = Scheduler()
    assert "no pipelines" in format_schedule(s)


def test_format_schedule_lists_pipelines():
    s = Scheduler()
    s.register("orders", "@daily")
    output = format_schedule(s)
    assert "orders" in output


def test_schedule_summary_counts_due():
    now = datetime(2024, 1, 1, 12, 0, 0)
    s = Scheduler()
    s.register("a", "every 30m")
    s.register("b", "every 30m")
    s.get("b").mark_run(now)
    summary = schedule_summary(s, now)
    assert summary.startswith("1/2")
