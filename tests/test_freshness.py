"""Tests for pipewarden.freshness."""

from datetime import datetime, timedelta

import pytest

from pipewarden.freshness import (
    FreshnessRule,
    FreshnessViolation,
    FreshnessReport,
    check_freshness,
)


def _rule(max_age_seconds: int = 3600) -> FreshnessRule:
    return FreshnessRule(
        table="events",
        timestamp_field="created_at",
        max_age=timedelta(seconds=max_age_seconds),
        description="events must be fresh",
    )


def _recent_row() -> dict:
    return {"id": 1, "created_at": datetime.utcnow().isoformat()}


def _stale_row(hours_ago: int = 5) -> dict:
    ts = datetime.utcnow() - timedelta(hours=hours_ago)
    return {"id": 2, "created_at": ts.isoformat()}


# --- FreshnessRule.check ---

def test_no_violation_when_data_is_fresh():
    rule = _rule(max_age_seconds=3600)
    result = rule.check([_recent_row()])
    assert result is None


def test_violation_when_data_is_stale():
    rule = _rule(max_age_seconds=3600)
    result = rule.check([_stale_row(hours_ago=5)])
    assert isinstance(result, FreshnessViolation)
    assert result.table == "events"


def test_violation_when_no_rows():
    rule = _rule()
    result = rule.check([])
    assert isinstance(result, FreshnessViolation)
    assert "No rows" in result.message


def test_violation_when_field_missing():
    rule = _rule()
    result = rule.check([{"id": 1}])  # no 'created_at'
    assert isinstance(result, FreshnessViolation)
    assert result.latest_ts is None


def test_violation_when_field_unparseable():
    rule = _rule()
    result = rule.check([{"created_at": "not-a-date"}])
    assert isinstance(result, FreshnessViolation)


def test_uses_latest_timestamp_among_rows():
    rule = _rule(max_age_seconds=3600)
    rows = [_stale_row(hours_ago=10), _recent_row()]
    result = rule.check(rows)
    assert result is None  # recent row should pass


def test_violation_str_contains_table_name():
    rule = _rule()
    v = FreshnessViolation(table="events", rule=rule, message="too old", latest_ts=None)
    assert "events" in str(v)


def test_violation_str_contains_latest_ts():
    ts = datetime(2024, 1, 15, 12, 0, 0)
    rule = _rule()
    v = FreshnessViolation(table="events", rule=rule, message="too old", latest_ts=ts)
    assert "2024-01-15" in str(v)


# --- FreshnessReport ---

def test_report_passed_when_no_violations():
    report = FreshnessReport()
    assert report.passed is True


def test_report_fails_when_violation_added():
    rule = _rule()
    report = FreshnessReport()
    report.add(FreshnessViolation(table="events", rule=rule, message="stale", latest_ts=None))
    assert report.passed is False
    assert len(report.violations) == 1


# --- check_freshness ---

def test_check_freshness_no_violations():
    rule = _rule(max_age_seconds=3600)
    report = check_freshness([rule], {"events": [_recent_row()]})
    assert report.passed


def test_check_freshness_missing_table_triggers_violation():
    rule = _rule()
    report = check_freshness([rule], {})  # table not provided
    assert not report.passed


def test_check_freshness_multiple_rules():
    r1 = FreshnessRule(table="orders", timestamp_field="ts", max_age=timedelta(hours=1))
    r2 = FreshnessRule(table="events", timestamp_field="created_at", max_age=timedelta(hours=1))
    table_rows = {
        "orders": [{"ts": datetime.utcnow().isoformat()}],
        "events": [{"created_at": (datetime.utcnow() - timedelta(hours=5)).isoformat()}],
    }
    report = check_freshness([r1, r2], table_rows)
    assert not report.passed
    assert len(report.violations) == 1
    assert report.violations[0].table == "events"
