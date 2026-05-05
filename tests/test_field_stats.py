"""Tests for pipewarden.field_stats."""
import pytest

from pipewarden.field_stats import FieldStats, FieldStatsReport, compute_field_stats


def _rows():
    return [
        {"name": "Alice", "age": 30, "score": 9.5},
        {"name": "Bob",   "age": 25, "score": 7.0},
        {"name": None,    "age": 40, "score": None},
        {"name": "Alice", "age": 30, "score": 8.0},
    ]


# --- FieldStats unit tests ---

def test_field_stats_total_count():
    s = FieldStats(name="x")
    for v in [1, 2, None]:
        s.record(v)
    assert s.total_count == 3


def test_field_stats_null_count():
    s = FieldStats(name="x")
    for v in [1, None, None]:
        s.record(v)
    assert s.null_count == 2


def test_field_stats_non_null_count():
    s = FieldStats(name="x")
    for v in [1, None, 3]:
        s.record(v)
    assert s.non_null_count == 2


def test_field_stats_null_rate():
    s = FieldStats(name="x")
    for v in [1, None]:
        s.record(v)
    assert s.null_rate == pytest.approx(0.5)


def test_field_stats_null_rate_zero_rows():
    s = FieldStats(name="x")
    assert s.null_rate == 0.0


def test_field_stats_unique_count():
    s = FieldStats(name="x")
    for v in ["a", "b", "a", None]:
        s.record(v)
    assert s.unique_count == 2


def test_field_stats_min_max_mean():
    s = FieldStats(name="x")
    for v in [10, 20, 30]:
        s.record(v)
    assert s.min_value == pytest.approx(10.0)
    assert s.max_value == pytest.approx(30.0)
    assert s.mean_value == pytest.approx(20.0)


def test_field_stats_no_numeric_values_returns_none():
    s = FieldStats(name="x")
    s.record("hello")
    assert s.min_value is None
    assert s.max_value is None
    assert s.mean_value is None


def test_field_stats_bool_not_treated_as_numeric():
    s = FieldStats(name="x")
    s.record(True)
    s.record(False)
    assert s.min_value is None


# --- compute_field_stats integration tests ---

def test_report_contains_all_fields():
    report = compute_field_stats("t", _rows())
    assert set(report.field_names) == {"name", "age", "score"}


def test_report_null_count_for_name():
    report = compute_field_stats("t", _rows())
    assert report.get("name").null_count == 1


def test_report_unique_count_for_name():
    report = compute_field_stats("t", _rows())
    # Alice, Bob (None excluded)
    assert report.get("name").unique_count == 2


def test_report_mean_age():
    report = compute_field_stats("t", _rows())
    assert report.get("age").mean_value == pytest.approx((30 + 25 + 40 + 30) / 4)


def test_report_get_missing_field_returns_none():
    report = compute_field_stats("t", _rows())
    assert report.get("nonexistent") is None


def test_report_table_name_stored():
    report = compute_field_stats("orders", _rows())
    assert report.table == "orders"


def test_report_empty_rows_yields_empty_report():
    report = compute_field_stats("t", [])
    assert report.field_names == []
