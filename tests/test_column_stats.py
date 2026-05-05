"""Tests for pipewarden.column_stats."""
import pytest
from pipewarden.column_stats import ColumnStats, ColumnStatsReport, compute_column_stats


def _rows():
    return [
        {"age": 30, "name": "Alice", "score": 9.5},
        {"age": 25, "name": "Bob",   "score": None},
        {"age": None, "name": "Alice", "score": 7.0},
        {"age": 40, "name": "Carol",  "score": 8.0},
    ]


def test_report_contains_all_columns():
    report = compute_column_stats("users", _rows())
    assert set(report.column_names()) == {"age", "name", "score"}


def test_total_count_equals_row_count():
    report = compute_column_stats("users", _rows())
    assert report.get("age").total == 4


def test_null_count_correct():
    report = compute_column_stats("users", _rows())
    assert report.get("age").null_count == 1
    assert report.get("score").null_count == 1
    assert report.get("name").null_count == 0


def test_non_null_count_correct():
    report = compute_column_stats("users", _rows())
    assert report.get("age").non_null_count == 3


def test_null_rate_calculation():
    report = compute_column_stats("users", _rows())
    cs = report.get("age")
    assert abs(cs.null_rate - 0.25) < 1e-9


def test_fill_rate_calculation():
    report = compute_column_stats("users", _rows())
    cs = report.get("name")
    assert abs(cs.fill_rate - 1.0) < 1e-9


def test_min_max_numeric():
    report = compute_column_stats("users", _rows())
    cs = report.get("age")
    assert cs.min_value == 25
    assert cs.max_value == 40


def test_unique_values_count():
    report = compute_column_stats("users", _rows())
    # Alice appears twice, Bob and Carol once
    assert report.get("name").unique_values == 3


def test_empty_rows_returns_empty_report():
    report = compute_column_stats("empty", [])
    assert report.column_names() == []


def test_all_null_column():
    rows = [{"x": None}, {"x": None}]
    report = compute_column_stats("t", rows)
    cs = report.get("x")
    assert cs.null_count == 2
    assert cs.null_rate == 1.0
    assert cs.min_value is None
    assert cs.max_value is None


def test_get_missing_column_returns_none():
    report = compute_column_stats("t", _rows())
    assert report.get("nonexistent") is None


def test_null_rate_zero_rows():
    cs = ColumnStats(name="x")
    assert cs.null_rate == 0.0
    assert cs.fill_rate == 1.0
