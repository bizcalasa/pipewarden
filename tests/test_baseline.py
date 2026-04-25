"""Tests for pipewarden.baseline."""

import json
import pytest
from pathlib import Path

from pipewarden.baseline import (
    BaselineEntry,
    BaselineReport,
    BaselineDiff,
    save_baseline,
    load_baseline,
    compare_to_baseline,
)


def _make_report(*entries):
    return BaselineReport(entries=list(entries))


def _entry(table="orders", error_count=0, is_valid=True):
    return BaselineEntry(table=table, error_count=error_count, is_valid=is_valid)


# --- BaselineReport helpers ---

def test_get_returns_entry_by_name():
    report = _make_report(_entry("orders"), _entry("users"))
    result = report.get("users")
    assert result is not None
    assert result.table == "users"


def test_get_returns_none_for_missing_table():
    report = _make_report(_entry("orders"))
    assert report.get("missing") is None


def test_table_names_returns_all_names():
    report = _make_report(_entry("a"), _entry("b"), _entry("c"))
    assert report.table_names() == ["a", "b", "c"]


# --- save / load round-trip ---

def test_save_and_load_round_trip(tmp_path):
    report = _make_report(
        _entry("orders", error_count=3, is_valid=False),
        _entry("users", error_count=0, is_valid=True),
    )
    dest = tmp_path / "baseline.json"
    save_baseline(report, dest)
    loaded = load_baseline(dest)
    assert len(loaded.entries) == 2
    orders = loaded.get("orders")
    assert orders.error_count == 3
    assert orders.is_valid is False


def test_save_produces_valid_json(tmp_path):
    report = _make_report(_entry("t", error_count=1, is_valid=False))
    dest = tmp_path / "b.json"
    save_baseline(report, dest)
    data = json.loads(dest.read_text())
    assert isinstance(data, list)
    assert data[0]["table"] == "t"


# --- compare_to_baseline ---

def test_compare_detects_regression():
    baseline = _make_report(_entry("orders", error_count=0, is_valid=True))
    current = _make_report(_entry("orders", error_count=5, is_valid=False))
    diffs = compare_to_baseline(baseline, current)
    assert "orders" in diffs
    assert diffs["orders"].regressed is True
    assert diffs["orders"].improved is False
    assert diffs["orders"].status_changed is True


def test_compare_detects_improvement():
    baseline = _make_report(_entry("orders", error_count=10, is_valid=False))
    current = _make_report(_entry("orders", error_count=2, is_valid=False))
    diffs = compare_to_baseline(baseline, current)
    assert diffs["orders"].improved is True
    assert diffs["orders"].regressed is False
    assert diffs["orders"].status_changed is False


def test_compare_skips_new_tables():
    baseline = _make_report(_entry("orders"))
    current = _make_report(_entry("orders"), _entry("new_table"))
    diffs = compare_to_baseline(baseline, current)
    assert "new_table" not in diffs
    assert "orders" in diffs


def test_compare_no_diff_when_identical():
    baseline = _make_report(_entry("orders", error_count=2, is_valid=False))
    current = _make_report(_entry("orders", error_count=2, is_valid=False))
    diffs = compare_to_baseline(baseline, current)
    d = diffs["orders"]
    assert d.regressed is False
    assert d.improved is False
    assert d.status_changed is False
