"""Tests for pipewarden.row_counter."""
import pytest
from pipewarden.row_counter import (
    RowCountSnapshot,
    RowCountDelta,
    RowCountReport,
    compare_row_counts,
)


# --- RowCountSnapshot ---

def test_snapshot_set_and_get():
    snap = RowCountSnapshot()
    snap.set("orders", 100)
    assert snap.get("orders") == 100


def test_snapshot_get_missing_returns_none():
    snap = RowCountSnapshot()
    assert snap.get("missing") is None


def test_snapshot_negative_count_raises():
    snap = RowCountSnapshot()
    with pytest.raises(ValueError):
        snap.set("orders", -1)


def test_snapshot_table_names():
    snap = RowCountSnapshot()
    snap.set("a", 1)
    snap.set("b", 2)
    assert set(snap.table_names()) == {"a", "b"}


# --- RowCountDelta ---

def test_delta_computes_positive_delta():
    d = RowCountDelta(table="orders", previous=50, current=80)
    assert d.delta == 30


def test_delta_computes_negative_delta():
    d = RowCountDelta(table="orders", previous=100, current=60)
    assert d.delta == -40


def test_delta_none_when_previous_missing():
    d = RowCountDelta(table="orders", previous=None, current=10)
    assert d.delta is None


def test_delta_is_new_when_no_previous():
    d = RowCountDelta(table="orders", previous=None, current=10)
    assert d.is_new is True
    assert d.is_removed is False


def test_delta_is_removed_when_no_current():
    d = RowCountDelta(table="orders", previous=10, current=None)
    assert d.is_removed is True
    assert d.is_new is False


def test_delta_changed_true_when_different():
    d = RowCountDelta(table="orders", previous=10, current=20)
    assert d.changed is True


def test_delta_changed_false_when_same():
    d = RowCountDelta(table="orders", previous=10, current=10)
    assert d.changed is False


# --- compare_row_counts ---

def test_no_changes_when_counts_equal():
    prev = RowCountSnapshot(counts={"orders": 100, "users": 50})
    curr = RowCountSnapshot(counts={"orders": 100, "users": 50})
    report = compare_row_counts(prev, curr)
    assert report.has_changes is False


def test_detects_count_change():
    prev = RowCountSnapshot(counts={"orders": 100})
    curr = RowCountSnapshot(counts={"orders": 150})
    report = compare_row_counts(prev, curr)
    assert report.has_changes is True
    assert len(report.changed_tables) == 1
    assert report.changed_tables[0].delta == 50


def test_detects_new_table():
    prev = RowCountSnapshot(counts={})
    curr = RowCountSnapshot(counts={"orders": 10})
    report = compare_row_counts(prev, curr)
    assert len(report.new_tables) == 1
    assert report.new_tables[0].table == "orders"


def test_detects_removed_table():
    prev = RowCountSnapshot(counts={"orders": 10})
    curr = RowCountSnapshot(counts={})
    report = compare_row_counts(prev, curr)
    assert len(report.removed_tables) == 1
    assert report.removed_tables[0].table == "orders"


def test_report_covers_all_tables():
    prev = RowCountSnapshot(counts={"a": 1, "b": 2})
    curr = RowCountSnapshot(counts={"b": 3, "c": 4})
    report = compare_row_counts(prev, curr)
    tables = {d.table for d in report.deltas}
    assert tables == {"a", "b", "c"}
