"""Tests for pipewarden.metric_tracker."""

import pytest
from pipewarden.metric_tracker import MetricDelta, MetricSnapshot, MetricTracker


def _snap(table: str, **metrics: float) -> MetricSnapshot:
    s = MetricSnapshot(table=table)
    for k, v in metrics.items():
        s.set(k, v)
    return s


# --- MetricSnapshot ---

def test_snapshot_get_returns_value():
    s = _snap("orders", row_count=100.0)
    assert s.get("row_count") == 100.0


def test_snapshot_get_returns_none_for_missing():
    s = _snap("orders")
    assert s.get("row_count") is None


# --- MetricDelta ---

def test_delta_changed_true_when_values_differ():
    d = MetricDelta(table="t", name="m", previous=1.0, current=2.0)
    assert d.changed is True


def test_delta_changed_false_when_values_equal():
    d = MetricDelta(table="t", name="m", previous=5.0, current=5.0)
    assert d.changed is False


def test_delta_value_computed_correctly():
    d = MetricDelta(table="t", name="m", previous=10.0, current=13.0)
    assert d.delta == pytest.approx(3.0)


def test_delta_is_none_when_previous_missing():
    d = MetricDelta(table="t", name="m", previous=None, current=5.0)
    assert d.delta is None


def test_is_new_when_no_previous():
    d = MetricDelta(table="t", name="m", previous=None, current=5.0)
    assert d.is_new is True
    assert d.is_removed is False


def test_is_removed_when_no_current():
    d = MetricDelta(table="t", name="m", previous=5.0, current=None)
    assert d.is_removed is True
    assert d.is_new is False


# --- MetricTracker ---

def test_latest_returns_none_for_unknown_table():
    tracker = MetricTracker()
    assert tracker.latest("missing") is None


def test_record_and_retrieve_latest():
    tracker = MetricTracker()
    s = _snap("orders", row_count=50.0)
    tracker.record(s)
    assert tracker.latest("orders") is s


def test_previous_returns_none_with_single_snapshot():
    tracker = MetricTracker()
    tracker.record(_snap("orders", row_count=50.0))
    assert tracker.previous("orders") is None


def test_previous_returns_second_to_last():
    tracker = MetricTracker()
    s1 = _snap("orders", row_count=50.0)
    s2 = _snap("orders", row_count=60.0)
    tracker.record(s1)
    tracker.record(s2)
    assert tracker.previous("orders") is s1


def test_deltas_empty_with_no_snapshots():
    tracker = MetricTracker()
    assert tracker.deltas("orders") == []


def test_deltas_all_new_with_single_snapshot():
    tracker = MetricTracker()
    tracker.record(_snap("orders", row_count=100.0, null_rate=0.1))
    deltas = tracker.deltas("orders")
    assert all(d.is_new for d in deltas)
    assert {d.name for d in deltas} == {"row_count", "null_rate"}


def test_deltas_detects_change_between_runs():
    tracker = MetricTracker()
    tracker.record(_snap("orders", row_count=100.0))
    tracker.record(_snap("orders", row_count=120.0))
    deltas = tracker.deltas("orders")
    assert len(deltas) == 1
    assert deltas[0].delta == pytest.approx(20.0)


def test_tables_returns_sorted_names():
    tracker = MetricTracker()
    tracker.record(_snap("users", row_count=10.0))
    tracker.record(_snap("orders", row_count=5.0))
    assert tracker.tables() == ["orders", "users"]
