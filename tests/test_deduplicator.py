"""Tests for pipewarden.deduplicator."""

import pytest

from pipewarden.deduplicator import (
    DeduplicationReport,
    DuplicateGroup,
    detect_duplicates,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _rows_unique():
    return [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
    ]


def _rows_with_dupes():
    return [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 1, "name": "alice"},   # duplicate of index 0
        {"id": 3, "name": "carol"},
        {"id": 2, "name": "bob"},    # duplicate of index 1
        {"id": 2, "name": "bob"},    # another duplicate of index 1
    ]


# ---------------------------------------------------------------------------
# DuplicateGroup
# ---------------------------------------------------------------------------

def test_duplicate_group_count():
    g = DuplicateGroup(key=(1,), row_indices=[0, 2])
    assert g.count == 2


# ---------------------------------------------------------------------------
# DeduplicationReport
# ---------------------------------------------------------------------------

def test_report_no_duplicates_has_duplicates_false():
    report = detect_duplicates("orders", _rows_unique(), key_fields=["id"])
    assert not report.has_duplicates


def test_report_no_duplicates_duplicate_row_count_zero():
    report = detect_duplicates("orders", _rows_unique(), key_fields=["id"])
    assert report.duplicate_row_count == 0


def test_report_stores_table_name():
    report = detect_duplicates("users", _rows_unique(), key_fields=["id"])
    assert report.table == "users"


def test_report_stores_total_rows():
    report = detect_duplicates("users", _rows_unique(), key_fields=["id"])
    assert report.total_rows == 3


# ---------------------------------------------------------------------------
# detect_duplicates — basic detection
# ---------------------------------------------------------------------------

def test_detects_duplicate_groups():
    report = detect_duplicates("orders", _rows_with_dupes(), key_fields=["id"])
    assert report.has_duplicates
    assert len(report.duplicate_groups) == 2  # id=1 and id=2


def test_duplicate_row_count_excludes_first_occurrence():
    report = detect_duplicates("orders", _rows_with_dupes(), key_fields=["id"])
    # id=1 has 2 rows → 1 extra; id=2 has 3 rows → 2 extras
    assert report.duplicate_row_count == 3


def test_get_group_returns_correct_group():
    report = detect_duplicates("orders", _rows_with_dupes(), key_fields=["id"])
    group = report.get_group((1,))
    assert group is not None
    assert group.count == 2


def test_get_group_returns_none_for_missing_key():
    report = detect_duplicates("orders", _rows_with_dupes(), key_fields=["id"])
    assert report.get_group((99,)) is None


# ---------------------------------------------------------------------------
# detect_duplicates — composite keys
# ---------------------------------------------------------------------------

def test_composite_key_no_duplicates_when_combined_unique():
    rows = [
        {"date": "2024-01-01", "region": "us", "val": 10},
        {"date": "2024-01-01", "region": "eu", "val": 20},
        {"date": "2024-01-02", "region": "us", "val": 30},
    ]
    report = detect_duplicates("sales", rows, key_fields=["date", "region"])
    assert not report.has_duplicates


def test_composite_key_detects_duplicate():
    rows = [
        {"date": "2024-01-01", "region": "us", "val": 10},
        {"date": "2024-01-01", "region": "us", "val": 99},  # duplicate key
    ]
    report = detect_duplicates("sales", rows, key_fields=["date", "region"])
    assert report.has_duplicates
    assert report.duplicate_row_count == 1


# ---------------------------------------------------------------------------
# detect_duplicates — edge cases
# ---------------------------------------------------------------------------

def test_empty_rows_returns_clean_report():
    report = detect_duplicates("empty", [], key_fields=["id"])
    assert not report.has_duplicates
    assert report.total_rows == 0


def test_empty_key_fields_raises():
    with pytest.raises(ValueError, match="key_fields"):
        detect_duplicates("t", _rows_unique(), key_fields=[])


def test_missing_field_treated_as_none_and_grouped():
    rows = [
        {"id": 1},
        {"name": "x"},   # id missing → key is (None,)
        {"name": "y"},   # id missing → key is (None,)
    ]
    report = detect_duplicates("t", rows, key_fields=["id"])
    assert report.has_duplicates
    group = report.get_group((None,))
    assert group is not None and group.count == 2
