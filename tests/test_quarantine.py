"""Tests for pipewarden.quarantine and pipewarden.quarantine_reporter."""
import pytest

from pipewarden.quarantine import (
    QuarantinedRow,
    QuarantineReport,
    quarantine_from_runner,
)
from pipewarden.quarantine_reporter import (
    format_quarantined_row,
    format_quarantine_report,
    quarantine_summary,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeError:
    """Minimal stand-in for ValidationError."""
    def __init__(self, row_index: int, field: str, message: str):
        self.row_index = row_index
        self.field = field
        self.message = message

    def __str__(self):
        return f"row {self.row_index}, field '{self.field}': {self.message}"


# ---------------------------------------------------------------------------
# QuarantinedRow
# ---------------------------------------------------------------------------

def test_quarantined_row_stores_data():
    qr = QuarantinedRow(row_index=3, row={"a": 1}, reasons=["bad type"])
    assert qr.row_index == 3
    assert qr.row == {"a": 1}
    assert qr.reasons == ["bad type"]


# ---------------------------------------------------------------------------
# QuarantineReport
# ---------------------------------------------------------------------------

def test_report_starts_clean():
    report = QuarantineReport(table_name="users")
    assert report.is_clean
    assert report.count == 0
    assert report.quarantined_rows == []


def test_report_add_increases_count():
    report = QuarantineReport(table_name="users")
    report.add(0, {"id": None}, ["null not allowed"])
    assert report.count == 1
    assert not report.is_clean


def test_report_add_multiple_rows():
    report = QuarantineReport(table_name="orders")
    report.add(0, {"id": None}, ["null not allowed"])
    report.add(2, {"id": "x"}, ["wrong type"])
    assert report.count == 2
    assert len(report.quarantined_rows) == 2


def test_report_quarantined_rows_returns_copy():
    report = QuarantineReport(table_name="t")
    report.add(0, {}, ["err"])
    rows = report.quarantined_rows
    rows.clear()
    assert report.count == 1  # original unaffected


# ---------------------------------------------------------------------------
# quarantine_from_runner
# ---------------------------------------------------------------------------

def test_quarantine_from_runner_no_errors_is_clean():
    rows = [{"id": 1}, {"id": 2}]
    report = quarantine_from_runner("users", rows, [])
    assert report.is_clean


def test_quarantine_from_runner_groups_errors_by_row():
    rows = [{"id": None, "name": 99}, {"id": 1, "name": "Alice"}]
    errors = [
        _FakeError(0, "id", "null not allowed"),
        _FakeError(0, "name", "expected string"),
    ]
    report = quarantine_from_runner("users", rows, errors)
    assert report.count == 1
    assert len(report.quarantined_rows[0].reasons) == 2


def test_quarantine_from_runner_multiple_rows():
    rows = [{"id": None}, {"id": 1}, {"id": None}]
    errors = [_FakeError(0, "id", "null"), _FakeError(2, "id", "null")]
    report = quarantine_from_runner("tbl", rows, errors)
    assert report.count == 2


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

def test_format_quarantined_row_contains_index_and_reason():
    qr = QuarantinedRow(row_index=5, row={}, reasons=["null not allowed"])
    out = format_quarantined_row(qr)
    assert "5" in out
    assert "null not allowed" in out


def test_format_report_clean_shows_no_rows():
    report = QuarantineReport(table_name="clean_table")
    out = format_quarantine_report(report)
    assert "No rows quarantined" in out
    assert "clean_table" in out


def test_format_report_shows_count_and_reasons():
    report = QuarantineReport(table_name="dirty")
    report.add(1, {}, ["bad value"])
    out = format_quarantine_report(report)
    assert "1 row quarantined" in out
    assert "bad value" in out


def test_quarantine_summary_clean():
    report = QuarantineReport(table_name="t")
    s = quarantine_summary(report)
    assert "clean" in s
    assert "t" in s


def test_quarantine_summary_with_violations():
    report = QuarantineReport(table_name="t")
    report.add(0, {}, ["err"])
    report.add(1, {}, ["err2"])
    s = quarantine_summary(report)
    assert "2 rows quarantined" in s
