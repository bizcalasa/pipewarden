"""Tests for pipewarden.audit and pipewarden.audit_reporter."""

import json
from pathlib import Path

import pytest

from pipewarden.audit import (
    AuditEntry,
    AuditLog,
    build_entry,
    load_audit_log,
    save_audit_log,
)
from pipewarden.audit_reporter import (
    audit_summary,
    format_audit_log,
    format_entry,
    format_table_history,
)


def _entry(table="orders", is_valid=True, errors=0, rows=10, source=None):
    return AuditEntry(
        table=table,
        run_at="2024-01-01T00:00:00+00:00",
        is_valid=is_valid,
        error_count=errors,
        row_count=rows,
        source=source,
    )


def test_entry_to_dict_round_trips():
    e = _entry()
    assert AuditEntry.from_dict(e.to_dict()) == e


def test_build_entry_sets_fields():
    e = build_entry("users", is_valid=False, error_count=3, row_count=50, source="s3")
    assert e.table == "users"
    assert not e.is_valid
    assert e.error_count == 3
    assert e.row_count == 50
    assert e.source == "s3"
    assert e.run_at  # non-empty timestamp


def test_log_record_and_retrieve():
    log = AuditLog()
    e = _entry(table="orders")
    log.record(e)
    assert log.entries_for("orders") == [e]
    assert log.entries_for("missing") == []


def test_last_entry_returns_most_recent():
    log = AuditLog()
    e1 = _entry(rows=5)
    e2 = _entry(rows=20)
    log.record(e1)
    log.record(e2)
    assert log.last_entry("orders") is e2


def test_last_entry_none_for_unknown_table():
    log = AuditLog()
    assert log.last_entry("nope") is None


def test_table_names_unique(tmp_path):
    log = AuditLog()
    log.record(_entry(table="a"))
    log.record(_entry(table="b"))
    log.record(_entry(table="a"))
    assert sorted(log.table_names()) == ["a", "b"]


def test_save_and_load_round_trip(tmp_path):
    log = AuditLog()
    log.record(_entry(table="orders", rows=100))
    p = tmp_path / "audit.json"
    save_audit_log(log, p)
    loaded = load_audit_log(p)
    assert len(loaded.entries) == 1
    assert loaded.entries[0].row_count == 100


def test_load_missing_file_returns_empty(tmp_path):
    log = load_audit_log(tmp_path / "nonexistent.json")
    assert log.entries == []


def test_format_entry_ok():
    line = format_entry(_entry(is_valid=True, errors=0, rows=10))
    assert "[OK]" in line
    assert "orders" in line
    assert "rows=10" in line


def test_format_entry_fail():
    line = format_entry(_entry(is_valid=False, errors=5, rows=10))
    assert "[FAIL]" in line
    assert "errors=5" in line


def test_format_entry_includes_source():
    line = format_entry(_entry(source="gcs"))
    assert "[gcs]" in line


def test_format_audit_log_empty():
    assert "No audit" in format_audit_log(AuditLog())


def test_format_table_history_unknown():
    msg = format_table_history(AuditLog(), "missing")
    assert "missing" in msg


def test_audit_summary_counts():
    log = AuditLog()
    log.record(_entry(table="a", is_valid=True))
    log.record(_entry(table="b", is_valid=False))
    summary = audit_summary(log)
    assert "2 run(s)" in summary
    assert "1 passed" in summary
    assert "1 failed" in summary


def test_audit_summary_empty():
    assert "empty" in audit_summary(AuditLog())
