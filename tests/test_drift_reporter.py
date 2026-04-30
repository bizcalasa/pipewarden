"""Tests for pipewarden.drift_reporter."""

import pytest

from pipewarden.schema import TableSchema, FieldSchema, FieldType
from pipewarden.drift_detector import DriftReport, DriftSummary, detect_drift_for_schemas
from pipewarden.drift_reporter import format_drift_report, format_drift_summary, drift_summary_line
from pipewarden.watcher import save_snapshot
from pipewarden.differ import diff_schemas


def _make_schema(name, fields=None):
    if fields is None:
        fields = [FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False)]
    return TableSchema(name=name, fields=fields)


def test_format_report_no_drift():
    report = DriftReport(table_name="orders", has_drift=False)
    text = format_drift_report(report)
    assert "orders" in text
    assert "No drift" in text


def test_format_report_missing_snapshot():
    report = DriftReport(table_name="orders", has_drift=False, snapshot_missing=True)
    text = format_drift_report(report)
    assert "No snapshot" in text


def test_format_report_shows_added_field(tmp_path):
    old = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", old)
    new_fields = old.fields + [
        FieldSchema(name="email", field_type=FieldType.STRING, nullable=True)
    ]
    new = TableSchema(name="orders", fields=new_fields)
    diff = diff_schemas(old, new)
    report = DriftReport(table_name="orders", has_drift=True, diff=diff)
    text = format_drift_report(report)
    assert "email" in text
    assert "+" in text


def test_format_report_shows_removed_field():
    old_fields = [
        FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
        FieldSchema(name="name", field_type=FieldType.STRING, nullable=True),
    ]
    new_fields = [FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False)]
    old = TableSchema(name="orders", fields=old_fields)
    new = TableSchema(name="orders", fields=new_fields)
    diff = diff_schemas(old, new)
    report = DriftReport(table_name="orders", has_drift=True, diff=diff)
    text = format_drift_report(report)
    assert "name" in text
    assert "-" in text


def test_drift_summary_line_ok(tmp_path):
    schema = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", schema)
    summary = detect_drift_for_schemas({"orders": schema}, str(tmp_path))
    line = drift_summary_line(summary)
    assert "OK" in line
    assert "1 table" in line


def test_drift_summary_line_drift(tmp_path):
    old = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", old)
    new_fields = old.fields + [
        FieldSchema(name="extra", field_type=FieldType.STRING, nullable=True)
    ]
    new = TableSchema(name="orders", fields=new_fields)
    summary = detect_drift_for_schemas({"orders": new}, str(tmp_path))
    line = drift_summary_line(summary)
    assert "DRIFT DETECTED" in line


def test_format_drift_summary_multiple_tables(tmp_path):
    s1 = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", s1)
    s2 = _make_schema("users")
    summary = detect_drift_for_schemas({"orders": s1, "users": s2}, str(tmp_path))
    text = format_drift_summary(summary)
    assert "orders" in text
    assert "users" in text
