"""Tests for pipewarden.drift_detector."""

import json
import os
import pytest

from pipewarden.schema import TableSchema, FieldSchema, FieldType
from pipewarden.watcher import save_snapshot
from pipewarden.drift_detector import (
    detect_schema_drift,
    detect_drift_for_schemas,
    DriftReport,
    DriftSummary,
)


def _make_schema(name: str, fields=None) -> TableSchema:
    if fields is None:
        fields = [
            FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="name", field_type=FieldType.STRING, nullable=True),
        ]
    return TableSchema(name=name, fields=fields)


def test_no_drift_when_schema_unchanged(tmp_path):
    schema = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", schema)
    report = detect_schema_drift("orders", schema, str(tmp_path))
    assert report.has_drift is False
    assert report.snapshot_missing is False


def test_drift_detected_when_field_added(tmp_path):
    old_schema = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", old_schema)
    new_fields = old_schema.fields + [
        FieldSchema(name="email", field_type=FieldType.STRING, nullable=True)
    ]
    new_schema = TableSchema(name="orders", fields=new_fields)
    report = detect_schema_drift("orders", new_schema, str(tmp_path))
    assert report.has_drift is True
    assert "email" in (report.diff.added_fields if report.diff else [])


def test_drift_detected_when_field_removed(tmp_path):
    old_schema = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", old_schema)
    new_schema = TableSchema(
        name="orders",
        fields=[FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False)],
    )
    report = detect_schema_drift("orders", new_schema, str(tmp_path))
    assert report.has_drift is True
    assert "name" in (report.diff.removed_fields if report.diff else [])


def test_missing_snapshot_returns_flag(tmp_path):
    schema = _make_schema("users")
    report = detect_schema_drift("users", schema, str(tmp_path))
    assert report.snapshot_missing is True
    assert report.has_drift is False


def test_summary_has_correct_counts(tmp_path):
    s1 = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", s1)
    s2 = _make_schema("users")
    # no snapshot for users -> missing
    schemas = {"orders": s1, "users": s2}
    summary = detect_drift_for_schemas(schemas, str(tmp_path))
    assert summary.total == 2
    assert summary.clean == 1
    assert summary.missing_snapshots == 1
    assert summary.drifted == 0


def test_summary_counts_drifted(tmp_path):
    old = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", old)
    new_fields = old.fields + [
        FieldSchema(name="extra", field_type=FieldType.STRING, nullable=True)
    ]
    new = TableSchema(name="orders", fields=new_fields)
    summary = detect_drift_for_schemas({"orders": new}, str(tmp_path))
    assert summary.drifted == 1


def test_drift_report_summary_string_no_drift(tmp_path):
    schema = _make_schema("orders")
    save_snapshot(str(tmp_path), "orders", schema)
    report = detect_schema_drift("orders", schema, str(tmp_path))
    assert "No drift" in report.summary()


def test_drift_report_summary_string_missing(tmp_path):
    schema = _make_schema("orders")
    report = detect_schema_drift("orders", schema, str(tmp_path))
    assert "No snapshot" in report.summary()
