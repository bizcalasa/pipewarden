"""Tests for pipewarden.profiler and pipewarden.profile_reporter."""

import pytest

from pipewarden.profiler import profile_rows, FieldProfile, ProfileReport
from pipewarden.profile_reporter import (
    format_field_profile,
    format_profile_report,
    profile_summary,
)
from pipewarden.schema import FieldSchema, FieldType, TableSchema


def _make_schema() -> TableSchema:
    return TableSchema(
        name="orders",
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=True),
            FieldSchema(name="label", type=FieldType.STRING, nullable=True),
        ],
    )


def test_profile_row_count():
    schema = _make_schema()
    rows = [{"id": 1, "amount": 9.99, "label": "A"}] * 5
    report = profile_rows(schema, rows)
    assert report.row_count == 5


def test_profile_null_count():
    schema = _make_schema()
    rows = [
        {"id": 1, "amount": None, "label": "A"},
        {"id": 2, "amount": 3.5, "label": None},
        {"id": 3, "amount": None, "label": None},
    ]
    report = profile_rows(schema, rows)
    assert report.fields["amount"].null_count == 2
    assert report.fields["label"].null_count == 2
    assert report.fields["id"].null_count == 0


def test_null_rate_calculation():
    schema = _make_schema()
    rows = [
        {"id": 1, "amount": None, "label": "x"},
        {"id": 2, "amount": None, "label": "y"},
        {"id": 3, "amount": 1.0, "label": "z"},
        {"id": 4, "amount": 2.0, "label": "w"},
    ]
    report = profile_rows(schema, rows)
    assert report.fields["amount"].null_rate == pytest.approx(0.5)


def test_dominant_type_detected():
    schema = _make_schema()
    rows = [
        {"id": 1, "amount": 1.0, "label": "a"},
        {"id": 2, "amount": 2.0, "label": "b"},
        {"id": 3, "amount": None, "label": "c"},
    ]
    report = profile_rows(schema, rows)
    assert report.fields["amount"].dominant_type == "float"


def test_empty_rows_gives_zero_counts():
    schema = _make_schema()
    report = profile_rows(schema, [])
    assert report.row_count == 0
    for fp in report.fields.values():
        assert fp.total_count == 0
        assert fp.null_rate == 0.0
        assert fp.dominant_type is None


def test_format_field_profile_contains_name():
    fp = FieldProfile(name="amount", total_count=4, null_count=2, type_counts={"float": 2})
    output = format_field_profile(fp)
    assert "amount" in output
    assert "50.0%" in output
    assert "float" in output


def test_format_profile_report_contains_table_name():
    schema = _make_schema()
    rows = [{"id": i, "amount": float(i), "label": "x"} for i in range(3)]
    report = profile_rows(schema, rows)
    output = format_profile_report(report)
    assert "orders" in output
    assert "Rows scanned" in output


def test_profile_summary_ok():
    schema = _make_schema()
    rows = [{"id": i, "amount": float(i), "label": "x"} for i in range(4)]
    report = profile_rows(schema, rows)
    summary = profile_summary(report)
    assert "OK" in summary


def test_profile_summary_warns_on_high_nulls():
    schema = _make_schema()
    rows = [
        {"id": 1, "amount": None, "label": None},
        {"id": 2, "amount": None, "label": None},
        {"id": 3, "amount": None, "label": None},
    ]
    report = profile_rows(schema, rows)
    summary = profile_summary(report)
    assert "WARNING" in summary
    assert "amount" in summary
