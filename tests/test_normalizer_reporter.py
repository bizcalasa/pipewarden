"""Tests for pipewarden.normalizer_reporter."""
from pipewarden.normalizer import NormalizationReport
from pipewarden.normalizer_reporter import (
    format_normalization_report,
    normalization_summary,
)


def _report(table: str = "users", rows: int = 5, fields: dict | None = None) -> NormalizationReport:
    r = NormalizationReport(table=table)
    r.rows_processed = rows
    for fname, count in (fields or {}).items():
        for _ in range(count):
            r.record(fname)
    return r


def test_format_report_contains_table_name():
    out = format_normalization_report(_report("orders"))
    assert "orders" in out


def test_format_report_shows_rows_processed():
    out = format_normalization_report(_report(rows=10))
    assert "10" in out


def test_format_report_shows_per_field_counts():
    out = format_normalization_report(_report(fields={"email": 3}))
    assert "email" in out
    assert "3" in out


def test_format_report_no_transformations_message():
    out = format_normalization_report(_report())
    assert "No fields were transformed" in out


def test_format_report_singular_change_label():
    out = format_normalization_report(_report(fields={"name": 1}))
    assert "1 change" in out


def test_format_report_plural_changes_label():
    out = format_normalization_report(_report(fields={"name": 4}))
    assert "4 changes" in out


def test_summary_contains_row_count():
    reports = [_report(rows=7), _report(rows=3)]
    out = normalization_summary(reports)
    assert "10" in out


def test_summary_contains_table_count():
    reports = [_report(), _report()]
    out = normalization_summary(reports)
    assert "2" in out


def test_summary_contains_transformation_count():
    reports = [_report(fields={"a": 2}), _report(fields={"b": 3})]
    out = normalization_summary(reports)
    assert "5" in out


def test_summary_singular_table_label():
    out = normalization_summary([_report()])
    assert "1 table" in out


def test_summary_plural_rows_label():
    out = normalization_summary([_report(rows=5)])
    assert "5 rows" in out
