"""Tests for pipewarden.checksum_reporter."""

from __future__ import annotations

from pipewarden.checksum import ChecksumRecord, ChecksumReport
from pipewarden.checksum_reporter import (
    checksum_summary,
    format_checksum_report,
    format_record,
)


def _record(
    table: str = "orders",
    schema_cs: str = "a" * 64,
    data_cs: str | None = None,
) -> ChecksumRecord:
    return ChecksumRecord(table=table, schema_checksum=schema_cs, data_checksum=data_cs)


def test_format_record_contains_table_name():
    out = format_record(_record(table="sales"))
    assert "sales" in out


def test_format_record_shows_schema_checksum_prefix():
    out = format_record(_record(schema_cs="abcdef1234567890" + "x" * 48))
    assert "abcdef1234567890" in out


def test_format_record_no_data_checksum_shows_na():
    out = format_record(_record(data_cs=None))
    assert "n/a" in out


def test_format_record_with_data_checksum_shows_prefix():
    out = format_record(_record(data_cs="deadbeef" + "0" * 56))
    assert "deadbeef" in out


def test_format_report_empty():
    report = ChecksumReport()
    out = format_checksum_report(report)
    assert "no entries" in out


def test_format_report_contains_header():
    report = ChecksumReport()
    report.add(_record())
    out = format_checksum_report(report)
    assert "Checksum Report" in out


def test_format_report_contains_table_name():
    report = ChecksumReport()
    report.add(_record(table="invoices"))
    out = format_checksum_report(report)
    assert "invoices" in out


def test_checksum_summary_counts_tables():
    report = ChecksumReport()
    report.add(_record(table="a"))
    report.add(_record(table="b"))
    out = checksum_summary(report)
    assert "2" in out


def test_checksum_summary_counts_data_checksums():
    report = ChecksumReport()
    report.add(_record(table="a", data_cs="x" * 64))
    report.add(_record(table="b", data_cs=None))
    out = checksum_summary(report)
    assert "1" in out
