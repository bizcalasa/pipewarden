"""Tests for pipewarden.partition and pipewarden.partition_reporter."""
import pytest

from pipewarden.partition import (
    PartitionKey,
    PartitionReport,
    PartitionViolation,
    validate_partitions,
)
from pipewarden.partition_reporter import (
    format_partition_report,
    format_violation,
    partition_summary,
)


# ---------------------------------------------------------------------------
# PartitionKey
# ---------------------------------------------------------------------------

def test_partition_key_any_value_accepts_non_none():
    pk = PartitionKey(name="region")
    assert pk.validate("us-east-1") is True


def test_partition_key_any_value_rejects_none():
    pk = PartitionKey(name="region")
    assert pk.validate(None) is False


def test_partition_key_expected_values_accepts_valid():
    pk = PartitionKey(name="env", expected_values=["prod", "staging"])
    assert pk.validate("prod") is True


def test_partition_key_expected_values_rejects_unknown():
    pk = PartitionKey(name="env", expected_values=["prod", "staging"])
    assert pk.validate("dev") is False


# ---------------------------------------------------------------------------
# PartitionViolation.__str__
# ---------------------------------------------------------------------------

def test_violation_str_contains_row_and_key():
    v = PartitionViolation(row_index=3, key="region", value="xx", reason="unexpected value; allowed: ['us']")
    text = str(v)
    assert "Row 3" in text
    assert "region" in text
    assert "xx" in text


# ---------------------------------------------------------------------------
# validate_partitions
# ---------------------------------------------------------------------------

def test_valid_rows_produce_no_violations():
    keys = [PartitionKey(name="env", expected_values=["prod"])]
    rows = [{"env": "prod", "val": 1}, {"env": "prod", "val": 2}]
    report = validate_partitions("orders", rows, keys)
    assert report.is_valid
    assert report.violations == []


def test_missing_key_produces_violation():
    keys = [PartitionKey(name="env", expected_values=["prod"])]
    rows = [{"val": 1}]  # 'env' absent
    report = validate_partitions("orders", rows, keys)
    assert not report.is_valid
    assert report.violations[0].reason == "missing partition key"


def test_unexpected_value_produces_violation():
    keys = [PartitionKey(name="env", expected_values=["prod"])]
    rows = [{"env": "dev"}]
    report = validate_partitions("orders", rows, keys)
    assert len(report.violations) == 1
    assert "unexpected value" in report.violations[0].reason


def test_multiple_keys_multiple_violations():
    keys = [
        PartitionKey(name="env", expected_values=["prod"]),
        PartitionKey(name="region", expected_values=["us"]),
    ]
    rows = [{"env": "dev", "region": "eu"}]
    report = validate_partitions("sales", rows, keys)
    assert len(report.violations) == 2


def test_report_summary_ok():
    report = PartitionReport(table="users")
    assert "valid" in report.summary()


def test_report_summary_fail():
    report = PartitionReport(table="users")
    report.add(PartitionViolation(row_index=0, key="env", value="x", reason="bad"))
    assert "1 partition violation" in report.summary()


# ---------------------------------------------------------------------------
# partition_reporter
# ---------------------------------------------------------------------------

def test_format_violation_contains_key():
    v = PartitionViolation(row_index=1, key="env", value="dev", reason="unexpected")
    assert "env" in format_violation(v)


def test_format_report_valid_shows_ok():
    report = PartitionReport(table="events")
    text = format_partition_report(report)
    assert "OK" in text
    assert "No partition violations" in text


def test_format_report_invalid_shows_failed():
    report = PartitionReport(table="events")
    report.add(PartitionViolation(row_index=0, key="env", value="x", reason="bad"))
    text = format_partition_report(report)
    assert "FAILED" in text


def test_partition_summary_all_pass():
    reports = [PartitionReport(table="a"), PartitionReport(table="b")]
    assert "passed" in partition_summary(reports)


def test_partition_summary_some_fail():
    r = PartitionReport(table="a")
    r.add(PartitionViolation(row_index=0, key="k", value=None, reason="missing partition key"))
    reports = [r, PartitionReport(table="b")]
    summary = partition_summary(reports)
    assert "1/2" in summary
