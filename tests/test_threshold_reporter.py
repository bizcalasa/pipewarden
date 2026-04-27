"""Tests for pipewarden.threshold_reporter."""

from pipewarden.threshold import ThresholdReport, ThresholdViolation
from pipewarden.threshold_reporter import (
    format_violation,
    format_threshold_report,
    threshold_summary,
)


def _violation(table="orders", rule="max_error_count", msg="too many errors"):
    return ThresholdViolation(table=table, rule_name=rule, message=msg)


def test_format_violation_contains_table():
    text = format_violation(_violation(table="users"))
    assert "users" in text


def test_format_violation_contains_rule_name():
    text = format_violation(_violation(rule="max_error_rate"))
    assert "max_error_rate" in text


def test_format_violation_contains_message():
    text = format_violation(_violation(msg="exceeded limit"))
    assert "exceeded limit" in text


def test_format_report_passed_shows_ok():
    report = ThresholdReport()
    text = format_threshold_report(report)
    assert "passed" in text.lower()


def test_format_report_shows_all_violations():
    report = ThresholdReport()
    report.add(_violation(table="a"))
    report.add(_violation(table="b"))
    text = format_threshold_report(report)
    assert "a" in text
    assert "b" in text


def test_format_report_shows_violation_count():
    report = ThresholdReport()
    report.add(_violation())
    report.add(_violation())
    text = format_threshold_report(report)
    assert "2" in text


def test_threshold_summary_ok_when_no_violations():
    report = ThresholdReport()
    assert threshold_summary(report) == "thresholds: OK"


def test_threshold_summary_failed_singular():
    report = ThresholdReport()
    report.add(_violation())
    summary = threshold_summary(report)
    assert "FAILED" in summary
    assert "1 violation" in summary


def test_threshold_summary_failed_plural():
    report = ThresholdReport()
    report.add(_violation())
    report.add(_violation())
    summary = threshold_summary(report)
    assert "2 violations" in summary
