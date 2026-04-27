"""Tests for pipewarden.expectation and pipewarden.expectation_reporter."""

import pytest

from pipewarden.expectation import (
    ExpectationRule,
    ExpectationSet,
    ExpectationViolation,
    ExpectationReport,
)
from pipewarden.expectation_reporter import (
    format_violation,
    format_expectation_report,
    expectation_summary,
)


def _positive_rule() -> ExpectationRule:
    return ExpectationRule(
        name="always_pass",
        description="Always passes",
        predicate=lambda row: True,
    )


def _negative_rule() -> ExpectationRule:
    return ExpectationRule(
        name="always_fail",
        description="Always fails",
        predicate=lambda row: False,
    )


def _age_rule() -> ExpectationRule:
    return ExpectationRule(
        name="age_positive",
        description="Age must be positive",
        predicate=lambda row: row.get("age", 0) > 0,
    )


def test_rule_passes_when_predicate_true():
    rule = _positive_rule()
    assert rule.check({"x": 1}) is True


def test_rule_fails_when_predicate_false():
    rule = _negative_rule()
    assert rule.check({"x": 1}) is False


def test_rule_handles_exception_gracefully():
    rule = ExpectationRule(
        name="boom",
        description="Raises",
        predicate=lambda row: 1 / 0,
    )
    assert rule.check({}) is False


def test_expectation_set_evaluate_no_violations():
    es = ExpectationSet(table="users")
    es.add(_positive_rule())
    report = es.evaluate([{"id": 1}, {"id": 2}])
    assert report.passed is True
    assert len(report.violations) == 0


def test_expectation_set_evaluate_detects_violations():
    es = ExpectationSet(table="users")
    es.add(_negative_rule())
    report = es.evaluate([{"id": 1}, {"id": 2}])
    assert report.passed is False
    assert len(report.violations) == 2


def test_violation_row_index_is_correct():
    es = ExpectationSet(table="orders")
    es.add(_age_rule())
    rows = [{"age": 5}, {"age": -1}, {"age": 10}]
    report = es.evaluate(rows)
    assert len(report.violations) == 1
    assert report.violations[0].row_index == 1


def test_violation_str_contains_rule_name_and_index():
    v = ExpectationViolation(rule_name="age_positive", row_index=3, row={})
    s = str(v)
    assert "age_positive" in s
    assert "3" in s


def test_format_violation_contains_row_and_rule():
    v = ExpectationViolation(rule_name="not_null", row_index=7, row={})
    out = format_violation(v)
    assert "7" in out
    assert "not_null" in out


def test_format_report_passed():
    report = ExpectationReport(table="events")
    out = format_expectation_report(report)
    assert "OK" in out
    assert "events" in out


def test_format_report_failed_shows_count():
    report = ExpectationReport(table="events")
    report.add_violation(ExpectationViolation("r", 0, {}))
    report.add_violation(ExpectationViolation("r", 1, {}))
    out = format_expectation_report(report)
    assert "FAILED" in out
    assert "2" in out


def test_expectation_summary_all_pass():
    reports = [ExpectationReport(table="a"), ExpectationReport(table="b")]
    out = expectation_summary(reports)
    assert "2" in out
    assert "passed" in out


def test_expectation_summary_empty():
    out = expectation_summary([])
    assert "No" in out
