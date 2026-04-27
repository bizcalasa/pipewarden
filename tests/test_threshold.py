"""Tests for pipewarden.threshold."""

import pytest
from pipewarden.threshold import (
    ThresholdRule,
    ThresholdViolation,
    ThresholdReport,
    ThresholdEngine,
    max_error_rate_rule,
    max_error_count_rule,
)
from pipewarden.validator import ValidationResult, ValidationError


def _make_result(errors: int = 0, row_count: int = 10) -> ValidationResult:
    result = ValidationResult()
    result.row_count = row_count
    for i in range(errors):
        result.add_error(ValidationError(row=i, field="f", message="bad"))
    return result


# --- ThresholdRule ---

def test_rule_fires_when_predicate_true():
    rule = ThresholdRule("always", lambda r: True, "always fires")
    assert rule.check(_make_result()) is True


def test_rule_does_not_fire_when_predicate_false():
    rule = ThresholdRule("never", lambda r: False, "never fires")
    assert rule.check(_make_result()) is False


def test_rule_handles_exception_gracefully():
    def _boom(r):
        raise RuntimeError("oops")
    rule = ThresholdRule("boom", _boom, "explodes")
    assert rule.check(_make_result()) is False


# --- ThresholdViolation ---

def test_violation_str_contains_table_and_rule():
    v = ThresholdViolation(table="orders", rule_name="max_error_count", message="too many")
    text = str(v)
    assert "orders" in text
    assert "max_error_count" in text


# --- ThresholdReport ---

def test_empty_report_passes():
    report = ThresholdReport()
    assert report.passed is True
    assert report.violation_count == 0


def test_report_with_violation_fails():
    report = ThresholdReport()
    report.add(ThresholdViolation("t", "r", "msg"))
    assert report.passed is False
    assert report.violation_count == 1


# --- ThresholdEngine ---

def test_engine_no_rules_passes():
    engine = ThresholdEngine()
    result = _make_result(errors=5)
    report = engine.evaluate("table", result)
    assert report.passed is True


def test_engine_detects_violation():
    engine = ThresholdEngine()
    engine.add_rule(max_error_count_rule(2))
    result = _make_result(errors=5)
    report = engine.evaluate("orders", result)
    assert report.passed is False
    assert report.violations[0].table == "orders"


def test_engine_multiple_rules_all_checked():
    engine = ThresholdEngine()
    engine.add_rule(max_error_count_rule(1))
    engine.add_rule(max_error_rate_rule(0.1))
    result = _make_result(errors=5, row_count=10)
    report = engine.evaluate("t", result)
    assert report.violation_count == 2


# --- Built-in rules ---

def test_max_error_count_passes_when_under():
    rule = max_error_count_rule(10)
    assert rule.check(_make_result(errors=5)) is False


def test_max_error_count_fails_when_over():
    rule = max_error_count_rule(3)
    assert rule.check(_make_result(errors=5)) is True


def test_max_error_rate_passes_when_under():
    rule = max_error_rate_rule(0.5)
    assert rule.check(_make_result(errors=4, row_count=10)) is False


def test_max_error_rate_fails_when_over():
    rule = max_error_rate_rule(0.3)
    assert rule.check(_make_result(errors=5, row_count=10)) is True


def test_max_error_rate_zero_rows_passes():
    rule = max_error_rate_rule(0.0)
    assert rule.check(_make_result(errors=0, row_count=0)) is False
