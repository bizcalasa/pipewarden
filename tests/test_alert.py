"""Tests for pipewarden.alert and pipewarden.alert_reporter."""

import pytest
from pipewarden.validator import ValidationResult, ValidationError
from pipewarden.alert import (
    AlertRule,
    Alert,
    AlertEngine,
    error_threshold_rule,
    any_failure_rule,
)
from pipewarden.alert_reporter import format_alert, format_alert_block, alert_summary


def _ok_result() -> ValidationResult:
    return ValidationResult(errors=[])


def _fail_result(n: int = 1) -> ValidationResult:
    errors = [
        ValidationError(row=i, field="f", message="bad", expected="str", actual="int")
        for i in range(n)
    ]
    return ValidationResult(errors=errors)


# --- AlertRule ---

def test_rule_matches_when_predicate_true():
    rule = AlertRule("r", "desc", predicate=lambda r: True)
    assert rule.matches(_ok_result()) is True


def test_rule_does_not_match_when_predicate_false():
    rule = AlertRule("r", "desc", predicate=lambda r: False)
    assert rule.matches(_ok_result()) is False


def test_rule_handles_exception_gracefully():
    rule = AlertRule("r", "desc", predicate=lambda r: 1 / 0)
    assert rule.matches(_ok_result()) is False


# --- Built-in rules ---

def test_any_failure_rule_fires_on_failure():
    rule = any_failure_rule()
    assert rule.matches(_fail_result()) is True


def test_any_failure_rule_silent_on_ok():
    rule = any_failure_rule()
    assert rule.matches(_ok_result()) is False


def test_error_threshold_rule_fires_above_threshold():
    rule = error_threshold_rule("thresh", max_errors=2)
    assert rule.matches(_fail_result(n=3)) is True


def test_error_threshold_rule_silent_at_threshold():
    rule = error_threshold_rule("thresh", max_errors=2)
    assert rule.matches(_fail_result(n=2)) is False


# --- AlertEngine ---

def test_engine_returns_no_alerts_for_ok_result():
    engine = AlertEngine()
    engine.add_rule(any_failure_rule())
    alerts = engine.evaluate("orders", _ok_result())
    assert alerts == []


def test_engine_returns_alert_for_failed_result():
    engine = AlertEngine()
    engine.add_rule(any_failure_rule())
    alerts = engine.evaluate("orders", _fail_result())
    assert len(alerts) == 1
    assert alerts[0].table == "orders"
    assert alerts[0].rule_name == "any_failure"


def test_engine_multiple_rules_all_evaluated():
    engine = AlertEngine()
    engine.add_rule(any_failure_rule())
    engine.add_rule(error_threshold_rule("high", max_errors=0))
    alerts = engine.evaluate("users", _fail_result(n=2))
    assert len(alerts) == 2


# --- Alert.__str__ ---

def test_alert_str_format():
    a = Alert(rule_name="any_failure", table="orders", message="errors detected")
    assert "ALERT" in str(a)
    assert "orders" in str(a)


# --- alert_reporter ---

def test_format_alert_contains_rule_and_table():
    a = Alert(rule_name="thresh", table="payments", message="too many errors")
    out = format_alert(a)
    assert "thresh" in out
    assert "payments" in out


def test_format_alert_block_no_alerts():
    assert format_alert_block([]) == "No alerts fired."


def test_format_alert_block_with_alerts():
    alerts = [
        Alert("r1", "t1", "m1"),
        Alert("r2", "t2", "m2"),
    ]
    out = format_alert_block(alerts)
    assert "2" in out
    assert "r1" in out


def test_alert_summary_ok():
    assert "0 alerts" in alert_summary([])


def test_alert_summary_with_alerts():
    alerts = [Alert("r", "orders", "m"), Alert("r", "users", "m")]
    out = alert_summary(alerts)
    assert "2 alert" in out
    assert "orders" in out
    assert "users" in out
