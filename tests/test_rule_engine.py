"""Tests for pipewarden.rule_engine."""

import pytest

from pipewarden.rule_engine import (
    Rule,
    RuleSet,
    RuleViolation,
    apply_rules,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _positive_rule() -> Rule:
    return Rule(
        name="positive",
        field_name="amount",
        predicate=lambda v: v is not None and v > 0,
        message="amount must be positive",
    )


def _nonempty_rule() -> Rule:
    return Rule(
        name="nonempty",
        field_name="name",
        predicate=lambda v: bool(v),
        message="name must not be empty",
    )


# ---------------------------------------------------------------------------
# Rule.check
# ---------------------------------------------------------------------------

def test_rule_passes_when_predicate_true():
    rule = _positive_rule()
    assert rule.check(10) is None


def test_rule_fails_when_predicate_false():
    rule = _positive_rule()
    assert rule.check(-1) == "amount must be positive"


def test_rule_handles_exception_gracefully():
    bad_rule = Rule(
        name="boom",
        field_name="x",
        predicate=lambda v: 1 / 0,  # always raises
        message="unreachable",
    )
    result = bad_rule.check(5)
    assert result is not None
    assert "boom" in result
    assert "exception" in result.lower()


# ---------------------------------------------------------------------------
# RuleSet
# ---------------------------------------------------------------------------

def test_ruleset_rules_for_filters_by_field():
    rs = RuleSet()
    rs.add(_positive_rule())
    rs.add(_nonempty_rule())
    assert len(rs.rules_for("amount")) == 1
    assert len(rs.rules_for("name")) == 1
    assert rs.rules_for("missing") == []


# ---------------------------------------------------------------------------
# RuleViolation.__str__
# ---------------------------------------------------------------------------

def test_violation_str_contains_key_info():
    v = RuleViolation(row_index=3, field_name="amount", rule_name="positive", message="must be positive")
    s = str(v)
    assert "Row 3" in s
    assert "amount" in s
    assert "positive" in s


# ---------------------------------------------------------------------------
# apply_rules
# ---------------------------------------------------------------------------

def test_apply_rules_no_violations():
    rs = RuleSet()
    rs.add(_positive_rule())
    rows = [{"amount": 5}, {"amount": 100}]
    assert apply_rules(rows, rs) == []


def test_apply_rules_detects_violation():
    rs = RuleSet()
    rs.add(_positive_rule())
    rows = [{"amount": 5}, {"amount": -3}]
    violations = apply_rules(rows, rs)
    assert len(violations) == 1
    assert violations[0].row_index == 1
    assert violations[0].rule_name == "positive"


def test_apply_rules_multiple_fields():
    rs = RuleSet()
    rs.add(_positive_rule())
    rs.add(_nonempty_rule())
    rows = [{"amount": -1, "name": ""}]
    violations = apply_rules(rows, rs)
    assert len(violations) == 2


def test_apply_rules_missing_field_treated_as_none():
    rs = RuleSet()
    rs.add(_positive_rule())
    rows = [{"other": 99}]  # 'amount' key absent
    violations = apply_rules(rows, rs)
    assert len(violations) == 1
    assert violations[0].field_name == "amount"
