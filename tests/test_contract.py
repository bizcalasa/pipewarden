"""Tests for pipewarden.contract module."""

import pytest
from pipewarden.contract import ContractRule, ContractViolation, ContractReport, DataContract


def _positive_rule():
    return ContractRule(name="always_pass", description="always true", predicate=lambda row: True)


def _negative_rule():
    return ContractRule(name="always_fail", description="always false", predicate=lambda row: False)


def _boom_rule():
    def _raise(row):
        raise RuntimeError("boom")
    return ContractRule(name="boom", description="raises", predicate=_raise)


def test_rule_passes_when_predicate_true():
    rule = _positive_rule()
    assert rule.check({"x": 1}) is True


def test_rule_fails_when_predicate_false():
    rule = _negative_rule()
    assert rule.check({"x": 1}) is False


def test_rule_handles_exception_gracefully():
    rule = _boom_rule()
    assert rule.check({}) is False


def test_violation_str_contains_table_and_rule():
    v = ContractViolation(table="orders", rule_name="not_null", row_index=3, description="field is null")
    s = str(v)
    assert "orders" in s
    assert "not_null" in s
    assert "3" in s


def test_report_passed_when_no_violations():
    report = ContractReport(table="users", total_rows=10)
    assert report.passed is True
    assert report.violation_count == 0


def test_report_failed_when_violations_added():
    report = ContractReport(table="users", total_rows=10)
    report.add_violation(
        ContractViolation(table="users", rule_name="r", row_index=0, description="d")
    )
    assert report.passed is False
    assert report.violation_count == 1


def test_contract_enforce_no_violations():
    contract = DataContract(table="orders")
    contract.add_rule(_positive_rule())
    rows = [{"id": 1}, {"id": 2}]
    report = contract.enforce(rows)
    assert report.passed
    assert report.total_rows == 2


def test_contract_enforce_produces_violations():
    contract = DataContract(table="orders")
    contract.add_rule(_negative_rule())
    rows = [{"id": 1}, {"id": 2}]
    report = contract.enforce(rows)
    assert not report.passed
    assert report.violation_count == 2


def test_contract_enforce_multiple_rules():
    contract = DataContract(table="t")
    contract.add_rule(_positive_rule())
    contract.add_rule(_negative_rule())
    rows = [{"a": 1}]
    report = contract.enforce(rows)
    assert report.violation_count == 1


def test_contract_enforce_empty_rows():
    contract = DataContract(table="t")
    contract.add_rule(_negative_rule())
    report = contract.enforce([])
    assert report.passed
    assert report.total_rows == 0
