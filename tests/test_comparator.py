"""Tests for pipewarden.comparator and pipewarden.comparison_reporter."""

import pytest
from pipewarden.validator import ValidationResult, ValidationError
from pipewarden.comparator import TableComparison, compare_runs
from pipewarden.comparison_reporter import format_table_comparison, format_run_comparison, comparison_summary


def _ok_result(table="t") -> ValidationResult:
    return ValidationResult(table_name=table, errors=[])


def _fail_result(table="t", n=1) -> ValidationResult:
    errors = [ValidationError(row=i, field="f", message="bad") for i in range(n)]
    return ValidationResult(table_name=table, errors=errors)


# --- TableComparison ---

def test_error_delta_no_change():
    comp = TableComparison("t", _ok_result(), _ok_result())
    assert comp.error_delta == 0


def test_error_delta_regression():
    comp = TableComparison("t", _ok_result(), _fail_result(n=3))
    assert comp.error_delta == 3


def test_error_delta_improvement():
    comp = TableComparison("t", _fail_result(n=5), _fail_result(n=2))
    assert comp.error_delta == -3


def test_status_changed_pass_to_fail():
    comp = TableComparison("t", _ok_result(), _fail_result())
    assert comp.status_changed is True


def test_status_unchanged():
    comp = TableComparison("t", _ok_result(), _ok_result())
    assert comp.status_changed is False


def test_is_new_table():
    comp = TableComparison("t", None, _ok_result())
    assert comp.is_new is True
    assert comp.is_removed is False


def test_is_removed_table():
    comp = TableComparison("t", _ok_result(), None)
    assert comp.is_removed is True
    assert comp.is_new is False


# --- compare_runs ---

def test_compare_runs_all_matching():
    baseline = {"a": _ok_result("a"), "b": _fail_result("b", 2)}
    current = {"a": _ok_result("a"), "b": _fail_result("b", 2)}
    run = compare_runs(baseline, current)
    assert len(run.comparisons) == 2
    assert not run.has_regressions


def test_compare_runs_detects_regression():
    baseline = {"a": _ok_result("a")}
    current = {"a": _fail_result("a", 3)}
    run = compare_runs(baseline, current)
    assert run.has_regressions
    assert "a" in run.regressed_tables


def test_compare_runs_detects_improvement():
    baseline = {"a": _fail_result("a", 5)}
    current = {"a": _fail_result("a", 2)}
    run = compare_runs(baseline, current)
    assert "a" in run.improved_tables


def test_compare_runs_new_table_failing():
    run = compare_runs({}, {"x": _fail_result("x")})
    assert "x" in run.regressed_tables


# --- comparison_reporter ---

def test_format_table_comparison_no_change():
    comp = TableComparison("orders", _ok_result("orders"), _ok_result("orders"))
    out = format_table_comparison(comp)
    assert "orders" in out
    assert "no change" in out


def test_format_run_comparison_no_regressions():
    run = compare_runs({"a": _ok_result("a")}, {"a": _ok_result("a")})
    out = format_run_comparison(run)
    assert "No regressions" in out


def test_format_run_comparison_empty():
    from pipewarden.comparator import RunComparison
    out = format_run_comparison(RunComparison())
    assert "no tables" in out


def test_comparison_summary_counts():
    baseline = {"a": _ok_result("a"), "b": _fail_result("b", 3)}
    current = {"a": _fail_result("a", 1), "b": _fail_result("b", 1)}
    run = compare_runs(baseline, current)
    summary = comparison_summary(run)
    assert "2 table(s)" in summary
