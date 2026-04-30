"""Tests for pipewarden.scorer."""

import pytest

from pipewarden.scorer import (
    TableScore,
    ScoringReport,
    score_result,
    score_results,
)
from pipewarden.validator import ValidationResult, ValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok_result() -> ValidationResult:
    return ValidationResult()


def _fail_result(n: int = 3) -> ValidationResult:
    r = ValidationResult()
    for i in range(n):
        r.add_error(ValidationError(row=i, field="x", message="bad"))
    return r


# ---------------------------------------------------------------------------
# TableScore
# ---------------------------------------------------------------------------

def test_score_perfect():
    ts = TableScore(table_name="t", total_rows=10, total_checks=10, failed_checks=0)
    assert ts.score == 100.0
    assert ts.grade == "A"


def test_score_zero_checks_is_100():
    ts = TableScore(table_name="t", total_rows=0, total_checks=0, failed_checks=0)
    assert ts.score == 100.0


def test_score_partial():
    ts = TableScore(table_name="t", total_rows=10, total_checks=10, failed_checks=5)
    assert ts.score == 50.0
    assert ts.grade == "D"


def test_grade_f():
    ts = TableScore(table_name="t", total_rows=10, total_checks=10, failed_checks=9)
    assert ts.grade == "F"


def test_grade_b():
    ts = TableScore(table_name="t", total_rows=100, total_checks=100, failed_checks=15)
    assert ts.grade == "B"


def test_passed_checks_computed():
    ts = TableScore(table_name="t", total_rows=10, total_checks=10, failed_checks=3)
    assert ts.passed_checks == 7


# ---------------------------------------------------------------------------
# ScoringReport
# ---------------------------------------------------------------------------

def test_report_starts_empty():
    r = ScoringReport()
    assert r.scores == []
    assert r.overall_score == 100.0


def test_report_add_and_get():
    ts = TableScore("orders", 5, 5, 0)
    r = ScoringReport()
    r.add(ts)
    assert r.get("orders") is ts


def test_report_get_missing_returns_none():
    r = ScoringReport()
    assert r.get("missing") is None


def test_overall_score_weighted():
    r = ScoringReport()
    r.add(TableScore("a", 10, 10, 0))   # 10 passed
    r.add(TableScore("b", 10, 10, 10))  # 0 passed
    assert r.overall_score == 50.0


# ---------------------------------------------------------------------------
# score_result / score_results
# ---------------------------------------------------------------------------

def test_score_result_no_errors():
    ts = score_result("users", _ok_result(), total_rows=20)
    assert ts.table_name == "users"
    assert ts.total_rows == 20
    assert ts.failed_checks == 0
    assert ts.score == 100.0


def test_score_result_with_errors():
    ts = score_result("users", _fail_result(5), total_rows=20)
    assert ts.failed_checks == 5
    assert ts.score < 100.0


def test_score_results_builds_report():
    results = {
        "a": _ok_result(),
        "b": _fail_result(2),
    }
    row_counts = {"a": 10, "b": 10}
    report = score_results(results, row_counts)
    assert len(report.scores) == 2
    assert report.get("a") is not None
    assert report.get("b") is not None


def test_score_results_missing_row_count_defaults_to_zero():
    results = {"x": _fail_result(3)}
    report = score_results(results, {})
    ts = report.get("x")
    assert ts is not None
    assert ts.total_rows == 0
