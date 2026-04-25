"""Tests for pipewarden.summarizer and pipewarden.summary_reporter."""

import pytest

from pipewarden.validator import ValidationError, ValidationResult
from pipewarden.summarizer import PipelineSummary, summarize
from pipewarden.summary_reporter import format_pipeline_summary, format_per_table_status


def _ok_result() -> ValidationResult:
    return ValidationResult(errors=[])


def _fail_result(n: int = 1) -> ValidationResult:
    errors = [
        ValidationError(row=i, field="col", message="bad") for i in range(n)
    ]
    return ValidationResult(errors=errors)


# --- PipelineSummary unit tests ---

def test_empty_summary_is_clean():
    s = PipelineSummary()
    assert s.is_clean
    assert s.total_tables == 0
    assert s.total_errors == 0


def test_add_result_increments_total():
    s = PipelineSummary()
    s.add_result("orders", _ok_result())
    assert s.total_tables == 1
    assert s.valid_tables == 1
    assert s.invalid_tables == 0


def test_failed_table_counted():
    s = PipelineSummary()
    s.add_result("orders", _ok_result())
    s.add_result("users", _fail_result(3))
    assert s.valid_tables == 1
    assert s.invalid_tables == 1
    assert s.total_errors == 3
    assert not s.is_clean


def test_failed_tables_list():
    s = PipelineSummary()
    s.add_result("a", _ok_result())
    s.add_result("b", _fail_result())
    s.add_result("c", _fail_result(2))
    assert sorted(s.failed_tables()) == ["b", "c"]


def test_summarize_helper():
    results = {"t1": _ok_result(), "t2": _fail_result(5)}
    s = summarize(results)
    assert s.total_tables == 2
    assert s.total_errors == 5


# --- summary_reporter tests ---

def test_format_pipeline_summary_clean():
    s = summarize({"orders": _ok_result(), "users": _ok_result()})
    text = format_pipeline_summary(s)
    assert "OK" in text
    assert "2" in text  # total tables


def test_format_pipeline_summary_failed():
    s = summarize({"orders": _ok_result(), "users": _fail_result(2)})
    text = format_pipeline_summary(s)
    assert "FAILED" in text
    assert "users" in text
    assert "2" in text


def test_format_per_table_status_ok():
    s = summarize({"orders": _ok_result()})
    text = format_per_table_status(s)
    assert "orders" in text
    assert "OK" in text


def test_format_per_table_status_failed():
    s = summarize({"items": _fail_result(4)})
    text = format_per_table_status(s)
    assert "FAILED" in text
    assert "4" in text


def test_format_per_table_status_empty():
    s = PipelineSummary()
    text = format_per_table_status(s)
    assert "No tables" in text
