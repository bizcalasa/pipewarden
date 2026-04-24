"""Tests for pipewarden.reporter formatting utilities."""

import pytest
from pipewarden.validator import ValidationResult, ValidationError
from pipewarden.reporter import format_error, format_result, format_summary


def _make_error(row: int = 0, field: str = "col", msg: str = "bad value") -> ValidationError:
    return ValidationError(row_index=row, field_name=field, message=msg)


def _make_result(schema_name: str, errors=None, row_count: int = 5) -> ValidationResult:
    result = ValidationResult(schema_name=schema_name)
    result.row_count = row_count
    for e in (errors or []):
        result.add_error(e)
    return result


def test_format_error_contains_row_and_field():
    err = _make_error(row=3, field="age", msg="expected int")
    text = format_error(err)
    assert "row 3" in text
    assert "age" in text
    assert "expected int" in text


def test_format_result_valid_shows_ok():
    result = _make_result("users", errors=[], row_count=10)
    text = format_result(result)
    assert "OK" in text
    assert "10 rows" in text
    assert "users" in text


def test_format_result_invalid_shows_failed():
    errors = [_make_error(1, "email", "null not allowed")]
    result = _make_result("orders", errors=errors, row_count=4)
    text = format_result(result)
    assert "FAILED" in text
    assert "1 error" in text
    assert "email" in text


def test_format_result_includes_source():
    result = _make_result("products", row_count=2)
    text = format_result(result, source="data/products.jsonl")
    assert "data/products.jsonl" in text


def test_format_result_singular_row():
    result = _make_result("events", errors=[], row_count=1)
    text = format_result(result)
    assert "1 row" in text
    assert "rows" not in text


def test_format_result_singular_error():
    errors = [_make_error()]
    result = _make_result("logs", errors=errors, row_count=3)
    text = format_result(result)
    assert "1 error" in text
    assert "errors" not in text


def test_format_summary_counts():
    r1 = _make_result("a", errors=[], row_count=5)
    r2 = _make_result("b", errors=[_make_error(), _make_error()], row_count=3)
    text = format_summary([r1, r2])
    assert "Schemas checked : 2" in text
    assert "Passed          : 1" in text
    assert "Failed          : 1" in text
    assert "Total rows      : 8" in text
    assert "Total errors    : 2" in text


def test_format_summary_all_pass():
    results = [_make_result("x", errors=[], row_count=2) for _ in range(3)]
    text = format_summary(results)
    assert "Failed          : 0" in text
    assert "Passed          : 3" in text
