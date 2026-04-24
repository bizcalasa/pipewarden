"""Tests for pipewarden.validator."""

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.validator import ValidationError, ValidationResult, validate_dataset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schema() -> TableSchema:
    return TableSchema(
        name="users",
        fields=[
            FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="name", field_type=FieldType.STRING, nullable=False),
            FieldSchema(name="score", field_type=FieldType.FLOAT, nullable=True),
        ],
    )


# ---------------------------------------------------------------------------
# ValidationResult helpers
# ---------------------------------------------------------------------------

def test_result_is_valid_when_no_errors():
    result = ValidationResult(table="t", total_rows=5)
    assert result.is_valid is True
    assert result.error_count == 0


def test_result_summary_ok():
    result = ValidationResult(table="t", total_rows=3)
    assert "[OK]" in result.summary()
    assert "3 rows" in result.summary()


def test_result_summary_fail():
    result = ValidationResult(table="t", total_rows=3)
    result.errors.append(ValidationError(0, "id", None, "null not allowed"))
    assert "[FAIL]" in result.summary()
    assert "1 error" in result.summary()


def test_validation_error_str():
    err = ValidationError(row_index=2, column="name", value=None, message="null not allowed")
    text = str(err)
    assert "Row 2" in text
    assert "name" in text


# ---------------------------------------------------------------------------
# validate_dataset — happy path
# ---------------------------------------------------------------------------

def test_valid_rows_produce_no_errors():
    schema = _make_schema()
    rows = [
        {"id": 1, "name": "Alice", "score": 9.5},
        {"id": 2, "name": "Bob", "score": None},
    ]
    result = validate_dataset(schema, rows)
    assert result.is_valid
    assert result.total_rows == 2


# ---------------------------------------------------------------------------
# validate_dataset — error cases
# ---------------------------------------------------------------------------

def test_wrong_type_reported():
    schema = _make_schema()
    rows = [{"id": "not-an-int", "name": "Alice", "score": None}]
    result = validate_dataset(schema, rows)
    assert not result.is_valid
    assert any(e.column == "id" for e in result.errors)


def test_null_non_nullable_field_reported():
    schema = _make_schema()
    rows = [{"id": 1, "name": None, "score": 1.0}]
    result = validate_dataset(schema, rows)
    assert not result.is_valid
    assert any(e.column == "name" for e in result.errors)


def test_missing_key_treated_as_null():
    schema = _make_schema()
    rows = [{"id": 1, "score": 3.0}]  # 'name' is missing
    result = validate_dataset(schema, rows)
    assert not result.is_valid
    assert any(e.column == "name" for e in result.errors)


def test_empty_dataset_is_valid():
    schema = _make_schema()
    result = validate_dataset(schema, [])
    assert result.is_valid
    assert result.total_rows == 0


# ---------------------------------------------------------------------------
# strict mode
# ---------------------------------------------------------------------------

def test_strict_mode_flags_unknown_columns():
    schema = _make_schema()
    rows = [{"id": 1, "name": "Alice", "score": None, "extra": "surprise"}]
    result = validate_dataset(schema, rows, strict=True)
    assert not result.is_valid
    assert any(e.column == "extra" for e in result.errors)


def test_non_strict_mode_ignores_unknown_columns():
    schema = _make_schema()
    rows = [{"id": 1, "name": "Alice", "score": None, "extra": "surprise"}]
    result = validate_dataset(schema, rows, strict=False)
    assert result.is_valid
