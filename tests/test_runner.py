"""Tests for pipewarden.runner.validate_rows."""

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.runner import validate_rows


def _make_schema(fields):
    """Helper: build a TableSchema from a list of (name, type, nullable) tuples."""
    return TableSchema(
        name="test_table",
        fields=[
            FieldSchema(name=n, field_type=t, nullable=nullable)
            for n, t, nullable in fields
        ],
    )


def test_valid_rows_returns_no_errors():
    schema = _make_schema([("id", FieldType.INTEGER, False), ("name", FieldType.STRING, False)])
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    result = validate_rows(rows, schema)
    assert result.is_valid


def test_wrong_type_produces_error():
    schema = _make_schema([("age", FieldType.INTEGER, False)])
    rows = [{"age": "not-an-int"}]
    result = validate_rows(rows, schema)
    assert not result.is_valid
    assert result.error_count == 1
    assert result.errors[0].field == "age"


def test_null_in_non_nullable_field():
    schema = _make_schema([("score", FieldType.FLOAT, False)])
    rows = [{"score": None}]
    result = validate_rows(rows, schema)
    assert not result.is_valid


def test_null_in_nullable_field_is_ok():
    schema = _make_schema([("score", FieldType.FLOAT, True)])
    rows = [{"score": None}]
    result = validate_rows(rows, schema)
    assert result.is_valid


def test_missing_key_non_nullable_is_error():
    schema = _make_schema([("id", FieldType.INTEGER, False), ("name", FieldType.STRING, False)])
    rows = [{"id": 1}]  # 'name' is absent
    result = validate_rows(rows, schema)
    assert not result.is_valid
    assert any(e.field == "name" for e in result.errors)


def test_missing_key_nullable_is_ok():
    schema = _make_schema([("id", FieldType.INTEGER, False), ("note", FieldType.STRING, True)])
    rows = [{"id": 5}]  # 'note' is absent but nullable
    result = validate_rows(rows, schema)
    assert result.is_valid


def test_extra_field_raises_error_by_default():
    schema = _make_schema([("id", FieldType.INTEGER, False)])
    rows = [{"id": 1, "extra": "surprise"}]
    result = validate_rows(rows, schema)
    assert not result.is_valid
    assert any("extra" in e.field for e in result.errors)


def test_extra_field_allowed_when_flag_set():
    schema = _make_schema([("id", FieldType.INTEGER, False)])
    rows = [{"id": 1, "extra": "surprise"}]
    result = validate_rows(rows, schema, allow_extra_fields=True)
    assert result.is_valid


def test_multiple_rows_accumulate_errors():
    schema = _make_schema([("val", FieldType.INTEGER, False)])
    rows = [{"val": 1}, {"val": "bad"}, {"val": None}, {"val": 4}]
    result = validate_rows(rows, schema)
    assert result.error_count == 2


def test_empty_rows_is_valid():
    schema = _make_schema([("id", FieldType.INTEGER, False)])
    result = validate_rows([], schema)
    assert result.is_valid
