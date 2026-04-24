"""Unit tests for pipewarden.schema module."""

import pytest
from pipewarden.schema import FieldSchema, FieldType, TableSchema


# --- FieldSchema tests ---

def test_field_valid_string():
    f = FieldSchema(name="username", field_type=FieldType.STRING)
    assert f.validate_value("alice") == []


def test_field_wrong_type():
    f = FieldSchema(name="age", field_type=FieldType.INTEGER)
    errors = f.validate_value("not_an_int")
    assert len(errors) == 1
    assert "age" in errors[0]


def test_field_null_not_allowed():
    f = FieldSchema(name="email", field_type=FieldType.STRING, nullable=False, required=True)
    errors = f.validate_value(None)
    assert any("null" in e.lower() or "required" in e.lower() for e in errors)


def test_field_null_allowed():
    f = FieldSchema(name="nickname", field_type=FieldType.STRING, nullable=True)
    assert f.validate_value(None) == []


def test_field_float_accepts_int():
    f = FieldSchema(name="score", field_type=FieldType.FLOAT)
    assert f.validate_value(42) == []


# --- TableSchema tests ---

def make_table():
    return TableSchema(
        name="users",
        fields=[
            FieldSchema("id", FieldType.INTEGER),
            FieldSchema("name", FieldType.STRING),
            FieldSchema("active", FieldType.BOOLEAN, nullable=True),
        ],
    )


def test_table_valid_row():
    table = make_table()
    errors = table.validate_row({"id": 1, "name": "Alice", "active": True})
    assert errors == []


def test_table_missing_required_field():
    table = make_table()
    errors = table.validate_row({"id": 1})
    assert any("name" in e for e in errors)


def test_table_unexpected_field():
    table = make_table()
    errors = table.validate_row({"id": 1, "name": "Bob", "active": False, "extra": "oops"})
    assert any("extra" in e for e in errors)


def test_table_field_names():
    table = make_table()
    assert table.field_names() == ["id", "name", "active"]


def test_table_get_field():
    table = make_table()
    f = table.get_field("name")
    assert f is not None
    assert f.field_type == FieldType.STRING


def test_table_get_missing_field():
    table = make_table()
    assert table.get_field("nonexistent") is None
