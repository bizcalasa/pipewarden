"""Tests for pipewarden.differ module."""

import pytest
from pipewarden.schema import TableSchema, FieldSchema, FieldType
from pipewarden.differ import diff_schemas, SchemaDiff


def _make_schema(fields_def):
    """Helper: build a TableSchema from a list of (name, type, nullable) tuples."""
    fields = [
        FieldSchema(name=n, field_type=FieldType(t), nullable=nullable)
        for n, t, nullable in fields_def
    ]
    return TableSchema(name="test_table", fields=fields)


def test_no_changes_returns_empty_diff():
    schema = _make_schema([("id", "integer", False), ("name", "string", True)])
    diff = diff_schemas(schema, schema)
    assert not diff.has_changes
    assert diff.added_fields == []
    assert diff.removed_fields == []
    assert diff.type_changes == []
    assert diff.nullability_changes == []


def test_added_field_detected():
    old = _make_schema([("id", "integer", False)])
    new = _make_schema([("id", "integer", False), ("email", "string", True)])
    diff = diff_schemas(old, new)
    assert diff.has_changes
    assert "email" in diff.added_fields
    assert diff.removed_fields == []


def test_removed_field_detected():
    old = _make_schema([("id", "integer", False), ("email", "string", True)])
    new = _make_schema([("id", "integer", False)])
    diff = diff_schemas(old, new)
    assert diff.has_changes
    assert "email" in diff.removed_fields
    assert diff.added_fields == []


def test_type_change_detected():
    old = _make_schema([("score", "integer", False)])
    new = _make_schema([("score", "float", False)])
    diff = diff_schemas(old, new)
    assert diff.has_changes
    assert len(diff.type_changes) == 1
    assert "score" in diff.type_changes[0]
    assert "integer" in diff.type_changes[0]
    assert "float" in diff.type_changes[0]


def test_nullability_change_detected():
    old = _make_schema([("name", "string", False)])
    new = _make_schema([("name", "string", True)])
    diff = diff_schemas(old, new)
    assert diff.has_changes
    assert len(diff.nullability_changes) == 1
    assert "name" in diff.nullability_changes[0]


def test_summary_no_changes():
    schema = _make_schema([("id", "integer", False)])
    diff = diff_schemas(schema, schema)
    summary = diff.summary()
    assert "No schema changes" in summary


def test_summary_with_changes():
    old = _make_schema([("id", "integer", False), ("old_col", "string", True)])
    new = _make_schema([("id", "float", True), ("new_col", "boolean", False)])
    diff = diff_schemas(old, new)
    summary = diff.summary()
    assert "ADDED" in summary
    assert "REMOVED" in summary
    assert "TYPE" in summary
    assert "NULL" in summary
    assert "new_col" in summary
    assert "old_col" in summary
