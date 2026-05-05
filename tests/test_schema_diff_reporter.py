"""Tests for pipewarden.schema_diff_reporter."""

from __future__ import annotations

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.differ import diff_schemas
from pipewarden.schema_diff_reporter import (
    format_schema_diff,
    schema_diff_summary,
)


def _make_schema(name: str, fields: list[tuple[str, FieldType, bool]]) -> TableSchema:
    return TableSchema(
        table_name=name,
        fields=[
            FieldSchema(field_name=f, field_type=t, nullable=n)
            for f, t, n in fields
        ],
    )


def test_format_no_changes_shows_no_changes_message():
    s = _make_schema("orders", [("id", FieldType.INTEGER, False)])
    diff = diff_schemas(s, s)
    result = format_schema_diff("orders", diff)
    assert "No schema changes" in result
    assert "orders" in result


def test_format_added_field_shows_plus_prefix():
    old = _make_schema("orders", [("id", FieldType.INTEGER, False)])
    new = _make_schema("orders", [
        ("id", FieldType.INTEGER, False),
        ("amount", FieldType.FLOAT, True),
    ])
    diff = diff_schemas(old, new)
    result = format_schema_diff("orders", diff)
    assert "+ amount" in result
    assert "float" in result


def test_format_removed_field_shows_minus_prefix():
    old = _make_schema("orders", [
        ("id", FieldType.INTEGER, False),
        ("legacy", FieldType.STRING, True),
    ])
    new = _make_schema("orders", [("id", FieldType.INTEGER, False)])
    diff = diff_schemas(old, new)
    result = format_schema_diff("orders", diff)
    assert "- legacy" in result


def test_format_type_change_shows_tilde_prefix():
    old = _make_schema("users", [("age", FieldType.STRING, False)])
    new = _make_schema("users", [("age", FieldType.INTEGER, False)])
    diff = diff_schemas(old, new)
    result = format_schema_diff("users", diff)
    assert "~ age" in result
    assert "string" in result
    assert "integer" in result


def test_format_breaking_diff_shows_breaking_label():
    old = _make_schema("sales", [("price", FieldType.FLOAT, False)])
    new = _make_schema("sales", [])
    diff = diff_schemas(old, new)
    result = format_schema_diff("sales", diff)
    assert "BREAKING" in result


def test_format_non_breaking_diff_omits_breaking_label():
    old = _make_schema("sales", [("id", FieldType.INTEGER, False)])
    new = _make_schema("sales", [
        ("id", FieldType.INTEGER, False),
        ("note", FieldType.STRING, True),
    ])
    diff = diff_schemas(old, new)
    result = format_schema_diff("sales", diff)
    assert "BREAKING" not in result


def test_schema_diff_summary_empty():
    result = schema_diff_summary({})
    assert "No schema diffs" in result


def test_schema_diff_summary_counts():
    s = _make_schema("t", [("id", FieldType.INTEGER, False)])
    old2 = _make_schema("u", [("x", FieldType.STRING, False)])
    new2 = _make_schema("u", [])
    diffs = {
        "t": diff_schemas(s, s),
        "u": diff_schemas(old2, new2),
    }
    result = schema_diff_summary(diffs)
    assert "2 table(s)" in result
    assert "1 with changes" in result
    assert "1 breaking" in result
