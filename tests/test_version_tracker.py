"""Tests for pipewarden.version_tracker."""

import json
import os
import tempfile

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.version_tracker import (
    VersionHistory,
    load_history,
    record_version,
    save_history,
)


def _make_schema(name: str = "orders", fields=None) -> TableSchema:
    if fields is None:
        fields = [
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=True),
        ]
    return TableSchema(name=name, fields=fields)


def test_first_record_creates_version_1():
    history = VersionHistory(table_name="orders")
    schema = _make_schema()
    v = record_version(history, schema)
    assert v.version == 1


def test_first_record_summary_is_initial():
    history = VersionHistory(table_name="orders")
    v = record_version(history, _make_schema())
    assert v.change_summary == "initial version"


def test_second_record_increments_version():
    history = VersionHistory(table_name="orders")
    s1 = _make_schema()
    record_version(history, s1)
    s2 = _make_schema()
    v2 = record_version(history, s2, previous=s1)
    assert v2.version == 2


def test_no_changes_summary():
    history = VersionHistory(table_name="orders")
    s = _make_schema()
    record_version(history, s)
    v2 = record_version(history, s, previous=s)
    assert v2.change_summary == "no changes"


def test_added_field_reflected_in_summary():
    history = VersionHistory(table_name="orders")
    s1 = _make_schema()
    s2 = _make_schema(
        fields=s1.fields + [FieldSchema(name="note", type=FieldType.STRING, nullable=True)]
    )
    record_version(history, s1)
    v2 = record_version(history, s2, previous=s1)
    assert "note" in v2.change_summary


def test_fields_dict_stored_correctly():
    history = VersionHistory(table_name="orders")
    v = record_version(history, _make_schema())
    assert v.fields["id"] == "integer"
    assert v.fields["amount"] == "float"


def test_latest_returns_most_recent():
    history = VersionHistory(table_name="orders")
    s = _make_schema()
    record_version(history, s)
    record_version(history, s, previous=s)
    assert history.latest().version == 2


def test_latest_returns_none_for_empty_history():
    history = VersionHistory(table_name="orders")
    assert history.latest() is None


def test_save_and_load_round_trip():
    history = VersionHistory(table_name="orders")
    s = _make_schema()
    record_version(history, s)
    record_version(history, s, previous=s)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = tmp.name
    try:
        save_history(history, path)
        loaded = load_history("orders", path)
        assert len(loaded.versions) == 2
        assert loaded.versions[0].version == 1
        assert loaded.versions[1].version == 2
    finally:
        os.unlink(path)


def test_load_history_missing_file_returns_empty():
    history = load_history("orders", "/nonexistent/path/history.json")
    assert history.versions == []
    assert history.table_name == "orders"


def test_save_produces_valid_json():
    history = VersionHistory(table_name="orders")
    record_version(history, _make_schema())
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
        path = tmp.name
    try:
        save_history(history, path)
        with open(path) as fh:
            data = json.load(fh)
        assert isinstance(data, list)
        assert data[0]["version"] == 1
    finally:
        os.unlink(path)
