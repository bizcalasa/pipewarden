"""Tests for pipewarden.watcher — snapshot persistence and drift detection."""

import json
import os
import tempfile

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.watcher import (
    _schema_to_dict,
    detect_drift,
    load_snapshot,
    save_snapshot,
    watch_directory,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schema(name: str, fields: dict[str, FieldType]) -> TableSchema:
    """Build a minimal TableSchema for testing."""
    return TableSchema(
        name=name,
        fields=[
            FieldSchema(name=fname, type=ftype, nullable=False)
            for fname, ftype in fields.items()
        ],
    )


# ---------------------------------------------------------------------------
# _schema_to_dict
# ---------------------------------------------------------------------------

def test_schema_to_dict_round_trips_field_names():
    schema = _make_schema("orders", {"id": FieldType.INTEGER, "total": FieldType.FLOAT})
    result = _schema_to_dict(schema)
    assert set(result["fields"].keys()) == {"id", "total"}


def test_schema_to_dict_stores_type_and_nullable():
    schema = TableSchema(
        name="users",
        fields=[
            FieldSchema(name="email", type=FieldType.STRING, nullable=True),
        ],
    )
    result = _schema_to_dict(schema)
    assert result["fields"]["email"]["type"] == "string"
    assert result["fields"]["email"]["nullable"] is True


# ---------------------------------------------------------------------------
# save_snapshot / load_snapshot
# ---------------------------------------------------------------------------

def test_save_and_load_snapshot_round_trip():
    schema = _make_schema("products", {"sku": FieldType.STRING, "price": FieldType.FLOAT})
    with tempfile.TemporaryDirectory() as tmpdir:
        snapshot_path = os.path.join(tmpdir, "snap.json")
        save_snapshot({"products": schema}, snapshot_path)
        loaded = load_snapshot(snapshot_path)

    assert "products" in loaded
    assert loaded["products"]["fields"]["sku"]["type"] == "string"


def test_save_snapshot_creates_valid_json():
    schema = _make_schema("events", {"ts": FieldType.STRING})
    with tempfile.TemporaryDirectory() as tmpdir:
        snapshot_path = os.path.join(tmpdir, "snap.json")
        save_snapshot({"events": schema}, snapshot_path)
        with open(snapshot_path) as fh:
            data = json.load(fh)  # must not raise
    assert "events" in data


def test_load_snapshot_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_snapshot("/nonexistent/path/snap.json")


# ---------------------------------------------------------------------------
# detect_drift
# ---------------------------------------------------------------------------

def test_detect_drift_no_changes():
    schemas = {"tbl": _make_schema("tbl", {"id": FieldType.INTEGER})}
    with tempfile.TemporaryDirectory() as tmpdir:
        snap = os.path.join(tmpdir, "snap.json")
        save_snapshot(schemas, snap)
        diffs = detect_drift(schemas, snap)
    assert diffs == {}


def test_detect_drift_new_table():
    old = {"tbl": _make_schema("tbl", {"id": FieldType.INTEGER})}
    new = {
        "tbl": _make_schema("tbl", {"id": FieldType.INTEGER}),
        "new_tbl": _make_schema("new_tbl", {"name": FieldType.STRING}),
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        snap = os.path.join(tmpdir, "snap.json")
        save_snapshot(old, snap)
        diffs = detect_drift(new, snap)
    assert "new_tbl" in diffs
    assert diffs["new_tbl"].has_changes


def test_detect_drift_removed_table():
    old = {
        "tbl": _make_schema("tbl", {"id": FieldType.INTEGER}),
        "gone": _make_schema("gone", {"x": FieldType.STRING}),
    }
    new = {"tbl": _make_schema("tbl", {"id": FieldType.INTEGER})}
    with tempfile.TemporaryDirectory() as tmpdir:
        snap = os.path.join(tmpdir, "snap.json")
        save_snapshot(old, snap)
        diffs = detect_drift(new, snap)
    assert "gone" in diffs


def test_detect_drift_field_type_change():
    old = {"tbl": _make_schema("tbl", {"amount": FieldType.INTEGER})}
    new = {"tbl": _make_schema("tbl", {"amount": FieldType.FLOAT})}
    with tempfile.TemporaryDirectory() as tmpdir:
        snap = os.path.join(tmpdir, "snap.json")
        save_snapshot(old, snap)
        diffs = detect_drift(new, snap)
    assert "tbl" in diffs
    assert any("amount" in c for c in diffs["tbl"].changed_fields)


# ---------------------------------------------------------------------------
# watch_directory
# ---------------------------------------------------------------------------

def test_watch_directory_returns_diffs_for_changed_schemas():
    schema_def = {
        "table": "metrics",
        "fields": [
            {"name": "value", "type": "integer", "nullable": False},
        ],
    }
    with tempfile.TemporaryDirectory() as schema_dir:
        schema_file = os.path.join(schema_dir, "metrics.json")
        with open(schema_file, "w") as fh:
            json.dump(schema_def, fh)

        with tempfile.TemporaryDirectory() as snap_dir:
            snap_path = os.path.join(snap_dir, "snap.json")

            # First call — no snapshot yet, should create one and return empty diffs
            diffs_first = watch_directory(schema_dir, snap_path)
            assert isinstance(diffs_first, dict)

            # Mutate the schema on disk
            schema_def["fields"][0]["type"] = "float"
            with open(schema_file, "w") as fh:
                json.dump(schema_def, fh)

            diffs_second = watch_directory(schema_dir, snap_path)
            assert "metrics" in diffs_second
