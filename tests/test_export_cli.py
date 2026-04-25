"""Integration-style tests for pipewarden.export_cli."""

from __future__ import annotations

import json
import csv
import io
from pathlib import Path

import pytest

from pipewarden.export_cli import cmd_export_validate, cmd_export_profile


SCHEMA_DICT = {
    "table": "users",
    "fields": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "name", "type": "string", "nullable": False},
        {"name": "score", "type": "float", "nullable": True},
    ],
}

VALID_ROWS = [
    {"id": 1, "name": "Alice", "score": 9.5},
    {"id": 2, "name": "Bob", "score": None},
]

INVALID_ROWS = [
    {"id": "bad", "name": "Alice", "score": 9.5},
]


def _write_schema(tmp_path: Path) -> Path:
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(SCHEMA_DICT))
    return p


def _write_jsonl(tmp_path: Path, rows, name="data.jsonl") -> Path:
    p = tmp_path / name
    p.write_text("\n".join(json.dumps(r) for r in rows))
    return p


class _Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# --- export-validate JSON ---

def test_export_validate_json_valid_data(tmp_path, capsys):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, VALID_ROWS)
    args = _Args(schema=str(schema_path), data=str(data_path), format="json", output=None)
    rc = cmd_export_validate(args)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert rc == 0
    assert parsed[0]["is_valid"] is True


def test_export_validate_json_invalid_data(tmp_path, capsys):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, INVALID_ROWS)
    args = _Args(schema=str(schema_path), data=str(data_path), format="json", output=None)
    rc = cmd_export_validate(args)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert rc == 1
    assert parsed[0]["is_valid"] is False


def test_export_validate_csv_format(tmp_path, capsys):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, INVALID_ROWS)
    args = _Args(schema=str(schema_path), data=str(data_path), format="csv", output=None)
    cmd_export_validate(args)
    captured = capsys.readouterr()
    reader = csv.DictReader(io.StringIO(captured.out))
    rows = list(reader)
    assert len(rows) >= 1
    assert "field" in rows[0]


def test_export_validate_writes_to_file(tmp_path):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, VALID_ROWS)
    out_path = tmp_path / "result.json"
    args = _Args(schema=str(schema_path), data=str(data_path), format="json", output=str(out_path))
    cmd_export_validate(args)
    assert out_path.exists()
    parsed = json.loads(out_path.read_text())
    assert isinstance(parsed, list)


# --- export-profile ---

def test_export_profile_json_output(tmp_path, capsys):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, VALID_ROWS)
    args = _Args(schema=str(schema_path), data=str(data_path), output=None)
    rc = cmd_export_profile(args)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert rc == 0
    assert "row_count" in parsed
    assert "fields" in parsed


def test_export_profile_writes_to_file(tmp_path):
    schema_path = _write_schema(tmp_path)
    data_path = _write_jsonl(tmp_path, VALID_ROWS)
    out_path = tmp_path / "profile.json"
    args = _Args(schema=str(schema_path), data=str(data_path), output=str(out_path))
    cmd_export_profile(args)
    assert out_path.exists()
    parsed = json.loads(out_path.read_text())
    assert parsed["row_count"] == 2
