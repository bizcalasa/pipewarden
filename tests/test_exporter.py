"""Tests for pipewarden.exporter."""

from __future__ import annotations

import json
import csv
import io

import pytest

from pipewarden.validator import ValidationResult, ValidationError
from pipewarden.profiler import FieldProfile, ProfileReport
from pipewarden.exporter import (
    result_to_dict,
    export_results_json,
    export_results_csv,
    export_profile_json,
)


def _make_error(row: int, field: str, msg: str) -> ValidationError:
    return ValidationError(row_index=row, field_name=field, message=msg)


def _make_result(table: str, errors=None) -> ValidationResult:
    result = ValidationResult(table_name=table)
    for e in errors or []:
        result.add_error(e)
    return result


def _make_profile_report() -> ProfileReport:
    report = ProfileReport(row_count=10)
    fp = FieldProfile(total=10, null_count=2, type_counts={"str": 8})
    report.fields["name"] = fp
    return report


# --- result_to_dict ---

def test_result_to_dict_valid_result():
    result = _make_result("orders")
    d = result_to_dict(result)
    assert d["table"] == "orders"
    assert d["is_valid"] is True
    assert d["error_count"] == 0
    assert d["errors"] == []


def test_result_to_dict_with_errors():
    result = _make_result("users", [_make_error(3, "email", "null not allowed")])
    d = result_to_dict(result)
    assert d["is_valid"] is False
    assert d["error_count"] == 1
    assert d["errors"][0]["row"] == 3
    assert d["errors"][0]["field"] == "email"


# --- export_results_json ---

def test_export_results_json_is_valid_json():
    results = [_make_result("orders"), _make_result("users")]
    output = export_results_json(results)
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == 2


def test_export_results_json_contains_table_names():
    results = [_make_result("orders")]
    output = export_results_json(results)
    parsed = json.loads(output)
    assert parsed[0]["table"] == "orders"


# --- export_results_csv ---

def test_export_results_csv_has_header():
    results = [_make_result("orders")]
    output = export_results_csv(results)
    assert "table" in output
    assert "field" in output


def test_export_results_csv_contains_error_rows():
    results = [_make_result("users", [_make_error(1, "age", "wrong type")])]
    output = export_results_csv(results)
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["field"] == "age"
    assert rows[0]["table"] == "users"


def test_export_results_csv_empty_when_no_errors():
    results = [_make_result("orders")]
    output = export_results_csv(results)
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert rows == []


# --- export_profile_json ---

def test_export_profile_json_is_valid_json():
    report = _make_profile_report()
    output = export_profile_json(report)
    parsed = json.loads(output)
    assert "row_count" in parsed
    assert "fields" in parsed


def test_export_profile_json_field_data():
    report = _make_profile_report()
    output = export_profile_json(report)
    parsed = json.loads(output)
    assert "name" in parsed["fields"]
    assert parsed["fields"]["name"]["null_count"] == 2
    assert parsed["fields"]["name"]["dominant_type"] == "str"
