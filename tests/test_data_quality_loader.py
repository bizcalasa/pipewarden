"""Tests for pipewarden.data_quality_loader."""
import json
import pytest
from pathlib import Path
from pipewarden.data_quality_loader import (
    load_quality_report_from_dict,
    load_quality_report_from_file,
    load_quality_reports_from_dir,
)


_BASIC_DICT = {
    "table": "orders",
    "dimensions": [
        {"name": "completeness", "passed": 10, "total": 10},
        {"name": "validity", "passed": 8, "total": 10, "weight": 1.5},
    ],
}


def test_load_from_dict_basic():
    report = load_quality_report_from_dict(_BASIC_DICT)
    assert report.table_name == "orders"
    assert len(report.dimensions) == 2


def test_load_from_dict_dimension_values():
    report = load_quality_report_from_dict(_BASIC_DICT)
    validity = report.get_dimension("validity")
    assert validity is not None
    assert validity.passed == 8
    assert validity.total == 10
    assert validity.weight == 1.5


def test_load_from_dict_missing_table_raises():
    with pytest.raises(ValueError, match="'table'"):
        load_quality_report_from_dict({"dimensions": []})


def test_load_from_dict_missing_dim_key_raises():
    data = {"table": "t", "dimensions": [{"name": "completeness", "passed": 5}]}
    with pytest.raises(ValueError, match="missing keys"):
        load_quality_report_from_dict(data)


def test_load_from_dict_no_dimensions():
    report = load_quality_report_from_dict({"table": "empty"})
    assert report.dimensions == []


def test_load_from_file(tmp_path: Path):
    p = tmp_path / "orders.json"
    p.write_text(json.dumps(_BASIC_DICT), encoding="utf-8")
    report = load_quality_report_from_file(str(p))
    assert report.table_name == "orders"


def test_load_from_dir(tmp_path: Path):
    for name in ("a", "b", "c"):
        data = {"table": name, "dimensions": []}
        (tmp_path / f"{name}.json").write_text(json.dumps(data), encoding="utf-8")
    reports = load_quality_reports_from_dir(str(tmp_path))
    assert len(reports) == 3
    assert {r.table_name for r in reports} == {"a", "b", "c"}


def test_load_from_dir_empty(tmp_path: Path):
    reports = load_quality_reports_from_dir(str(tmp_path))
    assert reports == []
