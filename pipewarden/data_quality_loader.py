"""Load data quality configuration from dicts or JSON files."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
from pipewarden.data_quality import DataQualityReport, QualityDimension


_REQUIRED_DIM_KEYS = {"name", "passed", "total"}


def _parse_dimension(raw: Dict[str, Any]) -> QualityDimension:
    missing = _REQUIRED_DIM_KEYS - raw.keys()
    if missing:
        raise ValueError(f"Dimension missing keys: {missing}")
    return QualityDimension(
        name=raw["name"],
        passed=int(raw["passed"]),
        total=int(raw["total"]),
        weight=float(raw.get("weight", 1.0)),
    )


def load_quality_report_from_dict(data: Dict[str, Any]) -> DataQualityReport:
    """Build a DataQualityReport from a plain dictionary."""
    if "table" not in data:
        raise ValueError("Quality report dict must contain 'table' key.")
    report = DataQualityReport(table_name=data["table"])
    for raw_dim in data.get("dimensions", []):
        report.add_dimension(_parse_dimension(raw_dim))
    return report


def load_quality_report_from_file(path: str) -> DataQualityReport:
    """Load a DataQualityReport from a JSON file."""
    p = Path(path)
    with p.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return load_quality_report_from_dict(data)


def load_quality_reports_from_dir(directory: str) -> List[DataQualityReport]:
    """Load all *.json quality reports from a directory."""
    reports = []
    for p in sorted(Path(directory).glob("*.json")):
        reports.append(load_quality_report_from_file(str(p)))
    return reports
