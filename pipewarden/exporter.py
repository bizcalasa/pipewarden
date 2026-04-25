"""Export validation results and profiles to various output formats."""

from __future__ import annotations

import csv
import io
import json
from typing import List

from pipewarden.validator import ValidationResult
from pipewarden.profiler import ProfileReport


def result_to_dict(result: ValidationResult) -> dict:
    """Serialise a ValidationResult to a plain dictionary."""
    return {
        "table": result.table_name,
        "is_valid": result.is_valid,
        "error_count": result.error_count,
        "errors": [
            {
                "row": e.row_index,
                "field": e.field_name,
                "message": e.message,
            }
            for e in result.errors
        ],
    }


def export_results_json(results: List[ValidationResult]) -> str:
    """Return a JSON string representing a list of ValidationResults."""
    payload = [result_to_dict(r) for r in results]
    return json.dumps(payload, indent=2)


def export_results_csv(results: List[ValidationResult]) -> str:
    """Return a CSV string with one row per validation error."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=["table", "row", "field", "message"]
    )
    writer.writeheader()
    for result in results:
        for error in result.errors:
            writer.writerow(
                {
                    "table": result.table_name,
                    "row": error.row_index,
                    "field": error.field_name,
                    "message": error.message,
                }
            )
    return output.getvalue()


def export_profile_json(report: ProfileReport) -> str:
    """Return a JSON string representing a ProfileReport."""
    fields = {}
    for name, profile in report.fields.items():
        fields[name] = {
            "total": profile.total,
            "null_count": profile.null_count,
            "null_rate": round(profile.null_rate, 4),
            "dominant_type": profile.dominant_type,
            "type_counts": profile.type_counts,
        }
    payload = {"row_count": report.row_count, "fields": fields}
    return json.dumps(payload, indent=2)
