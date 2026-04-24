"""Formatting and reporting utilities for validation results."""

from typing import List
from pipewarden.validator import ValidationResult, ValidationError


def _pluralize(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural


def format_error(error: ValidationError) -> str:
    """Return a single-line string representation of a ValidationError."""
    return (
        f"  [row {error.row_index}] field '{error.field_name}': {error.message}"
    )


def format_result(result: ValidationResult, source: str = "") -> str:
    """Return a human-readable report for a single ValidationResult."""
    lines: List[str] = []
    header = f"Schema: {result.schema_name}"
    if source:
        header += f"  |  Source: {source}"
    lines.append(header)
    lines.append("-" * len(header))

    if result.is_valid:
        lines.append(f"  OK — {result.row_count} {_pluralize(result.row_count, 'row', 'rows')} validated, no errors.")
    else:
        lines.append(
            f"  FAILED — {result.error_count} "
            f"{_pluralize(result.error_count, 'error', 'errors')} "
            f"across {result.row_count} {_pluralize(result.row_count, 'row', 'rows')}."
        )
        for err in result.errors:
            lines.append(format_error(err))

    return "\n".join(lines)


def format_summary(results: List[ValidationResult]) -> str:
    """Return an aggregated summary report for multiple ValidationResults."""
    total_rows = sum(r.row_count for r in results)
    total_errors = sum(r.error_count for r in results)
    failed = sum(1 for r in results if not r.is_valid)
    passed = len(results) - failed

    lines = [
        "=" * 40,
        "PIPEWARDEN VALIDATION SUMMARY",
        "=" * 40,
        f"  Schemas checked : {len(results)}",
        f"  Passed          : {passed}",
        f"  Failed          : {failed}",
        f"  Total rows      : {total_rows}",
        f"  Total errors    : {total_errors}",
        "=" * 40,
    ]
    return "\n".join(lines)
