"""Formatting helpers for ColumnMappingReport."""
from __future__ import annotations

from pipewarden.column_mapper import ColumnMappingReport, MappingRecord


def _pluralize(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def format_mapping_record(record: MappingRecord) -> str:
    return (
        f"  {record.source_field!r:20s} -> {record.target_field!r:20s} "
        f"({_pluralize(record.rows_mapped, 'row')})"
    )


def format_column_mapping_report(report: ColumnMappingReport) -> str:
    lines = [f"Column Mapping Report — {report.table}"]
    if not report.records:
        lines.append("  (no mappings defined)")
        return "\n".join(lines)
    for rec in report.records:
        lines.append(format_mapping_record(rec))
    lines.append(f"  Total: {_pluralize(report.total_mappings(), 'transformation')}")
    return "\n".join(lines)


def mapping_summary(report: ColumnMappingReport) -> str:
    """One-line summary suitable for pipeline-level roll-ups."""
    n = len(report.records)
    t = report.total_mappings()
    status = "OK" if n > 0 else "EMPTY"
    return f"[{status}] {report.table}: {_pluralize(n, 'mapping')}, {_pluralize(t, 'transformation')}"
