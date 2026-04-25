"""Formatting utilities for ProfileReport output."""

from __future__ import annotations

from pipewarden.profiler import FieldProfile, ProfileReport


def format_field_profile(fp: FieldProfile) -> str:
    null_pct = f"{fp.null_rate * 100:.1f}%"
    dominant = fp.dominant_type or "n/a"
    return (
        f"  [{fp.name}] "
        f"total={fp.total_count} "
        f"nulls={fp.null_count} ({null_pct}) "
        f"dominant_type={dominant}"
    )


def format_profile_report(report: ProfileReport) -> str:
    lines = [
        f"Profile: {report.table_name}",
        f"  Rows scanned : {report.row_count}",
        f"  Fields       : {len(report.fields)}",
        "  ---",
    ]
    for fp in report.fields.values():
        lines.append(format_field_profile(fp))
    return "\n".join(lines)


def profile_summary(report: ProfileReport) -> str:
    """Return a one-line summary suitable for CLI output."""
    high_null_fields = [
        fp.name for fp in report.fields.values() if fp.null_rate > 0.5
    ]
    if high_null_fields:
        flagged = ", ".join(high_null_fields)
        return (
            f"{report.table_name}: {report.row_count} rows — "
            f"WARNING high null rate in: {flagged}"
        )
    return f"{report.table_name}: {report.row_count} rows — OK"
