"""Human-readable formatting for partition validation reports."""
from __future__ import annotations

from pipewarden.partition import PartitionReport, PartitionViolation


def format_violation(v: PartitionViolation) -> str:
    """Return a single-line description of a partition violation."""
    return f"  [row {v.row_index}] key='{v.key}' value={v.value!r}: {v.reason}"


def format_partition_report(report: PartitionReport) -> str:
    """Return a multi-line report block for *report*."""
    lines: list[str] = []
    status = "OK" if report.is_valid else "FAILED"
    lines.append(f"Partition validation [{status}]: {report.table}")
    if report.is_valid:
        lines.append("  No partition violations detected.")
    else:
        for v in report.violations:
            lines.append(format_violation(v))
    return "\n".join(lines)


def partition_summary(reports: list[PartitionReport]) -> str:
    """Return a one-line summary across multiple partition reports."""
    total = len(reports)
    failed = sum(1 for r in reports if not r.is_valid)
    if failed == 0:
        return f"All {total} table(s) passed partition validation."
    return f"{failed}/{total} table(s) failed partition validation."
