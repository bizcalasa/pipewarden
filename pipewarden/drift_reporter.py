"""Formatting helpers for drift detection reports."""

from pipewarden.drift_detector import DriftReport, DriftSummary


def format_drift_report(report: DriftReport) -> str:
    lines = [report.summary()]
    if report.has_drift and report.diff is not None:
        diff = report.diff
        for f in diff.added_fields:
            lines.append(f"  + added field   : {f}")
        for f in diff.removed_fields:
            lines.append(f"  - removed field : {f}")
        for f, (old, new) in diff.type_changes.items():
            lines.append(f"  ~ type changed  : {f}  ({old} -> {new})")
        for f, (old, new) in diff.nullability_changes.items():
            old_s = "nullable" if old else "required"
            new_s = "nullable" if new else "required"
            lines.append(f"  ~ nullable changed: {f}  ({old_s} -> {new_s})")
    return "\n".join(lines)


def format_drift_summary(summary: DriftSummary) -> str:
    lines = []
    for report in summary.reports:
        lines.append(format_drift_report(report))
    return "\n".join(lines)


def drift_summary_line(summary: DriftSummary) -> str:
    parts = [
        f"{summary.total} table(s) checked",
        f"{summary.drifted} drifted",
        f"{summary.clean} clean",
    ]
    if summary.missing_snapshots:
        parts.append(f"{summary.missing_snapshots} missing snapshot(s)")
    status = "DRIFT DETECTED" if summary.drifted else "OK"
    return f"[{status}] " + ", ".join(parts) + "."
