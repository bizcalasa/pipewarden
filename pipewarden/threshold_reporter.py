"""Formatting helpers for threshold reports."""

from pipewarden.threshold import ThresholdReport, ThresholdViolation


def format_violation(violation: ThresholdViolation) -> str:
    """Return a single-line string describing a violation."""
    return f"  ❌  [{violation.table}] {violation.rule_name}: {violation.message}"


def format_threshold_report(report: ThresholdReport) -> str:
    """Return a multi-line formatted threshold report."""
    if report.passed:
        return "Threshold checks passed. No violations found."

    lines = [f"Threshold violations ({report.violation_count} found):"]
    for v in report.violations:
        lines.append(format_violation(v))
    return "\n".join(lines)


def threshold_summary(report: ThresholdReport) -> str:
    """Return a short one-line summary suitable for pipeline output."""
    if report.passed:
        return "thresholds: OK"
    n = report.violation_count
    noun = "violation" if n == 1 else "violations"
    return f"thresholds: FAILED ({n} {noun})"


def format_violations_by_table(report: ThresholdReport) -> str:
    """Return a multi-line report with violations grouped by table name.

    Violations for the same table are listed together under a table header,
    making it easier to scan results when multiple rules apply to one table.
    """
    if report.passed:
        return "Threshold checks passed. No violations found."

    grouped: dict[str, list[ThresholdViolation]] = {}
    for v in report.violations:
        grouped.setdefault(v.table, []).append(v)

    lines = [f"Threshold violations ({report.violation_count} found):"]
    for table, violations in grouped.items():
        lines.append(f"  [{table}]")
        for v in violations:
            lines.append(f"    ❌  {v.rule_name}: {v.message}")
    return "\n".join(lines)
