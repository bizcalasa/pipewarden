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
