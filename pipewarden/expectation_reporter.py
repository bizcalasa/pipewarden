"""Formatting helpers for ExpectationReport output."""

from __future__ import annotations

from pipewarden.expectation import ExpectationReport, ExpectationViolation


def format_violation(v: ExpectationViolation) -> str:
    """Return a human-readable string for a single violation."""
    return f"  - [row {v.row_index}] '{v.rule_name}' failed"


def format_expectation_report(report: ExpectationReport) -> str:
    """Return a multi-line summary of an expectation report."""
    lines = [f"Table: {report.table}"]
    if report.passed:
        lines.append("  Status: OK — all expectations passed")
    else:
        count = len(report.violations)
        noun = "violation" if count == 1 else "violations"
        lines.append(f"  Status: FAILED — {count} {noun}")
        for v in report.violations:
            lines.append(format_violation(v))
    return "\n".join(lines)


def expectation_summary(reports: list[ExpectationReport]) -> str:
    """Return a one-line summary across multiple expectation reports."""
    total = len(reports)
    failed = sum(1 for r in reports if not r.passed)
    passed = total - failed
    if total == 0:
        return "No expectation reports."
    noun = "table" if total == 1 else "tables"
    return (
        f"{total} {noun} checked: {passed} passed, {failed} failed expectations."
    )
