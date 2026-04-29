"""Human-readable formatting for QuarantineReport."""
from __future__ import annotations

from pipewarden.quarantine import QuarantineReport, QuarantinedRow


def format_quarantined_row(qrow: QuarantinedRow) -> str:
    """Format a single quarantined row as a readable string."""
    reasons_str = "; ".join(qrow.reasons)
    return f"  [row {qrow.row_index}] {reasons_str}"


def format_quarantine_report(report: QuarantineReport) -> str:
    """Format the full quarantine report for a table."""
    lines = [f"Quarantine report for '{report.table_name}':"]
    if report.is_clean:
        lines.append("  No rows quarantined.")
        return "\n".join(lines)

    noun = "row" if report.count == 1 else "rows"
    lines.append(f"  {report.count} {noun} quarantined.")
    for qrow in report.quarantined_rows:
        lines.append(format_quarantined_row(qrow))
    return "\n".join(lines)


def quarantine_summary(report: QuarantineReport) -> str:
    """Return a one-line summary of the quarantine report."""
    if report.is_clean:
        return f"{report.table_name}: clean (0 quarantined)"
    noun = "row" if report.count == 1 else "rows"
    return f"{report.table_name}: {report.count} {noun} quarantined"
