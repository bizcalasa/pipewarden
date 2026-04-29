"""Human-readable formatting for NormalizationReport."""
from __future__ import annotations

from pipewarden.normalizer import NormalizationReport


def _pluralize(n: int, singular: str, plural: str) -> str:
    return singular if n == 1 else plural


def format_normalization_report(report: NormalizationReport) -> str:
    """Return a multiline string summarising a NormalizationReport."""
    lines = [
        f"Normalization report: {report.table}",
        f"  Rows processed : {report.rows_processed}",
        f"  Transformations: {report.total_transformations}",
    ]
    if report.rules_applied:
        lines.append("  Per-field:")
        for fname, count in sorted(report.rules_applied.items()):
            noun = _pluralize(count, "change", "changes")
            lines.append(f"    {fname}: {count} {noun}")
    else:
        lines.append("  No fields were transformed.")
    return "\n".join(lines)


def normalization_summary(reports: list[NormalizationReport]) -> str:
    """Return a one-line summary across multiple reports."""
    total_rows = sum(r.rows_processed for r in reports)
    total_tx = sum(r.total_transformations for r in reports)
    tables = len(reports)
    t_noun = _pluralize(tables, "table", "tables")
    return (
        f"Normalized {total_rows} "
        + _pluralize(total_rows, "row", "rows")
        + f" across {tables} {t_noun} "
        + f"({total_tx} "
        + _pluralize(total_tx, "transformation", "transformations")
        + " applied)"
    )
