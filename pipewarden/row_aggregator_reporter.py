from __future__ import annotations

from pipewarden.row_aggregator import RowAggregationReport


def _fmt(value: object) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def format_aggregation_report(report: RowAggregationReport) -> str:
    lines = [
        f"Aggregation Report — {report.table}",
        f"  Rows processed : {report.row_count}",
    ]
    if not report.records:
        lines.append("  No aggregations defined.")
        return "\n".join(lines)

    lines.append("  Results:")
    for rec in report.records:
        lines.append(f"    [{rec.rule_name}] {rec.column} -> {_fmt(rec.result)}")

    return "\n".join(lines)


def aggregation_summary(report: RowAggregationReport) -> str:
    n = len(report.records)
    noun = "aggregation" if n == 1 else "aggregations"
    return (
        f"{report.table}: {report.row_count} rows, "
        f"{n} {noun} computed."
    )
