"""Human-readable formatting for data quality reports."""
from __future__ import annotations
from typing import List
from pipewarden.data_quality import DataQualityReport, QualityDimension


def _bar(score: float, width: int = 20) -> str:
    filled = int(round(score / 100 * width))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_dimension(dim: QualityDimension) -> str:
    bar = _bar(dim.score)
    return (
        f"  {dim.name:<16} {bar} {dim.score:>6.1f}%  "
        f"({dim.passed}/{dim.total} passed, weight={dim.weight})"
    )


def format_quality_report(report: DataQualityReport) -> str:
    lines = [
        f"Data Quality Report — {report.table_name}",
        "-" * 60,
    ]
    for dim in report.dimensions:
        lines.append(format_dimension(dim))
    lines.append("-" * 60)
    lines.append(
        f"  Overall Score: {report.overall_score:.1f}%  Grade: {report.grade}"
    )
    return "\n".join(lines)


def quality_summary(reports: List[DataQualityReport]) -> str:
    if not reports:
        return "No quality reports available."
    lines = ["Pipeline Quality Summary", "=" * 40]
    for r in reports:
        lines.append(f"  {r.table_name:<30} {r.overall_score:>6.1f}%  [{r.grade}]")
    scores = [r.overall_score for r in reports]
    avg = sum(scores) / len(scores)
    lines.append("=" * 40)
    lines.append(f"  Average: {avg:.1f}%")
    return "\n".join(lines)
