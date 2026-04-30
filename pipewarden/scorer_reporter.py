"""Human-readable formatting for ScoringReport and TableScore."""

from __future__ import annotations

from pipewarden.scorer import ScoringReport, TableScore


def format_table_score(ts: TableScore) -> str:
    status = "OK" if ts.score >= 80 else "WARN" if ts.score >= 50 else "FAIL"
    return (
        f"[{status}] {ts.table_name}: score={ts.score:.1f} "
        f"grade={ts.grade} "
        f"({ts.passed_checks}/{ts.total_checks} checks passed, "
        f"{ts.total_rows} rows)"
    )


def format_scoring_report(report: ScoringReport) -> str:
    if not report.scores:
        return "No tables scored."
    lines = ["=== Quality Scoring Report ==="]
    for ts in report.scores:
        lines.append(format_table_score(ts))
    lines.append(f"Overall score: {report.overall_score:.1f}")
    return "\n".join(lines)


def scoring_summary(report: ScoringReport) -> str:
    total = len(report.scores)
    passing = sum(1 for s in report.scores if s.score >= 80)
    failing = total - passing
    overall = report.overall_score
    grade_counts: dict[str, int] = {}
    for s in report.scores:
        grade_counts[s.grade] = grade_counts.get(s.grade, 0) + 1
    grade_str = ", ".join(f"{g}:{c}" for g, c in sorted(grade_counts.items()))
    return (
        f"{total} table(s) scored | "
        f"{passing} passing, {failing} failing | "
        f"overall={overall:.1f} | grades=[{grade_str}]"
    )
