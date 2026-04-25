"""Format RunComparison results for CLI output."""

from pipewarden.comparator import RunComparison, TableComparison


def _delta_str(delta: int) -> str:
    if delta > 0:
        return f"+{delta} errors"
    if delta < 0:
        return f"{delta} errors"
    return "no change"


def format_table_comparison(comp: TableComparison) -> str:
    if comp.is_new:
        status = "NEW"
        detail = "no baseline"
    elif comp.is_removed:
        status = "REMOVED"
        detail = "not in current run"
    elif comp.status_changed:
        old = "PASS" if comp.baseline.is_valid else "FAIL"
        new = "PASS" if comp.current.is_valid else "FAIL"
        status = f"{old} -> {new}"
        detail = _delta_str(comp.error_delta)
    else:
        status = "PASS" if comp.current.is_valid else "FAIL"
        detail = _delta_str(comp.error_delta)
    return f"  [{status}] {comp.table_name}: {detail}"


def format_run_comparison(run: RunComparison) -> str:
    lines = ["Run Comparison:"]
    if not run.comparisons:
        lines.append("  (no tables to compare)")
        return "\n".join(lines)

    for comp in run.comparisons:
        lines.append(format_table_comparison(comp))

    lines.append("")
    if run.has_regressions:
        regressed = ", ".join(run.regressed_tables)
        lines.append(f"⚠ Regressions detected in: {regressed}")
    else:
        lines.append("✓ No regressions detected.")

    if run.improved_tables:
        improved = ", ".join(run.improved_tables)
        lines.append(f"✓ Improvements in: {improved}")

    return "\n".join(lines)


def comparison_summary(run: RunComparison) -> str:
    total = len(run.comparisons)
    regressions = len(run.regressed_tables)
    improvements = len(run.improved_tables)
    return (
        f"{total} table(s) compared — "
        f"{regressions} regression(s), {improvements} improvement(s)."
    )
