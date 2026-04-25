"""Human-readable formatting for PipelineSummary objects."""

from pipewarden.summarizer import PipelineSummary


def _pluralize(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural


def format_pipeline_summary(summary: PipelineSummary) -> str:
    """Return a multi-line string describing the full pipeline validation run."""
    lines = []
    lines.append("=== Pipeline Validation Summary ===")
    lines.append(
        f"Tables checked : {summary.total_tables}"
    )
    lines.append(
        f"Passed         : {summary.valid_tables}"
    )
    lines.append(
        f"Failed         : {summary.invalid_tables}"
    )
    lines.append(
        f"Total errors   : {summary.total_errors}"
    )

    if summary.is_clean:
        lines.append("Status         : OK — all tables are valid")
    else:
        failed = summary.failed_tables()
        lines.append("Status         : FAILED")
        noun = _pluralize(len(failed), "table", "tables")
        lines.append(f"Failing {noun}  : {', '.join(failed)}")

    return "\n".join(lines)


def format_per_table_status(summary: PipelineSummary) -> str:
    """Return a compact per-table status listing."""
    if not summary.results:
        return "No tables recorded."

    lines = []
    for table_name, result in sorted(summary.results.items()):
        status = "OK" if result.is_valid else f"FAILED ({result.error_count} error(s))"
        lines.append(f"  {table_name}: {status}")
    return "\n".join(lines)
