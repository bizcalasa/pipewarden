"""Human-readable formatting for sampling results."""
from __future__ import annotations

from pipewarden.sampling import SampleResult


def format_sample_result(result: SampleResult) -> str:
    """Return a single-line summary of a :class:`SampleResult`."""
    pct = f"{result.coverage() * 100:.1f}"
    return (
        f"[{result.strategy.upper()}] "
        f"sampled {result.sample_size} / {result.total_seen} rows "
        f"({pct}% coverage)"
    )


def format_sample_preview(result: SampleResult, max_rows: int = 5) -> str:
    """Return a short preview of the first *max_rows* sampled rows."""
    if not result.rows:
        return "  (no rows sampled)"
    lines = [format_sample_result(result), "  Preview:"]
    for i, row in enumerate(result.rows[:max_rows]):
        lines.append(f"    [{i}] {row}")
    if result.sample_size > max_rows:
        lines.append(f"    ... ({result.sample_size - max_rows} more rows)")
    return "\n".join(lines)


def sampling_summary(result: SampleResult) -> str:
    """One-line pass/info string suitable for pipeline summary output."""
    if result.total_seen == 0:
        return "SAMPLING: no rows available"
    return f"SAMPLING OK: {format_sample_result(result)}"
