"""Human-readable formatting for Scheduler state."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pipewarden.scheduler import ScheduleEntry, Scheduler


def _fmt_delta(entry: ScheduleEntry, now: Optional[datetime] = None) -> str:
    """Return a short human-readable string for how long ago a pipeline ran."""
    if entry.last_run is None:
        return "never run"
    now = now or datetime.utcnow()
    delta = now - entry.last_run
    total_seconds = int(delta.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s ago"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    return f"{total_seconds // 3600}h ago"


def format_entry(entry: ScheduleEntry, now: Optional[datetime] = None) -> str:
    """Format a single ScheduleEntry as a one-line status string."""
    due_marker = " [DUE]" if entry.is_due(now) else ""
    interval_mins = int(entry.interval.total_seconds() // 60)
    last = _fmt_delta(entry, now)
    return f"  {entry.name}: every {interval_mins}m | last={last}{due_marker}"


def format_schedule(scheduler: Scheduler, now: Optional[datetime] = None) -> str:
    """Format the full scheduler state as a multi-line report."""
    names = scheduler.pipeline_names()
    if not names:
        return "Scheduler: no pipelines registered."
    lines = ["Scheduled pipelines:"]
    for name in names:
        entry = scheduler.get(name)
        if entry:
            lines.append(format_entry(entry, now))
    return "\n".join(lines)


def schedule_summary(scheduler: Scheduler, now: Optional[datetime] = None) -> str:
    """Return a one-line summary of how many pipelines are due."""
    due_count = len(scheduler.due(now))
    total = len(scheduler.pipeline_names())
    noun = "pipeline" if due_count == 1 else "pipelines"
    return f"{due_count}/{total} {noun} due to run."


def format_next_run(entry: ScheduleEntry, now: Optional[datetime] = None) -> str:
    """Return a short human-readable string for when a pipeline is next due.

    If the pipeline has never run, the next run is considered immediately due.
    """
    if entry.last_run is None:
        return "now"
    now = now or datetime.utcnow()
    next_run = entry.last_run + entry.interval
    delta = next_run - now
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "now"
    if total_seconds < 60:
        return f"in {total_seconds}s"
    if total_seconds < 3600:
        return f"in {total_seconds // 60}m"
    return f"in {total_seconds // 3600}h"
