"""Lightweight run-schedule registry for pipewarden.

Allows users to register named pipelines with a cron-style interval
string and query which pipelines are due to run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Supported shorthand intervals
_SHORTHAND: Dict[str, timedelta] = {
    "@hourly": timedelta(hours=1),
    "@daily": timedelta(days=1),
    "@weekly": timedelta(weeks=1),
}

_MINUTES_RE = re.compile(r"^every (\d+)m$")


def parse_interval(interval: str) -> timedelta:
    """Convert an interval string to a timedelta.

    Supported formats:
      - ``@hourly``, ``@daily``, ``@weekly``
      - ``every <N>m``  (e.g. ``every 30m``)
    """
    if interval in _SHORTHAND:
        return _SHORTHAND[interval]
    m = _MINUTES_RE.match(interval.strip())
    if m:
        return timedelta(minutes=int(m.group(1)))
    raise ValueError(f"Unsupported interval format: {interval!r}")


@dataclass
class ScheduleEntry:
    name: str
    interval: timedelta
    last_run: Optional[datetime] = None

    def is_due(self, now: Optional[datetime] = None) -> bool:
        """Return True if the pipeline is due to run."""
        if self.last_run is None:
            return True
        now = now or datetime.utcnow()
        return (now - self.last_run) >= self.interval

    def mark_run(self, now: Optional[datetime] = None) -> None:
        """Record that the pipeline ran at *now* (defaults to utcnow)."""
        self.last_run = now or datetime.utcnow()


@dataclass
class Scheduler:
    _entries: Dict[str, ScheduleEntry] = field(default_factory=dict)

    def register(self, name: str, interval: str) -> ScheduleEntry:
        """Register a pipeline by name with an interval string."""
        entry = ScheduleEntry(name=name, interval=parse_interval(interval))
        self._entries[name] = entry
        return entry

    def get(self, name: str) -> Optional[ScheduleEntry]:
        return self._entries.get(name)

    def due(self, now: Optional[datetime] = None) -> List[ScheduleEntry]:
        """Return all entries that are currently due to run."""
        return [e for e in self._entries.values() if e.is_due(now)]

    def pipeline_names(self) -> List[str]:
        return list(self._entries.keys())
