"""Track and compare numeric metrics across pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MetricSnapshot:
    """A single recorded snapshot of metrics for a named table."""

    table: str
    metrics: Dict[str, float] = field(default_factory=dict)

    def get(self, name: str) -> Optional[float]:
        return self.metrics.get(name)

    def set(self, name: str, value: float) -> None:
        self.metrics[name] = value


@dataclass
class MetricDelta:
    """Difference between two metric snapshots for a table."""

    table: str
    name: str
    previous: Optional[float]
    current: Optional[float]

    @property
    def changed(self) -> bool:
        return self.previous != self.current

    @property
    def delta(self) -> Optional[float]:
        if self.previous is None or self.current is None:
            return None
        return self.current - self.previous

    @property
    def is_new(self) -> bool:
        return self.previous is None and self.current is not None

    @property
    def is_removed(self) -> bool:
        return self.previous is not None and self.current is None


class MetricTracker:
    """Stores metric snapshots and computes deltas between runs."""

    def __init__(self) -> None:
        self._snapshots: Dict[str, List[MetricSnapshot]] = {}

    def record(self, snapshot: MetricSnapshot) -> None:
        self._snapshots.setdefault(snapshot.table, []).append(snapshot)

    def latest(self, table: str) -> Optional[MetricSnapshot]:
        snaps = self._snapshots.get(table)
        return snaps[-1] if snaps else None

    def previous(self, table: str) -> Optional[MetricSnapshot]:
        snaps = self._snapshots.get(table)
        return snaps[-2] if snaps and len(snaps) >= 2 else None

    def deltas(self, table: str) -> List[MetricDelta]:
        """Return per-metric deltas between the two most recent snapshots."""
        curr = self.latest(table)
        prev = self.previous(table)
        if curr is None:
            return []
        all_keys = set(curr.metrics) | (set(prev.metrics) if prev else set())
        return [
            MetricDelta(
                table=table,
                name=k,
                previous=prev.get(k) if prev else None,
                current=curr.get(k),
            )
            for k in sorted(all_keys)
        ]

    def tables(self) -> List[str]:
        return sorted(self._snapshots.keys())
