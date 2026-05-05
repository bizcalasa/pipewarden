"""Field-level statistics: min, max, mean, unique count, and sample values."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FieldStats:
    """Aggregated statistics for a single field across all rows."""

    name: str
    total_count: int = 0
    null_count: int = 0
    unique_values: set = field(default_factory=set)
    _numeric_values: List[float] = field(default_factory=list, repr=False)

    @property
    def non_null_count(self) -> int:
        return self.total_count - self.null_count

    @property
    def null_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count

    @property
    def unique_count(self) -> int:
        return len(self.unique_values)

    @property
    def min_value(self) -> Optional[float]:
        return min(self._numeric_values) if self._numeric_values else None

    @property
    def max_value(self) -> Optional[float]:
        return max(self._numeric_values) if self._numeric_values else None

    @property
    def mean_value(self) -> Optional[float]:
        if not self._numeric_values:
            return None
        return sum(self._numeric_values) / len(self._numeric_values)

    def record(self, value: Any) -> None:
        self.total_count += 1
        if value is None:
            self.null_count += 1
            return
        self.unique_values.add(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            self._numeric_values.append(float(value))


@dataclass
class FieldStatsReport:
    """Collection of FieldStats for all fields in a table."""

    table: str
    _stats: Dict[str, FieldStats] = field(default_factory=dict, repr=False)

    def get(self, field_name: str) -> Optional[FieldStats]:
        return self._stats.get(field_name)

    @property
    def field_names(self) -> List[str]:
        return list(self._stats.keys())

    def _ensure(self, field_name: str) -> FieldStats:
        if field_name not in self._stats:
            self._stats[field_name] = FieldStats(name=field_name)
        return self._stats[field_name]


def compute_field_stats(table: str, rows: List[Dict[str, Any]]) -> FieldStatsReport:
    """Compute per-field statistics from a list of row dicts."""
    report = FieldStatsReport(table=table)
    for row in rows:
        for key, value in row.items():
            report._ensure(key).record(value)
    return report
