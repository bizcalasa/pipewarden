"""Column-level statistics collector for ETL pipeline profiling."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ColumnStats:
    """Aggregated statistics for a single column across all rows."""
    name: str
    total: int = 0
    null_count: int = 0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    unique_values: int = 0
    _seen: set = field(default_factory=set, repr=False, compare=False)

    @property
    def non_null_count(self) -> int:
        return self.total - self.null_count

    @property
    def null_rate(self) -> float:
        return self.null_count / self.total if self.total > 0 else 0.0

    @property
    def fill_rate(self) -> float:
        return 1.0 - self.null_rate


@dataclass
class ColumnStatsReport:
    """Collection of ColumnStats for a single table."""
    table: str
    stats: Dict[str, ColumnStats] = field(default_factory=dict)

    def get(self, column: str) -> Optional[ColumnStats]:
        return self.stats.get(column)

    def column_names(self) -> List[str]:
        return list(self.stats.keys())


def compute_column_stats(table: str, rows: List[Dict[str, Any]]) -> ColumnStatsReport:
    """Compute per-column statistics from a list of row dicts."""
    report = ColumnStatsReport(table=table)

    for row in rows:
        for col, value in row.items():
            if col not in report.stats:
                report.stats[col] = ColumnStats(name=col)

            cs = report.stats[col]
            cs.total += 1

            if value is None:
                cs.null_count += 1
                continue

            cs._seen.add(value)
            cs.unique_values = len(cs._seen)

            try:
                if cs.min_value is None or value < cs.min_value:
                    cs.min_value = value
                if cs.max_value is None or value > cs.max_value:
                    cs.max_value = value
            except TypeError:
                pass

    return report
