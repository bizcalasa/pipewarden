"""Row counter module for tracking row counts across pipeline runs."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RowCountSnapshot:
    """A snapshot of row counts for a set of tables at a point in time."""
    counts: Dict[str, int] = field(default_factory=dict)

    def get(self, table: str) -> Optional[int]:
        return self.counts.get(table)

    def set(self, table: str, count: int) -> None:
        if count < 0:
            raise ValueError(f"Row count cannot be negative: {count}")
        self.counts[table] = count

    def table_names(self) -> List[str]:
        return list(self.counts.keys())


@dataclass
class RowCountDelta:
    """Difference in row counts between two snapshots for a single table."""
    table: str
    previous: Optional[int]
    current: Optional[int]

    @property
    def delta(self) -> Optional[int]:
        if self.previous is None or self.current is None:
            return None
        return self.current - self.previous

    @property
    def is_new(self) -> bool:
        return self.previous is None and self.current is not None

    @property
    def is_removed(self) -> bool:
        return self.previous is not None and self.current is None

    @property
    def changed(self) -> bool:
        return self.previous != self.current


@dataclass
class RowCountReport:
    """Report comparing row counts between two snapshots."""
    deltas: List[RowCountDelta] = field(default_factory=list)

    def add(self, delta: RowCountDelta) -> None:
        self.deltas.append(delta)

    @property
    def has_changes(self) -> bool:
        return any(d.changed for d in self.deltas)

    @property
    def new_tables(self) -> List[RowCountDelta]:
        return [d for d in self.deltas if d.is_new]

    @property
    def removed_tables(self) -> List[RowCountDelta]:
        return [d for d in self.deltas if d.is_removed]

    @property
    def changed_tables(self) -> List[RowCountDelta]:
        return [d for d in self.deltas if d.changed and not d.is_new and not d.is_removed]


def compare_row_counts(
    previous: RowCountSnapshot,
    current: RowCountSnapshot,
) -> RowCountReport:
    """Compare two row count snapshots and return a report of changes."""
    report = RowCountReport()
    all_tables = set(previous.table_names()) | set(current.table_names())
    for table in sorted(all_tables):
        delta = RowCountDelta(
            table=table,
            previous=previous.get(table),
            current=current.get(table),
        )
        report.add(delta)
    return report
