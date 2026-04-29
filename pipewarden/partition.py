"""Partition-aware validation utilities for ETL pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional


@dataclass
class PartitionKey:
    """Describes a single partition key and its expected values."""

    name: str
    expected_values: Optional[List[Any]] = None  # None means any value is OK

    def validate(self, value: Any) -> bool:
        """Return True if *value* is acceptable for this partition key."""
        if self.expected_values is None:
            return value is not None
        return value in self.expected_values


@dataclass
class PartitionViolation:
    """Records a single partition key violation found in a row."""

    row_index: int
    key: str
    value: Any
    reason: str

    def __str__(self) -> str:
        return f"Row {self.row_index}: partition key '{self.key}' = {self.value!r} — {self.reason}"


@dataclass
class PartitionReport:
    """Aggregates all partition violations for a dataset."""

    table: str
    violations: List[PartitionViolation] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0

    def add(self, v: PartitionViolation) -> None:
        self.violations.append(v)

    def summary(self) -> str:
        if self.is_valid:
            return f"{self.table}: all partition keys valid"
        return f"{self.table}: {len(self.violations)} partition violation(s)"


def validate_partitions(
    table: str,
    rows: Iterable[Dict[str, Any]],
    keys: List[PartitionKey],
) -> PartitionReport:
    """Validate each row against the declared partition keys."""
    report = PartitionReport(table=table)
    for idx, row in enumerate(rows):
        for pk in keys:
            raw = row.get(pk.name)
            if raw is None:
                report.add(
                    PartitionViolation(
                        row_index=idx,
                        key=pk.name,
                        value=raw,
                        reason="missing partition key",
                    )
                )
            elif not pk.validate(raw):
                expected = pk.expected_values
                report.add(
                    PartitionViolation(
                        row_index=idx,
                        key=pk.name,
                        value=raw,
                        reason=f"unexpected value; allowed: {expected}",
                    )
                )
    return report
