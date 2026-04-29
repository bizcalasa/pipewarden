"""Deduplication utilities for detecting and reporting duplicate rows in ETL pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass
class DuplicateGroup:
    """A group of row indices that share the same key."""

    key: Tuple[Any, ...]
    row_indices: List[int]

    @property
    def count(self) -> int:
        return len(self.row_indices)

    def __repr__(self) -> str:  # pragma: no cover
        return f"DuplicateGroup(key={self.key!r}, count={self.count})"


@dataclass
class DeduplicationReport:
    """Summary of duplicate detection across a table."""

    table: str
    key_fields: List[str]
    total_rows: int
    duplicate_groups: List[DuplicateGroup] = field(default_factory=list)

    @property
    def duplicate_row_count(self) -> int:
        """Total number of rows that are duplicates (extras beyond the first)."""
        return sum(g.count - 1 for g in self.duplicate_groups)

    @property
    def has_duplicates(self) -> bool:
        return len(self.duplicate_groups) > 0

    def get_group(self, key: Tuple[Any, ...]) -> Optional[DuplicateGroup]:
        for g in self.duplicate_groups:
            if g.key == key:
                return g
        return None


def _extract_key(
    row: Dict[str, Any], key_fields: List[str]
) -> Tuple[Any, ...]:
    return tuple(row.get(f) for f in key_fields)


def detect_duplicates(
    table: str,
    rows: Sequence[Dict[str, Any]],
    key_fields: List[str],
) -> DeduplicationReport:
    """Scan *rows* and return a report of any duplicate composite keys.

    Args:
        table: Logical table name (used in the report).
        rows: Sequence of row dicts to inspect.
        key_fields: Field names that together form the deduplication key.

    Returns:
        A :class:`DeduplicationReport` describing any duplicates found.
    """
    if not key_fields:
        raise ValueError("key_fields must contain at least one field name")

    seen: Dict[Tuple[Any, ...], List[int]] = {}
    for idx, row in enumerate(rows):
        key = _extract_key(row, key_fields)
        seen.setdefault(key, []).append(idx)

    groups = [
        DuplicateGroup(key=k, row_indices=indices)
        for k, indices in seen.items()
        if len(indices) > 1
    ]

    return DeduplicationReport(
        table=table,
        key_fields=key_fields,
        total_rows=len(rows),
        duplicate_groups=groups,
    )
