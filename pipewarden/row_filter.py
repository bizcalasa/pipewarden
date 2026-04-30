"""Row filtering utilities for pipewarden pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class FilterRule:
    """A single predicate-based row filter rule."""
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    description: str = ""

    def matches(self, row: Dict[str, Any]) -> bool:
        """Return True if the row satisfies this filter (i.e. should be kept)."""
        try:
            return bool(self.predicate(row))
        except Exception:
            return False


@dataclass
class FilterResult:
    """Result of applying a set of filter rules to a collection of rows."""
    table: str
    total_rows: int = 0
    kept_rows: List[Dict[str, Any]] = field(default_factory=list)
    dropped_rows: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def dropped_count(self) -> int:
        return len(self.dropped_rows)

    @property
    def kept_count(self) -> int:
        return len(self.kept_rows)

    @property
    def drop_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return self.dropped_count / self.total_rows


@dataclass
class RowFilter:
    """Applies a collection of FilterRules to rows and produces a FilterResult."""
    table: str
    rules: List[FilterRule] = field(default_factory=list)

    def add(self, rule: FilterRule) -> None:
        self.rules.append(rule)

    def apply(self, rows: List[Dict[str, Any]]) -> FilterResult:
        result = FilterResult(table=self.table, total_rows=len(rows))
        for row in rows:
            if all(r.matches(row) for r in self.rules):
                result.kept_rows.append(row)
            else:
                result.dropped_rows.append(row)
        return result

    def rules_for(self, name: str) -> Optional[FilterRule]:
        for rule in self.rules:
            if rule.name == name:
                return rule
        return None
