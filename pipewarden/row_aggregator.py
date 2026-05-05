from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence


@dataclass
class AggregationRule:
    """Defines a single aggregation applied to a column."""

    name: str
    column: str
    func: Callable[[List[Any]], Any]
    label: Optional[str] = None

    def apply(self, values: List[Any]) -> Any:
        try:
            return self.func(values)
        except Exception:
            return None

    def __repr__(self) -> str:  # pragma: no cover
        lbl = self.label or self.name
        return f"AggregationRule({lbl!r}, column={self.column!r})"


@dataclass
class AggregationRecord:
    rule_name: str
    column: str
    result: Any


@dataclass
class RowAggregationReport:
    table: str
    row_count: int
    records: List[AggregationRecord] = field(default_factory=list)

    def add(self, record: AggregationRecord) -> None:
        self.records.append(record)

    def get(self, rule_name: str) -> Optional[AggregationRecord]:
        for r in self.records:
            if r.rule_name == rule_name:
                return r
        return None

    def result_map(self) -> Dict[str, Any]:
        return {r.rule_name: r.result for r in self.records}


def aggregate_rows(
    table: str,
    rows: Sequence[Dict[str, Any]],
    rules: Sequence[AggregationRule],
) -> RowAggregationReport:
    """Apply all aggregation rules to the given rows and return a report."""
    report = RowAggregationReport(table=table, row_count=len(rows))

    column_cache: Dict[str, List[Any]] = {}
    for rule in rules:
        if rule.column not in column_cache:
            column_cache[rule.column] = [
                row.get(rule.column) for row in rows
            ]
        values = column_cache[rule.column]
        result = rule.apply(values)
        report.add(AggregationRecord(rule_name=rule.name, column=rule.column, result=result))

    return report
