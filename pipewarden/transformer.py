"""Row-level field transformation pipeline with per-field rule chaining."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class TransformRule:
    """A single named transformation applied to a specific field."""

    name: str
    field_name: str
    transform: Callable[[Any], Any]

    def apply(self, value: Any) -> tuple[Any, bool]:
        """Apply the transform. Returns (new_value, success)."""
        try:
            return self.transform(value), True
        except Exception:
            return value, False


@dataclass
class TransformRecord:
    """Tracks a single field transformation that was applied."""

    row_index: int
    field_name: str
    rule_name: str
    original: Any
    transformed: Any


@dataclass
class TransformReport:
    """Aggregated report of all transformations applied to a dataset."""

    table_name: str
    rows_processed: int = 0
    records: List[TransformRecord] = field(default_factory=list)

    def record(self, row_index: int, field_name: str, rule_name: str,
               original: Any, transformed: Any) -> None:
        self.records.append(
            TransformRecord(row_index, field_name, rule_name, original, transformed)
        )

    @property
    def total_transformations(self) -> int:
        return len(self.records)

    def transformations_for_field(self, field_name: str) -> List[TransformRecord]:
        return [r for r in self.records if r.field_name == field_name]


def transform_rows(
    table_name: str,
    rows: List[Dict[str, Any]],
    rules: List[TransformRule],
) -> tuple[List[Dict[str, Any]], TransformReport]:
    """Apply transform rules to each row, returning mutated rows and a report."""
    report = TransformReport(table_name=table_name, rows_processed=len(rows))
    # Build a lookup: field_name -> list of rules (in order)
    rule_map: Dict[str, List[TransformRule]] = {}
    for rule in rules:
        rule_map.setdefault(rule.field_name, []).append(rule)

    output: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        new_row = dict(row)
        for fname, frules in rule_map.items():
            if fname not in new_row:
                continue
            current = new_row[fname]
            for rule in frules:
                result, ok = rule.apply(current)
                if ok and result != current:
                    report.record(idx, fname, rule.name, current, result)
                    current = result
            new_row[fname] = current
        output.append(new_row)
    return output, report
