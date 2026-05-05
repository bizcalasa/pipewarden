"""Column mapping: rename, alias, and remap fields across schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MappingRule:
    source_field: str
    target_field: str
    transform: Optional[callable] = None

    def apply(self, value):
        """Return transformed value, or original if no transform defined."""
        if self.transform is None:
            return value
        try:
            return self.transform(value)
        except Exception:
            return value

    def __repr__(self) -> str:
        return f"MappingRule({self.source_field!r} -> {self.target_field!r})"


@dataclass
class MappingRecord:
    source_field: str
    target_field: str
    rows_mapped: int = 0


@dataclass
class ColumnMappingReport:
    table: str
    records: List[MappingRecord] = field(default_factory=list)

    def record(self, source: str, target: str, count: int) -> None:
        self.records.append(MappingRecord(source, target, count))

    def total_mappings(self) -> int:
        return sum(r.rows_mapped for r in self.records)

    def mapped_fields(self) -> List[str]:
        return [r.target_field for r in self.records]

    def summary(self) -> str:
        n = len(self.records)
        t = self.total_mappings()
        return f"{self.table}: {n} field(s) mapped, {t} total row transformation(s)"


def map_rows(
    rows: List[Dict],
    rules: List[MappingRule],
    table: str = "unknown",
) -> tuple[List[Dict], ColumnMappingReport]:
    """Apply mapping rules to a list of row dicts.

    Returns a new list of rows with renamed/transformed fields and a report.
    Unknown fields not covered by any rule are passed through unchanged.
    """
    report = ColumnMappingReport(table=table)
    counts: Dict[str, int] = {r.source_field: 0 for r in rules}
    rule_map: Dict[str, MappingRule] = {r.source_field: r for r in rules}

    mapped: List[Dict] = []
    for row in rows:
        new_row: Dict = {}
        for key, value in row.items():
            if key in rule_map:
                rule = rule_map[key]
                new_row[rule.target_field] = rule.apply(value)
                counts[key] += 1
            else:
                new_row[key] = value
        mapped.append(new_row)

    for rule in rules:
        report.record(rule.source_field, rule.target_field, counts[rule.source_field])

    return mapped, report
