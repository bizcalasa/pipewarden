"""Field-level value normalization for ETL pipeline rows."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class NormalizerRule:
    """A named transformation applied to a specific field."""
    field_name: str
    transform: Callable[[Any], Any]
    description: str = ""

    def apply(self, value: Any) -> Any:
        """Return the transformed value, or the original if transform raises."""
        try:
            return self.transform(value)
        except Exception:
            return value


@dataclass
class NormalizationReport:
    """Collects per-field transformation counts for a batch of rows."""
    table: str
    rules_applied: Dict[str, int] = field(default_factory=dict)
    rows_processed: int = 0

    def record(self, field_name: str) -> None:
        self.rules_applied[field_name] = self.rules_applied.get(field_name, 0) + 1

    @property
    def total_transformations(self) -> int:
        return sum(self.rules_applied.values())


class Normalizer:
    """Applies a collection of NormalizerRules to rows."""

    def __init__(self, table: str) -> None:
        self.table = table
        self._rules: List[NormalizerRule] = []

    def add(self, rule: NormalizerRule) -> None:
        self._rules.append(rule)

    def rules_for(self, field_name: str) -> List[NormalizerRule]:
        return [r for r in self._rules if r.field_name == field_name]

    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Return a new row dict with all matching rules applied."""
        result = dict(row)
        for rule in self._rules:
            if rule.field_name in result:
                result[rule.field_name] = rule.apply(result[rule.field_name])
        return result

    def normalize_rows(
        self, rows: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], NormalizationReport]:
        """Normalize a list of rows and return them with a report."""
        report = NormalizationReport(table=self.table)
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for rule in self._rules:
                if rule.field_name in new_row:
                    original = new_row[rule.field_name]
                    new_row[rule.field_name] = rule.apply(original)
                    if new_row[rule.field_name] != original:
                        report.record(rule.field_name)
            normalized.append(new_row)
            report.rows_processed += 1
        return normalized, report
