"""Expectation engine: define and evaluate data expectations against rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass
class ExpectationRule:
    """A single named expectation applied to a table row."""

    name: str
    description: str
    predicate: Callable[[dict], bool]

    def check(self, row: dict) -> bool:
        """Return True if the row satisfies the expectation."""
        try:
            return bool(self.predicate(row))
        except Exception:
            return False


@dataclass
class ExpectationViolation:
    """Records a single failed expectation for a specific row."""

    rule_name: str
    row_index: int
    row: dict

    def __str__(self) -> str:
        return f"[row {self.row_index}] Expectation '{self.rule_name}' violated"


@dataclass
class ExpectationReport:
    """Aggregates all violations found when running expectations over a dataset."""

    table: str
    violations: List[ExpectationViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def add_violation(self, violation: ExpectationViolation) -> None:
        self.violations.append(violation)


@dataclass
class ExpectationSet:
    """Collection of expectation rules for a named table."""

    table: str
    _rules: List[ExpectationRule] = field(default_factory=list)

    def add(self, rule: ExpectationRule) -> None:
        self._rules.append(rule)

    def rules(self) -> List[ExpectationRule]:
        return list(self._rules)

    def evaluate(self, rows: List[dict]) -> ExpectationReport:
        """Run all expectations against every row and return a report."""
        report = ExpectationReport(table=self.table)
        for idx, row in enumerate(rows):
            for rule in self._rules:
                if not rule.check(row):
                    report.add_violation(
                        ExpectationViolation(
                            rule_name=rule.name,
                            row_index=idx,
                            row=row,
                        )
                    )
        return report
