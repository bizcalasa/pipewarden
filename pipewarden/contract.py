"""Data contract definitions and enforcement for pipeline tables."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class ContractRule:
    name: str
    description: str
    predicate: Callable[[Dict[str, Any]], bool]

    def check(self, row: Dict[str, Any]) -> bool:
        try:
            return self.predicate(row)
        except Exception:
            return False


@dataclass
class ContractViolation:
    table: str
    rule_name: str
    row_index: int
    description: str

    def __str__(self) -> str:
        return f"[{self.table}] row {self.row_index}: rule '{self.rule_name}' violated — {self.description}"


@dataclass
class ContractReport:
    table: str
    total_rows: int
    violations: List[ContractViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def add_violation(self, violation: ContractViolation) -> None:
        self.violations.append(violation)

    @property
    def violation_count(self) -> int:
        return len(self.violations)


@dataclass
class DataContract:
    table: str
    rules: List[ContractRule] = field(default_factory=list)

    def add_rule(self, rule: ContractRule) -> None:
        self.rules.append(rule)

    def enforce(self, rows: List[Dict[str, Any]]) -> ContractReport:
        report = ContractReport(table=self.table, total_rows=len(rows))
        for idx, row in enumerate(rows):
            for rule in self.rules:
                if not rule.check(row):
                    report.add_violation(
                        ContractViolation(
                            table=self.table,
                            rule_name=rule.name,
                            row_index=idx,
                            description=rule.description,
                        )
                    )
        return report
