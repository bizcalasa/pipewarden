"""Threshold enforcement for pipeline metrics and validation results."""

from dataclasses import dataclass, field
from typing import Optional, Callable, List
from pipewarden.validator import ValidationResult


@dataclass
class ThresholdRule:
    """A single threshold rule applied to a ValidationResult."""
    name: str
    predicate: Callable[[ValidationResult], bool]
    message: str

    def check(self, result: ValidationResult) -> bool:
        """Return True if the result violates this threshold."""
        try:
            return self.predicate(result)
        except Exception:
            return False


@dataclass
class ThresholdViolation:
    """Represents a single threshold violation."""
    table: str
    rule_name: str
    message: str

    def __str__(self) -> str:
        return f"[{self.table}] {self.rule_name}: {self.message}"


@dataclass
class ThresholdReport:
    """Aggregated results of threshold checks across tables."""
    violations: List[ThresholdViolation] = field(default_factory=list)

    def add(self, violation: ThresholdViolation) -> None:
        self.violations.append(violation)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    @property
    def violation_count(self) -> int:
        return len(self.violations)


class ThresholdEngine:
    """Applies a set of threshold rules to validation results."""

    def __init__(self) -> None:
        self._rules: List[ThresholdRule] = []

    def add_rule(self, rule: ThresholdRule) -> None:
        self._rules.append(rule)

    def evaluate(self, table: str, result: ValidationResult) -> ThresholdReport:
        report = ThresholdReport()
        for rule in self._rules:
            if rule.check(result):
                report.add(ThresholdViolation(
                    table=table,
                    rule_name=rule.name,
                    message=rule.message,
                ))
        return report


def max_error_rate_rule(max_rate: float) -> ThresholdRule:
    """Built-in rule: fail if error_count / row_count exceeds max_rate."""
    def _check(result: ValidationResult) -> bool:
        if result.row_count == 0:
            return False
        return (result.error_count / result.row_count) > max_rate

    pct = int(max_rate * 100)
    return ThresholdRule(
        name="max_error_rate",
        predicate=_check,
        message=f"Error rate exceeded {pct}% threshold",
    )


def max_error_count_rule(max_errors: int) -> ThresholdRule:
    """Built-in rule: fail if absolute error count exceeds max_errors."""
    return ThresholdRule(
        name="max_error_count",
        predicate=lambda r: r.error_count > max_errors,
        message=f"Error count exceeded threshold of {max_errors}",
    )
