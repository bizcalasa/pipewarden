"""Alert rules and notification hooks for pipeline validation events."""

from dataclasses import dataclass, field
from typing import Callable, List, Optional
from pipewarden.validator import ValidationResult


@dataclass
class AlertRule:
    """Defines a condition that triggers an alert."""
    name: str
    description: str
    predicate: Callable[[ValidationResult], bool]

    def matches(self, result: ValidationResult) -> bool:
        """Return True if this alert rule fires for the given result."""
        try:
            return self.predicate(result)
        except Exception:
            return False


@dataclass
class Alert:
    """Represents a fired alert for a validation result."""
    rule_name: str
    table: str
    message: str

    def __str__(self) -> str:
        return f"[ALERT] {self.rule_name} on '{self.table}': {self.message}"


@dataclass
class AlertEngine:
    """Evaluates alert rules against validation results."""
    rules: List[AlertRule] = field(default_factory=list)

    def add_rule(self, rule: AlertRule) -> None:
        self.rules.append(rule)

    def evaluate(self, table: str, result: ValidationResult) -> List[Alert]:
        """Return all alerts that fire for the given result."""
        alerts = []
        for rule in self.rules:
            if rule.matches(result):
                alerts.append(Alert(
                    rule_name=rule.name,
                    table=table,
                    message=rule.description,
                ))
        return alerts


def error_threshold_rule(name: str, max_errors: int) -> AlertRule:
    """Built-in rule: fires when error count exceeds a threshold."""
    return AlertRule(
        name=name,
        description=f"Error count exceeded threshold of {max_errors}",
        predicate=lambda r: r.error_count() > max_errors,
    )


def any_failure_rule() -> AlertRule:
    """Built-in rule: fires whenever a result is not valid."""
    return AlertRule(
        name="any_failure",
        description="One or more validation errors detected",
        predicate=lambda r: not r.is_valid(),
    )
