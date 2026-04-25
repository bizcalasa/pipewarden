"""Rule engine for applying custom field-level validation rules to rows."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Rule:
    """A named validation rule with a predicate function and error message."""

    name: str
    field_name: str
    predicate: Callable[[Any], bool]
    message: str

    def check(self, value: Any) -> Optional[str]:
        """Return error message if rule fails, else None."""
        try:
            if not self.predicate(value):
                return self.message
        except Exception as exc:  # noqa: BLE001
            return f"Rule '{self.name}' raised an exception: {exc}"
        return None


@dataclass
class RuleSet:
    """A collection of rules grouped by field name."""

    rules: List[Rule] = field(default_factory=list)

    def add(self, rule: Rule) -> None:
        self.rules.append(rule)

    def rules_for(self, field_name: str) -> List[Rule]:
        return [r for r in self.rules if r.field_name == field_name]


@dataclass
class RuleViolation:
    """Records a single rule violation for a specific row and field."""

    row_index: int
    field_name: str
    rule_name: str
    message: str

    def __str__(self) -> str:
        return (
            f"Row {self.row_index} | field '{self.field_name}' "
            f"| rule '{self.rule_name}': {self.message}"
        )


def apply_rules(
    rows: List[Dict[str, Any]],
    ruleset: RuleSet,
) -> List[RuleViolation]:
    """Apply all rules in *ruleset* to every row and return violations."""
    violations: List[RuleViolation] = []
    for idx, row in enumerate(rows):
        for rule in ruleset.rules:
            value = row.get(rule.field_name)
            error = rule.check(value)
            if error is not None:
                violations.append(
                    RuleViolation(
                        row_index=idx,
                        field_name=rule.field_name,
                        rule_name=rule.name,
                        message=error,
                    )
                )
    return violations
