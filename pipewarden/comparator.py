"""Compare validation results across multiple runs or tables."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewarden.validator import ValidationResult


@dataclass
class TableComparison:
    table_name: str
    baseline: Optional[ValidationResult]
    current: Optional[ValidationResult]

    @property
    def error_delta(self) -> int:
        """Positive means more errors, negative means fewer."""
        baseline_count = self.baseline.error_count if self.baseline else 0
        current_count = self.current.error_count if self.current else 0
        return current_count - baseline_count

    @property
    def status_changed(self) -> bool:
        if self.baseline is None or self.current is None:
            return True
        return self.baseline.is_valid != self.current.is_valid

    @property
    def is_new(self) -> bool:
        return self.baseline is None and self.current is not None

    @property
    def is_removed(self) -> bool:
        return self.baseline is not None and self.current is None


@dataclass
class RunComparison:
    comparisons: List[TableComparison] = field(default_factory=list)

    @property
    def has_regressions(self) -> bool:
        return any(c.error_delta > 0 or (c.status_changed and c.current and not c.current.is_valid)
                   for c in self.comparisons)

    @property
    def improved_tables(self) -> List[str]:
        return [c.table_name for c in self.comparisons if c.error_delta < 0]

    @property
    def regressed_tables(self) -> List[str]:
        return [c.table_name for c in self.comparisons
                if c.error_delta > 0 or (c.is_new and c.current and not c.current.is_valid)]


def compare_runs(
    baseline: Dict[str, ValidationResult],
    current: Dict[str, ValidationResult],
) -> RunComparison:
    """Compare two sets of validation results keyed by table name."""
    all_tables = set(baseline.keys()) | set(current.keys())
    comparisons = [
        TableComparison(
            table_name=table,
            baseline=baseline.get(table),
            current=current.get(table),
        )
        for table in sorted(all_tables)
    ]
    return RunComparison(comparisons=comparisons)
