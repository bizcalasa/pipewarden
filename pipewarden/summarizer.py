"""Summarizer module: aggregates validation results across multiple tables."""

from dataclasses import dataclass, field
from typing import Dict, List

from pipewarden.validator import ValidationResult


@dataclass
class PipelineSummary:
    """Aggregated summary of validation results across multiple tables."""

    results: Dict[str, ValidationResult] = field(default_factory=dict)

    def add_result(self, table_name: str, result: ValidationResult) -> None:
        """Register a validation result for a named table."""
        self.results[table_name] = result

    @property
    def total_tables(self) -> int:
        return len(self.results)

    @property
    def valid_tables(self) -> int:
        return sum(1 for r in self.results.values() if r.is_valid)

    @property
    def invalid_tables(self) -> int:
        return self.total_tables - self.valid_tables

    @property
    def total_errors(self) -> int:
        return sum(r.error_count for r in self.results.values())

    @property
    def is_clean(self) -> bool:
        """True only when every table passed validation."""
        return self.invalid_tables == 0

    def failed_tables(self) -> List[str]:
        """Return names of tables that have at least one validation error."""
        return [name for name, r in self.results.items() if not r.is_valid]


def summarize(results: Dict[str, ValidationResult]) -> PipelineSummary:
    """Build a PipelineSummary from a mapping of table name -> ValidationResult."""
    summary = PipelineSummary()
    for table_name, result in results.items():
        summary.add_result(table_name, result)
    return summary
