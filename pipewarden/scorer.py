"""Data quality scorer: assigns a numeric quality score to validation results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewarden.validator import ValidationResult


@dataclass
class TableScore:
    table_name: str
    total_rows: int
    total_checks: int
    failed_checks: int

    @property
    def passed_checks(self) -> int:
        return self.total_checks - self.failed_checks

    @property
    def score(self) -> float:
        """Return a 0.0–100.0 quality score."""
        if self.total_checks == 0:
            return 100.0
        return round(100.0 * self.passed_checks / self.total_checks, 2)

    @property
    def grade(self) -> str:
        s = self.score
        if s >= 95:
            return "A"
        if s >= 80:
            return "B"
        if s >= 65:
            return "C"
        if s >= 50:
            return "D"
        return "F"


@dataclass
class ScoringReport:
    scores: List[TableScore] = field(default_factory=list)

    def add(self, ts: TableScore) -> None:
        self.scores.append(ts)

    def get(self, table_name: str) -> Optional[TableScore]:
        for s in self.scores:
            if s.table_name == table_name:
                return s
        return None

    @property
    def overall_score(self) -> float:
        if not self.scores:
            return 100.0
        total = sum(s.total_checks for s in self.scores)
        if total == 0:
            return 100.0
        passed = sum(s.passed_checks for s in self.scores)
        return round(100.0 * passed / total, 2)


def score_result(table_name: str, result: ValidationResult, total_rows: int) -> TableScore:
    """Derive a TableScore from a ValidationResult."""
    failed = len(result.errors)
    # Each row contributes one check per field touched; use error count as proxy.
    # total_checks = total_rows when we treat each row as one check unit.
    total_checks = max(total_rows, failed)
    return TableScore(
        table_name=table_name,
        total_rows=total_rows,
        total_checks=total_checks,
        failed_checks=failed,
    )


def score_results(results: Dict[str, ValidationResult], row_counts: Dict[str, int]) -> ScoringReport:
    """Build a ScoringReport from a mapping of table name -> ValidationResult."""
    report = ScoringReport()
    for table_name, result in results.items():
        rows = row_counts.get(table_name, 0)
        report.add(score_result(table_name, result, rows))
    return report
