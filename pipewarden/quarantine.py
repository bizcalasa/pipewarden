"""Quarantine module: isolates rows that fail validation for later inspection."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class QuarantinedRow:
    """A single row that failed validation, along with its failure reasons."""
    row_index: int
    row: Dict[str, Any]
    reasons: List[str] = field(default_factory=list)

    def __repr__(self) -> str:  # pragma: no cover
        return f"QuarantinedRow(row_index={self.row_index}, reasons={self.reasons})"


@dataclass
class QuarantineReport:
    """Collects quarantined rows for a single table."""
    table_name: str
    _rows: List[QuarantinedRow] = field(default_factory=list)

    def add(self, row_index: int, row: Dict[str, Any], reasons: List[str]) -> None:
        """Add a row to quarantine."""
        self._rows.append(QuarantinedRow(row_index=row_index, row=row, reasons=reasons))

    @property
    def quarantined_rows(self) -> List[QuarantinedRow]:
        return list(self._rows)

    @property
    def count(self) -> int:
        return len(self._rows)

    @property
    def is_clean(self) -> bool:
        return len(self._rows) == 0


def quarantine_from_runner(
    table_name: str,
    rows: List[Dict[str, Any]],
    validation_result_errors: Optional[List[Any]] = None,
) -> QuarantineReport:
    """Build a QuarantineReport from a list of ValidationErrors produced by the runner."""
    report = QuarantineReport(table_name=table_name)
    if not validation_result_errors:
        return report

    # Group errors by row_index
    error_map: Dict[int, List[str]] = {}
    for err in validation_result_errors:
        idx = err.row_index
        error_map.setdefault(idx, []).append(str(err))

    for idx, reasons in error_map.items():
        row = rows[idx] if idx < len(rows) else {}
        report.add(row_index=idx, row=row, reasons=reasons)

    return report
