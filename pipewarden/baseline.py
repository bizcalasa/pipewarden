"""Baseline management: save and compare pipeline validation run results."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class BaselineEntry:
    table: str
    error_count: int
    is_valid: bool


@dataclass
class BaselineReport:
    entries: List[BaselineEntry] = field(default_factory=list)

    def get(self, table: str) -> Optional[BaselineEntry]:
        for entry in self.entries:
            if entry.table == table:
                return entry
        return None

    def table_names(self) -> List[str]:
        return [e.table for e in self.entries]


def save_baseline(report: BaselineReport, path: Path) -> None:
    """Persist a BaselineReport to a JSON file."""
    data = [
        {"table": e.table, "error_count": e.error_count, "is_valid": e.is_valid}
        for e in report.entries
    ]
    path.write_text(json.dumps(data, indent=2))


def load_baseline(path: Path) -> BaselineReport:
    """Load a BaselineReport from a JSON file."""
    raw = json.loads(path.read_text())
    entries = [
        BaselineEntry(
            table=item["table"],
            error_count=item["error_count"],
            is_valid=item["is_valid"],
        )
        for item in raw
    ]
    return BaselineReport(entries=entries)


@dataclass
class BaselineDiff:
    table: str
    previous_errors: int
    current_errors: int
    previous_valid: bool
    current_valid: bool

    @property
    def regressed(self) -> bool:
        return self.current_errors > self.previous_errors

    @property
    def improved(self) -> bool:
        return self.current_errors < self.previous_errors

    @property
    def status_changed(self) -> bool:
        return self.previous_valid != self.current_valid


def compare_to_baseline(
    baseline: BaselineReport, current: BaselineReport
) -> Dict[str, BaselineDiff]:
    """Return a mapping of table name to BaselineDiff for tables present in both runs."""
    diffs: Dict[str, BaselineDiff] = {}
    for entry in current.entries:
        prev = baseline.get(entry.table)
        if prev is None:
            continue
        diffs[entry.table] = BaselineDiff(
            table=entry.table,
            previous_errors=prev.error_count,
            current_errors=entry.error_count,
            previous_valid=prev.is_valid,
            current_valid=entry.is_valid,
        )
    return diffs
