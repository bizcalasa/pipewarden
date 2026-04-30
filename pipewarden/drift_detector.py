"""Drift detection: compare a live schema against a saved snapshot and report changes."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewarden.schema import TableSchema
from pipewarden.differ import diff_schemas, SchemaDiff
from pipewarden.watcher import load_snapshot


@dataclass
class DriftReport:
    table_name: str
    has_drift: bool
    diff: Optional[SchemaDiff] = None
    snapshot_missing: bool = False
    message: str = ""

    def summary(self) -> str:
        if self.snapshot_missing:
            return f"[{self.table_name}] No snapshot found — cannot detect drift."
        if not self.has_drift:
            return f"[{self.table_name}] No drift detected."
        return f"[{self.table_name}] Drift detected: {self.diff.summary() if self.diff else 'unknown changes'}"


@dataclass
class DriftSummary:
    reports: List[DriftReport] = field(default_factory=list)

    def add(self, report: DriftReport) -> None:
        self.reports.append(report)

    @property
    def total(self) -> int:
        return len(self.reports)

    @property
    def drifted(self) -> int:
        return sum(1 for r in self.reports if r.has_drift)

    @property
    def clean(self) -> int:
        return sum(1 for r in self.reports if not r.has_drift and not r.snapshot_missing)

    @property
    def missing_snapshots(self) -> int:
        return sum(1 for r in self.reports if r.snapshot_missing)


def detect_schema_drift(table_name: str, live_schema: TableSchema, snapshot_dir: str) -> DriftReport:
    """Compare a live TableSchema against its saved snapshot."""
    snapshot = load_snapshot(snapshot_dir, table_name)
    if snapshot is None:
        return DriftReport(
            table_name=table_name,
            has_drift=False,
            snapshot_missing=True,
            message="Snapshot not found.",
        )
    diff = diff_schemas(snapshot, live_schema)
    return DriftReport(
        table_name=table_name,
        has_drift=diff.has_changes,
        diff=diff,
        message=diff.summary(),
    )


def detect_drift_for_schemas(
    schemas: Dict[str, TableSchema], snapshot_dir: str
) -> DriftSummary:
    """Run drift detection for multiple schemas and return a consolidated summary."""
    summary = DriftSummary()
    for name, schema in schemas.items():
        report = detect_schema_drift(name, schema, snapshot_dir)
        summary.add(report)
    return summary
