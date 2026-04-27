"""Audit log for pipeline validation runs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


@dataclass
class AuditEntry:
    table: str
    run_at: str
    is_valid: bool
    error_count: int
    row_count: int
    source: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "run_at": self.run_at,
            "is_valid": self.is_valid,
            "error_count": self.error_count,
            "row_count": self.row_count,
            "source": self.source,
        }

    @staticmethod
    def from_dict(data: dict) -> "AuditEntry":
        return AuditEntry(
            table=data["table"],
            run_at=data["run_at"],
            is_valid=data["is_valid"],
            error_count=data["error_count"],
            row_count=data["row_count"],
            source=data.get("source"),
        )


@dataclass
class AuditLog:
    entries: List[AuditEntry] = field(default_factory=list)

    def record(self, entry: AuditEntry) -> None:
        self.entries.append(entry)

    def entries_for(self, table: str) -> List[AuditEntry]:
        return [e for e in self.entries if e.table == table]

    def last_entry(self, table: str) -> Optional[AuditEntry]:
        matches = self.entries_for(table)
        return matches[-1] if matches else None

    def table_names(self) -> List[str]:
        seen: list = []
        for e in self.entries:
            if e.table not in seen:
                seen.append(e.table)
        return seen


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_audit_log(log: AuditLog, path: Path) -> None:
    records = [e.to_dict() for e in log.entries]
    path.write_text(json.dumps(records, indent=2))


def load_audit_log(path: Path) -> AuditLog:
    if not path.exists():
        return AuditLog()
    raw = json.loads(path.read_text())
    entries = [AuditEntry.from_dict(r) for r in raw]
    return AuditLog(entries=entries)


def build_entry(
    table: str,
    is_valid: bool,
    error_count: int,
    row_count: int,
    source: Optional[str] = None,
) -> AuditEntry:
    return AuditEntry(
        table=table,
        run_at=now_utc(),
        is_valid=is_valid,
        error_count=error_count,
        row_count=row_count,
        source=source,
    )
