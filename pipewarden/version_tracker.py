"""Track schema versions over time, assigning monotonic version numbers and recording change history."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewarden.differ import diff_schemas
from pipewarden.schema import TableSchema


@dataclass
class SchemaVersion:
    version: int
    table_name: str
    timestamp: str
    change_summary: str
    fields: Dict[str, str]  # field_name -> type string

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "table_name": self.table_name,
            "timestamp": self.timestamp,
            "change_summary": self.change_summary,
            "fields": self.fields,
        }

    @staticmethod
    def from_dict(d: dict) -> "SchemaVersion":
        return SchemaVersion(
            version=d["version"],
            table_name=d["table_name"],
            timestamp=d["timestamp"],
            change_summary=d["change_summary"],
            fields=d["fields"],
        )


@dataclass
class VersionHistory:
    table_name: str
    versions: List[SchemaVersion] = field(default_factory=list)

    def latest(self) -> Optional[SchemaVersion]:
        return self.versions[-1] if self.versions else None

    def next_version_number(self) -> int:
        return (self.versions[-1].version + 1) if self.versions else 1


def _schema_fields_dict(schema: TableSchema) -> Dict[str, str]:
    return {f.name: f.type.value for f in schema.fields}


def record_version(
    history: VersionHistory,
    schema: TableSchema,
    previous: Optional[TableSchema] = None,
) -> SchemaVersion:
    """Append a new version entry to *history* and return it."""
    if previous is not None:
        diff = diff_schemas(previous, schema)
        summary = diff.summary() if diff.has_changes() else "no changes"
    else:
        summary = "initial version"

    version = SchemaVersion(
        version=history.next_version_number(),
        table_name=schema.name,
        timestamp=datetime.now(timezone.utc).isoformat(),
        change_summary=summary,
        fields=_schema_fields_dict(schema),
    )
    history.versions.append(version)
    return version


def save_history(history: VersionHistory, path: str) -> None:
    with open(path, "w") as fh:
        json.dump([v.to_dict() for v in history.versions], fh, indent=2)


def load_history(table_name: str, path: str) -> VersionHistory:
    if not os.path.exists(path):
        return VersionHistory(table_name=table_name)
    with open(path) as fh:
        raw = json.load(fh)
    versions = [SchemaVersion.from_dict(d) for d in raw]
    return VersionHistory(table_name=table_name, versions=versions)
