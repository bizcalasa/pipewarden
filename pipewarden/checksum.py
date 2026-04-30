"""Schema and data checksum utilities for detecting silent changes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipewarden.schema import TableSchema


def _stable_json(obj: Any) -> str:
    """Serialize obj to JSON with sorted keys for stable hashing."""
    return json.dumps(obj, sort_keys=True, default=str)


def checksum_schema(schema: TableSchema) -> str:
    """Return a SHA-256 hex digest representing the schema structure."""
    fields = [
        {"name": f.name, "type": f.type.value, "nullable": f.nullable}
        for f in sorted(schema.fields, key=lambda x: x.name)
    ]
    payload = {"table": schema.name, "fields": fields}
    return hashlib.sha256(_stable_json(payload).encode()).hexdigest()


def checksum_rows(rows: List[Dict[str, Any]]) -> str:
    """Return a SHA-256 hex digest over a list of data rows."""
    payload = [_stable_json(row) for row in rows]
    combined = "\n".join(payload)
    return hashlib.sha256(combined.encode()).hexdigest()


@dataclass
class ChecksumRecord:
    table: str
    schema_checksum: str
    data_checksum: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "schema_checksum": self.schema_checksum,
            "data_checksum": self.data_checksum,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ChecksumRecord":
        return ChecksumRecord(
            table=d["table"],
            schema_checksum=d["schema_checksum"],
            data_checksum=d.get("data_checksum"),
        )


@dataclass
class ChecksumReport:
    records: List[ChecksumRecord] = field(default_factory=list)

    def add(self, record: ChecksumRecord) -> None:
        self.records.append(record)

    def get(self, table: str) -> Optional[ChecksumRecord]:
        for r in self.records:
            if r.table == table:
                return r
        return None

    def table_names(self) -> List[str]:
        return [r.table for r in self.records]
