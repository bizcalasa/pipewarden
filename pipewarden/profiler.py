"""Field-level data profiling for ETL pipeline rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipewarden.schema import TableSchema


@dataclass
class FieldProfile:
    name: str
    total_count: int = 0
    null_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=dict)

    @property
    def null_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count

    @property
    def dominant_type(self) -> Optional[str]:
        if not self.type_counts:
            return None
        return max(self.type_counts, key=lambda k: self.type_counts[k])


@dataclass
class ProfileReport:
    table_name: str
    row_count: int
    fields: Dict[str, FieldProfile] = field(default_factory=dict)

    def get_field(self, name: str) -> Optional[FieldProfile]:
        return self.fields.get(name)


def profile_rows(
    schema: TableSchema,
    rows: List[Dict[str, Any]],
) -> ProfileReport:
    """Profile a list of rows against the given schema."""
    report = ProfileReport(
        table_name=schema.name,
        row_count=len(rows),
        fields={f.name: FieldProfile(name=f.name) for f in schema.fields},
    )

    for row in rows:
        for field_schema in schema.fields:
            fp = report.fields[field_schema.name]
            fp.total_count += 1
            value = row.get(field_schema.name)
            if value is None:
                fp.null_count += 1
            else:
                type_name = type(value).__name__
                fp.type_counts[type_name] = fp.type_counts.get(type_name, 0) + 1

    return report
