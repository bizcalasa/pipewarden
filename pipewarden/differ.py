"""Schema diff utility for comparing two TableSchema definitions."""

from dataclasses import dataclass, field
from typing import List
from pipewarden.schema import TableSchema


@dataclass
class SchemaDiff:
    """Result of comparing two TableSchema instances."""
    table_name: str
    added_fields: List[str] = field(default_factory=list)
    removed_fields: List[str] = field(default_factory=list)
    type_changes: List[str] = field(default_factory=list)
    nullability_changes: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_fields
            or self.removed_fields
            or self.type_changes
            or self.nullability_changes
        )

    def summary(self) -> str:
        if not self.has_changes:
            return f"[{self.table_name}] No schema changes detected."
        lines = [f"[{self.table_name}] Schema differences:"]
        for f_name in self.added_fields:
            lines.append(f"  + ADDED   {f_name}")
        for f_name in self.removed_fields:
            lines.append(f"  - REMOVED {f_name}")
        for msg in self.type_changes:
            lines.append(f"  ~ TYPE    {msg}")
        for msg in self.nullability_changes:
            lines.append(f"  ~ NULL    {msg}")
        return "\n".join(lines)

    def is_breaking(self) -> bool:
        """Return True if the diff contains breaking changes.

        Breaking changes are those that may cause downstream consumers or
        pipelines to fail: removed fields and type changes are considered
        breaking, while added fields and nullability changes are not.
        """
        return bool(self.removed_fields or self.type_changes)


def diff_schemas(old: TableSchema, new: TableSchema) -> SchemaDiff:
    """Compare two TableSchema objects and return a SchemaDiff."""
    result = SchemaDiff(table_name=new.name)

    old_fields = {f.name: f for f in old.fields}
    new_fields = {f.name: f for f in new.fields}

    old_names = set(old_fields)
    new_names = set(new_fields)

    result.added_fields = sorted(new_names - old_names)
    result.removed_fields = sorted(old_names - new_names)

    for name in sorted(old_names & new_names):
        old_f = old_fields[name]
        new_f = new_fields[name]
        if old_f.field_type != new_f.field_type:
            result.type_changes.append(
                f"{name}: {old_f.field_type.value} -> {new_f.field_type.value}"
            )
        if old_f.nullable != new_f.nullable:
            result.nullability_changes.append(
                f"{name}: nullable={old_f.nullable} -> nullable={new_f.nullable}"
            )

    return result
