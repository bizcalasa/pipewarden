"""Human-readable formatting for SchemaDiff results."""

from __future__ import annotations

from pipewarden.differ import SchemaDiff


def format_added(diff: SchemaDiff) -> str:
    if not diff.added:
        return ""
    lines = ["  Added fields:"]
    for name, field in diff.added.items():
        nullable = "nullable" if field.nullable else "required"
        lines.append(f"    + {name} ({field.field_type.value}, {nullable})")
    return "\n".join(lines)


def format_removed(diff: SchemaDiff) -> str:
    if not diff.removed:
        return ""
    lines = ["  Removed fields:"]
    for name, field in diff.removed.items():
        nullable = "nullable" if field.nullable else "required"
        lines.append(f"    - {name} ({field.field_type.value}, {nullable})")
    return "\n".join(lines)


def format_changed(diff: SchemaDiff) -> str:
    if not diff.changed:
        return ""
    lines = ["  Changed fields:"]
    for name, (old, new) in diff.changed.items():
        parts = []
        if old.field_type != new.field_type:
            parts.append(f"type {old.field_type.value} -> {new.field_type.value}")
        if old.nullable != new.nullable:
            old_n = "nullable" if old.nullable else "required"
            new_n = "nullable" if new.nullable else "required"
            parts.append(f"{old_n} -> {new_n}")
        lines.append(f"    ~ {name}: {', '.join(parts)}")
    return "\n".join(lines)


def format_schema_diff(table_name: str, diff: SchemaDiff) -> str:
    if not diff.has_changes():
        return f"[{table_name}] No schema changes detected."

    header = f"[{table_name}] Schema diff — {diff.summary()}"
    if diff.is_breaking():
        header += " [BREAKING]"

    sections = [header]
    for section in (format_added(diff), format_removed(diff), format_changed(diff)):
        if section:
            sections.append(section)
    return "\n".join(sections)


def schema_diff_summary(diffs: dict[str, SchemaDiff]) -> str:
    if not diffs:
        return "No schema diffs to report."
    total = len(diffs)
    changed = sum(1 for d in diffs.values() if d.has_changes())
    breaking = sum(1 for d in diffs.values() if d.is_breaking())
    return (
        f"{total} table(s) compared: {changed} with changes, "
        f"{breaking} breaking."
    )
