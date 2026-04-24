"""Schema drift watcher for pipewarden.

Provides functionality to snapshot schema state and detect drift over time
by comparing a current schema directory against a previously saved snapshot.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from pipewarden.differ import SchemaDiff, diff_schemas
from pipewarden.schema import TableSchema
from pipewarden.schema_loader import load_schemas_from_dir


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def _schema_to_dict(schema: TableSchema) -> dict:
    """Serialise a TableSchema to a plain dict suitable for JSON storage."""
    return {
        "table": schema.table_name,
        "fields": [
            {
                "name": f.name,
                "type": f.field_type.value,
                "nullable": f.nullable,
            }
            for f in schema.fields
        ],
    }


def save_snapshot(schemas: Dict[str, TableSchema], snapshot_path: str | Path) -> None:
    """Write a snapshot of *schemas* to *snapshot_path* as JSON.

    Args:
        schemas: Mapping of table name -> TableSchema to snapshot.
        snapshot_path: Destination file path (will be created/overwritten).
    """
    snapshot_path = Path(snapshot_path)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {name: _schema_to_dict(schema) for name, schema in schemas.items()}
    with snapshot_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def load_snapshot(snapshot_path: str | Path) -> Dict[str, TableSchema]:
    """Load a previously saved snapshot from *snapshot_path*.

    Args:
        snapshot_path: Path to a JSON snapshot file produced by :func:`save_snapshot`.

    Returns:
        Mapping of table name -> TableSchema.

    Raises:
        FileNotFoundError: If *snapshot_path* does not exist.
        ValueError: If the snapshot JSON is malformed.
    """
    from pipewarden.schema_loader import load_schema_from_dict  # local import to avoid cycles

    snapshot_path = Path(snapshot_path)
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot file not found: {snapshot_path}")

    with snapshot_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    schemas: Dict[str, TableSchema] = {}
    for name, raw in payload.items():
        schemas[name] = load_schema_from_dict(raw)
    return schemas


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------

def detect_drift(
    current_schemas: Dict[str, TableSchema],
    snapshot_schemas: Dict[str, TableSchema],
) -> Dict[str, SchemaDiff]:
    """Compare *current_schemas* against *snapshot_schemas* and return any diffs.

    Only tables that exist in **both** mappings are compared.  Tables that
    appear only in the current set are reported as *new* (empty diff), and
    tables that have disappeared are reported with all fields removed.

    Args:
        current_schemas: The live/current schema state.
        snapshot_schemas: The previously snapshotted schema state.

    Returns:
        A dict mapping table name -> :class:`~pipewarden.differ.SchemaDiff`.
        Only entries with actual changes (or removals) are included.
    """
    results: Dict[str, SchemaDiff] = {}

    all_tables = set(current_schemas) | set(snapshot_schemas)

    for table in sorted(all_tables):
        current = current_schemas.get(table)
        snapshot = snapshot_schemas.get(table)

        if current is None:
            # Table was removed — treat snapshot as "old", compare against empty
            empty = TableSchema(table_name=table, fields=[])
            diff = diff_schemas(old=snapshot, new=empty)  # type: ignore[arg-type]
        elif snapshot is None:
            # Brand-new table — no prior snapshot to compare against; skip.
            continue
        else:
            diff = diff_schemas(old=snapshot, new=current)

        if diff.has_changes:
            results[table] = diff

    return results


def watch_directory(
    schema_dir: str | Path,
    snapshot_path: str | Path,
    *,
    update_snapshot: bool = False,
) -> Dict[str, SchemaDiff]:
    """High-level helper: load schemas from *schema_dir*, compare against snapshot.

    Args:
        schema_dir: Directory containing YAML/JSON schema definition files.
        snapshot_path: Path to the snapshot file to compare against.
        update_snapshot: When ``True``, overwrite the snapshot with the current
            state after comparison (useful for CI "ratchet" workflows).

    Returns:
        Mapping of table name -> :class:`~pipewarden.differ.SchemaDiff` for
        every table where drift was detected.  Empty dict means no drift.
    """
    current = load_schemas_from_dir(str(schema_dir))

    try:
        snapshot = load_snapshot(snapshot_path)
    except FileNotFoundError:
        # No previous snapshot — create one and report no drift.
        save_snapshot(current, snapshot_path)
        return {}

    diffs = detect_drift(current, snapshot)

    if update_snapshot:
        save_snapshot(current, snapshot_path)

    return diffs
