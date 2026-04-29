"""Load a SchemaCatalog from a directory of schema files."""

from __future__ import annotations

import os
from typing import List, Optional

from pipewarden.catalog import SchemaCatalog
from pipewarden.schema_loader import load_schema_from_file


def load_catalog_from_dir(
    directory: str,
    tags: Optional[List[str]] = None,
    source_label: Optional[str] = None,
) -> SchemaCatalog:
    """Walk *directory* and register every .json schema file found.

    Parameters
    ----------
    directory:
        Path to scan for ``*.json`` schema files.
    tags:
        Optional list of tags applied to every entry loaded from this dir.
    source_label:
        Human-readable label recorded on each entry (defaults to the file path).
    """
    catalog = SchemaCatalog()
    if not os.path.isdir(directory):
        raise NotADirectoryError(f"Not a directory: {directory}")

    for fname in sorted(os.listdir(directory)):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(directory, fname)
        schema = load_schema_from_file(fpath)
        catalog.register(
            schema,
            source=source_label or fpath,
            tags=tags,
        )
    return catalog


def merge_catalogs(*catalogs: SchemaCatalog) -> SchemaCatalog:
    """Merge multiple catalogs into one; later catalogs overwrite earlier ones on name collision."""
    merged = SchemaCatalog()
    for cat in catalogs:
        for entry in cat:
            merged.register(entry.schema, source=entry.source, tags=list(entry.tags))
    return merged
