"""Human-readable formatting for SchemaCatalog."""

from __future__ import annotations

from typing import List

from pipewarden.catalog import CatalogEntry, SchemaCatalog
from pipewarden.schema import field_names


def format_entry(entry: CatalogEntry) -> str:
    """Single-line summary for a catalog entry."""
    tag_str = f"  [{', '.join(entry.tags)}]" if entry.tags else ""
    source_str = f"  (source: {entry.source})" if entry.source else ""
    field_count = len(field_names(entry.schema))
    return f"  {entry.name}: {field_count} field(s){tag_str}{source_str}"


def format_catalog(catalog: SchemaCatalog) -> str:
    """Full listing of all entries in the catalog."""
    if len(catalog) == 0:
        return "Catalog is empty."
    lines: List[str] = [f"Schema Catalog ({len(catalog)} table(s)):"]
    for entry in sorted(catalog, key=lambda e: e.name):
        lines.append(format_entry(entry))
    return "\n".join(lines)


def catalog_summary(catalog: SchemaCatalog) -> str:
    """One-line summary suitable for pipeline reports."""
    total = len(catalog)
    noun = "table" if total == 1 else "tables"
    return f"Catalog: {total} {noun} registered."
