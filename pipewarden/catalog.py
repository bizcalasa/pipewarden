"""Schema catalog: registry for tracking and retrieving table schemas by name."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

from pipewarden.schema import TableSchema


@dataclass
class CatalogEntry:
    name: str
    schema: TableSchema
    source: Optional[str] = None  # e.g. file path or data-source label
    tags: List[str] = field(default_factory=list)

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


class SchemaCatalog:
    """In-memory registry of TableSchema objects keyed by table name."""

    def __init__(self) -> None:
        self._entries: Dict[str, CatalogEntry] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def register(
        self,
        schema: TableSchema,
        source: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> CatalogEntry:
        entry = CatalogEntry(
            name=schema.name,
            schema=schema,
            source=source,
            tags=list(tags or []),
        )
        self._entries[schema.name] = entry
        return entry

    def remove(self, name: str) -> bool:
        if name in self._entries:
            del self._entries[name]
            return True
        return False

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, name: str) -> Optional[CatalogEntry]:
        return self._entries.get(name)

    def all_entries(self) -> List[CatalogEntry]:
        return list(self._entries.values())

    def table_names(self) -> List[str]:
        return list(self._entries.keys())

    def filter_by_tag(self, tag: str) -> List[CatalogEntry]:
        return [e for e in self._entries.values() if e.has_tag(tag)]

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[CatalogEntry]:
        return iter(self._entries.values())

    def __contains__(self, name: str) -> bool:
        return name in self._entries
