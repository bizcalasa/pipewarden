"""Field-level lineage tracking: maps source fields to derived/destination fields."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class FieldRef:
    """A reference to a specific field within a table."""
    table: str
    field: str

    def __repr__(self) -> str:
        return f"{self.table}.{self.field}"

    def __hash__(self) -> int:
        return hash((self.table, self.field))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldRef):
            return NotImplemented
        return self.table == other.table and self.field == other.field


@dataclass
class FieldLineageGraph:
    """Directed graph tracking field-level data lineage."""
    _edges: Dict[FieldRef, Set[FieldRef]] = field(default_factory=dict)  # source -> derived
    _reverse: Dict[FieldRef, Set[FieldRef]] = field(default_factory=dict)  # derived -> sources

    def add_mapping(self, source: FieldRef, derived: FieldRef) -> None:
        """Record that `derived` is produced from `source`."""
        self._edges.setdefault(source, set()).add(derived)
        self._reverse.setdefault(derived, set()).add(source)

    def downstream(self, ref: FieldRef) -> List[FieldRef]:
        """Return all fields directly derived from `ref`."""
        return sorted(self._edges.get(ref, set()), key=lambda r: (r.table, r.field))

    def upstream(self, ref: FieldRef) -> List[FieldRef]:
        """Return all fields that `ref` is derived from."""
        return sorted(self._reverse.get(ref, set()), key=lambda r: (r.table, r.field))

    def all_refs(self) -> List[FieldRef]:
        """Return every FieldRef known to the graph."""
        all_nodes: Set[FieldRef] = set(self._edges.keys()) | set(self._reverse.keys())
        return sorted(all_nodes, key=lambda r: (r.table, r.field))

    def mappings(self) -> List[Tuple[FieldRef, FieldRef]]:
        """Return all (source, derived) pairs."""
        result = []
        for src, targets in self._edges.items():
            for tgt in targets:
                result.append((src, tgt))
        return sorted(result, key=lambda p: (p[0].table, p[0].field, p[1].table, p[1].field))

    def is_root(self, ref: FieldRef) -> bool:
        """True if `ref` has no upstream sources."""
        return ref not in self._reverse or len(self._reverse[ref]) == 0

    def is_leaf(self, ref: FieldRef) -> bool:
        """True if `ref` has no downstream derived fields."""
        return ref not in self._edges or len(self._edges[ref]) == 0
