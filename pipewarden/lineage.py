"""Field-level lineage tracking for ETL pipeline schemas."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class LineageNode:
    """Represents a single table/field node in the lineage graph."""
    table: str
    field_name: str

    def __hash__(self) -> int:
        return hash((self.table, self.field_name))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LineageNode):
            return False
        return self.table == other.table and self.field_name == other.field_name

    def __repr__(self) -> str:
        return f"{self.table}.{self.field_name}"


@dataclass
class LineageGraph:
    """Directed graph tracking field-level data lineage."""
    _edges: Dict[LineageNode, List[LineageNode]] = field(default_factory=dict)

    def add_edge(self, source: LineageNode, target: LineageNode) -> None:
        """Register a lineage relationship from source to target."""
        if source not in self._edges:
            self._edges[source] = []
        if target not in self._edges[source]:
            self._edges[source].append(target)

    def upstream(self, node: LineageNode) -> List[LineageNode]:
        """Return all direct upstream sources for a given node."""
        return [
            src for src, targets in self._edges.items()
            if node in targets
        ]

    def downstream(self, node: LineageNode) -> List[LineageNode]:
        """Return all direct downstream targets for a given node."""
        return self._edges.get(node, [])

    def all_nodes(self) -> List[LineageNode]:
        """Return all unique nodes present in the graph."""
        nodes: set = set(self._edges.keys())
        for targets in self._edges.values():
            nodes.update(targets)
        return list(nodes)

    def ancestors(self, node: LineageNode) -> List[LineageNode]:
        """Return all transitive upstream ancestors for a given node.

        Performs a breadth-first traversal following upstream edges until
        no new sources are found.
        """
        visited: List[LineageNode] = []
        queue = self.upstream(node)
        while queue:
            current = queue.pop(0)
            if current not in visited:
                visited.append(current)
                queue.extend(self.upstream(current))
        return visited

    def to_dict(self) -> Dict[str, List[str]]:
        """Serialize the graph to a JSON-compatible dictionary."""
        return {
            repr(src): [repr(t) for t in targets]
            for src, targets in self._edges.items()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, List[str]]) -> "LineageGraph":
        """Deserialize a graph from a dictionary."""
        graph = cls()
        for src_str, target_strs in data.items():
            src_table, src_field = src_str.split(".", 1)
            src = LineageNode(table=src_table, field_name=src_field)
            for t_str in target_strs:
                t_table, t_field = t_str.split(".", 1)
                graph.add_edge(src, LineageNode(table=t_table, field_name=t_field))
        return graph
