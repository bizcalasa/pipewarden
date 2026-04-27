"""Dependency resolution for ETL pipeline tables."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class DependencyNode:
    name: str
    depends_on: List[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DependencyNode) and self.name == other.name

    def __repr__(self) -> str:
        return f"DependencyNode({self.name!r})"


class DependencyGraph:
    def __init__(self) -> None:
        self._nodes: Dict[str, DependencyNode] = {}

    def add_node(self, node: DependencyNode) -> None:
        self._nodes[node.name] = node

    def node_names(self) -> List[str]:
        return list(self._nodes.keys())

    def get(self, name: str) -> Optional[DependencyNode]:
        return self._nodes.get(name)

    def dependencies_of(self, name: str) -> List[str]:
        node = self._nodes.get(name)
        return list(node.depends_on) if node else []

    def dependents_of(self, name: str) -> List[str]:
        return [
            n.name for n in self._nodes.values() if name in n.depends_on
        ]

    def resolve_order(self) -> List[str]:
        """Return a topologically sorted list of node names.

        Raises ValueError on circular dependencies.
        """
        visited: Set[str] = set()
        visiting: Set[str] = set()
        order: List[str] = []

        def visit(name: str) -> None:
            if name in visiting:
                raise ValueError(f"Circular dependency detected at '{name}'")
            if name in visited:
                return
            visiting.add(name)
            for dep in self.dependencies_of(name):
                visit(dep)
            visiting.discard(name)
            visited.add(name)
            order.append(name)

        for name in self._nodes:
            visit(name)

        return order
