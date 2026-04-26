"""Human-readable reporting for field lineage graphs."""

from pipewarden.lineage import LineageGraph, LineageNode
from typing import List


def format_node(node: LineageNode) -> str:
    return f"{node.table}.{node.field_name}"


def format_upstream(node: LineageNode, graph: LineageGraph) -> str:
    """Format upstream sources for a node."""
    sources = graph.upstream(node)
    if not sources:
        return f"  {format_node(node)} <- (no upstream)"
    lines = [f"  {format_node(node)} <-"]
    for src in sources:
        lines.append(f"    {format_node(src)}")
    return "\n".join(lines)


def format_downstream(node: LineageNode, graph: LineageGraph) -> str:
    """Format downstream targets for a node."""
    targets = graph.downstream(node)
    if not targets:
        return f"  {format_node(node)} -> (no downstream)"
    lines = [f"  {format_node(node)} ->"]
    for t in targets:
        lines.append(f"    {format_node(t)}")
    return "\n".join(lines)


def format_lineage_report(graph: LineageGraph) -> str:
    """Format a full lineage report showing all edges."""
    lines = ["=== Field Lineage Report ==="]
    edge_dict = graph.to_dict()
    if not edge_dict:
        lines.append("  (no lineage edges registered)")
        return "\n".join(lines)
    for src_str, target_strs in sorted(edge_dict.items()):
        for t_str in target_strs:
            lines.append(f"  {src_str} -> {t_str}")
    return "\n".join(lines)


def lineage_summary(graph: LineageGraph) -> str:
    """Return a one-line summary of the lineage graph size."""
    node_count = len(graph.all_nodes())
    edge_count = sum(len(v) for v in graph.to_dict().values())
    n_word = "node" if node_count == 1 else "nodes"
    e_word = "edge" if edge_count == 1 else "edges"
    return f"Lineage graph: {node_count} {n_word}, {edge_count} {e_word}"
