"""Formatting helpers for field-level lineage reports."""

from __future__ import annotations
from typing import List
from pipewarden.field_lineage import FieldRef, FieldLineageGraph


def format_field_ref(ref: FieldRef) -> str:
    return f"{ref.table}.{ref.field}"


def format_upstream(ref: FieldRef, graph: FieldLineageGraph) -> str:
    sources = graph.upstream(ref)
    if not sources:
        return f"  {format_field_ref(ref)}  <-- (root)"
    lines = [f"  {format_field_ref(ref)}  <-- upstream:"]
    for src in sources:
        lines.append(f"    - {format_field_ref(src)}")
    return "\n".join(lines)


def format_downstream(ref: FieldRef, graph: FieldLineageGraph) -> str:
    derived = graph.downstream(ref)
    if not derived:
        return f"  {format_field_ref(ref)}  --> (leaf)"
    lines = [f"  {format_field_ref(ref)}  --> downstream:"]
    for dst in derived:
        lines.append(f"    - {format_field_ref(dst)}")
    return "\n".join(lines)


def format_mappings(graph: FieldLineageGraph) -> str:
    pairs = graph.mappings()
    if not pairs:
        return "No field mappings recorded."
    lines = ["Field Lineage Mappings:", "-" * 40]
    for src, tgt in pairs:
        lines.append(f"  {format_field_ref(src)}  -->  {format_field_ref(tgt)}")
    return "\n".join(lines)


def format_field_lineage_report(graph: FieldLineageGraph) -> str:
    refs = graph.all_refs()
    if not refs:
        return "Field lineage graph is empty."
    lines = ["Field Lineage Report", "=" * 40]
    for ref in refs:
        marker = "[root]" if graph.is_root(ref) else ""
        marker += " [leaf]" if graph.is_leaf(ref) else ""
        label = f"{format_field_ref(ref)}  {marker.strip()}"
        lines.append(label)
        for src in graph.upstream(ref):
            lines.append(f"    upstream:   {format_field_ref(src)}")
        for dst in graph.downstream(ref):
            lines.append(f"    downstream: {format_field_ref(dst)}")
    return "\n".join(lines)


def field_lineage_summary(graph: FieldLineageGraph) -> str:
    total_refs = len(graph.all_refs())
    total_mappings = len(graph.mappings())
    roots = sum(1 for r in graph.all_refs() if graph.is_root(r))
    leaves = sum(1 for r in graph.all_refs() if graph.is_leaf(r))
    return (
        f"Field lineage: {total_refs} fields, "
        f"{total_mappings} mappings, "
        f"{roots} root(s), {leaves} leaf/leaves"
    )
