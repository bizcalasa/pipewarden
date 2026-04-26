"""Tests for pipewarden.lineage and pipewarden.lineage_reporter."""

import pytest
from pipewarden.lineage import LineageNode, LineageGraph
from pipewarden.lineage_reporter import (
    format_lineage_report,
    lineage_summary,
    format_upstream,
    format_downstream,
)


def _node(table: str, field: str) -> LineageNode:
    return LineageNode(table=table, field_name=field)


def test_node_repr():
    n = _node("orders", "customer_id")
    assert repr(n) == "orders.customer_id"


def test_node_equality():
    assert _node("a", "x") == _node("a", "x")
    assert _node("a", "x") != _node("a", "y")


def test_node_hashable():
    s = {_node("a", "x"), _node("a", "x"), _node("b", "y")}
    assert len(s) == 2


def test_add_edge_records_downstream():
    g = LineageGraph()
    src = _node("raw", "id")
    tgt = _node("clean", "id")
    g.add_edge(src, tgt)
    assert tgt in g.downstream(src)


def test_add_edge_records_upstream():
    g = LineageGraph()
    src = _node("raw", "id")
    tgt = _node("clean", "id")
    g.add_edge(src, tgt)
    assert src in g.upstream(tgt)


def test_no_duplicate_edges():
    g = LineageGraph()
    src = _node("a", "x")
    tgt = _node("b", "x")
    g.add_edge(src, tgt)
    g.add_edge(src, tgt)
    assert len(g.downstream(src)) == 1


def test_all_nodes_includes_both_ends():
    g = LineageGraph()
    src = _node("raw", "id")
    tgt = _node("clean", "id")
    g.add_edge(src, tgt)
    nodes = g.all_nodes()
    assert src in nodes
    assert tgt in nodes


def test_to_dict_round_trips():
    g = LineageGraph()
    g.add_edge(_node("a", "x"), _node("b", "x"))
    g.add_edge(_node("b", "x"), _node("c", "x"))
    d = g.to_dict()
    g2 = LineageGraph.from_dict(d)
    assert g2.to_dict() == d


def test_empty_graph_to_dict():
    g = LineageGraph()
    assert g.to_dict() == {}


def test_format_lineage_report_empty():
    g = LineageGraph()
    report = format_lineage_report(g)
    assert "no lineage edges" in report


def test_format_lineage_report_shows_edges():
    g = LineageGraph()
    g.add_edge(_node("raw", "user_id"), _node("clean", "user_id"))
    report = format_lineage_report(g)
    assert "raw.user_id" in report
    assert "clean.user_id" in report
    assert "->" in report


def test_lineage_summary_counts():
    g = LineageGraph()
    g.add_edge(_node("a", "x"), _node("b", "x"))
    g.add_edge(_node("a", "x"), _node("c", "x"))
    summary = lineage_summary(g)
    assert "3 nodes" in summary
    assert "2 edges" in summary


def test_lineage_summary_empty():
    g = LineageGraph()
    summary = lineage_summary(g)
    assert "0 nodes" in summary
    assert "0 edges" in summary


def test_format_upstream_no_sources():
    g = LineageGraph()
    n = _node("clean", "id")
    result = format_upstream(n, g)
    assert "no upstream" in result


def test_format_downstream_no_targets():
    g = LineageGraph()
    n = _node("raw", "id")
    result = format_downstream(n, g)
    assert "no downstream" in result
