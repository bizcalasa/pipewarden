"""Tests for field_lineage and field_lineage_reporter."""

import pytest
from pipewarden.field_lineage import FieldRef, FieldLineageGraph
from pipewarden.field_lineage_reporter import (
    format_field_ref,
    format_upstream,
    format_downstream,
    format_mappings,
    format_field_lineage_report,
    field_lineage_summary,
)


def _ref(table: str, field: str) -> FieldRef:
    return FieldRef(table=table, field=field)


def test_field_ref_repr():
    assert repr(_ref("orders", "user_id")) == "orders.user_id"


def test_field_ref_equality():
    assert _ref("t", "f") == _ref("t", "f")
    assert _ref("t", "f") != _ref("t", "g")


def test_field_ref_hashable():
    s = {_ref("t", "f"), _ref("t", "f"), _ref("t", "g")}
    assert len(s) == 2


def test_add_mapping_records_downstream():
    g = FieldLineageGraph()
    src = _ref("raw", "amount")
    dst = _ref("agg", "total")
    g.add_mapping(src, dst)
    assert dst in g.downstream(src)


def test_add_mapping_records_upstream():
    g = FieldLineageGraph()
    src = _ref("raw", "amount")
    dst = _ref("agg", "total")
    g.add_mapping(src, dst)
    assert src in g.upstream(dst)


def test_is_root_when_no_upstream():
    g = FieldLineageGraph()
    src = _ref("raw", "id")
    dst = _ref("clean", "id")
    g.add_mapping(src, dst)
    assert g.is_root(src)
    assert not g.is_root(dst)


def test_is_leaf_when_no_downstream():
    g = FieldLineageGraph()
    src = _ref("raw", "id")
    dst = _ref("clean", "id")
    g.add_mapping(src, dst)
    assert g.is_leaf(dst)
    assert not g.is_leaf(src)


def test_all_refs_returns_all_nodes():
    g = FieldLineageGraph()
    g.add_mapping(_ref("a", "x"), _ref("b", "y"))
    refs = g.all_refs()
    assert _ref("a", "x") in refs
    assert _ref("b", "y") in refs
    assert len(refs) == 2


def test_mappings_returns_all_pairs():
    g = FieldLineageGraph()
    g.add_mapping(_ref("a", "x"), _ref("b", "y"))
    g.add_mapping(_ref("a", "x"), _ref("c", "z"))
    pairs = g.mappings()
    assert len(pairs) == 2


def test_format_field_ref():
    assert format_field_ref(_ref("orders", "total")) == "orders.total"


def test_format_upstream_root():
    g = FieldLineageGraph()
    g.add_mapping(_ref("raw", "id"), _ref("clean", "id"))
    result = format_upstream(_ref("raw", "id"), g)
    assert "root" in result


def test_format_upstream_shows_sources():
    g = FieldLineageGraph()
    g.add_mapping(_ref("raw", "id"), _ref("clean", "id"))
    result = format_upstream(_ref("clean", "id"), g)
    assert "raw.id" in result


def test_format_downstream_leaf():
    g = FieldLineageGraph()
    g.add_mapping(_ref("raw", "id"), _ref("clean", "id"))
    result = format_downstream(_ref("clean", "id"), g)
    assert "leaf" in result


def test_format_mappings_empty():
    g = FieldLineageGraph()
    assert "No field mappings" in format_mappings(g)


def test_format_mappings_shows_arrow():
    g = FieldLineageGraph()
    g.add_mapping(_ref("raw", "amt"), _ref("agg", "total"))
    result = format_mappings(g)
    assert "-->" in result
    assert "raw.amt" in result
    assert "agg.total" in result


def test_format_field_lineage_report_empty():
    g = FieldLineageGraph()
    assert "empty" in format_field_lineage_report(g)


def test_format_field_lineage_report_contains_refs():
    g = FieldLineageGraph()
    g.add_mapping(_ref("src", "col"), _ref("dst", "col"))
    result = format_field_lineage_report(g)
    assert "src.col" in result
    assert "dst.col" in result


def test_field_lineage_summary_counts():
    g = FieldLineageGraph()
    g.add_mapping(_ref("a", "x"), _ref("b", "y"))
    s = field_lineage_summary(g)
    assert "2 fields" in s
    assert "1 mappings" in s
