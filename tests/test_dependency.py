"""Tests for pipewarden.dependency."""
import pytest
from pipewarden.dependency import DependencyNode, DependencyGraph


def _graph(*nodes: DependencyNode) -> DependencyGraph:
    g = DependencyGraph()
    for n in nodes:
        g.add_node(n)
    return g


def test_node_repr():
    n = DependencyNode("orders")
    assert repr(n) == "DependencyNode('orders')"


def test_node_equality():
    assert DependencyNode("a") == DependencyNode("a")
    assert DependencyNode("a") != DependencyNode("b")


def test_node_hashable():
    s = {DependencyNode("x"), DependencyNode("x"), DependencyNode("y")}
    assert len(s) == 2


def test_add_node_and_retrieve():
    g = DependencyGraph()
    n = DependencyNode("raw")
    g.add_node(n)
    assert g.get("raw") == n


def test_get_missing_returns_none():
    g = DependencyGraph()
    assert g.get("missing") is None


def test_node_names_lists_all():
    g = _graph(DependencyNode("a"), DependencyNode("b"))
    assert set(g.node_names()) == {"a", "b"}


def test_dependencies_of_returns_deps():
    g = _graph(DependencyNode("clean", depends_on=["raw"]))
    assert g.dependencies_of("clean") == ["raw"]


def test_dependencies_of_missing_returns_empty():
    g = DependencyGraph()
    assert g.dependencies_of("ghost") == []


def test_dependents_of_finds_downstream():
    g = _graph(
        DependencyNode("raw"),
        DependencyNode("clean", depends_on=["raw"]),
        DependencyNode("agg", depends_on=["clean"]),
    )
    assert g.dependents_of("raw") == ["clean"]
    assert g.dependents_of("clean") == ["agg"]


def test_dependents_of_missing_returns_empty():
    """A node not present in the graph should yield no dependents."""
    g = DependencyGraph()
    assert g.dependents_of("ghost") == []


def test_resolve_order_simple_chain():
    g = _graph(
        DependencyNode("agg", depends_on=["clean"]),
        DependencyNode("clean", depends_on=["raw"]),
        DependencyNode("raw"),
    )
    order = g.resolve_order()
    assert order.index("raw") < order.index("clean")
    assert order.index("clean") < order.index("agg")


def test_resolve_order_no_deps():
    g = _graph(DependencyNode("a"), DependencyNode("b"))
    order = g.resolve_order()
    assert set(order) == {"a", "b"}


def test_resolve_order_circular_raises():
    g = _graph(
        DependencyNode("a", depends_on=["b"]),
        DependencyNode("b", depends_on=["a"]),
    )
    with pytest.raises(ValueError, match="Circular dependency"):
        g.resolve_order()


def test_resolve_order_diamond():
    """A diamond-shaped dependency graph should resolve without errors.

    Both 'left' and 'right' depend on 'root', and 'tip' depends on both.
    The resolved order must place 'root' first and 'tip' last.
    """
    g = _graph(
        DependencyNode("root"),
        DependencyNode("left", depends_on=["root"]),
        DependencyNode("right", depends_on=["root"]),
        DependencyNode("tip", depends_on=["left", "right"]),
    )
    order = g.resolve_order()
    assert order.index("root") < order.index("left")
    assert order.index("root") < order.index("right")
    assert order.index("left") < order.index("tip")
    assert order.index("right") < order.index("tip")
