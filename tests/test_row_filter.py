"""Tests for pipewarden.row_filter."""
import pytest
from pipewarden.row_filter import FilterRule, FilterResult, RowFilter


def _positive_rule() -> FilterRule:
    return FilterRule(name="always_keep", predicate=lambda row: True, description="keeps all")


def _age_rule() -> FilterRule:
    return FilterRule(name="min_age", predicate=lambda row: row.get("age", 0) >= 18)


def _nonempty_name_rule() -> FilterRule:
    return FilterRule(name="nonempty_name", predicate=lambda row: bool(row.get("name")))


def _boom_rule() -> FilterRule:
    def _raise(row):
        raise RuntimeError("boom")
    return FilterRule(name="boom", predicate=_raise)


def test_rule_matches_when_predicate_true():
    rule = _positive_rule()
    assert rule.matches({"x": 1}) is True


def test_rule_does_not_match_when_predicate_false():
    rule = _age_rule()
    assert rule.matches({"age": 10}) is False


def test_rule_handles_exception_gracefully():
    rule = _boom_rule()
    assert rule.matches({}) is False


def test_filter_keeps_all_when_no_rules():
    rf = RowFilter(table="users")
    rows = [{"id": 1}, {"id": 2}]
    result = rf.apply(rows)
    assert result.kept_count == 2
    assert result.dropped_count == 0


def test_filter_drops_rows_failing_predicate():
    rf = RowFilter(table="users")
    rf.add(_age_rule())
    rows = [{"age": 10}, {"age": 25}, {"age": 18}]
    result = rf.apply(rows)
    assert result.kept_count == 2
    assert result.dropped_count == 1


def test_filter_all_rules_must_pass():
    rf = RowFilter(table="users")
    rf.add(_age_rule())
    rf.add(_nonempty_name_rule())
    rows = [
        {"age": 20, "name": "Alice"},
        {"age": 20, "name": ""},
        {"age": 15, "name": "Bob"},
    ]
    result = rf.apply(rows)
    assert result.kept_count == 1
    assert result.dropped_count == 2


def test_drop_rate_zero_when_no_rows():
    rf = RowFilter(table="empty")
    result = rf.apply([])
    assert result.drop_rate == 0.0
    assert result.total_rows == 0


def test_drop_rate_calculation():
    rf = RowFilter(table="users")
    rf.add(_age_rule())
    rows = [{"age": 5}, {"age": 20}, {"age": 30}, {"age": 10}]
    result = rf.apply(rows)
    assert result.drop_rate == pytest.approx(0.5)


def test_rules_for_returns_correct_rule():
    rf = RowFilter(table="users")
    rule = _age_rule()
    rf.add(rule)
    found = rf.rules_for("min_age")
    assert found is rule


def test_rules_for_returns_none_when_missing():
    rf = RowFilter(table="users")
    assert rf.rules_for("nonexistent") is None


def test_filter_result_table_name():
    rf = RowFilter(table="orders")
    result = rf.apply([{"id": 1}])
    assert result.table == "orders"
