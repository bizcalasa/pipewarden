import pytest
from pipewarden.row_aggregator import (
    AggregationRule,
    AggregationRecord,
    RowAggregationReport,
    aggregate_rows,
)


def _rows():
    return [
        {"age": 10, "score": 80, "name": "alice"},
        {"age": 20, "score": 90, "name": "bob"},
        {"age": 30, "score": None, "name": "carol"},
    ]


def _sum_rule():
    return AggregationRule(name="sum_age", column="age", func=sum)


def _count_rule():
    return AggregationRule(
        name="count_scores",
        column="score",
        func=lambda vals: sum(1 for v in vals if v is not None),
    )


def test_aggregate_row_count():
    report = aggregate_rows("t", _rows(), [_sum_rule()])
    assert report.row_count == 3


def test_aggregate_sum_correct():
    report = aggregate_rows("t", _rows(), [_sum_rule()])
    rec = report.get("sum_age")
    assert rec is not None
    assert rec.result == 60


def test_aggregate_custom_func():
    report = aggregate_rows("t", _rows(), [_count_rule()])
    rec = report.get("count_scores")
    assert rec is not None
    assert rec.result == 2


def test_aggregate_multiple_rules():
    report = aggregate_rows("t", _rows(), [_sum_rule(), _count_rule()])
    assert len(report.records) == 2


def test_result_map_keys():
    report = aggregate_rows("t", _rows(), [_sum_rule(), _count_rule()])
    m = report.result_map()
    assert "sum_age" in m
    assert "count_scores" in m


def test_result_map_values():
    report = aggregate_rows("t", _rows(), [_sum_rule()])
    assert report.result_map()["sum_age"] == 60


def test_get_returns_none_for_missing():
    report = aggregate_rows("t", _rows(), [_sum_rule()])
    assert report.get("nonexistent") is None


def test_rule_returns_none_on_exception():
    boom = AggregationRule(name="boom", column="age", func=lambda v: 1 / 0)
    report = aggregate_rows("t", _rows(), [boom])
    assert report.get("boom").result is None


def test_empty_rows_sum_is_zero():
    report = aggregate_rows("t", [], [_sum_rule()])
    assert report.row_count == 0
    assert report.get("sum_age").result == 0


def test_column_cache_used_for_same_column():
    rule1 = AggregationRule(name="min_age", column="age", func=min)
    rule2 = AggregationRule(name="max_age", column="age", func=max)
    report = aggregate_rows("t", _rows(), [rule1, rule2])
    assert report.get("min_age").result == 10
    assert report.get("max_age").result == 30


def test_table_name_stored():
    report = aggregate_rows("my_table", _rows(), [])
    assert report.table == "my_table"


def test_no_rules_produces_empty_records():
    report = aggregate_rows("t", _rows(), [])
    assert report.records == []
