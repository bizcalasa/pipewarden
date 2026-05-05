import pytest
from pipewarden.row_aggregator import (
    AggregationRule,
    RowAggregationReport,
    AggregationRecord,
    aggregate_rows,
)
from pipewarden.row_aggregator_reporter import (
    format_aggregation_report,
    aggregation_summary,
)


def _report():
    rules = [
        AggregationRule(name="total", column="amount", func=sum),
        AggregationRule(name="avg", column="amount", func=lambda v: sum(v) / len(v) if v else None),
    ]
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    return aggregate_rows("orders", rows, rules)


def test_format_report_contains_table_name():
    out = format_aggregation_report(_report())
    assert "orders" in out


def test_format_report_shows_row_count():
    out = format_aggregation_report(_report())
    assert "3" in out


def test_format_report_shows_rule_names():
    out = format_aggregation_report(_report())
    assert "total" in out
    assert "avg" in out


def test_format_report_shows_column_names():
    out = format_aggregation_report(_report())
    assert "amount" in out


def test_format_report_no_rules_shows_message():
    report = aggregate_rows("t", [], [])
    out = format_aggregation_report(report)
    assert "No aggregations" in out


def test_format_report_none_result_shows_na():
    rule = AggregationRule(name="boom", column="x", func=lambda v: 1 / 0)
    report = aggregate_rows("t", [{"x": 1}], [rule])
    out = format_aggregation_report(report)
    assert "N/A" in out


def test_summary_contains_table_name():
    out = aggregation_summary(_report())
    assert "orders" in out


def test_summary_contains_row_count():
    out = aggregation_summary(_report())
    assert "3" in out


def test_summary_singular_noun():
    rules = [AggregationRule(name="total", column="x", func=sum)]
    report = aggregate_rows("t", [{"x": 1}], rules)
    out = aggregation_summary(report)
    assert "aggregation" in out
    assert "aggregations" not in out


def test_summary_plural_noun():
    out = aggregation_summary(_report())
    assert "aggregations" in out
