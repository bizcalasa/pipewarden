"""Tests for pipewarden.transformer."""
import pytest
from pipewarden.transformer import (
    TransformRule,
    TransformReport,
    transform_rows,
)


def _upper_rule(field_name: str) -> TransformRule:
    return TransformRule(name="upper", field_name=field_name,
                         transform=lambda v: v.upper() if isinstance(v, str) else v)


def _strip_rule(field_name: str) -> TransformRule:
    return TransformRule(name="strip", field_name=field_name,
                         transform=lambda v: v.strip() if isinstance(v, str) else v)


def _int_rule(field_name: str) -> TransformRule:
    return TransformRule(name="to_int", field_name=field_name, transform=int)


def _boom_rule(field_name: str) -> TransformRule:
    def _raise(v):
        raise ValueError("boom")
    return TransformRule(name="boom", field_name=field_name, transform=_raise)


# --- TransformRule unit tests ---

def test_rule_applies_transform():
    rule = _upper_rule("name")
    result, ok = rule.apply("hello")
    assert ok is True
    assert result == "HELLO"


def test_rule_returns_original_on_exception():
    rule = _boom_rule("name")
    result, ok = rule.apply("hello")
    assert ok is False
    assert result == "hello"


# --- transform_rows tests ---

def test_no_rules_returns_rows_unchanged():
    rows = [{"a": 1, "b": "hello"}]
    out, report = transform_rows("t", rows, [])
    assert out == rows
    assert report.total_transformations == 0


def test_single_rule_applied():
    rows = [{"name": "alice"}]
    out, report = transform_rows("t", rows, [_upper_rule("name")])
    assert out[0]["name"] == "ALICE"
    assert report.total_transformations == 1


def test_chained_rules_applied_in_order():
    rows = [{"name": "  alice  "}]
    rules = [_strip_rule("name"), _upper_rule("name")]
    out, report = transform_rows("t", rows, rules)
    assert out[0]["name"] == "ALICE"
    assert report.total_transformations == 2


def test_missing_field_skipped_gracefully():
    rows = [{"age": 30}]
    out, report = transform_rows("t", rows, [_upper_rule("name")])
    assert out[0] == {"age": 30}
    assert report.total_transformations == 0


def test_rows_processed_count():
    rows = [{"x": "a"}, {"x": "b"}, {"x": "c"}]
    _, report = transform_rows("t", rows, [_upper_rule("x")])
    assert report.rows_processed == 3


def test_failed_transform_leaves_value_unchanged():
    rows = [{"score": "not-a-number"}]
    out, report = transform_rows("t", rows, [_int_rule("score")])
    assert out[0]["score"] == "not-a-number"
    assert report.total_transformations == 0


def test_transformations_for_field_filters_correctly():
    rows = [{"a": "hello", "b": "world"}]
    rules = [_upper_rule("a"), _upper_rule("b")]
    _, report = transform_rows("t", rows, rules)
    assert len(report.transformations_for_field("a")) == 1
    assert len(report.transformations_for_field("b")) == 1
    assert len(report.transformations_for_field("c")) == 0


def test_report_table_name_set():
    _, report = transform_rows("my_table", [], [])
    assert report.table_name == "my_table"


def test_no_record_when_value_unchanged():
    # transform returns same value — should not record
    rule = TransformRule(name="noop", field_name="x", transform=lambda v: v)
    rows = [{"x": 42}]
    _, report = transform_rows("t", rows, [rule])
    assert report.total_transformations == 0
