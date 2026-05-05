"""Tests for pipewarden.column_mapper and column_mapper_reporter."""
import pytest

from pipewarden.column_mapper import (
    MappingRule,
    ColumnMappingReport,
    map_rows,
)
from pipewarden.column_mapper_reporter import (
    format_column_mapping_report,
    mapping_summary,
)


# ---------------------------------------------------------------------------
# MappingRule
# ---------------------------------------------------------------------------

def test_rule_repr_contains_fields():
    rule = MappingRule("src", "tgt")
    assert "src" in repr(rule)
    assert "tgt" in repr(rule)


def test_rule_apply_no_transform_returns_value():
    rule = MappingRule("a", "b")
    assert rule.apply(42) == 42


def test_rule_apply_with_transform():
    rule = MappingRule("a", "b", transform=str.upper)
    assert rule.apply("hello") == "HELLO"


def test_rule_apply_exception_returns_original():
    def boom(v):
        raise ValueError("nope")

    rule = MappingRule("a", "b", transform=boom)
    assert rule.apply("x") == "x"


# ---------------------------------------------------------------------------
# map_rows
# ---------------------------------------------------------------------------

def _rows():
    return [
        {"first_name": "Alice", "age": 30},
        {"first_name": "Bob", "age": 25},
    ]


def test_map_rows_renames_field():
    rules = [MappingRule("first_name", "name")]
    mapped, _ = map_rows(_rows(), rules, table="users")
    assert all("name" in row for row in mapped)
    assert all("first_name" not in row for row in mapped)


def test_map_rows_passes_unmapped_fields_through():
    rules = [MappingRule("first_name", "name")]
    mapped, _ = map_rows(_rows(), rules, table="users")
    assert all("age" in row for row in mapped)


def test_map_rows_applies_transform():
    rules = [MappingRule("first_name", "name", transform=str.upper)]
    mapped, _ = map_rows(_rows(), rules, table="users")
    assert mapped[0]["name"] == "ALICE"
    assert mapped[1]["name"] == "BOB"


def test_map_rows_report_records_count():
    rules = [MappingRule("first_name", "name")]
    _, report = map_rows(_rows(), rules, table="users")
    assert len(report.records) == 1
    assert report.records[0].rows_mapped == 2


def test_map_rows_total_mappings():
    rules = [
        MappingRule("first_name", "name"),
        MappingRule("age", "years"),
    ]
    _, report = map_rows(_rows(), rules, table="users")
    assert report.total_mappings() == 4  # 2 rows x 2 fields


def test_map_rows_empty_rows():
    rules = [MappingRule("a", "b")]
    mapped, report = map_rows([], rules, table="t")
    assert mapped == []
    assert report.total_mappings() == 0


# ---------------------------------------------------------------------------
# ColumnMappingReport
# ---------------------------------------------------------------------------

def test_report_mapped_fields():
    rules = [MappingRule("first_name", "name"), MappingRule("age", "years")]
    _, report = map_rows(_rows(), rules, table="users")
    assert set(report.mapped_fields()) == {"name", "years"}


def test_report_summary_contains_table():
    _, report = map_rows(_rows(), [MappingRule("first_name", "name")], table="users")
    assert "users" in report.summary()


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

def test_format_report_contains_table_name():
    _, report = map_rows(_rows(), [MappingRule("first_name", "name")], table="users")
    text = format_column_mapping_report(report)
    assert "users" in text


def test_format_report_shows_source_and_target():
    _, report = map_rows(_rows(), [MappingRule("first_name", "name")], table="users")
    text = format_column_mapping_report(report)
    assert "first_name" in text
    assert "name" in text


def test_format_report_empty_mappings_message():
    report = ColumnMappingReport(table="empty")
    text = format_column_mapping_report(report)
    assert "no mappings" in text


def test_mapping_summary_ok_status():
    _, report = map_rows(_rows(), [MappingRule("first_name", "name")], table="users")
    line = mapping_summary(report)
    assert "[OK]" in line
    assert "users" in line


def test_mapping_summary_empty_status():
    report = ColumnMappingReport(table="empty")
    line = mapping_summary(report)
    assert "[EMPTY]" in line
