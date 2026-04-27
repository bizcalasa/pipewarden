"""Tests for pipewarden.tag_engine."""

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.tag_engine import FieldTags, TagEngine, TagRule, TableTagReport


def _make_schema() -> TableSchema:
    return TableSchema(
        name="orders",
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="email", type=FieldType.STRING, nullable=True),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=False),
        ],
    )


def _nullable_rule() -> TagRule:
    return TagRule(
        tag="nullable",
        predicate=lambda name, fs: fs.nullable,
        description="field allows nulls",
    )


def _pii_rule() -> TagRule:
    return TagRule(
        tag="pii",
        predicate=lambda name, fs: "email" in name.lower(),
        description="field may contain PII",
    )


def test_rule_matches_when_predicate_true():
    rule = _nullable_rule()
    schema = _make_schema()
    email_field = schema.fields[1]  # nullable=True
    assert rule.matches(email_field.name, email_field) is True


def test_rule_does_not_match_when_predicate_false():
    rule = _nullable_rule()
    schema = _make_schema()
    id_field = schema.fields[0]  # nullable=False
    assert rule.matches(id_field.name, id_field) is False


def test_rule_handles_exception_gracefully():
    bad_rule = TagRule(tag="boom", predicate=lambda n, fs: 1 / 0)
    schema = _make_schema()
    assert bad_rule.matches("id", schema.fields[0]) is False


def test_field_tags_has_tag():
    ft = FieldTags(field_name="email", tags=["pii", "nullable"])
    assert ft.has_tag("pii") is True
    assert ft.has_tag("sensitive") is False


def test_tag_engine_add_and_list_rules():
    engine = TagEngine()
    engine.add_rule(_nullable_rule())
    engine.add_rule(_pii_rule())
    assert len(engine.rules()) == 2


def test_tag_table_assigns_correct_tags():
    engine = TagEngine()
    engine.add_rule(_nullable_rule())
    engine.add_rule(_pii_rule())

    report = engine.tag_table(_make_schema())

    assert report.table_name == "orders"
    email_tags = report.tags_for("email")
    assert email_tags is not None
    assert "nullable" in email_tags.tags
    assert "pii" in email_tags.tags


def test_non_matching_field_has_no_tags():
    engine = TagEngine()
    engine.add_rule(_pii_rule())

    report = engine.tag_table(_make_schema())
    id_tags = report.tags_for("id")
    assert id_tags is not None
    assert id_tags.tags == []


def test_all_tags_returns_mapping():
    engine = TagEngine()
    engine.add_rule(_nullable_rule())

    report = engine.tag_table(_make_schema())
    mapping = report.all_tags()

    assert isinstance(mapping, dict)
    assert mapping["email"] == ["nullable"]
    assert mapping["id"] == []


def test_tags_for_missing_field_returns_none():
    report = TableTagReport(table_name="orders")
    assert report.tags_for("nonexistent") is None
