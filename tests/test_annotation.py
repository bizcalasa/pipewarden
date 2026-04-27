"""Tests for pipewarden.annotation module."""

import pytest
from pipewarden.annotation import FieldAnnotation, TableAnnotation


def _field_ann(**kwargs) -> FieldAnnotation:
    defaults = {"field_name": "email", "notes": "user email", "owner": "data-team"}
    defaults.update(kwargs)
    return FieldAnnotation(**defaults)


# --- FieldAnnotation ---

def test_field_annotation_has_tag_true():
    ann = _field_ann(tags=["pii", "required"])
    assert ann.has_tag("pii") is True


def test_field_annotation_has_tag_false():
    ann = _field_ann(tags=["pii"])
    assert ann.has_tag("sensitive") is False


def test_field_annotation_to_dict_round_trip():
    ann = _field_ann(tags=["pii"], extra={"source": "crm"})
    restored = FieldAnnotation.from_dict(ann.to_dict())
    assert restored.field_name == ann.field_name
    assert restored.notes == ann.notes
    assert restored.owner == ann.owner
    assert restored.tags == ann.tags
    assert restored.extra == ann.extra


def test_field_annotation_defaults():
    ann = FieldAnnotation(field_name="id")
    assert ann.notes == ""
    assert ann.owner is None
    assert ann.tags == []
    assert ann.extra == {}


def test_field_annotation_from_dict_minimal():
    ann = FieldAnnotation.from_dict({"field_name": "score"})
    assert ann.field_name == "score"
    assert ann.tags == []


# --- TableAnnotation ---

def test_table_annotation_annotate_and_get_field():
    ta = TableAnnotation(table_name="users")
    fa = _field_ann(field_name="email")
    ta.annotate_field(fa)
    result = ta.get_field("email")
    assert result is fa


def test_table_annotation_get_missing_field_returns_none():
    ta = TableAnnotation(table_name="users")
    assert ta.get_field("nonexistent") is None


def test_table_annotation_to_dict_includes_fields():
    ta = TableAnnotation(table_name="orders", description="order records", owner="ops")
    ta.annotate_field(_field_ann(field_name="order_id", tags=["key"]))
    d = ta.to_dict()
    assert d["table_name"] == "orders"
    assert d["description"] == "order records"
    assert d["owner"] == "ops"
    assert "order_id" in d["fields"]


def test_table_annotation_round_trip():
    ta = TableAnnotation(table_name="events", description="event log")
    ta.annotate_field(_field_ann(field_name="ts", notes="timestamp", tags=["audit"]))
    restored = TableAnnotation.from_dict(ta.to_dict())
    assert restored.table_name == ta.table_name
    assert restored.description == ta.description
    field = restored.get_field("ts")
    assert field is not None
    assert field.has_tag("audit")


def test_table_annotation_multiple_fields():
    ta = TableAnnotation(table_name="users")
    ta.annotate_field(FieldAnnotation(field_name="id"))
    ta.annotate_field(FieldAnnotation(field_name="name", tags=["pii"]))
    ta.annotate_field(FieldAnnotation(field_name="email", tags=["pii"]))
    assert len(ta.fields) == 3
    assert ta.get_field("name").has_tag("pii")
