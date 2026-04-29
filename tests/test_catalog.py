"""Tests for pipewarden.catalog."""

from __future__ import annotations

import pytest

from pipewarden.catalog import CatalogEntry, SchemaCatalog
from pipewarden.schema import FieldSchema, FieldType, TableSchema


def _make_schema(name: str = "orders") -> TableSchema:
    return TableSchema(
        name=name,
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=True),
        ],
    )


def test_register_adds_entry():
    catalog = SchemaCatalog()
    schema = _make_schema("orders")
    catalog.register(schema)
    assert "orders" in catalog


def test_get_returns_entry():
    catalog = SchemaCatalog()
    schema = _make_schema("orders")
    catalog.register(schema)
    entry = catalog.get("orders")
    assert entry is not None
    assert entry.name == "orders"


def test_get_returns_none_for_missing():
    catalog = SchemaCatalog()
    assert catalog.get("nonexistent") is None


def test_remove_entry():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("orders"))
    removed = catalog.remove("orders")
    assert removed is True
    assert "orders" not in catalog


def test_remove_missing_returns_false():
    catalog = SchemaCatalog()
    assert catalog.remove("ghost") is False


def test_len_reflects_registered_count():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("a"))
    catalog.register(_make_schema("b"))
    assert len(catalog) == 2


def test_table_names_lists_all():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("a"))
    catalog.register(_make_schema("b"))
    assert set(catalog.table_names()) == {"a", "b"}


def test_filter_by_tag():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("pii_table"), tags=["pii", "sensitive"])
    catalog.register(_make_schema("plain_table"), tags=["public"])
    results = catalog.filter_by_tag("pii")
    assert len(results) == 1
    assert results[0].name == "pii_table"


def test_entry_has_tag_true():
    entry = CatalogEntry(name="t", schema=_make_schema("t"), tags=["pii"])
    assert entry.has_tag("pii") is True


def test_entry_has_tag_false():
    entry = CatalogEntry(name="t", schema=_make_schema("t"), tags=[])
    assert entry.has_tag("pii") is False


def test_source_stored_on_entry():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("orders"), source="db://prod")
    entry = catalog.get("orders")
    assert entry.source == "db://prod"


def test_register_overwrites_existing():
    catalog = SchemaCatalog()
    s1 = _make_schema("orders")
    s2 = TableSchema(name="orders", fields=[FieldSchema("x", FieldType.STRING, False)])
    catalog.register(s1)
    catalog.register(s2)
    assert len(catalog) == 1
    assert len(catalog.get("orders").schema.fields) == 1
