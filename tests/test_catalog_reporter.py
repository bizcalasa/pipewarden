"""Tests for pipewarden.catalog_reporter."""

from __future__ import annotations

from pipewarden.catalog import SchemaCatalog
from pipewarden.catalog_reporter import catalog_summary, format_catalog, format_entry
from pipewarden.schema import FieldSchema, FieldType, TableSchema


def _make_schema(name: str = "orders") -> TableSchema:
    return TableSchema(
        name=name,
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="value", type=FieldType.FLOAT, nullable=True),
        ],
    )


def _make_catalog(*names: str) -> SchemaCatalog:
    catalog = SchemaCatalog()
    for n in names:
        catalog.register(_make_schema(n))
    return catalog


def test_format_entry_contains_name():
    catalog = _make_catalog("orders")
    entry = catalog.get("orders")
    assert "orders" in format_entry(entry)


def test_format_entry_shows_field_count():
    catalog = _make_catalog("orders")
    entry = catalog.get("orders")
    result = format_entry(entry)
    assert "2 field" in result


def test_format_entry_shows_tags():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("orders"), tags=["pii"])
    entry = catalog.get("orders")
    assert "pii" in format_entry(entry)


def test_format_entry_shows_source():
    catalog = SchemaCatalog()
    catalog.register(_make_schema("orders"), source="db://prod")
    entry = catalog.get("orders")
    assert "db://prod" in format_entry(entry)


def test_format_catalog_empty():
    catalog = SchemaCatalog()
    result = format_catalog(catalog)
    assert "empty" in result.lower()


def test_format_catalog_lists_all_tables():
    catalog = _make_catalog("orders", "users")
    result = format_catalog(catalog)
    assert "orders" in result
    assert "users" in result


def test_format_catalog_shows_count():
    catalog = _make_catalog("a", "b", "c")
    result = format_catalog(catalog)
    assert "3" in result


def test_catalog_summary_singular():
    catalog = _make_catalog("orders")
    result = catalog_summary(catalog)
    assert "1 table" in result


def test_catalog_summary_plural():
    catalog = _make_catalog("a", "b")
    result = catalog_summary(catalog)
    assert "2 tables" in result


def test_catalog_summary_empty():
    catalog = SchemaCatalog()
    result = catalog_summary(catalog)
    assert "0" in result
