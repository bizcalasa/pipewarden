"""Tests for pipewarden.checksum."""

from __future__ import annotations

import pytest

from pipewarden.schema import FieldSchema, FieldType, TableSchema
from pipewarden.checksum import (
    ChecksumRecord,
    ChecksumReport,
    checksum_rows,
    checksum_schema,
)


def _make_schema(name: str = "orders") -> TableSchema:
    return TableSchema(
        name=name,
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=True),
        ],
    )


def test_checksum_schema_returns_string():
    schema = _make_schema()
    result = checksum_schema(schema)
    assert isinstance(result, str)
    assert len(result) == 64  # SHA-256 hex


def test_checksum_schema_stable():
    schema = _make_schema()
    assert checksum_schema(schema) == checksum_schema(schema)


def test_checksum_schema_changes_on_field_addition():
    s1 = _make_schema()
    s2 = TableSchema(
        name="orders",
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=True),
            FieldSchema(name="note", type=FieldType.STRING, nullable=True),
        ],
    )
    assert checksum_schema(s1) != checksum_schema(s2)


def test_checksum_schema_changes_on_table_rename():
    s1 = _make_schema("orders")
    s2 = _make_schema("invoices")
    assert checksum_schema(s1) != checksum_schema(s2)


def test_checksum_rows_returns_string():
    rows = [{"id": 1, "amount": 9.99}]
    result = checksum_rows(rows)
    assert isinstance(result, str)
    assert len(result) == 64


def test_checksum_rows_stable():
    rows = [{"id": 1}, {"id": 2}]
    assert checksum_rows(rows) == checksum_rows(rows)


def test_checksum_rows_changes_on_different_data():
    rows1 = [{"id": 1}]
    rows2 = [{"id": 2}]
    assert checksum_rows(rows1) != checksum_rows(rows2)


def test_checksum_rows_empty_list():
    result = checksum_rows([])
    assert isinstance(result, str)


def test_record_to_dict_round_trip():
    rec = ChecksumRecord(table="t", schema_checksum="abc", data_checksum="def")
    d = rec.to_dict()
    restored = ChecksumRecord.from_dict(d)
    assert restored.table == rec.table
    assert restored.schema_checksum == rec.schema_checksum
    assert restored.data_checksum == rec.data_checksum


def test_record_from_dict_missing_data_checksum():
    d = {"table": "t", "schema_checksum": "xyz"}
    rec = ChecksumRecord.from_dict(d)
    assert rec.data_checksum is None


def test_report_add_and_get():
    report = ChecksumReport()
    rec = ChecksumRecord(table="orders", schema_checksum="aaa")
    report.add(rec)
    assert report.get("orders") is rec


def test_report_get_missing_returns_none():
    report = ChecksumReport()
    assert report.get("missing") is None


def test_report_table_names():
    report = ChecksumReport()
    report.add(ChecksumRecord(table="a", schema_checksum="1"))
    report.add(ChecksumRecord(table="b", schema_checksum="2"))
    assert set(report.table_names()) == {"a", "b"}
