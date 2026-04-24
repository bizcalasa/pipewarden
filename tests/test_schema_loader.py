"""Unit tests for pipewarden.schema_loader module."""

import pytest
from pipewarden.schema import FieldType
from pipewarden.schema_loader import load_schema_from_dict, load_schema_from_file

import tempfile
import os


SAMPLE_SCHEMA_DICT = {
    "table": {
        "name": "orders",
        "description": "Order records",
        "fields": [
            {"name": "order_id", "type": "integer"},
            {"name": "customer", "type": "string", "nullable": False},
            {"name": "amount", "type": "float", "nullable": True, "required": False},
        ],
    }
}


def test_load_from_dict_basic():
    schema = load_schema_from_dict(SAMPLE_SCHEMA_DICT)
    assert schema.name == "orders"
    assert len(schema.fields) == 3


def test_load_from_dict_field_types():
    schema = load_schema_from_dict(SAMPLE_SCHEMA_DICT)
    assert schema.get_field("order_id").field_type == FieldType.INTEGER
    assert schema.get_field("amount").field_type == FieldType.FLOAT


def test_load_from_dict_nullable_flag():
    schema = load_schema_from_dict(SAMPLE_SCHEMA_DICT)
    assert schema.get_field("amount").nullable is True
    assert schema.get_field("customer").nullable is False


def test_load_from_dict_missing_table_key():
    with pytest.raises(ValueError, match="'table'"):
        load_schema_from_dict({"not_table": {}})


def test_load_from_dict_invalid_field_type():
    bad = {"table": {"name": "t", "fields": [{"name": "x", "type": "uuid"}]}}
    with pytest.raises(ValueError, match="Invalid or missing field type"):
        load_schema_from_dict(bad)


def test_load_from_file(tmp_path):
    yaml_content = """
table:
  name: products
  fields:
    - name: product_id
      type: integer
    - name: title
      type: string
"""
    schema_file = tmp_path / "products.yaml"
    schema_file.write_text(yaml_content)

    schema = load_schema_from_file(str(schema_file))
    assert schema.name == "products"
    assert schema.field_names() == ["product_id", "title"]


def test_load_from_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_schema_from_file("/nonexistent/path/schema.yaml")


def test_load_from_file_wrong_extension(tmp_path):
    bad_file = tmp_path / "schema.json"
    bad_file.write_text("{}")
    with pytest.raises(ValueError, match="YAML"):
        load_schema_from_file(str(bad_file))
