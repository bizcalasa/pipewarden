"""Load TableSchema definitions from YAML configuration files."""

import yaml
from pathlib import Path
from typing import Dict

from pipewarden.schema import FieldSchema, FieldType, TableSchema


def _parse_field(raw: dict) -> FieldSchema:
    """Parse a single field definition from a dict."""
    try:
        field_type = FieldType(raw["type"])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Invalid or missing field type: {raw.get('type')}") from exc

    return FieldSchema(
        name=raw["name"],
        field_type=field_type,
        nullable=raw.get("nullable", False),
        required=raw.get("required", True),
        description=raw.get("description"),
    )


def load_schema_from_dict(data: dict) -> TableSchema:
    """Build a TableSchema from a plain dictionary (e.g. parsed YAML)."""
    if "table" not in data:
        raise ValueError("Schema definition must contain a 'table' key.")

    table_data = data["table"]
    fields = [_parse_field(f) for f in table_data.get("fields", [])]

    return TableSchema(
        name=table_data["name"],
        fields=fields,
        description=table_data.get("description"),
    )


def load_schema_from_file(path: str) -> TableSchema:
    """Load a TableSchema from a YAML file path."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    if file_path.suffix not in (".yaml", ".yml"):
        raise ValueError(f"Schema file must be a YAML file (.yaml/.yml): {path}")

    with open(file_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    return load_schema_from_dict(raw)


def load_schemas_from_dir(directory: str) -> Dict[str, TableSchema]:
    """Load all YAML schema files from a directory. Returns a dict keyed by table name."""
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    schemas: Dict[str, TableSchema] = {}
    for yaml_file in sorted(dir_path.glob("*.y*ml")):
        schema = load_schema_from_file(str(yaml_file))
        schemas[schema.name] = schema

    return schemas
