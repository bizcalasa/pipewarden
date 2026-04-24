"""Schema definition and validation models for pipewarden."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class FieldType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    NULL = "null"


@dataclass
class FieldSchema:
    name: str
    field_type: FieldType
    nullable: bool = False
    required: bool = True
    description: Optional[str] = None

    def validate_value(self, value: Any) -> List[str]:
        """Validate a single value against this field schema. Returns list of errors."""
        errors = []

        if value is None:
            if not self.nullable and self.required:
                errors.append(f"Field '{self.name}' is required and cannot be null.")
            return errors

        type_map = {
            FieldType.STRING: str,
            FieldType.INTEGER: int,
            FieldType.FLOAT: (int, float),
            FieldType.BOOLEAN: bool,
        }

        expected = type_map.get(self.field_type)
        if expected and not isinstance(value, expected):
            errors.append(
                f"Field '{self.name}' expected {self.field_type.value}, "
                f"got {type(value).__name__}."
            )

        return errors


@dataclass
class TableSchema:
    name: str
    fields: List[FieldSchema] = field(default_factory=list)
    description: Optional[str] = None

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> Optional[FieldSchema]:
        return next((f for f in self.fields if f.name == name), None)

    def validate_row(self, row: Dict[str, Any]) -> List[str]:
        """Validate a single data row against the table schema."""
        errors = []

        for field_schema in self.fields:
            if field_schema.required and field_schema.name not in row:
                errors.append(f"Missing required field: '{field_schema.name}'.")
                continue
            value = row.get(field_schema.name)
            errors.extend(field_schema.validate_value(value))

        unexpected = set(row.keys()) - set(self.field_names())
        for col in unexpected:
            errors.append(f"Unexpected field '{col}' not defined in schema.")

        return errors
