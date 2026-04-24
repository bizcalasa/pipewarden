"""Validates a dataset (list of row dicts) against a TableSchema."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .schema import TableSchema, validate_value


@dataclass
class ValidationError:
    row_index: int
    column: str
    value: Any
    message: str

    def __str__(self) -> str:
        return f"Row {self.row_index} | {self.column!r}: {self.message} (got {self.value!r})"


@dataclass
class ValidationResult:
    table: str
    total_rows: int
    errors: List[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        return len(self.errors)

    def summary(self) -> str:
        if self.is_valid:
            return f"[OK] '{self.table}': {self.total_rows} rows validated, no errors."
        return (
            f"[FAIL] '{self.table}': {self.total_rows} rows validated, "
            f"{self.error_count} error(s) found."
        )


def validate_dataset(
    schema: TableSchema,
    rows: List[Dict[str, Any]],
    strict: bool = False,
) -> ValidationResult:
    """Validate a list of row dicts against *schema*.

    Args:
        schema:  The TableSchema to validate against.
        rows:    Iterable of dicts representing data rows.
        strict:  When True, unknown columns (not in schema) are also reported
                 as errors.

    Returns:
        A ValidationResult with any errors found.
    """
    result = ValidationResult(table=schema.name, total_rows=len(rows))
    schema_field_names = {f.name for f in schema.fields}

    for idx, row in enumerate(rows):
        # Check every field defined in the schema
        for schema_field in schema.fields:
            value = row.get(schema_field.name)  # None if key missing
            ok, message = validate_value(schema_field, value)
            if not ok:
                result.errors.append(
                    ValidationError(
                        row_index=idx,
                        column=schema_field.name,
                        value=value,
                        message=message or "validation failed",
                    )
                )

        # Optionally flag columns present in the row but absent from schema
        if strict:
            for col in row:
                if col not in schema_field_names:
                    result.errors.append(
                        ValidationError(
                            row_index=idx,
                            column=col,
                            value=row[col],
                            message="unknown column not present in schema",
                        )
                    )

    return result
