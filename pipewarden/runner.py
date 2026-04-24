"""Runner module: validates a dataset (list of row dicts) against a TableSchema."""

from typing import Any, Dict, List

from pipewarden.schema import TableSchema, validate_value
from pipewarden.validator import ValidationError, ValidationResult


def validate_rows(
    rows: List[Dict[str, Any]],
    schema: TableSchema,
    *,
    allow_extra_fields: bool = False,
) -> ValidationResult:
    """Validate a list of row dicts against *schema*.

    Args:
        rows: Sequence of dicts representing data rows.
        schema: The TableSchema to validate against.
        allow_extra_fields: When False (default), columns present in a row but
            absent from the schema are reported as errors.

    Returns:
        A :class:`~pipewarden.validator.ValidationResult` describing every
        validation error found (or none if the data is valid).
    """
    errors: List[ValidationError] = []
    expected_fields = {f.name: f for f in schema.fields}

    for row_idx, row in enumerate(rows):
        # Check for unexpected fields
        if not allow_extra_fields:
            for col in row:
                if col not in expected_fields:
                    errors.append(
                        ValidationError(
                            field=col,
                            row=row_idx,
                            message=f"Unexpected field '{col}' not defined in schema '{schema.name}'",
                        )
                    )

        # Validate each schema field against the row value
        for field_name, field_schema in expected_fields.items():
            value = row.get(field_name)  # None when key is absent
            if field_name not in row:
                # Treat missing key the same as an explicit None
                if not field_schema.nullable:
                    errors.append(
                        ValidationError(
                            field=field_name,
                            row=row_idx,
                            message=f"Field '{field_name}' is missing and not nullable",
                        )
                    )
                continue

            ok, msg = validate_value(value, field_schema)
            if not ok:
                errors.append(
                    ValidationError(
                        field=field_name,
                        row=row_idx,
                        message=msg or f"Validation failed for field '{field_name}'",
                    )
                )

    return ValidationResult(errors=errors)
