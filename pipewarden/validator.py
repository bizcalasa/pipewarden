"""Core validation result and error types for pipewarden."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ValidationError:
    """Represents a single field-level validation failure."""
    row: int
    field: str
    message: str

    def __str__(self) -> str:
        return f"Row {self.row} | {self.field}: {self.message}"


class ValidationResult:
    """Accumulates errors from validating a table."""

    def __init__(self) -> None:
        self._errors: List[ValidationError] = []
        self.row_count: int = 0

    def add_error(self, error: ValidationError) -> None:
        self._errors.append(error)

    @property
    def is_valid(self) -> bool:
        return len(self._errors) == 0

    @property
    def error_count(self) -> int:
        return len(self._errors)

    @property
    def errors(self) -> List[ValidationError]:
        return list(self._errors)

    def summary(self) -> str:
        if self.is_valid:
            return f"OK — {self.row_count} rows validated"
        return (
            f"FAILED — {self.error_count} error(s) in {self.row_count} rows"
        )

    @property
    def error_rate(self) -> Optional[float]:
        """Return fraction of rows with errors, or None if row_count is 0."""
        if self.row_count == 0:
            return None
        return self.error_count / self.row_count
