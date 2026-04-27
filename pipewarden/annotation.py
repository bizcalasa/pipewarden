"""Field and table annotation support for pipewarden schemas."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FieldAnnotation:
    """Holds free-form annotations attached to a single schema field."""

    field_name: str
    notes: str = ""
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, str] = field(default_factory=dict)

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags

    def to_dict(self) -> Dict:
        return {
            "field_name": self.field_name,
            "notes": self.notes,
            "owner": self.owner,
            "tags": list(self.tags),
            "extra": dict(self.extra),
        }

    @staticmethod
    def from_dict(data: Dict) -> "FieldAnnotation":
        return FieldAnnotation(
            field_name=data["field_name"],
            notes=data.get("notes", ""),
            owner=data.get("owner"),
            tags=list(data.get("tags", [])),
            extra=dict(data.get("extra", {})),
        )


@dataclass
class TableAnnotation:
    """Holds annotations for an entire table, including per-field annotations."""

    table_name: str
    description: str = ""
    owner: Optional[str] = None
    fields: Dict[str, FieldAnnotation] = field(default_factory=dict)

    def annotate_field(self, annotation: FieldAnnotation) -> None:
        self.fields[annotation.field_name] = annotation

    def get_field(self, field_name: str) -> Optional[FieldAnnotation]:
        return self.fields.get(field_name)

    def to_dict(self) -> Dict:
        return {
            "table_name": self.table_name,
            "description": self.description,
            "owner": self.owner,
            "fields": {k: v.to_dict() for k, v in self.fields.items()},
        }

    @staticmethod
    def from_dict(data: Dict) -> "TableAnnotation":
        ta = TableAnnotation(
            table_name=data["table_name"],
            description=data.get("description", ""),
            owner=data.get("owner"),
        )
        for fname, fdata in data.get("fields", {}).items():
            ta.fields[fname] = FieldAnnotation.from_dict(fdata)
        return ta
