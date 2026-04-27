"""Tag engine for labelling fields and tables with metadata tags."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewarden.schema import TableSchema


@dataclass
class TagRule:
    """A rule that assigns a tag to a field when its predicate is satisfied."""

    tag: str
    predicate: Callable[[str, object], bool]  # (field_name, field_schema) -> bool
    description: str = ""

    def matches(self, field_name: str, field_schema: object) -> bool:
        try:
            return bool(self.predicate(field_name, field_schema))
        except Exception:
            return False


@dataclass
class FieldTags:
    """Tags assigned to a single field."""

    field_name: str
    tags: List[str] = field(default_factory=list)

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


@dataclass
class TableTagReport:
    """Collection of field tag results for a table."""

    table_name: str
    field_tags: List[FieldTags] = field(default_factory=list)

    def tags_for(self, field_name: str) -> Optional[FieldTags]:
        for ft in self.field_tags:
            if ft.field_name == field_name:
                return ft
        return None

    def all_tags(self) -> Dict[str, List[str]]:
        return {ft.field_name: ft.tags for ft in self.field_tags}


class TagEngine:
    """Applies tag rules to table schemas and produces tag reports."""

    def __init__(self) -> None:
        self._rules: List[TagRule] = []

    def add_rule(self, rule: TagRule) -> None:
        self._rules.append(rule)

    def tag_table(self, schema: TableSchema) -> TableTagReport:
        report = TableTagReport(table_name=schema.name)
        for fs in schema.fields:
            matched = [
                rule.tag
                for rule in self._rules
                if rule.matches(fs.name, fs)
            ]
            report.field_tags.append(FieldTags(field_name=fs.name, tags=matched))
        return report

    def rules(self) -> List[TagRule]:
        return list(self._rules)
