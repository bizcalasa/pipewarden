"""Field enrichment module: apply derived/computed fields to rows."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class EnricherRule:
    """A single enrichment rule that computes a new (or overwritten) field."""

    output_field: str
    transform: Callable[[Dict[str, Any]], Any]
    description: str = ""

    def apply(self, row: Dict[str, Any]) -> Any:
        """Return the computed value; return None on any exception."""
        try:
            return self.transform(row)
        except Exception:
            return None


@dataclass
class EnrichmentRecord:
    """Tracks which fields were added/updated for a single row."""

    row_index: int
    enriched_fields: List[str] = field(default_factory=list)


@dataclass
class EnrichmentReport:
    """Aggregated results of an enrichment pass over a table."""

    table: str
    rows_processed: int = 0
    records: List[EnrichmentRecord] = field(default_factory=list)

    def record(self, row_index: int, enriched_fields: List[str]) -> None:
        self.rows_processed += 1
        self.records.append(EnrichmentRecord(row_index, enriched_fields))

    @property
    def total_enrichments(self) -> int:
        return sum(len(r.enriched_fields) for r in self.records)

    def fields_enriched(self) -> List[str]:
        """Unique field names that were enriched across all rows."""
        seen: Dict[str, None] = {}
        for rec in self.records:
            for f in rec.enriched_fields:
                seen[f] = None
        return list(seen)


def enrich_rows(
    table: str,
    rows: List[Dict[str, Any]],
    rules: List[EnricherRule],
    *,
    in_place: bool = False,
) -> tuple[List[Dict[str, Any]], EnrichmentReport]:
    """Apply *rules* to every row, returning enriched rows and a report."""
    report = EnrichmentReport(table=table)
    output: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows):
        enriched = row if in_place else dict(row)
        touched: List[str] = []
        for rule in rules:
            value = rule.apply(enriched)
            enriched[rule.output_field] = value
            touched.append(rule.output_field)
        output.append(enriched)
        report.record(idx, touched)

    return output, report
