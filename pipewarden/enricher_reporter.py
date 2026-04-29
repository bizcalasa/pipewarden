"""Human-readable formatting for EnrichmentReport."""
from __future__ import annotations

from pipewarden.enricher import EnrichmentReport


def _pluralize(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def format_enrichment_report(report: EnrichmentReport) -> str:
    """Return a multi-line summary of an enrichment run."""
    lines = [
        f"Enrichment report — table: {report.table}",
        f"  Rows processed : {report.rows_processed}",
        f"  Total enrichments : {report.total_enrichments}",
    ]
    enriched = report.fields_enriched()
    if enriched:
        lines.append("  Fields enriched : " + ", ".join(sorted(enriched)))
    else:
        lines.append("  Fields enriched : (none)")
    return "\n".join(lines)


def enrichment_summary(report: EnrichmentReport) -> str:
    """Return a one-line summary suitable for pipeline-level dashboards."""
    n = report.total_enrichments
    r = report.rows_processed
    return (
        f"[{report.table}] enriched {_pluralize(r, 'row')} "
        f"with {_pluralize(n, 'enrichment')}"
    )
