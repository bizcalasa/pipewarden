"""Tests for pipewarden.enricher and pipewarden.enricher_reporter."""
import pytest

from pipewarden.enricher import (
    EnricherRule,
    EnrichmentReport,
    enrich_rows,
)
from pipewarden.enricher_reporter import (
    enrichment_summary,
    format_enrichment_report,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _full_name_rule() -> EnricherRule:
    return EnricherRule(
        output_field="full_name",
        transform=lambda r: f"{r['first']} {r['last']}",
        description="Concatenate first and last name",
    )


def _upper_rule() -> EnricherRule:
    return EnricherRule(
        output_field="email_upper",
        transform=lambda r: r["email"].upper(),
    )


_ROWS = [
    {"first": "Alice", "last": "Smith", "email": "alice@example.com"},
    {"first": "Bob", "last": "Jones", "email": "bob@example.com"},
]


# ---------------------------------------------------------------------------
# EnricherRule tests
# ---------------------------------------------------------------------------

def test_rule_applies_transform():
    rule = _full_name_rule()
    result = rule.apply({"first": "Alice", "last": "Smith"})
    assert result == "Alice Smith"


def test_rule_returns_none_on_exception():
    rule = EnricherRule(output_field="x", transform=lambda r: r["missing_key"])
    assert rule.apply({}) is None


# ---------------------------------------------------------------------------
# enrich_rows tests
# ---------------------------------------------------------------------------

def test_enrich_rows_adds_field():
    rows, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    assert rows[0]["full_name"] == "Alice Smith"
    assert rows[1]["full_name"] == "Bob Jones"


def test_enrich_rows_does_not_mutate_originals_by_default():
    original = [{"first": "Alice", "last": "Smith", "email": "a@b.com"}]
    enrich_rows("t", original, [_full_name_rule()])
    assert "full_name" not in original[0]


def test_enrich_rows_in_place_mutates_rows():
    rows = [{"first": "Alice", "last": "Smith", "email": "a@b.com"}]
    enrich_rows("t", rows, [_full_name_rule()], in_place=True)
    assert "full_name" in rows[0]


def test_report_rows_processed():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    assert report.rows_processed == 2


def test_report_total_enrichments():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule(), _upper_rule()])
    assert report.total_enrichments == 4  # 2 rows × 2 rules


def test_report_fields_enriched():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule(), _upper_rule()])
    assert set(report.fields_enriched()) == {"full_name", "email_upper"}


def test_report_table_name():
    _, report = enrich_rows("orders", _ROWS, [_full_name_rule()])
    assert report.table == "orders"


# ---------------------------------------------------------------------------
# Reporter tests
# ---------------------------------------------------------------------------

def test_format_report_contains_table_name():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    text = format_enrichment_report(report)
    assert "users" in text


def test_format_report_shows_rows_processed():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    text = format_enrichment_report(report)
    assert "2" in text


def test_format_report_shows_enriched_fields():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    text = format_enrichment_report(report)
    assert "full_name" in text


def test_enrichment_summary_one_liner():
    _, report = enrich_rows("users", _ROWS, [_full_name_rule()])
    summary = enrichment_summary(report)
    assert "users" in summary
    assert "enrichment" in summary
