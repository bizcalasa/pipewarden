"""Tests for pipewarden.normalizer."""
import pytest
from pipewarden.normalizer import NormalizerRule, Normalizer, NormalizationReport


def _upper_rule(field: str) -> NormalizerRule:
    return NormalizerRule(field_name=field, transform=str.upper, description="uppercase")


def _strip_rule(field: str) -> NormalizerRule:
    return NormalizerRule(field_name=field, transform=str.strip, description="strip")


def _int_rule(field: str) -> NormalizerRule:
    return NormalizerRule(field_name=field, transform=int, description="to int")


# --- NormalizerRule ---

def test_rule_applies_transform():
    rule = _upper_rule("name")
    assert rule.apply("alice") == "ALICE"


def test_rule_returns_original_on_exception():
    rule = _int_rule("age")
    assert rule.apply("not-a-number") == "not-a-number"


def test_rule_field_name_stored():
    rule = _strip_rule("email")
    assert rule.field_name == "email"


# --- Normalizer.normalize_row ---

def test_normalize_row_transforms_matching_field():
    n = Normalizer("users")
    n.add(_upper_rule("name"))
    result = n.normalize_row({"name": "alice", "age": 30})
    assert result["name"] == "ALICE"
    assert result["age"] == 30


def test_normalize_row_ignores_missing_field():
    n = Normalizer("users")
    n.add(_upper_rule("missing"))
    row = {"name": "alice"}
    result = n.normalize_row(row)
    assert result == {"name": "alice"}


def test_normalize_row_does_not_mutate_original():
    n = Normalizer("users")
    n.add(_upper_rule("name"))
    original = {"name": "alice"}
    n.normalize_row(original)
    assert original["name"] == "alice"


# --- Normalizer.normalize_rows ---

def test_normalize_rows_returns_correct_count():
    n = Normalizer("users")
    n.add(_upper_rule("name"))
    rows = [{"name": "alice"}, {"name": "bob"}]
    results, report = n.normalize_rows(rows)
    assert len(results) == 2
    assert report.rows_processed == 2


def test_normalize_rows_counts_transformations():
    n = Normalizer("users")
    n.add(_upper_rule("name"))
    rows = [{"name": "alice"}, {"name": "BOB"}]  # BOB unchanged
    _, report = n.normalize_rows(rows)
    assert report.rules_applied.get("name", 0) == 1


def test_normalize_rows_no_changes_empty_rules_applied():
    n = Normalizer("users")
    rows = [{"name": "ALICE"}]
    _, report = n.normalize_rows(rows)
    assert report.total_transformations == 0


# --- Normalizer.rules_for ---

def test_rules_for_returns_matching_rules():
    n = Normalizer("users")
    r1 = _upper_rule("name")
    r2 = _strip_rule("email")
    n.add(r1)
    n.add(r2)
    assert n.rules_for("name") == [r1]
    assert n.rules_for("email") == [r2]


def test_rules_for_returns_empty_for_unknown_field():
    n = Normalizer("users")
    assert n.rules_for("nonexistent") == []


# --- NormalizationReport ---

def test_report_total_transformations_sums_fields():
    r = NormalizationReport(table="t")
    r.record("a")
    r.record("a")
    r.record("b")
    assert r.total_transformations == 3
