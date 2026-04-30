"""Tests for pipewarden.scorer_reporter."""

import pytest

from pipewarden.scorer import TableScore, ScoringReport
from pipewarden.scorer_reporter import (
    format_table_score,
    format_scoring_report,
    scoring_summary,
)


def _ts(name="orders", total=10, failed=0) -> TableScore:
    return TableScore(
        table_name=name,
        total_rows=total,
        total_checks=total,
        failed_checks=failed,
    )


def _report(*scores: TableScore) -> ScoringReport:
    r = ScoringReport()
    for s in scores:
        r.add(s)
    return r


# ---------------------------------------------------------------------------
# format_table_score
# ---------------------------------------------------------------------------

def test_format_score_contains_table_name():
    out = format_table_score(_ts("invoices"))
    assert "invoices" in out


def test_format_score_shows_grade():
    out = format_table_score(_ts(total=10, failed=0))
    assert "grade=A" in out


def test_format_score_ok_status_when_high():
    out = format_table_score(_ts(total=10, failed=0))
    assert "[OK]" in out


def test_format_score_warn_status():
    out = format_table_score(_ts(total=10, failed=4))
    assert "[WARN]" in out


def test_format_score_fail_status():
    out = format_table_score(_ts(total=10, failed=8))
    assert "[FAIL]" in out


# ---------------------------------------------------------------------------
# format_scoring_report
# ---------------------------------------------------------------------------

def test_format_report_empty():
    out = format_scoring_report(ScoringReport())
    assert "No tables" in out


def test_format_report_contains_header():
    out = format_scoring_report(_report(_ts()))
    assert "Quality Scoring Report" in out


def test_format_report_contains_overall_score():
    out = format_scoring_report(_report(_ts(total=10, failed=0)))
    assert "Overall score" in out
    assert "100.0" in out


def test_format_report_lists_all_tables():
    out = format_scoring_report(_report(_ts("a"), _ts("b")))
    assert "a" in out
    assert "b" in out


# ---------------------------------------------------------------------------
# scoring_summary
# ---------------------------------------------------------------------------

def test_summary_contains_table_count():
    out = scoring_summary(_report(_ts("x"), _ts("y")))
    assert "2" in out


def test_summary_shows_overall():
    out = scoring_summary(_report(_ts(total=10, failed=0)))
    assert "overall=100.0" in out


def test_summary_shows_grades():
    out = scoring_summary(_report(_ts(total=10, failed=0)))
    assert "A:1" in out
