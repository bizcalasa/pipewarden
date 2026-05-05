"""Tests for pipewarden.data_quality."""
import pytest
from pipewarden.data_quality import (
    QualityDimension,
    DataQualityReport,
    build_quality_report,
)


def test_dimension_score_perfect():
    d = QualityDimension("completeness", passed=10, total=10)
    assert d.score == 100.0


def test_dimension_score_partial():
    d = QualityDimension("validity", passed=7, total=10)
    assert d.score == 70.0


def test_dimension_score_zero_total():
    d = QualityDimension("uniqueness", passed=0, total=0)
    assert d.score == 100.0


def test_dimension_weighted_score():
    d = QualityDimension("validity", passed=8, total=10, weight=2.0)
    assert d.weighted_score == pytest.approx(160.0)


def test_report_overall_score_no_dimensions():
    r = DataQualityReport(table_name="orders")
    assert r.overall_score == 100.0


def test_report_overall_score_uniform_weights():
    r = DataQualityReport(table_name="orders")
    r.add_dimension(QualityDimension("completeness", 10, 10, weight=1.0))
    r.add_dimension(QualityDimension("validity", 5, 10, weight=1.0))
    assert r.overall_score == pytest.approx(75.0)


def test_report_overall_score_weighted():
    r = DataQualityReport(table_name="orders")
    r.add_dimension(QualityDimension("completeness", 10, 10, weight=1.0))  # 100%
    r.add_dimension(QualityDimension("validity", 0, 10, weight=3.0))      # 0%
    # (100*1 + 0*3) / 4 = 25
    assert r.overall_score == pytest.approx(25.0)


def test_report_grade_a():
    r = DataQualityReport(table_name="t")
    r.add_dimension(QualityDimension("c", 100, 100))
    assert r.grade == "A"


def test_report_grade_f():
    r = DataQualityReport(table_name="t")
    r.add_dimension(QualityDimension("c", 1, 10))
    assert r.grade == "F"


def test_get_dimension_found():
    r = DataQualityReport(table_name="t")
    dim = QualityDimension("validity", 9, 10)
    r.add_dimension(dim)
    assert r.get_dimension("validity") is dim


def test_get_dimension_missing():
    r = DataQualityReport(table_name="t")
    assert r.get_dimension("completeness") is None


def test_build_quality_report_creates_three_dimensions():
    r = build_quality_report("users", 10, 10, 9, 10, 10, 10)
    assert len(r.dimensions) == 3
    names = {d.name for d in r.dimensions}
    assert names == {"completeness", "validity", "uniqueness"}


def test_build_quality_report_table_name():
    r = build_quality_report("users", 10, 10, 10, 10, 10, 10)
    assert r.table_name == "users"
