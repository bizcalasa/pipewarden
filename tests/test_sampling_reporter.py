"""Tests for pipewarden.sampling_reporter."""
from pipewarden.sampling import SampleResult
from pipewarden.sampling_reporter import (
    format_sample_result,
    format_sample_preview,
    sampling_summary,
)


def _result(sample: int, total: int, strategy: str = "random") -> SampleResult:
    rows = [{"id": i} for i in range(sample)]
    return SampleResult(rows=rows, total_seen=total, strategy=strategy)


def test_format_result_contains_strategy():
    r = _result(10, 100, "random")
    out = format_sample_result(r)
    assert "RANDOM" in out


def test_format_result_contains_counts():
    r = _result(25, 200, "first")
    out = format_sample_result(r)
    assert "25" in out
    assert "200" in out


def test_format_result_coverage_percentage():
    r = _result(50, 100)
    out = format_sample_result(r)
    assert "50.0%" in out


def test_format_preview_no_rows_message():
    r = SampleResult(rows=[], total_seen=0)
    out = format_sample_preview(r)
    assert "no rows sampled" in out


def test_format_preview_shows_rows():
    r = _result(3, 10)
    out = format_sample_preview(r, max_rows=3)
    assert "[0]" in out
    assert "[1]" in out
    assert "[2]" in out


def test_format_preview_truncates_with_ellipsis():
    r = _result(10, 50)
    out = format_sample_preview(r, max_rows=3)
    assert "more rows" in out


def test_sampling_summary_no_rows():
    r = SampleResult(rows=[], total_seen=0)
    out = sampling_summary(r)
    assert "no rows available" in out


def test_sampling_summary_ok():
    r = _result(20, 100)
    out = sampling_summary(r)
    assert out.startswith("SAMPLING OK")
