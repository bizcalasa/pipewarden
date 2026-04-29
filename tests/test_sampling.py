"""Tests for pipewarden.sampling."""
import pytest

from pipewarden.sampling import (
    SampleConfig,
    SampleResult,
    sample_rows,
)


def _rows(n: int):
    return [{"id": i, "value": i * 10} for i in range(n)]


# ---------------------------------------------------------------------------
# SampleConfig validation
# ---------------------------------------------------------------------------

def test_config_default_strategy():
    cfg = SampleConfig()
    assert cfg.strategy == "random"


def test_config_invalid_strategy_raises():
    with pytest.raises(ValueError, match="Unknown strategy"):
        SampleConfig(strategy="reservoir")


def test_config_size_zero_raises():
    with pytest.raises(ValueError, match="size must be"):
        SampleConfig(size=0)


# ---------------------------------------------------------------------------
# first strategy
# ---------------------------------------------------------------------------

def test_first_returns_first_n_rows():
    rows = _rows(20)
    result = sample_rows(rows, SampleConfig(strategy="first", size=5))
    assert result.rows == rows[:5]
    assert result.total_seen == 20


def test_first_fewer_rows_than_size():
    rows = _rows(3)
    result = sample_rows(rows, SampleConfig(strategy="first", size=10))
    assert len(result.rows) == 3
    assert result.total_seen == 3


# ---------------------------------------------------------------------------
# last strategy
# ---------------------------------------------------------------------------

def test_last_returns_last_n_rows():
    rows = _rows(20)
    result = sample_rows(rows, SampleConfig(strategy="last", size=5))
    assert result.rows == rows[-5:]
    assert result.total_seen == 20


def test_last_fewer_rows_than_size():
    rows = _rows(3)
    result = sample_rows(rows, SampleConfig(strategy="last", size=10))
    assert len(result.rows) == 3


# ---------------------------------------------------------------------------
# random strategy
# ---------------------------------------------------------------------------

def test_random_sample_size_correct():
    rows = _rows(200)
    result = sample_rows(rows, SampleConfig(strategy="random", size=50, seed=42))
    assert result.sample_size == 50
    assert result.total_seen == 200


def test_random_deterministic_with_seed():
    rows = _rows(100)
    cfg = SampleConfig(strategy="random", size=20, seed=7)
    r1 = sample_rows(rows, cfg)
    r2 = sample_rows(rows, cfg)
    assert r1.rows == r2.rows


def test_random_different_seeds_differ():
    rows = _rows(100)
    r1 = sample_rows(rows, SampleConfig(strategy="random", size=30, seed=1))
    r2 = sample_rows(rows, SampleConfig(strategy="random", size=30, seed=99))
    # With 100 rows and size=30, different seeds almost certainly differ
    assert r1.rows != r2.rows


# ---------------------------------------------------------------------------
# SampleResult helpers
# ---------------------------------------------------------------------------

def test_coverage_zero_when_no_rows():
    result = SampleResult(rows=[], total_seen=0)
    assert result.coverage() == 0.0


def test_coverage_calculation():
    result = SampleResult(rows=[{"id": i} for i in range(10)], total_seen=100)
    assert result.coverage() == pytest.approx(0.1)


def test_empty_iterable_produces_empty_result():
    result = sample_rows([], SampleConfig(strategy="first", size=10))
    assert result.rows == []
    assert result.total_seen == 0
