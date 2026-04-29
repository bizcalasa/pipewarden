"""Row sampling utilities for pipewarden."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List


@dataclass
class SampleConfig:
    """Configuration for a sampling strategy."""

    strategy: str = "random"   # "random" | "first" | "last"
    size: int = 100
    seed: int | None = None

    def __post_init__(self) -> None:
        valid = {"random", "first", "last"}
        if self.strategy not in valid:
            raise ValueError(f"Unknown strategy {self.strategy!r}. Choose from {valid}.")
        if self.size < 1:
            raise ValueError("size must be >= 1")


@dataclass
class SampleResult:
    """Holds sampled rows together with metadata."""

    rows: List[dict] = field(default_factory=list)
    total_seen: int = 0
    strategy: str = "random"

    @property
    def sample_size(self) -> int:
        return len(self.rows)

    def coverage(self) -> float:
        """Fraction of total rows included in the sample (0-1)."""
        if self.total_seen == 0:
            return 0.0
        return self.sample_size / self.total_seen


def sample_rows(rows: Iterable[dict], config: SampleConfig) -> SampleResult:
    """Sample *rows* according to *config* and return a :class:`SampleResult`."""
    if config.strategy == "first":
        return _sample_first(rows, config)
    if config.strategy == "last":
        return _sample_last(rows, config)
    return _sample_random(rows, config)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sample_first(rows: Iterable[dict], config: SampleConfig) -> SampleResult:
    collected: List[dict] = []
    total = 0
    for row in rows:
        total += 1
        if len(collected) < config.size:
            collected.append(row)
    return SampleResult(rows=collected, total_seen=total, strategy="first")


def _sample_last(rows: Iterable[dict], config: SampleConfig) -> SampleResult:
    """Reservoir of the *last* N rows."""
    buf: List[dict] = []
    total = 0
    for row in rows:
        total += 1
        buf.append(row)
        if len(buf) > config.size:
            buf.pop(0)
    return SampleResult(rows=buf, total_seen=total, strategy="last")


def _sample_random(rows: Iterable[dict], config: SampleConfig) -> SampleResult:
    """Reservoir sampling (Algorithm R) for uniform random sample."""
    rng = random.Random(config.seed)
    reservoir: List[dict] = []
    total = 0
    for row in rows:
        total += 1
        if len(reservoir) < config.size:
            reservoir.append(row)
        else:
            j = rng.randint(0, total - 1)
            if j < config.size:
                reservoir[j] = row
    return SampleResult(rows=reservoir, total_seen=total, strategy="random")
