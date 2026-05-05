"""Data quality scoring and reporting for ETL pipeline validation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class QualityDimension:
    """A single quality dimension score for a table."""
    name: str
    passed: int
    total: int
    weight: float = 1.0

    @property
    def score(self) -> float:
        if self.total == 0:
            return 100.0
        return round((self.passed / self.total) * 100.0, 2)

    @property
    def weighted_score(self) -> float:
        return self.score * self.weight


@dataclass
class DataQualityReport:
    """Aggregated data quality report for a table."""
    table_name: str
    dimensions: List[QualityDimension] = field(default_factory=list)

    def add_dimension(self, dimension: QualityDimension) -> None:
        self.dimensions.append(dimension)

    @property
    def overall_score(self) -> float:
        if not self.dimensions:
            return 100.0
        total_weight = sum(d.weight for d in self.dimensions)
        if total_weight == 0:
            return 100.0
        weighted_sum = sum(d.weighted_score for d in self.dimensions)
        return round(weighted_sum / total_weight, 2)

    @property
    def grade(self) -> str:
        s = self.overall_score
        if s >= 95:
            return "A"
        if s >= 85:
            return "B"
        if s >= 70:
            return "C"
        if s >= 50:
            return "D"
        return "F"

    def get_dimension(self, name: str) -> Optional[QualityDimension]:
        for d in self.dimensions:
            if d.name == name:
                return d
        return None


def build_quality_report(
    table_name: str,
    completeness_passed: int,
    completeness_total: int,
    validity_passed: int,
    validity_total: int,
    uniqueness_passed: int,
    uniqueness_total: int,
) -> DataQualityReport:
    """Convenience builder for a standard 3-dimension quality report."""
    report = DataQualityReport(table_name=table_name)
    report.add_dimension(QualityDimension("completeness", completeness_passed, completeness_total, weight=1.0))
    report.add_dimension(QualityDimension("validity", validity_passed, validity_total, weight=1.5))
    report.add_dimension(QualityDimension("uniqueness", uniqueness_passed, uniqueness_total, weight=1.0))
    return report
