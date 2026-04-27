"""Freshness checker: validates that data timestamps fall within expected windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional


@dataclass
class FreshnessRule:
    table: str
    timestamp_field: str
    max_age: timedelta
    description: str = ""

    def check(self, rows: List[dict]) -> Optional["FreshnessViolation"]:
        """Return a violation if no row has a recent-enough timestamp, or None."""
        if not rows:
            return FreshnessViolation(
                table=self.table,
                rule=self,
                message=f"No rows found; cannot determine freshness.",
                latest_ts=None,
            )
        latest: Optional[datetime] = None
        for row in rows:
            raw = row.get(self.timestamp_field)
            if raw is None:
                continue
            try:
                ts = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
                if latest is None or ts > latest:
                    latest = ts
            except (ValueError, TypeError):
                continue

        if latest is None:
            return FreshnessViolation(
                table=self.table,
                rule=self,
                message=f"Field '{self.timestamp_field}' has no parseable timestamps.",
                latest_ts=None,
            )

        age = datetime.utcnow() - latest
        if age > self.max_age:
            return FreshnessViolation(
                table=self.table,
                rule=self,
                message=(
                    f"Latest timestamp is {age} old; max allowed is {self.max_age}."
                ),
                latest_ts=latest,
            )
        return None


@dataclass
class FreshnessViolation:
    table: str
    rule: FreshnessRule
    message: str
    latest_ts: Optional[datetime]

    def __str__(self) -> str:
        ts_str = self.latest_ts.isoformat() if self.latest_ts else "N/A"
        return f"[STALE] {self.table}: {self.message} (latest={ts_str})"


@dataclass
class FreshnessReport:
    violations: List[FreshnessViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def add(self, violation: FreshnessViolation) -> None:
        self.violations.append(violation)


def check_freshness(rules: List[FreshnessRule], table_rows: dict[str, List[dict]]) -> FreshnessReport:
    """Run all freshness rules against the provided table data."""
    report = FreshnessReport()
    for rule in rules:
        rows = table_rows.get(rule.table, [])
        violation = rule.check(rows)
        if violation is not None:
            report.add(violation)
    return report
