"""Record-level validation combining schema checks, contracts, and expectations."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipewarden.validator import ValidationResult
from pipewarden.contract import ContractReport, ContractRule
from pipewarden.expectation import ExpectationReport, ExpectationRule
from pipewarden.runner import validate_rows
from pipewarden.schema import TableSchema


@dataclass
class RecordValidationReport:
    table: str
    schema_result: Optional[ValidationResult] = None
    contract_report: Optional[ContractReport] = None
    expectation_report: Optional[ExpectationReport] = None

    @property
    def is_valid(self) -> bool:
        schema_ok = self.schema_result is None or self.schema_result.is_valid
        contract_ok = (
            self.contract_report is None or not self.contract_report.violations
        )
        expectation_ok = (
            self.expectation_report is None or not self.expectation_report.violations
        )
        return schema_ok and contract_ok and expectation_ok

    @property
    def total_issues(self) -> int:
        count = 0
        if self.schema_result:
            count += len(self.schema_result.errors)
        if self.contract_report:
            count += len(self.contract_report.violations)
        if self.expectation_report:
            count += len(self.expectation_report.violations)
        return count


def validate_records(
    table: str,
    rows: List[Dict[str, Any]],
    schema: Optional[TableSchema] = None,
    contract_rules: Optional[List[ContractRule]] = None,
    expectation_rules: Optional[List[ExpectationRule]] = None,
) -> RecordValidationReport:
    """Run all enabled validation layers against a list of rows."""
    report = RecordValidationReport(table=table)

    if schema is not None:
        results = validate_rows(schema, rows)
        merged = ValidationResult(table)
        for r in results:
            for e in r.errors:
                merged.add_error(e)
        report.schema_result = merged

    if contract_rules:
        from pipewarden.contract import ContractViolation
        violations: List = []
        for idx, row in enumerate(rows):
            for rule in contract_rules:
                v = rule.check(row, idx)
                if v is not None:
                    violations.append(v)
        report.contract_report = ContractReport(table=table, violations=violations)

    if expectation_rules:
        from pipewarden.expectation import ExpectationViolation
        exp_violations: List = []
        for idx, row in enumerate(rows):
            for rule in expectation_rules:
                v = rule.check(row, idx)
                if v is not None:
                    exp_violations.append(v)
        report.expectation_report = ExpectationReport(
            table=table, violations=exp_violations
        )

    return report
