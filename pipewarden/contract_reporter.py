"""Formatting utilities for data contract reports."""

from pipewarden.contract import ContractReport, ContractViolation


def format_violation(violation: ContractViolation) -> str:
    return str(violation)


def format_contract_report(report: ContractReport) -> str:
    lines = []
    status = "PASSED" if report.passed else "FAILED"
    lines.append(f"Contract [{report.table}]: {status}")
    lines.append(f"  Rows checked : {report.total_rows}")
    lines.append(f"  Violations   : {report.violation_count}")
    if not report.passed:
        lines.append("  Details:")
        for v in report.violations:
            lines.append(f"    - {format_violation(v)}")
    return "\n".join(lines)


def contract_summary(reports: list) -> str:
    total = len(reports)
    passed = sum(1 for r in reports if r.passed)
    failed = total - passed
    noun = "contract" if total == 1 else "contracts"
    lines = [
        f"Contract summary: {total} {noun} checked, {passed} passed, {failed} failed."
    ]
    for r in reports:
        mark = "OK" if r.passed else "FAIL"
        lines.append(f"  [{mark}] {r.table}")
    return "\n".join(lines)
