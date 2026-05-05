"""Formatting helpers for RecordValidationReport."""
from pipewarden.record_validator import RecordValidationReport


def format_record_validation_report(report: RecordValidationReport) -> str:
    lines = [f"Record Validation — {report.table}"]
    status = "PASSED" if report.is_valid else "FAILED"
    lines.append(f"  Status : {status}")
    lines.append(f"  Issues : {report.total_issues}")

    if report.schema_result and report.schema_result.errors:
        lines.append(f"  Schema errors ({len(report.schema_result.errors)}):")
        for e in report.schema_result.errors:
            lines.append(f"    - {e}")

    if report.contract_report and report.contract_report.violations:
        lines.append(f"  Contract violations ({len(report.contract_report.violations)}):")
        for v in report.contract_report.violations:
            lines.append(f"    - {v}")

    if report.expectation_report and report.expectation_report.violations:
        lines.append(
            f"  Expectation violations ({len(report.expectation_report.violations)}):"
        )
        for v in report.expectation_report.violations:
            lines.append(f"    - {v}")

    return "\n".join(lines)


def record_validation_summary(report: RecordValidationReport) -> str:
    status = "ok" if report.is_valid else "FAILED"
    return (
        f"{report.table}: {status} "
        f"({report.total_issues} issue(s))"
    )
