"""Tests for record_validator and record_validator_reporter."""
import pytest
from pipewarden.schema import TableSchema, FieldSchema, FieldType
from pipewarden.contract import ContractRule
from pipewarden.expectation import ExpectationRule
from pipewarden.record_validator import validate_records, RecordValidationReport
from pipewarden.record_validator_reporter import (
    format_record_validation_report,
    record_validation_summary,
)


def _make_schema():
    return TableSchema(
        name="orders",
        fields=[
            FieldSchema(name="id", type=FieldType.INTEGER, nullable=False),
            FieldSchema(name="amount", type=FieldType.FLOAT, nullable=False),
        ],
    )


def _positive_contract():
    return ContractRule("amount_positive", lambda row, _: row.get("amount", 0) > 0)


def _nonempty_expectation():
    return ExpectationRule("id_present", lambda row, _: row.get("id") is not None)


# --- RecordValidationReport ---

def test_report_is_valid_when_no_issues():
    report = RecordValidationReport(table="orders")
    assert report.is_valid is True


def test_report_total_issues_zero_when_clean():
    report = RecordValidationReport(table="orders")
    assert report.total_issues == 0


# --- validate_records with schema ---

def test_schema_validation_passes_for_valid_rows():
    schema = _make_schema()
    rows = [{"id": 1, "amount": 9.99}, {"id": 2, "amount": 5.0}]
    report = validate_records("orders", rows, schema=schema)
    assert report.is_valid
    assert report.total_issues == 0


def test_schema_validation_fails_for_wrong_type():
    schema = _make_schema()
    rows = [{"id": "not-an-int", "amount": 5.0}]
    report = validate_records("orders", rows, schema=schema)
    assert not report.is_valid
    assert report.total_issues > 0


def test_schema_validation_fails_for_null_in_required_field():
    schema = _make_schema()
    rows = [{"id": None, "amount": 5.0}]
    report = validate_records("orders", rows, schema=schema)
    assert not report.is_valid


# --- validate_records with contracts ---

def test_contract_passes_for_valid_rows():
    rows = [{"id": 1, "amount": 10.0}]
    report = validate_records("orders", rows, contract_rules=[_positive_contract()])
    assert report.is_valid
    assert report.contract_report is not None
    assert len(report.contract_report.violations) == 0


def test_contract_fails_for_negative_amount():
    rows = [{"id": 1, "amount": -5.0}]
    report = validate_records("orders", rows, contract_rules=[_positive_contract()])
    assert not report.is_valid
    assert len(report.contract_report.violations) == 1


# --- validate_records with expectations ---

def test_expectation_passes_when_id_present():
    rows = [{"id": 42, "amount": 1.0}]
    report = validate_records("orders", rows, expectation_rules=[_nonempty_expectation()])
    assert report.is_valid


def test_expectation_fails_when_id_missing():
    rows = [{"id": None, "amount": 1.0}]
    report = validate_records("orders", rows, expectation_rules=[_nonempty_expectation()])
    assert not report.is_valid
    assert report.total_issues == 1


# --- combined ---

def test_combined_all_layers_pass():
    schema = _make_schema()
    rows = [{"id": 1, "amount": 3.0}]
    report = validate_records(
        "orders",
        rows,
        schema=schema,
        contract_rules=[_positive_contract()],
        expectation_rules=[_nonempty_expectation()],
    )
    assert report.is_valid
    assert report.total_issues == 0


def test_combined_counts_issues_across_layers():
    schema = _make_schema()
    rows = [{"id": None, "amount": -1.0}]
    report = validate_records(
        "orders",
        rows,
        schema=schema,
        contract_rules=[_positive_contract()],
        expectation_rules=[_nonempty_expectation()],
    )
    assert not report.is_valid
    assert report.total_issues >= 3


# --- reporter ---

def test_format_report_contains_table_name():
    report = validate_records("orders", [{"id": 1, "amount": 5.0}])
    text = format_record_validation_report(report)
    assert "orders" in text


def test_format_report_shows_passed_when_valid():
    report = validate_records("orders", [{"id": 1, "amount": 5.0}])
    text = format_record_validation_report(report)
    assert "PASSED" in text


def test_format_report_shows_failed_when_invalid():
    schema = _make_schema()
    report = validate_records("orders", [{"id": "bad", "amount": 5.0}], schema=schema)
    text = format_record_validation_report(report)
    assert "FAILED" in text


def test_summary_ok_for_valid():
    report = RecordValidationReport(table="orders")
    line = record_validation_summary(report)
    assert "ok" in line
    assert "orders" in line


def test_summary_failed_for_invalid():
    schema = _make_schema()
    report = validate_records("orders", [{"id": None, "amount": -1.0}], schema=schema)
    line = record_validation_summary(report)
    assert "FAILED" in line
