"""Tests for pipewarden.contract_loader module."""

import json
import os
import tempfile

import pytest
from pipewarden.contract_loader import (
    load_contract_from_dict,
    load_contract_from_file,
    load_contracts_from_dir,
)


def _basic_dict():
    return {
        "table": "orders",
        "rules": [
            {"type": "not_null", "name": "id_not_null", "field": "id", "description": "id must not be null"},
            {"type": "min_value", "name": "positive_amount", "field": "amount", "value": 0, "description": "amount >= 0"},
        ],
    }


def test_load_from_dict_basic():
    contract = load_contract_from_dict(_basic_dict())
    assert contract.table == "orders"
    assert len(contract.rules) == 2


def test_load_from_dict_missing_table_raises():
    with pytest.raises(ValueError, match="table"):
        load_contract_from_dict({"rules": []})


def test_load_from_dict_unknown_rule_type_raises():
    data = {"table": "t", "rules": [{"type": "magic", "field": "x", "name": "n", "description": "d"}]}
    with pytest.raises(ValueError, match="Unknown contract rule type"):
        load_contract_from_dict(data)


def test_not_null_rule_passes():
    contract = load_contract_from_dict(_basic_dict())
    report = contract.enforce([{"id": 1, "amount": 10}])
    assert report.passed


def test_not_null_rule_fails_on_null():
    contract = load_contract_from_dict(_basic_dict())
    report = contract.enforce([{"id": None, "amount": 10}])
    assert not report.passed


def test_min_value_rule_fails_when_below():
    contract = load_contract_from_dict(_basic_dict())
    report = contract.enforce([{"id": 1, "amount": -5}])
    assert not report.passed


def test_not_empty_rule():
    data = {
        "table": "users",
        "rules": [{"type": "not_empty", "name": "name_not_empty", "field": "name", "description": "name must not be blank"}],
    }
    contract = load_contract_from_dict(data)
    assert contract.enforce([{"name": "Alice"}]).passed
    assert not contract.enforce([{"name": "  "}]).passed


def test_load_from_file(tmp_path):
    p = tmp_path / "orders.json"
    p.write_text(json.dumps(_basic_dict()))
    contract = load_contract_from_file(str(p))
    assert contract.table == "orders"


def test_load_contracts_from_dir(tmp_path):
    for name in ["a", "b"]:
        (tmp_path / f"{name}.json").write_text(json.dumps({"table": name, "rules": []}))
    contracts = load_contracts_from_dir(str(tmp_path))
    assert len(contracts) == 2
    tables = {c.table for c in contracts}
    assert tables == {"a", "b"}
