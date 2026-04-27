"""Load DataContract definitions from dict or JSON file."""

import json
from pathlib import Path
from typing import Any, Callable, Dict, List

from pipewarden.contract import ContractRule, DataContract

# Registry of built-in predicate factories keyed by rule type
_BUILTIN: Dict[str, Callable[[Dict[str, Any]], Callable[[Dict], bool]]] = {
    "not_null": lambda cfg: (lambda row: row.get(cfg["field"]) is not None),
    "min_value": lambda cfg: (lambda row: (v := row.get(cfg["field"])) is not None and v >= cfg["value"]),
    "max_value": lambda cfg: (lambda row: (v := row.get(cfg["field"])) is not None and v <= cfg["value"]),
    "not_empty": lambda cfg: (lambda row: bool(str(row.get(cfg["field"], "")).strip())),
}


def _build_predicate(rule_cfg: Dict[str, Any]) -> Callable[[Dict], bool]:
    rule_type = rule_cfg.get("type", "")
    factory = _BUILTIN.get(rule_type)
    if factory is None:
        raise ValueError(f"Unknown contract rule type: '{rule_type}'")
    return factory(rule_cfg)


def load_contract_from_dict(data: Dict[str, Any]) -> DataContract:
    table = data.get("table")
    if not table:
        raise ValueError("Contract definition must include a 'table' key.")
    contract = DataContract(table=table)
    for rule_cfg in data.get("rules", []):
        predicate = _build_predicate(rule_cfg)
        contract.add_rule(
            ContractRule(
                name=rule_cfg.get("name", rule_cfg.get("type", "unnamed")),
                description=rule_cfg.get("description", ""),
                predicate=predicate,
            )
        )
    return contract


def load_contract_from_file(path: str) -> DataContract:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return load_contract_from_dict(data)


def load_contracts_from_dir(directory: str) -> List[DataContract]:
    contracts = []
    for p in sorted(Path(directory).glob("*.json")):
        contracts.append(load_contract_from_file(str(p)))
    return contracts
