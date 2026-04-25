"""CLI sub-commands for exporting validation and profile results."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewarden.schema_loader import load_schema_from_file
from pipewarden.runner import validate_rows
from pipewarden.profiler import profile_rows
from pipewarden.exporter import (
    export_results_json,
    export_results_csv,
    export_profile_json,
)


def _load_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def cmd_export_validate(args: argparse.Namespace) -> int:
    schema = load_schema_from_file(args.schema)
    rows = _load_jsonl(args.data)
    results = validate_rows(schema, rows)

    if args.format == "json":
        output = export_results_json([results])
    elif args.format == "csv":
        output = export_results_csv([results])
    else:
        print(f"Unknown format: {args.format}", file=sys.stderr)
        return 1

    _write_output(output, args.output)
    return 0 if results.is_valid else 1


def cmd_export_profile(args: argparse.Namespace) -> int:
    schema = load_schema_from_file(args.schema)
    rows = _load_jsonl(args.data)
    report = profile_rows(schema, rows)
    output = export_profile_json(report)
    _write_output(output, args.output)
    return 0


def _write_output(content: str, path: str | None) -> None:
    if path:
        Path(path).write_text(content, encoding="utf-8")
    else:
        print(content)


def build_export_parser(subparsers: argparse._SubParsersAction) -> None:
    # validate export
    p_val = subparsers.add_parser(
        "export-validate", help="Validate data and export results"
    )
    p_val.add_argument("schema", help="Path to schema JSON file")
    p_val.add_argument("data", help="Path to JSONL data file")
    p_val.add_argument(
        "--format", choices=["json", "csv"], default="json", help="Output format"
    )
    p_val.add_argument("--output", default=None, help="Output file path (stdout if omitted)")
    p_val.set_defaults(func=cmd_export_validate)

    # profile export
    p_prof = subparsers.add_parser(
        "export-profile", help="Profile data and export report"
    )
    p_prof.add_argument("schema", help="Path to schema JSON file")
    p_prof.add_argument("data", help="Path to JSONL data file")
    p_prof.add_argument("--output", default=None, help="Output file path (stdout if omitted)")
    p_prof.set_defaults(func=cmd_export_profile)
