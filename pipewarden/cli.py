"""Minimal CLI entry-point for pipewarden.

Usage examples
--------------
    pipewarden validate --schema schemas/users.json --data data/users.jsonl
    pipewarden validate --schema schemas/ --data data/users.jsonl --table users
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

from pipewarden.schema_loader import load_schema_from_file, load_schemas_from_dir
from pipewarden.runner import validate_rows


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Read a newline-delimited JSON file into a list of dicts."""
    rows = []
    with path.open() as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"[ERROR] Invalid JSON on line {lineno}: {exc}", file=sys.stderr)
                sys.exit(1)
    return rows


def cmd_validate(args: argparse.Namespace) -> int:
    schema_path = Path(args.schema)

    if not schema_path.exists():
        print(f"[ERROR] Schema path not found: {schema_path}", file=sys.stderr)
        return 1

    if schema_path.is_dir():
        if not args.table:
            print("[ERROR] --table is required when --schema points to a directory.", file=sys.stderr)
            return 1
        schemas = load_schemas_from_dir(schema_path)
        if args.table not in schemas:
            print(f"[ERROR] Table '{args.table}' not found in schema directory.", file=sys.stderr)
            return 1
        schema = schemas[args.table]
    else:
        schema = load_schema_from_file(schema_path)

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"[ERROR] Data file not found: {data_path}", file=sys.stderr)
        return 1

    rows = _load_jsonl(data_path)
    result = validate_rows(rows, schema, allow_extra_fields=args.allow_extra)

    if result.is_valid:
        print(f"[OK] '{schema.name}' — {len(rows)} row(s) validated successfully.")
        return 0
    else:
        print(f"[FAIL] '{schema.name}' — {result.error_count} error(s) found:")
        for err in result.errors:
            print(f"  {err}")
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewarden",
        description="Validate ETL pipeline data against JSON schemas.",
    )
    sub = parser.add_subparsers(dest="command")

    val = sub.add_parser("validate", help="Validate a JSONL data file against a schema.")
    val.add_argument("--schema", required=True, help="Path to a schema JSON file or directory.")
    val.add_argument("--data", required=True, help="Path to a JSONL data file.")
    val.add_argument("--table", default=None, help="Table name (required when --schema is a directory).")
    val.add_argument("--allow-extra", action="store_true", help="Ignore fields not defined in the schema.")

    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "validate":
        sys.exit(cmd_validate(args))
    else:
        print(f"[ERROR] Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)
