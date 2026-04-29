"""CLI sub-command for sampling rows from a JSONL file."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewarden.sampling import SampleConfig, sample_rows
from pipewarden.sampling_reporter import format_sample_preview


def _load_jsonl(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"[WARN] line {lineno} is not valid JSON: {exc}", file=sys.stderr)
    return rows


def cmd_sample(args: argparse.Namespace) -> int:
    """Entry-point for the *sample* sub-command."""
    rows = _load_jsonl(args.data)

    config = SampleConfig(
        strategy=args.strategy,
        size=args.size,
        seed=args.seed,
    )

    result = sample_rows(rows, config)
    print(format_sample_preview(result, max_rows=args.preview))

    if args.output:
        out_path = Path(args.output)
        with out_path.open("w", encoding="utf-8") as fh:
            for row in result.rows:
                fh.write(json.dumps(row) + "\n")
        print(f"Wrote {result.sample_size} rows to {out_path}")

    return 0


def build_sample_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("sample", help="Sample rows from a JSONL data file")
    p.add_argument("data", help="Path to JSONL data file")
    p.add_argument(
        "--strategy",
        choices=["random", "first", "last"],
        default="random",
        help="Sampling strategy (default: random)",
    )
    p.add_argument(
        "--size",
        type=int,
        default=100,
        help="Number of rows to sample (default: 100)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    p.add_argument(
        "--preview",
        type=int,
        default=5,
        metavar="N",
        help="Number of preview rows to print (default: 5)",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Optional path to write sampled rows as JSONL",
    )
    p.set_defaults(func=cmd_sample)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipewarden-sample")
    sub = parser.add_subparsers(dest="command")
    build_sample_parser(sub)
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
