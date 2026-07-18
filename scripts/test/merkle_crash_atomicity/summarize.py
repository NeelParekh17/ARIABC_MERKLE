#!/usr/bin/env python3
"""Summarize one crash-atomicity campaign and fail if any case is incomplete."""

from __future__ import annotations

import json
import pathlib
import sys


def parse_env(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key] = value
    return values


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize.py RESULT_ROOT", file=sys.stderr)
        return 2

    root = pathlib.Path(sys.argv[1]).resolve()
    results = []
    for result_file in sorted(root.glob("*/result.env")):
        row = parse_env(result_file)
        row["artifact"] = str(result_file.parent)
        results.append(row)

    failed = [row for row in results if row.get("status") != "PASS"]
    summary = {
        "result_root": str(root),
        "cases_recorded": len(results),
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "all_runs_valid": bool(results) and not failed,
        "failures": failed,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["all_runs_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
