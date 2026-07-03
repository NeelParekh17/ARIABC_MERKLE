#!/usr/bin/env python3
"""Validate the Python environment contract for the recovery benchmark."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path


def parse_version(text: str) -> tuple[int, int]:
    parts = text.split(".")
    if len(parts) < 2:
        raise ValueError(f"invalid version string: {text}")
    return int(parts[0]), int(parts[1])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, required=True)
    args = parser.parse_args()

    contract = json.loads(args.contract.read_text())
    min_major, min_minor = parse_version(contract["python_min_version"])
    if sys.version_info[:2] < (min_major, min_minor):
        raise RuntimeError(
            f"python {min_major}.{min_minor}+ required, found {sys.version_info[0]}.{sys.version_info[1]}"
        )

    missing = []
    for module_name in contract.get("required_modules", []):
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            missing.append(f"{module_name}: {exc}")
    if missing:
        raise RuntimeError("missing required Python module(s): " + "; ".join(missing))

    print(json.dumps({"python": f"{sys.version_info[0]}.{sys.version_info[1]}", "contract": str(args.contract)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
