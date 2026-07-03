#!/usr/bin/env python3
"""Write host metadata for a recovery benchmark run."""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover - best effort metadata
    psycopg = None


def read_mem_total_kib() -> int | None:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return None
    for line in meminfo.read_text().splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return int(parts[1])
                except ValueError:
                    return None
    return None


def filesystem_info(path: Path) -> dict[str, int]:
    st = os.statvfs(path)
    return {
        "path": str(path),
        "block_size": st.f_bsize,
        "fragment_size": st.f_frsize,
        "blocks": st.f_blocks,
        "blocks_free": st.f_bfree,
        "blocks_available": st.f_bavail,
        "bytes_total": st.f_blocks * st.f_frsize,
        "bytes_free": st.f_bavail * st.f_frsize,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--filesystem-path", type=Path, default=Path.cwd())
    args = parser.parse_args()

    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "cpu": platform.processor() or platform.machine(),
        "memory": {
            "mem_total_kib": read_mem_total_kib(),
        },
        "filesystem": filesystem_info(args.filesystem_path.resolve()),
        "python_executable": sys.executable,
        "python_version": sys.version.replace(os.linesep, " "),
        "psycopg_version": getattr(psycopg, "__version__", None),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
