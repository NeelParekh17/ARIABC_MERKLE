#!/usr/bin/env python3
"""Create a scoped source snapshot manifest for the synced remote benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


IGNORED_DIR_NAMES = {".git", ".bench_tmp", "__pycache__", "results", "fetched"}
IGNORED_FILE_SUFFIXES = {
    ".pyc", ".pyo", ".copybin", ".tar", ".zip",
    ".o", ".a", ".d", ".gcda", ".gcno",
    ".pc", ".list", ".stamp",
}
IGNORED_FILE_NAMES = {
    "results.zip",
    "bki-stamp",
    "header-stamp",
    "schemapg.h",
    "errcodes.h",
    "fmgroids.h",
    "fmgrprotos.h",
    "lwlocknames.h",
    "lwlocknames.c",
    "probes.h",
    "plerrcodes.h",
    "pg_config.h",
    "pg_config_ext.h",
    "pg_config_os.h",
    "ecpg_config.h",
    "pg_config_paths.h",
    "objfiles.txt",
    "exports.list",
    "snowball_create.sql",
}


def read_roots(path: Path) -> list[str]:
    roots: list[str] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        roots.append(line)
    return roots


def is_selected(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root)
    if any(part in IGNORED_DIR_NAMES for part in rel.parts):
        return False
    if path.name in IGNORED_FILE_NAMES:
        return False
    # Shared-library files (*.so, *.so.1, libfoo.so.1.2.3, etc.)
    if path.suffix == ".so" or ".so." in path.name:
        return False
    if path.suffix in IGNORED_FILE_SUFFIXES:
        return False
    # Generated catalog headers: *_d.h and *_d.dat
    name = path.name
    if name.endswith("_d.h") or name.endswith("_d.dat"):
        return False
    return True


def gather_files(repo_root: Path, roots: list[str]) -> list[Path]:
    files: dict[str, Path] = {}
    for root_text in roots:
        root = repo_root / root_text
        if not root.exists() and not root.is_symlink():
            raise RuntimeError(f"missing source root: {root_text}")
        if root.is_file() or root.is_symlink():
            candidates = [root]
        else:
            candidates = [p for p in root.rglob("*") if p.is_file() or p.is_symlink()]
        for path in candidates:
            if not is_selected(path, repo_root):
                continue
            rel = path.relative_to(repo_root).as_posix()
            files.setdefault(rel, path)
    return [files[key] for key in sorted(files)]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_entry(path: Path, repo_root: Path) -> dict[str, object]:
    data = path.read_bytes()
    rel = path.relative_to(repo_root).as_posix()
    return {
        "relative_path": rel,
        "byte_size": len(data),
        "sha256": sha256_bytes(data),
    }


def git_optional(repo_root: Path) -> tuple[str | None, str | None]:
    head = None
    status = None
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
    except Exception:
        head = None
    try:
        status = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo_root, text=True)
        status = status.rstrip("\n")
    except Exception:
        status = None
    return head, status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--roots-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    roots = read_roots(args.roots_file)
    files = gather_files(repo_root, roots)
    entries = [file_entry(path, repo_root) for path in files]
    tree_hash = sha256_bytes(
        "\n".join(
            f"{entry['sha256']} {entry['byte_size']} {entry['relative_path']}"
            for entry in sorted(entries, key=lambda item: item["relative_path"])
        ).encode()
        + b"\n"
    )
    git_head, git_status = git_optional(repo_root)
    payload = {
        "run_id": args.run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "local_repo_path": str(repo_root),
        "git_head_if_available": git_head,
        "git_status_porcelain_if_available": git_status,
        "source_roots": roots,
        "included_file_count": len(entries),
        "source_tree_sha256": tree_hash,
        "files": sorted(entries, key=lambda item: item["relative_path"]),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
