#!/usr/bin/env python3
"""Verify a scoped source snapshot manifest against a local tree."""

from __future__ import annotations

import argparse
import hashlib
import json
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


def is_selected(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root)
    if any(part in IGNORED_DIR_NAMES for part in rel.parts):
        return False
    if "openssl" in rel.parts or "openssl_compat_libs" in rel.parts:
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


def collect_files(repo_root: Path, roots: list[str]) -> list[Path]:
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
            files.setdefault(path.relative_to(repo_root).as_posix(), path)
    return [files[key] for key in sorted(files)]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    manifest = json.loads(args.manifest.read_text())
    roots = list(manifest.get("source_roots") or [])
    if not roots:
        raise RuntimeError("manifest missing source_roots")

    actual_files = collect_files(repo_root, roots)
    actual_map = {
        path.relative_to(repo_root).as_posix(): path
        for path in actual_files
    }
    expected_entries = manifest.get("files") or []
    expected_map = {entry["relative_path"]: entry for entry in expected_entries}

    missing = sorted(set(expected_map) - set(actual_map))
    unexpected = sorted(set(actual_map) - set(expected_map))
    if missing:
        raise RuntimeError(f"missing synced file(s): {missing[:5]}")
    if unexpected:
        raise RuntimeError(f"unexpected file(s) in synced source roots: {unexpected[:5]}")

    for rel, entry in expected_map.items():
        path = actual_map[rel]
        data = path.read_bytes()
        byte_size = len(data)
        digest = sha256_bytes(data)
        if byte_size != int(entry["byte_size"]):
            raise RuntimeError(f"size mismatch for {rel}: {byte_size} != {entry['byte_size']}")
        if digest != entry["sha256"]:
            raise RuntimeError(f"sha256 mismatch for {rel}")

    tree_hash = sha256_bytes(
        "\n".join(
            f"{entry['sha256']} {entry['byte_size']} {entry['relative_path']}"
            for entry in sorted(expected_entries, key=lambda item: item["relative_path"])
        ).encode()
        + b"\n"
    )
    if tree_hash != manifest.get("source_tree_sha256"):
        raise RuntimeError("source_tree_sha256 mismatch")

    if int(manifest.get("included_file_count", -1)) != len(expected_entries):
        raise RuntimeError("included_file_count mismatch")
    print(args.manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
