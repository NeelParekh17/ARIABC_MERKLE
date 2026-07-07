#!/usr/bin/env python3
"""Package compact recovery benchmark artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path


ALLOWED = {
    "config.json",
    "environment.txt",
    "host_info.json",
    "python_environment.json",
    "progress.json",
    "progress.jsonl",
    "source_snapshot.json",
    "dataset_sizes.csv",
    "runs.csv",
    "phase_timings.csv",
    "timing_contract.csv",
    "planner_checks.csv",
    "bucket_consistency_summary.csv",
    "schema_fidelity.csv",
    "corruption_manifest.json",
    "verification_results.csv",
    "profile_operations.csv",
    "profile_summary.csv",
    "profile_summary_per_run.csv",
    "profile_summary_by_geometry.csv",
    "merkle_backend_profile.csv",
    "deep_plan_summary.csv",
    "deep_plan_profiles.jsonl",
    "profiling_report.md",
    "artifact_manifest.json",
    "stdout.log",
    "stderr.log",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def allowed_file(path: Path, result_dir: Path, artifact_mode: str) -> bool:
    rel = path.relative_to(result_dir).as_posix()
    if rel in ALLOWED:
        return True
    if rel.startswith("plots/") and rel.endswith(".svg"):
        return True
    if artifact_mode == "debug" and rel == "bucket_consistency.csv":
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--artifact-mode", choices=["summary", "debug"], default="summary")
    parser.add_argument("--max-summary-mb", type=int, default=50)
    args = parser.parse_args()

    result_dir = args.result_dir.resolve()
    output = args.output or result_dir.with_suffix(".tar.gz")
    config = json.loads((result_dir / "config.json").read_text())
    if int(config.get("benchmark_schema_version", 0)) < 2:
        raise RuntimeError("refusing to package pre-v2 benchmark results")

    forbidden = []
    files = []
    for path in sorted(p for p in result_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(result_dir).as_posix()
        if path.suffix == ".copybin" or rel.startswith(("pgdata/", "scratch/", "build/", "install/")) or path.suffix in {".tar", ".zip"}:
            forbidden.append(rel)
            continue
        if allowed_file(path, result_dir, args.artifact_mode):
            files.append(path)
    if forbidden:
        raise RuntimeError(f"forbidden artifact(s) present: {forbidden[:5]}")

    manifest = []
    for path in files:
        manifest.append(
            {
                "path": path.relative_to(result_dir).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    manifest_path = result_dir / "artifact_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if manifest_path not in files:
        files.append(manifest_path)

    total_size = sum(p.stat().st_size for p in files)
    if args.artifact_mode == "summary" and total_size > args.max_summary_mb * 1024 * 1024:
        raise RuntimeError(f"summary artifact size {total_size} exceeds {args.max_summary_mb} MiB")

    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w:gz") as tar:
        for path in files:
            tar.add(path, arcname=path.relative_to(result_dir).as_posix())
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
