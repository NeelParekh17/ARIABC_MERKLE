#!/usr/bin/env python3
"""Validate and curate existing OOM v2 artifacts; never contact hosts or run workloads.

Publish once into a fresh archive, preserving historical values and figures.
--validate-only works on either the raw campaign or the curated archive.
"""
import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
from pathlib import Path

CAMPAIGN = "v2_20261001"
REMOTE = "/home/neel/claude_ctl/results/oom_v2_20261001"
COMBOS = [("a", "0.0"), ("a", "0.99"), ("a", "1.2"), ("b", "0.99"),
          ("c", "0.99"), ("d", "0.99"), ("f", "0.99")]
MODES = ["pg", "bcdb_det", "bcdb_merkle"]
WORKERS = [1, 4, 8, 16]
RESERVE = 2**30


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(2**20), b""):
            value.update(chunk)
    return value.hexdigest()


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def identity(row):
    return row["workload"], str(float(row["skew"])), int(row["workers"]), row["mode"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(root):
    runs = list((root / "campaign").glob("run_*/summary.csv"))
    require(len(runs) == 1, "Expected one completed campaign")
    run = runs[0].parent
    rows = read_csv(root / "summary.csv")
    require(rows == read_csv(runs[0]), "Top-level and runner summaries differ")
    expected = {(wl, skew, w, mode) for wl, skew in COMBOS for w in WORKERS for mode in MODES}
    require(len(rows) == 84 and {identity(r) for r in rows} == expected, "84-case coverage mismatch")
    manifest = json.loads((run / "campaign.json").read_text())
    arguments = manifest["arguments"]
    for key, value in {"seed": 42, "trials": 1, "usertable_fillfactor": 90,
                       "gen_min_mem_available_mb": 4608, "pg_exec_mode": "event",
                       "pg_retry_jitter": "on", "reset_mode": "delta",
                       "delta_content_check": "sampled", "verify_mode": "fast"}.items():
        require(arguments[key] == value, f"Incorrect argument {key}")
    hashes = json.loads((root / "provenance/published_workloads.json").read_text())
    require(len(hashes) == 7 and hashes == manifest["workloads"], "Published workload manifest mismatch")
    source_files = json.loads((root / "provenance/snapshot_files.json").read_text())
    require(source_files == json.loads((root / "build_evidence/snapshot_files.json").read_text()),
            "Frozen source and successful build manifests differ")
    runner = "scripts/distributed/run_oom_100m_benchmark.py"
    require(source_files[runner] == manifest["source_sha256"][runner], "Measured runner differs from frozen source")
    freeze = json.loads((root / "provenance/freeze.json").read_text())
    require((root / "provenance/source.tar.gz.sha256").read_text().split()[0] == freeze["snapshot_sha256"],
            "Frozen source archive hash record mismatch")
    require((root / "build_evidence/BUILD_STATUS").read_text().strip() == "OK", "Successful PG build status missing")
    for name, metadata in hashes.items():
        require(digest(run / "workloads" / name) == metadata["sha256"], f"SQL hash mismatch: {name}")
    verification = json.loads((root / "baseline_evidence/verification.json").read_text())
    require(verification["keyspace"] == "100000000|1|100000000" and
            verification["reloptions"] == "{fillfactor=90}" and
            verification["merkle_verify"] == "t", "Baseline verification mismatch")
    golden = json.loads((root / "baseline_evidence/golden_manifest.json").read_text())
    plain = json.loads((root / "baseline_evidence/plain_manifest.json").read_text())
    require(golden["heap_bytes"] == plain["heap_bytes"] and
            plain["derived_from_pg_control_sha256"] == golden["pg_control_sha256"],
            "Plain and Merkle baseline derivation mismatch")
    executables = set()
    terminal = re.compile(r"^single-gateway-direct-(\d+)\s+1\s+(?:(?:SELECT|UPDATE)\s+1|INSERT\s+0\s+1)(?:\s|$)")
    for row in rows:
        case = run / Path(row["artifact_dir"]).name
        require(case.is_dir(), f"Missing case {case}")
        require(not (case / "FAILED.txt").exists() and not (case / "cleanup_errors.txt").exists(),
                f"Failed case {case}")
        result = json.loads((case / "result.json").read_text())
        setup = json.loads((case / "setup.json").read_text())
        for key, value in result["row"].items():
            require(row[key] == str(value), f"Summary differs from result: {case.name}/{key}")
        for key in ["divergence_count", "permanent_failures", "retry_exhausted"]:
            require(int(row[key]) == 0, f"Nonzero {key}: {case.name}")
        require(int(row["trial"]) == 1 and int(row["db_rows"]) == 100000000 and
                int(row["total_queries"]) == int(row["validated_completed_queries"]) == 20000 and
                row["isolation"] == "serializable" and row["shared_buffers"] == "32MB" and
                math.isfinite(float(row["tps"])) and float(row["tps"]) > 0,
                f"Invalid case counters/configuration: {case.name}")
        gateway = result["gateway"]
        require(gateway["gateway_returncode"] == 0 and gateway["validated_row_results"] == 20000 and
                gateway["empty_results"] == 0, f"Invalid gateway completion: {case.name}")
        settings = setup["settings"]
        for key, value in {"default_transaction_isolation": "serializable",
                           "transaction_isolation": "serializable", "shared_buffers": "4096",
                           "merkle_apply_synchronous_direct": "on", "fsync": "on",
                           "full_page_writes": "on", "synchronous_commit": "on",
                           "wal_compression": "off"}.items():
            require(settings[key] == value, f"Incorrect setting {key}: {case.name}")
        require(setup["sizes"]["heap_bytes"] == golden["heap_bytes"], f"Heap differs: {case.name}")
        require(setup["settle"]["settled"] and "CACHES_DROPPED" in setup["cache_drop_output"],
                f"Missing cold/settle evidence: {case.name}")
        for filename in ["io.json", "checkpoint.json", "checkpoint.log", "postgres.log",
                         "gateway.log", "server.log", "server.err.log", "telemetry.jsonl", "cache_policy.txt"]:
            require((case / filename).is_file(), f"Missing {filename}: {case.name}")
        ids = []
        with (case / "server.log").open() as handle:
            for line in handle:
                if line.startswith("single-gateway-direct-"):
                    match = terminal.match(line)
                    require(match is not None, f"Unsuccessful terminal record: {case.name}")
                    ids.append(int(match.group(1)))
        require(len(ids) == 20000 and set(ids) == set(range(1, 20001)), f"Request coverage: {case.name}")
        name = f"ycsb_{row['workload']}_skew_{float(row['skew'])}_20000.sql"
        require(row["workload_sha256"] == hashes[name]["sha256"], f"Case SQL hash: {case.name}")
        executables.add(tuple((e["host"], e["path"], e["sha256"]) for e in setup["provenance"]["executables"]))
        if row["mode"] == "pg":
            require(row["pg_retry_jitter"] == "on", f"PG retry jitter disabled: {case.name}")
        if row["mode"] == "bcdb_merkle":
            require(row["merkle_verify"] == "PASS" and (case / "merkle_verify.txt").read_text().strip() == "t",
                    f"Merkle verification failed: {case.name}")
            for key, value in {"partitions": 200, "fanout": 32, "split_threshold": 1024,
                               "merge_threshold": 256, "row_hash_format_version": 1}.items():
                require(setup["merkle_stats"][key] == verification["merkle_stats"][key] == value,
                        f"Merkle geometry/hash format mismatch: {case.name}")
    require(len(executables) == 1, "Executable provenance differs among modes/cases")
    require("CAMPAIGN_DONE 84/84" in (root / "status.txt").read_text(), "Campaign DONE missing")
    print("PUBLICATION_VALIDATION_PASS: 84 cases; 1,680,000 unique per-case terminal results; "
          "28 Merkle PASS; zero divergence/permanent failures/exhausted retries; 7 SQL hashes")
    return rows, run


def archive(source, destination):
    # Keep every case file and preparation metadata. Avoid duplicating identical SQL.
    directories = ["campaign", "generation", "baseline_evidence", "build_evidence", "provenance",
                   "preflight", "stages"] + [p.name for p in source.glob("prelaunch*") if p.is_dir()]
    files = sorted({p for name in directories for p in (source / name).rglob("*") if p.is_file()} |
                   {p for p in source.iterdir() if p.is_file()})
    require(not destination.exists(), f"Archive exists; use --validate-only: {destination}")
    estimate = sum(p.stat().st_size for p in files)
    require(shutil.disk_usage(destination.parent).free - estimate - 32 * 2**20 >= RESERVE,
            "Archive would leave less than 1 GiB free; stop before copying")
    destination.mkdir()
    records, seen_sql = [], {}
    for path in files:
        relative = path.relative_to(source)
        if path.name.endswith((".tar", ".tar.gz", ".tgz", ".tar.xz", ".tar.bz2")):
            raise ValueError(f"Unexpected bulk archive: {path}; retain remotely only")
        mapped = relative
        if "failed" in path.name or "stale" in path.name:
            mapped = Path("failed_preparation_no_data") / relative
        target = destination / mapped
        target.parent.mkdir(parents=True, exist_ok=True)
        sha = digest(path)
        if path.suffix == ".sql" and sha in seen_sql:
            target.symlink_to(os.path.relpath(seen_sql[sha], target.parent))
            policy = "identical SQL relative link"
        else:
            require(shutil.disk_usage(destination).free - path.stat().st_size >= RESERVE,
                    "Free-space reserve reached; archive remains partial")
            shutil.copy2(path, target)
            policy = "byte-identical copy"
            if path.suffix == ".sql":
                seen_sql[sha] = target
        require(digest(target) == sha, f"Copy hash mismatch: {target}")
        records.append([str(relative), str(mapped), sha, path.stat().st_size, policy])
    with (destination / "SOURCE_TO_ARCHIVE.csv").open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["source_path", "archive_path", "sha256", "bytes", "policy"])
        writer.writerows(records)
    controller = destination / "controller_scripts"
    controller.mkdir()
    for name in ["arguments.sh", "campaign_v2.sh"]:
        shutil.copy2(Path(__file__).parent / name, controller / name)
    (controller / "README.md").write_text(
        "# Final controller configuration\n\n"
        "These copies come from the supplied publication workspace, not a new remote read. "
        "The final V2_COMMON array matches the saved campaign.json arguments and the documented "
        "commands. The controller scripts differ from those embedded in the frozen source tarball: "
        "the orchestrator changed the load memory guard to 4608 MB and added --checksum to "
        "snapshot rsync after source freeze. Use these final orchestration settings with the "
        "frozen PG/C++/runner source; the measured runner hash matches the frozen manifest.\n")
    (destination / "CURATION.md").write_text(
        "# Fresh OOM v2 archive\n\n"
        f"Original: `neel@10.129.27.111:{REMOTE}/`. Local input: `{source}`.\n\n"
        "Every per-case file is retained unchanged: results, setup, terminal logs/profiles, "
        "I/O, checkpoint, cache policy, telemetry, and Merkle verification. Campaign, generation, "
        "baseline verification, preflight, build and source provenance are retained. Identical SQL "
        "copies are relative links to one archived copy; SOURCE_TO_ARCHIVE.csv records each input "
        "hash and path mapping. Raw CSV/JSON paths remain the original remote paths.\n\n"
        "Failed build-attempt logs are under `failed_preparation_no_data/`; they produced no data. "
        "Generation `run_20261001_193009_25578d88` is the aborted load; "
        "`run_20261001_193334_df20ccfe` is the single completed clean generation. The partial "
        "remote load was deleted before restarting. The MemAvailable guard changed from 12288 "
        "to 4608 MB because this fork pins about 6.3 GB shared memory even with 512MB buffers "
        "(orchestrator account, not a new measurement here). Stale same-size source/build state "
        "on .247 was moved aside; snapshot rsync now uses --checksum.\n\n"
        "No source/build tarballs or database files were copied. `remote_bulk_sha256.txt` "
        f"records the omitted archives and their SHA-256; originals remain at `{REMOTE}/` on .111. "
        "Missing failed_*_provenance directories were already omitted from the local input. "
        "No build, database, benchmark, or remote command runs during publication.\n\n"
        "`controller_scripts/` preserves the supplied final controller configuration; its README "
        "distinguishes post-freeze orchestration corrections from frozen built source.\n\n"
        "`publication_input/` preserves the pre-v2 consolidated summary and README. "
        "`ARCHIVE_SHA256SUMS` covers every archived file (including linked SQL) except itself.\n")


def append_summary(root, archive_root, rows, run):
    path = root / "summary.csv"
    original = path.read_text()
    historical = read_csv(path)
    require(not any(r.get("campaign") == CAMPAIGN for r in historical), "V2 already appended")
    fields = next(csv.reader(io.StringIO(original)))
    require("campaign" not in fields and "fillfactor" not in fields, "Unexpected existing campaign schema")
    extra = ["campaign", "fillfactor"]
    # Preserve the literal original row representation as well as every existing field value.
    lines = original.splitlines()
    output = [lines[0] + "," + ",".join(extra)]
    for line, row in zip(lines[1:], historical):
        label = {"bcdb_merkle_s1024": "s1024_20261001", "det_control": "det_control_20261001"}.get(
            row["mode"], "published_20260929")
        output.append(line + "," + label + ",")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields + extra, lineterminator="\n")
    for row in rows:
        item = dict(row, campaign=CAMPAIGN, fillfactor=90,
                    source_run=str(archive_root.relative_to(root) / "campaign" / run.name),
                    artifact_dir=str((archive_root / "campaign" / run.name / Path(row["artifact_dir"]).name).resolve()))
        if row["mode"] == "bcdb_merkle":
            setup = json.loads((run / Path(row["artifact_dir"]).name / "setup.json").read_text())
            for key in ["fanout", "partitions", "split_threshold", "merge_threshold"]:
                item[key] = setup["merkle_stats"][key]
            item["baseline_node_rows"] = setup["merkle_stats"]["total_nodes"]
            item["baseline_leaf_rows"] = setup["merkle_stats"]["leaf_nodes"]
        writer.writerow(item)
    path.write_text("\n".join(output) + "\n" + buffer.getvalue())
    after = read_csv(path)
    require(len(after) == len(historical) + 84, "Consolidated count mismatch")
    require([{k: r[k] for k in fields} for r in after[:len(historical)]] == historical,
            "Historical values changed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(".bench_tmp/oom_v2_20261001"))
    parser.add_argument("--root", type=Path, default=Path("Final_Results/OOM_100M"))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    rows, run = validate(args.source)
    if args.validate_only:
        return
    archive_root = args.root / "runs" / CAMPAIGN
    archive(args.source, archive_root)
    inputs = archive_root / "publication_input"
    inputs.mkdir()
    for name in ["summary.csv", "README.md"]:
        shutil.copy2(args.root / name, inputs / ("summary_before_v2.csv" if name == "summary.csv" else "README_before_v2.md"))
    previous = args.root / "figures/previous"
    require(not previous.exists(), "Previous figures already exist; refuse overwrite")
    previous.mkdir()
    for path in sorted((args.root / "figures").iterdir()):
        if path.is_file():
            path.rename(previous / path.name)
    append_summary(args.root, archive_root, rows, run)
    checksums = [f"{digest(p)}  {p.relative_to(archive_root)}\n" for p in sorted(archive_root.rglob("*"))
                 if p.is_file() and p.name != "ARCHIVE_SHA256SUMS"]
    (archive_root / "ARCHIVE_SHA256SUMS").write_text("".join(checksums))
    print(f"Published {archive_root}; 136 historical + 84 v2 = 220 rows; previous figures preserved")


if __name__ == "__main__":
    main()
