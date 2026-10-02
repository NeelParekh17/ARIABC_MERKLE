#!/usr/bin/env python3
"""Read existing OOM artifacts; never contact hosts or execute workloads."""
import argparse
import collections
import csv
import hashlib
import json
import re
from pathlib import Path


def load_case(path):
    path = Path(path)
    result = json.loads((path / "result.json").read_text())
    row = result["row"]
    setup = json.loads((path / "setup.json").read_text())
    io = json.loads((path / "io.json").read_text())
    checkpoint = json.loads((path / "checkpoint.json").read_text())
    assert not (path / "FAILED.txt").exists(), path
    assert not (path / "cleanup_errors.txt").exists(), path
    assert row["divergence_count"] == row["permanent_failures"] == 0, path
    assert row["validated_completed_queries"] == row["total_queries"], path
    assert row.get("isolation", setup["settings"]["transaction_isolation"]) == "serializable", path
    assert setup["settings"]["full_page_writes"] == "on", path
    if row["mode"] == "bcdb_merkle":
        assert row["merkle_verify"] == "PASS", path
        assert (path / "merkle_verify.txt").read_text().strip() == "t", path
    for metric in ("blks_read", "blks_hit"):
        assert row[metric] == io["pg_after"][metric] - io["pg_before"][metric], path
    assert abs(row["device_read_mib"] -
               (io["device_after"]["read_sectors"] - io["device_before"]["read_sectors"]) / 2048) < .01
    return row, setup, io, checkpoint


def workload_counts(path, row):
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == row["workload_sha256"], path
    counts = collections.Counter()
    for line in data.decode().splitlines():
        sql = line.strip().upper()
        if not sql:
            continue
        if sql.startswith(("UPDATE ", "WITH YCSB_READ ")):
            counts["updates"] += 1
        elif sql.startswith("INSERT "):
            counts["inserts"] += 1
        elif sql.startswith("SELECT "):
            counts["reads"] += 1
        else:
            raise ValueError(f"Unexpected statement family in {path}: {sql[:40]}")
    assert sum(counts.values()) == row["total_queries"], path
    return counts


def log_evidence(path, checkpoint):
    profiles = [line for line in (path / "server.log").read_text().splitlines()
                if line.startswith("PROFILE_SERVER ")]
    profile = dict(re.findall(r"(\w+)=(\S+)", profiles[-1])) if profiles else {}
    distances = []
    for line in (path / "postgres.log").read_text().splitlines():
        if "checkpoint complete" not in line:
            continue
        values = re.search(r"write=([0-9.]+) s, sync=([0-9.]+) s, total=([0-9.]+) s.*distance=([0-9]+) kB", line)
        if values and all(abs(float(values[i + 1]) * 1000 - checkpoint[k]) < .01
                          for i, k in enumerate(("write_ms", "sync_ms", "total_ms"))):
            distances.append(int(values[4]))
    # Matching durations can be ambiguous (e.g. two zero-duration checkpoints).
    # Distance is checkpoint-to-checkpoint WAL, not a workload LSN measurement.
    return profile, distances


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--published", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    # Refuse overwrites, including analysis artifacts.
    args.out_dir.mkdir(parents=True, exist_ok=False)
    output = []
    settings_seen = collections.defaultdict(set)
    controls = []
    for comparison in csv.DictReader(args.comparison.open()):
        if not comparison["new_artifacts"]:
            continue
        cases = {m: load_case(comparison[m + "_artifacts"])
                 for m in ("det", "old", "new")}
        d, o, n = (cases[m][0] for m in ("det", "old", "new"))
        assert len({x["workload_sha256"] for x in (d, o, n)}) == 1
        path = Path(comparison["new_artifacts"])
        sql = path.parent / "workloads" / f"ycsb_{n['workload']}_skew_{n['skew']}_{n['total_queries']}.sql"
        counts = workload_counts(sql, n)
        changes = counts["updates"] + counts["inserts"]
        row = dict(workload=n["workload"], skew=n["skew"], workers=n["workers"], **counts)
        row.update(updates=counts["updates"], inserts=counts["inserts"], reads=counts["reads"])
        total = n["total_queries"]
        row["mutations"] = changes
        row["mutation_fraction"] = changes / total
        for m, (r, s, io, cp) in cases.items():
            row[m + "_tps"] = r["tps"]
            row[m + "_workload_us_per_stmt"] = r["wall_time_ms"] * 1000 / total
            for metric in ("device_read_mib", "device_write_mib", "checkpoint_write_mib",
                           "blks_read", "blks_hit", "blk_read_time_ms", "blk_write_time_ms",
                           "device_read_await_ms", "buffers_backend"):
                row[m + "_" + metric] = r[metric]
            row[m + "_continuous_write_mib"] = io["continuous_write_mib"]
            row[m + "_checkpoint_total_ms"] = cp["total_ms"]
            row[m + "_artifacts"] = r["artifact_dir"]
            profile, distances = log_evidence(Path(comparison[m + "_artifacts"]), cp)
            row[m + "_checkpoint_distance_kib"] = distances[0] if len(distances) == 1 else None
            row[m + "_checkpoint_distance_matches"] = len(distances)
            for metric in ("exec_calls", "pg_query_ms", "result_format_ms", "retry_attempts_total",
                           "retry_exhausted_total", "ordered_apply_wait_ms", "kafka_send_calls"):
                row[m + "_server_" + metric] = float(profile[metric]) if metric in profile else None
            for k in ("wal_compression", "wal_level", "default_transaction_isolation",
                      "full_page_writes", "fsync", "synchronous_commit", "merkle_apply_synchronous_direct"):
                settings_seen[m + ":" + k].add(s["settings"][k])
        row["new_det"] = n["tps"] / d["tps"]
        row["new_old"] = n["tps"] / o["tps"]
        row["delta_wall_us_per_stmt"] = (n["wall_time_ms"] - d["wall_time_ms"]) * 1000 / total
        row["delta_pg_read_us_per_stmt"] = (n["blk_read_time_ms"] - d["blk_read_time_ms"]) * 1000 / total
        # Mixed-workload normalization, NOT measured latency of individual mutations.
        row["delta_wall_us_per_mutation_equivalent"] = row["delta_wall_us_per_stmt"] * total / changes
        row["delta_pg_read_us_per_mutation_equivalent"] = row["delta_pg_read_us_per_stmt"] * total / changes
        row["delta_checkpoint_distance_kib_per_mutation_equivalent"] = (
            (row["new_checkpoint_distance_kib"] - row["det_checkpoint_distance_kib"]) / changes
            if all(row[m + "_checkpoint_distance_kib"] is not None for m in ("det", "new")) else None)
        for metric in ("device_read_mib", "device_write_mib", "checkpoint_write_mib", "continuous_write_mib"):
            row["delta_" + metric + "_kib_per_mutation_equivalent"] = (row["new_" + metric] - row["det_" + metric]) * 1024 / changes
        for metric in ("blks_read", "blks_hit"):
            row["delta_" + metric + "_per_mutation_equivalent"] = (n[metric] - d[metric]) / changes
        if comparison["control_artifacts"]:
            control = load_case(comparison["control_artifacts"])[0]
            assert control["workload_sha256"] == n["workload_sha256"]
            controls.append(dict(workload=n["workload"], workers=n["workers"],
                                 control_det=control["tps"] / d["tps"],
                                 new_control=n["tps"] / control["tps"]))
        output.append(row)
    with (args.out_dir / "decomposition.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    published = list(csv.DictReader(args.published.open()))
    c_ratios = []
    for r in published:
        if r["workload"] == "c" and r["mode"] == "bcdb_merkle":
            det = next(d for d in published if (d["workload"], d["skew"], d["workers"], d["trial"], d["mode"]) ==
                       ("c", r["skew"], r["workers"], r["trial"], "bcdb_det"))
            c_ratios.append(dict(workers=int(r["workers"]), merkle_det=float(r["tps"]) / float(det["tps"]),
                                 delta_read_mib=float(r["device_read_mib"]) - float(det["device_read_mib"])))
    (args.out_dir / "evidence.json").write_text(json.dumps(dict(
        cases=len(output), settings={k: sorted(v) for k, v in settings_seen.items()},
        controls=controls, archived_read_only=c_ratios,
        limitations=["Mutation-equivalent attribution assumes zero direct Merkle maintenance on SELECT; cache interference remains.",
                     "PG buffer reads are not device reads. No per-relation WAL or CPU timing in these artifacts.",
                     "Checkpoint distance is a WAL proxy across checkpoints, not a workload-only LSN counter; ambiguous matches stay blank.",
                     "At workers > 1 aggregate PG read time overlaps; it cannot be subtracted from wall time.",
                     "Provided 3000-update relation/WAL probe is a separate plain-SQL experiment."]), indent=2) + "\n")
    lines = ["| Workload | Skew | Workers | New/det | Δ wall µs/stmt | Δ PG read µs/stmt (overlaps) | Δ device KiB/mutation | Δ buffers/mutation |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in output:
        lines.append(f"| {r['workload'].upper()} | {r['skew']} | {r['workers']} | {r['new_det']:.3f} | "
                     f"{r['delta_wall_us_per_stmt']:.1f} | {r['delta_pg_read_us_per_stmt']:.1f} | "
                     f"{r['delta_device_read_mib_kib_per_mutation_equivalent']:.2f} | {r['delta_blks_read_per_mutation_equivalent']:.2f} |")
    (args.out_dir / "table.md").write_text("\n".join(lines) + "\n")
    print(f"Analyzed {len(output)} accepted pairs into {args.out_dir}")


if __name__ == "__main__":
    main()
