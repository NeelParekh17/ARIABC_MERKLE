#!/usr/bin/env python3
"""Validate and plot archived OOM optimization data; never execute workloads."""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


REPO = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = REPO / "Final_Results/OOM_100M/optimizations_20261001"
POINTS = [("a", 0.0, 1), ("a", 0.0, 16), ("f", 0.99, 1), ("f", 0.99, 16)]
MODES = ("bcdb_det", "bcdb_merkle")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_text())


def read_run(group, experiment, treatment, fillfactor, wal_compression):
    runs = sorted(group.glob("run_*"))
    require(len(runs) == 1, f"Expected one run in {group}")
    run = runs[0]
    campaign = read_json(run / "campaign.json")
    args = campaign["arguments"]
    require(args["remote_host"] == "10.129.148.247", f"Unexpected host: {run}")
    require(args["trials"] == 1 and args["txs"] == 20000, f"Dimensions: {run}")
    require(not args["dry_run"] and not args["preflight_only"], f"Unmeasured: {run}")
    require(args["reset_mode"] == "delta", f"Reset policy: {run}")
    require(args["oom_opt_wal_compression"] == wal_compression, f"Treatment: {run}")
    require(campaign["cache_policy"] == "cold_start_os_cache_unbounded", str(run))
    recorded = list(csv.DictReader((run / "summary.csv").open()))
    cases = sorted(run.glob("*/result.json"))
    require(len(cases) == len(recorded) == 4 * len(args["modes"]), str(run))
    output = []
    for result_path in cases:
        case = result_path.parent
        result = read_json(result_path)
        row = result["row"]
        setup = read_json(case / "setup.json")
        io = read_json(case / "io.json")
        settings = setup["settings"]
        require(not (case / "FAILED.txt").exists(), str(case))
        require(not (case / "cleanup_errors.txt").exists(), str(case))
        require(row["total_queries"] == row["validated_completed_queries"] == 20000, str(case))
        require(row["divergence_count"] == row["permanent_failures"] == 0, str(case))
        require(row["retry_exhausted"] == 0, str(case))
        require(result["gateway"]["gateway_returncode"] == 0, str(case))
        require(result["gateway"]["validated_row_results"] == 20000, str(case))
        require(row["isolation"] == "serializable", str(case))
        for key in ("transaction_isolation", "default_transaction_isolation"):
            require(settings[key] == "serializable", f"{key}: {case}")
        for key in ("fsync", "full_page_writes", "synchronous_commit", "merkle_apply_synchronous_direct"):
            require(settings[key] == "on", f"{key}: {case}")
        require(settings["wal_compression"] == wal_compression, str(case))
        require(int(settings["shared_buffers"]) * int(settings["block_size"]) == 32 * 2**20, str(case))
        require("CACHES_DROPPED" in setup["cache_drop_output"], str(case))
        require(row["reset_mode"] == "delta", str(case))
        stats = setup.get("merkle_stats") or {}
        if row["mode"] == "bcdb_merkle":
            require(row["merkle_verify"] == "PASS", str(case))
            require((case / "merkle_verify.txt").read_text().strip() == "t", str(case))
            require((stats["fanout"], stats["split_threshold"], stats["merge_threshold"]) ==
                    (32, 1024, 256), str(case))
        n = row["total_queries"]
        require(math.isclose(row["tps"], n * 1000 / row["wall_time_ms"]), str(case))
        for metric, counter in (("device_read_mib", "read_sectors"),
                                ("device_write_mib", "write_sectors")):
            measured = (io["device_after"][counter] - io["device_before"][counter]) / 2048
            require(math.isclose(row[metric], measured, abs_tol=1e-9), f"{metric}: {case}")
        for metric in ("blks_read", "blks_hit"):
            require(row[metric] == io["pg_after"][metric] - io["pg_before"][metric], str(case))
        saved = next(r for r in recorded if (r["workload"], float(r["skew"]), r["mode"],
                                             int(r["workers"])) ==
                     (row["workload"], row["skew"], row["mode"], row["workers"]))
        for metric in ("tps", "device_read_mib", "device_write_mib"):
            require(float(saved[metric]) == row[metric], f"Summary mismatch: {case}")
        sql = run / "workloads" / f"ycsb_{row['workload']}_skew_{row['skew']}_{n}.sql"
        sql_bytes = sql.read_bytes()
        digest = hashlib.sha256(sql_bytes).hexdigest()
        require(digest == row["workload_sha256"], f"Workload mismatch: {case}")
        statements = [line.strip().upper() for line in sql_bytes.decode().splitlines() if line.strip()]
        require(len(statements) == n, str(sql))
        updates = sum(line.startswith(("UPDATE ", "WITH YCSB_READ ")) for line in statements)
        provenance = result["provenance"]
        require(provenance["executables"] == setup["provenance"]["executables"], str(case))
        hashes = {Path(e["path"]).name: e["sha256"] for e in provenance["executables"]}
        # These campaigns contain database-wide buffer stats, not per-table HOT
        # before/after snapshots. Do not substitute mutation counts for HOT stats.
        output.append(dict(
            experiment=experiment, treatment=treatment, workload=row["workload"],
            skew=row["skew"], workers=row["workers"], mode=row["mode"], trial=row["trial"],
            fillfactor=fillfactor, wal_compression=wal_compression, total_queries=n,
            update_statements=updates, tps=row["tps"], wall_time_ms=row["wall_time_ms"],
            device_read_mib=row["device_read_mib"], device_write_mib=row["device_write_mib"],
            read_kb_per_statement=row["device_read_mib"] * 2**20 / (1000 * n),
            write_kb_per_statement=row["device_write_mib"] * 2**20 / (1000 * n),
            checkpoint_write_mib=row["checkpoint_write_mib"], blks_read=row["blks_read"],
            blks_hit=row["blks_hit"], blk_read_time_ms=row["blk_read_time_ms"],
            hot_fraction="", hot_measurement="not measured: no per-user-table HOT snapshots",
            isolation=row["isolation"], retry_attempts=row["retry_attempts"],
            retry_exhausted=row["retry_exhausted"], divergence_count=row["divergence_count"],
            permanent_failures=row["permanent_failures"],
            validated_completed_queries=row["validated_completed_queries"],
            merkle_verify=row["merkle_verify"], fanout=stats.get("fanout", ""),
            split_threshold=stats.get("split_threshold", ""),
            merge_threshold=stats.get("merge_threshold", ""),
            heap_bytes=setup["sizes"]["heap_bytes"],
            install_dir=provenance["install_dir"], baseline_dir=provenance["baseline_dir"],
            postgres_sha256=hashes["postgres"], server_sha256=hashes["ariabc_pg_server"],
            gateway_sha256=hashes["ariabc_pg_gateway"], workload_sha256=digest,
            artifact_dir=str(case.relative_to(REPO)), original_artifact_dir=row["artifact_dir"]))
    return output


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def compare(rows):
    output = []
    for experiment, before, after in (("wal_compression", "off", "on"),
                                      ("fillfactor90", "ff100", "ff90")):
        data = [r for r in rows if r["experiment"] == experiment]
        for workload, skew, workers in POINTS:
            def select(treatment, mode):
                matches = [r for r in data if (r["treatment"], r["mode"], r["workload"],
                                               r["skew"], r["workers"]) ==
                           (treatment, mode, workload, skew, workers)]
                require(len(matches) == 1, f"Missing/duplicate {experiment} point")
                return matches[0]
            ratios = {t: select(t, "bcdb_merkle")["tps"] / select(t, "bcdb_det")["tps"]
                      for t in (before, after)}
            for mode in MODES:
                a, b = select(before, mode), select(after, mode)
                require(a["workload_sha256"] == b["workload_sha256"], experiment)
                output.append(dict(experiment=experiment, workload=workload, skew=skew,
                                   workers=workers, mode=mode, baseline=before, treatment=after,
                                   baseline_tps=a["tps"], treatment_tps=b["tps"],
                                   tps_change_pct=100 * (b["tps"] / a["tps"] - 1),
                                   baseline_read_kb=a["read_kb_per_statement"],
                                   treatment_read_kb=b["read_kb_per_statement"],
                                   baseline_write_kb=a["write_kb_per_statement"],
                                   treatment_write_kb=b["write_kb_per_statement"],
                                   write_change_pct=100 * (b["write_kb_per_statement"] /
                                                           a["write_kb_per_statement"] - 1),
                                   baseline_merkle_det=ratios[before], treatment_merkle_det=ratios[after],
                                   baseline_artifacts=a["artifact_dir"], treatment_artifacts=b["artifact_dir"]))
    return output


def write_tables(root, pairs):
    for experiment in ("wal_compression", "fillfactor90"):
        lines = ["| Point | Mode | Baseline TPS | Treatment TPS | TPS change | Read KB/stmt before → after | Write KB/stmt before → after | Write change | Merkle/det before → after |",
                 "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in pairs:
            if r["experiment"] != experiment:
                continue
            mode = "det" if r["mode"] == "bcdb_det" else "Merkle"
            ratio = (f"{r['baseline_merkle_det']:.3f} → {r['treatment_merkle_det']:.3f}"
                     if mode == "Merkle" else "—")
            lines.append(f"| {r['workload'].upper()} θ{r['skew']} w{r['workers']} | {mode} | "
                         f"{r['baseline_tps']:,.3f} | {r['treatment_tps']:,.3f} | {r['tps_change_pct']:+.2f}% | "
                         f"{r['baseline_read_kb']:.3f} → {r['treatment_read_kb']:.3f} | "
                         f"{r['baseline_write_kb']:.3f} → {r['treatment_write_kb']:.3f} | "
                         f"{r['write_change_pct']:+.2f}% | {ratio} |")
        (root / experiment / "table.md").write_text("\n".join(lines) + "\n")


def plot(root, rows):
    fig, axes = plt.subplots(2, 4, figsize=(19, 9))
    colors = ("#355c9a", "#91b2df", "#bd5d2f", "#efb085")
    labels = ["A θ0\nw1", "A θ0\nw16", "F θ0.99\nw1", "F θ0.99\nw16"]
    x = np.arange(len(POINTS))
    for i, (experiment, treatments, title) in enumerate([
            ("wal_compression", ("off", "on"), "WAL compression: off → on"),
            ("fillfactor90", ("ff100", "ff90"), "Heap fillfactor: 100 → 90 (WAL off)")]):
        data = {(r["treatment"], r["mode"], r["workload"], r["skew"], r["workers"]): r
                for r in rows if r["experiment"] == experiment}
        for j, (metric, ylabel) in enumerate([
                ("tps", "Statements/s"), ("ratio", "Merkle / det TPS"),
                ("read_kb_per_statement", "Device read KB / statement"),
                ("write_kb_per_statement", "Device write KB / statement")]):
            ax = axes[i, j]
            if metric == "ratio":
                for k, treatment in enumerate(treatments):
                    values = [data[(treatment, "bcdb_merkle", *p)]["tps"] /
                              data[(treatment, "bcdb_det", *p)]["tps"] for p in POINTS]
                    ax.bar(x + (k - .5) * .34, values, .34, label=treatment, color=colors[k * 2])
                ax.axhline(1, color="0.4", lw=1, ls="--")
                ax.set_ylim(0, 1.1)
            else:
                for k, (mode, treatment) in enumerate((m, t) for m in MODES for t in treatments):
                    values = [data[(treatment, mode, *p)][metric] for p in POINTS]
                    label = ("det" if mode == "bcdb_det" else "Merkle") + " " + treatment
                    ax.bar(x + (k - 1.5) * .19, values, .19, label=label, color=colors[k])
                ax.set_ylim(bottom=0)
            ax.set_xticks(x, labels)
            ax.set_ylabel(ylabel)
            ax.set_title(title if j == 0 else ["", "Throughput ratio", "Timed reads", "Timed writes"][j])
            ax.grid(axis="y", alpha=.2)
            ax.set_axisbelow(True)
            ax.legend(fontsize=8, ncol=2)
    fig.suptitle("OOM 100M optimization experiments · 2026-10-01 · SERIALIZABLE · synchronous Merkle S1024", fontsize=16)
    fig.text(.5, .015, "Single cold trial per point; shared-host variation. KB = 1000 bytes. Device workload window excludes the separate checkpoint.\n"
             "Fillfactor-100 controls reuse WAL-off observations. HOT fraction unmeasured; fillfactor-90 Merkle baseline contains old node dead tuples.",
             ha="center", fontsize=10)
    fig.tight_layout(rect=(0, .065, 1, .95))
    fig.savefig(root / "optimization_experiments.png", dpi=180)
    fig.savefig(root / "optimization_experiments.pdf")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    root = args.root.resolve()
    rows = []
    for group in ("det_off", "det_on", "merkle_off", "merkle_on"):
        treatment = group.rsplit("_", 1)[1]
        rows.extend(read_run(root / "wal_compression" / group, "wal_compression", treatment, 100, treatment))
    for group in ("det_off", "merkle_off"):
        rows.extend(read_run(root / "fillfactor90/controls_ff100" / group, "fillfactor90", "ff100", 100, "off"))
    rows.extend(read_run(root / "fillfactor90/ff90_off", "fillfactor90", "ff90", 90, "off"))
    require(len(rows) == 32, "Expected 32 rows representing 24 unique measured cases")
    require(len({(r["postgres_sha256"], r["server_sha256"], r["gateway_sha256"]) for r in rows}) == 1,
            "Executable identity differs")
    require(len({r["install_dir"] for r in rows}) == 1, "Install differs")
    for workload, skew, _ in POINTS:
        require(len({r["workload_sha256"] for r in rows if r["workload"] == workload and r["skew"] == skew}) == 1,
                "Workload identity differs")
    rows.sort(key=lambda r: (r["experiment"], r["workload"], r["workers"], r["mode"], r["treatment"]))
    pairs = compare(rows)
    write_csv(root / "summary.csv", rows)
    write_csv(root / "comparison.csv", pairs)
    write_tables(root, pairs)
    plot(root, rows)
    print(f"Validated 24 unique cases; wrote 32 summary rows, 16 paired comparisons and figures to {root}")


if __name__ == "__main__":
    main()
