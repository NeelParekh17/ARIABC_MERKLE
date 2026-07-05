#!/usr/bin/env python3
"""Aggregate a threaded Raft-direct sweep and draw TPS versus worker count.

Usage:
  python3 scripts/distributed/plot_threaded_raft_direct_sweep.py \
      --input scripts/bench_full_results/thread_sweep_*/raw_runs.csv \
      --out-dir scripts/bench_full_results/thread_sweep_*/
"""

from __future__ import annotations

import argparse
import csv
import math
import pathlib
import statistics
import sys
from collections import defaultdict

import matplotlib.pyplot as plt


def as_float(row: dict[str, str], key: str) -> float:
    try:
        return float(row.get(key, ""))
    except (TypeError, ValueError):
        return math.nan


def as_int(row: dict[str, str], key: str) -> int:
    try:
        return int(float(row.get(key, "")))
    except (TypeError, ValueError):
        return -1


def load_valid_rows(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    valid: list[dict[str, str]] = []
    rejected: list[str] = []
    for row in rows:
        threads = as_int(row, "threads")
        tps = as_float(row, "tps")
        ok = (
            threads > 0
            and math.isfinite(tps)
            and tps > 0
            and as_int(row, "merkle_pass") == 1
            and as_int(row, "divergence_count") == 0
            and as_int(row, "permanent_failures") == 0
        )
        if ok:
            valid.append(row)
        else:
            rejected.append(
                f"threads={row.get('threads')} rep={row.get('rep')} "
                f"tps={row.get('tps')} merkle={row.get('merkle_pass')} "
                f"divergence={row.get('divergence_count')} "
                f"failures={row.get('permanent_failures')}"
            )

    if rejected:
        raise SystemExit(
            "Refusing to graph invalid benchmark runs:\n  - " + "\n  - ".join(rejected)
        )
    if not valid:
        raise SystemExit("No valid rows found in the input CSV.")
    return valid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=pathlib.Path)
    parser.add_argument("--out-dir", required=True, type=pathlib.Path)
    parser.add_argument(
        "--title",
        default="Threaded Raft-Direct: matched worker sweep",
    )
    args = parser.parse_args()

    rows = load_valid_rows(args.input)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    grouped: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[as_int(row, "threads")].append(row)

    summary_path = args.out_dir / "thread_sweep_summary.csv"
    summary_rows: list[dict[str, object]] = []

    for threads in sorted(grouped):
        group = grouped[threads]
        tps_values = sorted(as_float(row, "tps") for row in group)
        median_tps = statistics.median(tps_values)
        representative = max(group, key=lambda row: as_float(row, "tps"))

        summary_rows.append(
            {
                "threads": threads,
                "repetitions": len(group),
                "tps_min": f"{min(tps_values):.2f}",
                "tps_median": f"{median_tps:.2f}",
                "tps_max": f"{max(tps_values):.2f}",
                "orderer_policy": representative.get("orderer_policy", ""),
                "bcdb_init_arg_size": representative.get("bcdb_init_arg_size", ""),
                "target_entries": representative.get("target_entries", ""),
                "linger_us": representative.get("linger_us", ""),
                "det_window": representative.get("det_window", ""),
                "entries_per_fsync_best": representative.get("entries_per_fsync", ""),
                "append_entries_avg_best": representative.get("append_entries_avg", ""),
                "fsync_p95_best_ms": representative.get("fsync_p95", ""),
                "max_pqexec_best": representative.get("max_pqexec", ""),
                "artifact_best": representative.get("artifact_dir", ""),
            }
        )

    fields = list(summary_rows[0].keys())
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary_rows)

    xs = [int(row["threads"]) for row in summary_rows]
    ys = [float(row["tps_median"]) for row in summary_rows]
    low = [y - float(row["tps_min"]) for y, row in zip(ys, summary_rows)]
    high = [float(row["tps_max"]) - y for y, row in zip(ys, summary_rows)]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.errorbar(xs, ys, yerr=[low, high], marker="o", capsize=4)
    ax.set_title(args.title)
    ax.set_xlabel("Matched workers: gateway = executor = PG connections = BCDB")
    ax.set_ylabel("Completed TPS")
    ax.set_xticks(xs)
    ax.grid(True, alpha=0.3)

    for x, y in zip(xs, ys):
        ax.annotate(f"{y:.0f}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center")

    fig.tight_layout()
    graph_path = args.out_dir / "thread_sweep_tps.png"
    fig.savefig(graph_path, dpi=180)
    plt.close(fig)

    print(summary_path)
    print(graph_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
