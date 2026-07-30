#!/usr/bin/env python3
"""Generate updated analysis PNG charts for Dynamic_merkle_docs/plots/."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[3]
DOCS_PLOTS = ROOT / "Dynamic_merkle_docs" / "plots"
DOCS_PLOTS.mkdir(parents=True, exist_ok=True)

STATIC_CSV = (
    ROOT
    / "scripts"
    / "benchmark"
    / "recovery"
    / "fetched"
    / "ariabc-recovery-best-scaling-f32-l1024-k75-c300-20260714T040459Z-0068d0"
    / "phase_timings.csv"
)
DYNAMIC_CSV = (
    ROOT
    / "scripts"
    / "benchmark"
    / "recovery"
    / "fetched"
    / "ariabc-recovery-size-scaling-k75-c300-20260730T210552Z-007541"
    / "phase_timings.csv"
)


def parse_warm_phases(csv_path: Path, is_dynamic: bool = False) -> dict[int, dict[str, float]]:
    with csv_path.open() as f:
        lines = f.readlines()
    header_idx = [i for i, l in enumerate(lines) if "run_id" in l][0]
    reader = csv.DictReader(lines[header_idx:])

    data: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in reader:
        if r.get("method") != "merkle":
            continue
        run_id = r["run_id"]
        if is_dynamic and "-r0" in run_id:
            continue
        n_val = None
        for seg in run_id.split("-"):
            if seg.startswith("n") and seg[1:].isdigit():
                n_val = int(seg[1:])
                break
        if n_val is None:
            continue
        data[n_val][r["phase"]].append(float(r["ms"]))

    res: dict[int, dict[str, float]] = {}
    for n in sorted(data.keys()):
        res[n] = {p: median(vals) for p, vals in data[n].items()}
    return res


def main() -> None:
    static_data = parse_warm_phases(STATIC_CSV, is_dynamic=False)
    dynamic_data = parse_warm_phases(DYNAMIC_CSV, is_dynamic=True)

    scale_points = sorted(dynamic_data.keys())
    x_labels = [f"{n // 1_000_000}M" for n in scale_points]
    x = list(range(len(scale_points)))

    plt.rcParams.update({"font.size": 11, "figure.titlesize": 14, "axes.titlesize": 13})

    # 1. Repair Write Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    static_rep = [static_data[n].get("repair_write_ms", 0) for n in scale_points]
    dynamic_rep = [dynamic_data[n].get("repair_write_ms", 0) for n in scale_points]

    idx_20m = scale_points.index(20_000_000)
    static_20m_val = static_rep[idx_20m]

    static_rep_plot = list(static_rep)
    static_rep_plot[idx_20m] = np.nan

    plt.plot(x, static_rep_plot, label="Static Repair Write", color="#d62728", marker="o", linewidth=2.5)
    plt.plot(
        x,
        dynamic_rep,
        label="Dynamic Repair Write (DML + Merkle Apply)",
        color="#1f77b4",
        marker="s",
        linewidth=2.5,
    )

    plt.annotate(
        f"Static 20M Spike:\n{static_20m_val:,.1f} ms",
        xy=(x[idx_20m], 950),
        xytext=(x[idx_20m], 870),
        arrowprops=dict(facecolor="#d62728", edgecolor="#d62728", arrowstyle="->", lw=1.5),
        ha="center",
        va="top",
        fontsize=9.5,
        fontweight="bold",
        color="#d62728",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#fff0f0", edgecolor="#d62728", lw=1),
    )

    plt.ylim(450, 980)

    plt.title("Repair Write Latency: Static vs Dynamic (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Repair Write Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="lower right")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "repair_write_comparison.png")
    plt.close()

    # 2. Post-Repair Confirmation Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    static_conf = [
        static_data[n].get("targeted_post_repair_confirmation_ms", 0) for n in scale_points
    ]
    dynamic_conf = [
        dynamic_data[n].get("targeted_post_repair_confirmation_ms", 0) for n in scale_points
    ]

    plt.plot(x, static_conf, label="Static Confirmation Barrier", color="#d62728", marker="o", linewidth=2.5)
    plt.plot(
        x,
        dynamic_conf,
        label="Dynamic Post-Repair Confirmation Barrier",
        color="#2ca02c",
        marker="^",
        linewidth=2.5,
    )

    plt.title("Targeted Post-Repair Confirmation Latency (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Confirmation Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9)
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "post_repair_confirmation_comparison.png")
    plt.close()

    # 3. Phase Stacked Composition Comparison
    plt.figure(figsize=(12, 6), dpi=300)

    loc = np.array([dynamic_data[n].get("tree_localisation_ms", 0) for n in scale_points])
    fet = np.array([dynamic_data[n].get("candidate_row_fetch_ms", 0) for n in scale_points])
    cmp = np.array([dynamic_data[n].get("row_comparison_ms", 0) for n in scale_points])
    rep = np.array([dynamic_data[n].get("repair_write_ms", 0) for n in scale_points])
    cnf = np.array(
        [dynamic_data[n].get("targeted_post_repair_confirmation_ms", 0) for n in scale_points]
    )
    orc = np.array([dynamic_data[n].get("recovery_orchestration_ms", 0) for n in scale_points])

    width = 0.55
    plt.bar(x, loc, width, label="Tree Localisation", color="#1f77b4")
    plt.bar(x, fet, width, bottom=loc, label="Candidate Row Fetch", color="#ff7f0e")
    plt.bar(x, cmp, width, bottom=loc + fet, label="Row Comparison", color="#2ca02c")
    plt.bar(x, rep, width, bottom=loc + fet + cmp, label="Repair Write (DML + Merkle Apply)", color="#d62728")
    plt.bar(
        x,
        cnf,
        width,
        bottom=loc + fet + cmp + rep,
        label="Post-Repair Confirmation",
        color="#9467bd",
    )
    plt.bar(
        x,
        orc,
        width,
        bottom=loc + fet + cmp + rep + cnf,
        label="Orchestration / Other",
        color="#8c564b",
    )

    plt.title("Optimized Dynamic Recovery Phase Timing Composition (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.4, axis="y")
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "phase_stacked_composition.png")
    plt.close()

    print("Successfully updated affected analysis PNG charts in Dynamic_merkle_docs/plots/")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
