#!/usr/bin/env python3
"""
plot_dataset_creation.py
Generate comparative visualization plots from dataset creation benchmark results.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def load_data(csv_path: Path):
    """Load benchmark metrics from CSV file."""
    rows = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "scale": int(r["scale"]),
                "scale_label": r["scale_label"],
                "repetitions": int(r["repetitions"]),
                "heap_ms": float(r["heap_ms_mean"]),
                "pkey_btree_ms": float(r["pkey_btree_ms_mean"]),
                "merkle_am_ms": float(r["merkle_am_ms_mean"]),
                "lookup_btree_ms": float(r["lookup_btree_ms_mean"]),
                "analyze_ms": float(r["analyze_ms_mean"]),
                "total_ms": float(r["total_ms_mean"]),
                "total_s": float(r["total_s_mean"]),
                "tps": float(r["tps_mean"]),
                "heap_mb": float(r["heap_mb"]),
                "pkey_mb": float(r["pkey_mb"]),
                "merkle_mb": float(r["merkle_mb"]),
                "lookup_mb": float(r["lookup_mb"]),
                "total_mb": float(r["total_mb"]),
                "merkle_tree_height": int(r["merkle_tree_height"]),
            })
    return rows


def plot_component_breakdown(rows, output_dir: Path):
    """Plot stacked bar chart of component creation times across scales."""
    labels = [r["scale_label"] for r in rows]
    heap = np.array([r["heap_ms"] / 1000.0 for r in rows])
    pkey = np.array([r["pkey_btree_ms"] / 1000.0 for r in rows])
    merkle = np.array([r["merkle_am_ms"] / 1000.0 for r in rows])
    lookup = np.array([r["lookup_btree_ms"] / 1000.0 for r in rows])
    analyze = np.array([r["analyze_ms"] / 1000.0 for r in rows])

    x = np.arange(len(labels))
    width = 0.55

    fig, ax = plt.subplots(figsize=(15, 7), dpi=180)

    p1 = ax.bar(x, heap, width, label="Heap Populate (generate_series)", color="#2b5c8f")
    p2 = ax.bar(x, pkey, width, bottom=heap, label="PK B-Tree (usertable_pkey)", color="#3a9278")
    p3 = ax.bar(x, merkle, width, bottom=heap + pkey, label="Merkle AM Index (usertable_merkle_idx)", color="#e27c3e")
    p4 = ax.bar(x, lookup, width, bottom=heap + pkey + merkle, label="Lookup B-Tree (partition_lookup_idx)", color="#8b5bb5")
    p5 = ax.bar(x, analyze, width, bottom=heap + pkey + merkle + lookup, label="ANALYZE & Catalog Stats", color="#a3a3a3")

    ax.set_ylabel("Dataset Creation Time (Seconds)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Dataset Scale (Tuples)", fontsize=12, fontweight="bold")
    ax.set_title("Dataset Creation Time Breakdown Across Scales (All Indexes Included)", fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10.5, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper left", frameon=True, facecolor="white", framealpha=0.95, fontsize=10)

    # Add total time text above each bar
    for i in range(len(rows)):
        tot = rows[i]["total_s"]
        ax.text(x[i], tot + max(tot * 0.02, 0.01), f"{tot:.1f}s\n({rows[i]['tps']:,.0f} tps)", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    plt.tight_layout()
    out_file = output_dir / "dataset_creation_component_breakdown.png"
    plt.savefig(out_file)
    plt.close()
    print(f"Generated plot: {out_file}")


def plot_scaling_and_throughput(rows, output_dir: Path):
    """Plot dual-axis line chart for total build time and throughput."""
    labels = [r["scale_label"] for r in rows]
    scales = [r["scale"] for r in rows]
    times_s = [r["total_s"] for r in rows]
    tps = [r["tps"] for r in rows]

    x = np.arange(len(labels))

    fig, ax1 = plt.subplots(figsize=(15, 6.5), dpi=180)

    color1 = "#1f77b4"
    ax1.set_xlabel("Dataset Scale (Tuples)", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Total Creation Time (Seconds)", color=color1, fontsize=12, fontweight="bold")
    line1 = ax1.plot(x, times_s, color=color1, marker="o", linewidth=2.5, markersize=8, label="Creation Time (s)")
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=10.5, fontweight="bold")
    ax1.grid(True, linestyle="--", alpha=0.4)

    for i, txt in enumerate(times_s):
        ax1.annotate(f"{txt:.1f}s", (x[i], times_s[i]), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8, fontweight="bold", color=color1)

    ax2 = ax1.twinx()
    color2 = "#2ca02c"
    ax2.set_ylabel("Creation Throughput (Tuples / Sec)", color=color2, fontsize=12, fontweight="bold")
    line2 = ax2.plot(x, tps, color=color2, marker="s", linewidth=2.5, linestyle="--", markersize=8, label="Throughput (tup/s)")
    ax2.tick_params(axis="y", labelcolor=color2)

    for i, txt in enumerate(tps):
        ax2.annotate(f"{txt:,.0f} tps", (x[i], tps[i]), textcoords="offset points", xytext=(0, -15), ha="center", fontsize=8, fontweight="bold", color=color2)

    lines = line1 + line2
    labels_leg = [l.get_label() for l in lines]
    ax1.legend(lines, labels_leg, loc="center left", frameon=True, facecolor="white", framealpha=0.9, fontsize=10)

    plt.title("AriaBC Dataset Creation Scaling & Ingestion Throughput", fontsize=14, fontweight="bold", pad=15)
    plt.tight_layout()
    out_file = output_dir / "dataset_creation_scaling_and_throughput.png"
    plt.savefig(out_file)
    plt.close()
    print(f"Generated plot: {out_file}")


def plot_storage_footprint(rows, output_dir: Path):
    """Plot storage footprint (MB) across scales."""
    labels = [r["scale_label"] for r in rows]
    heap_mb = [r["heap_mb"] for r in rows]
    pkey_mb = [r["pkey_mb"] for r in rows]
    merkle_mb = [r["merkle_mb"] for r in rows]
    lookup_mb = [r["lookup_mb"] for r in rows]

    x = np.arange(len(labels))
    width = 0.18

    fig, ax = plt.subplots(figsize=(15, 7), dpi=180)

    ax.bar(x - 1.5 * width, heap_mb, width, label="Heap Table", color="#2b5c8f")
    ax.bar(x - 0.5 * width, pkey_mb, width, label="PK B-Tree", color="#3a9278")
    ax.bar(x + 0.5 * width, merkle_mb, width, label="Merkle AM Index", color="#e27c3e")
    ax.bar(x + 1.5 * width, lookup_mb, width, label="Lookup B-Tree", color="#8b5bb5")

    ax.set_ylabel("Storage Size on Disk (MB)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Dataset Scale (Tuples)", fontsize=12, fontweight="bold")
    ax.set_title("Storage Footprint by Component Across Scales", fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10.5, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper left", frameon=True, facecolor="white", framealpha=0.95, fontsize=10)

    for i in range(len(rows)):
        tot = rows[i]["total_mb"]
        max_val = max(heap_mb[i], pkey_mb[i], merkle_mb[i], lookup_mb[i])
        ax.text(x[i], max_val + max(tot * 0.03, 0.5), f"{tot:,.0f} MB", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    plt.tight_layout()
    out_file = output_dir / "dataset_creation_storage_footprint.png"
    plt.savefig(out_file)
    plt.close()
    print(f"Generated plot: {out_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate plots for dataset creation benchmark.")
    parser.add_argument("--csv", default="results/dataset_creation_results.csv", help="Path to dataset_creation_results.csv")
    parser.add_argument("--output-dir", default="results", help="Directory to save generated plots")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_data(csv_path)
    if not rows:
        print("No data rows found in CSV.")
        return

    plot_component_breakdown(rows, output_dir)
    plot_scaling_and_throughput(rows, output_dir)
    plot_storage_footprint(rows, output_dir)
    print("All benchmark visualization plots generated successfully!")


if __name__ == "__main__":
    main()
