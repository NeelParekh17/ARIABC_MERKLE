#!/usr/bin/env python3
"""
plot_dataset_creation.py
Generate publication-quality visualization plots from dataset creation benchmark results.
Handles multi-scale data (1k to 50M) with dual-panel views and precise unit formatting.
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


def format_time_label(seconds: float) -> str:
    """Format seconds into clean human-readable text (ms, s, or m s)."""
    if seconds < 1.0:
        return f"{seconds * 1000.0:.1f} ms"
    elif seconds < 60.0:
        return f"{seconds:.2f} s"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.1f}s"


def format_size_label(mb: float) -> str:
    """Format megabytes into clean human-readable text (KB, MB, GB)."""
    if mb < 1.0:
        return f"{mb * 1024.0:.0f} KB"
    elif mb < 1000.0:
        return f"{mb:.1f} MB"
    else:
        return f"{mb / 1024.0:.2f} GB"


def format_throughput_label(tps: float) -> str:
    """Format throughput into clean text (e.g. 265.4k tps)."""
    if tps >= 1000.0:
        return f"{tps / 1000.0:.1f}k tps"
    return f"{tps:.0f} tps"


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
                "merkle_total_nodes": int(r["merkle_total_nodes"]),
                "merkle_leaf_nodes": int(r["merkle_leaf_nodes"]),
                "merkle_tree_height": int(r["merkle_tree_height"]),
            })
    return rows


def plot_component_breakdown_dual(rows, output_dir: Path):
    """
    Plot dual-panel stacked bar chart:
    - Left panel: Small scales (1k to 1M)
    - Right panel: Large production scales (3M to 50M)
    This prevents small scales from being flattened by 5 orders of magnitude.
    """
    small_rows = [r for r in rows if r["scale"] <= 1_000_000]
    large_rows = [r for r in rows if r["scale"] >= 1_000_000]

    colors = {
        "heap": "#2b5c8f",
        "pkey": "#3a9278",
        "merkle": "#e27c3e",
        "lookup": "#8b5bb5",
        "analyze": "#888888",
    }

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), dpi=200, gridspec_kw={"width_ratios": [1, 1.8]})

    # --- Left Panel: 1k to 1M ---
    labels1 = [r["scale_label"] for r in small_rows]
    heap1 = np.array([r["heap_ms"] / 1000.0 for r in small_rows])
    pkey1 = np.array([r["pkey_btree_ms"] / 1000.0 for r in small_rows])
    merkle1 = np.array([r["merkle_am_ms"] / 1000.0 for r in small_rows])
    lookup1 = np.array([r["lookup_btree_ms"] / 1000.0 for r in small_rows])
    analyze1 = np.array([r["analyze_ms"] / 1000.0 for r in small_rows])
    x1 = np.arange(len(labels1))
    w1 = 0.5

    ax1.bar(x1, heap1, w1, label="Heap Populate (generate_series)", color=colors["heap"])
    ax1.bar(x1, pkey1, w1, bottom=heap1, label="PK B-Tree (usertable_pkey)", color=colors["pkey"])
    ax1.bar(x1, merkle1, w1, bottom=heap1 + pkey1, label="Merkle AM Index (usertable_merkle_idx)", color=colors["merkle"])
    ax1.bar(x1, lookup1, w1, bottom=heap1 + pkey1 + merkle1, label="Lookup B-Tree (partition_lookup_idx)", color=colors["lookup"])
    ax1.bar(x1, analyze1, w1, bottom=heap1 + pkey1 + merkle1 + lookup1, label="ANALYZE & Catalog Stats", color=colors["analyze"])

    ax1.set_title("Small & Medium Scales (1k → 1M)", fontsize=13, fontweight="bold", pad=10)
    ax1.set_ylabel("Dataset Creation Time (Seconds)", fontsize=11, fontweight="bold")
    ax1.set_xlabel("Scale (Tuples)", fontsize=11, fontweight="bold")
    ax1.set_xticks(x1)
    ax1.set_xticklabels(labels1, fontsize=10.5, fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.5)
    ax1.set_ylim(0, max(small_rows[-1]["total_s"] * 1.25, 1.0))

    for i in range(len(small_rows)):
        tot = small_rows[i]["total_s"]
        t_label = format_time_label(tot)
        tps_label = format_throughput_label(small_rows[i]["tps"])
        ax1.text(x1[i], tot + (small_rows[-1]["total_s"] * 0.03), f"{t_label}\n({tps_label})", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    # --- Right Panel: 1M to 50M ---
    labels2 = [r["scale_label"] for r in large_rows]
    heap2 = np.array([r["heap_ms"] / 1000.0 for r in large_rows])
    pkey2 = np.array([r["pkey_btree_ms"] / 1000.0 for r in large_rows])
    merkle2 = np.array([r["merkle_am_ms"] / 1000.0 for r in large_rows])
    lookup2 = np.array([r["lookup_btree_ms"] / 1000.0 for r in large_rows])
    analyze2 = np.array([r["analyze_ms"] / 1000.0 for r in large_rows])
    x2 = np.arange(len(labels2))
    w2 = 0.55

    ax2.bar(x2, heap2, w2, color=colors["heap"])
    ax2.bar(x2, pkey2, w2, bottom=heap2, color=colors["pkey"])
    ax2.bar(x2, merkle2, w2, bottom=heap2 + pkey2, color=colors["merkle"])
    ax2.bar(x2, lookup2, w2, bottom=heap2 + pkey2 + merkle2, color=colors["lookup"])
    ax2.bar(x2, analyze2, w2, bottom=heap2 + pkey2 + merkle2 + lookup2, color=colors["analyze"])

    ax2.set_title("Production Multi-Million Scales (1M → 50M)", fontsize=13, fontweight="bold", pad=10)
    ax2.set_ylabel("Dataset Creation Time (Seconds)", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Scale (Tuples)", fontsize=11, fontweight="bold")
    ax2.set_xticks(x2)
    ax2.set_xticklabels(labels2, fontsize=10.5, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.5)
    ax2.set_ylim(0, large_rows[-1]["total_s"] * 1.18)

    for i in range(len(large_rows)):
        tot = large_rows[i]["total_s"]
        t_label = format_time_label(tot)
        tps_label = format_throughput_label(large_rows[i]["tps"])
        ax2.text(x2[i], tot + (large_rows[-1]["total_s"] * 0.025), f"{t_label}\n({tps_label})", ha="center", va="bottom", fontsize=8, fontweight="bold")

    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.99), ncol=5, frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=10)

    plt.suptitle("AriaBC Dataset Creation & Indexing Time Breakdown Across Scales (1k → 50M)", fontsize=15, fontweight="bold", y=1.04)
    plt.tight_layout()
    out_file = output_dir / "dataset_creation_component_breakdown.png"
    plt.savefig(out_file, bbox_inches="tight")
    plt.close()
    print(f"Generated plot: {out_file}")


def plot_scaling_and_throughput(rows, output_dir: Path):
    """Plot dual-axis line chart for total build time and throughput across all 13 scales."""
    labels = [r["scale_label"] for r in rows]
    times_s = [r["total_s"] for r in rows]
    tps = [r["tps"] for r in rows]

    x = np.arange(len(labels))

    fig, ax1 = plt.subplots(figsize=(16, 7), dpi=200)

    color1 = "#1f77b4"
    ax1.set_xlabel("Dataset Scale (Tuples)", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Total Creation Time (Seconds)", color=color1, fontsize=12, fontweight="bold")
    line1 = ax1.plot(x, times_s, color=color1, marker="o", linewidth=2.8, markersize=8, label="Total Creation Time (s)")
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=10.5, fontweight="bold")
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.set_ylim(0, max(times_s) * 1.15)

    for i, t in enumerate(times_s):
        lbl = format_time_label(t)
        y_offset = 8 if i % 2 == 0 else 18
        ax1.annotate(lbl, (x[i], t), textcoords="offset points", xytext=(0, y_offset), ha="center", fontsize=8.5, fontweight="bold", color=color1)

    ax2 = ax1.twinx()
    color2 = "#2ca02c"
    ax2.set_ylabel("Creation Throughput (Tuples / Sec)", color=color2, fontsize=12, fontweight="bold")
    line2 = ax2.plot(x, tps, color=color2, marker="s", linewidth=2.8, linestyle="--", markersize=8, label="Throughput (tup/s)")
    ax2.tick_params(axis="y", labelcolor=color2)
    ax2.set_ylim(0, max(tps) * 1.25)

    for i, tp in enumerate(tps):
        lbl = format_throughput_label(tp)
        y_offset = -16 if i % 2 == 0 else -25
        ax2.annotate(lbl, (x[i], tp), textcoords="offset points", xytext=(0, y_offset), ha="center", fontsize=8.5, fontweight="bold", color=color2)

    lines = line1 + line2
    labels_leg = [l.get_label() for l in lines]
    ax1.legend(lines, labels_leg, loc="upper left", frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=11)

    plt.title("AriaBC Dataset Creation Linear Scaling & Ingestion Throughput (1k → 50M)", fontsize=14, fontweight="bold", pad=15)
    plt.tight_layout()
    out_file = output_dir / "dataset_creation_scaling_and_throughput.png"
    plt.savefig(out_file, bbox_inches="tight")
    plt.close()
    print(f"Generated plot: {out_file}")


def plot_storage_footprint_dual(rows, output_dir: Path):
    """
    Plot dual-panel storage footprint:
    - Left: Small scales (1k to 1M) with exact KB/MB units
    - Right: Large scales (1M to 50M) with exact MB/GB units
    """
    small_rows = [r for r in rows if r["scale"] <= 1_000_000]
    large_rows = [r for r in rows if r["scale"] >= 1_000_000]

    colors = {
        "heap": "#2b5c8f",
        "pkey": "#3a9278",
        "merkle": "#e27c3e",
        "lookup": "#8b5bb5",
    }

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), dpi=200, gridspec_kw={"width_ratios": [1, 1.8]})

    # --- Left Panel: 1k to 1M (MB) ---
    labels1 = [r["scale_label"] for r in small_rows]
    heap1 = [r["heap_mb"] for r in small_rows]
    pkey1 = [r["pkey_mb"] for r in small_rows]
    merkle1 = [r["merkle_mb"] for r in small_rows]
    lookup1 = [r["lookup_mb"] for r in small_rows]
    x1 = np.arange(len(labels1))
    w1 = 0.18

    ax1.bar(x1 - 1.5 * w1, heap1, w1, label="Heap Table", color=colors["heap"])
    ax1.bar(x1 - 0.5 * w1, pkey1, w1, label="PK B-Tree", color=colors["pkey"])
    ax1.bar(x1 + 0.5 * w1, merkle1, w1, label="Merkle AM Index", color=colors["merkle"])
    ax1.bar(x1 + 1.5 * w1, lookup1, w1, label="Lookup B-Tree", color=colors["lookup"])

    ax1.set_title("Small & Medium Scales Storage (1k → 1M)", fontsize=13, fontweight="bold", pad=10)
    ax1.set_ylabel("Storage Size on Disk (MB)", fontsize=11, fontweight="bold")
    ax1.set_xlabel("Scale (Tuples)", fontsize=11, fontweight="bold")
    ax1.set_xticks(x1)
    ax1.set_xticklabels(labels1, fontsize=10.5, fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.5)
    ax1.set_ylim(0, small_rows[-1]["total_mb"] * 1.25)

    for i in range(len(small_rows)):
        tot = small_rows[i]["total_mb"]
        max_v = max(heap1[i], pkey1[i], merkle1[i], lookup1[i])
        ax1.text(x1[i], max_v + (small_rows[-1]["total_mb"] * 0.04), f"Total:\n{format_size_label(tot)}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    # --- Right Panel: 1M to 50M (GB) ---
    labels2 = [r["scale_label"] for r in large_rows]
    heap2 = [r["heap_mb"] / 1024.0 for r in large_rows]
    pkey2 = [r["pkey_mb"] / 1024.0 for r in large_rows]
    merkle2 = [r["merkle_mb"] / 1024.0 for r in large_rows]
    lookup2 = [r["lookup_mb"] / 1024.0 for r in large_rows]
    total_gb2 = [r["total_mb"] / 1024.0 for r in large_rows]
    x2 = np.arange(len(labels2))
    w2 = 0.18

    ax2.bar(x2 - 1.5 * w2, heap2, w2, color=colors["heap"])
    ax2.bar(x2 - 0.5 * w2, pkey2, w2, color=colors["pkey"])
    ax2.bar(x2 + 0.5 * w2, merkle2, w2, color=colors["merkle"])
    ax2.bar(x2 + 1.5 * w2, lookup2, w2, color=colors["lookup"])

    ax2.set_title("Production Multi-Million Scales Storage (1M → 50M)", fontsize=13, fontweight="bold", pad=10)
    ax2.set_ylabel("Storage Size on Disk (GB)", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Scale (Tuples)", fontsize=11, fontweight="bold")
    ax2.set_xticks(x2)
    ax2.set_xticklabels(labels2, fontsize=10.5, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.5)
    ax2.set_ylim(0, total_gb2[-1] * 1.18)

    for i in range(len(large_rows)):
        tot = large_rows[i]["total_mb"]
        max_v = max(heap2[i], pkey2[i], merkle2[i], lookup2[i])
        ax2.text(x2[i], max_v + (total_gb2[-1] * 0.03), f"Total:\n{format_size_label(tot)}", ha="center", va="bottom", fontsize=8, fontweight="bold")

    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.99), ncol=4, frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=10)

    plt.suptitle("AriaBC Storage Footprint by Relation Component (1k → 50M)", fontsize=15, fontweight="bold", y=1.04)
    plt.tight_layout()
    out_file = output_dir / "dataset_creation_storage_footprint.png"
    plt.savefig(out_file, bbox_inches="tight")
    plt.close()
    print(f"Generated plot: {out_file}")


def plot_percentage_share_area(rows, output_dir: Path):
    """Plot 100% stacked area / bar chart showing percentage share of each component across scales."""
    labels = [r["scale_label"] for r in rows]
    total_ms = np.array([r["total_ms"] for r in rows])
    heap_pct = np.array([r["heap_ms"] / r["total_ms"] * 100.0 for r in rows])
    pkey_pct = np.array([r["pkey_btree_ms"] / r["total_ms"] * 100.0 for r in rows])
    merkle_pct = np.array([r["merkle_am_ms"] / r["total_ms"] * 100.0 for r in rows])
    lookup_pct = np.array([r["lookup_btree_ms"] / r["total_ms"] * 100.0 for r in rows])
    analyze_pct = np.array([r["analyze_ms"] / r["total_ms"] * 100.0 for r in rows])

    x = np.arange(len(labels))
    w = 0.55

    fig, ax = plt.subplots(figsize=(16, 7), dpi=200)

    p1 = ax.bar(x, heap_pct, w, label="Heap Populate", color="#2b5c8f")
    p2 = ax.bar(x, pkey_pct, w, bottom=heap_pct, label="PK B-Tree", color="#3a9278")
    p3 = ax.bar(x, merkle_pct, w, bottom=heap_pct + pkey_pct, label="Merkle AM Index", color="#e27c3e")
    p4 = ax.bar(x, lookup_pct, w, bottom=heap_pct + pkey_pct + merkle_pct, label="Lookup B-Tree", color="#8b5bb5")
    p5 = ax.bar(x, analyze_pct, w, bottom=heap_pct + pkey_pct + merkle_pct + lookup_pct, label="ANALYZE & Catalog Stats", color="#888888")

    ax.set_ylabel("Component Share of Total Creation Time (%)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Dataset Scale (Tuples)", fontsize=12, fontweight="bold")
    ax.set_title("Creation Time Percentage Share by Component Across Scales (1k → 50M)", fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10.5, fontweight="bold")
    ax.set_ylim(0, 100)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=5, frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=10)

    for i in range(len(rows)):
        # Annotate dominant components if share > 7%
        cum = 0
        for pct, color in [(heap_pct[i], "white"), (pkey_pct[i], "white"), (merkle_pct[i], "white"), (lookup_pct[i], "white"), (analyze_pct[i], "black")]:
            if pct > 6.5:
                ax.text(x[i], cum + pct / 2.0, f"{pct:.1f}%", ha="center", va="center", color=color, fontsize=7.5, fontweight="bold")
            cum += pct

    plt.tight_layout()
    out_file = output_dir / "dataset_creation_component_percentage_share.png"
    plt.savefig(out_file, bbox_inches="tight")
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

    plot_component_breakdown_dual(rows, output_dir)
    plot_scaling_and_throughput(rows, output_dir)
    plot_storage_footprint_dual(rows, output_dir)
    plot_percentage_share_area(rows, output_dir)
    print("All enhanced visualization plots generated successfully!")


if __name__ == "__main__":
    main()
