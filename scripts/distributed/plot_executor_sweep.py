#!/usr/bin/env python3
"""
Plot TPS across executor worker thread counts.
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
    if not path.exists():
        raise SystemExit(f"Input file not found: {path}")

    valid: list[dict[str, str]] = []
    rejected: list[str] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip subsequent header rows if present
            if row.get("run_id") == "run_id" or not row.get("run_id"):
                continue

            workers = as_int(row, "server_workers")
            tps = as_float(row, "tps")
            merkle = as_int(row, "merkle_pass")
            div = as_int(row, "divergence_count")
            fail = as_int(row, "permanent_failures")

            ok = (
                workers > 0
                and math.isfinite(tps)
                and tps > 0
                and merkle == 1
                and div == 0
                and fail == 0
            )

            if ok:
                valid.append(row)
            else:
                rejected.append(
                    f"run_id={row.get('run_id')} workers={row.get('server_workers')} "
                    f"tps={row.get('tps')} merkle={row.get('merkle_pass')} "
                    f"div={row.get('divergence_count')} fail={row.get('permanent_failures')}"
                )

    if rejected:
        print("Warning: The following runs were rejected due to invalid metrics or correctness failures:", file=sys.stderr)
        for rej in rejected:
            print(f"  - {rej}", file=sys.stderr)

    if not valid:
        raise SystemExit("No valid data rows found to plot.")

    return valid


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot executor worker sweep TPS.")
    parser.add_argument("--input-csv", required=True, type=pathlib.Path, help="Path to summary.csv")
    parser.add_argument("--output-img", required=True, type=pathlib.Path, help="Path to save the output graph")
    parser.add_argument("--title", default="Executor Worker Sweep: TPS vs Executor Workers", help="Title of the plot")
    args = parser.parse_args()

    rows = load_valid_rows(args.input_csv)

    # Group by server_workers
    grouped: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        workers = as_int(row, "server_workers")
        tps = as_float(row, "tps")
        grouped[workers].append(tps)

    xs = sorted(grouped.keys())
    ys_median = []
    ys_min = []
    ys_max = []

    for x in xs:
        tps_list = grouped[x]
        ys_median.append(statistics.median(tps_list))
        ys_min.append(min(tps_list))
        ys_max.append(max(tps_list))

    # Plot styling
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    
    fig, ax = plt.subplots(figsize=(10, 6), dpi=180)
    
    # Custom color palette (sleek Indigo/Violet theme)
    primary_color = "#6366F1"  # Indigo
    fill_color = "#E0E7FF"     # Very light Indigo
    text_color = "#1E293B"     # Dark Slate
    
    # Plot range/fill for min-max
    ax.fill_between(xs, ys_min, ys_max, color=primary_color, alpha=0.15, label="Min-Max Range")
    
    # Plot line and marker points
    ax.plot(xs, ys_median, color=primary_color, linewidth=2.5, marker="o", markersize=8, label="Median TPS")
    
    # Error bars for variability
    yerr_low = [med - mn for med, mn in zip(ys_median, ys_min)]
    yerr_high = [mx - med for med, mx in zip(ys_median, ys_max)]
    ax.errorbar(xs, ys_median, yerr=[yerr_low, yerr_high], fmt="none", ecolor=primary_color, elinewidth=1.5, capsize=4)

    # Annotate points with median values
    for x, y in zip(xs, ys_median):
        ax.annotate(
            f"{y:.1f}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
            fontweight="semibold",
            color=text_color
        )

    ax.set_title(args.title, fontsize=14, fontweight="bold", pad=15, color=text_color)
    ax.set_xlabel("Server Executor Workers", fontsize=11, labelpad=10, color=text_color)
    ax.set_ylabel("Completed Transactions Per Second (TPS)", fontsize=11, labelpad=10, color=text_color)
    
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    
    # Grid customization
    ax.grid(True, linestyle="--", alpha=0.5, color="#CBD5E1")
    
    # Legend
    ax.legend(loc="lower right", frameon=True, facecolor="white", edgecolor="#E2E8F0")
    
    # Remove top and right spines
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
        
    fig.tight_layout()
    args.output_img.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output_img, bbox_inches="tight")
    plt.close(fig)

    print(f"Successfully generated graph: {args.output_img}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
