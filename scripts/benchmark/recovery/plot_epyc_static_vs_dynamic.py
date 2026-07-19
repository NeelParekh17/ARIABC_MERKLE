#!/usr/bin/env python3
"""Plot the authoritative EPYC static-versus-dynamic recovery comparison."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


SIZES = [
    1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000,
    15_000_000, 20_000_000, 25_000_000, 30_000_000,
    40_000_000, 50_000_000,
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_artifact(path: Path) -> tuple[dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    runs = read_csv(path / "runs.csv")
    run_to_size = {row["run_id"]: int(row["tuple_count"]) for row in runs}
    grouped_runs: dict[int, list[dict[str, str]]] = defaultdict(list)
    grouped_phases: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in runs:
        grouped_runs[int(row["tuple_count"])].append(row)
    for row in read_csv(path / "phase_timings.csv"):
        grouped_phases[run_to_size[row["run_id"]]][row["phase"]].append(float(row["ms"]))

    run_values: dict[int, dict[str, float]] = {}
    phase_values: dict[int, dict[str, float]] = {}
    for size, rows in grouped_runs.items():
        values: dict[str, float] = {}
        for key in rows[0]:
            try:
                values[key] = statistics.median(float(row[key]) for row in rows)
            except (ValueError, TypeError):
                continue
        run_values[size] = values
    for size, phases in grouped_phases.items():
        phase_values[size] = {
            phase: statistics.median(values) for phase, values in phases.items()
        }
    return run_values, phase_values


def load_sizes(path: Path) -> dict[int, dict[str, float]]:
    result: dict[int, dict[str, float]] = {}
    for row in read_csv(path / "dataset_sizes.csv"):
        values: dict[str, float] = {}
        for key, value in row.items():
            try:
                values[key] = float(value)
            except (ValueError, TypeError):
                continue
        result[int(row["tuple_count"])] = values
    return result


def configure() -> None:
    plt.rcParams.update({
        "figure.dpi": 120,
        "savefig.dpi": 180,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "font.size": 10,
    })


def save(fig, output: Path) -> None:
    fig.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-artifact", type=Path, required=True)
    parser.add_argument("--dynamic-artifact", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    static_runs, static_phases = load_artifact(args.static_artifact)
    dynamic_runs, dynamic_phases = load_artifact(args.dynamic_artifact)
    static_sizes = load_sizes(args.static_artifact)
    dynamic_sizes = load_sizes(args.dynamic_artifact)
    sizes_m = [size / 1_000_000 for size in SIZES]
    configure()

    # Overall recovery latency.
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(sizes_m, [static_runs[s]["restore_repair_ms"] for s in SIZES],
            marker="o", linewidth=2.2, label="Static F32/L1024 (3-run median)")
    ax.plot(sizes_m, [dynamic_runs[s]["restore_repair_ms"] for s in SIZES],
            marker="o", linewidth=2.2, label="Native dynamic (1 run)")
    ax.set(title="EPYC recovery latency: static vs native dynamic",
           xlabel="Table rows (millions)", ylabel="Recovery latency (ms)")
    ax.set_xticks(sizes_m)
    ax.legend()
    save(fig, args.output_dir / "epyc_static_vs_dynamic_recovery.png")

    # Common phase comparison.
    mappings = [
        ("Tree localisation", "tree_localisation_ms", "tree_localisation_ms"),
        ("Candidate fetch", "candidate_row_fetch_ms", "candidate_summary_fetch_ms"),
        ("Comparison", "row_comparison_ms", "summary_comparison_ms"),
        ("Repair write", "repair_write_ms", "repair_write_ms"),
        ("Post-repair confirmation", "targeted_post_repair_confirmation_ms", "targeted_post_repair_confirmation_ms"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.5))
    for ax, (title, static_key, dynamic_key) in zip(axes.flat, mappings):
        ax.plot(sizes_m, [static_phases[s][static_key] for s in SIZES], marker="o", label="Static")
        ax.plot(sizes_m, [dynamic_phases[s][dynamic_key] for s in SIZES], marker="o", label="Dynamic")
        ax.set_title(title)
        ax.set_xlabel("Rows (M)")
        ax.set_ylabel("ms")
        ax.set_xticks(sizes_m[::2])
    axes.flat[0].legend()
    axes.flat[-1].axis("off")
    fig.suptitle("EPYC recovery phases (static medians vs dynamic single run)", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_phases.png")

    # Leaf geometry and occupancy.
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.3))
    axes[0].plot(sizes_m, [static_sizes[s]["mean"] for s in SIZES], marker="o", label="Static")
    axes[0].plot(sizes_m, [dynamic_sizes[s]["mean"] for s in SIZES], marker="o", label="Dynamic")
    axes[0].axhline(32, color="black", linestyle="--", linewidth=1, label="Dynamic leaf capacity")
    axes[0].set(title="Average physical rows per leaf", xlabel="Rows (M)", ylabel="Rows per leaf")
    axes[0].legend()
    axes[1].plot(sizes_m, [static_sizes[s]["total_leaf_count"] for s in SIZES], marker="o", label="Static")
    axes[1].plot(sizes_m, [dynamic_sizes[s]["total_leaf_count"] for s in SIZES], marker="o", label="Dynamic")
    axes[1].set(title="Physical leaf count", xlabel="Rows (M)", ylabel="Leaves", yscale="log")
    axes[1].yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    axes[1].legend()
    fig.suptitle("EPYC Merkle geometry: fixed static leaves vs bounded dynamic leaves", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_leaf_geometry.png")

    # Localization frontier and candidate payload.
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    axes[0, 0].step(sizes_m, [dynamic_runs[s]["localisation_levels_visited"] for s in SIZES], where="mid")
    axes[0, 0].set(title="Dynamic logical levels visited", xlabel="Rows (M)", ylabel="Levels", yticks=[1, 2, 3, 4])
    axes[0, 1].plot(sizes_m, [dynamic_runs[s]["logical_ranges_compared"] for s in SIZES], marker="o", label="Ranges compared")
    axes[0, 1].plot(sizes_m, [dynamic_runs[s]["range_summary_rows_read"] for s in SIZES], marker="o", label="Summary rows (both replicas)")
    axes[0, 1].set(title="Dynamic localization frontier", xlabel="Rows (M)", ylabel="Items")
    axes[0, 1].legend()
    axes[1, 0].plot(sizes_m, [static_runs[s]["total_candidate_rows"] for s in SIZES], marker="o", label="Static candidate heap rows")
    axes[1, 0].plot(sizes_m, [dynamic_runs[s]["dynamic_candidate_summary_items_fetched"] for s in SIZES], marker="o", label="Dynamic candidate summaries")
    axes[1, 0].axhline(4800, color="black", linestyle="--", linewidth=1, label="Dynamic hard bound")
    axes[1, 0].set(title="Candidate payload", xlabel="Rows (M)", ylabel="Items fetched")
    axes[1, 0].legend()
    axes[1, 1].plot(sizes_m, [static_runs[s]["mean_rows_per_bad_leaf"] for s in SIZES], marker="o", label="Static rows / bad leaf")
    axes[1, 1].plot(sizes_m, [dynamic_runs[s]["dynamic_candidate_summary_items_fetched"] / dynamic_runs[s]["bad_range_count"] for s in SIZES], marker="o", label="Dynamic summaries / bad logical range")
    axes[1, 1].set(title="Candidate density", xlabel="Rows (M)", ylabel="Items per selected range")
    axes[1, 1].legend()
    fig.suptitle("EPYC localization and candidate-work scaling", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_localisation_payload.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
