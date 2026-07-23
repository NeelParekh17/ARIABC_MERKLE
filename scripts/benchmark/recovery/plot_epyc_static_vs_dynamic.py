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
    parser.add_argument("--prior-dynamic-artifact", type=Path)
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
            marker="o", linewidth=2.2, label="Latest dynamic v8 (1 run)")
    ax.set(title="EPYC sparse recovery: static best vs latest dynamic",
           xlabel="Table rows (millions)", ylabel="Recovery latency (ms, log scale)",
           yscale="log")
    ax.set_xticks(sizes_m)
    ax.legend()
    save(fig, args.output_dir / "epyc_static_vs_dynamic_recovery.png")

    # Dynamic lifecycle time is not directly comparable with the older static
    # artifact: schema v6 explicitly includes pre-recovery setup and cleanup,
    # while the schema-v3 static timer does not expose the same boundary.
    fig, ax = plt.subplots(figsize=(10, 5.5))
    setup = [dynamic_phases[s]["pre_recovery_setup_ms"] for s in SIZES]
    recovery = [dynamic_runs[s]["restore_repair_ms"] for s in SIZES]
    cleanup = [dynamic_runs[s]["cleanup_ms"] for s in SIZES]
    residual = [max(0, dynamic_runs[s]["total_ms"] - a - b - c)
                for s, a, b, c in zip(SIZES, setup, recovery, cleanup)]
    ax.stackplot(sizes_m, [setup, recovery, cleanup, residual],
                 labels=["Pre-recovery setup", "Sparse recovery", "Cleanup", "Harness overhead"],
                 colors=["#4c78a8", "#54a24b", "#f58518", "#b8b8b8"], alpha=0.9)
    ax.set(title="Latest dynamic v8 lifecycle cost (not a static-comparable recovery timer)",
           xlabel="Table rows (millions)", ylabel="Observed lifecycle time (ms)")
    ax.set_xticks(sizes_m)
    ax.legend()
    save(fig, args.output_dir / "epyc_dynamic_lifecycle_cost.png")

    # Physical storage: auxiliary static footprint versus native dynamic index,
    # and the full schema footprint including the common table/index base.
    static_aux = [static_sizes[s]["merkle_index_bytes"] +
                  static_sizes[s].get("leaf_lookup_index_bytes", 0)
                  for s in SIZES]
    dynamic_index = [dynamic_sizes[s]["merkle_index_bytes"] for s in SIZES]
    static_schema = [static_sizes[s]["total_schema_bytes"] for s in SIZES]
    dynamic_schema = [dynamic_sizes[s]["total_schema_bytes"] for s in SIZES]
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    axes[0].plot(sizes_m, [v / 1e9 for v in static_aux], marker="o", label="Static auxiliary")
    axes[0].plot(sizes_m, [v / 1e9 for v in dynamic_index], marker="o", label="Dynamic native index v8")
    axes[0].set(title="Merkle index storage", xlabel="Rows (M)", ylabel="Bytes (GB)", yscale="log")
    axes[0].legend()
    axes[1].plot(sizes_m, [v / 1e9 for v in static_schema], marker="o", label="Static total schema")
    axes[1].plot(sizes_m, [v / 1e9 for v in dynamic_schema], marker="o", label="Dynamic total schema v8")
    axes[1].set(title="Overall schema storage", xlabel="Rows (M)", ylabel="Bytes (GB)", yscale="log")
    axes[1].legend()
    fig.suptitle("EPYC storage cost: static best versus compact dynamic v8", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_storage.png")

    # Storage premium of the latest dynamic layout relative to static.
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    index_premium = [(d - s) / s * 100 for s, d in zip(static_aux, dynamic_index)]
    schema_premium = [(d - s) / s * 100 for s, d in zip(static_schema, dynamic_schema)]
    axes[0].plot(sizes_m, index_premium, marker="o", color="darkorange")
    axes[0].set(title="Dynamic native-index premium versus static auxiliary",
                xlabel="Rows (M)", ylabel="Premium (%)")
    axes[0].axhline(0, color="black", linewidth=0.8)
    axes[1].plot(sizes_m, schema_premium, marker="o", color="darkorange")
    axes[1].set(title="Dynamic total-schema premium versus static",
                xlabel="Rows (M)", ylabel="Premium (%)")
    axes[1].axhline(0, color="black", linewidth=0.8)
    fig.suptitle("Latest dynamic v8 storage cost relative to static", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_storage_tradeoff.png")

    # Normalising by row count distinguishes a linear representation cost from
    # a capacity-driven blow-up and makes deployment sizing directly usable.
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    axes[0].plot(sizes_m, [v / s for v, s in zip(static_aux, SIZES)], marker="o", label="Static auxiliary")
    axes[0].plot(sizes_m, [v / s for v, s in zip(dynamic_index, SIZES)], marker="o", label="Dynamic native index v8")
    axes[0].set(title="Merkle storage per table row", xlabel="Rows (M)", ylabel="Bytes per row")
    axes[0].legend()
    axes[1].plot(sizes_m, [v / s for v, s in zip(static_schema, SIZES)], marker="o", label="Static total schema")
    axes[1].plot(sizes_m, [v / s for v, s in zip(dynamic_schema, SIZES)], marker="o", label="Dynamic total schema v8")
    axes[1].set(title="Total schema storage per row", xlabel="Rows (M)", ylabel="Bytes per row")
    axes[1].legend()
    fig.suptitle("EPYC deployment storage intensity", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_storage_per_row.png")

    # Common phase comparison.
    mappings = [
        ("Tree localisation", "tree_localisation_ms", "tree_localisation_ms"),
        ("Candidate fetch", "candidate_row_fetch_ms", "candidate_summary_fetch_ms"),
        ("Comparison", "row_comparison_ms", "summary_comparison_ms"),
        ("Repair write", "repair_write_ms", "repair_write_ms"),
        ("Post-repair confirmation / commit", "targeted_post_repair_confirmation_ms", "native_commit_visibility_ms"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.5))
    for ax, (title, static_key, dynamic_key) in zip(axes.flat, mappings):
        ax.plot(sizes_m, [static_phases[s][static_key] for s in SIZES], marker="o", label="Static best")
        ax.plot(sizes_m, [dynamic_phases[s][dynamic_key] for s in SIZES], marker="o", label="Latest dynamic")
        ax.set_title(title)
        ax.set_xlabel("Rows (M)")
        ax.set_ylabel("ms")
        ax.set_xticks(sizes_m[::2])
        if title == "Repair write":
            ax.set_yscale("log")
    axes.flat[0].legend()
    axes.flat[-1].axis("off")
    fig.suptitle("EPYC recovery phases: static best vs latest dynamic", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_phases.png")

    # Stacked recovery-phase cost exposes where the overall latency is spent.
    stack_mappings = [
        ("tree_localisation_ms", "tree_localisation_ms", "Tree localisation"),
        ("candidate_row_fetch_ms", "candidate_summary_fetch_ms", "Candidate fetch"),
        ("row_comparison_ms", "summary_comparison_ms", "Comparison"),
        ("repair_write_ms", "repair_write_ms", "Repair write"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.8), sharey=True)
    colors = ["#4c78a8", "#f58518", "#54a24b", "#e45756"]
    for ax, label, runs, phases in [
        (axes[0], "Static F32/L1024", static_runs, static_phases),
        (axes[1], "Dynamic v8", dynamic_runs, dynamic_phases),
    ]:
        values = [[phases[s][dynamic_key if label == "Dynamic v8" else static_key]
                   for s in SIZES] for static_key, dynamic_key, _ in stack_mappings]
        ax.stackplot(sizes_m, values, labels=[title for _, _, title in stack_mappings],
                     colors=colors, alpha=0.9)
        ax.plot(sizes_m, [runs[s]["restore_repair_ms"] for s in SIZES],
                color="black", linewidth=1.2, linestyle="--", label="Paper recovery total")
        ax.set(title=label, xlabel="Rows (M)", ylabel="Phase time (ms)")
        ax.set_xticks(sizes_m[::2])
        ax.legend(loc="upper left", fontsize=8)
    fig.suptitle("Where recovery time is spent", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_phase_cost.png")

    # Percentage phase composition makes the isolated 40M repair-write tail
    # visible without letting absolute latency obscure the normal case.
    dynamic_phase_keys = [
        ("tree_localisation_ms", "Tree localisation"),
        ("candidate_summary_fetch_ms", "Summary fetch"),
        ("summary_comparison_ms", "Summary comparison"),
        ("exact_heap_fetch_ms", "Exact heap fetch"),
        ("repair_write_ms", "Repair write"),
        ("native_commit_visibility_ms", "Commit visibility"),
        ("post_commit_relocalisation_ms", "Post-commit relocalisation"),
    ]
    totals = [dynamic_runs[s]["restore_repair_ms"] for s in SIZES]
    values = [[100 * dynamic_phases[s][key] / total for s, total in zip(SIZES, totals)]
              for key, _ in dynamic_phase_keys]
    accounted = [sum(series[i] for series in values) for i in range(len(SIZES))]
    values.append([max(0, 100 - value) for value in accounted])
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.stackplot(sizes_m, values,
                 labels=[label for _, label in dynamic_phase_keys] + ["Other orchestration"],
                 alpha=0.9)
    ax.set(title="Latest dynamic v8 sparse-recovery phase share",
           xlabel="Table rows (millions)", ylabel="Share of restore_repair_ms (%)",
           ylim=(0, 100))
    ax.set_xticks(sizes_m)
    ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=8)
    save(fig, args.output_dir / "epyc_dynamic_phase_share.png")

    if args.prior_dynamic_artifact:
        prior_runs, _ = load_artifact(args.prior_dynamic_artifact)
        fig, ax = plt.subplots(figsize=(10, 5.5))
        ax.plot(sizes_m, [prior_runs[s]["restore_repair_ms"] for s in SIZES],
                marker="o", linewidth=2.0, label="Prior v8 sweep (20260722T033848Z)")
        ax.plot(sizes_m, [dynamic_runs[s]["restore_repair_ms"] for s in SIZES],
                marker="o", linewidth=2.0, label="Latest v8 sweep (20260722T152802Z)")
        ax.set(title="Single-repetition dynamic v8 sweep stability",
               xlabel="Table rows (millions)", ylabel="Sparse recovery latency (ms)")
        ax.set_xticks(sizes_m)
        ax.legend()
        save(fig, args.output_dir / "epyc_dynamic_run_stability.png")

    # Leaf geometry and occupancy.
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.3))
    axes[0].plot(sizes_m, [static_sizes[s]["mean"] for s in SIZES], marker="o", label="Static best")
    axes[0].plot(sizes_m, [dynamic_sizes[s]["mean"] for s in SIZES], marker="o", label="Latest dynamic")
    axes[0].axhline(32, color="black", linestyle="--", linewidth=1, label="Dynamic leaf capacity")
    axes[0].set(title="Average physical rows per leaf", xlabel="Rows (M)", ylabel="Rows per leaf")
    axes[0].legend()
    axes[1].plot(sizes_m, [static_sizes[s]["total_leaf_count"] for s in SIZES], marker="o", label="Static best")
    axes[1].plot(sizes_m, [dynamic_sizes[s]["total_leaf_count"] for s in SIZES], marker="o", label="Latest dynamic")
    axes[1].set(title="Physical leaf count", xlabel="Rows (M)", ylabel="Leaves", yscale="log")
    axes[1].yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    axes[1].legend()
    fig.suptitle("EPYC Merkle geometry: fixed static leaves vs bounded dynamic leaves", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_leaf_geometry.png")

    # Tree levels and physical-node growth.
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    axes[0].step(sizes_m, [static_sizes[s]["tree_levels"] for s in SIZES], where="mid",
                 marker="o", label="Static tree levels")
    axes[0].step(sizes_m, [dynamic_runs[s]["localisation_levels_visited"] for s in SIZES],
                 where="mid", marker="o", label="Dynamic levels visited")
    axes[0].set(title="Logical recovery levels", xlabel="Rows (M)", ylabel="Levels")
    axes[0].legend()
    axes[1].plot(sizes_m, [static_sizes[s]["total_leaf_count"] for s in SIZES],
                 marker="o", label="Static leaves")
    axes[1].plot(sizes_m, [dynamic_sizes[s]["dynamic_node_count"] for s in SIZES],
                 marker="o", label="Dynamic physical nodes")
    axes[1].set(title="Physical tree growth", xlabel="Rows (M)", ylabel="Nodes / leaves", yscale="log")
    axes[1].legend()
    fig.suptitle("EPYC tree levels and physical structure", fontsize=14)
    save(fig, args.output_dir / "epyc_static_vs_dynamic_levels.png")

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
