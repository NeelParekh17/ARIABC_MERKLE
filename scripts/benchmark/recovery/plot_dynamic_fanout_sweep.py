#!/usr/bin/env python3
"""Plot the complete native dynamic logical-fanout sweep."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt


PHASES = [
    ("tree_localisation_ms", "Tree localisation"),
    ("candidate_summary_fetch_ms", "Summary fetch"),
    ("summary_comparison_ms", "Summary comparison"),
    ("exact_heap_fetch_ms", "Exact heap fetch"),
    ("repair_write_ms", "Repair write"),
    ("native_commit_visibility_ms", "Commit visibility"),
    ("post_commit_relocalisation_ms", "Post-commit relocalisation"),
]


def rows(path: Path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_dynamic(path: Path) -> dict[int, dict[str, float]]:
    runs = {int(row["tuple_count"]): row for row in rows(path / "runs.csv")}
    phase_by_run = {}
    for row in rows(path / "phase_timings.csv"):
        phase_by_run.setdefault(row["run_id"], {})[row["phase"]] = float(row["ms"])
    stats = {}
    for row in rows(path / "dynamic_tree_stats.csv"):
        if row["stage"] == "index_build":
            stats[int(row["tuple_count"])] = json.loads(row["raw_stats"])
    sizes = {int(row["tuple_count"]): row for row in rows(path / "dataset_sizes.csv")}
    result = {}
    for size, run in runs.items():
        phases = phase_by_run[run["run_id"]]
        value = {"restore_repair_ms": float(run["restore_repair_ms"]),
                 "total_ms": float(run["total_ms"]),
                 "levels": int(run["localisation_levels_visited"]),
                 "logical_ranges_compared": int(run["logical_ranges_compared"]),
                 "range_summary_rows": int(run["range_summary_rows_read"]),
                 "bad_partitions": int(run["bad_partition_count"]),
                 "ranges": int(run["localised_bad_range_count"]),
                 "summary_items": int(run["dynamic_candidate_summary_items_fetched"]),
                 "index_bytes": int(run["healthy_index_relation_bytes"]),
                 "schema_bytes": int(sizes[size]["total_schema_bytes"]),
                 "leaf_count": int(stats[size]["leaf_count"]),
                 "node_count": int(stats[size]["node_count"]),
                 "max_depth": int(stats[size]["max_depth"]),
                 "max_leaf_occupancy": int(stats[size]["max_leaf_items"]),
                 "fanout": int(run["fanout"])}
        for key, _ in PHASES:
            value[key] = phases[key]
        result[size] = value
    return result


def load_static(path: Path) -> tuple[dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    all_run_rows = rows(path / "runs.csv")
    run_rows = [row for row in all_run_rows if int(row["tuple_count"]) <= 10_000_000]
    run_to_size = {row["run_id"]: int(row["tuple_count"]) for row in all_run_rows}
    grouped = {}
    for row in run_rows:
        grouped.setdefault(int(row["tuple_count"]), []).append(row)
    phases = {}
    for row in rows(path / "phase_timings.csv"):
        size = run_to_size[row["run_id"]]
        if size > 10_000_000:
            continue
        phases.setdefault(size, {}).setdefault(row["phase"], []).append(float(row["ms"]))
    result = {}
    for size, values in grouped.items():
        median = lambda key: sorted(float(v[key]) for v in values)[len(values) // 2]
        result[size] = {"restore_repair_ms": median("restore_repair_ms"),
                        "schema_bytes": int(next(r["total_schema_bytes"] for r in rows(path / "dataset_sizes.csv") if int(r["tuple_count"]) == size)),
                        "index_bytes": int(next(r["merkle_index_bytes"] for r in rows(path / "dataset_sizes.csv") if int(r["tuple_count"]) == size)) + int(next(r["leaf_lookup_index_bytes"] for r in rows(path / "dataset_sizes.csv") if int(r["tuple_count"]) == size))}
    phase_medians = {size: {key: sorted(values)[len(values) // 2] for key, values in phase_map.items()} for size, phase_map in phases.items()}
    return result, phase_medians


def save(fig, path: Path):
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-artifact", type=Path, required=True)
    parser.add_argument("--fanout-artifact", action="append", required=True,
                        help="FANOUT=PATH; repeat for each fanout")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fanouts = {}
    for spec in args.fanout_artifact:
        fanout, path = spec.split("=", 1)
        fanouts[int(fanout)] = load_dynamic(Path(path))
    static, static_phases = load_static(args.static_artifact)
    sizes = sorted(next(iter(fanouts.values())).keys())
    x = [size / 1_000_000 for size in sizes]
    colors = {2: "#4c78a8", 4: "#f58518", 8: "#54a24b", 16: "#e45756", 32: "#b279a2"}

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for fanout, data in sorted(fanouts.items()):
        ax.plot(x, [data[s]["restore_repair_ms"] for s in sizes], marker="o", label=f"Dynamic F{fanout}", color=colors[fanout])
    ax.plot(x, [static[s]["restore_repair_ms"] for s in sizes], "k--", marker="s", label="Static F32/L1024 median")
    ax.set(title="EPYC sparse recovery across every logical fanout", xlabel="Rows (millions)", ylabel="restore_repair_ms (ms)")
    ax.set_xticks(x); ax.legend(ncol=2)
    save(fig, args.output_dir / "epyc_dynamic_fanout_recovery.png")

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    phase_plot = [("tree_localisation_ms", "Tree localisation"), ("candidate_summary_fetch_ms", "Summary fetch"),
                  ("repair_write_ms", "Repair write"), ("native_commit_visibility_ms", "Commit visibility")]
    for ax, (key, title) in zip(axes.flat, phase_plot):
        for fanout, data in sorted(fanouts.items()):
            ax.plot(x, [data[s][key] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
        ax.plot(x, [static_phases[s].get({"tree_localisation_ms": "tree_localisation_ms", "candidate_summary_fetch_ms": "candidate_row_fetch_ms", "repair_write_ms": "repair_write_ms", "native_commit_visibility_ms": "targeted_post_repair_confirmation_ms"}[key], 0) for s in sizes], "k--", label="Static")
        ax.set_title(title); ax.set_xlabel("Rows (M)"); ax.set_ylabel("ms"); ax.set_xticks(x)
    axes.flat[0].legend(ncol=2, fontsize=8)
    fig.suptitle("Fanout effect on recovery phases (static reference dashed)")
    save(fig, args.output_dir / "epyc_dynamic_fanout_phases.png")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    for fanout, data in sorted(fanouts.items()):
        axes[0].plot(x, [data[s]["levels"] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
    for fanout, data in sorted(fanouts.items()):
        axes[1].plot([data[s]["levels"] for s in sizes], [data[s]["tree_localisation_ms"] for s in sizes], marker="o", color=colors[fanout], label=f"F{fanout}")
    axes[0].set(title="Logical levels visited", xlabel="Rows (M)", ylabel="Levels"); axes[0].set_xticks(x)
    axes[1].set(title="Levels do not explain localisation cost", xlabel="Levels visited", ylabel="Tree localisation (ms)")
    axes[0].legend(ncol=2, fontsize=8); axes[1].legend(ncol=2, fontsize=8)
    save(fig, args.output_dir / "epyc_dynamic_fanout_localisation.png")

    fig, axes = plt.subplots(1, 3, figsize=(16, 5.3))
    fanout_values = sorted(fanouts)
    size_colors = {1_000_000: "#4c78a8", 3_000_000: "#f58518",
                   5_000_000: "#54a24b", 10_000_000: "#e45756"}
    for size in sizes:
        label = f"{size // 1_000_000}M"
        axes[0].plot(fanout_values,
                     [fanouts[f][size]["logical_ranges_compared"] for f in fanout_values],
                     marker="o", label=label, color=size_colors[size])
        axes[1].plot(fanout_values,
                     [fanouts[f][size]["range_summary_rows"] for f in fanout_values],
                     marker="o", label=label, color=size_colors[size])
        axes[2].plot(fanout_values,
                     [fanouts[f][size]["tree_localisation_ms"] /
                      fanouts[f][size]["levels"] for f in fanout_values],
                     marker="o", label=label, color=size_colors[size])
    axes[0].set(title="Logical ranges compared", xlabel="Logical fanout", ylabel="Ranges")
    axes[1].set(title="Healthy + damaged summary rows", xlabel="Logical fanout", ylabel="Rows")
    axes[2].set(title="Localisation cost per logical level", xlabel="Logical fanout", ylabel="ms / level")
    for ax in axes:
        ax.set_xscale("log", base=2); ax.set_xticks(fanout_values, labels=[str(f) for f in fanout_values])
    axes[0].legend(title="Rows")
    fig.suptitle("Why wider dynamic fanout can cost more despite fewer levels")
    save(fig, args.output_dir / "epyc_dynamic_fanout_localisation_work.png")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    for fanout, data in sorted(fanouts.items()):
        axes[0].plot(x, [data[s]["summary_items"] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
        axes[1].plot(x, [data[s]["ranges"] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
    axes[0].set(title="Fetched dynamic summary items", xlabel="Rows (M)", ylabel="Items"); axes[0].set_xticks(x)
    axes[1].set(title="Localised logical ranges", xlabel="Rows (M)", ylabel="Ranges"); axes[1].set_xticks(x)
    axes[0].legend(ncol=2, fontsize=8)
    save(fig, args.output_dir / "epyc_dynamic_fanout_candidate_work.png")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    for fanout, data in sorted(fanouts.items()):
        axes[0].plot(x, [data[s]["index_bytes"] / 1e6 for s in sizes], marker="o", label=f"Dynamic F{fanout}", color=colors[fanout])
        axes[1].plot(x, [data[s]["schema_bytes"] / 1e6 for s in sizes], marker="o", label=f"Dynamic F{fanout}", color=colors[fanout])
    axes[0].plot(x, [static[s]["index_bytes"] / 1e6 for s in sizes], "k--", marker="s", label="Static auxiliary")
    axes[1].plot(x, [static[s]["schema_bytes"] / 1e6 for s in sizes], "k--", marker="s", label="Static total schema")
    axes[0].set(title="Merkle storage by fanout", xlabel="Rows (M)", ylabel="MB", yscale="log"); axes[0].set_xticks(x)
    axes[1].set(title="Total schema storage by fanout", xlabel="Rows (M)", ylabel="MB"); axes[1].set_xticks(x)
    axes[0].legend(ncol=2, fontsize=8); axes[1].legend(ncol=2, fontsize=8)
    save(fig, args.output_dir / "epyc_dynamic_fanout_storage.png")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    for fanout, data in sorted(fanouts.items()):
        axes[0].plot(x, [data[s]["leaf_count"] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
        axes[1].plot(x, [data[s]["node_count"] for s in sizes], marker="o", label=f"F{fanout}", color=colors[fanout])
    axes[0].set(title="Dynamic leaf count", xlabel="Rows (M)", ylabel="Leaves"); axes[0].set_xticks(x)
    axes[1].set(title="Dynamic node count", xlabel="Rows (M)", ylabel="Nodes"); axes[1].set_xticks(x)
    axes[0].legend(ncol=2, fontsize=8)
    save(fig, args.output_dir / "epyc_dynamic_fanout_tree_geometry.png")

    with (args.output_dir / "fanout_sweep_summary.csv").open("w", newline="") as handle:
        fields = ["fanout", "rows"] + [key for key, _ in PHASES] + ["restore_repair_ms", "total_ms", "levels", "logical_ranges_compared", "range_summary_rows", "bad_partitions", "ranges", "summary_items", "index_bytes", "schema_bytes", "leaf_count", "node_count", "max_depth", "max_leaf_occupancy"]
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for fanout, data in sorted(fanouts.items()):
            for size in sizes:
                out = {"fanout": fanout, "rows": size}; out.update(data[size]); writer.writerow(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
