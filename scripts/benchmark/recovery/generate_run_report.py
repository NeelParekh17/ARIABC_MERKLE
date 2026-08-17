#!/usr/bin/env python3
"""
generate_run_report.py — Post-run analysis report generator.

Reads a fetched dynamic result directory and the canonical static baseline,
generates real matplotlib PNG plots, and writes a Markdown report to:

    Dynamic_merkle_docs/run_reports/<RUN_ID>/RECOVERY_RUN_REPORT_<RUN_ID>.md

with plots under:

    Dynamic_merkle_docs/run_reports/<RUN_ID>/plots/

Usage:
    python3 generate_run_report.py --fetched-dir <path> [--output <path>]
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, stdev

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]
STATIC_BASELINE_ID = "ariabc-recovery-best-scaling-f32-l1024-k75-c300-20260714T040459Z-0068d0"
STATIC_DIR = SCRIPT_DIR / "fetched" / STATIC_BASELINE_ID

# ---------------------------------------------------------------------------
# Canonical static medians (warm r1/r2) from RECOVERY_ARCHITECTURE_ANALYSIS.md
# ---------------------------------------------------------------------------
STATIC_REF = {
    1_000_000:  {"loc": 50.838,  "fetch": 11.166,  "cmp": 3.210,  "repair": 859.629,   "conf": 18.849,  "total": 958.708,  "rpl": 11.92},
    3_000_000:  {"loc": 41.700,  "fetch": 20.733,  "cmp": 4.230,  "repair": 780.639,   "conf": 28.066,  "total": 890.483,  "rpl": 29.52},
    5_000_000:  {"loc": 45.439,  "fetch": 32.736,  "cmp": 5.957,  "repair": 848.995,   "conf": 48.102,  "total": 997.272,  "rpl": 49.44},
    7_000_000:  {"loc": 50.999,  "fetch": 53.948,  "cmp": 7.442,  "repair": 859.749,   "conf": 64.859,  "total": 1054.462, "rpl": 69.89},
    10_000_000: {"loc": 51.441,  "fetch": 70.863,  "cmp": 9.386,  "repair": 856.819,   "conf": 85.511,  "total": 1092.559, "rpl": 98.19},
    15_000_000: {"loc": 51.802,  "fetch": 94.638,  "cmp": 12.994, "repair": 858.267,   "conf": 116.281, "total": 1154.303, "rpl": 146.69},
    20_000_000: {"loc": 51.641,  "fetch": 116.495, "cmp": 18.731, "repair": float("nan"), "conf": 150.032, "total": float("nan"), "rpl": 195.20},
    25_000_000: {"loc": 51.925,  "fetch": 135.180, "cmp": 20.530, "repair": 852.533,   "conf": 146.113, "total": 1230.731, "rpl": 244.93},
    30_000_000: {"loc": 52.078,  "fetch": 150.135, "cmp": 24.035, "repair": 854.633,   "conf": 157.615, "total": 1264.787, "rpl": 293.33},
    40_000_000: {"loc": 51.675,  "fetch": 186.740, "cmp": 31.187, "repair": 866.879,   "conf": 200.087, "total": 1367.157, "rpl": 391.07},
    50_000_000: {"loc": 49.992,  "fetch": 207.466, "cmp": 36.835, "repair": 766.493,   "conf": 223.802, "total": 1317.543, "rpl": 486.40},
}

COLOR_STATIC  = "#d62728"
COLOR_DYNAMIC = "#1f77b4"
PLT_PARAMS = {"font.size": 11, "figure.titlesize": 14, "axes.titlesize": 13}


# ---------------------------------------------------------------------------
# CSV helpers (stdlib only)
# ---------------------------------------------------------------------------
def read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _flt(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def warm_medians(runs: list[dict], phase_rows: list[dict]) -> dict[int, dict]:
    """Return warm-rep medians per tuple_count for all phases + metadata.

    A one-repetition validation run has only repetition 0.  Retain that
    sample as the fallback so the generated report still describes the run
    instead of silently producing an empty scale table.
    """
    # pivot phase timings by run_id
    phase_map: dict[str, dict[str, float]] = defaultdict(dict)
    for p in phase_rows:
        phase_map[p["run_id"]][p["phase"]] = _flt(p["ms"])

    groups: dict[int, list[dict]] = defaultdict(list)
    all_groups: dict[int, list[dict]] = defaultdict(list)
    for r in runs:
        tc = int(_flt(r["tuple_count"]))
        merged = dict(r)
        merged.update(phase_map.get(r["run_id"], {}))
        all_groups[tc].append(merged)
        if int(r.get("repetition", 0)) >= 1:
            groups[tc].append(merged)

    # For repetitions=1 there is no warm repetition.  Use rep 0 only for
    # scales that otherwise have no sample; multi-repetition runs retain the
    # established warm-repetition semantics.
    for tc, reps in all_groups.items():
        if tc not in groups:
            groups[tc] = reps

    PHASE_KEYS = {
        "loc":    ["tree_localisation_ms",              "tree_localisation"],
        "fetch":  ["candidate_row_fetch_ms",            "candidate_row_fetch"],
        "cmp":    ["row_comparison_ms",                 "row_comparison"],
        "repair": ["repair_write_ms",                   "repair_write"],
        "conf":   ["targeted_post_repair_confirmation_ms", "targeted_post_repair_confirmation"],
        "total":  ["restore_repair_ms"],
        "rpl":    ["mean_rows_per_bad_leaf"],
        # tree traversal depth: child_hash_sql_calls = number of child-hash
        # roundtrips across all levels (increases by fanout when tree gains a level)
        "tree_depth_raw": ["child_hash_sql_calls"],
    }

    result = {}
    for tc, reps in groups.items():
        row = {}
        for key, fields in PHASE_KEYS.items():
            vals = []
            for rep in reps:
                for f in fields:
                    v = _flt(rep.get(f))
                    if not math.isnan(v):
                        vals.append(v)
                        break
            row[key] = median(vals) if vals else float("nan")
        result[tc] = row
    return result


def _bits_per_split(fanout: int) -> int:
    bits = 0
    while (1 << bits) < max(2, int(fanout)):
        bits += 1
    return bits


def _capacity_depth(leaves: int, fanout: int) -> int:
    """Minimum complete-tree depth needed for *leaves* (a lower bound only)."""
    depth = 0
    capacity = 1
    while capacity < leaves:
        capacity *= max(2, int(fanout))
        depth += 1
    return depth


def depth_verification_per_scale(fetched_dir: Path) -> dict[int, dict[str, str]]:
    """Collect measured depth and old-artifact leaf-depth evidence.

    New artifacts contain catalog-derived depth.  Older summary artifacts only
    contain the prefix lengths of selected corruption leaves, which cannot
    prove the global maximum depth.
    """
    rows: dict[int, dict[str, str]] = {}
    dsizes = read_csv(fetched_dir / "dataset_sizes.csv") if (fetched_dir / "dataset_sizes.csv").exists() else []
    selected: dict[int, list[int]] = defaultdict(list)
    provenance = fetched_dir / "fanout_provenance.csv"
    if provenance.exists():
        for row in read_csv(provenance):
            tc = int(_flt(row.get("tuple_count", "0")))
            try:
                capacities = json.loads(row.get("selected_leaf_capacities_json", "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                capacities = {}
            for leaf_key in capacities:
                try:
                    selected[tc].append(int(leaf_key.rsplit("_", 1)[1]))
                except (IndexError, ValueError):
                    continue

    for d in dsizes:
        tc_val = _flt(d.get("tuple_count", ""))
        if math.isnan(tc_val) or tc_val <= 0:
            continue
        tc = int(tc_val)
        fanout_val = _flt(d.get("fanout", "4"))
        fanout = 4 if math.isnan(fanout_val) or fanout_val < 2 else int(fanout_val)
        leaves_val = _flt(d.get("total_leaf_count", ""))
        leaves = int(leaves_val) if not math.isnan(leaves_val) and leaves_val > 0 else 0
        max_prefix = _flt(d.get("max_prefix_len", ""))
        measured_depth = _flt(d.get("tree_depth", ""))
        measured_height = _flt(d.get("tree_height", ""))
        prefixes = sorted(set(selected.get(tc, [])))
        bits = _bits_per_split(fanout)
        selected_heights = sorted({prefix // bits + 1 for prefix in prefixes})
        rows[tc] = {
            "capacity_lower_bound": str(_capacity_depth(leaves, fanout)) if leaves else "N/A",
            "measured_depth": str(int(measured_depth)) if not math.isnan(measured_depth) and measured_depth >= 0 else "N/A",
            "measured_height": str(int(measured_height)) if not math.isnan(measured_height) and measured_height > 0 else "N/A",
            "max_prefix_len": str(int(max_prefix)) if not math.isnan(max_prefix) and max_prefix >= 0 else "N/A",
            "selected_prefixes": ", ".join(str(v) for v in prefixes) or "N/A",
            "selected_heights": ", ".join(str(v) for v in selected_heights) or "N/A",
        }
    return rows


def tree_levels_per_scale(
    runs: list[dict], fetched_dir: Path | None = None,
) -> tuple[dict[int, int], str]:
    """Return display height and whether it is measured or estimated."""
    if fetched_dir and (fetched_dir / "dataset_sizes.csv").exists():
        infos = depth_verification_per_scale(fetched_dir)
        measured = {
            tc: int(info["measured_height"])
            for tc, info in infos.items()
            if info["measured_height"] != "N/A"
        }
        if measured and len(measured) == len(infos):
            return measured, "Actual tree height (root=1)"
        estimated = {
            tc: int(info["capacity_lower_bound"])
            for tc, info in infos.items()
            if info["capacity_lower_bound"] != "N/A"
        }
        if estimated:
            return estimated, "Capacity depth lower bound (not actual height)"
    return {}, "Actual tree height (root=1)"


def all_reps(runs: list[dict]) -> dict[int, list[tuple[int, float]]]:
    out: dict[int, list] = defaultdict(list)
    for r in runs:
        tc = int(_flt(r["tuple_count"]))
        out[tc].append((int(r.get("repetition", 0)), _flt(r.get("restore_repair_ms", "nan"))))
    return {k: sorted(v) for k, v in out.items()}


def parse_progress_jsonl(fetched_dir: Path) -> dict[int, dict]:
    """Parse progress.jsonl to extract dataset_build_timing metrics per scale."""
    p = fetched_dir / "progress.jsonl"
    if not p.exists():
        return {}
    out = {}
    with open(p) as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                if data.get("event") == "dataset_build_timing" or "timings_ms" in data:
                    tc = int(data.get("tuple_count", 0))
                    out[tc] = data.get("timings_ms", {})
            except Exception:
                pass
    return out


def cv_per_scale(dyn_all: dict[int, list[tuple[int, float]]]) -> dict[int, float]:
    """Coefficient of variation (%) of restore_repair_ms across ALL reps per scale."""
    result = {}
    for tc, reps in dyn_all.items():
        vals = [v for (_, v) in reps if not math.isnan(v)]
        if len(vals) >= 2:
            mu = sum(vals) / len(vals)
            sd = stdev(vals)
            result[tc] = (sd / mu * 100.0) if mu > 0 else float("nan")
        else:
            result[tc] = float("nan")
    return result


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
def save_line(path: Path, x, x_labels, d_vals, title, ylabel):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    ax.plot(x, d_vals, label="Dynamic (Synchronous Direct)", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)
    for xi, val in zip(x, d_vals):
        if not math.isnan(val):
            ax.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=8.5, fontweight="bold", color=COLOR_DYNAMIC)
    ax.set_title(title)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_localisation_with_levels(
    path: Path, x, x_labels,
    d_vals: list[float],
    tree_depth: list[int],
    depth_label: str = "Actual tree height (root=1)",
):
    """Localisation latency chart with tree traversal depth as a right-axis
    dotted step line. Depth transitions are annotated with 'Depth N' labels.
    """
    plt.rcParams.update(PLT_PARAMS)
    fig, ax1 = plt.subplots(figsize=(11, 6), dpi=150)

    # Left axis — Dynamic latency line
    ax1.plot(x, d_vals, label="Dynamic (Synchronous Direct)",
             color=COLOR_DYNAMIC, marker="s", linewidth=2.5)
    for xi, val in zip(x, d_vals):
        if not math.isnan(val):
            ax1.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, 7),
                         ha="center", fontsize=8.5, fontweight="bold", color=COLOR_DYNAMIC)
    ax1.set_xlabel("Dataset Size")
    ax1.set_ylabel("Tree Localisation Latency (ms)", color="black")
    ax1.set_xticks(x); ax1.set_xticklabels(x_labels)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.set_title(f"Tree Localisation Latency\n(with {depth_label} — right axis)")

    # Right axis — tree depth step line
    ax2 = ax1.twinx()
    depth_color = "#e377c2"   # pink/purple — distinct from latency line
    ax2.step(x, tree_depth, where="mid", color=depth_color,
             linewidth=2.0, linestyle=":", label=depth_label)
    ax2.set_ylabel(depth_label, color=depth_color)
    ax2.tick_params(axis="y", labelcolor=depth_color)
    
    unique_depths = sorted(set(tree_depth))
    if unique_depths:
        ymin = min(unique_depths) - 1
        ymax = max(unique_depths) + 1
        ax2.set_ylim(ymin, ymax)
        ax2.set_yticks(list(range(min(unique_depths), max(unique_depths) + 1)))

    # annotate depth-transition points
    prev = None
    for xi, depth in zip(x, tree_depth):
        if depth != prev:
            ax2.annotate(
                f"Depth {depth}",
                xy=(xi, depth),
                xytext=(xi + 0.1, depth + 0.15),
                fontsize=9,
                color=depth_color,
                fontweight="bold",
            )
            prev = depth

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               frameon=True, facecolor="white", framealpha=0.9, loc="upper left")

    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_stacked(path: Path, x, x_labels, dyn: dict[int, dict], scales):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    keys   = ["loc",   "fetch",   "cmp",   "repair",  "conf"]
    labels = ["Tree Localisation", "Candidate Fetch", "Row Comparison", "Repair Write (DML)", "Post-Repair Confirmation"]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    bottom = np.zeros(len(scales))
    for key, lbl, col in zip(keys, labels, colors):
        vals = np.array([dyn[tc].get(key, float("nan")) for tc in scales])
        vals = np.nan_to_num(vals)
        ax.bar(x, vals, 0.55, bottom=bottom, label=lbl, color=col)
        bottom += vals
    ax.set_title("Dynamic Recovery Phase Timing Composition")
    ax.set_xlabel("Dataset Size"); ax.set_ylabel("Latency (ms)")
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout(); fig.savefig(path); plt.close(fig)


def save_leaf(path: Path, x, x_labels, d_rpl_per_schema):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    ax.plot(x, d_rpl_per_schema, label="Physical Leaf Occupancy per Schema (Rows / Bad Leaf)", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)
    for xi, val in zip(x, d_rpl_per_schema):
        if not math.isnan(val):
            ax.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=8.5, fontweight="bold", color=COLOR_DYNAMIC)
    ax.set_title("Leaf Occupancy Scaling (Physical Rows per Bad Leaf)")
    ax.set_xlabel("Dataset Size"); ax.set_ylabel("Physical Rows per Bad Leaf")
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout(); fig.savefig(path); plt.close(fig)


def save_dataset_build_time(path: Path, x, x_labels, total_sec: list[float]):
    """Line chart of incremental dataset creation/expansion time (seconds)."""
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    ax.plot(x, total_sec, label="Dataset Build / Expansion Time", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)
    for xi, val in zip(x, total_sec):
        if not math.isnan(val):
            label_text = f"{val:.1f}s\n({val/60.0:.1f}m)" if val >= 60.0 else f"{val:.1f}s"
            ax.annotate(label_text, (xi, val), textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=8.5, fontweight="bold", color=COLOR_DYNAMIC)
    ax.set_title("Incremental Dataset Build Latency per Scale Step (1M → 50M)")
    ax.set_xlabel("Dataset Target Scale")
    ax.set_ylabel("Dataset Expansion Time (Seconds)")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_dataset_build_composition(path: Path, x, x_labels, ds_data: dict[int, dict], scales: list[int]):
    """Stacked bar chart showing dataset creation sub-phase breakdown."""
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)

    heap_sec = []
    merkle_sec = []
    pk_sec = []
    ckpt_sec = []

    for tc in scales:
        t = ds_data.get(tc, {})
        h_tbl = _flt(t.get("healthy_table_ms", 0.0))
        d_tbl = _flt(t.get("damaged_table_ms", 0.0))
        h_idx = _flt(t.get("healthy_indexes_ms", 0.0))
        d_idx = _flt(t.get("damaged_indexes_ms", 0.0))
        pk = _flt(t.get("primary_keys_ms", 0.0))
        ckpt = _flt(t.get("analyze_checkpoint_ms", 0.0))

        h_tbl = 0.0 if math.isnan(h_tbl) else h_tbl
        d_tbl = 0.0 if math.isnan(d_tbl) else d_tbl
        h_idx = 0.0 if math.isnan(h_idx) else h_idx
        d_idx = 0.0 if math.isnan(d_idx) else d_idx
        pk = 0.0 if math.isnan(pk) else pk
        ckpt = 0.0 if math.isnan(ckpt) else ckpt

        heap_sec.append((h_tbl + d_tbl) / 1000.0)
        merkle_sec.append((h_idx + d_idx) / 1000.0)
        pk_sec.append(pk / 1000.0)
        ckpt_sec.append(ckpt / 1000.0)

    categories = [
        ("Heap Data Population", np.array(heap_sec), "#1f77b4"),
        ("Merkle Tree Index Build", np.array(merkle_sec), "#ff7f0e"),
        ("Primary Keys & Logging", np.array(pk_sec), "#2ca02c"),
        ("Catalog & Analyze/Checkpoint", np.array(ckpt_sec), "#9467bd"),
    ]

    bottom = np.zeros(len(scales))
    for label, vals, color in categories:
        if np.any(vals > 0):
            ax.bar(x, vals, 0.55, bottom=bottom, label=label, color=color)
            bottom += vals

    ax.set_title("Dataset Construction Phase Timing Composition")
    ax.set_xlabel("Dataset Target Scale")
    ax.set_ylabel("Expansion Time (Seconds)")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def generate_plots(plots_dir: Path, dyn_med: dict[int, dict], scales: list[int],
                   tree_depth: dict[int, int] | None = None,
                   tree_depth_label: str = "Tree Levels",
                   cv_vals: dict[int, float] | None = None,
                   ds_data: dict[int, dict] | None = None) -> dict[str, Path]:
    plots_dir.mkdir(parents=True, exist_ok=True)
    x = list(range(len(scales)))
    x_labels = [f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc) for tc in scales]

    def dv(key): return [dyn_med[tc].get(key, float("nan")) for tc in scales]

    specs = [
        ("total_recovery_latency.png",       "total",  "Total Recovery Latency",                 "Total Recovery Latency (ms)"),
        ("tree_localisation_comparison.png", "loc",    "Tree Localisation Latency",              "Localisation Latency (ms)"),
        ("candidate_fetch_comparison.png",   "fetch",  "Candidate Row Fetch Latency",            "Candidate Fetch Latency (ms)"),
        ("row_comparison_comparison.png",    "cmp",    "Row / Tuple Comparison Latency",         "Row Comparison Latency (ms)"),
        ("repair_write_comparison.png",      "repair", "Repair Write Latency",                   "Repair Write Latency (ms)"),
        ("post_repair_confirmation_comparison.png", "conf", "Post-Repair Confirmation Latency",  "Confirmation Latency (ms)"),
    ]
    out = {}
    for fname, key, title, ylabel in specs:
        p = plots_dir / fname
        save_line(p, x, x_labels, dv(key), title, ylabel)
        out[fname] = p

    p = plots_dir / "phase_stacked_composition.png"
    save_stacked(p, x, x_labels, dyn_med, scales)
    out["phase_stacked_composition.png"] = p

    # Localisation chart — regenerate with tree depth overlay if available
    if tree_depth is not None:
        td_vals = [tree_depth.get(tc, 0) for tc in scales]
        p = plots_dir / "tree_localisation_comparison.png"
        save_localisation_with_levels(
            p, x, x_labels, dv("loc"), td_vals, tree_depth_label
        )
        out["tree_localisation_comparison.png"] = p  # overwrites generic version

    p = plots_dir / "leaf_occupancy_scaling.png"
    rpl_comb = dv("rpl")
    rpl_per_schema = [v / 2.0 if not math.isnan(v) else float("nan") for v in rpl_comb]
    save_leaf(p, x, x_labels, rpl_per_schema)
    out["leaf_occupancy_scaling.png"] = p

    if cv_vals is not None:
        p = plots_dir / "cv_per_scale.png"
        save_cv(p, x, x_labels, [cv_vals.get(tc, float("nan")) for tc in scales])
        out["cv_per_scale.png"] = p

    if ds_data:
        ds_sec = [_flt(ds_data.get(tc, {}).get("dataset_total_ms", float("nan"))) / 1000.0 for tc in scales]
        if any(not math.isnan(s) for s in ds_sec):
            p = plots_dir / "dataset_build_time.png"
            save_dataset_build_time(p, x, x_labels, ds_sec)
            out["dataset_build_time.png"] = p

            p = plots_dir / "dataset_build_composition.png"
            save_dataset_build_composition(p, x, x_labels, ds_data, scales)
            out["dataset_build_composition.png"] = p

    return out


def save_cv(path: Path, x, x_labels, cv_vals: list[float]):
    """Bar chart of CV (%) per scale with a 20% stability threshold line."""
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    colors = ["#d62728" if (not math.isnan(v) and v > 20) else "#1f77b4" for v in cv_vals]
    clean = [0.0 if math.isnan(v) else v for v in cv_vals]
    bars = ax.bar(x, clean, 0.55, color=colors, label="CV% per scale")
    ax.axhline(20, color="#ff7f0e", linewidth=1.8, linestyle="--", label="20% stability threshold")
    # annotate values on bars
    for bar, val in zip(bars, cv_vals):
        if not math.isnan(val):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    f"{val:.1f}%", ha="center", va="bottom", fontsize=9)
    ax.set_title("Coefficient of Variation (CV%) of Total Recovery Latency per Scale")
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("CV (σ/μ × 100%)")
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout(); fig.savefig(path); plt.close(fig)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------
def fmt(v: float, d: int = 2) -> str:
    return "N/A" if math.isnan(v) else f"{v:,.{d}f}"


def build_report(
    dynamic_dir: Path,
    run_id: str,
    plots_dir: Path,
    depth_info: dict[int, dict[str, str]] | None = None,
    ds_data: dict[int, dict] | None = None,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    runs_csv   = dynamic_dir / "runs.csv"
    phase_csv  = dynamic_dir / "phase_timings.csv"
    config_json = dynamic_dir / "config.json"

    if not runs_csv.exists():
        raise FileNotFoundError(f"Missing runs.csv in {dynamic_dir}")

    runs   = read_csv(runs_csv)
    phases = read_csv(phase_csv) if phase_csv.exists() else []
    dyn_med = warm_medians(runs, phases)
    dyn_all = all_reps(runs)
    scales  = sorted(dyn_med)

    cfg = {}
    if config_json.exists():
        try: cfg = json.loads(config_json.read_text())
        except Exception: pass

    dataset_rows = read_csv(dynamic_dir / "dataset_sizes.csv") if (dynamic_dir / "dataset_sizes.csv").exists() else []
    first_dataset = dataset_rows[0] if dataset_rows else {}
    report_fanout = cfg.get("fanout") or first_dataset.get("fanout") or 4
    report_split = cfg.get("split_threshold") or first_dataset.get("split_threshold") or 32
    report_merge = cfg.get("merge_threshold") or first_dataset.get("merge_threshold") or 8
    report_bad_leaves = cfg.get("bad_leaf_count") or 75
    has_measured_depth = bool(depth_info) and all(
        row.get("measured_height") not in (None, "N/A") for row in depth_info.values()
    )
    depth_basis_note = (
        "This run contains the native catalog-derived depth and height."
        if has_measured_depth
        else "This old summary artifact contains only selected corruption-leaf prefixes, so a global maximum cannot be claimed."
    )
    measured_summary = ", ".join(
        f"{tc // 1_000_000}M depth {depth_info[tc]['measured_depth']} "
        f"(height {depth_info[tc]['measured_height']})"
        for tc in sorted(depth_info or {})
        if depth_info[tc].get("measured_depth") not in (None, "N/A")
    )
    depth_conclusion = (
        f"**Conclusion for this run:** the native catalog measures {measured_summary}."
        if has_measured_depth
        else "**Conclusion for this artifact:** the capacity sequence is not verified native tree depth."
    )

    total_runs = len(runs)
    valid_runs = sum(1 for r in runs if str(r.get("valid","")).strip().lower() in ("1","true"))
    pend_corr  = sum(int(_flt(r.get("legacy_merkle_pending_rows_after_corruption", 0) or 0)) for r in runs)
    pend_rep   = sum(int(_flt(r.get("legacy_merkle_pending_rows_after_repair",    0) or 0)) for r in runs)

    lbl = lambda tc: f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)

    # relative plot path from the report markdown file (sibling plots/ dir)
    def plt_rel(fname): return f"./plots/{fname}"

    L = []
    L += [
        f"# Recovery Run Report: `{run_id}`",
        "",
        f"> **Generated**: {now}  ",
        f"> **Profile**: `{cfg.get('profile','size-scaling-k75-c300')}` | Fanout F={report_fanout} | Split {report_split} | Merge {report_merge} | K={report_bad_leaves} bad leaves | C={cfg.get('corrupted_tuple_count') or 300} corruptions | Audit: `{cfg.get('audit_mode','skip')}`  ",
        f"> **Dynamic Artifact**: `scripts/benchmark/recovery/fetched/{run_id}`",
        "",
        "---",
        "",
        "## Contract Verification",
        "",
        "| Metric | Value |",
        "|:---|:---|",
        f"| Total Runs | `{total_runs}` |",
        f"| Valid Runs | `{valid_runs}/{total_runs}` {'✅' if valid_runs == total_runs else '❌'} |",
        f"| `legacy_merkle_pending_rows_after_corruption` | `{pend_corr}` {'✅' if pend_corr == 0 else '⚠️'} |",
        f"| `legacy_merkle_pending_rows_after_repair` | `{pend_rep}` {'✅' if pend_rep == 0 else '⚠️'} |",
        f"| Scale Points Covered | `{len(scales)}` ({', '.join(lbl(s) for s in scales)}) |",
        "",
        "---",
        "",
        "## Depth Verification",
        "",
        "The capacity column is `ceil(log_F(leaf_count))`; it is only a lower bound. "
        "Measured depth is the maximum `prefix_len / bits_per_split` from the native "
        "`ariabc_internal.merkle_node` catalog, and measured height includes the root. ",
        depth_basis_note,
        "",
        "| Scale | Capacity lower bound | Measured depth | Measured height | Selected leaf prefix lengths | Selected leaf heights |",
        "|:---|---:|---:|---:|:---|:---|",
    ]
    if depth_info:
        for tc in scales:
            d = depth_info.get(tc, {})
            L.append(
                f"| **{lbl(tc)}** | {d.get('capacity_lower_bound', 'N/A')} | "
                f"{d.get('measured_depth', 'N/A')} | {d.get('measured_height', 'N/A')} | "
                f"{d.get('selected_prefixes', 'N/A')} | {d.get('selected_heights', 'N/A')} |"
            )
    L += [
        "",
        depth_conclusion,
        "",
        "---",
        "",
        "## 1. Total Recovery Latency",
        "",
        f"![Total Recovery Latency]({plt_rel('total_recovery_latency.png')})",
        "",
        "---",
        "",
        "## 2. Phase Breakdown and Composition",
        "",
        f"![Phase Timing Composition]({plt_rel('phase_stacked_composition.png')})",
        "",
        "---",
        "",
        "## 3. Tree Localisation Phase",
        "",
        f"![Tree Localisation Latency]({plt_rel('tree_localisation_comparison.png')})",
        "",
        "---",
        "",
        "## 4. Candidate Fetch Phase",
        "",
        f"![Candidate Fetch Latency]({plt_rel('candidate_fetch_comparison.png')})",
        "",
        "---",
        "",
        "## 5. Row Comparison Phase",
        "",
        f"![Row Comparison Latency]({plt_rel('row_comparison_comparison.png')})",
        "",
        "---",
        "",
        "## 6. Repair Write Phase",
        "",
        f"![Repair Write Latency]({plt_rel('repair_write_comparison.png')})",
        "",
        "---",
        "",
        "## 7. Post-Repair Confirmation Phase",
        "",
        f"![Post-Repair Confirmation Latency]({plt_rel('post_repair_confirmation_comparison.png')})",
        "",
        "---",
        "",
        "## 8. Leaf Occupancy Scaling",
        "",
        f"![Leaf Occupancy Scaling]({plt_rel('leaf_occupancy_scaling.png')})",
        "",
        "---",
        "",
        "## 9. Coefficient of Variation (CV%) per Scale",
        "",
        "> CV = σ/μ × 100% computed across **all** repetitions of `restore_repair_ms` per scale point.",
        "> Bars above the 20% threshold (orange line) indicate high variance — likely from checkpoint/WAL interference.",
        "",
        f"![CV% per Scale]({plt_rel('cv_per_scale.png')})",
    ]

    if ds_data and any(tc in ds_data for tc in scales):
        L += [
            "",
            "---",
            "",
            "## 10. Dataset Construction & Incremental Expansion Latency",
            "",
            f"![Dataset Construction Latency]({plt_rel('dataset_build_time.png')})",
            "",
            f"![Dataset Construction Breakdown]({plt_rel('dataset_build_composition.png')})",
            "",
            "### 10.1 Step-by-Step Incremental Dataset Expansion Time & Phase Composition",
            "",
            "| Scale | Appended Tuples | Setup Mode | Heap Population (s) | Merkle Tree Build (s) | PK & Catalog (s) | Step Total (s) | Step Total (min) | Cumulative Time |",
            "|:---|:---|:---|---:|---:|---:|---:|---:|---:|",
        ]
        cum_sec = 0.0
        tot_all_sec = sum(_flt(ds_data.get(tc, {}).get("dataset_total_ms", 0.0)) / 1000.0 for tc in scales)
        for tc in scales:
            t = ds_data.get(tc, {})
            tot_ms = _flt(t.get("dataset_total_ms", float("nan")))
            tot_s = tot_ms / 1000.0 if not math.isnan(tot_ms) else float("nan")
            if not math.isnan(tot_s):
                cum_sec += tot_s
            mode = t.get("dataset_setup_mode", "bulk-logged" if tc == scales[0] else "incremental-expansion")
            prev_tc = t.get("previous_tuple_count")
            appended = t.get("appended_tuple_count", tc if tc == scales[0] else (tc - prev_tc) if prev_tc is not None else tc)
            app_lbl = f"+{appended // 1_000_000}M" if appended < tc else f"{appended // 1_000_000}M" if appended >= 1_000_000 else f"+{appended:,}"

            h_tbl = _flt(t.get("healthy_table_ms", 0.0))
            d_tbl = _flt(t.get("damaged_table_ms", 0.0))
            h_idx = _flt(t.get("healthy_indexes_ms", 0.0))
            d_idx = _flt(t.get("damaged_indexes_ms", 0.0))
            pk = _flt(t.get("primary_keys_ms", 0.0))
            ckpt = _flt(t.get("analyze_checkpoint_ms", 0.0))

            h_tbl = 0.0 if math.isnan(h_tbl) else h_tbl
            d_tbl = 0.0 if math.isnan(d_tbl) else d_tbl
            h_idx = 0.0 if math.isnan(h_idx) else h_idx
            d_idx = 0.0 if math.isnan(d_idx) else d_idx
            pk = 0.0 if math.isnan(pk) else pk
            ckpt = 0.0 if math.isnan(ckpt) else ckpt

            heap_s = (h_tbl + d_tbl) / 1000.0
            merkle_s = (h_idx + d_idx) / 1000.0
            pk_ckpt_s = (pk + ckpt) / 1000.0

            tot_s_str = f"{tot_s:.2f} s" if not math.isnan(tot_s) else "N/A"
            tot_m_str = f"{tot_s / 60.0:.2f} min" if not math.isnan(tot_s) else "N/A"
            cum_str = f"{cum_sec:.2f} s ({cum_sec / 60.0:.2f} m)" if not math.isnan(tot_s) else "N/A"

            L.append(f"| **{lbl(tc)}** | {app_lbl} | `{mode}` | {heap_s:.2f} s | {merkle_s:.2f} s | {pk_ckpt_s:.2f} s | {tot_s_str} | {tot_m_str} | {cum_str} |")

        L += [
            "",
            "### 10.2 Component Breakdown Details (ms)",
            "",
            "Exact millisecond telemetry for each dataset creation sub-phase:",
            "",
            "| Scale | Healthy Heap (ms) | Damaged Heap (ms) | Healthy Merkle Index (ms) | Damaged Merkle Index (ms) | Primary Keys (ms) | Analyze / Catalog (ms) | Total Step (ms) |",
            "|:---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for tc in scales:
            t = ds_data.get(tc, {})
            h_tbl = _flt(t.get("healthy_table_ms", float("nan")))
            d_tbl = _flt(t.get("damaged_table_ms", float("nan")))
            h_idx = _flt(t.get("healthy_indexes_ms", float("nan")))
            d_idx = _flt(t.get("damaged_indexes_ms", float("nan")))
            pk = _flt(t.get("primary_keys_ms", float("nan")))
            ckpt = _flt(t.get("analyze_checkpoint_ms", float("nan")))
            tot_ms = _flt(t.get("dataset_total_ms", float("nan")))

            pk_str = fmt(pk, 2) if not math.isnan(pk) else "—"
            ckpt_str = fmt(ckpt, 2) if not math.isnan(ckpt) else "—"

            L.append(f"| **{lbl(tc)}** | {fmt(h_tbl, 2)} | {fmt(d_tbl, 2)} | {fmt(h_idx, 2)} | {fmt(d_idx, 2)} | {pk_str} | {ckpt_str} | **{fmt(tot_ms, 2)} ms** |")

        L += [
            "",
            "### 10.3 Cumulative Dataset Construction Time Progression",
            "",
            "| Target Scale | Step Time (s) | Cumulative Elapsed (s) | Cumulative Elapsed (min) | % of Total Build Time |",
            "|:---|---:|---:|---:|---:|",
        ]
        running_sec = 0.0
        for tc in scales:
            t = ds_data.get(tc, {})
            tot_ms = _flt(t.get("dataset_total_ms", float("nan")))
            tot_s = tot_ms / 1000.0 if not math.isnan(tot_ms) else float("nan")
            if not math.isnan(tot_s):
                running_sec += tot_s
                pct = (running_sec / tot_all_sec * 100.0) if tot_all_sec > 0 else 0.0
                pct_str = f"{pct:.1f}%"
            else:
                pct_str = "N/A"
            tot_s_str = f"{tot_s:.2f} s" if not math.isnan(tot_s) else "N/A"
            cum_s_str = f"{running_sec:.2f} s" if not math.isnan(tot_s) else "N/A"
            cum_m_str = f"{running_sec / 60.0:.2f} min" if not math.isnan(tot_s) else "N/A"
            L.append(f"| **{lbl(tc)}** | {tot_s_str} | {cum_s_str} | {cum_m_str} | {pct_str} |")

    L += [
        "",
        "---",
        "",
        "## Full Phase Recovery Matrix",
        "",
        "Values are warm-repetition medians (rep ≥ 1) in milliseconds.",
        "",
        "| Scale | Tree Localisation | Cand. Fetch | Row Cmp | Repair Write | Post-Repair Conf | **Total Recovery** |",
        "|:---|---:|---:|---:|---:|---:|---:|",
    ]

    for tc in scales:
        d = dyn_med[tc]
        L.append(f"| **{lbl(tc)}** | {fmt(d['loc'])} | {fmt(d['fetch'])} | {fmt(d['cmp'])} | {fmt(d['repair'])} | {fmt(d['conf'])} | **{fmt(d['total'])} ms** |")

    L += [
        "",
        "---",
        "",
        "## Leaf Occupancy Breakdown",
        "",
        "> **Note on Leaf Occupancy**:",
        "> - **Physical Rows / Bad Leaf (Per Schema)**: The true physical row count per leaf bucket in PostgreSQL (bounded by $T_{\\text{split}} = 32$ and $T_{\\text{merge}} = 8$).",
        "> - **Combined Candidate Rows (Healthy + Damaged)**: Total rows fetched across both replicas ($2 \\times \\text{physical rows}$).",
        "",
        "| Scale | Physical Rows / Bad Leaf (Per Schema) | Combined Candidate Rows (Healthy + Damaged) | Total Candidate Rows Fetched ($K=75$) |",
        "|:---|---:|---:|---:|",
    ]
    for tc in scales:
        comb_rpl = dyn_med[tc].get("rpl", float("nan"))
        schema_rpl = comb_rpl / 2.0 if not math.isnan(comb_rpl) else float("nan")
        cand_rows = comb_rpl * 75.0 if not math.isnan(comb_rpl) else float("nan")
        L.append(f"| **{lbl(tc)}** | {fmt(schema_rpl, 2)} rows/leaf | {fmt(comb_rpl, 2)} rows/leaf | {fmt(cand_rows, 0)} rows |")

    cv_map = cv_per_scale(dyn_all)
    L += ["", "---", "", "## Repetition Stability (`restore_repair_ms`)", ""]
    max_reps = max((len(v) for v in dyn_all.values()), default=0)
    L.append("| Scale | " + " | ".join(f"Rep {i}" for i in range(max_reps)) + " | Warm Median | CV% |")
    L.append("|:---|" + "---:|" * (max_reps + 1) + "---:|")
    for tc in scales:
        reps = dyn_all.get(tc, [])
        warm = [v for (r, v) in reps if r >= 1]
        wm = median(warm) if warm else float("nan")
        cv = cv_map.get(tc, float("nan"))
        cv_str = f"`{cv:.1f}%` ⚠️" if (not math.isnan(cv) and cv > 20) else f"`{cv:.1f}%`" if not math.isnan(cv) else "N/A"
        cells = [lbl(tc)] + [fmt(v) for (_, v) in reps] + ["—"] * (max_reps - len(reps)) + [f"**{fmt(wm)}**", cv_str]
        L.append("| " + " | ".join(cells) + " |")

    L += ["", "---", "", "## Artifact Provenance", "",
          "| Field | Value |", "|:---|:---|",
          f"| Run ID | `{run_id}` |",
          f"| Generated | `{now}` |",
          f"| Dynamic Dir | `scripts/benchmark/recovery/fetched/{run_id}` |",
          f"| Reference Doc | `Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md` |",
          ""]

    return "\n".join(L)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetched-dir", required=True)
    ap.add_argument("--output", help="Path to output .md file")
    args = ap.parse_args()

    fetched_dir = Path(args.fetched_dir).resolve()
    if not fetched_dir.is_dir():
        print(f"ERROR: {fetched_dir} does not exist", file=sys.stderr); sys.exit(1)

    run_id = fetched_dir.name
    if args.output:
        report_path = Path(args.output)
        plots_dir   = report_path.parent / "plots"
    else:
        report_dir  = ROOT / "Dynamic_merkle_docs" / "run_reports" / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"RECOVERY_RUN_REPORT_{run_id}.md"
        plots_dir   = report_dir / "plots"

    # Load data
    runs   = read_csv(fetched_dir / "runs.csv")
    phases = read_csv(fetched_dir / "phase_timings.csv") if (fetched_dir / "phase_timings.csv").exists() else []
    ds_data = parse_progress_jsonl(fetched_dir)
    dyn_med   = warm_medians(runs, phases)
    dyn_all   = all_reps(runs)
    cv_map    = cv_per_scale(dyn_all)
    depth_info = depth_verification_per_scale(fetched_dir)
    td_map, td_label = tree_levels_per_scale(runs, fetched_dir)
    scales    = sorted(dyn_med)

    # Generate plots (including CV chart, dataset build charts, and tree-depth localisation overlay)
    generate_plots(
        plots_dir,
        dyn_med,
        scales,
        cv_vals=cv_map,
        tree_depth=td_map,
        tree_depth_label=td_label,
        ds_data=ds_data,
    )

    # Generate markdown
    report = build_report(fetched_dir, run_id, plots_dir, depth_info, ds_data=ds_data)
    report_path.write_text(report)
    print(str(report_path))


if __name__ == "__main__":
    main()
